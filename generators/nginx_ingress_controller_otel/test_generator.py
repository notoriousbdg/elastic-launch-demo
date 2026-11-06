"""nginx_ingress-specific tests for the nginx_ingress generator."""

from __future__ import annotations

import random
from unittest.mock import MagicMock, patch

from generators.nginx_ingress_controller_otel import SPEC, generator as mod
from generators.nginx_ingress_controller_otel.generator import (
    _BODY_STRUCTURED_PROPERTIES,
    _access_record,
    _error_record,
)


def _attr_map(record: dict) -> dict:
    out = {}
    for item in record.get("attributes") or []:
        key = item["key"]
        val = item["value"]
        if "stringValue" in val:
            out[key] = val["stringValue"]
        elif "intValue" in val:
            out[key] = int(val["intValue"])
        elif "doubleValue" in val:
            out[key] = val["doubleValue"]
        elif "kvlistValue" in val:
            nested = {}
            for sub in val["kvlistValue"].get("values", []):
                sv = sub["value"]
                if "doubleValue" in sv:
                    nested[sub["key"]] = sv["doubleValue"]
                elif "stringValue" in sv:
                    nested[sub["key"]] = sv["stringValue"]
            out[key] = nested
    return out


def _body_keys(record: dict) -> set[str]:
    values = (record.get("body") or {}).get("kvlistValue", {}).get("values") or []
    return {item["key"] for item in values}


def test_access_record_puts_dashboard_fields_in_body_and_attributes():
    rec = _access_record(random.Random(0), "demo", ["api-svc"], 0.0, 0)
    body_keys = _body_keys(rec)
    attrs = _attr_map(rec)
    for key in (
        "upstream.name",
        "url.path",
        "url.original",
        "user_agent.name",
        "http.response.status_code",
        "http.response.body.size",
        "upstream.response.size",
    ):
        assert key in body_keys, key
        assert key in attrs, key
    assert attrs["event.name"] == "nginx_ingress_controller.access"
    assert "source.address" in attrs
    geo = attrs["geo.location"]
    # Must be "lat,lon" for geo_point — not a stringified {'lat':…} dict
    # (``"lat" in geo`` false-positives on that broken form).
    assert isinstance(geo, str)
    parts = geo.split(",")
    assert len(parts) == 2
    lat, lon = float(parts[0]), float(parts[1])
    assert -90.0 <= lat <= 90.0
    assert -180.0 <= lon <= 180.0


def test_error_record_puts_dashboard_fields_in_body_and_attributes():
    rec = _error_record(random.Random(0), "demo", 0)
    body_keys = _body_keys(rec)
    attrs = _attr_map(rec)
    for key in ("log.level", "message", "source.file.name"):
        assert key in body_keys, key
        assert key in attrs, key
    assert attrs["event.name"] == "nginx_ingress_controller.error"
    assert attrs["log.level"] in ("E", "W", "F")


def test_body_structured_property_map_covers_dashboard_leaves():
    required = {
        "http.response.body.size",
        "http.response.status_code",
        "upstream.name",
        "upstream.response.size",
        "url.original",
        "url.path",
        "user_agent.name",
        "http.request.method",
        "log.level",
        "message",
        "source.file.name",
    }
    assert required <= set(_BODY_STRUCTURED_PROPERTIES)


# ── impact keys ────────────────────────────────────────────────────────────────
# nginx_ingress_controller_otel@0.4.1 ships two dashboards and NO alert rule templates,
# so these two keys only move dashboard panels; there is nothing to map 1:1.

def test_impact_keys_are_the_two_dashboard_knobs_and_documented_as_such():
    assert sorted(SPEC.impact_keys) == ["error_rate", "records_per_cycle"]
    assert SPEC.wants_chaos is True
    for key, meta in SPEC.impact_keys.items():
        assert "dashboard" in meta["doc"].lower() and "no alert" in meta["doc"].lower(), key


class _FakeChaos:
    def get_active_channels(self):
        return ["ch1"]


_REAL_RANDOM = random.Random


def _run_cycle(impact: dict | None, seed: int = 7):
    """One loop of run() with business_load pinned to 1.0 and a seeded RNG (run() builds its own);
    returns (access, error) records."""
    client = MagicMock()
    stop = MagicMock()
    stop.is_set.side_effect = [False, True]
    stop.wait.return_value = None
    registry, controller = {}, None
    if impact is not None:
        controller = _FakeChaos()
        registry = {"ch1": {"generator_impact": {"nginx_ingress_controller_otel": impact}}}
    with patch.object(mod, "business_load", return_value=1.0), \
            patch.object(mod, "ensure_dashboard_mappings_from_env"), \
            patch.object(mod.random, "Random", side_effect=lambda *a: _REAL_RANDOM(seed)):
        mod.run(client, stop, scenario_data={
            "namespace": "testns",
            "generator_config": {"chaos": "explicit"},
            "channel_registry": registry,
        }, chaos_controller=controller)
    calls = client.send_logs_scoped.call_args_list
    return calls[0].args[2], calls[1].args[2]


def _error_share(records: list[dict]) -> float:
    codes = [_attr_map(r)["http.response.status_code"] for r in records]
    return sum(1 for c in codes if int(c) >= 400) / len(codes)


def test_healthy_cycle_has_no_error_responses_and_the_default_volume():
    access, errors = _run_cycle(None)
    assert len(access) == 20
    assert _error_share(access) == 0.0
    assert len(errors) == 1


def test_error_rate_alone_raises_the_error_response_share_and_error_log_volume():
    access, errors = _run_cycle({"error_rate": 0.7})
    assert len(access) == 20            # volume unchanged by error_rate
    assert _error_share(access) >= 0.5  # 0.7 expected over 20 records (seeded)
    assert len(errors) == int(20 * 0.7)  # error logs scale with error_rate


def test_records_per_cycle_alone_sets_the_volume_without_adding_errors():
    access, errors = _run_cycle({"records_per_cycle": 250})
    assert len(access) == 250
    assert _error_share(access) == 0.0
    assert len(errors) == max(1, int(250 * 0.05))
