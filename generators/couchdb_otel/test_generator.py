"""CouchDB-specific tests for the couchdb_otel generator."""

from __future__ import annotations

import random
import threading
import time

from generators.couchdb_otel.generator import (
    HTTP_METHODS,
    HTTP_STATUS_CODES,
    VIEW_TYPES,
    CouchDBState,
    _build_metrics,
    run,
)


class _CaptureClient:
    def __init__(self):
        self.batches: list[tuple[dict, dict, list[dict]]] = []

    def send_metrics_scoped(self, resource, scope, metrics):
        self.batches.append((resource, scope, metrics))

    def close(self):
        pass


def _attr_map(metric: dict) -> dict[str, str]:
    dps = (metric.get("sum") or metric.get("gauge") or {}).get("dataPoints") or []
    if not dps:
        return {}
    out = {}
    for a in dps[0].get("attributes") or []:
        key = a.get("key")
        val = a.get("value", {})
        out[key] = val.get("stringValue") or val.get("intValue") or str(val)
    return out


def test_build_metrics_covers_dashboard_dimensions():
    """Overview panels BY http.method / view / status_code / operation."""
    state = CouchDBState(random.Random(0))
    state.tick(random.Random(0))
    metrics = _build_metrics(state, random.Random(0), lat_mult=1.0)

    by_name: dict[str, list[dict]] = {}
    for m in metrics:
        by_name.setdefault(m["name"], []).append(m)

    assert set(by_name) == {
        "couchdb.database.open",
        "couchdb.file_descriptor.open",
        "couchdb.average_request_time",
        "couchdb.httpd.bulk_requests",
        "couchdb.httpd.requests",
        "couchdb.httpd.responses",
        "couchdb.httpd.views",
        "couchdb.database.operations",
    }

    for name in ("couchdb.database.open", "couchdb.file_descriptor.open"):
        assert "sum" in by_name[name][0]
        assert by_name[name][0]["sum"]["isMonotonic"] is False

    assert "gauge" in by_name["couchdb.average_request_time"][0]

    methods = {_attr_map(m).get("http.method") for m in by_name["couchdb.httpd.requests"]}
    assert methods == set(HTTP_METHODS)

    statuses = {_attr_map(m).get("http.status_code") for m in by_name["couchdb.httpd.responses"]}
    assert statuses == set(HTTP_STATUS_CODES)
    assert "409" in statuses

    views = {_attr_map(m).get("view") for m in by_name["couchdb.httpd.views"]}
    assert views == set(VIEW_TYPES)

    ops = {_attr_map(m).get("operation") for m in by_name["couchdb.database.operations"]}
    assert ops == {"reads", "writes"}


def test_run_emits_couchdb_node_name_resource_attr():
    """Panels group BY couchdb.node.name — must be a resource attribute."""
    client = _CaptureClient()
    stop = threading.Event()

    def _target():
        run(
            client,
            stop,
            scenario_data={
                "namespace": "inttest",
                "generator_config": {
                    "service_name": "couchdb",
                    "host_name": "inttest-couchdb",
                    "node_name": "couchdb@inttest-couchdb",
                },
                "channel_registry": {},
            },
        )

    t = threading.Thread(target=_target)
    t.start()
    deadline = time.time() + 5
    while time.time() < deadline and not client.batches:
        time.sleep(0.05)
    stop.set()
    t.join(timeout=5)

    assert client.batches, "expected at least one metrics batch"
    resource, scope, metrics = client.batches[0]
    attrs = {a["key"]: a["value"].get("stringValue") for a in resource["attributes"]}
    assert attrs.get("couchdb.node.name") == "couchdb@inttest-couchdb"
    # Intake appends ".otel"; resource attr is the receiver name only.
    assert attrs.get("data_stream.dataset") == "couchdbreceiver"
    assert "couchdbreceiver" in scope["name"]
    assert {m["name"] for m in metrics} >= {
        "couchdb.httpd.requests",
        "couchdb.httpd.responses",
        "couchdb.httpd.views",
        "couchdb.average_request_time",
    }


def test_counters_start_near_zero_for_rate_charts():
    state = CouchDBState(random.Random(42))
    assert state.bulk_requests < 10_000
    assert state.db_reads < 10_000
    assert max(state.http_requests.values()) < 10_000
    assert max(state.http_responses.values()) < 10_000


def test_counters_keep_advancing():
    rng = random.Random(7)
    state = CouchDBState(rng)
    before = (state.db_reads, state.http_requests["GET"], state.bulk_requests)
    for _ in range(5):
        state.tick(rng)
    assert state.db_reads > before[0]
    assert state.http_requests["GET"] > before[1]
    assert state.bulk_requests > before[2]


def _gauge_value(metrics: list[dict], name: str) -> float:
    for m in metrics:
        if m["name"] == name and "gauge" in m:
            dp = m["gauge"]["dataPoints"][0]
            if "asDouble" in dp:
                return float(dp["asDouble"])
            return float(dp["asInt"])
    raise AssertionError(f"missing gauge {name}")


def _nonmono_value(metrics: list[dict], name: str) -> int:
    for m in metrics:
        if m["name"] == name and "sum" in m:
            dp = m["sum"]["dataPoints"][0]
            if "asInt" in dp:
                return int(dp["asInt"])
            return int(dp["asDouble"])
    raise AssertionError(f"missing sum {name}")

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.couchdb_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['conflict_storm_elevated_409_responses', 'file_descriptor_exhaustion_risk', 'high_5xx_server_error_rate', 'high_average_request_time_latency', 'high_error_rate_4xx_and_5xx', 'open_databases_count_high']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
