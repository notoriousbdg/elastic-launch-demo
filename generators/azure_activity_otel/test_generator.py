"""Azure Activity-specific tests for the azure_activity generator."""
from __future__ import annotations

import random

from generators.azure_activity_otel.generator import _generate_records


def _attrs(record: dict) -> dict:
    return {a["key"]: next(iter(a["value"].values())) for a in record.get("attributes", [])}


_IDENTITY_KEYS = (
    "azure.identity.authorization.evidence.principal.type",
    "user.id",
    "network.peer.address",
)


def test_every_category_populates_recent_activity_identity_columns():
    """Recent Activity KEEP shows principal.type / user.id / peer for all categories."""
    rng = random.Random(7)
    principals = ["admin@demo.onmicrosoft.com", "ci-service@demo.onmicrosoft.com"]
    # Large batch so every weighted category appears.
    records = _generate_records(
        principals, "sub-1", "demo-rg", "eastus", rng, count=80, error_rate=0.0,
    )
    categories = { _attrs(r).get("azure.category") for r in records }
    assert "Administrative" in categories
    assert categories >= {"ServiceHealth", "ResourceHealth", "Recommendation", "Alert"} or len(categories) >= 3

    for record in records:
        attrs = _attrs(record)
        for key in _IDENTITY_KEYS:
            assert attrs.get(key), f"{attrs.get('azure.category')}: missing {key}"


def test_admin_user_caller_is_email():
    rng = random.Random(1)
    principals = ["admin@demo.onmicrosoft.com"]
    # Force Administrative by generating until we get one with User type.
    found = False
    for _ in range(40):
        records = _generate_records(
            principals, "sub-1", "demo-rg", "eastus", rng, count=20, error_rate=0.0,
        )
        for record in records:
            attrs = _attrs(record)
            if attrs.get("azure.category") != "Administrative":
                continue
            if attrs.get("azure.identity.authorization.evidence.principal.type") == "User":
                assert attrs["user.id"] == "admin@demo.onmicrosoft.com"
                found = True
                break
        if found:
            break
    assert found


# --- 1:1 alert impact_keys coverage (auto) ---
from generators.azure_activity_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['change_storm_high_mutation_rate', 'failed_administrative_operations', 'resource_health_degradation', 'role_assignment_write_privilege_escalation', 'security_events', 'sensitive_key_access', 'service_health_incident']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


def test_change_storm_clears_its_threshold_from_one_caller_in_five_minutes(monkeypatch):
    """OOTB Change storm needs >= 50 mutations per caller; spreading them never fires."""
    from unittest.mock import MagicMock

    from generators.azure_activity_otel import generator as mod

    monkeypatch.setattr(mod, "business_load", lambda now=None: 0.15)
    ticks = 300 // mod.BATCH_INTERVAL_MAX
    stop = MagicMock()
    stop.is_set.side_effect = [False] * ticks + [True]
    client = MagicMock()

    class _Chaos:
        def get_active_channels(self):
            return [1]

    mod.run(client, stop, scenario_data={
        "namespace": "stormns",
        "channel_registry": {1: {"generator_impact": {
            "azure_activity_otel": {"change_storm_high_mutation_rate": 50.0},
        }}},
    }, chaos_controller=_Chaos())

    per_caller: dict[str, int] = {}
    for call in client.send_logs_scoped.call_args_list:
        for record in call.args[2]:
            attrs = _attrs(record)
            op = attrs.get("azure.operation.name", "")
            if (attrs.get("azure.category") == "Administrative"
                    and any(k in op for k in ("WRITE", "DELETE", "ACTION"))
                    and attrs.get("azure.result.type") not in ("Start", "Started")):
                caller = attrs.get("user.id")
                per_caller[caller] = per_caller.get(caller, 0) + 1
    assert max(per_caller.values()) >= 50


def test_administrative_records_carry_azure_duration_healthy_under_slo():
    """SLO "Administrative operation latency" averages azure.duration (< 3000 ms)
    over Administrative records excluding Start/Started."""
    rng = random.Random(11)
    principals = ["admin@demo.onmicrosoft.com", "ci-service@demo.onmicrosoft.com"]
    records = _generate_records(
        principals, "sub-1", "demo-rg", "eastus", rng, count=200, error_rate=0.0,
    )
    durations = []
    for record in records:
        attrs = _attrs(record)
        if attrs.get("azure.category") != "Administrative":
            continue
        assert int(attrs["azure.duration"]) == int(attrs["azure.operation.duration"])
        if attrs["azure.result.type"] not in ("Start", "Started"):
            durations.append(int(attrs["azure.duration"]))
    assert durations
    assert sum(durations) / len(durations) < 3000
