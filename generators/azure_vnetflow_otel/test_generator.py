"""Azure VNet flow-specific tests for impact_keys + chaos apply."""

from __future__ import annotations

import random

from generator_engine.identity import ScenarioIdentity
from generators.azure_vnetflow_otel import SPEC
from generators.azure_vnetflow_otel.generator import _generate_flow_record, _network_ids

_NET = _network_ids(ScenarioIdentity("test"))


def _attr_map(record: dict) -> dict:
    out = {}
    for a in record["attributes"]:
        val = next(iter(a["value"].values()))
        out[a["key"]] = int(val) if isinstance(val, str) and val.isdigit() else val
    return out


def _total_bytes(record: dict) -> int:
    attrs = _attr_map(record)
    return attrs["azure.vnet.flow.bytes_sent"] + attrs["azure.vnet.flow.bytes_received"]


def test_healthy_baseline_no_chaos():
    rng = random.Random(1)
    records = [_generate_flow_record(rng, net=_NET) for _ in range(200)]
    denies = sum(1 for r in records if _attr_map(r)["azure.vnet.flow.action"] == "Deny")
    assert 0.05 <= denies / len(records) <= 0.45


def test_deny_rate_chaos_increases_denies():
    rng = random.Random(1)
    records = [
        _generate_flow_record(rng, net=_NET, deny_rate=0.9, bytes_multiplier=1.0)
        for _ in range(200)
    ]
    denies = sum(1 for r in records if _attr_map(r)["azure.vnet.flow.action"] == "Deny")
    assert denies / len(records) >= 0.75


def test_denied_flows_have_state_d_and_no_counters():
    rng = random.Random(3)
    denied = [
        _attr_map(r)
        for r in (_generate_flow_record(rng, net=_NET, deny_rate=1.0) for _ in range(50))
    ]
    assert all(a["azure.vnet.flow.state"] == "D" for a in denied)
    assert all(a["azure.vnet.flow.bytes_sent"] == 0 and a["azure.vnet.flow.packets_received"] == 0 for a in denied)
    assert all(a["azure.vnet.flow.rule"].startswith(("DefaultRule_Deny", "UserRule_deny")) for a in denied)


def test_bytes_multiplier_inflates_flow_bytes():
    rng = random.Random(1)
    baseline = [_generate_flow_record(rng, net=_NET, deny_rate=0.0, bytes_multiplier=1.0) for _ in range(50)]
    rng = random.Random(1)
    inflated = [_generate_flow_record(rng, net=_NET, deny_rate=0.0, bytes_multiplier=5.0) for _ in range(50)]
    assert sum(map(_total_bytes, inflated)) > sum(map(_total_bytes, baseline)) * 3


def test_ids_come_from_scenario_identity():
    sub = ScenarioIdentity("test").azure_subscription_id
    assert _NET["nsg"].startswith(f"/subscriptions/{sub}/")
    assert all(v.startswith(f"/subscriptions/{sub}/") for v in _NET["vnets"])
    other = _network_ids(ScenarioIdentity("other"))
    assert other["nsg"] != _NET["nsg"]


def test_spec_wants_chaos_and_impact_keys():
    assert SPEC.wants_chaos is True
    assert SPEC.integrations == ()
    assert sorted(SPEC.impact_keys) == ["bytes_multiplier", "deny_rate"]
