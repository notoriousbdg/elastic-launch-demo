"""AWS VPC flow-specific tests for impact_keys + chaos apply."""

from __future__ import annotations

import random

from generator_engine.identity import ScenarioIdentity
from generators.aws_vpcflow_otel import SPEC
from generators.aws_vpcflow_otel.generator import _generate_flow_record

_ENI_IDS = [ScenarioIdentity("test").aws_id("eni", "flow", i) for i in range(1, 9)]


def _attr_map(record: dict) -> dict:
    out = {}
    for a in record["attributes"]:
        val = next(iter(a["value"].values()))
        out[a["key"]] = int(val) if isinstance(val, str) and val.isdigit() else val
    return out


def test_healthy_baseline_no_chaos():
    rng = random.Random(1)
    records = [_generate_flow_record(rng, eni_ids=_ENI_IDS) for _ in range(200)]
    rejects = sum(1 for r in records if _attr_map(r)["aws.vpc.flow.action"] == "REJECT")
    assert 0.05 <= rejects / len(records) <= 0.45


def test_reject_rate_chaos_increases_rejects():
    rng = random.Random(1)
    records = [
        _generate_flow_record(rng, eni_ids=_ENI_IDS, reject_rate=0.9, bytes_multiplier=1.0)
        for _ in range(200)
    ]
    rejects = sum(1 for r in records if _attr_map(r)["aws.vpc.flow.action"] == "REJECT")
    assert rejects / len(records) >= 0.75


def test_bytes_multiplier_inflates_flow_bytes():
    rng = random.Random(1)
    baseline = [_generate_flow_record(rng, eni_ids=_ENI_IDS, reject_rate=0.2, bytes_multiplier=1.0) for _ in range(50)]
    rng = random.Random(1)
    inflated = [_generate_flow_record(rng, eni_ids=_ENI_IDS, reject_rate=0.2, bytes_multiplier=5.0) for _ in range(50)]
    base_bytes = sum(_attr_map(r)["aws.vpc.flow.bytes"] for r in baseline)
    chaos_bytes = sum(_attr_map(r)["aws.vpc.flow.bytes"] for r in inflated)
    assert chaos_bytes > base_bytes * 3


def test_spec_wants_chaos_and_impact_keys():
    assert SPEC.wants_chaos is True
    assert sorted(SPEC.impact_keys) == ["bytes_multiplier", "reject_rate"]
