"""GCP VPC flow-specific tests for impact_keys + chaos apply."""

from __future__ import annotations

import random

from generators.gcp_vpcflow_otel import SPEC
from generators.gcp_vpcflow_otel.generator import _generate_flow_record


def _attr_map(record: dict) -> dict:
    out = {}
    for a in record["attributes"]:
        val = next(iter(a["value"].values()))
        out[a["key"]] = int(val) if isinstance(val, str) and val.isdigit() else val
    return out


_VPC_NAMES = ["demo-vpc-prod", "demo-vpc-staging", "demo-vpc-data"]


def test_healthy_baseline_no_chaos():
    rng = random.Random(1)
    records = [_generate_flow_record(rng, _VPC_NAMES) for _ in range(50)]
    assert all(_attr_map(r)["gcp.vpc.flow.bytes_sent"] >= 64 for r in records)


def test_bytes_multiplier_inflates_gcp_flow_bytes():
    rng = random.Random(1)
    baseline = [_generate_flow_record(rng, _VPC_NAMES, bytes_multiplier=1.0) for _ in range(50)]
    rng = random.Random(1)
    inflated = [_generate_flow_record(rng, _VPC_NAMES, bytes_multiplier=8.0) for _ in range(50)]
    base_bytes = sum(_attr_map(r)["gcp.vpc.flow.bytes_sent"] for r in baseline)
    chaos_bytes = sum(_attr_map(r)["gcp.vpc.flow.bytes_sent"] for r in inflated)
    assert chaos_bytes > base_bytes * 5


def test_spec_wants_chaos_and_impact_keys():
    assert SPEC.wants_chaos is True
    assert sorted(SPEC.impact_keys) == ["bytes_multiplier", "records_per_cycle"]
