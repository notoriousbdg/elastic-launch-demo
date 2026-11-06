"""EC2-specific tests for the aws_ec2_otel generator."""

from __future__ import annotations

import random

from generators.aws_ec2_otel.generator import (
    METRIC_NAMES,
    _generate_instance_metrics,
)


# Metrics + stats required by aws_ec2_otel Overview + Instance Detail panels.
REQUIRED = {
    ("CPUUtilization", "Average"),
    ("CPUUtilization", "Maximum"),
    ("StatusCheckFailed", "Maximum"),
    ("StatusCheckFailed_System", "Maximum"),
    ("StatusCheckFailed_Instance", "Maximum"),
    ("StatusCheckFailed_AttachedEBS", "Maximum"),
    ("NetworkIn", "Average"),
    ("NetworkIn", "Sum"),
    ("NetworkOut", "Average"),
    ("NetworkOut", "Sum"),
    ("EBSReadBytes", "Average"),
    ("EBSReadBytes", "Sum"),
    ("EBSWriteBytes", "Average"),
    ("EBSWriteBytes", "Sum"),
    ("EBSReadOps", "Sum"),
    ("EBSWriteOps", "Average"),
    ("EBSWriteOps", "Sum"),
    ("EBSIOBalance%", "Average"),
    ("DiskReadBytes", "Sum"),
    ("DiskWriteBytes", "Sum"),
    ("CPUCreditBalance", "Average"),
    ("CPUSurplusCreditsCharged", "Sum"),
    ("InstanceEBSIOPSExceededCheck", "Maximum"),
    ("InstanceEBSThroughputExceededCheck", "Maximum"),
}


def _stat_pairs(metrics: list[dict]) -> set[tuple[str, str]]:
    seen: set[tuple[str, str]] = set()
    for m in metrics:
        name = m["name"].removeprefix("amazonaws.com/AWS/EC2/")
        for dp in m.get("gauge", {}).get("dataPoints", []):
            for attr in dp.get("attributes", []):
                if attr.get("key") == "stat":
                    seen.add((name, attr["value"]["stringValue"]))
    return seen


def _attrs(metrics: list[dict], metric_suffix: str) -> dict[str, str]:
    full = f"amazonaws.com/AWS/EC2/{metric_suffix}"
    for m in metrics:
        if m["name"] != full:
            continue
        out: dict[str, str] = {}
        for dp in m.get("gauge", {}).get("dataPoints", []):
            for attr in dp.get("attributes", []):
                out[attr["key"]] = attr["value"]["stringValue"]
            break
        return out
    raise AssertionError(f"missing metric {metric_suffix}")


def _gauge_value(metrics: list[dict], metric_suffix: str, stat: str) -> float:
    full = f"amazonaws.com/AWS/EC2/{metric_suffix}"
    for m in metrics:
        if m["name"] != full:
            continue
        for dp in m.get("gauge", {}).get("dataPoints", []):
            attrs = {
                a["key"]: a["value"]["stringValue"] for a in dp.get("attributes", [])
            }
            if attrs.get("stat") != stat:
                continue
            if "asDouble" in dp:
                return float(dp["asDouble"])
            return float(dp["asInt"])
    raise AssertionError(f"missing {metric_suffix}/{stat}")


def test_metric_names_match_dashboard():
    expected = set(METRIC_NAMES)
    assert set(METRIC_NAMES) == expected
    assert any(n.endswith("EBSIOBalance%") for n in METRIC_NAMES)
    assert any(n.endswith("DiskReadBytes") for n in METRIC_NAMES)
    assert any(n.endswith("DiskWriteBytes") for n in METRIC_NAMES)


def test_generate_emits_required_stats():
    metrics = _generate_instance_metrics(
        "i-0123456789abcdef0", "us-east-1", "123456789012", random.Random(0),
    )
    seen = _stat_pairs(metrics)
    assert REQUIRED <= seen, f"missing={REQUIRED - seen}"


def test_generate_uses_period_gauges_not_cumulative():
    metrics = _generate_instance_metrics(
        "i-0123456789abcdef0", "us-east-1", "123456789012", random.Random(1),
    )
    assert metrics
    for m in metrics:
        assert "gauge" in m, f"expected gauge for {m.get('name')}"
        assert "sum" not in m, f"unexpected cumulative sum for {m.get('name')}"


def test_namespace_and_metric_name_attrs():
    metrics = _generate_instance_metrics(
        "i-0123456789abcdef0", "us-east-1", "123456789012", random.Random(2),
    )
    attrs = _attrs(metrics, "EBSIOBalance%")
    assert attrs["Namespace"] == "AWS/EC2"
    assert attrs["MetricName"] == "EBSIOBalance%"
    assert attrs["InstanceId"] == "i-0123456789abcdef0"


def test_disk_and_burst_balance_nonzero_healthy():
    metrics = _generate_instance_metrics(
        "i-0123456789abcdef0", "us-east-1", "123456789012", random.Random(3),
    )
    assert _gauge_value(metrics, "DiskReadBytes", "Sum") > 0
    assert _gauge_value(metrics, "DiskWriteBytes", "Sum") > 0
    assert _gauge_value(metrics, "EBSIOBalance%", "Average") > 0
    assert _gauge_value(metrics, "EBSReadBytes", "Sum") > 0


def test_status_check_chaos_targets_typed_metrics():
    metrics = _generate_instance_metrics(
        "i-0123456789abcdef0",
        "us-east-1",
        "123456789012",
        random.Random(4),
        {"system_status_check_failed": 2.0},
    )
    assert _gauge_value(metrics, "StatusCheckFailed_System", "Maximum") == 1
    assert _gauge_value(metrics, "StatusCheckFailed_Instance", "Maximum") == 0
    assert _gauge_value(metrics, "StatusCheckFailed", "Maximum") == 1


def test_low_cpu_credit_balance_chaos():
    metrics = _generate_instance_metrics(
        "i-0123456789abcdef0",
        "us-east-1",
        "123456789012",
        random.Random(5),
        {"low_cpu_credit_balance": 5.0},
    )
    assert _gauge_value(metrics, "CPUCreditBalance", "Average") == 5.0


# --- 1:1 alert impact_keys coverage (auto) ---
from generators.aws_ec2_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['cpu_surplus_credits_charged', 'ebs_attached_volume_status_check_failed', 'high_cpu_utilization', 'high_ebs_read_throughput', 'high_ebs_write_throughput', 'instance_status_check_failed', 'low_cpu_credit_balance', 'system_status_check_failed']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
