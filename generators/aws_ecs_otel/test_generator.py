"""ECS-specific tests for the aws_ecs_otel generator."""
from __future__ import annotations

import random

from generators.aws_ecs_otel.generator import (
    METRIC_NAMES,
    ECSServiceState,
    _generate_cluster_metrics,
    _generate_service_metrics,
)


def _attr_map(metric: dict) -> dict:
    dps = metric.get("gauge", {}).get("dataPoints") or metric.get("sum", {}).get("dataPoints") or []
    if not dps:
        return {}
    return {a["key"]: next(iter(a["value"].values())) for a in dps[0].get("attributes", [])}


def _value(metric: dict) -> float:
    dp = (metric.get("gauge", {}) or metric.get("sum", {})).get("dataPoints", [{}])[0]
    if "asDouble" in dp:
        return float(dp["asDouble"])
    return float(dp.get("asInt", 0))


def test_metric_names_match_dashboard() -> None:
    """Dashboard + alerts query AWS/ECS classic metrics, not Container Insights."""
    assert "amazonaws.com/AWS/ECS/CPUUtilization" in METRIC_NAMES
    assert "amazonaws.com/AWS/ECS/MemoryUtilization" in METRIC_NAMES
    assert "amazonaws.com/AWS/ECS/ActiveConnectionCount" in METRIC_NAMES
    assert "amazonaws.com/AWS/ECS/NewConnectionCount" in METRIC_NAMES
    assert "amazonaws.com/AWS/ECS/ProcessedBytes" in METRIC_NAMES
    assert "amazonaws.com/AWS/ECS/RequestCount" in METRIC_NAMES
    assert "amazonaws.com/AWS/ECS/CPUReservation" in METRIC_NAMES
    assert "amazonaws.com/AWS/ECS/MemoryReservation" in METRIC_NAMES
    assert not any("ContainerInsights" in n for n in METRIC_NAMES)


def test_service_metrics_emit_required_stats() -> None:
    rng = random.Random(42)
    state = ECSServiceState(rng, desired_tasks=3)
    metrics = _generate_service_metrics(
        "demo-cluster", "demo-api", state, 3, "us-east-1", "123456789012", rng,
    )

    cpu = [m for m in metrics if m["name"].endswith("CPUUtilization")]
    assert {_attr_map(m)["stat"] for m in cpu} == {"Average", "Maximum"}
    assert all(0 <= _value(m) <= 100 for m in cpu)

    mem = [m for m in metrics if m["name"].endswith("MemoryUtilization")]
    assert {_attr_map(m)["stat"] for m in mem} == {"Average", "Maximum"}

    active = next(m for m in metrics if m["name"].endswith("ActiveConnectionCount"))
    attrs = _attr_map(active)
    assert attrs["stat"] == "Sum"  # Service Detail dashboard filters stat == "Sum"
    assert attrs["Namespace"] == "AWS/ECS"
    assert attrs["MetricName"] == "ActiveConnectionCount"
    assert attrs["ServiceName"] == "demo-api"
    assert attrs["ClusterName"] == "demo-cluster"

    new_conn = next(m for m in metrics if m["name"].endswith("NewConnectionCount"))
    assert _attr_map(new_conn)["stat"] == "Sum"
    processed = next(m for m in metrics if m["name"].endswith("ProcessedBytes"))
    assert _attr_map(processed)["stat"] == "Sum"

    # Service Detail "Request count" panel: stat == "Sum" on RequestCount, with service dims
    requests = next(m for m in metrics if m["name"].endswith("RequestCount"))
    req_attrs = _attr_map(requests)
    assert req_attrs["stat"] == "Sum"
    assert req_attrs["ServiceName"] == "demo-api"
    assert req_attrs["ClusterName"] == "demo-cluster"
    assert _value(requests) > 0


def test_cluster_reservation_metrics() -> None:
    rng = random.Random(7)
    states = {"api": ECSServiceState(rng, 3), "worker": ECSServiceState(rng, 3)}
    metrics = _generate_cluster_metrics(
        "demo-cluster", states, 3, "us-east-1", "123456789012", rng,
    )
    names = {m["name"] for m in metrics}
    assert names == {
        "amazonaws.com/AWS/ECS/CPUReservation",
        "amazonaws.com/AWS/ECS/MemoryReservation",
    }
    for m in metrics:
        attrs = _attr_map(m)
        assert attrs["stat"] == "Average"
        assert "ServiceName" not in attrs
        assert attrs["ClusterName"] == "demo-cluster"
        assert attrs["Namespace"] == "AWS/ECS"

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.aws_ecs_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['cpu_reservation_high', 'cpu_utilization_high', 'memory_reservation_high', 'memory_utilization_high']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
