"""SQS-specific tests for the aws_sqs_otel generator."""

import random

from generators.aws_sqs_otel.generator import SQSQueueState, _generate_queue_metrics


def _attr_map(metric: dict) -> dict:
    dps = metric.get("gauge", {}).get("dataPoints") or metric.get("sum", {}).get("dataPoints") or []
    if not dps:
        return {}
    return {a["key"]: next(iter(a["value"].values())) for a in dps[0].get("attributes", [])}


def test_sqs_message_counts_are_gauges_not_counters():
    """Dashboard sum_over_time rejects counter_long; CloudWatch Sum is a period delta gauge."""
    rng = random.Random(1)
    state = SQSQueueState(rng)
    metrics = _generate_queue_metrics(
        "demo-orders", state, "us-east-1", "123456789012", rng,
    )
    by_name = {m["name"]: m for m in metrics}
    for name in (
        "amazonaws.com/AWS/SQS/NumberOfMessagesSent",
        "amazonaws.com/AWS/SQS/NumberOfMessagesReceived",
        "amazonaws.com/AWS/SQS/NumberOfMessagesDeleted",
        "amazonaws.com/AWS/SQS/NumberOfEmptyReceives",
    ):
        m = by_name[name]
        assert "gauge" in m, f"{name} must be a gauge, got {m.keys()}"
        assert "sum" not in m
        assert _attr_map(m).get("stat") == "Sum"


def test_sqs_depth_metrics_use_maximum_stat():
    rng = random.Random(2)
    state = SQSQueueState(rng)
    metrics = _generate_queue_metrics(
        "demo-orders", state, "us-east-1", "123456789012", rng,
    )
    stats: dict[str, set] = {}
    for m in metrics:
        stats.setdefault(m["name"], set()).add(_attr_map(m).get("stat"))
    for name in (
        "amazonaws.com/AWS/SQS/ApproximateNumberOfMessagesVisible",
        "amazonaws.com/AWS/SQS/ApproximateNumberOfMessagesNotVisible",
        "amazonaws.com/AWS/SQS/ApproximateAgeOfOldestMessage",
    ):
        # Alerts read Maximum; the package ML job (queue backlog anomaly) reads Average.
        assert {"Maximum", "Average"} <= stats[name], (name, stats[name])

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.aws_sqs_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['dlq_has_messages', 'high_backlog', 'in_flight_saturation', 'oldest_message_age_high', 'producer_send_rate_drop']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
