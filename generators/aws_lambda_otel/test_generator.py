"""Lambda-specific tests for the aws_lambda_otel generator."""

from __future__ import annotations

import random
from collections import defaultdict

import pytest

from generators.aws_lambda_otel.generator import (
    METRIC_NAMES,
    _generate_function_metrics,
    LambdaFunctionState,
)


REQUIRED = {
    ("Invocations", "Sum"),
    ("Errors", "Sum"),
    ("Throttles", "Sum"),
    ("AsyncEventsDropped", "Sum"),
    ("Duration", "Average"),
    ("Duration", "Maximum"),
    ("Duration", "Sum"),
    ("ConcurrentExecutions", "Maximum"),
    ("ConcurrentExecutions", "Average"),  # package ML job (performance anomaly)
    ("AsyncEventAge", "Average"),
    ("AsyncEventAge", "Maximum"),
    ("AsyncEventsReceived", "Sum"),
    ("IteratorAge", "Maximum"),
}


def _stat_pairs(metrics: list[dict]) -> set[tuple[str, str]]:
    seen: set[tuple[str, str]] = set()
    for m in metrics:
        name = m["name"].removeprefix("amazonaws.com/AWS/Lambda/")
        for dp in m.get("gauge", {}).get("dataPoints", []):
            attrs = {a["key"]: a["value"].get("stringValue") for a in dp.get("attributes", [])}
            if "stat" in attrs:
                seen.add((name, attrs["stat"]))
    return seen


def test_metric_names_include_async_dropped():
    assert "amazonaws.com/AWS/Lambda/AsyncEventsDropped" in METRIC_NAMES


def test_generate_emits_required_stats_without_resource():
    state = LambdaFunctionState(random.Random(0))
    metrics = _generate_function_metrics(
        "demo-api-handler", state, "us-east-1", "123456789012", random.Random(0),
    )
    seen = _stat_pairs(metrics)
    assert REQUIRED <= seen, f"missing={REQUIRED - seen}"

    for m in metrics:
        assert "gauge" in m, f"expected period gauge for {m.get('name')}"
        assert "sum" not in m, f"unexpected cumulative counter for {m.get('name')}"
        for dp in m["gauge"]["dataPoints"]:
            keys = {a["key"] for a in dp.get("attributes", [])}
            assert "Resource" not in keys, "Resource dimension blanks dashboard panels"


@pytest.fixture(autouse=True, params=[0.15, 0.6, 1.0], ids=lambda v: f"load{v}")
def _business_load(request, monkeypatch):
    """Pin the time-of-day load so the alert tests do not depend on when they run."""
    from generators.aws_lambda_otel import generator as gen

    monkeypatch.setattr(gen, "business_load", lambda now=None: request.param)


# ── package alert thresholds, evaluated on one emitted cycle ──────────────────
# Mirrors aws_lambda_otel@0.12.0 alerting_rule_template ES|QL. SUM() of the long
# gauges is a long, so the ratio alerts integer-divide: errors // invocations.

def _value(dp: dict) -> float:
    return float(dp["asDouble"] if "asDouble" in dp else dp["asInt"])


def _values(metrics: list[dict]) -> dict[tuple[str, str], float]:
    """(metric, stat) -> value. A missing series reads as 0: presence is asserted by REQUIRED."""
    out: dict[tuple[str, str], float] = defaultdict(float)
    for m in metrics:
        name = m["name"].removeprefix("amazonaws.com/AWS/Lambda/")
        dp = m["gauge"]["dataPoints"][0]
        attrs = {a["key"]: a["value"].get("stringValue") for a in dp.get("attributes", [])}
        out[(name, attrs["stat"])] = _value(dp)
    return out


ALERTS = {
    "dead_letter_errors": lambda v: v[("DeadLetterErrors", "Sum")] > 0,
    "destination_delivery_failures": lambda v: v[("DestinationDeliveryFailures", "Sum")] > 0,
    "high_async_event_age": lambda v: v[("AsyncEventAge", "Maximum")] > 60000,
    "high_concurrent_executions": lambda v: v[("ConcurrentExecutions", "Maximum")] > 800,
    "high_average_duration": lambda v: v[("Duration", "Average")] > 3000,
    "high_tail_duration": lambda v: v[("Duration", "Maximum")] > 10000,
    "high_error_rate": lambda v: (
        v[("Invocations", "Sum")] > 10
        and int(v[("Errors", "Sum")]) // int(v[("Invocations", "Sum")]) * 100.0 > 1.0
    ),
    "high_iterator_age": lambda v: v[("IteratorAge", "Maximum")] > 60000,
    "high_throttle_rate": lambda v: (
        v[("Invocations", "Sum")] + v[("Throttles", "Sum")] > 10
        and int(v[("Throttles", "Sum")])
        // int(v[("Invocations", "Sum")] + v[("Throttles", "Sum")]) * 100.0 > 0.5
    ),
}

# GT scenario values (scenarios_testing/gt-awslambda/channels)
GT_VALUES = {
    "dead_letter_errors": 50.0,
    "destination_delivery_failures": 50.0,
    "high_async_event_age": 450000.0,
    "high_concurrent_executions": 6000.0,
    "high_average_duration": 22500.0,
    "high_tail_duration": 75000.0,
    "high_error_rate": 50.0,
    "high_iterator_age": 450000.0,
    "high_throttle_rate": 50.0,
}


def _cycle(chaos: dict | None, seed: int) -> dict[tuple[str, str], float]:
    rng = random.Random(seed)
    state = LambdaFunctionState(rng)
    return _values(
        _generate_function_metrics("demo-fn", state, "us-east-1", "123456789012", rng, chaos)
    )


def _breached(values) -> set[str]:
    return {key for key, fires in ALERTS.items() if fires(values)}


def test_alert_table_covers_every_impact_key():
    assert set(ALERTS) == set(_SPEC_FOR_ALERT_KEYS.impact_keys) == set(GT_VALUES)


def test_healthy_cycle_breaches_no_alert():
    for seed in range(50):
        assert _breached(_cycle(None, seed)) == set(), seed


@pytest.mark.parametrize("key", sorted(GT_VALUES))
def test_fault_alone_breaches_only_its_own_alert(key):
    for seed in range(20):
        assert _breached(_cycle({key: GT_VALUES[key]}, seed)) == {key}, (key, seed)


def test_async_event_age_emits_average_and_maximum():
    """The dashboard reads Average; the alert reads Maximum."""
    values = _cycle({"high_async_event_age": 450000.0}, 1)
    assert values[("AsyncEventAge", "Average")] > 60000
    assert values[("AsyncEventAge", "Maximum")] >= values[("AsyncEventAge", "Average")]
    healthy = _cycle(None, 1)
    assert healthy[("AsyncEventAge", "Maximum")] >= healthy[("AsyncEventAge", "Average")]


def test_duration_maximum_is_never_below_average():
    for chaos in (None, {"high_average_duration": 22500.0}, {"high_tail_duration": 75000.0}):
        for seed in range(20):
            v = _cycle(chaos, seed)
            assert v[("Duration", "Maximum")] >= v[("Duration", "Average")], (chaos, seed)

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.aws_lambda_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['dead_letter_errors', 'destination_delivery_failures', 'high_async_event_age', 'high_average_duration', 'high_concurrent_executions', 'high_error_rate', 'high_iterator_age', 'high_tail_duration', 'high_throttle_rate']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
