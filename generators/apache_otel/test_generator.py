"""Apache-specific tests for key apply + healthy baseline."""
from __future__ import annotations

import random

import pytest

from generators.apache_otel import SPEC
from generators.apache_otel.generator import ApacheState, _generate_metrics


def _metric_val(metrics, name, attr_key=None, attr_val=None) -> float:
    for m in metrics:
        if m["name"] != name:
            continue
        kind = "sum" if "sum" in m else "gauge"
        dp = m[kind]["dataPoints"][0]
        if attr_key is not None:
            attrs = {a["key"]: next(iter(a["value"].values())) for a in dp.get("attributes", [])}
            if attrs.get(attr_key) != attr_val:
                continue
        return float(dp.get("asDouble", dp.get("asInt")))
    raise AssertionError(f"missing {name} {attr_key}={attr_val}")


def _gauge(metrics, name, attr_key=None, attr_val=None) -> float:
    return _metric_val(metrics, name, attr_key, attr_val)


def test_healthy_baseline_no_chaos():
    rng = random.Random(1)
    state = ApacheState(rng)
    metrics = _generate_metrics(state, rng, 256, None)
    idle = _metric_val(metrics, "apache.workers", "state", "idle")
    busy = _metric_val(metrics, "apache.workers", "state", "busy")
    assert idle >= 5
    assert busy / max(1, busy + idle) <= 0.85
    assert _gauge(metrics, "apache.load.1") <= 0.8


def test_workers_use_state_attr_and_non_monotonic_sum():
    rng = random.Random(1)
    state = ApacheState(rng)
    metrics = _generate_metrics(state, rng, 256, None)
    workers = [m for m in metrics if m["name"] == "apache.workers"]
    assert workers
    for m in workers:
        assert "sum" in m
        assert m["sum"]["isMonotonic"] is False
        dp = m["sum"]["dataPoints"][0]
        attrs = {a["key"]: next(iter(a["value"].values())) for a in dp["attributes"]}
        assert "state" in attrs
        assert "workers.state" not in attrs


def test_scoreboard_uses_receiver_state_enum():
    rng = random.Random(1)
    state = ApacheState(rng)
    metrics = _generate_metrics(state, rng, 256, None)
    scoreboard = [m for m in metrics if m["name"] == "apache.scoreboard"]
    assert scoreboard
    states = set()
    for m in scoreboard:
        assert "sum" in m
        assert m["sum"]["isMonotonic"] is False
        dp = m["sum"]["dataPoints"][0]
        attrs = {a["key"]: next(iter(a["value"].values())) for a in dp["attributes"]}
        assert "state" in attrs
        assert "scoreboard.state" not in attrs
        states.add(attrs["state"])
    assert "waiting" in states
    assert "busy" not in states
    assert "idle" not in states


def test_worker_saturation_breaches_idle_and_busy_alerts():
    rng = random.Random(1)
    state = ApacheState(rng)
    metrics = _generate_metrics(state, rng, 256, {"high_busy_workers_ratio": 0.95})
    idle = _metric_val(metrics, "apache.workers", "state", "idle")
    busy = _metric_val(metrics, "apache.workers", "state", "busy")
    assert idle < 5
    assert busy / (busy + idle) > 0.85


def test_low_idle_threshold_value_breaches_alert():
    """GT channels set low_idle_workers=5.0 (alert threshold); emit must be < 5."""
    rng = random.Random(1)
    state = ApacheState(rng)
    metrics = _generate_metrics(state, rng, 256, {"low_idle_workers": 5.0})
    idle = _metric_val(metrics, "apache.workers", "state", "idle")
    assert idle < 5
    assert idle <= 4


def test_server_load_breaches_load_alerts():
    rng = random.Random(1)
    state = ApacheState(rng)
    metrics = _generate_metrics(state, rng, 256, {"high_server_load": 6.0})
    assert _gauge(metrics, "apache.load.1") > 5.0


@pytest.mark.parametrize(
    "chaos", [{"worker_pool_exhaustion": 1.0}, {"high_busy_workers_ratio": 0.9}]
)
def test_worker_saturation_keys_exhaust_the_pool(chaos):
    rng = random.Random(1)
    state = ApacheState(rng)
    metrics = _generate_metrics(state, rng, 256, chaos)
    busy = _metric_val(metrics, "apache.workers", "state", "busy")
    idle = _metric_val(metrics, "apache.workers", "state", "idle")
    assert busy / (busy + idle) > 0.85


def test_restart_detected_keeps_uptime_low():
    rng = random.Random(1)
    state = ApacheState(rng)
    state.uptime = 10_000
    state.tick(rng, {"server_restart_detected": 100})
    assert state.uptime < 300
    metrics = _generate_metrics(state, rng, 256, {"server_restart_detected": 100})
    uptime_metrics = [m for m in metrics if m["name"] == "apache.uptime"]
    assert uptime_metrics
    assert "gauge" in uptime_metrics[0]
    assert "sum" not in uptime_metrics[0]


def test_request_rate_zero_suppresses_requests():
    rng = random.Random(1)
    state = ApacheState(rng)
    before = state.requests
    state.tick(rng, {"no_requests_received": 1.0})
    assert state.requests == before


def test_high_request_rate_gt_value_breaches():
    """GT sets high_request_rate: 85.0 — any positive key must tip hard (counter delta)."""
    rng = random.Random(1)
    state = ApacheState(rng)
    before = state.requests
    state.tick(rng, {"high_request_rate": 85.0})
    assert state.requests - before >= 3000
    metrics = _generate_metrics(state, rng, 256, {"high_request_rate": 85.0})
    req = [m for m in metrics if m["name"] == "apache.requests"]
    assert req and "sum" in req[0] and "gauge" not in req[0]
    assert req[0]["sum"]["isMonotonic"] is True


def test_no_requests_emits_counter_for_rate_alert():
    rng = random.Random(1)
    state = ApacheState(rng)
    metrics = _generate_metrics(state, rng, 256, {"no_requests_received": 1.0})
    req = [m for m in metrics if m["name"] == "apache.requests"]
    assert req and "sum" in req[0]
    assert req[0]["sum"]["isMonotonic"] is True


def test_healthy_requests_emitted_as_monotonic_counter():
    """The overview dashboard uses RATE(apache.requests), so it is always a counter."""
    rng = random.Random(1)
    state = ApacheState(rng)
    metrics = _generate_metrics(state, rng, 256, None)
    req = [m for m in metrics if m["name"] == "apache.requests"]
    assert req and "sum" in req[0] and "gauge" not in req[0]
    assert req[0]["sum"]["isMonotonic"] is True


def test_counters_start_near_zero_for_rate_charts():
    rng = random.Random(42)
    state = ApacheState(rng)
    assert state.requests < 10_000
    assert state.bytes_sent < 10_000_000


# --- 1:1 alert impact_keys coverage (auto) ---
from generators.apache_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['high_busy_workers_ratio', 'high_request_rate', 'high_server_load', 'high_server_load_2', 'low_idle_workers', 'no_requests_received', 'server_restart_detected', 'worker_pool_exhaustion']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
