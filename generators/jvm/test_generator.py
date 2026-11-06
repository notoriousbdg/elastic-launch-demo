"""Technology-specific tests for the ``jvm`` generator."""
from __future__ import annotations

import random

from generators.jvm import SPEC
from generators.jvm.generator import JvmState, _generate_metrics


def _gauge(metrics, name, attr_key=None, attr_val=None) -> float:
    for m in metrics:
        if m["name"] != name:
            continue
        dp = m["gauge"]["dataPoints"][0]
        if attr_key is not None:
            attrs = {
                a["key"]: next(iter(a["value"].values()))
                for a in dp.get("attributes", [])
            }
            if attrs.get(attr_key) != attr_val:
                continue
        return float(dp.get("asDouble", dp.get("asInt")))
    raise AssertionError(f"missing {name} {attr_key}={attr_val}")


def _histogram_sum(metrics, gc_name: str) -> float:
    for m in metrics:
        if m["name"] != "jvm.gc.duration":
            continue
        dp = m["histogram"]["dataPoints"][0]
        attrs = {
            a["key"]: next(iter(a["value"].values()))
            for a in dp.get("attributes", [])
        }
        if attrs.get("jvm.gc.name") == gc_name:
            return float(dp["sum"])
    raise AssertionError(f"missing gc histogram for {gc_name}")


def test_healthy_baseline_no_chaos():
    rng = random.Random(1)
    state = JvmState(rng)
    metrics = _generate_metrics(state, rng, None)
    cpu = _gauge(metrics, "jvm.cpu.recent_utilization")
    old_used = _gauge(metrics, "jvm.memory.used", "jvm.memory.pool.name", "G1 Old Gen")
    old_limit = _gauge(metrics, "jvm.memory.limit", "jvm.memory.pool.name", "G1 Old Gen")
    assert cpu < 0.6
    assert old_used / old_limit < 0.75


def test_heap_pressure_scales_used_toward_limit():
    rng = random.Random(1)
    state = JvmState(rng)
    baseline = _gauge(
        _generate_metrics(state, rng, None),
        "jvm.memory.used",
        "jvm.memory.pool.name",
        "G1 Old Gen",
    )
    state = JvmState(random.Random(1))
    pressured = _gauge(
        _generate_metrics(state, rng, {"channel_id": 1, "heap_pressure": 0.95}),
        "jvm.memory.used",
        "jvm.memory.pool.name",
        "G1 Old Gen",
    )
    assert pressured > baseline * 1.3


def test_gc_pause_multiplier_inflates_old_gc_sum():
    rng = random.Random(2)
    state = JvmState(rng)
    baseline = _histogram_sum(_generate_metrics(state, rng, None), "G1 Old Generation")
    state = JvmState(random.Random(2))
    inflated = _histogram_sum(
        _generate_metrics(state, rng, {"channel_id": 1, "gc_pause_multiplier": 8.0}),
        "G1 Old Generation",
    )
    assert inflated > baseline * 2


def test_impact_keys_declared():
    assert sorted(SPEC.impact_keys) == ["gc_pause_multiplier", "heap_pressure"]
