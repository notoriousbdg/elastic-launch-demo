"""Memcached impact_keys and metric semantics tests."""

from __future__ import annotations

from generators.memcached_otel import SPEC
from generators.memcached_otel.generator import METRIC_NAMES, miss_ratio_percent


def test_dashboard_required_metrics_are_declared():
    required = {
        "memcached.bytes",
        "memcached.commands",
        "memcached.connections.current",
        "memcached.cpu.usage",
        "memcached.current_items",
        "memcached.evictions",
        "memcached.network",
        "memcached.operation_hit_ratio",
        "memcached.operations",
    }
    assert required.issubset(METRIC_NAMES)


def test_operation_hit_ratio_emits_miss_percent():
    assert miss_ratio_percent(90, 10) == 10.0
    assert miss_ratio_percent(0, 0) == 0.0
    assert miss_ratio_percent(50, 50) == 50.0

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.memcached_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['high_connection_count', 'high_cpu_usage', 'high_eviction_rate', 'low_command_throughput', 'low_hit_ratio_for_get_operations']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


# --- per-alert behaviour: each key alone breaches only its own alert ------------------
# Drives run() for several cycles (fixed 20 s emit interval) and evaluates the package
# alert ES|QL (v0.3.0) over the last WINDOW cycles.

import random  # noqa: E402
from unittest.mock import MagicMock, patch  # noqa: E402

import pytest  # noqa: E402

from generators.memcached_otel import generator as mod  # noqa: E402

DEFAULTS = {key: meta["default"] for key, meta in _SPEC_FOR_ALERT_KEYS.impact_keys.items()}
SEEDS = range(8)
WINDOW = 6
INTERVAL_S = 20.0


class _SeqChaos:
    """Channel ch1 is active for the first ``faulted`` cycles, then resolves."""

    def __init__(self, faulted):
        self._faulted = faulted
        self._calls = 0

    def get_active_channels(self):
        self._calls += 1
        return ["ch1"] if self._calls <= self._faulted else []


def _simulate(impact, seed, faulted, cycles):
    client = MagicMock()
    stop = MagicMock()
    stop.is_set.side_effect = [False] * cycles + [True]
    stop.wait.return_value = None
    real_random = random.Random
    with patch.object(mod, "BATCH_INTERVAL_MIN", INTERVAL_S), \
            patch.object(mod, "BATCH_INTERVAL_MAX", INTERVAL_S), \
            patch.object(mod.random, "Random", lambda *a: real_random(seed)):
        mod.run(
            client, stop,
            scenario_data={
                "namespace": "testns",
                "generator_config": {"chaos": "explicit"},
                "channel_registry": {"ch1": {"generator_impact": {"memcached_otel": impact}}},
            },
            chaos_controller=_SeqChaos(faulted),
        )
    series: dict[tuple, list[float]] = {}
    for call in client.send_metrics_scoped.call_args_list:
        for m in call.args[2]:
            dp = (m.get("gauge") or m["sum"])["dataPoints"][0]
            value = float(dp["asInt"] if "asInt" in dp else dp["asDouble"])
            attrs = tuple(sorted(
                (a["key"], next(iter(a["value"].values()))) for a in dp.get("attributes", [])
            ))
            series.setdefault((m["name"], attrs), []).append(value)
    return {k: v[-WINDOW:] for k, v in series.items()}


def _rate(series, metric):
    """SUM(RATE(metric)) in units per second over the window."""
    elapsed = (WINDOW - 1) * INTERVAL_S
    return sum(w[-1] - w[0] for (name, _), w in series.items() if name == metric) / elapsed


def _breached(series):
    conns = series[("memcached.connections.current", ())]
    hit = series[("memcached.operation_hit_ratio", (("operation", "get"),))]
    return {
        "high_connection_count": conns[-1] > 800,
        "high_cpu_usage": _rate(series, "memcached.cpu.usage") > 0.8,
        "high_eviction_rate": _rate(series, "memcached.evictions") > 10.0,
        "low_command_throughput": _rate(series, "memcached.commands") < 1.0,
        "low_hit_ratio_for_get_operations": sum(hit) / len(hit) > 20.0,
    }


# GT channel values (hit ratio probes 50, below the 80% line).
FAULTS = {
    "high_connection_count": 6000.0,
    "high_cpu_usage": 85.0,
    "high_eviction_rate": 75.0,
    "low_command_throughput": 5.0,
    "low_hit_ratio_for_get_operations": 50.0,
}


def _hits(breached):
    return sorted(k for k, hit in breached.items() if hit)


def test_fault_table_covers_every_impact_key():
    assert sorted(FAULTS) == sorted(_SPEC_FOR_ALERT_KEYS.impact_keys)


@pytest.mark.parametrize("key", sorted(FAULTS))
def test_key_alone_breaches_only_its_own_alert(key):
    for seed in SEEDS:
        series = _simulate({**DEFAULTS, key: FAULTS[key]}, seed, faulted=99, cycles=WINDOW + 2)
        assert _hits(_breached(series)) == [key], f"seed {seed}: {key} fired {_hits(_breached(series))}"


def test_healthy_run_breaches_nothing():
    for seed in SEEDS:
        series = _simulate(dict(DEFAULTS), seed, faulted=0, cycles=WINDOW + 2)
        assert not _hits(_breached(series)), f"seed {seed}: {_hits(_breached(series))}"


@pytest.mark.parametrize("key", sorted(FAULTS))
def test_alert_clears_after_the_fault_resolves(key):
    for seed in SEEDS:
        series = _simulate({**DEFAULTS, key: FAULTS[key]}, seed, faulted=6, cycles=6 + WINDOW + 2)
        assert not _hits(_breached(series)), (
            f"seed {seed}: {_hits(_breached(series))} still firing after {key} resolved"
        )


@pytest.mark.parametrize("key,breaching,healthy", [
    ("low_command_throughput", 0.0, 100.0),          # 0, the worst value, must breach
    ("low_hit_ratio_for_get_operations", 0.0, 90.0),  # 0% hit ratio
    ("high_eviction_rate", 11.0, 5.0),                # evictions per second
    ("high_cpu_usage", 90.0, 50.0),                   # percent of one core
    ("high_connection_count", 900.0, 500.0),
])
def test_key_units_match_the_documented_thresholds(key, breaching, healthy):
    for seed in SEEDS:
        hit = _breached(_simulate({**DEFAULTS, key: breaching}, seed, 99, WINDOW + 2))[key]
        miss = _breached(_simulate({**DEFAULTS, key: healthy}, seed, 99, WINDOW + 2))[key]
        assert hit and not miss, f"seed {seed}: {key} breaching={hit} healthy={miss}"
