"""IBM MQ impact_keys coverage tests."""

from __future__ import annotations

from generators.ibmmq_otel import SPEC

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.ibmmq_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['cpu_load_high', 'failed_connection_attempts', 'failed_message_operations', 'fdc_files_increasing', 'high_log_write_latency', 'high_transaction_rollback_ratio', 'log_filesystem_critically_low', 'messages_expiring_before_consumption', 'queue_manager_filesystem_low']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


# --- per-alert behaviour: each key alone breaches only its own alert -----------------
# The helpers evaluate the package's alert ES|QL (v0.3.1) over a window of emit cycles:
# RATE/DELTA > 0 is "the counter moved in the window", AVG_OVER_TIME / LAST_OVER_TIME
# follow the gauge.

import random  # noqa: E402

import pytest  # noqa: E402

from generators.ibmmq_otel.generator import _IBMMQCounters, emit_cycle  # noqa: E402

DEFAULTS = {key: meta["default"] for key, meta in SPEC.impact_keys.items()}
SEEDS = range(20)
WINDOW = 12


def _value(metric):
    dp = (metric.get("gauge") or metric["sum"])["dataPoints"][0]
    return float(dp["asInt"] if "asInt" in dp else dp["asDouble"])


def _simulate(chaos_by_cycle, seed):
    """Return {metric name: [value per cycle]} for one run."""
    rng = random.Random(seed)
    counters = _IBMMQCounters(rng)
    fdc = 0
    series: dict[str, list[float]] = {}
    for chaos in chaos_by_cycle:
        metrics, fdc = emit_cycle(counters, fdc, rng, chaos, 1.0, "QM1")
        for metric in metrics:
            series.setdefault(metric["name"], []).append(_value(metric))
    return series


def _moved(series, *names):
    return sum(series[n][-1] - series[n][0] for n in names) > 0


def _avg(values):
    return sum(values) / len(values)


def _breached(series):
    """Alert key -> breached, for the last WINDOW cycles of the run."""
    s = {name: values[-WINDOW:] for name, values in series.items()}
    commit = s["ibmmq_qmgr_commit_total"][-1] - s["ibmmq_qmgr_commit_total"][0]
    rollback = s["ibmmq_qmgr_rollback_total"][-1] - s["ibmmq_qmgr_rollback_total"][0]
    return {
        "cpu_load_high": _avg(s["ibmmq_qmgr_cpu_load_five_minute_average_percentage"]) > 85,
        "messages_expiring_before_consumption": _moved(s, "ibmmq_qmgr_expired_message_total"),
        "failed_connection_attempts": _moved(
            s, "ibmmq_qmgr_failed_mqconn_mqconnx_total", "ibmmq_qmgr_failed_mqopen_total"),
        "failed_message_operations": _moved(
            s, "ibmmq_qmgr_failed_mqput_total", "ibmmq_qmgr_failed_mqput1_total",
            "ibmmq_qmgr_failed_mqget_total"),
        "fdc_files_increasing": _moved(s, "ibmmq_qmgr_fdc_files"),
        "high_log_write_latency": _avg(s["ibmmq_qmgr_log_write_latency_seconds"]) > 0.005,
        "high_transaction_rollback_ratio": commit > 0 and rollback / (commit + rollback) * 100 > 5,
        "log_filesystem_critically_low": s["ibmmq_qmgr_log_file_system_free_space_percentage"][-1] < 10,
        "queue_manager_filesystem_low": s["ibmmq_qmgr_queue_manager_file_system_free_space_percentage"][-1] < 20,
    }


def _run(overrides, seed, cycles=WINDOW + 4):
    chaos = {**DEFAULTS, **overrides}
    return _breached(_simulate([chaos] * cycles, seed))


# GT channel values, with the queue-manager filesystem probe below its 20% floor.
FAULTS = {
    "cpu_load_high": 637.0,
    "messages_expiring_before_consumption": 50.0,
    "failed_connection_attempts": 50.0,
    "failed_message_operations": 50.0,
    "fdc_files_increasing": 50.0,
    "high_log_write_latency": 75.0,
    "high_transaction_rollback_ratio": 50.0,
    "log_filesystem_critically_low": 5.0,
    "queue_manager_filesystem_low": 10.0,
}


def test_fault_table_covers_every_impact_key():
    assert sorted(FAULTS) == sorted(SPEC.impact_keys)


@pytest.mark.parametrize("key", sorted(FAULTS))
def test_key_alone_breaches_only_its_own_alert(key):
    for seed in SEEDS:
        breached = {k for k, hit in _run({key: FAULTS[key]}, seed).items() if hit}
        assert breached == {key}, f"seed {seed}: {key} fired {sorted(breached)}"


def test_healthy_run_breaches_nothing():
    for seed in SEEDS:
        breached = {k for k, hit in _run({}, seed).items() if hit}
        assert not breached, f"seed {seed}: healthy run fired {sorted(breached)}"


def test_chaos_none_breaches_nothing():
    series = _simulate([None] * (WINDOW + 4), 1)
    assert not any(_breached(series).values())


@pytest.mark.parametrize("key", sorted(FAULTS))
def test_alert_clears_after_the_fault_resolves(key):
    faulted = [{**DEFAULTS, key: FAULTS[key]}] * 6
    healthy = [{**DEFAULTS}] * (WINDOW + 4)
    for seed in SEEDS:
        breached = {k for k, hit in _breached(_simulate(faulted + healthy, seed)).items() if hit}
        assert not breached, f"seed {seed}: {sorted(breached)} still firing after {key} resolved"


@pytest.mark.parametrize("key,breaching,healthy", [
    ("log_filesystem_critically_low", 9.0, 50.0),
    ("queue_manager_filesystem_low", 19.0, 50.0),
    ("high_log_write_latency", 6.0, 0.0),
    ("high_transaction_rollback_ratio", 6.0, 0.0),
    ("failed_connection_attempts", 1.0, 0.0),
    ("failed_message_operations", 1.0, 0.0),
])
def test_key_units_match_the_documented_thresholds(key, breaching, healthy):
    for seed in SEEDS:
        assert _run({key: breaching}, seed)[key], f"seed {seed}: {key}={breaching} did not breach"
        assert not _run({key: healthy}, seed)[key], f"seed {seed}: {key}={healthy} breached"
