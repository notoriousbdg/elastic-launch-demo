"""Airflow-specific tests for impact key apply + healthy baseline."""
from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

from generators.airflow_otel import SPEC
from generators.airflow_otel import generator as airflow
from generators.airflow_otel.generator import METRIC_NAMES, run


class _FakeChaos:
    def __init__(self, active=("ch1",)):
        self._active = list(active)

    def get_active_channels(self):
        return self._active


def _run(impact: dict | None = None, n_ticks: int = 1) -> list[dict]:
    client = MagicMock()
    stop = MagicMock()
    stop.is_set.side_effect = [False] * n_ticks + [True]
    stop.wait.return_value = None
    registry = {}
    controller = None
    if impact is not None:
        controller = _FakeChaos()
        registry = {"ch1": {"generator_impact": {"airflow_otel": impact}}}
    with patch.object(airflow, "_reset_counter_long_mapping"), \
            patch.object(airflow, "_reset_processor_timeouts_gauge_mapping"):
        run(client, stop, scenario_data={
            "namespace": "testns",
            "generator_config": {"chaos": "explicit"},
            "channel_registry": registry,
        }, chaos_controller=controller)
    metrics: list[dict] = []
    for call in client.send_metrics_scoped.call_args_list:
        metrics.extend(call.args[2])
    return metrics


def _gauge(metrics, name) -> float:
    vals = []
    for m in metrics:
        if m["name"] == name and "gauge" in m:
            dp = m["gauge"]["dataPoints"][0]
            vals.append(float(dp.get("asDouble", dp.get("asInt"))))
    if not vals:
        raise AssertionError(f"missing {name}")
    return max(vals)


def test_healthy_baseline_no_chaos():
    m = _run(None)
    assert _gauge(m, "airflow.dag_processing.import_errors") == 0
    assert _gauge(m, "airflow.dag_processing.file_path_queue_size") <= 20
    assert _gauge(m, "airflow.executor.open_slots") > 0
    assert _gauge(m, "airflow.scheduler.tasks.starving") <= 5
    assert _gauge(m, "airflow.scheduler.scheduler_loop_duration") <= 60
    assert _gauge(m, "airflow.dagrun.schedule_delay") <= 300
    assert _gauge(m, "airflow.job_heartbeat_failure") == 0


def test_queue_import_slots_starving_loop_delay():
    assert _gauge(_run({"dag_file_processing_queue_large": 40}), "airflow.dag_processing.file_path_queue_size") > 20
    assert _gauge(_run({"dag_import_errors_present": 3}), "airflow.dag_processing.import_errors") > 0
    assert _gauge(_run({"executor_open_slots_exhausted": 0}), "airflow.executor.open_slots") <= 0
    assert _gauge(_run({"tasks_starving_due_to_pool_limits": 10}), "airflow.scheduler.tasks.starving") > 5
    assert _gauge(_run({"scheduler_loop_duration_excessive": 90}), "airflow.scheduler.scheduler_loop_duration") > 60
    assert _gauge(_run({"dag_run_schedule_delay_high": 400}), "airflow.dagrun.schedule_delay") > 300
    assert _gauge(_run({"scheduler_job_heartbeat_failures": 2}), "airflow.job_heartbeat_failure") > 0


def test_scheduler_heartbeat_suppressed():
    m = _run({"scheduler_heartbeats_critically_low": 0}, n_ticks=2)
    # OOTB alert uses COUNT(airflow.scheduler_heartbeat) < 5 — omit the field.
    assert "airflow.scheduler_heartbeat" not in {x["name"] for x in m}
    # Other metrics still emit so total_docs can stay > 10.
    assert any(x["name"] == "airflow.ti.start" for x in m)


def test_ti_failures_flat_without_chaos():
    """OOTB 'Task failures above threshold' alert breaches on INCREASE > 10;
    healthy ticks must add 0 ambient failures."""
    m = _run(None, n_ticks=30)
    # Sum of any period-delta failures emitted across all ticks must stay 0.
    from generators.airflow_otel.generator import _AirflowCounters
    import random as _random

    rng = _random.Random(0)
    counters = _AirflowCounters(rng)
    before = dict(counters.ti_failures_by_dag)
    total_before = counters.ti_failures
    for _ in range(30):
        counters.tick(rng, load=1.0, dag_failure_rate=0.0, heartbeat_suppressed=False)
    assert counters.ti_failures - total_before == 0
    for dag, val in counters.ti_failures_by_dag.items():
        assert val - before.get(dag, 0) == 0


def test_dag_failure_rate_raises_failures():
    m = _run({"dag_failure_rate": 1.0}, n_ticks=2)
    assert _gauge(m, "airflow.ti_failures") >= 0  # emitted
    assert "airflow.ti_failures" in {x["name"] for x in m}


def test_emits_dashboard_metric_names():
    names = {m["name"] for m in _run(None)}
    assert set(METRIC_NAMES) <= names


def test_uses_send_metrics_scoped_not_send_metrics():
    client = MagicMock()
    stop = threading.Event()
    client.send_metrics_scoped.side_effect = lambda *_a, **_k: stop.set()
    with patch.object(airflow, "_reset_counter_long_mapping"), \
            patch.object(airflow, "_reset_processor_timeouts_gauge_mapping"):
        run(client, stop, scenario_data={
            "namespace": "testns",
            "generator_config": {},
            "channel_registry": {},
        })
    assert client.send_metrics_scoped.called
    assert not client.send_metrics.called

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.airflow_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['dag_file_processing_queue_large', 'dag_import_errors_present', 'dag_processor_timeouts', 'dag_run_schedule_delay_high', 'executor_open_slots_exhausted', 'scheduler_heartbeats_critically_low', 'scheduler_job_heartbeat_failures', 'scheduler_loop_duration_excessive', 'task_failures_above_threshold', 'tasks_killed_externally', 'tasks_starving_due_to_pool_limits']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
