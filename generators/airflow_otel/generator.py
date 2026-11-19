#!/usr/bin/env python3
"""Apache Airflow Generator — sends Airflow StatsD-style metrics via OTLP.

Emits data matching the airflow_otel content package
(discovery.datasets: airflow.otel). Metric names match the bundled
airflow_otel dashboard ES|QL queries (StatsD prefix ``airflow``).

Dashboard panels call ``SUM`` on StatsD counter names (ti_failures,
ti_successes, scheduler_heartbeat, …). Those aggregations reject
``counter_long``, so counter-shaped series are emitted as period-delta
gauges — same pattern as traefik_otel / influxdb_otel. Sticky
``counter_long`` mappings from earlier cumulative sums are cleared on
startup by deleting the data stream when needed.

Authority:
  - elastic/integrations/packages/airflow_otel/kibana/dashboard/*.json
  - elastic/integrations/packages/airflow_otel/manifest.yml
  - open-telemetry/opentelemetry-collector-contrib/receiver/statsdreceiver

Usage (standalone):
    python3 -m generators.airflow_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/airflow_otel@0.3.0"

import logging
import os
import random
import signal
import threading

from app.telemetry import OTLPClient
from app.config import NAMESPACE
try:
    from generators.airflow_otel import SPEC
except Exception:  # pragma: no cover
    SPEC = None

from generator_engine.db_common import (
    CounterState,
    build_cumulative_sum,
    build_gauge,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

AIRFLOW_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/statsdreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("airflow-generator")
EMIT_INTERVAL_S = 30.0

_METRICS_DATA_STREAM = "metrics-airflow.otel-default"

# Metric names queried by airflow_otel dashboards (overview / tasks /
# scheduler-capacity / dag-processing). Do not invent alternate spellings —
# e.g. dashboards use airflow.scheduler_heartbeat, not airflow.scheduler.heartbeat.
METRIC_NAMES = (
    "airflow.scheduler_heartbeat",
    "airflow.ti_successes",
    "airflow.ti_failures",
    "airflow.ti.start",
    "airflow.ti.finish",
    "airflow.executor.open_slots",
    "airflow.executor.queued_tasks",
    "airflow.executor.running_tasks",
    "airflow.scheduler.tasks.starving",
    "airflow.scheduler.tasks.killed_externally",
    "airflow.scheduler.critical_section_duration",
    "airflow.scheduler.scheduler_loop_duration",
    "airflow.dag_processing.import_errors",
    "airflow.dag_processing.file_path_queue_size",
    "airflow.dag_processing.processor_timeouts",
    "airflow.dag_processing.total_parse_time",
    "airflow.dag_file_refresh_error",
    "airflow.operator_failures",
    # Hardcoded in the Tasks dashboard duration panel (example DAG/task).
    "airflow.dag.metrics_generator.work_task_1.duration",
    "airflow.job_heartbeat_failure",
    "airflow.dagrun.schedule_delay",
)

# Panels call SUM() on these — must map as gauges, not counter_long.
# processor_timeouts is excluded: OOTB alert uses INCREASE() and needs counter.
_SUM_AGG_FIELDS = (
    "airflow.scheduler_heartbeat",
    "airflow.ti_successes",
    "airflow.ti_failures",
    "airflow.ti.start",
    "airflow.ti.finish",
    "airflow.scheduler.tasks.killed_externally",
    "airflow.dag_file_refresh_error",
    "airflow.operator_failures",
)

# OOTB alert INCREASE(airflow.dag_processing.processor_timeouts) needs counter.
_COUNTER_ALERT_FIELDS = (
    "airflow.dag_processing.processor_timeouts",
)

_DAG_NAMES = [
    "etl_pipeline", "data_sync", "ml_training", "report_generation",
    "cleanup_job", "metrics_generator",
]

_OPERATORS = (
    "BashOperator",
    "PythonOperator",
    "KubernetesPodOperator",
    "SqlSensor",
    "EmailOperator",
)


def _default_cloud() -> dict:
    return {"provider": "aws", "platform": "aws_ec2",
            "region": "us-east-1", "availability_zone": "us-east-1a"}


def _period_delta_gauge(
    name: str,
    unit: str,
    value: int,
    attrs: dict | None,
    last_emitted: dict[tuple, int],
) -> dict:
    """Emit the tick-over-tick increase as a gauge (mimics StatsD + SUM)."""
    key = (name, tuple(sorted((attrs or {}).items())))
    prev = last_emitted.get(key)
    if prev is None:
        last_emitted[key] = int(value)
        delta = 0
    else:
        delta = max(0, int(value) - int(prev))
        last_emitted[key] = int(value)
    return build_gauge(name, unit, delta, attrs or {})


def _sum_agg_fields_mapped_as_counter(elastic_url: str, api_key: str) -> bool:
    """Return True if any SUM-agg field is still mapped as a TSDB counter."""
    import httpx

    url = (
        f"{elastic_url.rstrip('/')}/{_METRICS_DATA_STREAM}/_field_caps"
        f"?fields={','.join(_SUM_AGG_FIELDS)}"
    )
    try:
        resp = httpx.get(
            url,
            headers={
                "Authorization": f"ApiKey {api_key}",
                "Content-Type": "application/json",
            },
            timeout=15,
        )
    except Exception as exc:
        logger.warning("Could not inspect %s field caps: %s", _METRICS_DATA_STREAM, exc)
        return False
    if resp.status_code == 404:
        return False
    if resp.status_code >= 300:
        logger.warning(
            "field_caps for %s failed (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )
        return False

    fields = resp.json().get("fields", {})
    for name in _SUM_AGG_FIELDS:
        for meta in fields.get(name, {}).values():
            if meta.get("time_series_metric") == "counter" or meta.get("type") == "counter_long":
                return True
    return False


def _reset_counter_long_mapping() -> None:
    """Delete the metrics data stream if SUM-agg fields are stuck as counters.

    Fresh gauge samples then recreate the stream without ``counter_long``,
    unblocking Overview / Tasks / DAG Processing ``SUM`` panels. No-op when
    ES credentials are absent or mapping is already correct.
    """
    try:
        from app.telemetry import ESBulkClient as _Bulk
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for counter mapping check: %s", exc)
        return

    client = _Bulk()
    if not client.configured:
        return

    if not _sum_agg_fields_mapped_as_counter(client.elastic_url, client.api_key):
        logger.info(
            "airflow SUM-agg fields are not mapped as counters; leaving %s alone",
            _METRICS_DATA_STREAM,
        )
        return

    import httpx

    try:
        resp = httpx.delete(
            f"{client.elastic_url.rstrip('/')}/_data_stream/{_METRICS_DATA_STREAM}",
            headers={
                "Authorization": f"ApiKey {client.api_key}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )
    except Exception as exc:
        logger.warning("Could not delete %s: %s", _METRICS_DATA_STREAM, exc)
        return

    if resp.status_code < 300:
        logger.info(
            "Deleted %s so SUM-agg fields remap as gauges (were counter_long)",
            _METRICS_DATA_STREAM,
        )
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist yet", _METRICS_DATA_STREAM)
    else:
        logger.warning(
            "Could not delete %s (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )


def _counter_alert_fields_mapped_as_gauge(elastic_url: str, api_key: str) -> bool:
    """Return True if INCREASE()-alert fields are still mapped as gauges."""
    import httpx

    url = (
        f"{elastic_url.rstrip('/')}/{_METRICS_DATA_STREAM}/_field_caps"
        f"?fields={','.join(_COUNTER_ALERT_FIELDS)}"
    )
    try:
        resp = httpx.get(
            url,
            headers={
                "Authorization": f"ApiKey {api_key}",
                "Content-Type": "application/json",
            },
            timeout=15,
        )
    except Exception as exc:
        logger.warning("Could not inspect %s field caps: %s", _METRICS_DATA_STREAM, exc)
        return False
    if resp.status_code == 404:
        return False
    if resp.status_code >= 300:
        logger.warning(
            "field_caps for %s failed (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )
        return False

    fields = resp.json().get("fields", {})
    for name in _COUNTER_ALERT_FIELDS:
        for key in (name, f"metrics.{name}"):
            for meta in fields.get(key, {}).values():
                if meta.get("time_series_metric") == "gauge":
                    return True
    return False


def _reset_processor_timeouts_gauge_mapping() -> None:
    """Delete the metrics stream if processor_timeouts is stuck as a gauge.

    OOTB alert uses INCREASE() which requires counter_long. Earlier builds
    emitted period-delta gauges; that mapping is sticky until the stream is
    recreated.
    """
    try:
        from app.telemetry import ESBulkClient as _Bulk
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for gauge mapping check: %s", exc)
        return

    client = _Bulk()
    if not client.configured:
        return

    if not _counter_alert_fields_mapped_as_gauge(client.elastic_url, client.api_key):
        logger.info(
            "airflow processor_timeouts is not stuck as gauge; leaving %s alone",
            _METRICS_DATA_STREAM,
        )
        return

    import httpx

    try:
        resp = httpx.delete(
            f"{client.elastic_url.rstrip('/')}/_data_stream/{_METRICS_DATA_STREAM}",
            headers={
                "Authorization": f"ApiKey {client.api_key}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )
    except Exception as exc:
        logger.warning("Could not delete %s: %s", _METRICS_DATA_STREAM, exc)
        return

    if resp.status_code < 300:
        logger.info(
            "Deleted %s so processor_timeouts remaps as counter (was gauge)",
            _METRICS_DATA_STREAM,
        )
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist yet", _METRICS_DATA_STREAM)
    else:
        logger.warning(
            "Could not delete %s (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )


class _AirflowCounters(CounterState):
    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.heartbeat = rng.randint(10_000, 500_000)
        self.ti_start = rng.randint(50_000, 5_000_000)
        self.ti_finish = rng.randint(50_000, 5_000_000)
        self.ti_successes = rng.randint(40_000, 4_000_000)
        self.ti_failures = rng.randint(100, 50_000)
        self.killed_externally = rng.randint(0, 500)
        self.processor_timeouts = 0
        self.dag_file_refresh_error = 0
        self.operator_failures = {
            op: rng.randint(0, 5_000) for op in _OPERATORS
        }
        self.ti_failures_by_dag = {
            dag: rng.randint(0, 8_000) for dag in _DAG_NAMES
        }
        self.ti_successes_by_dag = {
            dag: rng.randint(5_000, 500_000) for dag in _DAG_NAMES
        }

    def tick(self, rng: random.Random, load: float, dag_failure_rate: float,
             heartbeat_suppressed: bool) -> None:
        starts = max(1, int(rng.randint(2, 20) * load))
        self.ti_start += starts
        finishes = max(0, starts - rng.randint(0, 2))
        self.ti_finish += finishes
        # OOTB "Task failures above threshold" alert breaches on
        # INCREASE(ti_failures) > 10 — healthy runs must add 0 ambient
        # failures per tick (run() always calls tick() with dag_failure_rate
        # 0.0 and applies task_failures_above_threshold chaos separately).
        failures = int(finishes * dag_failure_rate) + (
            rng.randint(0, 2) if dag_failure_rate > 0 else 0
        )
        successes = max(0, finishes - failures)
        self.ti_successes += successes
        self.ti_failures += failures
        if not heartbeat_suppressed:
            self.heartbeat += max(1, int(rng.randint(1, 3) * load))
        self.killed_externally += rng.randint(0, 1) if dag_failure_rate > 0.3 else 0
        self.processor_timeouts += rng.randint(0, 1) if dag_failure_rate > 0.5 else 0
        self.dag_file_refresh_error += rng.randint(0, 1) if dag_failure_rate > 0.4 else 0

        # Distribute successes/failures across DAGs and operators.
        for _ in range(successes):
            dag = rng.choice(_DAG_NAMES)
            self.ti_successes_by_dag[dag] = self.ti_successes_by_dag.get(dag, 0) + 1
        for _ in range(failures):
            dag = rng.choice(_DAG_NAMES)
            self.ti_failures_by_dag[dag] = self.ti_failures_by_dag.get(dag, 0) + 1
            op = rng.choice(_OPERATORS)
            self.operator_failures[op] = self.operator_failures.get(op, 0) + 1



from generators.airflow_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop

def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    rng = random.Random()
    ns = scenario_data["namespace"] if scenario_data else NAMESPACE
    cfg = scenario_data.get("generator_config", {}) if scenario_data else {}
    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("airflow")
    dag_count: int = min(cfg.get("dag_count", 6), len(_DAG_NAMES))
    dag_names = _DAG_NAMES[:dag_count]
    cloud = cfg.get("cloud") or _default_cloud()
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("airflow")

    impact_keys = SPEC.impact_keys if SPEC is not None else {
        "dag_failure_rate": {"type": "float", "default": 0.0},
        "scheduler_heartbeat_suppressed": {"type": "bool", "default": False},
    }

    resource = build_signal_resource(
        ns=ns, signal_type="metrics", dataset=otel_dataset("airflow"),
        service_name=service_name, service_version="2.9.0",
        service_instance_id=f"{service_name}-scheduler-001",
        host_name=host_name, cloud=cloud,
    )
    scope = {"name": AIRFLOW_SCOPE, "version": SCOPE_VERSION}

    counters = _AirflowCounters(rng)
    last_emitted: dict[tuple, int] = {}

    def _seed(name: str, value: int, attrs: dict | None = None) -> None:
        key = (name, tuple(sorted((attrs or {}).items())))
        last_emitted[key] = int(value)

    # Seed so the first tick emits period deltas, not the huge baseline totals.
    _seed("airflow.scheduler_heartbeat", counters.heartbeat)
    _seed("airflow.ti.start", counters.ti_start)
    _seed("airflow.ti.finish", counters.ti_finish)
    _seed("airflow.scheduler.tasks.killed_externally", counters.killed_externally)
    _seed("airflow.dag_processing.processor_timeouts", counters.processor_timeouts)
    _seed("airflow.dag_file_refresh_error", counters.dag_file_refresh_error)
    for dag in dag_names:
        attrs_dag = {"dag_id": dag}
        _seed("airflow.ti_failures", counters.ti_failures_by_dag.get(dag, 0), attrs_dag)
        _seed("airflow.ti_successes", counters.ti_successes_by_dag.get(dag, 0), attrs_dag)
    for op, value in counters.operator_failures.items():
        _seed("airflow.operator_failures", value, {"operator": op})

    _reset_counter_long_mapping()
    _reset_processor_timeouts_gauge_mapping()
    logger.info("Airflow generator started (namespace=%s)", ns)

    def _emit_tick() -> None:
        nonlocal attrs_dag, dag, op, value
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="airflow_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        c = active_chaos or {}
        dag_queue_size = float(c.get("dag_file_processing_queue_large", 0.0))
        import_errors_key = float(c.get("dag_import_errors_present", 0.0))
        processor_timeouts_key = float(c.get("dag_processor_timeouts", 0.0))
        open_slots_key = float(c.get("executor_open_slots_exhausted", 100.0))
        job_hb_fail = float(c.get("scheduler_job_heartbeat_failures", 0.0))
        schedule_delay_s = float(c.get("dag_run_schedule_delay_high", 0.0))
        loop_duration_s = float(c.get("scheduler_loop_duration_excessive", 0.0))
        tasks_starving_key = float(c.get("tasks_starving_due_to_pool_limits", 0.0))
        task_failures_key = float(c.get("task_failures_above_threshold", 0.0))
        tasks_killed_key = float(c.get("tasks_killed_externally", 0.0))
        heartbeat_key = float(c.get("scheduler_heartbeats_critically_low", 100.0))
        # OOTB alert: COUNT(airflow.scheduler_heartbeat) < 5 — suppress field emission,
        # do not emit a low gauge value (that still increments hb_count).
        heartbeat_suppressed = (
            "scheduler_heartbeats_critically_low" in c and heartbeat_key < 5.0
        )
        load = business_load()

        counters.tick(
            rng, load=load, dag_failure_rate=0.0, heartbeat_suppressed=heartbeat_suppressed,
        )
        if processor_timeouts_key > 0:
            counters.processor_timeouts += int(processor_timeouts_key)
        if tasks_killed_key > 0:
            counters.killed_externally += int(tasks_killed_key)
        if task_failures_key > 0:
            counters.ti_failures += int(task_failures_key)
            dag = rng.choice(dag_names)
            counters.ti_failures_by_dag[dag] = counters.ti_failures_by_dag.get(dag, 0) + int(task_failures_key)

        metrics: list[dict] = []

        # Period-delta gauges for SUM() panels (not cumulative counter_long).
        if not heartbeat_suppressed:
            metrics.append(_period_delta_gauge(
                "airflow.scheduler_heartbeat", "{count}", counters.heartbeat, {}, last_emitted))
        metrics.append(_period_delta_gauge(
            "airflow.ti.start", "{tasks}", counters.ti_start, {}, last_emitted))
        metrics.append(_period_delta_gauge(
            "airflow.ti.finish", "{tasks}", counters.ti_finish, {}, last_emitted))
        metrics.append(_period_delta_gauge(
            "airflow.scheduler.tasks.killed_externally", "{tasks}",
            counters.killed_externally, {}, last_emitted))
        # OOTB alert uses INCREASE() — emit cumulative counter, not period gauge.
        metrics.append(build_cumulative_sum(
            "airflow.dag_processing.processor_timeouts", "{timeouts}",
            counters.processor_timeouts, counters.start_time_ns, {}))
        metrics.append(_period_delta_gauge(
            "airflow.dag_file_refresh_error", "{errors}",
            counters.dag_file_refresh_error, {}, last_emitted))

        # Per-DAG ti_* so overview SUM and Tasks BY attributes.dag_id both work
        # without double-counting an unlabeled series.
        for dag in dag_names:
            attrs_dag = {"dag_id": dag}
            metrics.append(_period_delta_gauge(
                "airflow.ti_failures", "{tasks}",
                counters.ti_failures_by_dag.get(dag, 0), attrs_dag, last_emitted))
            metrics.append(_period_delta_gauge(
                "airflow.ti_successes", "{tasks}",
                counters.ti_successes_by_dag.get(dag, 0), attrs_dag, last_emitted))

        # Per-operator failures (Tasks dashboard groups by attributes.operator)
        for op, value in counters.operator_failures.items():
            metrics.append(_period_delta_gauge(
                "airflow.operator_failures", "{failures}", value,
                {"operator": op}, last_emitted))

        # Executor gauges (LAST_OVER_TIME)
        if "executor_open_slots_exhausted" in c:
            open_slots = max(0, int(open_slots_key))
        else:
            open_slots = max(4, int(rng.randint(4, 32)))
        queued = int(rng.randint(0, 8) * load)
        running = int(rng.randint(0, 6) * load)
        metrics.append(build_gauge("airflow.executor.open_slots", "{slots}", open_slots, {}))
        metrics.append(build_gauge("airflow.executor.queued_tasks", "{tasks}", queued, {}))
        metrics.append(build_gauge("airflow.executor.running_tasks", "{tasks}", running, {}))

        # Scheduler gauges / timings
        starving = int(tasks_starving_key) if tasks_starving_key > 0 else rng.randint(0, 2)
        metrics.append(build_gauge("airflow.scheduler.tasks.starving", "{tasks}", starving, {}))
        metrics.append(build_gauge(
            "airflow.scheduler.critical_section_duration", "s",
            round(rng.uniform(0.001, 0.05), 4), {}))
        loop_val = loop_duration_s if loop_duration_s > 0 else round(rng.uniform(0.1, 5.0), 3)
        metrics.append(build_gauge(
            "airflow.scheduler.scheduler_loop_duration", "s", loop_val, {}))

        # DAG processing gauges / timings
        import_errors = int(import_errors_key) if import_errors_key > 0 else 0
        queue_size = int(dag_queue_size) if dag_queue_size > 0 else rng.randint(0, 10)
        metrics.append(build_gauge(
            "airflow.dag_processing.import_errors", "{errors}", import_errors, {}))
        metrics.append(build_gauge(
            "airflow.dag_processing.file_path_queue_size", "{files}", queue_size, {}))
        metrics.append(build_gauge(
            "airflow.job_heartbeat_failure", "{failures}", int(job_hb_fail), {}))
        delay = schedule_delay_s if schedule_delay_s > 0 else round(rng.uniform(0.0, 30.0), 1)
        metrics.append(build_gauge("airflow.dagrun.schedule_delay", "s", delay, {}))
        metrics.append(build_gauge(
            "airflow.dag_processing.total_parse_time", "s",
            round(rng.uniform(0.5, 30.0) * load, 2), {}))

        # Hardcoded example-DAG duration panel on the Tasks dashboard
        metrics.append(build_gauge(
            "airflow.dag.metrics_generator.work_task_1.duration", "ms",
            round(rng.uniform(50.0, 5_000.0), 1),
            {"dag_id": "metrics_generator"}))

        client.send_metrics_scoped(resource, scope, metrics)
    run_tick_loop(stop_event, _emit_tick, interval=lambda: EMIT_INTERVAL_S)

    logger.info("Airflow generator stopped.")


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "60"))
    def _stop(sig, frame): stop_event.set()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    client = OTLPClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()

if __name__ == "__main__":
    main()
