#!/usr/bin/env python3
"""InfluxDB Generator — sends InfluxDB Prometheus metrics via OTLP.

Emits data matching the influxdb_otel content package
(discovery.datasets: influxdb.otel).

Metric names and attributes are grounded from the Elastic package:
  - packages/influxdb_otel/_dev/shared/kibana/*.yaml (dashboard panels)
  - packages/influxdb_otel/kibana/alerting_rule_template/*.json
  - packages/influxdb_otel/kibana/slo_template/*.json

InfluxDB exposes these at /metrics; the Prometheus receiver scrapes them
with data_stream.dataset=influxdb (Elastic appends .otel).

Dashboard and alert ES|QL panels call MAX/MIN/SUM on Prometheus-style
totals (``http_api_requests_total``, ``http_write_request_count``, …).
Those aggregations reject ``counter_long``, so cumulative totals are
emitted as OTLP gauges (not monotonic sums). Values still increase over
time so MAX shows the total and alert MAX−MIN detects increases.

Earlier revisions emitted monotonic counters; once ES maps a field as
``counter_long`` the mapping is sticky. On startup we delete
``metrics-influxdb.otel-default`` when those fields are still counters.

Usage (standalone):
    python3 -m generators.influxdb_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity
from generator_engine.scrape_clock import scrape_now_ns, scrape_start_ns

SOURCE_INTEGRATION = "elastic/integrations packages/influxdb_otel@0.3.1"

import bisect
import logging
import math
import os
import random
import signal
import threading
import time

from app.telemetry import OTLPClient, _format_attributes
from app.config import NAMESPACE
try:
    from generators.influxdb_otel import SPEC
except Exception:  # pragma: no cover
    SPEC = None

from generator_engine.db_common import (
    CounterState,
    build_gauge,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

INFLUXDB_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("influxdb-generator")
EMIT_INTERVAL_S = 30.0

# Metric names required by dashboards, alerts, and SLO templates.
METRIC_NAMES = (
    # Instance
    "influxdb_uptime_seconds",
    "influxdb_buckets_total",
    "up",
    # HTTP API
    "http_api_requests_total",
    "http_api_request_duration_seconds",
    "http_write_request_count",
    "http_write_request_bytes",
    "http_query_request_count",
    "http_query_response_bytes",
    # Storage
    "storage_wal_size",
    "storage_wal_writes_err",
    "storage_cache_disk_bytes",
    "storage_cache_inuse_bytes",
    "storage_cache_writes_dropped",
    "storage_cache_writes_err",
    "storage_compactions_queued",
    "storage_shard_disk_size",
    "storage_shard_write_sum",
    "storage_shard_write_dropped_sum",
    "storage_shard_write_err_count",
    "storage_shard_write_err_sum",
    "storage_tsm_files_disk_bytes",
    "storage_writer_timeouts",
    # Query controller
    "qc_all_active",
    "qc_compiling_active",
    "qc_queueing_active",
    "qc_executing_active",
    "qc_memory_unused_bytes",
    "qc_requests_total",
    "qc_executing_duration_seconds",
    # Tasks
    "task_scheduler_total_execution_calls",
    "task_scheduler_total_execute_failure",
    "task_executor_workers_busy",
    "task_executor_promise_queue_usage",
    "task_executor_total_runs_active",
    # BoltDB + Go runtime
    "boltdb_reads_total",
    "boltdb_writes_total",
    "go_goroutines",
    "go_memstats_heap_inuse_bytes",
    "go_memstats_heap_sys_bytes",
)

# HTTP API series: (response_code, status_class) — status used by alerts.
_HTTP_API_SERIES = (
    ("200", "2XX"),
    ("204", "2XX"),
    ("400", "4XX"),
    ("401", "4XX"),
    ("500", "5XX"),
)

_WRITE_ENDPOINTS = ("/api/v2/write", "/api/v2/buckets", "/health")
_WRITE_STATUSES = ("204", "400", "500")
_COMPACTION_LEVELS = ("1", "2", "3", "4")

# Latency histogram bounds (seconds) for SLO templates.
_HTTP_LATENCY_BOUNDS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0]
_QC_LATENCY_BOUNDS = [0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]

# Dashboard/alert panels call MAX/MIN/SUM on these — must not be counter_long.
_MAX_AGG_FIELDS = (
    "http_api_requests_total",
    "http_write_request_count",
    "http_write_request_bytes",
    "http_query_request_count",
    "http_query_response_bytes",
    "storage_cache_writes_dropped",
    "storage_cache_writes_err",
    "storage_shard_write_sum",
    "storage_shard_write_dropped_sum",
    "storage_shard_write_err_count",
    "storage_shard_write_err_sum",
    "storage_wal_writes_err",
    "storage_writer_timeouts",
    "qc_requests_total",
    "task_scheduler_total_execution_calls",
    "task_scheduler_total_execute_failure",
    "boltdb_reads_total",
    "boltdb_writes_total",
)

_METRICS_DATA_STREAM = "metrics-influxdb.otel-default"


def _default_cloud() -> dict:
    return {"provider": "aws", "platform": "aws_ec2",
            "region": "us-east-1", "availability_zone": "us-east-1a"}


def _max_agg_fields_mapped_as_counter(elastic_url: str, api_key: str) -> bool:
    """Return True if any MAX-agg field is still mapped as a TSDB counter."""
    import httpx

    url = (
        f"{elastic_url.rstrip('/')}/{_METRICS_DATA_STREAM}/_field_caps"
        f"?fields={','.join(_MAX_AGG_FIELDS)}"
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
    for name in _MAX_AGG_FIELDS:
        for meta in fields.get(name, {}).values():
            if meta.get("time_series_metric") == "counter" or meta.get("type") == "counter_long":
                return True
    return False


def _reset_counter_long_mapping() -> None:
    """Delete the metrics data stream if MAX-agg fields are stuck as counters.

    Fresh gauge samples then recreate the stream without ``counter_long``,
    unblocking Overview / HTTP+Storage / Query+Tasks MAX/MIN panels.  No-op
    when ES credentials are absent or mapping is already correct.
    """
    try:
        from app.telemetry import ESBulkClient
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for counter mapping check: %s", exc)
        return

    client = ESBulkClient()
    if not client.configured:
        return

    if not _max_agg_fields_mapped_as_counter(client.elastic_url, client.api_key):
        logger.info(
            "influxdb MAX-agg fields are not mapped as counters; leaving %s alone",
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
            "Deleted %s so MAX-agg fields remap as gauges (were counter_long)",
            _METRICS_DATA_STREAM,
        )
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist yet", _METRICS_DATA_STREAM)
    else:
        logger.warning(
            "Could not delete %s (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )


# Healthy latency shapes (log-normal: median seconds, sigma). InfluxDB HTTP API
# calls are ~10 ms writes/queries; Flux query execution sits in the low hundreds
# of ms.  Tails are tight enough that >= 99.9% of healthy observations stay under
# the package SLO thresholds (HTTP 0.2 s, query controller 5 s).
_HTTP_LATENCY_SHAPE = (0.012, 0.7)
_QC_LATENCY_SHAPE = (0.25, 0.7)


def _sample_latencies(
    rng: random.Random, n: int, shape: tuple[float, float], latency_mult: float = 1.0,
) -> list[float]:
    """``n`` request latencies (seconds); ``latency_mult`` scales the median (fault)."""
    median, sigma = shape
    mu = math.log(median * max(latency_mult, 0.1))
    return [rng.lognormvariate(mu, sigma) for _ in range(max(n, 0))]


def _histogram(
    name: str,
    unit: str,
    samples: list[float],
    bounds: list[float],
    start_time_ns: str,
    attributes: dict | None = None,
) -> dict:
    """Build a DELTA OTLP explicit-bucket histogram of this scrape's observations.

    Delta (the shape after the EDOT ``cumulativetodelta`` processor) so each
    document holds one interval's latencies: the SLO range/value_count sums then
    reflect the current distribution and a latency fault moves the SLI.
    """
    bucket_counts = [0] * (len(bounds) + 1)
    for value in samples:
        bucket_counts[bisect.bisect_left(bounds, value)] += 1
    dp: dict = {
        "timeUnixNano": scrape_now_ns(),
        "startTimeUnixNano": start_time_ns,
        "count": len(samples),
        "sum": float(sum(samples)),
        "bucketCounts": bucket_counts,
        "explicitBounds": bounds,
    }
    if samples:
        dp["min"] = float(min(samples))
        dp["max"] = float(max(samples))
    if attributes:
        dp["attributes"] = _format_attributes(attributes)
    return {
        "name": name,
        "unit": unit,
        "histogram": {
            "dataPoints": [dp],
            "aggregationTemporality": 1,  # DELTA
        },
    }


class InfluxDBState(CounterState):
    """Increasing totals for InfluxDB Prometheus-style metrics.

    Values accumulate across ticks, but are emitted as gauges so dashboard
    MAX()/alert MAX−MIN aggregations accept the fields.
    """

    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.started_at = time.time() - rng.randint(86_400, 30 * 86_400)
        self.buckets = rng.randint(8, 40)
        self.failing_shard: dict | None = None

        # HTTP API totals keyed by (response_code, status)
        self.http_api = {
            series: rng.randint(50_000, 5_000_000) for series in _HTTP_API_SERIES
        }
        # Write requests keyed by (endpoint, status)
        self.http_write = {
            (ep, st): rng.randint(10_000, 2_000_000)
            for ep in _WRITE_ENDPOINTS
            for st in _WRITE_STATUSES
        }
        self.http_write_bytes = rng.randint(1_000_000_000, 50_000_000_000)
        self.http_query = rng.randint(20_000, 2_000_000)
        self.http_query_bytes = rng.randint(500_000_000, 20_000_000_000)

        # Error/drop counters stay at 0 when healthy — OOTB alerts fire on any increase.
        self.cache_dropped = 0
        self.cache_err = 0
        self.shard_write = rng.randint(1_000_000, 100_000_000)
        self.shard_dropped = 0
        self.shard_err_count = 0
        self.shard_err_sum = 0
        self.wal_err = 0
        self.writer_timeouts = 0

        self.qc_requests = rng.randint(10_000, 1_000_000)
        self.task_executions = rng.randint(1_000, 100_000)
        self.task_failures = 0
        self.boltdb_reads = rng.randint(100_000, 10_000_000)
        self.boltdb_writes = rng.randint(10_000, 1_000_000)

        # Observations in the current scrape interval (delta histograms).
        self.http_latency_n = 0
        self.qc_latency_n = 0

    def tick(self, rng: random.Random, load: float, write_error_rate: float) -> None:
        ok_delta = int(rng.randint(80, 400) * load)
        err_delta = int(ok_delta * write_error_rate)

        self.http_api[("200", "2XX")] += int(ok_delta * 0.6)
        self.http_api[("204", "2XX")] += int(ok_delta * 0.35)
        if err_delta > 0:
            self.http_api[("400", "4XX")] += max(1, int(err_delta * 0.4))
            self.http_api[("401", "4XX")] += max(0, int(err_delta * 0.2))
            self.http_api[("500", "5XX")] += max(0, int(err_delta * 0.4))

        for ep in _WRITE_ENDPOINTS:
            self.http_write[(ep, "204")] += int(ok_delta * (0.5 if ep.endswith("write") else 0.15))
            if err_delta > 0:
                self.http_write[(ep, "400")] += max(0, int(err_delta * 0.3))
                self.http_write[(ep, "500")] += max(0, int(err_delta * 0.4))

        self.http_write_bytes += int(ok_delta * rng.randint(200, 4_000))
        self.http_query += int(rng.randint(20, 150) * load)
        self.http_query_bytes += int(rng.randint(5_000, 80_000) * load)

        if err_delta > 0:
            self.cache_dropped += max(0, int(err_delta * 0.1))
            self.cache_err += max(0, int(err_delta * 0.15))
            self.shard_dropped += max(0, int(err_delta * 0.2))
            self.shard_err_count += max(0, int(err_delta * 0.25))
            self.shard_err_sum += max(0, int(err_delta * rng.randint(1, 20)))
            self.wal_err += max(0, int(err_delta * 0.1))
            if rng.random() < write_error_rate * 0.3:
                self.writer_timeouts += 1
            if rng.random() < write_error_rate * 0.15:
                self.task_failures += 1
        self.shard_write += int(ok_delta * rng.randint(50, 500))

        self.qc_requests += int(rng.randint(10, 80) * load)
        self.task_executions += int(rng.randint(1, 8) * load)
        self.boltdb_reads += rng.randint(50, 500)
        self.boltdb_writes += rng.randint(5, 40)

        self.http_latency_n = int(rng.randint(50, 300) * load)
        self.qc_latency_n = int(rng.randint(5, 40) * load)



from generators.influxdb_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("influxdb")
    org_id = cfg.get("org_id") or ScenarioIdentity(ns).label("org")
    cloud = cfg.get("cloud") or _default_cloud()
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("influxdb")

    impact_keys = SPEC.impact_keys if SPEC is not None else {
        "write_error_rate": {"type": "float", "default": 0.0},
        "compaction_backlog_multiplier": {"type": "float", "default": 1.0},
    }

    resource = build_signal_resource(
        ns=ns, signal_type="metrics", dataset=otel_dataset("influxdb"),
        service_name=service_name, service_version="2.7.4",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name, cloud=cloud,
        extra_attrs={"influxdb.org": org_id},
    )
    scope = {"name": INFLUXDB_SCOPE, "version": SCOPE_VERSION}
    state = InfluxDBState(rng)

    # Counter→gauge remap for MAX-agg fields (sticky TSDB mapping from older builds).
    _reset_counter_long_mapping()

    logger.info("InfluxDB generator started (namespace=%s, org=%s)", ns, org_id)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="influxdb_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        c = active_chaos or {}
        cache_write_errors = float(c.get("cache_write_errors", 0.0))
        compaction_backlog = float(c.get("compaction_queue_backlog", 0.0))
        dropped_writes = float(c.get("dropped_writes", 0.0))
        goroutine_count = float(c.get("go_goroutine_leak_suspected", 0.0))
        http_api_errors = float(c.get("http_api_high_error_rate", 0.0))
        query_queue_depth = float(c.get("query_controller_saturated", 0.0))
        scrape_down = float(c.get("scrape_target_down", 0.0)) > 0
        shard_write_errors = float(c.get("shard_write_errors", 0.0))
        task_failures = float(c.get("task_execution_failures", 0.0))
        wal_write_errors = float(c.get("wal_write_errors", 0.0))
        writer_timeouts = float(c.get("storage_writer_timeouts", 0.0))
        load = business_load()

        state.tick(rng, load, http_api_errors)
        if cache_write_errors > 0:
            state.cache_err += int(cache_write_errors)
        if dropped_writes > 0:
            state.shard_dropped += int(dropped_writes)
        if shard_write_errors > 0:
            state.shard_err_count += int(shard_write_errors)
            state.shard_err_sum += int(shard_write_errors)
        if wal_write_errors > 0:
            state.wal_err += int(wal_write_errors)
        if writer_timeouts > 0:
            state.writer_timeouts += int(writer_timeouts)
        if task_failures > 0:
            state.task_failures += int(task_failures)
        # OOTB HTTP error-rate alert SUM()s gauge totals (not INCREASE). After
        # chaos, 4XX/5XX gauge tips keep error_rate_pct high forever — zero
        # non-2XX buckets when chaos is off so the ratio clears.
        if http_api_errors <= 0:
            for key in list(state.http_api.keys()):
                _code, status = key
                if status != "2XX":
                    state.http_api[key] = 0
            for key in list(state.http_write.keys()):
                _ep, st = key
                if st not in ("204", "200"):
                    state.http_write[key] = 0
        metrics: list[dict] = []

        # ── Instance ──────────────────────────────────────────────────
        metrics.append(build_gauge(
            "influxdb_uptime_seconds", "s",
            int(time.time() - state.started_at), {}))
        metrics.append(build_gauge("influxdb_buckets_total", "1", state.buckets, {}))
        # OOTB scrape-down alert STATS MAX(server.address) — keep column present.
        metrics.append(build_gauge(
            "up", "1", 0 if scrape_down else 1,
            {"server.address": host_name},
        ))

        # ── HTTP API ──────────────────────────────────────────────────
        # Gauges: dashboards/alerts use MAX/MIN/SUM (not RATE).
        # Emit as float so OOTB ``errors / total * 100`` does not truncate
        # to 0 via ES|QL long/long division (error_rate_pct stayed 0.0).
        for (code, status), value in state.http_api.items():
            metrics.append(build_gauge(
                "http_api_requests_total", "{requests}", float(value),
                {"response_code": code, "status": status}))

        for (ep, st), value in state.http_write.items():
            metrics.append(build_gauge(
                "http_write_request_count", "{requests}", value,
                {"endpoint": ep, "status": st}))

        metrics.append(build_gauge(
            "http_write_request_bytes", "By", state.http_write_bytes, {}))
        metrics.append(build_gauge(
            "http_query_request_count", "{requests}", state.http_query, {}))
        metrics.append(build_gauge(
            "http_query_response_bytes", "By", state.http_query_bytes, {}))

        lat_mult = 1.0 + http_api_errors * 4.0
        interval_start = scrape_start_ns(int(EMIT_INTERVAL_S))
        metrics.append(_histogram(
            "http_api_request_duration_seconds", "s",
            _sample_latencies(rng, state.http_latency_n, _HTTP_LATENCY_SHAPE, lat_mult),
            _HTTP_LATENCY_BOUNDS, interval_start))

        # ── Storage ───────────────────────────────────────────────────
        wal = rng.randint(1 * 1024 * 1024, 80 * 1024 * 1024)
        cache_inuse = rng.randint(20 * 1024 * 1024, 400 * 1024 * 1024)
        cache_disk = int(cache_inuse * rng.uniform(0.8, 1.5))
        tsm = rng.randint(2 * 1024 * 1024 * 1024, 80 * 1024 * 1024 * 1024)
        shard = rng.randint(5 * 1024 * 1024 * 1024, 200 * 1024 * 1024 * 1024)
        # OOTB Shard write errors groups BY shard id/bucket: the failing shard must
        # stay the same while the fault is active, or each rule run sees a new one.
        if shard_write_errors <= 0:
            state.failing_shard = None
        shard_attrs = state.failing_shard or {
            "id": f"shard-{rng.randint(1, 8)}",
            "bucket": rng.choice(("metrics", "logs", "traces", "events")),
        }
        if shard_write_errors > 0:
            state.failing_shard = shard_attrs

        metrics.append(build_gauge("storage_wal_size", "By", wal, {}))
        metrics.append(build_gauge("storage_cache_inuse_bytes", "By", cache_inuse, {}))
        metrics.append(build_gauge("storage_cache_disk_bytes", "By", cache_disk, {}))
        metrics.append(build_gauge("storage_tsm_files_disk_bytes", "By", tsm, {}))
        metrics.append(build_gauge("storage_shard_disk_size", "By", shard, shard_attrs))

        metrics.append(build_gauge(
            "storage_cache_writes_dropped", "{writes}", state.cache_dropped, {}))
        metrics.append(build_gauge(
            "storage_cache_writes_err", "{writes}", state.cache_err, {}))
        metrics.append(build_gauge(
            "storage_shard_write_sum", "{points}", state.shard_write, shard_attrs))
        metrics.append(build_gauge(
            "storage_shard_write_dropped_sum", "{points}", state.shard_dropped, shard_attrs))
        metrics.append(build_gauge(
            "storage_shard_write_err_count", "{errors}", state.shard_err_count, shard_attrs))
        metrics.append(build_gauge(
            "storage_shard_write_err_sum", "{errors}", state.shard_err_sum, shard_attrs))
        metrics.append(build_gauge(
            "storage_wal_writes_err", "{errors}", state.wal_err, {}))
        metrics.append(build_gauge(
            "storage_writer_timeouts", "{timeouts}", state.writer_timeouts, {}))

        for level in _COMPACTION_LEVELS:
            queued = int(compaction_backlog) if compaction_backlog > 0 and level == "1" else rng.randint(0, 4)
            metrics.append(build_gauge(
                "storage_compactions_queued", "1", queued, {"level": level}))

        # ── Query controller ──────────────────────────────────────────
        compiling = rng.randint(0, max(1, int(3 * load)))
        queueing = int(query_queue_depth) if query_queue_depth > 0 else rng.randint(0, max(1, int(4 * load)))
        executing = rng.randint(0, max(1, int(6 * load)))
        metrics.append(build_gauge("qc_compiling_active", "{queries}", compiling, {}))
        metrics.append(build_gauge("qc_queueing_active", "{queries}", queueing, {}))
        metrics.append(build_gauge("qc_executing_active", "{queries}", executing, {}))
        metrics.append(build_gauge(
            "qc_all_active", "{queries}", compiling + queueing + executing, {}))
        # Alert fires when queueing >= 5 AND unused memory < 10 MiB.
        unused = (
            rng.randint(1_000_000, 5_000_000)
            if query_queue_depth >= 5
            else rng.randint(50 * 1024 * 1024, 800 * 1024 * 1024)
        )
        metrics.append(build_gauge("qc_memory_unused_bytes", "By", unused, {}))
        metrics.append(build_gauge(
            "qc_requests_total", "{requests}", state.qc_requests, {}))
        metrics.append(_histogram(
            "qc_executing_duration_seconds", "s",
            _sample_latencies(rng, state.qc_latency_n, _QC_LATENCY_SHAPE, lat_mult),
            _QC_LATENCY_BOUNDS, interval_start))

        # ── Tasks ─────────────────────────────────────────────────────
        metrics.append(build_gauge(
            "task_scheduler_total_execution_calls", "{calls}",
            state.task_executions, {}))
        metrics.append(build_gauge(
            "task_scheduler_total_execute_failure", "{failures}",
            state.task_failures, {}))
        metrics.append(build_gauge(
            "task_executor_workers_busy", "1",
            round(rng.uniform(0.1, min(0.95, 0.3 + http_api_errors)), 2), {}))
        metrics.append(build_gauge(
            "task_executor_promise_queue_usage", "1",
            round(rng.uniform(0.0, 0.6), 2), {}))
        metrics.append(build_gauge(
            "task_executor_total_runs_active", "{tasks}",
            rng.randint(0, max(1, int(5 * load))), {}))

        # ── BoltDB + Go ───────────────────────────────────────────────
        metrics.append(build_gauge(
            "boltdb_reads_total", "{reads}", state.boltdb_reads, {}))
        metrics.append(build_gauge(
            "boltdb_writes_total", "{writes}", state.boltdb_writes, {}))
        heap_inuse = rng.randint(100 * 1024 * 1024, 800 * 1024 * 1024)
        metrics.append(build_gauge("go_goroutines", "1", int(goroutine_count) if goroutine_count > 0 else rng.randint(80, 400), {}))
        metrics.append(build_gauge("go_memstats_heap_inuse_bytes", "By", heap_inuse, {}))
        metrics.append(build_gauge(
            "go_memstats_heap_sys_bytes", "By",
            int(heap_inuse * rng.uniform(1.2, 1.8)), {}))

        client.send_metrics_scoped(resource, scope, metrics)
    run_tick_loop(stop_event, _emit_tick, interval=lambda: EMIT_INTERVAL_S)

    logger.info("InfluxDB generator stopped.")


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "60"))

    def _stop(sig, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    client = OTLPClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
