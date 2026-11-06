#!/usr/bin/env python3
"""MySQL Log Generator — sends mysqlreceiver OTel events and metrics via OTLP.

Emits data matching the mysql_otel content package (discovery.datasets: mysqlreceiver),
which triggers Fleet auto-install and populates the bundled Overview / Performance /
Queries / Availability dashboards.

Signal types emitted:
  - Log events: db.server.query_sample (active queries) and db.server.top_query (digest summary)
  - Metrics: mysql.query.*, mysql.buffer_pool.*, mysql.events_statements_summary_by_digest.*,
             mysql.events_waits_current.timer_wait, mysql.table.*
  - Traces: correlated DB spans for APM Service Map topology (optional, per scenario)

Usage (standalone):
    python3 -m generators.mysql_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/mysql_otel@0.6.1"

import logging
import os
import random
import secrets
import signal
import threading
import time

from app.telemetry import OTLPClient, _format_attributes, SCHEMA_URL
from app.config import NAMESPACE
from app.trace_context import _trace_context_store
from generator_engine.db_common import (
    CounterState,
    build_cumulative_sum,
    build_gauge,
    build_non_monotonic_sum,
    impact_value,
    resolve_active_chaos,
)

logger = logging.getLogger("mysql-otel-generator")

# OOTB alert INCREASE(metrics.mysql.row_locks) needs counter_long. Older builds
# emitted gauges; that TSDB mapping is sticky until the stream is deleted.
# Dirty-ratio alert does long/long without TO_DOUBLE — usage must be double.
_ROW_LOCK_COUNTER_FIELDS = ("mysql.row_locks", "metrics.mysql.row_locks")
_USAGE_FLOAT_FIELDS = (
    "mysql.buffer_pool.usage",
    "metrics.mysql.buffer_pool.usage",
)
_METRICS_DATA_STREAM = "metrics-mysqlreceiver.otel-default"
# Healthy replica lag ceiling (s); package SLO needs < 5 s, alert fires > 60 s.
HEALTHY_REPLICATION_LAG_MAX_S = 2


def _row_lock_fields_mapped_as_gauge(elastic_url: str, api_key: str) -> bool:
    """Return True if mysql.row_locks is still mapped as a TSDB gauge."""
    import httpx

    url = (
        f"{elastic_url.rstrip('/')}/{_METRICS_DATA_STREAM}/_field_caps"
        f"?fields={','.join(_ROW_LOCK_COUNTER_FIELDS)}"
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
    for name in _ROW_LOCK_COUNTER_FIELDS:
        for meta in fields.get(name, {}).values():
            if meta.get("time_series_metric") == "gauge":
                return True
    return False


def _usage_fields_mapped_as_long(elastic_url: str, api_key: str) -> bool:
    """Return True if mysql.buffer_pool.usage is stuck as long (integer division)."""
    import httpx

    url = (
        f"{elastic_url.rstrip('/')}/{_METRICS_DATA_STREAM}/_field_caps"
        f"?fields={','.join(_USAGE_FLOAT_FIELDS)}"
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
        return False
    fields = resp.json().get("fields", {})
    for name in _USAGE_FLOAT_FIELDS:
        for meta in fields.get(name, {}).values():
            if meta.get("type") == "long":
                return True
    return False


def _reset_row_locks_gauge_mapping() -> None:
    """Delete the metrics stream if row_locks is gauge or usage is sticky long."""
    try:
        from app.telemetry import ESBulkClient
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for mysql mapping check: %s", exc)
        return

    client = ESBulkClient()
    if not client.configured:
        return

    need_row = _row_lock_fields_mapped_as_gauge(client.elastic_url, client.api_key)
    need_usage = _usage_fields_mapped_as_long(client.elastic_url, client.api_key)
    if not need_row and not need_usage:
        logger.info(
            "mysql mappings ok (row_locks counter, usage not long); leaving %s alone",
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
            "Deleted %s (row_locks_gauge=%s usage_long=%s)",
            _METRICS_DATA_STREAM,
            need_row,
            need_usage,
        )
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist yet", _METRICS_DATA_STREAM)
    else:
        logger.warning(
            "Could not delete %s (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )


# MySQL is the backend data tier for the demo: any active fault is observable
# as slow / failing queries downstream. This permissive matching keeps the
# trace<->log channel pivot demonstrable across every scenario.

SPAN_KIND_CLIENT = 3
STATUS_OK = 1
STATUS_ERROR = 2

MYSQL_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/mysqlreceiver"
)
SCOPE_VERSION = "0.115.0"

BATCH_INTERVAL_MIN = 2
BATCH_INTERVAL_MAX = 5
BATCH_SIZE_MIN = 3
BATCH_SIZE_MAX = 12

QUERY_TEMPLATES = [
    ("SELECT", "SELECT * FROM telemetry_readings WHERE timestamp > NOW() - INTERVAL 5 MINUTE AND subsystem = '{subsystem}' ORDER BY timestamp DESC LIMIT 1000"),
    ("SELECT", "SELECT sr.sensor_id, sr.reading, sc.baseline FROM sensor_data sr JOIN sensor_calibrations sc ON sr.sensor_id = sc.sensor_id WHERE sr.timestamp > NOW() - INTERVAL 10 MINUTE AND ABS(sr.reading - sc.baseline) > sc.threshold"),
    ("SELECT", "SELECT subsystem, COUNT(*) as error_count, AVG(severity_number) as avg_severity FROM log_entries WHERE severity_text IN ('ERROR', 'FATAL') AND timestamp > NOW() - INTERVAL 15 MINUTE GROUP BY subsystem HAVING error_count > 10"),
    ("INSERT", "INSERT INTO telemetry_readings (sensor_id, subsystem, reading, unit, timestamp, mission_phase) VALUES ('{sensor_id}', '{subsystem}', {reading}, '{unit}', NOW(), '{phase}')"),
    ("INSERT", "INSERT INTO sensor_calibrations (sensor_id, baseline, threshold, calibration_epoch, updated_at) VALUES ('{sensor_id}', {baseline}, {threshold}, '{epoch}', NOW())"),
    ("UPDATE", "UPDATE sensor_registry SET last_reading = {reading}, last_seen = NOW(), status = '{status}' WHERE sensor_id = '{sensor_id}'"),
    ("UPDATE", "UPDATE mission_events SET status = 'resolved', resolved_at = NOW(), resolution_notes = '{notes}' WHERE event_id = '{event_id}' AND status = 'active'"),
    ("SELECT", "SELECT me.event_id, me.channel, me.subsystem, COUNT(el.id) as cascade_count FROM mission_events me LEFT JOIN escalation_log el ON me.event_id = el.source_event WHERE me.status = 'active' GROUP BY me.event_id, me.channel, me.subsystem ORDER BY cascade_count DESC"),
    ("DELETE", "DELETE FROM metric_snapshots WHERE timestamp < NOW() - INTERVAL 24 HOUR AND archived = 1"),
    ("SELECT", "SELECT t.trace_id, COUNT(s.span_id) as span_count, MAX(s.duration_ms) as max_duration FROM trace_spans s JOIN (SELECT DISTINCT trace_id FROM trace_spans WHERE duration_ms > 5000 AND timestamp > NOW() - INTERVAL 5 MINUTE) t ON s.trace_id = t.trace_id GROUP BY t.trace_id"),
]

SUBSYSTEMS = ["propulsion", "guidance", "communications", "payload", "relay", "ground", "validation", "safety"]
PHASES = ["PRE-LAUNCH", "COUNTDOWN", "LAUNCH", "ASCENT"]

PROCESSLIST_COMMANDS = ["Query", "Execute", "Prepare", "Fetch"]
PROCESSLIST_STATES = [
    "Sending data", "Writing to net", "Sorting result", "init",
    "waiting for handler commit", "System lock", "Waiting for table metadata lock",
]
WAIT_TYPES = [
    "wait/io/file/sql/binlog",
    "wait/io/socket/sql/client_connection",
    "wait/synch/mutex/innodb/buf_pool_mutex",
    "wait/synch/rwlock/innodb/dict_operation_lock",
    "wait/io/file/innodb/innodb_data_file",
]
# Real mysqlreceiver operation/kind values matching OTel mysqlreceiver metadata.yaml
BUFFER_POOL_OPERATIONS = [
    "read_ahead_rnd", "read_ahead", "read_ahead_evicted",
    "read_requests", "reads", "wait_free", "write_requests",
]
BUFFER_POOL_PAGE_KINDS = ["data", "free", "misc", "total"]

ROW_OPERATIONS = ["inserted", "deleted", "read", "updated"]
HANDLER_KINDS = [
    "commit", "delete", "discover", "external_lock", "mrr_init", "prepare",
    "read_first", "read_key", "read_last", "read_next", "read_prev", "read_rnd", "read_rnd_next",
    "rollback", "savepoint", "savepoint_rollback", "update", "write",
]
NETWORK_IO_KINDS = ["received", "sent"]
LOCK_KINDS = ["immediate", "waited"]
PAGE_OPERATIONS = ["created", "read", "written"]
TABLE_IO_OPERATIONS = ["delete", "fetch", "insert", "update"]
LOG_OPERATIONS = ["waits", "write_requests", "writes"]
DOUBLE_WRITE_KINDS = ["pages_written", "writes"]
CONNECTION_ERROR_KINDS = [
    "accept", "internal", "max_connections", "peer_address",
    "select", "tcpwrap", "aborted", "aborted_clients", "locked",
]
THREAD_KINDS = ["connected", "running", "cached", "created"]


class MySQLMetricState(CounterState):
    """Tracks cumulative counter values for a MySQL instance.

    Subclasses CounterState so cumulative sums share a stable startTimeUnixNano
    across scrapes (required for ES|QL RATE over time buckets).
    """

    def __init__(self, rng: random.Random):
        # Keep CounterState.value near zero; start_time_ns must stay stable for RATE().
        super().__init__(rng, initial=0)
        # Start counters near zero so first RATE buckets aren't dominated by
        # multi-million initials (and restarts don't look like counter resets).
        self.query_count = rng.randint(0, 1_000)
        self.slow_count = 0
        self.client_count = rng.randint(0, 500)
        self.bp_operations = {op: rng.randint(0, 5_000) for op in BUFFER_POOL_OPERATIONS}
        # Ensure read_requests >> reads so buffer pool hit ratio panels are meaningful
        self.bp_operations["read_requests"] = max(
            self.bp_operations["read_requests"],
            self.bp_operations["reads"] * 50 + 1_000,
        )
        self.row_operations = {op: rng.randint(0, 1_000) for op in ROW_OPERATIONS}
        self.handlers = {k: rng.randint(0, 1_000) for k in HANDLER_KINDS}
        self.network_io = {k: rng.randint(0, 100_000) for k in NETWORK_IO_KINDS}
        self.locks = {k: rng.randint(0, 500) for k in LOCK_KINDS}
        self.row_lock_waits = 0
        self.row_lock_time = 0
        self.page_operations = {op: rng.randint(0, 1_000) for op in PAGE_OPERATIONS}
        self.log_operations = {op: rng.randint(0, 500) for op in LOG_OPERATIONS}
        self.double_writes = {k: rng.randint(0, 500) for k in DOUBLE_WRITE_KINDS}
        self.connection_errors = {k: 0 for k in CONNECTION_ERROR_KINDS}
        # (schema, table, operation) -> cumulative counters for table I/O waits
        self.table_io_wait_count: dict[tuple[str, str, str], int] = {}
        self.table_io_wait_time: dict[tuple[str, str, str], int] = {}

    def tick(self, rng: random.Random, active_chaos: dict | None = None):
        chaos = active_chaos or {}
        slow_extra = int(float(impact_value(chaos, "high_slow_query_rate", default=0.0) or 0.0) * 50)
        self.query_count += rng.randint(20, 200)
        self.slow_count += slow_extra
        self.client_count += rng.randint(10, 80)
        for op in BUFFER_POOL_OPERATIONS:
            if op == "read_requests":
                self.bp_operations[op] += rng.randint(1000, 5000)
            elif op == "reads":
                self.bp_operations[op] += rng.randint(10, 100)
            else:
                self.bp_operations[op] += rng.randint(100, 2000)
        for op in ROW_OPERATIONS:
            self.row_operations[op] += rng.randint(100, 5000)
        for k in HANDLER_KINDS:
            self.handlers[k] += rng.randint(10, 1000)
        self.network_io["received"] += rng.randint(10_000, 500_000)
        self.network_io["sent"] += rng.randint(10_000, 200_000)
        for k in LOCK_KINDS:
            self.locks[k] += rng.randint(0, 50)
        row_lock_extra = int(impact_value(chaos, "high_row_lock_contention", default=0) or 0)
        waits_delta = row_lock_extra
        self.row_lock_waits += waits_delta
        self.row_lock_time += waits_delta * rng.randint(1_000, 10_000)
        for op in PAGE_OPERATIONS:
            self.page_operations[op] += rng.randint(10, 500)
        for op in LOG_OPERATIONS:
            self.log_operations[op] += rng.randint(5, 200)
        for k in DOUBLE_WRITE_KINDS:
            self.double_writes[k] += rng.randint(1, 50)


def _build_log_resource(ns: str) -> dict:
    attrs = {
        "service.name": ScenarioIdentity(ns).service("mysql-primary"),
        "service.namespace": ns,
        "service.version": "8.0.36",
        "service.instance.id": ScenarioIdentity(ns).instance("mysql-primary", 1),
        "telemetry.sdk.language": "python",
        "telemetry.sdk.name": "opentelemetry",
        "telemetry.sdk.version": "1.24.0",
        "cloud.provider": "gcp",
        "cloud.platform": "gcp_compute_engine",
        "cloud.region": "us-central1",
        "cloud.availability_zone": "us-central1-b",
        "deployment.environment": f"production-{ns}",
        "host.name": ScenarioIdentity(ns).label("mysql-host"),
        "host.architecture": "amd64",
        "os.type": "linux",
        "data_stream.type": "logs",
        "data_stream.dataset": "mysqlreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _build_metrics_resource(ns: str) -> dict:
    attrs = {
        "service.name": ScenarioIdentity(ns).service("mysql-primary"),
        "service.namespace": ns,
        "service.version": "8.0.36",
        "service.instance.id": ScenarioIdentity(ns).instance("mysql-primary", 1),
        "telemetry.sdk.language": "python",
        "telemetry.sdk.name": "opentelemetry",
        "telemetry.sdk.version": "1.24.0",
        "cloud.provider": "gcp",
        "cloud.platform": "gcp_compute_engine",
        "cloud.region": "us-central1",
        "cloud.availability_zone": "us-central1-b",
        "deployment.environment": f"production-{ns}",
        "host.name": ScenarioIdentity(ns).label("mysql-host"),
        "host.architecture": "amd64",
        "os.type": "linux",
        "mysql.instance.endpoint": f"{ScenarioIdentity(ns).host('mysql-host')}:3306",
        "data_stream.type": "metrics",
        "data_stream.dataset": "mysqlreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _build_trace_resource(ns: str) -> dict:
    attrs = {
        "service.name": ScenarioIdentity(ns).service("mysql-primary"),
        "service.namespace": ns,
        "service.version": "8.0.36",
        "service.instance.id": ScenarioIdentity(ns).instance("mysql-primary", 1),
        "telemetry.sdk.language": "python",
        "telemetry.sdk.name": "opentelemetry",
        "telemetry.sdk.version": "1.24.0",
        "cloud.provider": "gcp",
        "cloud.platform": "gcp_compute_engine",
        "cloud.region": "us-central1",
        "cloud.availability_zone": "us-central1-b",
        "deployment.environment": f"production-{ns}",
        "host.name": ScenarioIdentity(ns).label("mysql-host"),
        "host.architecture": "amd64",
        "os.type": "linux",
        "data_stream.type": "traces",
        "data_stream.dataset": "generic",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _render_query(template: str, rng: random.Random, db_prefix: str) -> str:
    return template.format(
        subsystem=rng.choice(SUBSYSTEMS),
        sensor_id=f"SEN-{rng.randint(1000, 9999)}",
        reading=round(rng.uniform(0, 1000), 2),
        unit=rng.choice(["K", "PSI", "kg/s", "dB", "ms", "deg"]),
        phase=rng.choice(PHASES),
        baseline=round(rng.uniform(0, 500), 2),
        threshold=round(rng.uniform(1, 50), 2),
        epoch=f"E{rng.randint(100, 999)}",
        status=rng.choice(["NOMINAL", "WARNING", "CRITICAL"]),
        notes=f"Auto-remediated by agent at T-{rng.randint(1, 600)}",
        event_id=f"EVT-{rng.randint(10000, 99999)}",
    )


def _generate_query_sample_event(
    client: OTLPClient,
    rng: random.Random,
    databases: list,
    tables: dict,
    db_prefix: str,
    ns: str,
    active_chaos: dict | None = None,
) -> tuple[dict, dict]:
    """Generate a db.server.query_sample log event and a correlated DB trace span.

    When ``active_chaos`` is provided, the query is slow (>500ms),
    the span is marked STATUS_ERROR, and chaos.* attrs tag both signals.
    Trace_id/span_id sourced from the shared trace context store when available.
    """
    operation, template = rng.choice(QUERY_TEMPLATES)
    db = rng.choice(databases)
    table = rng.choice(tables[db])
    query = _render_query(template, rng, db_prefix)

    chaos = active_chaos or {}
    slow_fraction = float(impact_value(chaos, "high_slow_query_rate", default=0.0) or 0.0)
    # Emit duration when slow: fixed breach window (not a SPEC impact_key).
    slow_ms = 5000
    is_slow = bool(active_chaos) and rng.random() < max(slow_fraction, 0.01)
    # Stock Queries / Active Queries panels treat timer_wait as seconds.
    if is_slow:
        wait_s = rng.uniform(max(0.5, slow_ms / 1000.0), 15.0)
    else:
        wait_s = rng.uniform(0.001, 0.080)
    duration_ms = max(1, int(wait_s * 1000))

    trace_id = None
    span_id = None
    if active_chaos:
        for svc in active_chaos.get("affected_services", []):
            t, s = _trace_context_store.get(svc, namespace=ns)
            if t and s:
                trace_id, span_id = t, s
                break
    if trace_id is None:
        trace_id = secrets.token_hex(16)
        span_id = secrets.token_hex(8)

    attrs = {
        "db.query.text": query,
        "mysql.threads.thread_id": rng.randint(1, 500),
        "user.name": ScenarioIdentity(ns).database("app"),
        "db.namespace": db,
        "mysql.threads.processlist_command": rng.choice(PROCESSLIST_COMMANDS),
        "mysql.threads.processlist_state": rng.choice(PROCESSLIST_STATES),
        "mysql.wait_type": rng.choice(WAIT_TYPES),
        "mysql.events_waits_current.timer_wait": float(wait_s),
    }
    if active_chaos:
        attrs["chaos.channel"] = active_chaos["channel_id"]
        if active_chaos.get("name"):
            attrs["chaos.fault_type"] = active_chaos["name"]
        if active_chaos.get("subsystem"):
            attrs["chaos.subsystem"] = active_chaos["subsystem"]
        if active_chaos.get("error_type"):
            attrs["error.type"] = active_chaos["error_type"]

    severity = "ERROR" if active_chaos else ("WARN" if duration_ms > 200 else "INFO")
    log_record = client.build_log_record(
        severity=severity,
        body=query,
        attributes=attrs,
        trace_id=trace_id,
        span_id=span_id,
        event_name="db.server.query_sample",
    )

    span_status = STATUS_ERROR if (active_chaos or duration_ms > 500) else STATUS_OK
    _span_attrs = {
        "db.system": "mysql",
        "db.name": db,
        "db.statement": query,
        "db.operation": operation,
        "db.sql.table": table,
        "net.peer.name": ScenarioIdentity(ns).label("mysql-host"),
        "net.peer.port": 3306,
        "db.user": ScenarioIdentity(ns).database("app"),
    }
    if active_chaos:
        _span_attrs["chaos.channel"] = active_chaos["channel_id"]
        if active_chaos.get("name"):
            _span_attrs["chaos.fault_type"] = active_chaos["name"]
        if active_chaos.get("subsystem"):
            _span_attrs["chaos.subsystem"] = active_chaos["subsystem"]
        if active_chaos.get("error_type"):
            _span_attrs["error.type"] = active_chaos["error_type"]
    span = client.build_span(
        name=f"{operation} {table}",
        trace_id=trace_id,
        span_id=span_id,
        kind=SPAN_KIND_CLIENT,
        duration_ms=duration_ms,
        status_code=span_status,
        attributes=_span_attrs,
    )

    return log_record, span


def _generate_top_query_event(
    client: OTLPClient,
    rng: random.Random,
    databases: list,
    tables: dict,
    db_prefix: str,
) -> dict:
    """Generate a db.server.top_query log event (statement digest summary)."""
    operation, template = rng.choice(QUERY_TEMPLATES)
    db = rng.choice(databases)
    query = _render_query(template, rng, db_prefix)
    digest = secrets.token_hex(16)

    # Stock Queries KPIs: SUM(sum_timer_wait) as seconds, then
    # avg_time_ms = total_time_sec / total_calls * 1000. Emit delta seconds,
    # not ns-scale magnitudes (those show as millions of years / billions of ms).
    count_star = rng.randint(20, 2_000)
    avg_s = rng.uniform(0.0005, 0.050)
    if rng.random() < 0.05:
        avg_s = rng.uniform(0.1, 2.0)
    sum_timer_wait = float(count_star * avg_s)

    attrs = {
        "db.query.text": query,
        "mysql.events_statements_summary_by_digest.digest": digest,
        "mysql.events_statements_summary_by_digest.count_star": count_star,
        "mysql.events_statements_summary_by_digest.sum_timer_wait": sum_timer_wait,
        "db.namespace": db,
        "user.name": ScenarioIdentity(db_prefix).database("app"),
    }

    return client.build_log_record(
        severity="INFO",
        body=query,
        attributes=attrs,
        event_name="db.server.top_query",
    )


def _generate_mysql_metrics(
    state: MySQLMetricState, tables: dict, rng: random.Random,
    active_chaos: dict | None = None,
) -> list[dict]:
    chaos = active_chaos or {}
    state.tick(rng, active_chaos=chaos)
    start = state.start_time_ns
    metrics: list[dict] = []

    # ── Query counters ────────────────────────────────────────────────
    metrics.append(build_cumulative_sum("mysql.query.count", "1", state.query_count, start))
    metrics.append(build_cumulative_sum("mysql.query.slow.count", "1", state.slow_count, start))
    metrics.append(build_cumulative_sum("mysql.query.client.count", "1", state.client_count, start))

    # ── Row operations (rate panels in Overview + Performance) ────────
    for op in ROW_OPERATIONS:
        metrics.append(
            build_cumulative_sum("mysql.row_operations", "1", state.row_operations[op], start, {"operation": op})
        )

    # ── Handler statistics (Performance dashboard) ───────────────────
    for k in HANDLER_KINDS:
        metrics.append(
            build_cumulative_sum("mysql.handlers", "1", state.handlers[k], start, {"kind": k})
        )

    # ── Network I/O (Performance dashboard) ──────────────────────────
    for k in NETWORK_IO_KINDS:
        metrics.append(
            build_cumulative_sum("mysql.client.network.io", "By", state.network_io[k], start, {"kind": k})
        )

    # ── Threads (Overview + Availability dashboards) ──────────────────
    sat = float(chaos.get("thread_saturation", 0.0) or 0.0)
    connected = rng.randint(10, 200)
    if sat > 0:
        connected = max(connected, int(100 * sat / max(sat, 0.01)))  # placeholder
        # Prefer high running/connected ratio for alert (running/connected > 0.9)
        running = max(1, int(connected * min(sat, 0.99)))
    else:
        running = rng.randint(1, max(1, min(connected // 5, 20)))
    cached = rng.randint(0, 20)
    for k, v in [("connected", connected), ("running", running), ("cached", cached), ("created", connected + rng.randint(0, 50))]:
        metrics.append(build_gauge("mysql.threads", "1", v, {"kind": k}))
    metrics.append(build_gauge("mysql.max_used_connections", "1", rng.randint(connected, connected + 50)))

    # ── Connection errors (Overview + Availability dashboards) ────────
    conn_err_extra = int(impact_value(chaos, "high_connection_error_rate", default=0) or 0)
    if conn_err_extra > 0:
        state.connection_errors["aborted"] += conn_err_extra
    for k in CONNECTION_ERROR_KINDS:
        metrics.append(
            build_cumulative_sum("mysql.connection.errors", "1", state.connection_errors[k], start, {"error": k})
        )

    # ── Buffer pool pages — kind enum: data/free/misc/total (not dirty) ─
    total_pages = rng.randint(8000, 32000)
    # SPEC/alert use a 0–1 ratio (>0.75). GT often sends percent (75.0).
    dirty_raw = float(impact_value(chaos, "high_buffer_pool_dirty_page_ratio", default=0.0) or 0.0)
    if dirty_raw <= 0:
        dirty_ratio = 0.0
    elif dirty_raw > 1.0:
        dirty_ratio = dirty_raw / 100.0
    else:
        dirty_ratio = dirty_raw
    # Alert is strictly > 0.75; a probe of exactly 75% must still breach.
    if dirty_ratio >= 0.75:
        dirty_ratio = max(dirty_ratio, 0.85)
    dirty_ratio = min(dirty_ratio, 0.98)
    free_pages = rng.randint(100, total_pages // 4)
    misc_pages = rng.randint(0, 50)
    data_pages = max(0, total_pages - free_pages - misc_pages)
    dirty_pages = int(data_pages * dirty_ratio) if dirty_ratio > 0 else rng.randint(0, max(1, data_pages // 20))
    clean_data_pages = max(0, data_pages - dirty_pages)
    for kind, count in [
        ("data", data_pages), ("free", free_pages), ("misc", misc_pages), ("total", total_pages),
    ]:
        metrics.append(build_gauge("mysql.buffer_pool.pages", "1", count, {"kind": kind}))

    # data_pages status=dirty|clean (Availability dirty-pages panel)
    metrics.append(build_gauge("mysql.buffer_pool.data_pages", "1", dirty_pages, {"status": "dirty"}))
    metrics.append(build_gauge("mysql.buffer_pool.data_pages", "1", clean_data_pages, {"status": "clean"}))

    # ── Buffer pool limit/usage (Overview gauge + dirty ratio alert) ──
    # Emit usage as double — OOTB does dirty/total without TO_DOUBLE (long/long → 0).
    bp_size_bytes = total_pages * 16384  # 16KB pages
    dirty_bytes = dirty_pages * 16384
    # The receiver reports clean = data - dirty, so the alert's dirty/(dirty+clean) is
    # dirty_pages / data_pages. Counting free/misc pages as clean made the ratio
    # depend on the random pool size and miss the > 0.75 threshold.
    clean_bytes = clean_data_pages * 16384
    metrics.append(build_gauge("mysql.buffer_pool.limit", "By", bp_size_bytes))
    metrics.append(build_gauge("mysql.buffer_pool.usage", "By", float(dirty_bytes), {"status": "dirty"}))
    metrics.append(build_gauge("mysql.buffer_pool.usage", "By", float(clean_bytes), {"status": "clean"}))

    # ── Buffer pool operations — OTel ops incl. read_requests/reads ───
    for op in BUFFER_POOL_OPERATIONS:
        metrics.append(
            build_cumulative_sum(
                "mysql.buffer_pool.operations", "1", state.bp_operations[op], start, {"operation": op},
            )
        )

    # ── Lock statistics (Performance dashboard) ──────────────────────
    for k in LOCK_KINDS:
        metrics.append(build_cumulative_sum("mysql.locks", "1", state.locks[k], start, {"kind": k}))

    # Row locks — both kinds are monotonic cumulative sums (unit "1")
    metrics.append(build_cumulative_sum("mysql.row_locks", "1", state.row_lock_waits, start, {"kind": "waits"}))
    metrics.append(build_cumulative_sum("mysql.row_locks", "1", state.row_lock_time, start, {"kind": "time"}))

    # ── Page operations (Performance dashboard) ───────────────────────
    for op in PAGE_OPERATIONS:
        metrics.append(
            build_cumulative_sum("mysql.page_operations", "1", state.page_operations[op], start, {"operation": op})
        )

    # ── Log operations (Performance dashboard) ────────────────────────
    for op in LOG_OPERATIONS:
        metrics.append(
            build_cumulative_sum("mysql.log_operations", "1", state.log_operations[op], start, {"operation": op})
        )

    # ── Double writes (Performance dashboard) ─────────────────────────
    for k in DOUBLE_WRITE_KINDS:
        metrics.append(
            build_cumulative_sum("mysql.double_writes", "1", state.double_writes[k], start, {"kind": k})
        )

    # ── events_statements_summary_by_digest ──────────────────────────
    # sum_timer_wait is delta seconds (OTel metadata / stock Queries ES|QL).
    for _ in range(rng.randint(3, 8)):
        digest = secrets.token_hex(16)
        count_star = rng.randint(20, 2_000)
        avg_s = rng.uniform(0.0005, 0.050)
        if rng.random() < 0.05:
            avg_s = rng.uniform(0.1, 2.0)
        sum_wait = float(count_star * avg_s)
        attrs = {"digest": digest}
        metrics.append(build_gauge("mysql.events_statements_summary_by_digest.count_star", "1", count_star, attrs))
        metrics.append(build_gauge("mysql.events_statements_summary_by_digest.sum_timer_wait", "s", sum_wait, attrs))

    # ── events_waits_current.timer_wait ──────────────────────────────
    metrics.append(
        build_gauge(
            "mysql.events_waits_current.timer_wait", "s",
            round(rng.uniform(0.001, 5.0), 6),
            {"wait_type": rng.choice(WAIT_TYPES)},
        )
    )

    # ── Replication metrics (Availability dashboard) ──────────────────
    # Non-monotonic cumulative sums (UpDownCounter). A healthy replica keeps
    # up with its source: 0–2 s behind, under the package SLO (< 5 s per 1m
    # slice) and far below the 60 s alert / 30–60 s chart reference lines.
    lag_s = int(float(impact_value(chaos, "replication_lag", default=0.0) or 0.0))
    if lag_s <= 0:
        lag_s = rng.randint(0, HEALTHY_REPLICATION_LAG_MAX_S)
    metrics.append(build_non_monotonic_sum("mysql.replica.time_behind_source", "s", lag_s, start))
    metrics.append(build_non_monotonic_sum("mysql.replica.sql_delay", "s", lag_s, start))

    # ── Table metrics: size/rows + I/O waits ──────────────────────────
    for db, db_tables in tables.items():
        for tbl in db_tables[:2]:
            data_size = rng.randint(1_000_000, 500_000_000)
            index_size = rng.randint(100_000, data_size // 2)
            row_count = rng.randint(1000, 10_000_000)
            tbl_attrs_data = {"kind": "data", "schema": db, "table": tbl}
            tbl_attrs_idx = {"kind": "index", "schema": db, "table": tbl}
            metrics.append(build_gauge("mysql.table.size", "By", data_size, tbl_attrs_data))
            metrics.append(build_gauge("mysql.table.size", "By", index_size, tbl_attrs_idx))
            metrics.append(build_gauge("mysql.table.rows", "1", row_count, tbl_attrs_data))

            # Table I/O wait metrics — cumulative sums, ops: delete|fetch|insert|update
            for op in TABLE_IO_OPERATIONS:
                key = (db, tbl, op)
                if key not in state.table_io_wait_count:
                    state.table_io_wait_count[key] = rng.randint(0, 1_000)
                    state.table_io_wait_time[key] = rng.randint(0, 50_000_000)
                state.table_io_wait_count[key] += rng.randint(1, 500)
                state.table_io_wait_time[key] += rng.randint(1_000_000, 50_000_000)
                io_attrs = {"operation": op, "schema": db, "table": tbl}
                metrics.append(
                    build_cumulative_sum(
                        "mysql.table.io.wait.count", "1", state.table_io_wait_count[key], start, io_attrs,
                    )
                )
                metrics.append(
                    build_cumulative_sum(
                        "mysql.table.io.wait.time", "ns", state.table_io_wait_time[key], start, io_attrs,
                    )
                )

    return metrics



from generators.mysql_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run MySQL generator loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        db_prefix = ns.replace("-", "_")
    else:
        ns = NAMESPACE
        db_prefix = NAMESPACE.replace("-", "_")

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}

    databases = [
        ScenarioIdentity(ns).database("telemetry"),
        ScenarioIdentity(ns).database("mission"),
        ScenarioIdentity(ns).database("sensors"),
        ScenarioIdentity(ns).database("audit"),
    ]
    tables = {
        ScenarioIdentity(ns).database("telemetry"): ["telemetry_readings", "sensor_data", "metric_snapshots", "log_entries", "trace_spans"],
        ScenarioIdentity(ns).database("mission"): ["mission_events", "countdown_phases", "launch_parameters", "abort_criteria", "hold_records"],
        ScenarioIdentity(ns).database("sensors"): ["sensor_calibrations", "sensor_registry", "calibration_epochs", "sensor_thresholds", "validation_results"],
        ScenarioIdentity(ns).database("audit"): ["remediation_log", "escalation_log", "agent_actions", "operator_decisions", "safety_assessments"],
    }

    log_resource = _build_log_resource(ns)
    metrics_resource = _build_metrics_resource(ns)
    trace_resource = _build_trace_resource(ns)

    _emit_traces = True
    if scenario_data and "services" in scenario_data:
        _emit_traces = "mysql-primary" in scenario_data["services"]

    metric_state = MySQLMetricState(rng)

    _reset_row_locks_gauge_mapping()

    total_sent = 0
    total_spans = 0
    scrape_count = 0

    logger.info(
        "MySQL generator started (namespace=%s, db_prefix=%s, chaos_aware=%s)",
        ns, db_prefix, chaos_controller is not None,
    )

    sleep_time = None
    def _emit_tick() -> None:
        nonlocal scrape_count, sleep_time, total_sent, total_spans
        try:
            batch_size = rng.randint(BATCH_SIZE_MIN, BATCH_SIZE_MAX)

            chaos_mode = (scenario_data or {}).get("generator_config", {}).get("chaos", "any") if scenario_data else "any"
            if not scenario_data:
                chaos_mode = "any"
            cfg = scenario_data.get("generator_config", {}) if scenario_data else {}
            chaos_mode = cfg.get("chaos", "any")
            active_chaos = resolve_active_chaos(
                chaos_controller=chaos_controller,
                channel_registry=_channel_registry,
                generator_id="mysql_otel",
                chaos_mode=chaos_mode,
                impact_keys=_ALERT_SPEC.impact_keys,
            )
            # db.server.query_sample events + correlated trace spans
            sample_records = []
            spans = []
            for _ in range(batch_size):
                # During active chaos, ~50% of sampled queries reflect the fault
                req_chaos = active_chaos if (active_chaos and rng.random() < 0.5) else None
                log_record, span = _generate_query_sample_event(
                    client, rng, databases, tables, db_prefix, ns,
                    active_chaos=req_chaos,
                )
                sample_records.append(log_record)
                spans.append(span)

            scope = {"name": MYSQL_SCOPE, "version": SCOPE_VERSION}
            client.send_logs_scoped(log_resource, scope, sample_records)

            if spans and _emit_traces:
                client.send_traces(trace_resource, spans)
                total_spans += len(spans)

            # db.server.top_query events (fewer, represent digest summaries)
            top_count = rng.randint(1, 4)
            top_records = [
                _generate_top_query_event(client, rng, databases, tables, db_prefix)
                for _ in range(top_count)
            ]
            client.send_logs_scoped(log_resource, scope, top_records)

            # MySQL metrics (emit every other cycle to reduce volume)
            scrape_count += 1
            if scrape_count % 2 == 0:
                metrics = _generate_mysql_metrics(metric_state, tables, rng, active_chaos)
                client.send_metrics_scoped(metrics_resource, scope, metrics)

            total_sent += batch_size + top_count
            logger.info(
                "Sent %d query_sample + %d top_query events, %d spans (total=%d)",
                batch_size, top_count, len(spans), total_sent,
            )
        except Exception:
            logger.exception("MySQL generator tick failed; continuing")

        sleep_time = rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX)
    run_tick_loop(stop_event, _emit_tick, interval=lambda: sleep_time)

    logger.info("MySQL generator stopped. Total: %d events, %d spans", total_sent, total_spans)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    client = OTLPClient()
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    duration = int(os.environ.get("RUN_DURATION", "60"))
    timer = threading.Timer(duration, stop_event.set)
    timer.daemon = True
    timer.start()
    logger.info("Running for %ds (standalone mode)", duration)

    run(client, stop_event)
    timer.cancel()
    client.close()


if __name__ == "__main__":
    main()
