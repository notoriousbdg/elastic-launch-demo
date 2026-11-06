#!/usr/bin/env python3
"""PostgreSQL Generator — sends postgresqlreceiver OTel events and metrics via OTLP.

Emits data matching the postgresql_otel content package
(discovery.datasets: postgresqlreceiver.otel), which populates the bundled
Overview, Connections, IO Health, Locks, Query Performance, Workload, and
Active Queries dashboards.

Signal types:
  - Metrics: postgresql.backends, postgresql.commits, postgresql.rollbacks,
             postgresql.db_size, postgresql.deadlocks, postgresql.rows,
             postgresql.bgwriter.*, postgresql.blks_hit, postgresql.blks_read,
             postgresql.database.count, postgresql.database.locks,
             postgresql.temp_files, postgresql.temp.io, postgresql.connection.max,
             postgresql.tup_*, postgresql.calls, postgresql.total_exec_time
  - Logs: db.server.query_sample, db.server.top_query
  - Traces: correlated DB spans (optional, when service_name is in the scenario)

Field names and attribute keys come from:
  - opentelemetry-collector-contrib/receiver/postgresqlreceiver/metadata.yaml
  - elastic/integrations/packages/postgresql_otel/kibana/dashboard/*.json

Dashboard contracts that this generator must satisfy:
  - resource.attributes.postgresql.database.name on per-database metrics
  - attributes.lock_type / mode / relation on postgresql.database.locks
  - postgresql.temp_files and postgresql.temp.io as monotonic cumulative counters
  - attributes.type on postgresql.bgwriter.duration is sync|write
  - query_sample logs include wait_event, wait_event_type, network.peer.address

Usage (standalone):
    python3 -m generators.postgresql_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/postgresql_otel@0.5.0"

import logging
import os
import random
import secrets
import signal
import threading

from app.telemetry import OTLPClient, SCHEMA_URL, _format_attributes, _now_ns
from app.config import NAMESPACE
from app.trace_context import _trace_context_store
from generator_engine.db_common import (
    impact_value,
    CounterState,
    build_cumulative_sum,
    build_gauge,
    build_signal_resource,
    business_load,
    resolve_active_chaos,
)

# ── Grounding constants — do not change names ─────────────────────────────────

METRIC_NAMES = (
    "postgresql.backends",
    "postgresql.commits",
    "postgresql.rollbacks",
    "postgresql.db_size",
    "postgresql.deadlocks",
    "postgresql.rows",
    "postgresql.bgwriter.buffers.writes",
    "postgresql.bgwriter.duration",
    "postgresql.bgwriter.maxwritten",
    "postgresql.blks_hit",
    "postgresql.blks_read",
    "postgresql.database.count",
    "postgresql.database.locks",
    "postgresql.temp_files",
    "postgresql.temp.io",
    "postgresql.connection.max",
    "postgresql.tup_deleted",
    "postgresql.tup_fetched",
    "postgresql.tup_inserted",
    "postgresql.tup_returned",
    "postgresql.tup_updated",
    "postgresql.calls",
    "postgresql.total_exec_time",
    "postgresql.sequential_scans",
    "postgresql.wal.lag",
    "postgresql.replication.data_delay",
)

LOG_ATTRIBUTES = (
    "db.query.text",
    "db.system.name",
    "db.namespace",
    "db.collection.name",
    "postgresql.database.name",
    "postgresql.rolname",
    "postgresql.pid",
    "postgresql.application_name",
    "postgresql.state",
    "postgresql.wait_event",
    "postgresql.wait_event_type",
    "postgresql.total_exec_time",
    "postgresql.rows",
    "postgresql.calls",
    "network.peer.address",
    "user.name",
)

SPAN_KIND_CLIENT = 3
STATUS_OK = 1
STATUS_ERROR = 2

PG_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/postgresqlreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("postgresql-generator")

BATCH_INTERVAL_MIN = 2
BATCH_INTERVAL_MAX = 5
BATCH_SIZE_MIN = 2
BATCH_SIZE_MAX = 10

BGWRITER_BUFFER_TYPES = ["bgwriter", "backend", "backend_fsync", "checkpoints"]
BGWRITER_DURATION_TYPES = ["sync", "write"]  # metadata.yaml bg_duration_type
ROW_TYPES = ["dead", "live"]

# pg_locks.locktype / mode values used by the Locks dashboard
LOCK_TYPES = ["relation", "tuple", "transactionid", "virtualxid", "object", "advisory"]
LOCK_MODES = [
    "AccessShareLock",
    "RowShareLock",
    "RowExclusiveLock",
    "ShareUpdateExclusiveLock",
    "ShareLock",
    "ShareRowExclusiveLock",
    "ExclusiveLock",
    "AccessExclusiveLock",
]

WAIT_EVENT_TYPES = ["Lock", "LWLock", "Client", "Activity", "IO"]
# "Queries waiting on locks" alert fires on any wait_event_type == "Lock" row
# with blocked_count > 0. Never pick Lock outside the dedicated chaos branch.
HEALTHY_WAIT_EVENT_TYPES = [t for t in WAIT_EVENT_TYPES if t != "Lock"]
WAIT_EVENTS = {
    "Lock": ["relation", "tuple", "transactionid"],
    "LWLock": ["buffer_content", "wal_write", "shmem_index"],
    "Client": ["ClientRead", "ClientWrite"],
    "Activity": ["LogicalLauncherMain", "AutoVacuumMain"],
    "IO": ["BufFileRead", "BufFileWrite", "DataFileRead"],
}
STATES = ["active", "idle", "idle in transaction", "waiting"]
APPLICATION_NAMES = [
    "pgbouncer", "pg_dump", "application", "monitoring", "analytics", "migration"
]
CLIENT_ADDRESSES = [
    "10.0.1.12", "10.0.1.34", "10.0.2.8", "10.0.3.21", "10.0.4.55", "172.16.0.10",
]

# Default query templates.  The scenario can override with its own SQL.
DEFAULT_QUERY_TEMPLATES = [
    ("SELECT", "SELECT id, status, created_at FROM {table} WHERE status = $1 AND created_at > NOW() - INTERVAL '5 minutes' ORDER BY created_at DESC LIMIT 100"),
    ("SELECT", "SELECT t.id, COUNT(r.id) as count, AVG(r.value) as avg_val FROM {table} t LEFT JOIN related r ON t.id = r.ref_id WHERE t.status = 'active' GROUP BY t.id HAVING COUNT(r.id) > 5"),
    ("INSERT", "INSERT INTO {table} (status, payload, created_at) VALUES ($1, $2, NOW()) RETURNING id"),
    ("UPDATE", "UPDATE {table} SET status = $1, updated_at = NOW() WHERE id = $2 AND status != $1"),
    ("DELETE", "DELETE FROM {table} WHERE created_at < NOW() - INTERVAL '30 days' AND status = 'archived'"),
    ("SELECT", "SELECT pg_total_relation_size('{table}') AS total_size, pg_relation_size('{table}') AS table_size"),
    ("SELECT", "SELECT rolname, application_name, state, query, NOW() - query_start AS duration FROM pg_stat_activity WHERE state != 'idle' AND query_start < NOW() - INTERVAL '30 seconds' ORDER BY duration DESC"),
    ("SELECT", "WITH slow AS (SELECT id, payload FROM {table} WHERE status = 'pending' ORDER BY created_at LIMIT 1000 FOR UPDATE SKIP LOCKED) UPDATE {table} SET status = 'processing' FROM slow WHERE {table}.id = slow.id RETURNING {table}.id"),
]


class PostgreSQLMetricState(CounterState):
    """Tracks cumulative counter values for a PostgreSQL instance."""

    def __init__(self, rng: random.Random, n_databases: int = 4):
        super().__init__(rng)
        n = n_databases
        self.commits = {db: rng.randint(100_000, 5_000_000) for db in range(n)}
        # OOTB rollback-rate alert is SUM(rollbacks)/SUM(commits+rollbacks) > 1%.
        # Keep rollbacks at 0 absent chaos so the ratio stays 0.
        self.rollbacks = {db: 0 for db in range(n)}
        # OOTB alert is SUM(postgresql.deadlocks) > 0 — must stay flat at 0
        # absent chaos (it's an absolute-value check, not INCREASE/RATE).
        self.deadlocks = {db: 0 for db in range(n)}
        # Seed far above prior-series tips so SUM(hit)/SUM(hit+read) in the
        # 15m OOTB window is dominated by the healthy ratio after a restart.
        self.blks_hit = {db: rng.randint(20_000_000_000, 40_000_000_000) for db in range(n)}
        self.blks_read = {
            db: max(1, int(self.blks_hit[db] * rng.uniform(0.002, 0.003)))
            for db in range(n)
        }
        self.tup_inserted = {db: rng.randint(100_000, 10_000_000) for db in range(n)}
        self.tup_updated = {db: rng.randint(100_000, 5_000_000) for db in range(n)}
        self.tup_deleted = {db: rng.randint(10_000, 1_000_000) for db in range(n)}
        self.tup_fetched = {db: rng.randint(1_000_000, 100_000_000) for db in range(n)}
        self.tup_returned = {db: rng.randint(1_000_000, 200_000_000) for db in range(n)}
        self.calls = {db: rng.randint(100_000, 10_000_000) for db in range(n)}
        # OOTB alerts SUM absolute temp_files / temp.io / seq_scans / maxwritten
        # over the rule window — any nonzero seed fires. Keep at 0 absent chaos.
        self.temp_files = {db: 0 for db in range(n)}
        self.temp_io = {db: 0 for db in range(n)}
        # OOTB alert: SUM(bgwriter.buffers.writes) WHERE source == "backend" > 100.
        # Only the backend_buffer_writes_high fault should grow this source.
        self.bgwriter_writes = {
            t: (0 if t == "backend" else rng.randint(10_000, 1_000_000))
            for t in BGWRITER_BUFFER_TYPES
        }
        # OOTB alert: SUM(bgwriter.duration) > 30000 (ms). Start at 0 and keep
        # ambient per-tick growth tiny so the window sum stays well under 30s.
        self.bgwriter_duration = {t: 0.0 for t in BGWRITER_DURATION_TYPES}
        self.bgwriter_maxwritten = 0
        self.sequential_scans = {db: 0 for db in range(n)}

    def tick(self, rng: random.Random, n_databases: int, active_chaos: dict | None = None):
        chaos = active_chaos or {}
        deadlock_rate = float(impact_value(chaos, "deadlocks_detected", default=0.0))
        rollback_fraction = float(impact_value(chaos, "high_rollback_rate", default=0.0))
        seq_delta = int(impact_value(chaos, "high_sequential_scans", default=0))
        # Prefer the active channel key first — SPEC/GT often fills sibling
        # defaults with 0.0, and impact_value returns the first present key.
        temp_files_delta = int(impact_value(chaos, "high_temp_i_o_volume", "temp_files_created", default=0) or 0)
        # Derive temp IO bump from the same SPEC knobs (bytes per file delta).
        temp_io_delta = temp_files_delta * 100_000
        maxwritten_delta = int(impact_value(chaos, "bgwriter_maxwritten_stops", default=0))
        backend_writes_delta = int(impact_value(chaos, "backend_buffer_writes_high", default=0))
        checkpoint_ms = float(impact_value(chaos, "long_checkpoint_duration", default=0.0))

        # Hit-ratio target for this tick. SPEC/GT may pass 0 (healthy), a ratio
        # in (0,1], a percent (1–100], or a severity >100 meaning "force a miss".
        raw_hit = float(impact_value(chaos, "low_buffer_hit_ratio", default=0.0) or 0.0)
        if raw_hit <= 0.0:
            hit_ratio = 0.995
        elif raw_hit <= 1.0:
            hit_ratio = min(raw_hit, 0.98)
        elif raw_hit <= 100.0:
            hit_ratio = min(raw_hit / 100.0, 0.98)
        else:
            hit_ratio = 0.90

        for db in range(n_databases):
            base = rng.randint(50, 500)
            self.commits[db] += base
            fault_rollbacks = int(base * rollback_fraction)
            self.rollbacks[db] += fault_rollbacks
            extra_deadlocks = int(rng.uniform(0, max(deadlock_rate, 0.0) * 10)) if deadlock_rate else 0
            self.deadlocks[db] += extra_deadlocks
            # Drive hit ratio via blks_hit / blks_read tips.
            # OOTB alert SUMs absolute counter snapshots then
            # ``hit/(hit+read)*100 < 99``. Healthy tips seed in the tens of
            # billions, so chaos must either reset to a huge low-ratio tip that
            # dominates the windowed SUM, or the lookback still stays healthy.
            if raw_hit > 0.0:
                total_blks = 1_000_000_000_000  # swamp healthy tips in SUM()
                hit = max(1, int(total_blks * min(max(hit_ratio, 0.0), 1.0)))
                read = max(1, total_blks - hit)
                self.blks_hit[db] = hit
                self.blks_read[db] = read
            else:
                # After a swamp-tip fault, re-seed healthy tips so the windowed
                # ratio can clear (accumulating onto a 90% 1e12 tip never recovers).
                hit0, read0 = self.blks_hit[db], self.blks_read[db]
                if read0 > 0 and hit0 / (hit0 + read0) < 0.99:
                    self.blks_hit[db] = rng.randint(20_000_000_000, 40_000_000_000)
                    self.blks_read[db] = max(1, int(self.blks_hit[db] * rng.uniform(0.002, 0.003)))
                total_blks = rng.randint(10_000, 200_000)
                hit = max(0, int(total_blks * min(max(hit_ratio, 0.0), 1.0)))
                read = max(0, total_blks - hit)
                self.blks_hit[db] += hit
                self.blks_read[db] += read
            self.tup_inserted[db] += rng.randint(10, 500)
            self.tup_updated[db] += rng.randint(20, 300)
            self.tup_deleted[db] += rng.randint(0, 50)
            self.tup_fetched[db] += rng.randint(1_000, 20_000)
            self.tup_returned[db] += rng.randint(1_000, 50_000)
            self.calls[db] += rng.randint(50, 500)
            # Absolute-SUM OOTB alerts (temp files / temp.io / seq scans) fire on
            # any nonzero snapshot in the rule window. Reset to 0 when the fault
            # ends so rematerialized healthy docs can clear (stream wipe alone
            # is not enough if the counter tip stays elevated).
            if temp_files_delta > 0:
                self.temp_files[db] += temp_files_delta
                self.temp_io[db] += temp_io_delta
            else:
                self.temp_files[db] = 0
                self.temp_io[db] = 0
            if seq_delta > 0:
                self.sequential_scans[db] += seq_delta
            else:
                self.sequential_scans[db] = 0
        for t in BGWRITER_BUFFER_TYPES:
            if t == "backend":
                # Ambient traffic never triggers direct backend writes; only
                # the backend_buffer_writes_high fault should bump this source.
                # Reset to 0 when healthy so SUM(absolute) clears after resolve.
                if backend_writes_delta > 0:
                    self.bgwriter_writes[t] += backend_writes_delta
                else:
                    self.bgwriter_writes[t] = 0
            else:
                self.bgwriter_writes[t] += rng.randint(10, 200)
        # OOTB alert is SUM(absolute duration samples) > 30000 over the window.
        # Store per-interval duration (gauge semantics) so healthy stays tiny
        # and chaos (checkpoint_ms) can clear when the fault ends.
        n_types = max(1, len(BGWRITER_DURATION_TYPES))
        for t in BGWRITER_DURATION_TYPES:
            if checkpoint_ms > 0:
                self.bgwriter_duration[t] += checkpoint_ms / n_types
            else:
                self.bgwriter_duration[t] += rng.uniform(0.5, 5.0)
        # Same absolute-SUM pattern as backend writes / temp files.
        if maxwritten_delta > 0:
            self.bgwriter_maxwritten += maxwritten_delta
        else:
            self.bgwriter_maxwritten = 0


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_rds",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


def _build_resources(ns: str, cfg: dict) -> tuple[dict, dict, dict]:
    """Build log, base metrics, and trace resources from config."""
    ident = ScenarioIdentity(ns, cfg)
    service_name = ident.service("postgresql-primary")
    host_name = ident.host("postgresql-host")
    cloud = cfg.get("cloud") or _default_cloud()
    if isinstance(cloud, dict) and "provider" not in cloud:
        cloud = _default_cloud()

    common = dict(
        ns=ns,
        service_name=service_name,
        service_version="14.10",
        service_instance_id=ident.instance(service_name),
        host_name=host_name,
        cloud=cloud,
    )
    log_res = build_signal_resource(signal_type="logs", dataset="postgresqlreceiver", **common)
    met_res = build_signal_resource(signal_type="metrics", dataset="postgresqlreceiver", **common)
    trc_res = build_signal_resource(signal_type="traces", dataset="generic", **common)
    return log_res, met_res, trc_res


def _db_metrics_resource(base_resource: dict, database: str) -> dict:
    """Clone the metrics resource and add postgresql.database.name.

    The postgresql_otel dashboards group and filter by
    ``resource.attributes.postgresql.database.name`` (classic receiver field shape).
    """
    attrs = list(base_resource.get("attributes", []))
    # Drop any existing copy so re-emits stay idempotent.
    attrs = [a for a in attrs if a.get("key") != "postgresql.database.name"]
    attrs.append({"key": "postgresql.database.name", "value": {"stringValue": database}})
    return {
        "attributes": attrs,
        "schemaUrl": base_resource.get("schemaUrl", SCHEMA_URL),
    }


# Earlier generator revisions emitted temp_files / temp.io as gauges.  Once ES
# maps those fields as time_series_metric=gauge, INCREASE/RATE dashboard panels
# fail even after the generator switches to cumulative counters — the mapping
# is sticky on the backing index.  Reset the stream when we detect that.
_TEMP_COUNTER_FIELDS = ("postgresql.temp_files", "postgresql.temp.io", "postgresql.bgwriter.duration")
_METRICS_DATA_STREAM = "metrics-postgresqlreceiver.otel-default"


def _temp_fields_mapped_as_gauge(elastic_url: str, api_key: str) -> bool:
    """Return True if any temp_* field is still mapped as a TSDB gauge."""
    import httpx

    url = (
        f"{elastic_url.rstrip('/')}/{_METRICS_DATA_STREAM}/_field_caps"
        f"?fields={','.join(_TEMP_COUNTER_FIELDS)}"
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
    for name in _TEMP_COUNTER_FIELDS:
        for meta in fields.get(name, {}).values():
            if meta.get("time_series_metric") == "gauge":
                return True
    return False


def _reset_temp_gauge_mapping() -> None:
    """Delete the metrics data stream if temp_* fields are stuck as gauges.

    Fresh counter samples then recreate the stream with
    ``time_series_metric: counter``, unblocking Overview / Query Performance
    INCREASE/RATE panels.  No-op when ES credentials are absent or mapping is
    already correct.
    """
    try:
        from app.telemetry import ESBulkClient
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for temp_* mapping check: %s", exc)
        return

    client = ESBulkClient()
    if not client.configured:
        return

    if not _temp_fields_mapped_as_gauge(client.elastic_url, client.api_key):
        logger.info(
            "postgresql temp_* fields are not mapped as gauges; leaving %s alone",
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
            "Deleted %s so temp_files/temp.io remap as counters (was gauge)",
            _METRICS_DATA_STREAM,
        )
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist", _METRICS_DATA_STREAM)
    else:
        logger.warning(
            "Could not delete %s: %s %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )


def _generate_query_sample(
    client: OTLPClient,
    rng: random.Random,
    databases: list[str],
    tables: dict[str, list[str]],
    query_templates: list,
    cfg: dict,
    ns: str,
    active_chaos: dict | None = None,
    session: dict | None = None,
) -> tuple[dict, dict]:
    """Generate a db.server.query_sample log record and a correlated trace span.

    ``session`` replays one of the stuck long-running sessions from
    ``_stuck_sessions`` instead of drawing a fresh query.
    """
    session = session or {}
    db = session.get("db") or rng.choice(databases)
    table = session.get("table") or rng.choice(tables.get(db, ["records"]))
    operation, template = session.get("template") or rng.choice(query_templates)
    query = template.format(table=table)
    rolname = session.get("rolname") or rng.choice(cfg.get("users", ["app_user", "reporting"]))
    pid = session.get("pid") or rng.randint(10000, 99999)
    app_name = session.get("app_name") or rng.choice(APPLICATION_NAMES)

    chaos = active_chaos or {}
    slow_fraction = float(impact_value(chaos, "slow_top_queries", default=0.0))
    long_ms = session.get("exec_ms", 0)
    lock_wait_fraction = float(impact_value(chaos, "queries_waiting_on_locks", default=0.0))
    is_slow = rng.random() < slow_fraction
    if long_ms > 0:
        wait_ns = long_ms * 1_000_000
    elif is_slow:
        wait_ns = rng.randint(10_001 * 1_000_000, 50_000 * 1_000_000)
    else:
        wait_ns = rng.randint(1_000_000, 100_000_000)

    total_exec_ms = max(1, wait_ns // 1_000_000)
    rows = rng.randint(0, 10_000)

    # Active Queries table KEEP's wait_event_type / wait_event — empty strings
    # render as (null). Always emit realistic pg_stat_activity wait fields.
    if lock_wait_fraction and rng.random() < lock_wait_fraction:
        state = "waiting"
        wait_event_type = "Lock"
        wait_event = rng.choice(WAIT_EVENTS.get("Lock", ["relation", "tuple"]))
    else:
        state = rng.choice(["active", "idle in transaction", "idle", "waiting"])
        if state in ("idle", "idle in transaction"):
            # Idle backends wait on the client (standard PostgreSQL semantics).
            wait_event_type = "Client"
            wait_event = rng.choice(WAIT_EVENTS["Client"])
        elif state == "waiting" or rng.random() < 0.45:
            wait_event_type = rng.choice(HEALTHY_WAIT_EVENT_TYPES)
            wait_event = rng.choice(WAIT_EVENTS.get(wait_event_type, ["Unknown"]))
        else:
            # Active / running on CPU — still populate for the table; IO is common.
            wait_event_type = "IO"
            wait_event = rng.choice(WAIT_EVENTS["IO"])

    # Prefer a real trace ID from a running transaction
    trace_id = None
    span_id = None
    _ns = ns or ""
    if active_chaos:
        for svc in active_chaos.get("affected_services", []):
            t, s = _trace_context_store.get(svc, namespace=_ns)
            if t and s:
                trace_id, span_id = t, s
                break
    if trace_id is None:
        trace_id = secrets.token_hex(16)
        span_id = secrets.token_hex(8)

    attrs = {
        "db.query.text": query,
        "db.system.name": "postgresql",
        "db.namespace": db,
        "db.collection.name": table,
        "postgresql.database.name": db,
        "postgresql.rolname": rolname,
        "postgresql.pid": pid,
        "postgresql.application_name": app_name,
        "postgresql.state": state,
        "postgresql.wait_event_type": wait_event_type,
        "postgresql.wait_event": wait_event,
        "postgresql.total_exec_time": float(total_exec_ms),
        "postgresql.rows": rows,
        "network.peer.address": rng.choice(CLIENT_ADDRESSES),
        "user.name": rolname,
    }
    if active_chaos:
        attrs["chaos.channel"] = active_chaos["channel_id"]
        if active_chaos.get("name"):
            attrs["chaos.fault_type"] = active_chaos["name"]

    log_record = client.build_log_record(
        severity="INFO" if not active_chaos else "WARN",
        body=query,
        attributes=attrs,
        trace_id=trace_id,
        span_id=span_id,
        event_name="db.server.query_sample",
    )

    status_code = STATUS_ERROR if (active_chaos and is_slow) else STATUS_OK
    span_attrs = {
        "db.system.name": "postgresql",
        "db.namespace": db,
        "db.collection.name": table,
        "db.query.text": query,
        "db.operation.name": operation,
    }
    if active_chaos:
        span_attrs["chaos.channel"] = active_chaos["channel_id"]

    span = client.build_span(
        name=f"{operation} {db}.{table}",
        trace_id=trace_id,
        span_id=span_id,
        kind=SPAN_KIND_CLIENT,
        duration_ms=max(1, total_exec_ms),
        status_code=status_code,
        attributes=span_attrs,
    )
    return log_record, span


def _generate_top_query(
    client: OTLPClient,
    rng: random.Random,
    databases: list[str],
    tables: dict[str, list[str]],
    query_templates: list,
    cfg: dict,
    digest: dict | None = None,
) -> dict:
    """Generate a db.server.top_query log record (digest-level stats).

    ``digest`` replays one of the slow digests from ``_slow_digests``; without it
    the record stays under the OOTB Slow top queries bar.
    """
    digest = digest or {}
    db = digest.get("db") or rng.choice(databases)
    table = digest.get("table") or rng.choice(tables.get(db, ["records"]))
    _, template = digest.get("template") or rng.choice(query_templates)
    query = template.format(table=table)
    rolname = digest.get("rolname") or rng.choice(cfg.get("users", ["app_user"]))
    top_exec = digest.get("exec_ms") or float(rng.randint(1_000, 8_000))

    attrs = {
        "db.query.text": query,
        "db.system.name": "postgresql",
        "db.namespace": db,
        "postgresql.database.name": db,
        "postgresql.rolname": rolname,
        "postgresql.calls": digest.get("calls") or rng.randint(100, 50_000),
        "postgresql.total_exec_time": top_exec,
        "postgresql.rows": rng.randint(0, 1_000_000),
        "user.name": rolname,
    }
    return client.build_log_record(
        severity="INFO",
        body=query,
        attributes=attrs,
        event_name="db.server.top_query",
    )


def _stuck_sessions(
    rng: random.Random,
    databases: list[str],
    tables: dict[str, list[str]],
    query_templates: list,
    cfg: dict,
    long_ms: float,
) -> list[dict]:
    """The sessions stuck on long-running queries for the life of one fault.

    The OOTB Long-running queries rule keys each alert on the whole row (instance,
    database, query text, total_exec_time, pid), so these values must not change
    between scrapes or every rule run opens new alerts and recovers the old ones.
    The rule filters ``total_exec_time > 300000``; smaller probes are lifted over it.
    """
    exec_ms = int(long_ms) if long_ms > 300_000 else 300_001
    sessions = []
    for _ in range(2):
        db = rng.choice(databases)
        sessions.append({
            "db": db,
            "table": rng.choice(tables.get(db, ["records"])),
            "template": rng.choice(query_templates),
            "rolname": rng.choice(cfg.get("users", ["app_user", "reporting"])),
            "pid": rng.randint(10000, 99999),
            "app_name": rng.choice(APPLICATION_NAMES),
            "exec_ms": exec_ms,
        })
    return sessions


def _slow_digests(
    rng: random.Random,
    databases: list[str],
    tables: dict[str, list[str]],
    query_templates: list,
    cfg: dict,
    slow_probe: float,
) -> list[dict]:
    """The slow query digests for the life of one fault.

    The OOTB Slow top queries rule keys each alert on the whole row (instance,
    database, query text, total_exec_time, calls) and filters
    ``total_exec_time > 10000``. A fraction probe (0–1) picks a breaching time;
    an absolute ms probe is used, lifted over the bar.
    """
    digests = []
    for _ in range(2):
        db = rng.choice(databases)
        if slow_probe <= 1.0:
            exec_ms = float(rng.randint(10_001, 50_000))
        else:
            exec_ms = float(round(max(slow_probe, 10_001) * rng.uniform(1.0, 2.0)))
        digests.append({
            "db": db,
            "table": rng.choice(tables.get(db, ["records"])),
            "template": rng.choice(query_templates),
            "rolname": rng.choice(cfg.get("users", ["app_user"])),
            "calls": rng.randint(100, 50_000),
            "exec_ms": exec_ms,
        })
    return digests


def _allocate_connection_budget(
    state: PostgreSQLMetricState,
    n_databases: int,
    rng: random.Random,
    active_chaos: dict | None = None,
) -> None:
    """Set one instance max_conn and partition backends across databases.

    Connections Utilization is ``SUM(backends) / MAX(connection.max)``.  Each
    DB must not independently claim a full fraction of max_conn or the panel
    reads >100% (e.g. 175% with multiple databases).
    """
    n = max(1, n_databases)
    chaos = active_chaos or {}
    conn_util = float(impact_value(chaos, "connection_utilization_high", default=0.0))
    # OOTB: EVAL utilization_pct = ROUND(total_backends / conn_max * 100.0, 2)
    # ES|QL integer-divides SUM/MAX first, so util stays 0.0 until
    # SUM(backends) > MAX(connection.max). Keep a stable modest max_conn and
    # breach by pushing windowed SUM just over that ceiling (not by shrinking
    # max during chaos — MAX() would still see any prior larger values).
    max_conn = 10_000
    if conn_util > 0:
        # Fraction (0.9) or percent-like (90) from chaos / tests.
        frac = conn_util / 100.0 if conn_util > 1.0 else conn_util
        _ = min(max(frac, 0.85), 0.95)  # retained for SPEC/probe semantics
        # ~3 emit cycles of 5k exceed max_conn → integer ratio >=1 → pct>=100.
        total_backends = 5_000
    else:
        # Keep windowed SUM well under max_conn (integer util stays 0).
        total_backends = max(1, int(40 * rng.uniform(0.2, 0.7)))
    weights = [rng.random() + 0.05 for _ in range(n)]
    weight_sum = sum(weights)
    backends_per_db = [int(total_backends * w / weight_sum) for w in weights]
    # Fix rounding so SUM(backends) == total_backends (<= max_conn).
    backends_per_db[0] += total_backends - sum(backends_per_db)
    state._last_max_conn = max_conn
    state._backends_per_db = backends_per_db


def _generate_db_metrics(
    state: PostgreSQLMetricState,
    db_index: int,
    database: str,
    tables: list[str],
    rng: random.Random,
    load: float,
    active_chaos: dict | None = None,
    *,
    n_databases: int | None = None,
) -> list[dict]:
    """Per-database metrics (resource carries postgresql.database.name)."""
    metrics: list[dict] = []
    # Datapoint attr used by metadata.yaml (db.namespace); resource attr is
    # postgresql.database.name for stock dashboard queries.
    ns_attrs = {"db.namespace": database}

    chaos = active_chaos or {}
    backends_per_db = getattr(state, "_backends_per_db", None)
    if not backends_per_db:
        _allocate_connection_budget(
            state, n_databases or (db_index + 1), rng, chaos,
        )
        backends_per_db = state._backends_per_db
    # Clamp index for unit tests that call a single DB without pre-alloc.
    backends = backends_per_db[min(db_index, len(backends_per_db) - 1)]
    metrics.append(build_gauge("postgresql.backends", "{connection}", backends, ns_attrs))

    metrics.append(build_cumulative_sum("postgresql.commits", "{transaction}", state.commits[db_index], state.start_time_ns, ns_attrs))
    metrics.append(build_cumulative_sum("postgresql.rollbacks", "{transaction}", state.rollbacks[db_index], state.start_time_ns, ns_attrs))
    metrics.append(build_cumulative_sum("postgresql.deadlocks", "{deadlock}", state.deadlocks[db_index], state.start_time_ns, ns_attrs))

    db_size = int(rng.randint(10_000_000, 10_000_000_000) * (1 + 0.01 * db_index))
    metrics.append(build_gauge("postgresql.db_size", "By", db_size, ns_attrs))

    for metric, counter in [
        ("postgresql.tup_inserted", state.tup_inserted[db_index]),
        ("postgresql.tup_updated", state.tup_updated[db_index]),
        ("postgresql.tup_deleted", state.tup_deleted[db_index]),
        ("postgresql.tup_fetched", state.tup_fetched[db_index]),
        ("postgresql.tup_returned", state.tup_returned[db_index]),
    ]:
        metrics.append(build_cumulative_sum(metric, "{tuple}", counter, state.start_time_ns, ns_attrs))

    metrics.append(build_cumulative_sum("postgresql.blks_hit", "{block}", state.blks_hit[db_index], state.start_time_ns, ns_attrs))
    metrics.append(build_cumulative_sum("postgresql.blks_read", "{block}", state.blks_read[db_index], state.start_time_ns, ns_attrs))

    for row_type in ROW_TYPES:
        count = rng.randint(100_000, 10_000_000)
        metrics.append(build_gauge("postgresql.rows", "{row}", count, {**ns_attrs, "state": row_type}))

    # Locks dashboard: BY attributes.lock_type / mode / relation
    relation_pool = tables or ["records"]
    exclusive_lock_count = int(impact_value(chaos, "exclusive_lock_contention", default=0))
    lock_combos = max(4, rng.randint(4, 10))
    for i in range(lock_combos):
        mode = rng.choice(LOCK_MODES)
        lock_count = rng.randint(0, 5)
        # OOTB alert: SUM(locks) WHERE mode IN (Exclusive*, AccessExclusive*) > 5.
        # Healthy traffic must never hold exclusive-mode locks.
        if mode in ("ExclusiveLock", "AccessExclusiveLock"):
            lock_count = 0
        if exclusive_lock_count and mode in ("ExclusiveLock", "AccessExclusiveLock"):
            lock_count = max(lock_count, exclusive_lock_count)
        elif exclusive_lock_count and i == 0:
            mode = "ExclusiveLock"
            lock_count = exclusive_lock_count
        lock_attrs = {
            **ns_attrs,
            "lock_type": rng.choice(LOCK_TYPES),
            "mode": mode,
            "relation": rng.choice(relation_pool),
        }
        metrics.append(build_gauge("postgresql.database.locks", "{lock}", lock_count, lock_attrs))

    # temp_files / temp.io must be counters for INCREASE/RATE panels
    metrics.append(build_cumulative_sum(
        "postgresql.temp_files", "{file}", state.temp_files[db_index], state.start_time_ns, ns_attrs,
    ))
    metrics.append(build_cumulative_sum(
        "postgresql.temp.io", "By", state.temp_io[db_index], state.start_time_ns, ns_attrs,
    ))

    metrics.append(build_cumulative_sum("postgresql.calls", "{query}", state.calls[db_index], state.start_time_ns, ns_attrs))
    slow_ms = float(impact_value(chaos, "long_running_queries", default=0) or 0)
    exec_time = float(rng.randint(10_000, 500_000))
    if slow_ms > 0:
        exec_time = max(exec_time, slow_ms * rng.uniform(1.0, 3.0))
    metrics.append(build_gauge("postgresql.total_exec_time", "ms", exec_time, ns_attrs))
    metrics.append(build_cumulative_sum(
        "postgresql.sequential_scans", "{scans}",
        state.sequential_scans[db_index], state.start_time_ns, ns_attrs,
    ))

    return metrics


def _generate_instance_metrics(
    state: PostgreSQLMetricState,
    n_databases: int,
    rng: random.Random,
    active_chaos: dict | None = None,
) -> list[dict]:
    """Instance-level metrics (no per-database resource attribute)."""
    metrics: list[dict] = []
    chaos = active_chaos or {}
    max_conn = getattr(state, "_last_max_conn", None) or (100 + rng.randint(0, 100))
    metrics.append(build_gauge("postgresql.connection.max", "{connection}", max_conn))
    metrics.append(build_gauge("postgresql.database.count", "{database}", n_databases))
    # No package alert reads replication lag, so there is no fault key for it.
    metrics.append(build_gauge("postgresql.wal.lag", "s", 0.0))
    metrics.append(build_gauge("postgresql.replication.data_delay", "By", 0))

    for buf_type in BGWRITER_BUFFER_TYPES:
        metrics.append(build_cumulative_sum(
            "postgresql.bgwriter.buffers.writes",
            "{buffer}",
            state.bgwriter_writes[buf_type],
            state.start_time_ns,
            {"source": buf_type},
        ))
    for dur_type in BGWRITER_DURATION_TYPES:
        # IO Health dashboard uses RATE(bgwriter.duration) → must be counter_long.
        metrics.append(build_cumulative_sum(
            "postgresql.bgwriter.duration",
            "ms",
            state.bgwriter_duration[dur_type],
            state.start_time_ns,
            {"type": dur_type},
        ))
    # maxwritten has no attributes in metadata.yaml
    metrics.append(build_cumulative_sum(
        "postgresql.bgwriter.maxwritten",
        "{checkpoint}",
        state.bgwriter_maxwritten,
        state.start_time_ns,
    ))
    return metrics



from generators.postgresql_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run PostgreSQL generator loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {})
    else:
        ns = NAMESPACE
        cfg = {}

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    # Build databases and tables from config or defaults
    db_prefix = ns.replace("-", "_")
    databases: list[str] = cfg.get("databases") or [
        ScenarioIdentity(ns).database("app"),
        ScenarioIdentity(ns).database("analytics"),
        ScenarioIdentity(ns).database("archive"),
        ScenarioIdentity(ns).database("audit"),
    ]
    default_tables = {db: ["records", "events", "log_entries", "sessions"] for db in databases}
    tables: dict[str, list[str]] = cfg.get("tables") or default_tables

    # Build query templates from config or defaults
    raw_templates = cfg.get("queries")
    if raw_templates and isinstance(raw_templates, list):
        query_templates = [
            (q.get("operation", "SELECT"), q.get("sql", DEFAULT_QUERY_TEMPLATES[0][1]))
            for q in raw_templates
            if isinstance(q, dict)
        ]
    else:
        query_templates = DEFAULT_QUERY_TEMPLATES

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("postgresql-primary")
    log_resource, metrics_resource, trace_resource = _build_resources(ns, cfg)

    # Gauge→counter remap for temp_* (sticky TSDB mapping from older builds).
    _reset_temp_gauge_mapping()

    _emit_traces = True
    if scenario_data and "services" in scenario_data:
        _emit_traces = service_name in scenario_data["services"]


    metric_state = PostgreSQLMetricState(rng, n_databases=len(databases))
    stuck: list[dict] = []
    slow: list[dict] = []
    scrape_count = 0
    total_sent = 0
    total_spans = 0

    logger.info(
        "PostgreSQL generator started (namespace=%s, chaos_mode=%s, databases=%s)",
        ns, chaos_mode, databases,
    )

    scope = {"name": PG_SCOPE, "version": SCOPE_VERSION}

    def _emit_tick() -> None:
        nonlocal scrape_count, total_sent, total_spans, stuck, slow
        batch_size = rng.randint(BATCH_SIZE_MIN, BATCH_SIZE_MAX)

        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="postgresql_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        long_ms = float(impact_value(active_chaos, "long_running_queries", default=0.0) or 0.0)
        slow_probe = float(impact_value(active_chaos, "slow_top_queries", default=0.0) or 0.0)
        if long_ms <= 0:
            stuck = []
        elif not stuck:
            stuck = _stuck_sessions(rng, databases, tables, query_templates, cfg, long_ms)
        if slow_probe <= 0:
            slow = []
        elif not slow:
            slow = _slow_digests(rng, databases, tables, query_templates, cfg, slow_probe)

        # db.server.query_sample logs + correlated spans
        sample_records = []
        spans = []
        sessions = [None] * batch_size + stuck
        for session in sessions:
            req_chaos = active_chaos if (active_chaos and (session or rng.random() < 0.5)) else None
            log_record, span = _generate_query_sample(
                client, rng, databases, tables, query_templates, cfg, ns,
                active_chaos=req_chaos, session=session,
            )
            sample_records.append(log_record)
            spans.append(span)

        client.send_logs_scoped(log_resource, scope, sample_records)

        if spans and _emit_traces:
            client.send_traces(trace_resource, spans)
            total_spans += len(spans)

        # db.server.top_query logs
        top_count = rng.randint(1, 4)
        top_records = [
            _generate_top_query(client, rng, databases, tables, query_templates, cfg, digest)
            for digest in [None] * top_count + slow
        ]
        client.send_logs_scoped(log_resource, scope, top_records)

        # Metrics every other cycle: one resourceMetrics per database so
        # resource.attributes.postgresql.database.name is set correctly.
        scrape_count += 1
        if scrape_count % 2 == 0:
            metric_state.tick(rng, len(databases), active_chaos)
            load = business_load()
            _allocate_connection_budget(
                metric_state, len(databases), rng, active_chaos,
            )
            for i, db in enumerate(databases):
                db_metrics = _generate_db_metrics(
                    metric_state, i, db, tables.get(db, ["records"]), rng, load, active_chaos,
                    n_databases=len(databases),
                )
                client.send_metrics_scoped(
                    _db_metrics_resource(metrics_resource, db), scope, db_metrics,
                )
            instance_metrics = _generate_instance_metrics(metric_state, len(databases), rng, active_chaos)
            client.send_metrics_scoped(metrics_resource, scope, instance_metrics)
            # Force a fresh budget on the next scrape.
            metric_state._backends_per_db = None

        total_sent += batch_size + top_count
        logger.debug(
            "Sent %d query_sample + %d top_query, %d spans (total=%d)",
            batch_size, top_count, len(spans), total_sent,
        )

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("PostgreSQL generator stopped. Total: %d events, %d spans", total_sent, total_spans)


def main() -> None:  # pragma: no cover
    """Standalone entry point for local testing."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "60"))

    def _stop(sig, frame):
        logger.info("Stopping...")
        stop_event.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    client = OTLPClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
