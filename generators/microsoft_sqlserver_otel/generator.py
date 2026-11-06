#!/usr/bin/env python3
"""SQL Server Generator — sends sqlserverreceiver OTel metrics and logs via OTLP.

Emits data matching the microsoft_sqlserver_otel content package
(discovery.datasets: sqlserverreceiver.otel), which populates the bundled
Overview, Database I/O, Concurrency & Errors, Memory Saturation, and
Query Performance dashboards.

Signal types:
  - Metrics: sqlserver.batch.request.rate, sqlserver.batch.sql_compilation.rate,
             sqlserver.batch.sql_recompilation.rate, sqlserver.deadlock.rate,
             sqlserver.login.rate, sqlserver.logout.rate,
             sqlserver.page.buffer_cache.hit_ratio, sqlserver.page.life_expectancy,
             sqlserver.processes.blocked, sqlserver.user.connection.count,
             sqlserver.database.io, sqlserver.database.latency,
             sqlserver.database.operations, sqlserver.database.backup_or_restore.rate,
             sqlserver.database.execution.errors, sqlserver.database.full_scan.rate,
             sqlserver.database.tempdb.space, sqlserver.database.tempdb.version_store.size,
             sqlserver.lock.timeout.rate, sqlserver.lock.wait.count,
             sqlserver.lock.wait.rate, sqlserver.lock.wait_time.avg,
             sqlserver.memory.grants.pending.count, sqlserver.memory.usage,
             sqlserver.os.wait.duration, sqlserver.page.buffer_cache.free_list.stalls.rate,
             sqlserver.page.lookup.rate, sqlserver.transaction.delay,
             sqlserver.transaction.rate, sqlserver.transaction.write.rate,
             sqlserver.replica.data.rate
  - Logs: db.server.query_sample, db.server.top_query

Field names and attribute keys come from:
  - opentelemetry-collector-contrib/receiver/sqlserverreceiver/metadata.yaml
  - elastic/integrations/packages/microsoft_sqlserver_otel/kibana/dashboard/*.json

Usage (standalone):
    python3 -m generators.microsoft_sqlserver_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/microsoft_sqlserver_otel@0.3.1"

import logging
import os
import random
import signal
import threading
import time
from typing import Any

from app.telemetry import ESBulkClient, OTLPClient, SCHEMA_URL
from app.config import NAMESPACE
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
    "sqlserver.batch.request.rate",
    "sqlserver.batch.sql_compilation.rate",
    "sqlserver.batch.sql_recompilation.rate",
    "sqlserver.deadlock.rate",
    "sqlserver.login.rate",
    "sqlserver.logout.rate",
    "sqlserver.page.buffer_cache.hit_ratio",
    "sqlserver.page.life_expectancy",
    "sqlserver.processes.blocked",
    "sqlserver.user.connection.count",
    "sqlserver.database.io",
    "sqlserver.database.latency",
    "sqlserver.database.operations",
    "sqlserver.database.backup_or_restore.rate",
    "sqlserver.database.execution.errors",
    "sqlserver.database.full_scan.rate",
    "sqlserver.database.tempdb.space",
    "sqlserver.database.tempdb.version_store.size",
    "sqlserver.lock.timeout.rate",
    "sqlserver.lock.wait.count",
    "sqlserver.lock.wait.rate",
    "sqlserver.lock.wait_time.avg",
    "sqlserver.memory.grants.pending.count",
    "sqlserver.memory.usage",
    "sqlserver.os.wait.duration",
    "sqlserver.page.buffer_cache.free_list.stalls.rate",
    "sqlserver.page.lookup.rate",
    "sqlserver.transaction.delay",
    "sqlserver.transaction.rate",
    "sqlserver.transaction.write.rate",
    "sqlserver.replica.data.rate",
)

# Log attribute keys verified against sqlserverreceiver/metadata.yaml
LOG_ATTRIBUTES = (
    "db.query.text",
    "db.system.name",
    "user.name",
    "db.namespace",
    "sqlserver.session_id",
    "sqlserver.query_hash",
    "sqlserver.request_status",
    "sqlserver.wait_type",
    "sqlserver.cpu_time",
    "sqlserver.logical_reads",
    "sqlserver.total_elapsed_time",
    "sqlserver.blocking_session_id",
    "sqlserver.reads",
    "sqlserver.writes",
    "sqlserver.row_count",
)

SQLSERVER_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/sqlserverreceiver"
)
SCOPE_VERSION = "0.115.0"

FILE_TYPES = ("data", "log")
IO_DIRECTIONS = ("read", "write")
TEMPDB_STATES = ("free", "version_store", "internal_objects", "user_objects", "mixed_extent")

# wait.type → wait.category (both attrs required on sqlserver.os.wait.duration)
WAIT_TYPES: list[tuple[str, str]] = [
    ("ASYNC_IO_COMPLETION", "IO"),
    ("CXPACKET", "CPU"),
    ("LCK_M_S", "Lock"),
    ("LCK_M_X", "Lock"),
    ("PAGEIOLATCH_SH", "IO"),
    ("PAGEIOLATCH_EX", "IO"),
    ("SOS_SCHEDULER_YIELD", "CPU"),
    ("WRITELOG", "TranLogIO"),
    ("HADR_WORK_QUEUE", "Replication"),
]

logger = logging.getLogger("sqlserver-generator")

BATCH_INTERVAL_MIN = 10
BATCH_INTERVAL_MAX = 20


def _default_cloud() -> dict:
    return {
        "provider": "azure",
        "platform": "azure_sql_db",
        "region": "eastus",
        "availability_zone": "1",
    }


class SQLServerMetricState(CounterState):
    """Cumulative counter state for SQL Server."""

    def __init__(self, rng: random.Random, databases: list[str]):
        super().__init__(rng)
        self.db_io = {
            (db, ft, d): rng.randint(1_000_000, 1_000_000_000)
            for db in databases
            for ft in FILE_TYPES
            for d in IO_DIRECTIONS
        }
        # Latency is cumulative seconds (double) — keep fractional precision.
        self.db_latency = {
            (db, ft, d): rng.uniform(10.0, 500.0)
            for db in databases
            for ft in FILE_TYPES
            for d in IO_DIRECTIONS
        }
        self.db_ops = {
            (db, ft, d): rng.randint(10_000, 10_000_000)
            for db in databases
            for ft in FILE_TYPES
            for d in IO_DIRECTIONS
        }
        self.lock_wait_count = rng.randint(0, 100_000)
        # Seed low; OOTB alert fires when INCREASE(sum) > 60s in the window.
        self.os_wait_duration = {wt: rng.uniform(1.0, 30.0) for wt, _ in WAIT_TYPES}

    def tick(self, rng: random.Random, databases: list[str], active_chaos: dict | None = None):
        load = business_load()
        chaos = active_chaos or {}
        io_raw = float(impact_value(chaos, "database_i_o_latency_high", default=0.0) or 0.0)
        wait_mult = float(impact_value(chaos, "os_wait_duration_high", default=0.0) or 0.0)
        for db in databases:
            for ft in FILE_TYPES:
                for d in IO_DIRECTIONS:
                    self.db_io[(db, ft, d)] += int(rng.randint(1_000, 100_000) * load)
                    ops_delta = int(rng.randint(10, 1_000) * load)
                    # OOTB alert: INCREASE(latency)/INCREASE(ops) > 0.1s. Any positive key
                    # (GT probes send e.g. 75) forces >100ms per operation.
                    if io_raw > 0.0:
                        lat_delta = ops_delta * rng.uniform(0.15, 0.35)
                    else:
                        lat_delta = rng.uniform(0.001, 0.05)
                    self.db_latency[(db, ft, d)] += lat_delta
                    self.db_ops[(db, ft, d)] += ops_delta
        self.lock_wait_count += rng.randint(0, 10)
        # Healthy: tiny ambient wait growth (well under 60s/window). Chaos: wait_mult >> 1.
        ambient = 0.001 if wait_mult <= 0 else 0.0
        grow = wait_mult if wait_mult > 0 else ambient
        for wt, _ in WAIT_TYPES:
            self.os_wait_duration[wt] += rng.uniform(0.01, 0.5) * grow


def _db_metrics_resource(base_resource: dict, database: str) -> dict:
    """Clone the metrics resource and set sqlserver.database.name.

    Database I/O dashboards group by ``resource.attributes.sqlserver.database.name``
    (resource attribute in sqlserverreceiver/metadata.yaml).
    """
    attrs = list(base_resource.get("attributes", []))
    attrs = [a for a in attrs if a.get("key") != "sqlserver.database.name"]
    attrs.append({"key": "sqlserver.database.name", "value": {"stringValue": database}})
    return {
        "attributes": attrs,
        "schemaUrl": base_resource.get("schemaUrl", SCHEMA_URL),
    }


def _io_attrs(db: str, file_type: str, direction: str) -> dict:
    """Datapoint attrs for database.io / latency / operations."""
    logical = f"{db}_{file_type}"
    physical = f"D:\\\\MSSQL\\\\Data\\\\{logical}.{'mdf' if file_type == 'data' else 'ldf'}"
    return {
        "file_type": file_type,
        "direction": direction,
        "logical_filename": logical,
        "physical_filename": physical,
    }


def _resource_attr_map(resource: dict) -> dict[str, Any]:
    """Flatten OTLP resource attributes; drop data_stream.* (set on the doc root)."""
    out: dict[str, Any] = {}
    for attr in resource.get("attributes", []):
        key = attr["key"]
        if key.startswith("data_stream."):
            continue
        val = attr["value"]
        if "stringValue" in val:
            out[key] = val["stringValue"]
        elif "intValue" in val:
            out[key] = int(val["intValue"])
        elif "doubleValue" in val:
            out[key] = float(val["doubleValue"])
        elif "boolValue" in val:
            out[key] = bool(val["boolValue"])
    return out


def build_io_latency_ops_doc(
    resource: dict,
    *,
    database: str,
    file_type: str,
    direction: str,
    latency_s: float,
    operations: int,
    timestamp_ms: int | None = None,
    start_timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Co-locate latency + operations counters for the OOTB I/O latency alert.

    ``[SQL Server OTel] Database I/O latency high`` ANDs both fields on the
    same document before ``INCREASE(latency)/INCREASE(ops)``. OTLP is one
    metric per doc, so bulk-index a companion with both counters.
    """
    db_resource = _db_metrics_resource(resource, database)
    doc: dict[str, Any] = {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "s",
        "data_stream": {
            "type": "metrics",
            "dataset": "sqlserverreceiver.otel",
            "namespace": "default",
        },
        "attributes": _io_attrs(database, file_type, direction),
        "resource": {"attributes": _resource_attr_map(db_resource)},
        "scope": {"name": SQLSERVER_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "sqlserver.database.latency": float(latency_s),
            "sqlserver.database.operations": int(operations),
        },
    }
    if start_timestamp_ms is not None:
        doc["start_timestamp"] = int(start_timestamp_ms)
    return doc


def _generate_instance_metrics(
    state: SQLServerMetricState,
    rng: random.Random,
    active_chaos: dict | None = None,
) -> list[dict]:
    """Instance-level metrics (no per-database resource attribute)."""
    metrics: list[dict] = []
    load = business_load()
    chaos = active_chaos or {}

    batch_rate = int(rng.randint(100, 5_000) * load)
    metrics.append(build_gauge("sqlserver.batch.request.rate", "{requests}/s", batch_rate))
    metrics.append(build_gauge(
        "sqlserver.batch.sql_compilation.rate", "{compilations}/s",
        int(batch_rate * rng.uniform(0.05, 0.2)),
    ))
    recomp = int(batch_rate * rng.uniform(0.01, 0.05))
    metrics.append(build_gauge("sqlserver.batch.sql_recompilation.rate", "{compilations}/s", recomp))

    deadlock_rate = float(impact_value(chaos, "deadlocks_detected", default=0.0) or 0.0)
    metrics.append(build_gauge("sqlserver.deadlock.rate", "{deadlocks}/s", round(deadlock_rate, 3)))

    login_override = float(impact_value(chaos, "login_rate_spike", default=0.0) or 0.0)
    login_rate = login_override if login_override > 0 else float(rng.uniform(1, 20) * load)
    metrics.append(build_gauge("sqlserver.login.rate", "{logins}/s", int(login_rate)))
    metrics.append(build_gauge("sqlserver.logout.rate", "{logouts}/s", int(rng.uniform(1, 20) * load)))

    hit_raw = float(impact_value(chaos, "buffer_cache_hit_ratio_low", default=0.0) or 0.0)
    # Alert is MIN(hit_ratio) < 95. GT probes often send tokens like 200.0 (not a
    # valid %). Values in (0, 95) are explicit targets; ≥95 or >100 → force ~70.
    if hit_raw <= 0:
        hit_ratio = rng.uniform(96, 99.5)
    elif hit_raw < 95.0:
        hit_ratio = hit_raw
    else:
        hit_ratio = 70.0
    metrics.append(build_gauge("sqlserver.page.buffer_cache.hit_ratio", "%", round(hit_ratio, 1)))

    ple_raw = float(impact_value(chaos, "page_life_expectancy_low", default=3000.0))
    # SPEC default 3000 = healthy (filled on sibling channels; 0 is the worst value, not
    # "unset"). Alert is PLE < 300s. Probes in [300, 3000), e.g. GT 300.0, sit on or above
    # the line, so emit clearly under it.
    if ple_raw >= 3000.0:
        ple = rng.uniform(400, 3600)
    elif ple_raw < 300.0:
        ple = ple_raw
    else:
        ple = 120.0
    metrics.append(build_gauge("sqlserver.page.life_expectancy", "s", round(ple, 1)))

    blocked = int(impact_value(chaos, "blocked_processes", default=0) or 0)
    metrics.append(build_gauge("sqlserver.processes.blocked", "{processes}", blocked))

    cpu = rng.uniform(5, 40) * load
    metrics.append(build_gauge("sqlserver.cpu.utilization", "%", round(min(cpu, 100.0), 1)))

    conn = int(rng.randint(10, 200) * load)
    metrics.append(build_gauge("sqlserver.user.connection.count", "{connections}", conn))

    tempdb_raw = float(impact_value(chaos, "tempdb_space_low", default=1_024_000.0))
    # SPEC default 1024000 = healthy (sibling fill; 0 kB free is the worst value, not
    # "unset"). Alert is free space < 102400. GT probe 102400.0 sits on the threshold —
    # force clearly under when active.
    tempdb_free: int | None
    if tempdb_raw >= 1_024_000:
        tempdb_free = None
    elif tempdb_raw < 102_400:
        tempdb_free = int(tempdb_raw)
    else:
        tempdb_free = 50_000
    for ts in TEMPDB_STATES:
        if ts == "free" and tempdb_free is not None:
            space = tempdb_free
        else:
            space = rng.randint(200_000, 500_000) if ts == "free" else rng.randint(100, 50_000)
        metrics.append(build_gauge(
            "sqlserver.database.tempdb.space", "kBy",
            space, {"tempdb.state": ts},
        ))
    version_store = int(impact_value(chaos, "tempdb_version_store_large", default=0) or 0)
    if version_store <= 0:
        version_store = rng.randint(0, 1_000)
    metrics.append(build_gauge("sqlserver.database.tempdb.version_store.size", "kBy", version_store))

    lock_timeout = float(impact_value(chaos, "lock_timeouts", default=0.0) or 0.0)
    metrics.append(build_gauge("sqlserver.lock.timeout.rate", "{timeout}/s", round(lock_timeout, 3)))
    metrics.append(build_cumulative_sum(
        "sqlserver.lock.wait.count", "{wait}",
        state.lock_wait_count, state.start_time_ns,
    ))
    lock_wait = float(impact_value(chaos, "lock_wait_rate_high", default=0.0) or 0.0)
    if lock_wait <= 0:
        lock_wait = rng.uniform(0, 5) * load
    metrics.append(build_gauge(
        "sqlserver.lock.wait.rate", "{requests}/s",
        round(lock_wait, 2),
    ))
    lock_wait_avg = rng.uniform(0, 5)
    metrics.append(build_gauge("sqlserver.lock.wait_time.avg", "ms", round(lock_wait_avg, 2)))

    mem_grants_pending = int(chaos.get("memory_grants_pending", 0) or 0)
    metrics.append(build_gauge("sqlserver.memory.grants.pending.count", "{grants}", mem_grants_pending))
    mem_usage_kb = rng.randint(500_000, 2_000_000)
    metrics.append(build_gauge("sqlserver.memory.usage", "kBy", mem_usage_kb))

    stalls = float(impact_value(chaos, "free_list_stalls", default=0.0) or 0.0)
    metrics.append(build_gauge(
        "sqlserver.page.buffer_cache.free_list.stalls.rate",
        "{stalls}/s", round(stalls, 3),
    ))
    metrics.append(build_gauge(
        "sqlserver.page.lookup.rate", "{lookups}/s",
        round(rng.uniform(100, 10_000) * load, 2),
    ))

    for wt, category in WAIT_TYPES:
        metrics.append(build_cumulative_sum(
            "sqlserver.os.wait.duration", "s",
            float(round(state.os_wait_duration[wt], 6)),
            state.start_time_ns,
            {"wait.type": wt, "wait.category": category},
        ))

    txn_rate = rng.uniform(10, 1_000) * load
    metrics.append(build_gauge("sqlserver.transaction.rate", "{transactions}/s", round(txn_rate, 2)))
    metrics.append(build_gauge(
        "sqlserver.transaction.write.rate", "{transactions}/s",
        round(txn_rate * 0.3, 2),
    ))
    # transaction.delay is a non-monotonic sum (gauge-like); dashboards AVG it.
    txn_delay = rng.uniform(0, 50) * (20 if active_chaos else 1)
    metrics.append(build_gauge("sqlserver.transaction.delay", "ms", round(txn_delay, 2)))

    for direction in ("send", "receive"):
        replica_rate = rng.uniform(0, 50_000) * (0.1 if active_chaos else 1)
        metrics.append(build_gauge(
            "sqlserver.replica.data.rate", "By/s",
            round(replica_rate, 2),
            {"replica.direction": direction},
        ))

    return metrics


def _generate_db_metrics(
    state: SQLServerMetricState,
    database: str,
    rng: random.Random,
    active_chaos: dict | None = None,
) -> list[dict]:
    """Per-database metrics; caller sets resource.attributes.sqlserver.database.name."""
    metrics: list[dict] = []
    load = business_load()

    for ft in FILE_TYPES:
        for d in IO_DIRECTIONS:
            io_attr = _io_attrs(database, ft, d)
            metrics.append(build_cumulative_sum(
                "sqlserver.database.io", "By",
                state.db_io[(database, ft, d)], state.start_time_ns, io_attr,
            ))
            metrics.append(build_cumulative_sum(
                "sqlserver.database.latency", "s",
                float(round(state.db_latency[(database, ft, d)], 6)),
                state.start_time_ns, io_attr,
            ))
            metrics.append(build_cumulative_sum(
                "sqlserver.database.operations", "{operations}",
                state.db_ops[(database, ft, d)], state.start_time_ns, io_attr,
            ))

    metrics.append(build_gauge(
        "sqlserver.database.backup_or_restore.rate",
        "{backups_or_restores}/s", rng.uniform(0, 0.1),
    ))
    err_rate = float(impact_value(active_chaos, "execution_errors", default=0.0) or 0.0)
    metrics.append(build_gauge(
        "sqlserver.database.execution.errors", "{errors}", round(err_rate, 2),
    ))
    metrics.append(build_gauge(
        "sqlserver.database.full_scan.rate", "{scans}/s",
        round(rng.uniform(0.5, 20) * load, 2),
    ))

    return metrics


# ── Default query templates ───────────────────────────────────────────────────

_DEFAULT_QUERIES = [
    {"sql": "SELECT * FROM dbo.Orders WHERE CustomerId = @CustomerId AND Status = @Status", "db": "AppDB", "user": "app_user"},
    {"sql": "INSERT INTO dbo.AuditLog (EventTime, UserId, Action) VALUES (GETUTCDATE(), @UserId, @Action)", "db": "AppDB", "user": "app_user"},
    {"sql": "UPDATE dbo.Inventory SET Quantity = @Qty, UpdatedAt = GETUTCDATE() WHERE ItemId = @ItemId", "db": "AppDB", "user": "app_user"},
    {"sql": "SELECT COUNT(*) FROM dbo.Sessions WHERE UserId = @UserId AND ExpiresAt > GETUTCDATE()", "db": "AppDB", "user": "app_user"},
    {"sql": "EXEC dbo.usp_ProcessPayment @OrderId, @Amount, @Currency", "db": "AppDB", "user": "app_user"},
]

_REQUEST_STATUSES = ["running", "suspended", "sleeping", "background"]
_WAIT_TYPE_NAMES = [wt for wt, _ in WAIT_TYPES]


def _generate_logs(
    client: OTLPClient,
    queries: list[dict],
    databases: list[str],
    rng: random.Random,
    active_chaos: dict | None = None,
) -> list[dict]:
    """Build properly formatted OTLP log records via client.build_log_record."""
    logs: list[dict] = []

    chaos = active_chaos or {}
    # Baseline includes a small share of blocked samples so the Query Performance
    # "Recent query samples with blocking" panel is populated without a fault.
    slow_fraction = float(impact_value(chaos, "lock_wait_rate_high", "deadlocks_detected", default=0.15) or 0.15)
    if not active_chaos:
        slow_fraction = 0.15
    slow_ms = 2000
    chaos_blocker = int(impact_value(chaos, "blocked_processes", default=0) or 0)
    # Under chaos, most slow samples are blocked; baseline still emits some.
    block_fraction = 0.8 if active_chaos else 0.25

    for idx, q in enumerate(queries):
        is_slow = rng.random() < slow_fraction
        is_blocked = is_slow and rng.random() < block_fraction
        # Always emit at least one blocked sample so the blocking panel stays populated.
        if idx == 0 and not active_chaos:
            is_slow = True
            is_blocked = True
        # metadata.yaml: sqlserver.cpu_time / total_elapsed_time are seconds (double).
        # Dashboards multiply by 1000 to display milliseconds.
        cpu_s = (rng.uniform(0.001, 0.1) if not is_slow else rng.uniform(slow_ms / 1000, slow_ms / 200))
        elapsed_s = cpu_s * rng.uniform(1.0, 3.0)
        db = q.get("db") or rng.choice(databases)
        # Prefer scenario DB names when the template still has the default AppDB.
        if db == "AppDB" and databases:
            db = rng.choice(databases)
        sql = q["sql"]
        session_id = rng.randint(50, 500)
        if is_blocked:
            # Panel filters blocking_session_id > 0; prefer chaos override when set.
            blocker = chaos_blocker if chaos_blocker > 0 else rng.randint(50, 500)
            while blocker == session_id:
                blocker = rng.randint(50, 500)
        else:
            blocker = 0
        attrs = {
            "db.query.text": sql,
            "db.system.name": "microsoft.sql_server",
            "user.name": q.get("user", "app_user"),
            "db.namespace": db,
            "sqlserver.session_id": session_id,
            "sqlserver.query_hash": f"0x{''.join(rng.choices('0123456789ABCDEF', k=16))}",
            "sqlserver.request_status": "suspended" if is_blocked or is_slow else rng.choice(_REQUEST_STATUSES),
            "sqlserver.wait_type": (
                rng.choice([wt for wt in _WAIT_TYPE_NAMES if wt.startswith("LCK_")])
                if is_blocked
                else (rng.choice(_WAIT_TYPE_NAMES) if is_slow else "")
            ),
            "sqlserver.cpu_time": float(round(cpu_s, 6)),
            "sqlserver.logical_reads": rng.randint(10, 10_000) * (50 if is_slow else 1),
            "sqlserver.total_elapsed_time": float(round(elapsed_s, 6)),
            "sqlserver.blocking_session_id": blocker,
            "sqlserver.reads": rng.randint(0, 1_000),
            "sqlserver.writes": rng.randint(0, 100),
            "sqlserver.row_count": rng.randint(0, 10_000),
        }
        logs.append(client.build_log_record(
            severity="WARN" if is_slow else "INFO",
            body=sql,
            attributes=attrs,
            event_name="db.server.query_sample",
        ))

    # top_query — dashboards share cpu_time / logical_reads / total_elapsed_time fields
    tq = rng.choice(queries)
    tq_db = tq.get("db") or rng.choice(databases)
    if tq_db == "AppDB" and databases:
        tq_db = rng.choice(databases)
    tq_sql = tq["sql"]
    top_cpu = rng.uniform(0.1, 50.0) * (5 if active_chaos else 1)
    top_elapsed = top_cpu * rng.uniform(1.0, 4.0)
    logs.append(client.build_log_record(
        severity="INFO",
        body=tq_sql,
        attributes={
            "db.query.text": tq_sql,
            "db.system.name": "microsoft.sql_server",
            "db.namespace": tq_db,
            "sqlserver.query_hash": f"0x{''.join(rng.choices('0123456789ABCDEF', k=16))}",
            "sqlserver.cpu_time": float(round(top_cpu, 6)),
            "sqlserver.logical_reads": rng.randint(10_000, 10_000_000),
            "sqlserver.total_elapsed_time": float(round(top_elapsed, 6)),
            "sqlserver.reads": rng.randint(0, 50_000),
            "sqlserver.writes": rng.randint(0, 5_000),
            "sqlserver.row_count": rng.randint(0, 1_000_000),
            "sqlserver.execution_count": rng.randint(10, 50_000),
        },
        event_name="db.server.top_query",
    ))

    return logs



from generators.microsoft_sqlserver_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run SQL Server generator loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {})
    else:
        ns = NAMESPACE
        cfg = {}

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    db_prefix = ns.replace("-", "_")
    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("sqlserver-primary")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("sqlserver-host")
    instance_id = cfg.get("instance_id") or f"{host_name}\\MSSQLSERVER"

    default_dbs = [ScenarioIdentity(ns).database("app"), ScenarioIdentity(ns).database("reporting"), ScenarioIdentity(ns).database("audit")]
    databases: list[str] = cfg.get("databases") or default_dbs
    queries: list[dict] = cfg.get("queries") or _DEFAULT_QUERIES
    cloud = cfg.get("cloud") or _default_cloud()



    resource = build_signal_resource(
        ns=ns,
        signal_type="metrics",
        dataset="sqlserverreceiver",
        service_name=service_name,
        service_version="2022",
        service_instance_id=instance_id,
        host_name=host_name,
        cloud=cloud,
        extra_attrs={
            "server.address": host_name,
        },
    )
    log_resource = build_signal_resource(
        ns=ns,
        signal_type="logs",
        dataset="sqlserverreceiver",
        service_name=service_name,
        service_version="2022",
        service_instance_id=instance_id,
        host_name=host_name,
        cloud=cloud,
        extra_attrs={
            "server.address": host_name,
        },
    )

    metric_state = SQLServerMetricState(rng, databases)
    scope = {"name": SQLSERVER_SCOPE, "version": SCOPE_VERSION}

    try:
        es_bulk = ESBulkClient()
    except Exception as exc:  # pragma: no cover
        logger.warning("ESBulkClient unavailable (%s); I/O latency co-locate disabled", exc)
        es_bulk = None

    logger.info("SQL Server generator started (namespace=%s, instance=%s)", ns, instance_id)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="microsoft_sqlserver_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        metric_state.tick(rng, databases, active_chaos)

        # Instance-level metrics
        client.send_metrics_scoped(
            resource, scope, _generate_instance_metrics(metric_state, rng, active_chaos),
        )

        # Per-database metrics with resource.attributes.sqlserver.database.name
        bulk_docs: list[dict[str, Any]] = []
        ts_ms = int(time.time() * 1000)
        start_ms = int(int(metric_state.start_time_ns) / 1_000_000)
        for db in databases:
            client.send_metrics_scoped(
                _db_metrics_resource(resource, db),
                scope,
                _generate_db_metrics(metric_state, db, rng, active_chaos),
            )
            for ft in FILE_TYPES:
                for d in IO_DIRECTIONS:
                    bulk_docs.append(build_io_latency_ops_doc(
                        resource,
                        database=db,
                        file_type=ft,
                        direction=d,
                        latency_s=float(round(metric_state.db_latency[(db, ft, d)], 6)),
                        operations=int(metric_state.db_ops[(db, ft, d)]),
                        timestamp_ms=ts_ms,
                        start_timestamp_ms=start_ms,
                    ))

        if es_bulk is not None and es_bulk.configured and bulk_docs:
            es_bulk.send_bulk(
                "metrics-sqlserverreceiver.otel-default",
                bulk_docs,
                dynamic_templates={
                    "metrics.sqlserver.database.latency": "counter_double",
                    "metrics.sqlserver.database.operations": "counter_long",
                },
            )

        logs = _generate_logs(client, queries, databases, rng, active_chaos)
        client.send_logs_scoped(log_resource, scope, logs)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    if es_bulk is not None:
        es_bulk.close()
    logger.info("SQL Server generator stopped.")


def main() -> None:  # pragma: no cover
    """Standalone entry point for local testing."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
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
