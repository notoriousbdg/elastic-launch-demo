#!/usr/bin/env python3
"""Oracle Database Generator — sends oracledbreceiver OTel metrics and logs via OTLP.

Emits data matching the oracle_otel content package
(discovery.datasets: oracledbreceiver.otel), which populates the bundled
Overview, Resource Capacity, SQL Execution, Transaction Health, Active Query
Analysis, and Query Performance dashboards.

Signal types:
  - Metrics: oracledb.enqueue_deadlocks, oracledb.executions,
             oracledb.logical_reads, oracledb.physical_reads,
             oracledb.processes.limit, oracledb.processes.usage,
             oracledb.sessions.limit, oracledb.sessions.usage,
             oracledb.tablespace_size.limit, oracledb.tablespace_size.usage,
             oracledb.transactions.limit, oracledb.transactions.usage,
             oracledb.user_commits, oracledb.user_rollbacks,
             oracledb.dml_locks.limit, oracledb.dml_locks.usage,
             oracledb.enqueue_locks.limit, oracledb.enqueue_locks.usage,
             oracledb.enqueue_resources.limit, oracledb.enqueue_resources.usage,
             oracledb.hard_parses, oracledb.parse_calls, oracledb.pga_memory,
             oracledb.exchange_deadlocks, oracledb.cpu_time
  - Logs: db.server.query_sample, db.server.top_query

Also bulk-indexes co-located metric documents each tick so panels that
AND-filter metric pairs (tablespace usage+limit, physical+logical reads,
hard_parses+parse_calls) receive data. OTLP stores one metric per document,
so those filters never match OTLP-only data. Ratio counter docs are bulk-
indexed with ``dynamic_templates: counter_long`` (same as the OTel ES
exporter); without that, fields map as plain ``long`` and RATE() panels fail.

Field names and attribute keys come from:
  - opentelemetry-collector-contrib/receiver/oracledbreceiver/metadata.yaml
  - elastic/integrations/packages/oracle_otel/kibana/dashboard/*.json

Usage (standalone):
    python3 -m generators.oracle_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/oracle_otel@0.4.0"

import logging
import os
import random
import signal
import threading
import time
from typing import Any

from app.telemetry import ESBulkClient, OTLPClient
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
    "oracledb.enqueue_deadlocks",
    "oracledb.executions",
    "oracledb.logical_reads",
    "oracledb.physical_reads",
    "oracledb.processes.limit",
    "oracledb.processes.usage",
    "oracledb.sessions.limit",
    "oracledb.sessions.usage",
    "oracledb.tablespace_size.limit",
    "oracledb.tablespace_size.usage",
    "oracledb.transactions.limit",
    "oracledb.transactions.usage",
    "oracledb.user_commits",
    "oracledb.user_rollbacks",
    "oracledb.dml_locks.limit",
    "oracledb.dml_locks.usage",
    "oracledb.enqueue_locks.limit",
    "oracledb.enqueue_locks.usage",
    "oracledb.enqueue_resources.limit",
    "oracledb.enqueue_resources.usage",
    "oracledb.hard_parses",
    "oracledb.parse_calls",
    "oracledb.pga_memory",
    "oracledb.exchange_deadlocks",
    "oracledb.cpu_time",
)

# Log attribute keys verified against oracledbreceiver/metadata.yaml + dashboards
LOG_ATTRIBUTES = (
    "db.query.text",
    "db.system.name",
    "user.name",
    "db.namespace",
    "oracle.db.service",
    "client.address",
    "oracledb.sql_id",
    "oracledb.duration_sec",
    "oracledb.schemaname",
    "oracledb.program",
    "oracledb.sid",
    "oracledb.serial",
    "oracledb.status",
    "oracledb.state",
    "oracledb.wait_class",
    "oracledb.event",
    "oracledb.elapsed_time",
    "oracledb.executions",
    "oracledb.buffer_gets",
    "oracledb.disk_reads",
    "oracledb.rows_processed",
    "oracledb.cpu_time",
    "oracledb.application_wait_time",
    "oracledb.cluster_wait_time",
    "oracledb.concurrency_wait_time",
    "oracledb.user_io_wait_time",
    "oracledb.physical_read_bytes",
    "oracledb.physical_read_requests",
    "oracledb.physical_write_bytes",
    "oracledb.physical_write_requests",
)

ORACLE_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/oracledbreceiver"
)
SCOPE_VERSION = "0.115.0"

_METRICS_DATA_STREAM = "metrics-oracledbreceiver.otel-default"

# Co-located ratio docs must map as counters so dashboard RATE() panels work.
# Plain ESBulk create (no dynamic_templates) maps them as long and poisons the
# stream — same sticky-mapping problem as postgresql temp_* gauges.
_RATIO_COUNTER_FIELDS = (
    "oracledb.physical_reads",
    "oracledb.logical_reads",
    "oracledb.hard_parses",
    "oracledb.parse_calls",
)
_RATIO_COUNTER_DYNAMIC_TEMPLATES = {
    f"metrics.{name}": "counter_long" for name in _RATIO_COUNTER_FIELDS
}
_TABLESPACE_GAUGE_DYNAMIC_TEMPLATES = {
    "metrics.oracledb.tablespace_size.usage": "gauge_long",
    "metrics.oracledb.tablespace_size.limit": "gauge_long",
}

# Capacity / lock / deadlock alerts need these fields mapped even when OTLP
# intake drops the scoped batch (only bulk companions were rematerializing).
_CAPACITY_GAUGE_FIELDS = (
    "oracledb.processes.usage",
    "oracledb.processes.limit",
    "oracledb.sessions.usage",
    "oracledb.sessions.limit",
    "oracledb.transactions.usage",
    "oracledb.transactions.limit",
    "oracledb.dml_locks.usage",
    "oracledb.dml_locks.limit",
    "oracledb.enqueue_locks.usage",
    "oracledb.enqueue_locks.limit",
    "oracledb.enqueue_resources.usage",
    "oracledb.enqueue_resources.limit",
)
_CAPACITY_GAUGE_DYNAMIC_TEMPLATES = {
    f"metrics.{name}": "gauge_long" for name in _CAPACITY_GAUGE_FIELDS
}
_TXN_COUNTER_FIELDS = (
    "oracledb.user_commits",
    "oracledb.user_rollbacks",
    "oracledb.enqueue_deadlocks",
)
_TXN_COUNTER_DYNAMIC_TEMPLATES = {
    f"metrics.{name}": "counter_long" for name in _TXN_COUNTER_FIELDS
}

# V$SESSION.TYPE / STATUS values used by Resource Capacity panels
SESSION_STATUSES = ("ACTIVE", "INACTIVE")
SESSION_TYPES = ("USER", "BACKGROUND")

logger = logging.getLogger("oracle-generator")

BATCH_INTERVAL_MIN = 10
BATCH_INTERVAL_MAX = 20

_PROGRAMS = (
    "JDBC Thin Client",
    "sqlplus@app-host",
    "python3",
    "OMS",
    "w3wp.exe",
    "oracle@db-host (TNS V1-V3)",
)


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_rds",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


def _default_tablespaces(prefix: str) -> list[str]:
    return [
        f"{prefix}_DATA",
        f"{prefix}_INDEX",
        f"{prefix}_TEMP",
        f"{prefix}_UNDO",
    ]


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


def _ratio_counters_missing_counter_mapping(elastic_url: str, api_key: str) -> bool:
    """Return True if any ratio-panel counter is mapped without time_series_metric=counter."""
    import httpx

    url = (
        f"{elastic_url.rstrip('/')}/{_METRICS_DATA_STREAM}/_field_caps"
        f"?fields={','.join(_RATIO_COUNTER_FIELDS)}"
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
    for name in _RATIO_COUNTER_FIELDS:
        type_map = fields.get(name) or {}
        if not type_map:
            continue
        for meta in type_map.values():
            if meta.get("time_series_metric") != "counter":
                return True
    return False


def _reset_ratio_counter_mapping() -> None:
    """Delete the metrics data stream if ratio counters are stuck as plain long/gauge.

    Co-located bulk docs without ``dynamic_templates: counter_long`` map these
    fields as plain ``long``, which breaks Overview / SQL Execution RATE()
    panels.  Fresh counter samples (OTLP + templated bulk) then recreate the
    stream correctly.  No-op when ES credentials are absent or mapping is OK.
    """
    try:
        from app.telemetry import ESBulkClient
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for ratio counter mapping check: %s", exc)
        return

    client = ESBulkClient()
    if not client.configured:
        return

    if not _ratio_counters_missing_counter_mapping(client.elastic_url, client.api_key):
        logger.info(
            "oracle ratio counters already mapped as counters; leaving %s alone",
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
            "Deleted %s so physical/logical reads and parse counters remap as "
            "counter_long (were plain long/gauge)",
            _METRICS_DATA_STREAM,
        )
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist", _METRICS_DATA_STREAM)
    else:
        logger.warning(
            "Could not delete %s: %s %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )


def build_tablespace_utilization_doc(
    resource: dict,
    tablespace_name: str,
    usage_bytes: int,
    limit_bytes: int,
    *,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc with tablespace usage+limit co-located.

    Tablespace utilisation panels AND-filter both metrics on the same document.
    OTLP stores one metric per document, so those filters never match OTLP-only
    data.
    """
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "By",
        "data_stream": {
            "type": "metrics",
            "dataset": "oracledbreceiver.otel",
            "namespace": "default",
        },
        "attributes": {"tablespace_name": tablespace_name},
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": ORACLE_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "oracledb.tablespace_size.usage": int(usage_bytes),
            "oracledb.tablespace_size.limit": int(limit_bytes),
        },
    }


def build_buffer_cache_ratio_doc(
    resource: dict,
    physical_reads: int,
    logical_reads: int,
    *,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Co-locate physical+logical reads for buffer cache hit ratio panels."""
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "{reads}",
        "data_stream": {
            "type": "metrics",
            "dataset": "oracledbreceiver.otel",
            "namespace": "default",
        },
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": ORACLE_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "oracledb.physical_reads": int(physical_reads),
            "oracledb.logical_reads": int(logical_reads),
        },
    }


def build_hard_parse_ratio_doc(
    resource: dict,
    hard_parses: int,
    parse_calls: int,
    *,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Co-locate hard_parses+parse_calls for hard parse ratio panels."""
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "{parses}",
        "data_stream": {
            "type": "metrics",
            "dataset": "oracledbreceiver.otel",
            "namespace": "default",
        },
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": ORACLE_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "oracledb.hard_parses": int(hard_parses),
            "oracledb.parse_calls": int(parse_calls),
        },
    }


def _bulk_metric_doc(
    resource: dict,
    metrics: dict[str, int | float],
    *,
    unit: str,
    attributes: dict[str, Any] | None = None,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """ECS-shaped metrics doc for ES bulk dual-write."""
    doc: dict[str, Any] = {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": unit,
        "data_stream": {
            "type": "metrics",
            "dataset": "oracledbreceiver.otel",
            "namespace": "default",
        },
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": ORACLE_SCOPE, "version": SCOPE_VERSION},
        "metrics": metrics,
    }
    if attributes:
        doc["attributes"] = attributes
    return doc


def build_usage_limit_doc(
    resource: dict,
    usage_metric: str,
    limit_metric: str,
    usage: int,
    limit: int,
    *,
    unit: str,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Co-locate usage+limit gauges for utilisation alerts."""
    return _bulk_metric_doc(
        resource,
        {usage_metric: int(usage), limit_metric: int(limit)},
        unit=unit,
        timestamp_ms=timestamp_ms,
    )


def build_session_usage_docs(
    resource: dict,
    sess_limit: int,
    session_parts: list[tuple[dict[str, str], int]],
    *,
    timestamp_ms: int | None = None,
) -> list[dict[str, Any]]:
    """Session utilisation: limit doc + per type/status usage docs."""
    docs = [
        _bulk_metric_doc(
            resource,
            {"oracledb.sessions.limit": int(sess_limit)},
            unit="{sessions}",
            timestamp_ms=timestamp_ms,
        )
    ]
    for attrs, usage in session_parts:
        docs.append(
            _bulk_metric_doc(
                resource,
                {"oracledb.sessions.usage": int(usage)},
                unit="{sessions}",
                attributes=attrs,
                timestamp_ms=timestamp_ms,
            )
        )
    return docs


def build_rollback_ratio_doc(
    resource: dict,
    user_commits: int,
    user_rollbacks: int,
    *,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Co-locate commits+rollbacks for rollback-to-commit ratio alerts."""
    return _bulk_metric_doc(
        resource,
        {
            "oracledb.user_commits": int(user_commits),
            "oracledb.user_rollbacks": int(user_rollbacks),
        },
        unit="{commits}",
        timestamp_ms=timestamp_ms,
    )


def build_deadlock_doc(
    resource: dict,
    enqueue_deadlocks: int,
    *,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Emit enqueue_deadlocks tip so the OOTB column exists at steady state."""
    return _bulk_metric_doc(
        resource,
        {"oracledb.enqueue_deadlocks": int(enqueue_deadlocks)},
        unit="{deadlocks}",
        timestamp_ms=timestamp_ms,
    )


class OracleMetricState(CounterState):
    """Cumulative counter state for Oracle Database."""

    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.executions = rng.randint(1_000, 5_000_000)
        # logical/physical and parse_calls/hard_parses are independently
        # randomized ratio inputs — seeding them independently can start the
        # buffer-cache-hit-ratio and hard-parse-ratio panels already past
        # their alert thresholds on tick 1. Derive the smaller side as a
        # bounded fraction of the larger so healthy ratios hold from the start.
        self.logical_reads = rng.randint(1_000_000, 100_000_000)
        self.physical_reads = int(self.logical_reads * rng.uniform(0.01, 0.05))
        self.user_commits = rng.randint(10_000, 5_000_000)
        self.user_rollbacks = rng.randint(100, 50_000)
        self.parse_calls = rng.randint(100_000, 1_000_000)
        self.hard_parses = int(self.parse_calls * rng.uniform(0.005, 0.02))
        self.pga_memory = rng.randint(10_000_000, 5_000_000_000)
        # OOTB alert: SUM(enqueue_deadlocks) > 0 — start flat, chaos-only growth.
        self.enqueue_deadlocks = 0
        self.exchange_deadlocks = 0
        self.dml_locks_usage = rng.randint(0, 200)
        self.enqueue_locks_usage = rng.randint(0, 100)
        self.enqueue_resources_usage = rng.randint(0, 100)
        # Cumulative CPU seconds (metadata.yaml: value_type double, unit s)
        self.cpu_time = float(rng.uniform(1_000.0, 500_000.0))
        # tablespace_name -> [limit_bytes, usage_bytes]
        self.tablespaces: dict[str, list[int]] = {}

    def ensure_tablespaces(self, names: list[str], rng: random.Random) -> None:
        for name in names:
            if name in self.tablespaces:
                continue
            limit = rng.randint(10_000_000_000, 100_000_000_000)
            usage = int(limit * rng.uniform(0.2, 0.7))
            self.tablespaces[name] = [limit, usage]

    def tick(self, rng: random.Random, active_chaos: dict | None = None):
        load = business_load()
        chaos = active_chaos or {}
        hit_ratio = float(impact_value(chaos, "buffer_cache_hit_ratio_low", default=0.95))
        # GT probes often send percent (10.0 → 10% hit); SPEC healthy default is 100.0.
        if hit_ratio > 1.0:
            hit_ratio = hit_ratio / 100.0
        rollback_ratio = float(impact_value(chaos, "rollback_to_commit_ratio_high", default=0.0))
        hard_parse_ratio = float(impact_value(chaos, "hard_parse_ratio_high", default=0.0))
        deadlock_rate = float(impact_value(chaos, "enqueue_deadlocks_detected", default=0.0) or 0.0)
        dml_util = float(impact_value(chaos, "dml_lock_utilisation_high", default=0.0))
        enq_util = float(impact_value(chaos, "enqueue_lock_utilisation_high", default=0.0))
        ts_util = float(impact_value(chaos, "tablespace_utilisation_high", default=0.0))
        if dml_util > 1.0:
            dml_util = dml_util / 100.0
        if enq_util > 1.0:
            enq_util = enq_util / 100.0
        if ts_util > 1.0:
            ts_util = ts_util / 100.0
        if hard_parse_ratio > 1.0:
            hard_parse_ratio = hard_parse_ratio / 100.0
        if rollback_ratio > 1.0:
            rollback_ratio = rollback_ratio / 100.0

        self.executions += int(rng.randint(100, 10_000) * load)
        logical = int(rng.randint(10_000, 500_000) * load)
        # OOTB: hit_ratio_pct = (1 - physical/logical)*100 < 90.
        # Do not compare physical_frac to 0.05 — ``1.0 - 0.95`` is slightly
        # above 0.05 in float math, which incorrectly took the breach path
        # (max(..., 0.15) → constant 85% hit) on the healthy default.
        if hit_ratio >= 0.90:
            physical = max(1, int(logical * rng.uniform(0.01, 0.05)))
        else:
            physical_frac = max(0.0, 1.0 - min(hit_ratio, 1.0))
            physical = max(1, int(logical * max(physical_frac, 0.15)))
        self.logical_reads += logical
        self.physical_reads += physical
        commits = int(rng.randint(100, 5_000) * load)
        self.user_commits += commits
        if rollback_ratio > 0:
            self.user_rollbacks += (
                commits if rollback_ratio >= 1
                else int(commits * rollback_ratio / max(1e-6, 1.0 - rollback_ratio))
            )
        parse_calls = int(rng.randint(100, 5_000) * load)
        self.parse_calls += parse_calls
        self.hard_parses += rng.randint(0, 20) + int(parse_calls * hard_parse_ratio)
        self.pga_memory += rng.randint(-1_000_000, 5_000_000)
        self.pga_memory = max(10_000_000, self.pga_memory)
        deadlock_delta = int(max(0.0, deadlock_rate))
        self.enqueue_deadlocks += deadlock_delta
        self.exchange_deadlocks += deadlock_delta
        self.cpu_time += float(rng.uniform(0.5, 8.0) * load)
        if dml_util > 0:
            # OOTB utilisation_pct > 85 — lift percent probes into the breach band.
            self.dml_locks_usage = int(1800 * max(min(dml_util, 1.0), 0.90))
        else:
            self.dml_locks_usage = max(0, min(400, self.dml_locks_usage + rng.randint(-10, 10)))
        if enq_util > 0:
            self.enqueue_locks_usage = int(500 * max(min(enq_util, 1.0), 0.90))
            self.enqueue_resources_usage = int(500 * max(min(enq_util, 1.0), 0.90))
        else:
            self.enqueue_locks_usage = max(0, min(100, self.enqueue_locks_usage + rng.randint(-5, 5)))
            self.enqueue_resources_usage = max(0, min(100, self.enqueue_resources_usage + rng.randint(-5, 5)))
        for name, pair in self.tablespaces.items():
            limit, usage = pair
            if ts_util > 0:
                # OOTB utilisation_pct > 90 — GT 85.0% must be lifted into breach.
                pair[1] = int(limit * max(min(ts_util, 0.99), 0.95))
            else:
                growth = rng.randint(0, max(1, limit // 50_000))
                pair[1] = min(int(limit * 0.7), usage + growth)


def _generate_metrics(
    state: OracleMetricState,
    tablespaces: list[str],
    rng: random.Random,
    instance_name: str,
    active_chaos: dict | None = None,
) -> tuple[list[dict], dict[str, Any]]:
    state.ensure_tablespaces(tablespaces, rng)
    state.tick(rng, active_chaos)
    metrics: list[dict] = []
    load = business_load()

    # ── Cumulative counters ──
    metrics.append(build_cumulative_sum(
        "oracledb.enqueue_deadlocks", "{deadlocks}",
        state.enqueue_deadlocks, state.start_time_ns,
    ))
    metrics.append(build_cumulative_sum(
        "oracledb.exchange_deadlocks", "{deadlocks}",
        state.exchange_deadlocks, state.start_time_ns,
    ))
    metrics.append(build_cumulative_sum(
        "oracledb.executions", "{executions}",
        state.executions, state.start_time_ns,
    ))
    metrics.append(build_cumulative_sum(
        "oracledb.logical_reads", "{reads}",
        state.logical_reads, state.start_time_ns,
    ))
    metrics.append(build_cumulative_sum(
        "oracledb.physical_reads", "{reads}",
        state.physical_reads, state.start_time_ns,
    ))
    metrics.append(build_cumulative_sum(
        "oracledb.user_commits", "{commits}",
        state.user_commits, state.start_time_ns,
    ))
    metrics.append(build_cumulative_sum(
        "oracledb.user_rollbacks", "1",
        state.user_rollbacks, state.start_time_ns,
    ))
    metrics.append(build_cumulative_sum(
        "oracledb.hard_parses", "{parses}",
        state.hard_parses, state.start_time_ns,
    ))
    metrics.append(build_cumulative_sum(
        "oracledb.parse_calls", "{parses}",
        state.parse_calls, state.start_time_ns,
    ))
    metrics.append(build_cumulative_sum(
        "oracledb.pga_memory", "By",
        state.pga_memory, state.start_time_ns,
    ))
    # metadata.yaml: cumulative CPU time in seconds (double)
    metrics.append(build_cumulative_sum(
        "oracledb.cpu_time", "s",
        float(state.cpu_time), state.start_time_ns,
    ))

    # ── Process / session limits and usage ──
    chaos = active_chaos or {}
    proc_limit = 800
    proc_util = float(impact_value(chaos, "process_utilisation_high", default=0.0) or 0.0)
    if proc_util > 1.0:
        proc_util = proc_util / 100.0
    # OOTB: usage/limit > 85% — GT percent probes must land in the breach band.
    if proc_util > 0:
        proc_usage = int(proc_limit * max(proc_util, 0.90))
    else:
        proc_usage = int(rng.randint(50, 400) * load)
    metrics.append(build_gauge("oracledb.processes.limit", "{processes}", proc_limit))
    metrics.append(build_gauge("oracledb.processes.usage", "{processes}", proc_usage))

    sess_limit = 1200
    sess_util = float(impact_value(chaos, "session_utilisation_high", default=0.0) or 0.0)
    if sess_util > 1.0:
        sess_util = sess_util / 100.0
    if sess_util > 0:
        sess_usage_total = int(sess_limit * max(sess_util, 0.90))
    else:
        sess_usage_total = int(rng.randint(100, 600) * load)
    metrics.append(build_gauge("oracledb.sessions.limit", "{sessions}", sess_limit))
    # Per session_type/session_status breakdown (USER/BACKGROUND × ACTIVE/INACTIVE).
    # Shares must sum to 1 so SUM(usage) equals the intended total (OOTB peaks SUM).
    type_status = [(st, ss) for st in SESSION_TYPES for ss in SESSION_STATUSES]
    raw = [rng.uniform(0.1, 0.5) for _ in type_status]
    raw_sum = sum(raw) or 1.0
    session_parts: list[tuple[dict[str, str], int]] = []
    for (st, ss), share in zip(type_status, raw):
        part_usage = int(sess_usage_total * (share / raw_sum))
        part_attrs = {"session_type": st, "session_status": ss}
        session_parts.append((part_attrs, part_usage))
        metrics.append(build_gauge(
            "oracledb.sessions.usage", "{sessions}",
            part_usage,
            part_attrs,
        ))

    # ── Transaction limits / usage ──
    txn_limit = 2000
    txn_util = float(impact_value(chaos, "transaction_limit_utilisation_high", default=0.0) or 0.0)
    if txn_util > 1.0:
        txn_util = txn_util / 100.0
    if txn_util > 0:
        txn_usage = int(txn_limit * max(txn_util, 0.90))
    else:
        txn_usage = int(rng.randint(50, 500) * load)
    metrics.append(build_gauge("oracledb.transactions.limit", "{transactions}", txn_limit))
    metrics.append(build_gauge("oracledb.transactions.usage", "{transactions}", txn_usage))

    # ── Lock / resource limits and usage ──
    metrics.append(build_gauge("oracledb.dml_locks.limit", "{locks}", 1800))
    metrics.append(build_gauge("oracledb.dml_locks.usage", "{locks}", state.dml_locks_usage))
    metrics.append(build_gauge("oracledb.enqueue_locks.limit", "{locks}", 500))
    metrics.append(build_gauge("oracledb.enqueue_locks.usage", "{locks}", state.enqueue_locks_usage))
    metrics.append(build_gauge("oracledb.enqueue_resources.limit", "{resources}", 500))
    metrics.append(build_gauge("oracledb.enqueue_resources.usage", "{resources}", state.enqueue_resources_usage))

    # ── Per-tablespace size metrics (also bulk-co-located in run()) ──
    for ts in tablespaces:
        limit_bytes, usage_bytes = state.tablespaces[ts]
        ts_attr = {"tablespace_name": ts}
        metrics.append(build_gauge("oracledb.tablespace_size.limit", "By", limit_bytes, ts_attr))
        metrics.append(build_gauge("oracledb.tablespace_size.usage", "By", usage_bytes, ts_attr))

    snapshot = {
        "proc_usage": proc_usage,
        "proc_limit": proc_limit,
        "sess_limit": sess_limit,
        "session_parts": session_parts,
        "txn_usage": txn_usage,
        "txn_limit": txn_limit,
    }
    return metrics, snapshot


# ── Default query templates ────────────────────────────────────────────────────

_DEFAULT_QUERIES = [
    {"sql": "SELECT * FROM accounts WHERE status = :1 AND balance > :2", "schema": "FINANCE", "user": "APP_SCHEMA"},
    {"sql": "INSERT INTO audit_log (event_id, event_time, user_id, action) VALUES (:1, SYSDATE, :2, :3)", "schema": "AUDIT", "user": "APP_SCHEMA"},
    {"sql": "UPDATE orders SET status = :1, updated_at = SYSDATE WHERE order_id = :2", "schema": "ORDERS", "user": "APP_SCHEMA"},
    {"sql": "SELECT COUNT(*) FROM sessions WHERE user_id = :1 AND active = 1", "schema": "APP", "user": "APP_SCHEMA"},
    {"sql": "DELETE FROM temp_data WHERE created_at < SYSDATE - INTERVAL '7' DAY", "schema": "APP", "user": "APP_SCHEMA"},
]

_WAIT_CLASSES = ["User I/O", "System I/O", "Concurrency", "Configuration", "Application", "Network"]
_WAIT_EVENTS = ["db file sequential read", "latch: cache buffers chains", "log file sync", "db file scattered read"]
_CLIENT_ADDRESSES = [
    "10.0.1.24",
    "10.0.1.88",
    "10.0.2.15",
    "10.0.3.41",
    "192.168.10.12",
]


def _sql_id(rng: random.Random) -> str:
    return "".join(rng.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=13))


def _generate_logs(
    client: OTLPClient,
    service: str,
    queries: list[dict],
    rng: random.Random,
    active_chaos: dict | None = None,
    sid: str = "sid",
) -> list[dict]:
    """Build properly formatted OTLP log records via client.build_log_record."""
    logs: list[dict] = []

    chaos = active_chaos or {}
    # Drive log slow/timeout samples from SPEC alert keys.
    slow_fraction = float(impact_value(chaos, "hard_parse_ratio_high", default=0.05) or 0.05)
    if not active_chaos:
        slow_fraction = 0.05
    slow_ms = 2000
    conn_timeout_rate = float(impact_value(chaos, "session_utilisation_high", default=0.0) or 0.0)

    for q in queries:
        is_slow = rng.random() < slow_fraction
        duration_sec = (
            rng.uniform(slow_ms / 1000.0, slow_ms * 2 / 1000.0)
            if is_slow
            else rng.uniform(0.001, 0.1)
        )
        # metadata.yaml: cpu_time / elapsed_time are seconds (double)
        cpu_s = duration_sec * rng.uniform(0.2, 0.7)
        elapsed_s = duration_sec
        user_io_s = elapsed_s * rng.uniform(0.05, 0.4) if is_slow else elapsed_s * rng.uniform(0.0, 0.1)
        concurrency_s = elapsed_s * rng.uniform(0.0, 0.3) if is_slow else 0.0
        application_s = elapsed_s * rng.uniform(0.0, 0.1)
        cluster_s = elapsed_s * rng.uniform(0.0, 0.05)
        logs.append(client.build_log_record(
            severity="WARN" if is_slow else "INFO",
            body=q["sql"],
            event_name="db.server.query_sample",
            attributes={
                "db.query.text": q["sql"],
                "db.system.name": "oracle",
                "user.name": q.get("user", "APP_SCHEMA"),
                "db.namespace": q.get("schema", "APP"),
                "oracle.db.service": service,
                "client.address": rng.choice(_CLIENT_ADDRESSES),
                "oracledb.sql_id": _sql_id(rng),
                "oracledb.duration_sec": float(round(duration_sec, 4)),
                "oracledb.schemaname": q.get("schema", "APP"),
                "oracledb.program": rng.choice(_PROGRAMS),
                "oracledb.sid": sid,
                "oracledb.serial": str(rng.randint(1, 99999)),
                "oracledb.status": "ACTIVE" if is_slow else "INACTIVE",
                "oracledb.state": "WAITING" if is_slow else "ACTIVE",
                "oracledb.wait_class": rng.choice(_WAIT_CLASSES),
                "oracledb.event": rng.choice(_WAIT_EVENTS) if is_slow else "SQL*Net message from client",
                "oracledb.elapsed_time": float(round(elapsed_s, 6)),
                "oracledb.cpu_time": float(round(cpu_s, 6)),
                "oracledb.executions": rng.randint(1, 100),
                "oracledb.buffer_gets": rng.randint(10, 10_000),
                "oracledb.disk_reads": rng.randint(0, 500),
                "oracledb.rows_processed": rng.randint(0, 1_000),
                "oracledb.user_io_wait_time": float(round(user_io_s, 6)),
                "oracledb.concurrency_wait_time": float(round(concurrency_s, 6)),
                "oracledb.application_wait_time": float(round(application_s, 6)),
                "oracledb.cluster_wait_time": float(round(cluster_s, 6)),
            },
        ))

    # top_query — Query Performance dashboard (AWR-equivalent)
    tq = rng.choice(queries)
    chaos_mult = 3.0 if active_chaos else 1.0
    tq_elapsed = float(rng.uniform(1.0, 120.0) * chaos_mult)
    tq_cpu = float(tq_elapsed * rng.uniform(0.3, 0.8))
    tq_user_io = float(tq_elapsed * rng.uniform(0.05, 0.35))
    tq_concurrency = float(tq_elapsed * rng.uniform(0.0, 0.25))
    tq_application = float(tq_elapsed * rng.uniform(0.0, 0.1))
    tq_cluster = float(tq_elapsed * rng.uniform(0.0, 0.05))
    tq_executions = rng.randint(100, 10_000)
    tq_disk = rng.randint(0, 50_000)
    logs.append(client.build_log_record(
        severity="INFO",
        body=tq["sql"],
        event_name="db.server.top_query",
        attributes={
            "db.query.text": tq["sql"],
            "db.system.name": "oracle",
            "db.namespace": tq.get("schema", "APP"),
            "oracle.db.service": service,
            "oracledb.sql_id": _sql_id(rng),
            "oracledb.elapsed_time": float(round(tq_elapsed, 6)),
            "oracledb.cpu_time": float(round(tq_cpu, 6)),
            "oracledb.executions": tq_executions,
            "oracledb.buffer_gets": rng.randint(10_000, 1_000_000),
            "oracledb.disk_reads": tq_disk,
            "oracledb.rows_processed": rng.randint(0, 100_000),
            "oracledb.user_io_wait_time": float(round(tq_user_io, 6)),
            "oracledb.concurrency_wait_time": float(round(tq_concurrency, 6)),
            "oracledb.application_wait_time": float(round(tq_application, 6)),
            "oracledb.cluster_wait_time": float(round(tq_cluster, 6)),
            "oracledb.physical_read_bytes": int(tq_disk * rng.randint(4096, 16384)),
            "oracledb.physical_read_requests": int(tq_disk),
            "oracledb.physical_write_bytes": rng.randint(0, 50_000_000),
            "oracledb.physical_write_requests": rng.randint(0, 5_000),
        },
    ))

    # ORA-12170: TNS connection timeout — emitted when connection_timeout_rate > 0
    if conn_timeout_rate > 0 and rng.random() < conn_timeout_rate:
        client_ip = f"10.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}"
        logs.append(client.build_log_record(
            severity="ERROR",
            body="/* ORA-12170 TNS:Connect timeout */",
            event_name="db.server.query_sample",
            attributes={
                "db.system.name": "oracle",
                "oracle.db.service": service,
                "oracledb.status": "KILLED",
                "oracledb.state": "WAITING",
                "oracledb.wait_class": "Network",
                "oracledb.event": "SQL*Net message from client",
                "db.query.text": "/* ORA-12170 TNS:Connect timeout */",
                "error.type": "ORA-12170",
                "error.message": f"TNS:Connect timeout occurred connecting from {client_ip}",
                "client.address": client_ip,
            },
        ))

    return logs



from generators.oracle_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run Oracle generator loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {})
    else:
        ns = NAMESPACE
        cfg = {}

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    db_prefix = ns.upper().replace("-", "_")
    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("oracle-db")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("oracle-host")
    instance_name = cfg.get("instance_name") or ScenarioIdentity(ns).database("DB")
    db_service = cfg.get("db_service") or instance_name
    tablespaces: list[str] = cfg.get("tablespaces") or _default_tablespaces(db_prefix)
    queries: list[dict] = cfg.get("queries") or _DEFAULT_QUERIES
    cloud = cfg.get("cloud") or _default_cloud()



    resource = build_signal_resource(
        ns=ns,
        signal_type="metrics",
        dataset="oracledbreceiver",
        service_name=service_name,
        service_version="19.3.0",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name,
        cloud=cloud,
        extra_attrs={
            "server.address": host_name,
            "oracledb.instance.name": instance_name,
        },
    )
    log_resource = build_signal_resource(
        ns=ns,
        signal_type="logs",
        dataset="oracledbreceiver",
        service_name=service_name,
        service_version="19.3.0",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name,
        cloud=cloud,
        extra_attrs={
            "server.address": host_name,
            "oracledb.instance.name": instance_name,
        },
    )

    metric_state = OracleMetricState(rng)
    scope = {"name": ORACLE_SCOPE, "version": SCOPE_VERSION}
    # Secondary transport for AND-filtered co-located metric documents.
    try:
        es_bulk = ESBulkClient()
    except Exception as exc:  # pragma: no cover - missing http2/deps in some host envs
        logger.warning("ESBulkClient unavailable (%s); co-located docs disabled", exc)
        es_bulk = None

    # Sticky long mapping from earlier bulk docs without counter_long templates.
    _reset_ratio_counter_mapping()

    logger.info("Oracle generator started (namespace=%s, instance=%s)", ns, instance_name)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="oracle_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        metrics, gauge_snap = _generate_metrics(
            metric_state, tablespaces, rng, instance_name, active_chaos,
        )
        client.send_metrics_scoped(resource, scope, metrics)

        if es_bulk is not None and es_bulk.configured:
            ts_ms = int(time.time() * 1000)
            ratio_docs: list[dict[str, Any]] = [
                build_buffer_cache_ratio_doc(
                    resource,
                    metric_state.physical_reads,
                    metric_state.logical_reads,
                    timestamp_ms=ts_ms,
                ),
                build_hard_parse_ratio_doc(
                    resource,
                    metric_state.hard_parses,
                    metric_state.parse_calls,
                    timestamp_ms=ts_ms,
                ),
                build_rollback_ratio_doc(
                    resource,
                    metric_state.user_commits,
                    metric_state.user_rollbacks,
                    timestamp_ms=ts_ms,
                ),
                build_deadlock_doc(
                    resource,
                    metric_state.enqueue_deadlocks,
                    timestamp_ms=ts_ms,
                ),
            ]
            # OTel intake sets counter_long via bulk dynamic_templates; plain
            # create actions map these as long and break RATE() panels.
            es_bulk.send_bulk(
                _METRICS_DATA_STREAM,
                ratio_docs,
                dynamic_templates={
                    **_RATIO_COUNTER_DYNAMIC_TEMPLATES,
                    **_TXN_COUNTER_DYNAMIC_TEMPLATES,
                },
            )
            capacity_docs: list[dict[str, Any]] = [
                build_usage_limit_doc(
                    resource,
                    "oracledb.processes.usage",
                    "oracledb.processes.limit",
                    gauge_snap["proc_usage"],
                    gauge_snap["proc_limit"],
                    unit="{processes}",
                    timestamp_ms=ts_ms,
                ),
                build_usage_limit_doc(
                    resource,
                    "oracledb.transactions.usage",
                    "oracledb.transactions.limit",
                    gauge_snap["txn_usage"],
                    gauge_snap["txn_limit"],
                    unit="{transactions}",
                    timestamp_ms=ts_ms,
                ),
                build_usage_limit_doc(
                    resource,
                    "oracledb.dml_locks.usage",
                    "oracledb.dml_locks.limit",
                    metric_state.dml_locks_usage,
                    1800,
                    unit="{locks}",
                    timestamp_ms=ts_ms,
                ),
                build_usage_limit_doc(
                    resource,
                    "oracledb.enqueue_locks.usage",
                    "oracledb.enqueue_locks.limit",
                    metric_state.enqueue_locks_usage,
                    500,
                    unit="{locks}",
                    timestamp_ms=ts_ms,
                ),
                build_usage_limit_doc(
                    resource,
                    "oracledb.enqueue_resources.usage",
                    "oracledb.enqueue_resources.limit",
                    metric_state.enqueue_resources_usage,
                    500,
                    unit="{resources}",
                    timestamp_ms=ts_ms,
                ),
            ]
            capacity_docs.extend(
                build_session_usage_docs(
                    resource,
                    gauge_snap["sess_limit"],
                    gauge_snap["session_parts"],
                    timestamp_ms=ts_ms,
                )
            )
            es_bulk.send_bulk(
                _METRICS_DATA_STREAM,
                capacity_docs,
                dynamic_templates=_CAPACITY_GAUGE_DYNAMIC_TEMPLATES,
            )
            tablespace_docs: list[dict[str, Any]] = []
            for ts_name, (limit_bytes, usage_bytes) in metric_state.tablespaces.items():
                tablespace_docs.append(build_tablespace_utilization_doc(
                    resource,
                    ts_name,
                    usage_bytes,
                    limit_bytes,
                    timestamp_ms=ts_ms,
                ))
            if tablespace_docs:
                es_bulk.send_bulk(
                    _METRICS_DATA_STREAM,
                    tablespace_docs,
                    dynamic_templates=_TABLESPACE_GAUGE_DYNAMIC_TEMPLATES,
                )

        logs = _generate_logs(
            client, db_service, queries, rng, active_chaos,
            sid=ScenarioIdentity(ns).label("sid"),
        )
        client.send_logs_scoped(log_resource, scope, logs)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    if es_bulk is not None:
        es_bulk.close()
    logger.info("Oracle generator stopped.")


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
