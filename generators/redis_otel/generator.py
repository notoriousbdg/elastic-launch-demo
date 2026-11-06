#!/usr/bin/env python3
"""Redis Generator — sends redisreceiver OTel metrics via OTLP.

Emits data matching the redis_otel content package
(discovery.datasets: redisreceiver.otel), which populates the bundled
Overview and Memory & Persistence dashboards.

Signal types:
  - Metrics: redis.clients.connected, redis.clients.blocked, redis.commands,
             redis.memory.used, redis.memory.rss, redis.memory.peak,
             redis.memory.fragmentation_ratio, redis.maxmemory,
             redis.connections.received, redis.connections.rejected,
             redis.keys.evicted, redis.keys.expired,
             redis.keyspace.hits, redis.keyspace.misses,
             redis.slaves.connected, redis.latest_fork,
             redis.rdb.changes_since_last_save,
             redis.net.input, redis.net.output,
             redis.db.keys, redis.db.expires, redis.db.avg_ttl,
             redis.cpu.time, redis.replication.offset,
             redis.replication.backlog_first_byte_offset,
             redis.cmd.latency (attributes cmd, percentile p50/p99/p99.9)

Also bulk-indexes co-located documents each tick (OTLP is one-metric-per-doc):
  - ``redis.memory.used`` + ``redis.maxmemory`` (Memory Utilization Gauge)
  - ``redis.db.keys`` + ``redis.db.expires`` + ``redis.db.avg_ttl`` with
    ``attributes.db`` (Keys per Database With TTL / Avg TTL columns)

Field names and attribute keys come from:
  - opentelemetry-collector-contrib/receiver/redisreceiver/metadata.yaml
  - elastic/integrations/packages/redis_otel/kibana/dashboard/*.json

Usage (standalone):
    python3 -m generators.redis_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/redis_otel@0.3.2"

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
    "redis.clients.connected",
    "redis.clients.blocked",
    "redis.commands",
    "redis.memory.used",
    "redis.memory.rss",
    "redis.memory.peak",
    "redis.memory.fragmentation_ratio",
    "redis.maxmemory",
    "redis.connections.received",
    "redis.connections.rejected",
    "redis.keys.evicted",
    "redis.keys.expired",
    "redis.keyspace.hits",
    "redis.keyspace.misses",
    "redis.slaves.connected",
    "redis.latest_fork",
    "redis.rdb.changes_since_last_save",
    "redis.net.input",
    "redis.net.output",
    "redis.db.keys",
    "redis.db.expires",
    "redis.db.avg_ttl",
    "redis.cpu.time",
    "redis.replication.offset",
    "redis.replication.backlog_first_byte_offset",
    "redis.cmd.latency",
)

REDIS_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/redisreceiver"
)
SCOPE_VERSION = "0.115.0"

CPU_STATES = ("user", "sys")
DB_ATTR_KEY = "db"

_METRICS_DATA_STREAM = "metrics-redisreceiver.otel-default"

# redis.cmd.latency (INFO latencystats): one gauge per command x percentile, in
# seconds. The package SLO "P99 command latency" filters percentile: "p99" and
# needs MAX(redis.cmd.latency) < 0.01 s per server.
LATENCY_COMMANDS = ("get", "set", "hget", "hset", "lpush", "expire")
LATENCY_PERCENTILES = ("p50", "p99", "p99.9")
_P99_HEALTHY_RANGE_S = (0.0003, 0.004)
_P99_DEGRADED_RANGE_S = (0.02, 0.08)

# Overview RATE/INCREASE panels require counter_* — plain long from bulk create fails.
_COUNTER_LONG_FIELDS = frozenset(
    {
        "redis.connections.received",
        "redis.connections.rejected",
        "redis.keys.evicted",
        "redis.keys.expired",
        "redis.keyspace.hits",
        "redis.keyspace.misses",
        "redis.net.input",
        "redis.net.output",
    }
)
_COUNTER_DOUBLE_FIELDS = frozenset({"redis.cpu.time"})

logger = logging.getLogger("redis-generator")

BATCH_INTERVAL_MIN = 5
BATCH_INTERVAL_MAX = 15


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_ec2",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


class RedisMetricState(CounterState):
    """Cumulative counter state for Redis."""

    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.connections_received = rng.randint(100_000, 50_000_000)
        # OOTB alerts fire on absolute SUM/MAX > 0 for these — must start at 0
        # and only grow when the matching chaos fault is active (see tick()).
        self.connections_rejected = 0
        self.keys_evicted = 0
        self.keys_expired = rng.randint(0, 2_000_000)
        self.keyspace_hits = rng.randint(1_000_000, 500_000_000)
        self.keyspace_misses = rng.randint(50_000, 50_000_000)
        self.net_input = rng.randint(10_000_000, 10_000_000_000)
        self.net_output = rng.randint(10_000_000, 10_000_000_000)
        self.cpu_time = {s: rng.uniform(100.0, 50_000.0) for s in CPU_STATES}
        self.replication_offset = rng.randint(1_000_000, 500_000_000)
        self.rdb_changes = rng.randint(0, 100_000)
        self.last_evicted_delta = 0
        self.last_rejected_delta = 0

    def tick(
        self,
        rng: random.Random,
        *,
        eviction_multiplier: float = 1.0,
        connection_multiplier: float = 1.0,
    ):
        load = business_load()
        rate = max(1, int(load * 1000))

        self.connections_received += rng.randint(1, 50)
        # Healthy: no new rejections. Fault: reject when connection_multiplier > 1.
        if connection_multiplier > 1.0:
            self.connections_rejected += max(1, int(rng.randint(1, 5) * connection_multiplier))
        self.keys_evicted += (
            max(0, int(rng.randint(50, 150) * eviction_multiplier))
            if eviction_multiplier > 1.0
            else 0
        )
        self.keys_expired += rng.randint(0, 100)
        self.keyspace_hits += int(rate * 10)
        self.keyspace_misses += int(rate * rng.uniform(0.05, 0.2))
        self.net_input += rng.randint(1_000, 1_000_000)
        self.net_output += rng.randint(1_000, 5_000_000)
        for s in CPU_STATES:
            self.cpu_time[s] += rng.uniform(0.001, 0.5)
        self.replication_offset += rng.randint(100, 10_000)
        self.rdb_changes += rng.randint(0, 50)


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


def build_memory_utilization_doc(
    resource: dict,
    mem_used: int,
    maxmemory: int,
    *,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc carrying both memory gauges.

    The Memory Utilization Gauge panel AND-filters
    ``redis.memory.used`` and ``redis.maxmemory``. OTLP intake stores one
    metric per document, so those filters never match OTLP-only data. A
    co-located bulk document satisfies the panel without changing the
    dashboard.
    """
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "By",
        "data_stream": {
            "type": "metrics",
            "dataset": "redisreceiver.otel",
            "namespace": "default",
        },
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": REDIS_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "redis.memory.used": int(mem_used),
            "redis.maxmemory": int(maxmemory),
        },
    }


def build_db_keys_doc(
    resource: dict,
    db: int | str,
    keys: int,
    expires: int,
    avg_ttl: int,
    *,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc co-locating per-db keyspace gauges.

    Keys per Database AND-filters ``redis.db.keys`` / ``expires`` / ``avg_ttl``
    with ``attributes.db``. OTLP stores one metric per document, so With TTL /
    Avg TTL are null unless siblings share a document.
    """
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "1",
        "data_stream": {
            "type": "metrics",
            "dataset": "redisreceiver.otel",
            "namespace": "default",
        },
        "attributes": {DB_ATTR_KEY: str(db)},
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": REDIS_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "redis.db.keys": int(keys),
            "redis.db.expires": int(expires),
            "redis.db.avg_ttl": int(avg_ttl),
        },
    }


def build_overview_metrics_doc(
    resource: dict,
    overview: dict[str, Any],
    *,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """ES bulk companion for Overview panels when OTLP metrics ingest fails.

    OTLP often accepts 200 but never indexes on the demo ES (ALERTS #96). The
    memory/db companions alone leave Overview empty (clients/commands/etc.).
    """
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "1",
        "data_stream": {
            "type": "metrics",
            "dataset": "redisreceiver.otel",
            "namespace": "default",
        },
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": REDIS_SCOPE, "version": SCOPE_VERSION},
        "metrics": {f"redis.{k}": v for k, v in overview.items()},
    }


def build_cpu_time_docs(
    resource: dict,
    cpu_time: dict[str, float],
    *,
    timestamp_ms: int | None = None,
) -> list[dict[str, Any]]:
    """ES bulk companions for Memory & Persistence ``RATE(redis.cpu.time)``."""
    ts = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
    base = {
        "@timestamp": ts,
        "start_timestamp": ts - 60_000,
        "unit": "s",
        "temporality": "cumulative",
        "data_stream": {
            "type": "metrics",
            "dataset": "redisreceiver.otel",
            "namespace": "default",
        },
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": REDIS_SCOPE, "version": SCOPE_VERSION},
    }
    return [
        {
            **base,
            "attributes": {"state": state},
            "metrics": {"redis.cpu.time": float(value)},
        }
        for state, value in cpu_time.items()
    ]


def _bulk_dynamic_templates(docs: list[dict[str, Any]]) -> dict[str, str]:
    """Map RATE/INCREASE fields as counters; other bulk metrics as gauges."""
    templates: dict[str, str] = {}
    for doc in docs:
        for name, value in (doc.get("metrics") or {}).items():
            path = f"metrics.{name}"
            if name in _COUNTER_LONG_FIELDS:
                templates[path] = "counter_long"
            elif name in _COUNTER_DOUBLE_FIELDS:
                templates[path] = "counter_double"
            elif isinstance(value, float):
                templates[path] = "gauge_double"
            else:
                templates[path] = "gauge_long"
    return templates


def _counters_need_remap(elastic_url: str, api_key: str) -> bool:
    """True when RATE/INCREASE fields are sticky plain ``long`` (not counter_*)."""
    import httpx

    names = sorted(_COUNTER_LONG_FIELDS | _COUNTER_DOUBLE_FIELDS)
    field_list = ",".join(f"metrics.{n}" for n in names)
    url = f"{elastic_url.rstrip('/')}/{_METRICS_DATA_STREAM}/_field_caps?fields={field_list}"
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
    for name in names:
        caps = fields.get(f"metrics.{name}") or fields.get(name) or {}
        for meta in caps.values():
            mtype = meta.get("type")
            tsm = meta.get("time_series_metric")
            if mtype in ("counter_long", "counter_double") or tsm == "counter":
                continue
            if mtype in ("long", "double", "float") and tsm != "counter":
                return True
    return False


def _reset_redis_metrics_stream(elastic_url: str, api_key: str) -> None:
    """Delete the redis metrics stream so counters remap as counter_*."""
    import httpx

    url = f"{elastic_url.rstrip('/')}/_data_stream/{_METRICS_DATA_STREAM}"
    try:
        resp = httpx.delete(
            url,
            headers={"Authorization": f"ApiKey {api_key}"},
            timeout=30,
        )
    except Exception as exc:
        logger.warning("Could not delete %s: %s", _METRICS_DATA_STREAM, exc)
        return
    if resp.status_code in (200, 404):
        logger.info(
            "Reset %s so RATE/INCREASE fields remap as counters",
            _METRICS_DATA_STREAM,
        )
    else:
        logger.warning(
            "Delete %s failed (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )


def build_cmd_latency_metrics(rng: random.Random, degraded: bool = False) -> list[dict]:
    """``redis.cmd.latency`` gauges per command and percentile (seconds).

    Healthy p99 stays well under the SLO's 10 ms; ``degraded`` (memory
    swapping) pushes p99 into tens of milliseconds as page faults stall the
    event loop. p50 <= p99 <= p99.9 always holds.
    """
    lo, hi = _P99_DEGRADED_RANGE_S if degraded else _P99_HEALTHY_RANGE_S
    out: list[dict] = []
    for cmd in LATENCY_COMMANDS:
        p99 = rng.uniform(lo, hi)
        values = {"p50": p99 * rng.uniform(0.1, 0.3), "p99": p99, "p99.9": p99 * rng.uniform(1.2, 2.0)}
        for pct in LATENCY_PERCENTILES:
            out.append(build_gauge(
                "redis.cmd.latency", "s", round(values[pct], 6),
                {"cmd": cmd, "percentile": pct},
            ))
    return out


def _generate_metrics(
    state: RedisMetricState,
    keyspaces: list[int],
    rng: random.Random,
    maxmemory: int,
    active_chaos: dict | None = None,
) -> tuple[list[dict], int, int, list[tuple[int, int, int, int]], dict[str, Any]]:
    """Return ``(otlp_metrics, mem_used, maxmemory, keyspace_stats, overview)``.

    ``keyspace_stats`` is a list of ``(db, keys, expires, avg_ttl)`` for bulk
    co-located docs (OTLP gauges are still emitted separately).
    ``overview`` feeds the ES bulk Overview companion when OTLP never indexes.
    """
    chaos = active_chaos or {}
    eviction_mult = float(impact_value(chaos, "high_eviction_rate", default=1.0) or 1.0)
    conn_mult = float(impact_value(chaos, "rejected_connections", default=1.0) or 1.0)
    memory_pressure = float(impact_value(chaos, "memory_approaching_maxmemory", default=0.0) or 0.0)
    # Do not fold fragmentation into memory_pressure — that companion key is often
    # present as SPEC default 0.0 / GT severity 10.0 and must not drive maxmemory.
    fragmentation_pressure = float(
        impact_value(chaos, "memory_fragmentation_indicates_swapping", default=0.0) or 0.0
    )
    replica_disconnect = float(impact_value(chaos, "primary_has_no_connected_replicas", default=0.0) or 0.0)
    rejected_key = float(chaos.get("rejected_connections", 0.0) or 0.0)
    if rejected_key > 0:
        conn_mult = max(conn_mult, rejected_key)

    prev_evicted = state.keys_evicted
    prev_rejected = state.connections_rejected
    state.tick(rng, eviction_multiplier=eviction_mult, connection_multiplier=conn_mult)
    metrics: list[dict] = []
    load = business_load()

    # ── Connection / client gauges ──
    connected = int(rng.randint(10, 500) * load)
    if conn_mult > 1.0:
        connected = int(connected * conn_mult)
    blocked = rng.randint(0, 2)
    if conn_mult > 1.0:
        blocked = rng.randint(5, 50)
    metrics.append(build_gauge("redis.clients.connected", "{client}", connected))
    metrics.append(build_gauge("redis.clients.blocked", "{client}", blocked))

    # ── Throughput gauge ──
    ops_per_sec = int(rng.randint(1_000, 50_000) * load)
    metrics.append(build_gauge("redis.commands", "{ops}/s", ops_per_sec))

    # ── Memory gauges ──
    # OOTB swapping alert: AVG(fragmentation_ratio) < 1.0 (RSS < used).
    # SPEC default 100.0 = healthy; GT severity like 10.0 means "force swap".
    # A probe in (0, 1) is an explicit target ratio. Treat >= 100 as healthy so
    # injected SPEC defaults never keep the swapping branch latched on.
    swap_fault = (0 < fragmentation_pressure < 100.0)
    if memory_pressure > 0:
        # OOTB ES|QL uses ``used / max_mem * 100.0`` with long division first, so
        # any used < maxmemory evaluates to usage_pct=0. Breach requires used >= maxmemory
        # (integer quotient >= 1 → usage_pct >= 100). Keep RSS healthy here; swapping is
        # driven only by memory_fragmentation_indicates_swapping.
        mem_used = int(maxmemory * rng.uniform(1.0, 1.05))
        mem_rss = int(mem_used * rng.uniform(1.05, 1.3))
    elif 0 < fragmentation_pressure < 1.0:
        mem_used = int(maxmemory * rng.uniform(0.5, 0.75))
        mem_rss = int(mem_used * fragmentation_pressure)
    elif swap_fault:
        mem_used = int(maxmemory * rng.uniform(0.5, 0.75))
        # Swapping: RSS must be below used memory → frag ratio < 1.0
        mem_rss = int(mem_used * rng.uniform(0.55, 0.95))
    else:
        mem_used = int(maxmemory * rng.uniform(0.3, 0.75))
        mem_rss = int(mem_used * rng.uniform(1.1, 1.4))
    mem_peak = max(mem_used, int(mem_used * rng.uniform(1.0, 1.2)))
    frag_ratio = mem_rss / mem_used if mem_used else 1.0
    metrics.append(build_gauge("redis.memory.used", "By", mem_used))
    metrics.append(build_gauge("redis.memory.rss", "By", mem_rss))
    metrics.append(build_gauge("redis.memory.peak", "By", mem_peak))
    metrics.append(build_gauge("redis.memory.fragmentation_ratio", "1", round(frag_ratio, 3)))
    metrics.append(build_gauge("redis.maxmemory", "By", maxmemory))

    # Expose per-tick deltas for unit tests (INCREASE over a window in Kibana).
    state.last_evicted_delta = state.keys_evicted - prev_evicted
    state.last_rejected_delta = state.connections_rejected - prev_rejected

    # ── Cumulative counters ──
    metrics.append(build_cumulative_sum(
        "redis.connections.received", "{connection}",
        state.connections_received, state.start_time_ns,
    ))
    metrics.append(build_cumulative_sum(
        "redis.connections.rejected", "{connection}",
        state.connections_rejected, state.start_time_ns,
    ))
    metrics.append(build_cumulative_sum(
        "redis.keys.evicted", "{key}",
        state.keys_evicted, state.start_time_ns,
    ))
    metrics.append(build_cumulative_sum(
        "redis.keys.expired", "{event}",
        state.keys_expired, state.start_time_ns,
    ))
    metrics.append(build_cumulative_sum(
        "redis.keyspace.hits", "{hit}",
        state.keyspace_hits, state.start_time_ns,
    ))
    metrics.append(build_cumulative_sum(
        "redis.keyspace.misses", "{miss}",
        state.keyspace_misses, state.start_time_ns,
    ))
    metrics.append(build_cumulative_sum("redis.net.input", "By", state.net_input, state.start_time_ns))
    metrics.append(build_cumulative_sum("redis.net.output", "By", state.net_output, state.start_time_ns))

    # ── CPU time (per state) ──
    for cpu_state in CPU_STATES:
        metrics.append(build_cumulative_sum(
            "redis.cpu.time", "s",
            round(state.cpu_time[cpu_state], 3),
            state.start_time_ns,
            {"state": cpu_state},
        ))

    # ── Replication ──
    metrics.append(build_gauge("redis.replication.offset", "By", state.replication_offset))
    backlog_offset = max(0, state.replication_offset - rng.randint(1_000, 100_000))
    metrics.append(build_gauge("redis.replication.backlog_first_byte_offset", "By", backlog_offset))
    slaves = 0 if replica_disconnect > 0 else rng.randint(1, 3)
    metrics.append(build_gauge("redis.slaves.connected", "{replica}", slaves))

    # ── Command latency percentiles ──
    metrics.extend(build_cmd_latency_metrics(rng, degraded=swap_fault))

    # ── RDB persistence ──
    latest_fork = rng.randint(100, 5_000)
    metrics.append(build_gauge("redis.latest_fork", "us", latest_fork))
    metrics.append(build_gauge("redis.rdb.changes_since_last_save", "{change}", state.rdb_changes))

    # ── Per-keyspace metrics (attribute: db) ──
    keyspace_stats: list[tuple[int, int, int, int]] = []
    for ks in keyspaces:
        db_attr = {DB_ATTR_KEY: str(ks)}
        keys = rng.randint(100, 500_000)
        expires = int(keys * rng.uniform(0.1, 0.5))
        avg_ttl = rng.randint(60_000, 3_600_000)
        metrics.append(build_gauge("redis.db.keys", "{key}", keys, db_attr))
        metrics.append(build_gauge("redis.db.expires", "{key}", expires, db_attr))
        metrics.append(build_gauge("redis.db.avg_ttl", "ms", avg_ttl, db_attr))
        keyspace_stats.append((ks, keys, expires, avg_ttl))

    overview = {
        "clients.connected": int(connected),
        "clients.blocked": int(blocked),
        "commands": int(ops_per_sec),
        "memory.used": int(mem_used),
        "memory.rss": int(mem_rss),
        "memory.peak": int(mem_peak),
        "memory.fragmentation_ratio": float(round(frag_ratio, 3)),
        "maxmemory": int(maxmemory),
        "connections.received": int(state.connections_received),
        "connections.rejected": int(state.connections_rejected),
        "keys.evicted": int(state.keys_evicted),
        "keys.expired": int(state.keys_expired),
        "keyspace.hits": int(state.keyspace_hits),
        "keyspace.misses": int(state.keyspace_misses),
        "net.input": int(state.net_input),
        "net.output": int(state.net_output),
        "replication.offset": int(state.replication_offset),
        "replication.backlog_first_byte_offset": int(backlog_offset),
        "slaves.connected": int(slaves),
        "latest_fork": int(latest_fork),
        "rdb.changes_since_last_save": int(state.rdb_changes),
    }

    return metrics, mem_used, maxmemory, keyspace_stats, overview


from generators.redis_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run Redis generator loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {})
    else:
        ns = NAMESPACE
        cfg = {}

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("redis-cache")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("redis-host")
    keyspaces: list[int] = cfg.get("keyspaces") or [0]
    maxmemory: int = cfg.get("maxmemory_bytes", 2 * 1024 * 1024 * 1024)  # 2 GiB default
    cloud = cfg.get("cloud") or _default_cloud()

    resource = build_signal_resource(
        ns=ns,
        signal_type="metrics",
        dataset="redisreceiver",
        service_name=service_name,
        service_version="7.2.3",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name,
        cloud=cloud,
        extra_attrs={"server.address": host_name, "server.port": 6379},
    )

    metric_state = RedisMetricState(rng)
    scope = {"name": REDIS_SCOPE, "version": SCOPE_VERSION}
    # Secondary transport for co-located multi-metric panel docs.
    es_bulk = ESBulkClient()
    if es_bulk.configured and _counters_need_remap(es_bulk.elastic_url, es_bulk.api_key):
        _reset_redis_metrics_stream(es_bulk.elastic_url, es_bulk.api_key)

    logger.info("Redis generator started (namespace=%s, keyspaces=%s)", ns, keyspaces)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="redis_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        metrics, mem_used, maxmem, keyspace_stats, overview = _generate_metrics(
            metric_state, keyspaces, rng, maxmemory, active_chaos,
        )
        client.send_metrics_scoped(resource, scope, metrics)
        if es_bulk.configured:
            bulk_docs = [
                build_memory_utilization_doc(resource, mem_used, maxmem),
                build_overview_metrics_doc(resource, overview),
            ]
            bulk_docs.extend(
                build_db_keys_doc(resource, db, keys, expires, avg_ttl)
                for db, keys, expires, avg_ttl in keyspace_stats
            )
            bulk_docs.extend(
                build_cpu_time_docs(resource, metric_state.cpu_time)
            )
            es_bulk.send_bulk(
                _METRICS_DATA_STREAM,
                bulk_docs,
                dynamic_templates=_bulk_dynamic_templates(bulk_docs),
            )

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    es_bulk.close()
    logger.info("Redis generator stopped.")


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
