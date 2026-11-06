#!/usr/bin/env python3
"""Redis Enterprise Generator — sends Redis Enterprise cluster metrics via OTLP.

Emits data matching the redisenterprise_otel content package
(discovery.datasets: redisenterprise.otel).

Stock package dashboards call AVG/SUM/MAX on many series and AND-filter or
EVAL multiple fields on the same row. Monotonic OTLP sums map to
``counter_long`` and reject AVG/SUM; OTLP also stores one metric per
document so sibling fields are null unless co-located. This generator:

  - Emits dashboard AVG/SUM series as OTLP gauge ``asDouble``
  - Bulk-indexes co-located docs (``gauge_double``) for multi-metric panels
  - Deletes sticky ``counter_long`` / ``long`` mappings on startup when needed

Usage (standalone):
    python3 -m generators.redisenterprise_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/redisenterprise_otel@0.3.2"

import logging
import os
import random
import signal
import threading
import time
from typing import Any

from app.telemetry import ESBulkClient, OTLPClient
from app.config import NAMESPACE
try:
    from generators.redisenterprise_otel import SPEC
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
    stable_counter_start_ns,
)

RE_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("redisenterprise-generator")
EMIT_INTERVAL_S = 30.0

_METRICS_DATA_STREAM = "metrics-redisenterprise.otel-default"
_DATASET = "redisenterprise.otel"

# Fields that stock dashboards AVG/SUM/MAX/EVAL — must map as gauge_double.
_GAUGE_DOUBLE_FIELDS = (
    # Overview / database
    "bdb_up",
    "bdb_avg_latency",
    "bdb_avg_latency_max",
    "bdb_avg_read_latency",
    "bdb_avg_write_latency",
    "bdb_avg_other_latency",
    "bdb_avg_read_latency_max",
    "bdb_avg_write_latency_max",
    "bdb_avg_other_latency_max",
    "bdb_instantaneous_ops_per_sec",
    "bdb_used_memory",
    "bdb_memory_limit",
    "bdb_mem_frag_ratio",
    "bdb_conns",
    "bdb_shards_used",
    "bdb_no_of_keys",
    "bdb_no_of_expires",
    "bdb_read_hits",
    "bdb_read_misses",
    "bdb_evicted_objects",
    "bdb_expired_objects",
    "bdb_read_req",
    "bdb_write_req",
    "bdb_other_req",
    "bdb_ingress_bytes",
    "bdb_egress_bytes",
    "bdb_shard_cpu_user",
    "bdb_shard_cpu_system",
    # Node
    "node_up",
    "node_cpu_user",
    "node_cpu_system",
    "node_cpu_idle",
    "node_cpu_iowait",
    "node_free_memory",
    "node_available_memory",
    "node_ephemeral_storage_avail",
    "node_persistent_storage_avail",
    "node_conns",
    "node_total_req",
    "node_ingress_bytes",
    "node_egress_bytes",
    "node_cur_aof_rewrites",
    "node_cert_expiration_seconds",
    # Proxy / listener
    "dmcproxy_process_cpu_usage_percent",
    "dmcproxy_process_open_fds",
    "dmcproxy_process_max_fds",
    "dmcproxy_process_resident_memory_bytes",
    "dmcproxy_process_virtual_memory_bytes",
    "listener_acc_latency",
    "listener_acc_read_latency",
    "listener_acc_write_latency",
    "listener_total_req",
    "listener_read_req",
    "listener_write_req",
    "listener_conns",
    "listener_auth_errors",
    "listener_max_connections_exceeded",
    # Shard
    "redis_up",
    "redis_used_memory",
    "redis_used_memory_rss",
    "redis_maxmemory",
    "redis_mem_fragmentation_ratio",
    "redis_instantaneous_ops_per_sec",
    "redis_process_cpu_usage_percent",
    "redis_connected_clients",
    "redis_maxclients",
    "redis_blocked_clients",
    "redis_total_error_replies",
    "redis_current_cow_size",
    "redis_current_fork_perc",
    "redis_rdb_changes_since_last_save",
    "redis_eventloop_duration_sum",
    "redis_active_defrag_running",
    "redis_aof_rewrite_in_progress",
    "redis_rdb_bgsave_in_progress",
)

METRIC_NAMES = _GAUGE_DOUBLE_FIELDS


def _default_cloud() -> dict:
    return {"provider": "aws", "platform": "aws_ec2",
            "region": "us-east-1", "availability_zone": "us-east-1a"}


def _re_gauge(name: str, unit: str, value: int | float, attributes: dict | None = None) -> dict:
    """OTLP gauge with asDouble (AVG/SUM panels reject counter_long)."""
    return build_gauge(name, unit, float(value), attributes or {})


def _resource_attr_map(resource: dict) -> dict[str, Any]:
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


def _bulk_doc(
    resource: dict,
    metrics: dict[str, int | float],
    *,
    attributes: dict[str, Any] | None = None,
    unit: str = "1",
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """One ES metrics doc with several fields co-located as gauge_double."""
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": unit,
        "data_stream": {
            "type": "metrics",
            "dataset": _DATASET,
            "namespace": "default",
        },
        "attributes": dict(attributes or {}),
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": RE_SCOPE, "version": SCOPE_VERSION},
        "metrics": {name: float(value) for name, value in metrics.items()},
    }


def _gauge_templates_for_docs(docs: list[dict[str, Any]]) -> dict[str, str]:
    templates: dict[str, str] = {}
    for doc in docs:
        for name in doc.get("metrics", {}):
            templates[f"metrics.{name}"] = "gauge_double"
    return templates


def _fields_need_mapping_reset(elastic_url: str, api_key: str) -> bool:
    """True when dashboard gauges are counter_long or plain long without gauge.

    Healthy ``long`` + ``time_series_metric=gauge`` is NOT poison. Sticky
    ``counter_long`` / counter annotation, or bare ``long`` without gauge, is.
    """
    import httpx

    field_list = ",".join(f"metrics.{name}" for name in _GAUGE_DOUBLE_FIELDS)
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
    for name in _GAUGE_DOUBLE_FIELDS:
        caps = fields.get(f"metrics.{name}") or fields.get(name) or {}
        for meta in caps.values():
            mtype = meta.get("type")
            tsm = meta.get("time_series_metric")
            if tsm == "counter" or mtype == "counter_long":
                return True
            if mtype == "long" and tsm != "gauge":
                return True
    return False


def _dimensions_need_widen(elastic_url: str, api_key: str) -> bool:
    """True when listener/shard series lack required dimension attributes.

    Listener Detail needs ``endpoint`` / ``port`` / ``node``; Shard Detail needs
    ``node``. Narrower TSDB dimension sets reject later wider bulks forever.
    """
    import httpx

    field_list = ",".join(
        (
            "attributes.endpoint",
            "attributes.port",
            "attributes.node",
            "metrics.listener_total_req",
            "metrics.redis_up",
        )
    )
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
        logger.warning("Could not inspect %s dimension caps: %s", _METRICS_DATA_STREAM, exc)
        return False
    if resp.status_code == 404:
        return False
    if resp.status_code >= 300:
        logger.warning(
            "field_caps for %s dimensions failed (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )
        return False

    fields = resp.json().get("fields") or {}

    def _present(path: str) -> bool:
        return bool(fields.get(path))

    has_listener = _present("metrics.listener_total_req") or _present("listener_total_req")
    has_shard = _present("metrics.redis_up") or _present("redis_up")
    if has_listener and not (
        _present("attributes.endpoint")
        and _present("attributes.port")
        and _present("attributes.node")
    ):
        return True
    if has_shard and not _present("attributes.node"):
        return True
    return False


def _stream_needs_reset(elastic_url: str, api_key: str) -> bool:
    """True when mappings are poisoned or TSDB dimensions must widen."""
    return _fields_need_mapping_reset(elastic_url, api_key) or _dimensions_need_widen(
        elastic_url, api_key
    )


def _reset_sticky_long_mapping() -> None:
    """Delete the metrics data stream when mappings/dimensions are stuck."""
    try:
        from app.telemetry import ESBulkClient as _Bulk
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for mapping check: %s", exc)
        return

    try:
        client = _Bulk()
    except Exception as exc:
        logger.warning("ESBulkClient init failed for mapping check: %s", exc)
        return

    if not client.configured:
        return

    if not _stream_needs_reset(client.elastic_url, client.api_key):
        logger.info(
            "redisenterprise metrics/dimensions look healthy; leaving %s alone",
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
            "Deleted %s so metrics remap and dimensions can widen "
            "(counter_long / plain long / missing listener|shard attrs)",
            _METRICS_DATA_STREAM,
        )
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist yet", _METRICS_DATA_STREAM)
    else:
        logger.warning(
            "Could not delete %s (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )


class _DbState(CounterState):
    def __init__(self, rng: random.Random, db_id: int, memory_limit: int):
        super().__init__(rng)
        self.db_id = db_id
        self.memory_limit = memory_limit
        self.used_memory = rng.randint(memory_limit // 4, memory_limit // 2)
        self.total_req = rng.randint(0, 100000)
        self.keys = rng.randint(1000, 100000)
        self.read_hits = rng.randint(0, 50000)
        self.read_misses = rng.randint(0, 10000)
        # Starts at 0 — bdb_evicted_objects > 0 breaches the eviction alert.
        self.evicted = 0
        self.expired = rng.randint(0, 500)
        self.ingress = rng.randint(0, 10_000_000)
        self.egress = rng.randint(0, 20_000_000)


class _ShardState(CounterState):
    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.cmds = rng.randint(0, 50000)
        self.errors = rng.randint(0, 20)
        self.rdb_changes = rng.randint(0, 1000)


from generators.redisenterprise_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("redis-enterprise")
    cluster_name = cfg.get("cluster_name") or ScenarioIdentity(ns).label("cluster.redis.example.com")
    db_count: int = max(1, cfg.get("database_count", 2))
    node_count: int = max(1, cfg.get("node_count", 3))
    cloud = cfg.get("cloud") or _default_cloud()
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("re-node-1")

    impact_keys = SPEC.impact_keys if SPEC is not None else {
        "memory_pressure": {"type": "float", "default": 0.0},
        "latency_multiplier": {"type": "float", "default": 1.0},
    }

    total_mem = node_count * 64 * 1024 * 1024 * 1024  # 64 GB per node

    db_ids = [f"{i + 1}" for i in range(db_count)]
    db_states = {
        db_id: _DbState(rng, int(db_id), 16 * 1024 * 1024 * 1024)
        for db_id in db_ids
    }
    shard_ids = [f"redis-{db_id}-{s}" for db_id in db_ids for s in range(1, 3)]
    shard_states = {sid: _ShardState(rng) for sid in shard_ids}

    resource = build_signal_resource(
        ns=ns, signal_type="metrics", dataset=otel_dataset("redisenterprise"),
        service_name=service_name, service_version="7.4.0",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name, cloud=cloud,
        extra_attrs={"redis.cluster": cluster_name},
    )
    scope = {"name": RE_SCOPE, "version": SCOPE_VERSION}

    try:
        es_bulk = ESBulkClient()
    except Exception as exc:  # pragma: no cover
        logger.warning("ESBulkClient unavailable (%s); co-located docs disabled", exc)
        es_bulk = None

    _reset_sticky_long_mapping()

    logger.info(
        "Redis Enterprise generator started (namespace=%s, cluster=%s, dbs=%d, nodes=%d)",
        ns, cluster_name, db_count, node_count,
    )

    def _emit_tick() -> None:
        # One stable start for every sum keeps each series continuous (GUARDRAILS #23).
        sum_start = stable_counter_start_ns()
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="redisenterprise_otel", chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        c = active_chaos or {}
        mem_util_key = float(c.get("database_memory_utilization_critical", 0.0))
        latency_key = float(c.get("database_high_latency", 0.0))
        # GT probes use severity-like values (e.g. 10.0); SPEC healthy default is 100.
        # OOTB is ``bdb_up < 1`` — any probe below the healthy default means down.
        bdb_down = float(c.get("database_down", 100.0)) < 100.0
        node_down = bool(c.get("node_down", False))
        shard_down = bool(c.get("shard_down", False))
        cert_days = float(c.get("tls_certificate_expiring_soon", 300.0))
        # OOTB alert is ``days_until_expiry < 30``. GT probe 30.0 is on the
        # boundary and never fires — clamp active probes to strictly below 30.
        if active_chaos is not None and cert_days <= 30.0:
            cert_days = min(cert_days, 29.0)
        eviction_rate = float(c.get("database_unexpected_key_evictions", 0.0))
        auth_errors = float(c.get("authentication_failures_detected", 0.0))
        persist_fail = float(c.get("shard_persistence_failures", 0.0))
        fd_key = float(c.get("proxy_file_descriptor_saturation", 0.0))
        conn_key = float(c.get("shard_connection_saturation", 0.0))
        cpu_sat = float(c.get("node_cpu_saturation", 0.0))
        free_mem_gb = float(c.get("node_free_memory_low", 100.0))
        # OOTB free_gb < 1; GT probe 10.0 would stay healthy — clamp fault probes.
        if active_chaos is not None and free_mem_gb < 100.0:
            free_mem_gb = min(free_mem_gb, 0.5)
        storage_gb = float(c.get("node_storage_running_low", 100.0))
        # OOTB persistent/ephemeral_gb < 5; GT probe 10.0 stays healthy — clamp.
        if active_chaos is not None and storage_gb < 100.0:
            storage_gb = min(storage_gb, 4.0)
        frag_ratio = float(c.get("shard_high_memory_fragmentation", 0.0))
        load = business_load()
        mem_pressure = mem_util_key / 100.0 if mem_util_key > 0 else 0.0
        fd_pressure = fd_key / 100.0 if fd_key > 0 else 0.0
        conn_pressure = conn_key / 100.0 if conn_key > 0 else 0.0

        # Brief intermittent AOF/RDB activity so Shard Diagnostics charts move.
        # Persistence-failure alerts watch consecutive_failures only — not these.
        aof_active: set[str] = set()
        rdb_active: set[str] = set()
        if shard_ids and rng.random() < 0.08:
            pulse_sid = rng.choice(shard_ids)
            if rng.random() < 0.5:
                aof_active.add(pulse_sid)
            else:
                rdb_active.add(pulse_sid)

        metrics: list[dict] = []
        bulk_docs: list[dict[str, Any]] = []
        ts_ms = int(time.time() * 1000)
        cluster_attrs = {"cluster": cluster_name}

        # ------ Cluster level ------
        free_mem = int(total_mem * max(0.1, 1.0 - (0.5 + mem_pressure * 0.4)))
        metrics.append(_re_gauge("redis_enterprise_cluster_total_memory", "By", total_mem, cluster_attrs))
        metrics.append(_re_gauge("redis_enterprise_cluster_free_memory", "By", free_mem, cluster_attrs))
        cpu_idle = max(5.0, 80.0 - load * 50.0 - mem_pressure * 20.0)
        metrics.append(_re_gauge("redis_enterprise_cluster_cpu_idle", "%", round(cpu_idle, 1), cluster_attrs))
        total_conns = rng.randint(100, int(1000 * load) + 100)
        metrics.append(_re_gauge("redis_enterprise_cluster_conns", "{conns}", total_conns, cluster_attrs))
        metrics.append(_re_gauge(
            "redis_enterprise_cluster_persistent_storage_free", "By",
            rng.randint(50 * 1024 * 1024 * 1024, 200 * 1024 * 1024 * 1024), cluster_attrs,
        ))

        # ------ Node level ------
        for node_idx in range(1, node_count + 1):
            n_attrs = {**cluster_attrs, "node": str(node_idx)}
            cert_attrs = {
                **n_attrs,
                "path": f"/etc/opt/redislabs/node_{node_idx}_cert.pem",
            }
            node_total = total_mem // node_count
            node_free = max(0, int(node_total * max(0.05, 0.3 - mem_pressure * 0.25)))
            metrics.append(_re_gauge("redis_enterprise_node_available_memory", "By", node_free, n_attrs))
            metrics.append(_re_gauge("redis_enterprise_node_free_memory", "By", node_free, n_attrs))
            metrics.append(_re_gauge(
                "redis_enterprise_node_persistent_storage_free", "By",
                rng.randint(20 * 1024 * 1024 * 1024, 80 * 1024 * 1024 * 1024), n_attrs,
            ))
            metrics.append(_re_gauge(
                "redis_enterprise_node_persistent_storage_avail", "By",
                rng.randint(60 * 1024 * 1024 * 1024, 100 * 1024 * 1024 * 1024), n_attrs,
            ))
            metrics.append(_re_gauge(
                "redis_enterprise_node_ephemeral_storage_free", "By",
                rng.randint(5 * 1024 * 1024 * 1024, 20 * 1024 * 1024 * 1024), n_attrs,
            ))
            metrics.append(_re_gauge(
                "redis_enterprise_node_ephemeral_storage_avail", "By",
                rng.randint(10 * 1024 * 1024 * 1024, 30 * 1024 * 1024 * 1024), n_attrs,
            ))
            cpu_node_idle = max(5.0, cpu_idle + rng.uniform(-5.0, 5.0))
            metrics.append(_re_gauge("redis_enterprise_node_cpu_idle", "%", round(cpu_node_idle, 1), n_attrs))
            cpu_sys = round(rng.uniform(1.0, 8.0) * load, 1)
            cpu_user = round(rng.uniform(5.0, 30.0) * load, 1)
            metrics.append(_re_gauge("redis_enterprise_node_cpu_system", "%", cpu_sys, n_attrs))
            metrics.append(_re_gauge("redis_enterprise_node_cpu_user", "%", cpu_user, n_attrs))
            metrics.append(_re_gauge(
                "redis_enterprise_node_cpu_iowait", "%",
                round(rng.uniform(0.0, 5.0) * (1.0 + mem_pressure), 1), n_attrs,
            ))
            node_conns = total_conns // node_count
            metrics.append(_re_gauge("redis_enterprise_node_conns", "{conns}", node_conns, n_attrs))

            free_bytes = int(max(0.0, free_mem_gb) * 1024**3)
            if free_mem_gb >= 8.0 and mem_pressure <= 0:
                free_bytes = node_free
            persist_bytes = int(max(0.0, storage_gb) * 1024**3)
            eph_bytes = int(max(0.0, storage_gb) * 1024**3)
            if cpu_sat > 0:
                # The key is the node CPU utilisation percent of 8 cores.
                busy = min(cpu_sat, 100.0) / 100.0 * 8.0
                cpu_user_cores = round(busy * 0.875, 3)
                cpu_sys_cores = round(busy * 0.125, 3)
                cpu_idle_cores = round(max(0.0, 8.0 - busy), 3)
            else:
                cpu_user_cores = round(rng.uniform(0.2, 1.5) * load, 3)
                cpu_sys_cores = round(rng.uniform(0.05, 0.4) * load, 3)
                cpu_idle_cores = round(max(0.5, 8.0 - cpu_user_cores - cpu_sys_cores), 3)
            cpu_iowait_cores = round(rng.uniform(0.0, 0.2), 3)
            node_up_val = 0.0 if node_down else 1.0
            node_total_req = float(int(rng.uniform(1000, 50000) * load))
            node_ingress = float(int(rng.uniform(1e6, 5e7) * load))
            node_egress = float(int(rng.uniform(2e6, 1e8) * load))
            cert_secs = float(int(cert_days * 86400))

            metrics.append(_re_gauge("node_available_memory", "By", free_bytes if free_mem_gb < 8 else node_free, n_attrs))
            metrics.append(_re_gauge("node_free_memory", "By", free_bytes if free_mem_gb < 8 else node_free, n_attrs))
            metrics.append(_re_gauge("node_persistent_storage_avail", "By", persist_bytes, n_attrs))
            metrics.append(_re_gauge("node_ephemeral_storage_avail", "By", eph_bytes, n_attrs))
            metrics.append(_re_gauge("node_cpu_user", "{cores}", cpu_user_cores, n_attrs))
            metrics.append(_re_gauge("node_cpu_system", "{cores}", cpu_sys_cores, n_attrs))
            metrics.append(_re_gauge("node_cpu_iowait", "{cores}", cpu_iowait_cores, n_attrs))
            metrics.append(_re_gauge("node_cpu_idle", "{cores}", cpu_idle_cores, n_attrs))
            metrics.append(_re_gauge("node_conns", "{conns}", node_conns, n_attrs))
            metrics.append(_re_gauge("node_total_req", "{requests}", node_total_req, n_attrs))
            metrics.append(_re_gauge("node_ingress_bytes", "By", node_ingress, n_attrs))
            metrics.append(_re_gauge("node_egress_bytes", "By", node_egress, n_attrs))
            aof_on_node = sum(
                1 for sid in aof_active
                if str(((int(sid.split("-")[1]) - 1) % node_count) + 1) == str(node_idx)
            )
            metrics.append(_re_gauge("node_cur_aof_rewrites", "{rewrites}", aof_on_node, n_attrs))
            metrics.append(_re_gauge("node_cert_expiration_seconds", "s", cert_secs, cert_attrs))
            metrics.append(_re_gauge("node_up", "1", node_up_val, n_attrs))

            # Match OTLP dimension sets exactly: node_* use n_attrs; cert uses path.
            bulk_docs.append(_bulk_doc(
                resource,
                {
                    "node_up": node_up_val,
                    "node_cpu_user": cpu_user_cores,
                    "node_cpu_system": cpu_sys_cores,
                    "node_cpu_idle": cpu_idle_cores,
                    "node_cpu_iowait": cpu_iowait_cores,
                    "node_free_memory": free_bytes if free_mem_gb < 8 else float(node_free),
                    "node_available_memory": free_bytes if free_mem_gb < 8 else float(node_free),
                    "node_ephemeral_storage_avail": float(eph_bytes),
                    "node_persistent_storage_avail": float(persist_bytes),
                    "node_conns": float(node_conns),
                    "node_total_req": node_total_req,
                    "node_ingress_bytes": node_ingress,
                    "node_egress_bytes": node_egress,
                    "node_cur_aof_rewrites": 0.0,
                },
                attributes=n_attrs,
                timestamp_ms=ts_ms,
            ))
            bulk_docs.append(_bulk_doc(
                resource,
                {"node_cert_expiration_seconds": cert_secs},
                attributes=cert_attrs,
                timestamp_ms=ts_ms,
            ))

        # ------ Database (BDB) level ------
        for db_id, db_state in db_states.items():
            bdb_attrs = {**cluster_attrs, "bdb": db_id}
            port = 12000 + int(db_id)
            endpoint = f"{cluster_name}:{port}"
            # Listeners are pinned to a node; rotate across the cluster.
            listener_node = str(((int(db_id) - 1) % node_count) + 1)
            listener_attrs = {
                **bdb_attrs,
                "node": listener_node,
                "endpoint": endpoint,
                "port": str(port),
            }

            db_state.tick(rng, drift=int(500 * load))
            db_state.total_req += rng.randint(0, int(200 * load))
            db_state.keys = max(100, db_state.keys + rng.randint(-10, 20))
            db_state.used_memory = min(
                # Healthy usage stays at or below half the limit, so a memory fault
                # that resolves drops back under the 90% alert line.
                db_state.memory_limit if mem_pressure > 0 else db_state.memory_limit // 2,
                max(
                    db_state.memory_limit // 4,
                    db_state.used_memory + rng.randint(
                        -1024 * 1024, int(10 * 1024 * 1024 * mem_pressure + 1024 * 1024),
                    ),
                ),
            )
            hit_ratio = max(0.0, min(1.0, 0.85 - mem_pressure * 0.3))
            reads = int(db_state.total_req * 0.6)
            writes = int(db_state.total_req * 0.3)
            other = int(db_state.total_req * 0.1)
            db_state.read_hits += max(1, int(reads * hit_ratio * 0.01))
            db_state.read_misses += max(0, int(reads * (1.0 - hit_ratio) * 0.01))
            db_state.ingress += int(200 * load)
            db_state.egress += int(500 * load)
            db_state.expired += rng.randint(0, 5)
            # bdb_evicted_objects is a per-interval gauge, not a lifetime total: the alert
            # is MAX(...) > 0, so it must fall back to 0 once the fault resolves. Only the
            # eviction impact key drives it; memory pressure alone must not breach it.
            db_state.evicted = int(eviction_rate) if eviction_rate > 0 else 0

            if latency_key > 0:
                avg_lat = round(latency_key / 1000.0, 6)  # key is the average latency in ms
            else:
                avg_lat = round(rng.uniform(0.0001, 0.002), 6)
            if mem_pressure > 0:
                # OOTB ``avg_used / avg_limit * 100`` integer-divides first; used
                # must be >= limit or utilization_pct stays 0.
                db_state.used_memory = int(
                    db_state.memory_limit * rng.uniform(1.0, 1.05),
                )

            expires = int(db_state.keys * 0.3)
            shard_cpu_user = round(rng.uniform(0.05, 0.8) * load, 3)
            shard_cpu_system = round(rng.uniform(0.01, 0.2) * load, 3)
            ops = float(int(rng.uniform(100, 5000) * load))
            bdb_up_val = 0.0 if bdb_down else 1.0
            read_lat = round(avg_lat * 0.9, 6)
            write_lat = round(avg_lat * 1.1, 6)
            other_lat = round(avg_lat * 1.2, 6)
            frag = frag_ratio if frag_ratio > 0 else round(rng.uniform(1.0, 1.3), 2)

            metrics.append(_re_gauge("bdb_used_memory", "By", db_state.used_memory, bdb_attrs))
            metrics.append(_re_gauge("bdb_mem_size_calculated", "By", db_state.used_memory, bdb_attrs))
            metrics.append(_re_gauge("bdb_memory_limit", "By", db_state.memory_limit, bdb_attrs))
            metrics.append(_re_gauge("bdb_conns", "{conns}", rng.randint(10, 200), bdb_attrs))
            metrics.append(_re_gauge("bdb_connected_clients", "{clients}", rng.randint(5, 100), bdb_attrs))
            metrics.append(_re_gauge("bdb_total_req", "{requests}", db_state.total_req, bdb_attrs))
            metrics.append(_re_gauge("bdb_read_req", "{requests}", reads, bdb_attrs))
            metrics.append(_re_gauge("bdb_write_req", "{requests}", writes, bdb_attrs))
            metrics.append(_re_gauge("bdb_other_req", "{requests}", other, bdb_attrs))
            metrics.append(_re_gauge("bdb_no_of_keys", "{keys}", db_state.keys, bdb_attrs))
            metrics.append(_re_gauge("bdb_no_of_expires", "{keys}", expires, bdb_attrs))
            metrics.append(_re_gauge("bdb_expired_objects", "{keys}", db_state.expired, bdb_attrs))
            metrics.append(_re_gauge("bdb_evicted_objects", "{keys}", db_state.evicted, bdb_attrs))
            metrics.append(_re_gauge("bdb_hit_ratio", "1", round(hit_ratio, 3), bdb_attrs))
            metrics.append(_re_gauge("bdb_read_hits", "{hits}", db_state.read_hits, bdb_attrs))
            metrics.append(_re_gauge("bdb_write_hits", "{hits}", int(writes * hit_ratio), bdb_attrs))
            metrics.append(_re_gauge("bdb_read_misses", "{misses}", db_state.read_misses, bdb_attrs))
            metrics.append(_re_gauge("bdb_write_misses", "{misses}", int(writes * (1.0 - hit_ratio)), bdb_attrs))
            metrics.append(_re_gauge("bdb_avg_latency", "s", avg_lat, bdb_attrs))
            metrics.append(_re_gauge("bdb_avg_latency_max", "s", round(avg_lat * 2.0, 6), bdb_attrs))
            metrics.append(_re_gauge("bdb_avg_read_latency", "s", read_lat, bdb_attrs))
            metrics.append(_re_gauge("bdb_avg_write_latency", "s", write_lat, bdb_attrs))
            metrics.append(_re_gauge("bdb_avg_other_latency", "s", other_lat, bdb_attrs))
            metrics.append(_re_gauge("bdb_avg_read_latency_max", "s", round(read_lat * 2.0, 6), bdb_attrs))
            metrics.append(_re_gauge("bdb_avg_write_latency_max", "s", round(write_lat * 2.0, 6), bdb_attrs))
            metrics.append(_re_gauge("bdb_avg_other_latency_max", "s", round(other_lat * 2.0, 6), bdb_attrs))
            metrics.append(_re_gauge("bdb_up", "1", bdb_up_val, bdb_attrs))
            metrics.append(_re_gauge("bdb_instantaneous_ops_per_sec", "{ops/s}", ops, bdb_attrs))
            metrics.append(_re_gauge("bdb_mem_frag_ratio", "1", frag, bdb_attrs))
            metrics.append(_re_gauge("bdb_shards_used", "{shards}", 2, bdb_attrs))
            metrics.append(_re_gauge("bdb_ingress_bytes", "By", db_state.ingress, bdb_attrs))
            metrics.append(_re_gauge("bdb_egress_bytes", "By", db_state.egress, bdb_attrs))
            metrics.append(_re_gauge("bdb_fork_cpu_system", "%", round(rng.uniform(0.0, 5.0), 1), bdb_attrs))
            metrics.append(_re_gauge(
                "bdb_main_thread_cpu_system", "%",
                round(rng.uniform(1.0, 20.0) * load, 1), bdb_attrs,
            ))
            metrics.append(_re_gauge(
                "bdb_total_pending_writes", "{writes}",
                rng.randint(0, int(10 * mem_pressure + 1)), bdb_attrs,
            ))
            metrics.append(_re_gauge("bdb_shard_cpu_user", "{cores}", shard_cpu_user, bdb_attrs))
            metrics.append(_re_gauge("bdb_shard_cpu_system", "{cores}", shard_cpu_system, bdb_attrs))

            listener_total = float(int(rng.uniform(500, 20000) * load))
            listener_read = float(int(listener_total * 0.6))
            listener_write = float(int(listener_total * 0.35))
            listener_conns = float(rng.randint(10, 200))
            max_conn_exceeded = 0.0
            metrics.append(_re_gauge("listener_acc_latency", "s", avg_lat, listener_attrs))
            metrics.append(_re_gauge("listener_acc_read_latency", "s", read_lat, listener_attrs))
            metrics.append(_re_gauge("listener_acc_write_latency", "s", write_lat, listener_attrs))
            metrics.append(_re_gauge("listener_auth_errors", "{errors}", auth_errors, listener_attrs))
            metrics.append(_re_gauge("listener_conns", "{conns}", listener_conns, listener_attrs))
            metrics.append(_re_gauge("listener_total_req", "{requests}", listener_total, listener_attrs))
            metrics.append(_re_gauge("listener_read_req", "{requests}", listener_read, listener_attrs))
            metrics.append(_re_gauge("listener_write_req", "{requests}", listener_write, listener_attrs))
            metrics.append(_re_gauge(
                "listener_max_connections_exceeded", "{events}", max_conn_exceeded, listener_attrs,
            ))

            bdb_conns = float(rng.randint(10, 200))
            bulk_docs.append(_bulk_doc(
                resource,
                {
                    "bdb_up": bdb_up_val,
                    "bdb_avg_latency": avg_lat,
                    "bdb_avg_latency_max": round(avg_lat * 2.0, 6),
                    "bdb_avg_read_latency": read_lat,
                    "bdb_avg_write_latency": write_lat,
                    "bdb_avg_other_latency": other_lat,
                    "bdb_avg_read_latency_max": round(read_lat * 2.0, 6),
                    "bdb_avg_write_latency_max": round(write_lat * 2.0, 6),
                    "bdb_avg_other_latency_max": round(other_lat * 2.0, 6),
                    "bdb_instantaneous_ops_per_sec": ops,
                    "bdb_used_memory": float(db_state.used_memory),
                    "bdb_memory_limit": float(db_state.memory_limit),
                    "bdb_mem_frag_ratio": float(frag),
                    "bdb_conns": bdb_conns,
                    "bdb_shards_used": 2.0,
                    "bdb_no_of_keys": float(db_state.keys),
                    "bdb_no_of_expires": float(expires),
                    "bdb_read_hits": float(db_state.read_hits),
                    "bdb_read_misses": float(db_state.read_misses),
                    "bdb_evicted_objects": float(db_state.evicted),
                    "bdb_expired_objects": float(db_state.expired),
                    "bdb_read_req": float(reads),
                    "bdb_write_req": float(writes),
                    "bdb_other_req": float(other),
                    "bdb_ingress_bytes": float(db_state.ingress),
                    "bdb_egress_bytes": float(db_state.egress),
                    "bdb_shard_cpu_user": shard_cpu_user,
                    "bdb_shard_cpu_system": shard_cpu_system,
                },
                attributes=bdb_attrs,
                timestamp_ms=ts_ms,
            ))
            bulk_docs.append(_bulk_doc(
                resource,
                {
                    "listener_acc_latency": avg_lat,
                    "listener_acc_read_latency": read_lat,
                    "listener_acc_write_latency": write_lat,
                    "listener_total_req": listener_total,
                    "listener_read_req": listener_read,
                    "listener_write_req": listener_write,
                    "listener_conns": listener_conns,
                    "listener_auth_errors": float(auth_errors),
                    "listener_max_connections_exceeded": max_conn_exceeded,
                },
                attributes=listener_attrs,
                timestamp_ms=ts_ms,
            ))

        # ------ Shard level ------
        for shard_id, shard_state in shard_states.items():
            db_id = shard_id.split("-")[1]
            role = "master" if shard_id.endswith("-1") else "slave"
            node_id = str(((int(db_id) - 1) % node_count) + 1)
            s_attrs = {
                **cluster_attrs,
                "redis": shard_id,
                "role": role,
                "bdb": db_id,
                "node": node_id,
            }
            shard_state.tick(rng, drift=int(100 * load))
            shard_state.cmds += rng.randint(0, int(500 * load))
            shard_state.errors += rng.randint(0, 2)
            shard_state.rdb_changes += rng.randint(0, 20)
            shard_mem = rng.randint(512 * 1024 * 1024, 4 * 1024 * 1024 * 1024)
            total_keys = db_states[db_id].keys // 2 if role == "master" else db_states[db_id].keys
            # OOTB conn_util_pct = avg_clients/avg_max*100; keep clients >= maxclients
            # under pressure so integer-ish division still breaches >80.
            _maxclients = 10000
            if conn_pressure > 0:
                _conn_clients = max(_maxclients, int(_maxclients * conn_pressure))
            else:
                _conn_clients = rng.randint(5, 50)
            frag = frag_ratio if frag_ratio > 1.5 else round(rng.uniform(1.0, 1.5), 2)
            redis_up_val = 0.0 if shard_down else 1.0
            ops = float(int(rng.uniform(100, 5000) * load))
            cpu_pct = round(rng.uniform(1.0, 40.0) * load, 1)
            blocked = float(rng.randint(0, int(5 * mem_pressure + 1)))
            cow = float(rng.randint(0, 64 * 1024 * 1024))
            fork_pct = round(rng.uniform(0.0, 5.0), 2)
            loop_sum = round(rng.uniform(0.001, 0.05), 6)
            used_mem = float(int(shard_mem * 0.85))
            maxmem = float(db_states[db_id].memory_limit // 2)
            aof_in_progress = 1.0 if shard_id in aof_active else 0.0
            rdb_in_progress = 1.0 if shard_id in rdb_active else 0.0

            metrics.append(_re_gauge("redis_active_defrag_running", "1", 0, s_attrs))
            metrics.append(_re_gauge("redis_allocator_active", "By", shard_mem, s_attrs))
            metrics.append(_re_gauge("redis_allocator_allocated", "By", int(shard_mem * 0.85), s_attrs))
            metrics.append(_re_gauge("redis_allocator_resident", "By", shard_mem, s_attrs))
            metrics.append(_re_gauge("redis_aof_rewrite_in_progress", "1", aof_in_progress, s_attrs))
            metrics.append(_re_gauge("redis_blocked_clients", "{clients}", blocked, s_attrs))
            metrics.append(_re_gauge("redis_connected_clients", "{clients}", _conn_clients, s_attrs))
            metrics.append(_re_gauge("redis_connected_slaves", "{slaves}", 1 if role == "master" else 0, s_attrs))
            metrics.append(_re_gauge("redis_db0_keys", "{keys}", total_keys, s_attrs))
            metrics.append(_re_gauge("redis_db0_expires", "{keys}", int(total_keys * 0.3), s_attrs))
            metrics.append(_re_gauge("redis_instantaneous_ops_per_sec", "{ops/s}", ops, s_attrs))
            hits = int(shard_state.cmds * 0.85)
            misses = shard_state.cmds - hits
            metrics.append(build_cumulative_sum("redis_keyspace_hits", "{hits}", hits, sum_start, s_attrs))
            metrics.append(build_cumulative_sum("redis_keyspace_misses", "{misses}", misses, sum_start, s_attrs))
            metrics.append(_re_gauge("redis_master_link_status", "1", 1 if role == "slave" else 0, s_attrs))
            metrics.append(_re_gauge("redis_master_sync_in_progress", "1", 0, s_attrs))
            metrics.append(_re_gauge("redis_maxmemory", "By", maxmem, s_attrs))
            metrics.append(_re_gauge(
                "redis_mem_allocator_overhead", "By",
                rng.randint(1 * 1024 * 1024, 10 * 1024 * 1024), s_attrs,
            ))
            metrics.append(_re_gauge("redis_mem_fragmentation_ratio", "1", frag, s_attrs))
            metrics.append(_re_gauge("redis_maxclients", "{clients}", _maxclients, s_attrs))
            metrics.append(_re_gauge(
                "redis_aof_rewrites_consecutive_failures", "{failures}", persist_fail, s_attrs,
            ))
            metrics.append(_re_gauge(
                "redis_rdb_saves_consecutive_failures", "{failures}", persist_fail, s_attrs,
            ))
            metrics.append(_re_gauge("redis_up", "1", redis_up_val, s_attrs))
            metrics.append(_re_gauge("redis_used_memory", "By", used_mem, s_attrs))
            metrics.append(_re_gauge("redis_used_memory_rss", "By", shard_mem, s_attrs))
            metrics.append(_re_gauge(
                "redis_mem_not_counted_for_evict", "By",
                rng.randint(0, 1 * 1024 * 1024), s_attrs,
            ))
            metrics.append(_re_gauge("redis_rdb_bgsave_in_progress", "1", rdb_in_progress, s_attrs))
            metrics.append(_re_gauge("redis_repl_backlog_active", "1", 1 if role == "master" else 0, s_attrs))
            metrics.append(build_cumulative_sum(
                "redis_total_commands_processed", "{commands}", shard_state.cmds, sum_start, s_attrs,
            ))
            metrics.append(build_cumulative_sum(
                "redis_total_connections_received", "{conns}", shard_state.cmds // 100, sum_start, s_attrs,
            ))
            metrics.append(build_cumulative_sum(
                "redis_total_net_input_bytes", "By", shard_state.cmds * 100, sum_start, s_attrs,
            ))
            metrics.append(build_cumulative_sum(
                "redis_total_net_output_bytes", "By", shard_state.cmds * 300, sum_start, s_attrs,
            ))
            metrics.append(_re_gauge("redis_total_error_replies", "{errors}", shard_state.errors, s_attrs))
            metrics.append(_re_gauge("redis_process_cpu_usage_percent", "%", cpu_pct, s_attrs))
            metrics.append(_re_gauge("redis_current_cow_size", "By", cow, s_attrs))
            metrics.append(_re_gauge("redis_current_fork_perc", "%", fork_pct, s_attrs))
            metrics.append(_re_gauge(
                "redis_rdb_changes_since_last_save", "{changes}", shard_state.rdb_changes, s_attrs,
            ))
            metrics.append(_re_gauge("redis_eventloop_duration_sum", "s", loop_sum, s_attrs))

            bulk_docs.append(_bulk_doc(
                resource,
                {
                    "redis_up": redis_up_val,
                    "redis_used_memory": used_mem,
                    "redis_used_memory_rss": float(shard_mem),
                    "redis_maxmemory": maxmem,
                    "redis_mem_fragmentation_ratio": float(frag),
                    "redis_instantaneous_ops_per_sec": ops,
                    "redis_process_cpu_usage_percent": cpu_pct,
                    "redis_connected_clients": float(_conn_clients),
                    "redis_maxclients": float(_maxclients),
                    "redis_blocked_clients": blocked,
                    "redis_total_error_replies": float(shard_state.errors),
                    "redis_current_cow_size": cow,
                    "redis_current_fork_perc": fork_pct,
                    "redis_rdb_changes_since_last_save": float(shard_state.rdb_changes),
                    "redis_eventloop_duration_sum": loop_sum,
                    "redis_active_defrag_running": 0.0,
                    "redis_aof_rewrite_in_progress": aof_in_progress,
                    "redis_rdb_bgsave_in_progress": rdb_in_progress,
                },
                attributes=s_attrs,
                timestamp_ms=ts_ms,
            ))

        # ------ Proxy level ------
        for node_idx in range(1, node_count + 1):
            p_attrs = {**cluster_attrs, "node": str(node_idx), "proxy": f"proxy-{node_idx}"}
            cx_accepted = rng.randint(0, int(1000 * load))
            cx_closed = int(cx_accepted * 0.95)
            cx_current = cx_accepted - cx_closed
            metrics.append(build_cumulative_sum(
                "dmcproxy_client_connections_accepted", "{conns}", cx_accepted, sum_start, p_attrs,
            ))
            metrics.append(build_cumulative_sum(
                "dmcproxy_client_connections_closed", "{conns}", cx_closed, sum_start, p_attrs,
            ))
            metrics.append(_re_gauge("dmcproxy_client_connections_current", "{conns}", cx_current, p_attrs))
            metrics.append(build_cumulative_sum(
                "dmcproxy_server_connections_accepted", "{conns}", cx_accepted // 5, sum_start, p_attrs,
            ))
            metrics.append(build_cumulative_sum(
                "dmcproxy_server_connections_closed", "{conns}", cx_closed // 5, sum_start, p_attrs,
            ))
            metrics.append(_re_gauge(
                "dmcproxy_server_connections_current", "{conns}", cx_current // 5, p_attrs,
            ))
            metrics.append(build_cumulative_sum(
                "dmcproxy_server_connections_failed", "{conns}", 0, sum_start, p_attrs,
            ))
            metrics.append(build_cumulative_sum(
                "dmcproxy_client_bytes_tx", "By", cx_accepted * 500, sum_start, p_attrs,
            ))
            metrics.append(build_cumulative_sum(
                "dmcproxy_client_bytes_rx", "By", cx_accepted * 200, sum_start, p_attrs,
            ))
            metrics.append(build_cumulative_sum(
                "dmcproxy_server_bytes_tx", "By", cx_accepted * 200, sum_start, p_attrs,
            ))
            metrics.append(build_cumulative_sum(
                "dmcproxy_server_bytes_rx", "By", cx_accepted * 500, sum_start, p_attrs,
            ))
            metrics.append(_re_gauge("dmcproxy_threads_active", "{threads}", rng.randint(4, 16), p_attrs))
            max_fds = 65536.0
            open_fds = float(int(max_fds * fd_pressure) if fd_pressure > 0 else rng.randint(200, 2000))
            cpu_pct = round(rng.uniform(1.0, 25.0) * load, 1)
            rss = float(rng.randint(200 * 1024 * 1024, 800 * 1024 * 1024))
            virt = float(rss * rng.uniform(1.5, 3.0))
            metrics.append(_re_gauge("dmcproxy_process_max_fds", "{fds}", max_fds, p_attrs))
            metrics.append(_re_gauge("dmcproxy_process_open_fds", "{fds}", open_fds, p_attrs))
            metrics.append(_re_gauge("dmcproxy_process_cpu_usage_percent", "%", cpu_pct, p_attrs))
            metrics.append(_re_gauge("dmcproxy_process_resident_memory_bytes", "By", rss, p_attrs))
            metrics.append(_re_gauge("dmcproxy_process_virtual_memory_bytes", "By", virt, p_attrs))

            bulk_docs.append(_bulk_doc(
                resource,
                {
                    "dmcproxy_process_cpu_usage_percent": cpu_pct,
                    "dmcproxy_process_open_fds": open_fds,
                    "dmcproxy_process_max_fds": max_fds,
                    "dmcproxy_process_resident_memory_bytes": rss,
                    "dmcproxy_process_virtual_memory_bytes": virt,
                },
                attributes=p_attrs,
                timestamp_ms=ts_ms,
            ))

        client.send_metrics_scoped(resource, scope, metrics)
        if es_bulk is not None and es_bulk.configured and bulk_docs:
            # Do not sit in silent backoff forever after a dimension conflict —
            # after stream reset, co-location must resume every tick.
            if es_bulk.consecutive_failures >= es_bulk.max_failures_before_backoff:
                logger.warning(
                    "Clearing ESBulk backoff so redisenterprise co-location can retry"
                )
                es_bulk.consecutive_failures = 0
            indexed = es_bulk.send_bulk(
                _METRICS_DATA_STREAM,
                bulk_docs,
                dynamic_templates=_gauge_templates_for_docs(bulk_docs),
            )
            if indexed == 0:
                logger.warning(
                    "redisenterprise co-located bulk indexed 0 docs; will retry next tick"
                )
    run_tick_loop(stop_event, _emit_tick, interval=lambda: EMIT_INTERVAL_S)

    if es_bulk is not None:
        es_bulk.close()
    logger.info("Redis Enterprise generator stopped.")


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
