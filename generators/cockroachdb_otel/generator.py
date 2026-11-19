#!/usr/bin/env python3
"""CockroachDB Generator — sends CockroachDB Prometheus metrics via OTLP.

Emits data matching the cockroachdb_otel content package
(discovery.datasets: cockroachdb.otel).

Metric names match the cockroachdb_otel dashboard / alert ES|QL queries
(snake_case Prometheus style from CockroachDB ``/_status/vars``). Counter
names intentionally omit a ``_total`` suffix except where the upstream
package queries the ``_total`` form (e.g. ``txnwaitqueue_deadlocks_total``).

Node-level metrics carry ``attributes.node_id`` only.
Store-level metrics carry both ``attributes.node_id`` and ``attributes.store``.

Also bulk-indexes co-located metric documents each tick so panels that
AND-filter metric pairs (``capacity`` + ``capacity_used``,
``rocksdb_block_cache_hits`` + ``rocksdb_block_cache_misses``) or require
``WHERE ranges IS NOT NULL`` then ``MAX(replicas)`` /
``MAX(replicas_leaseholders)`` (Store Summary), or ``WHERE sql_conns`` then
``MAX(sql_txns_open)`` / ``MAX(sql_mem_root_current)`` (SQL Metrics by Node)
are populated — OTLP intake stores one metric per document, so those filters
never match OTLP-only data.

Usage (standalone):
    python3 -m generators.cockroachdb_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/cockroachdb_otel@0.3.0"

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
    MultiEntityState,
    build_cumulative_sum,
    build_gauge,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

COCKROACHDB_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("cockroachdb-generator")
EMIT_INTERVAL_S = 15.0

# Names grounded from cockroachdb_otel dashboards + alerting rule templates.
METRIC_NAMES = (
    # Overview / SQL
    "sql_conns",
    "sql_select_count",
    "sql_insert_count",
    "sql_update_count",
    "sql_delete_count",
    "sql_failure_count",
    "sql_query_count",
    "sql_conn_failures",
    "sql_txn_begin_count",
    "sql_txn_commit_count",
    "sql_txn_abort_count",
    "sql_distsql_contended_queries_count",
    "sql_statements_active",
    "sql_txns_open",
    "sql_mem_root_current",
    "txn_restarts_serializable",
    "txn_restarts_readwithinuncertainty",
    "txnwaitqueue_deadlocks_total",
    # Storage
    "capacity",
    "capacity_used",
    "admission_io_overload",
    "storage_l0_sublevels",
    "storage_l0_num_files",
    "storage_write_stalls",
    "rocksdb_read_amplification",
    "rocksdb_block_cache_hits",
    "rocksdb_block_cache_misses",
    "rocksdb_block_cache_usage",
    "sys_host_disk_iopsinprogress",
    # Replication / ranges
    "liveness_livenodes",
    "liveness_heartbeatfailures",
    "liveness_heartbeatsuccesses",
    "ranges",
    "ranges_unavailable",
    "ranges_underreplicated",
    "replicas",
    "replicas_leaseholders",
    "leases_transfers_success",
    "leases_transfers_error",
    "rebalancing_lease_transfers",
    "rebalancing_range_rebalances",
    "range_splits",
    "range_merges",
    # Node resources / RPC
    "sys_cpu_combined_percent_normalized",
    "sys_runnable_goroutines_per_cpu",
    "rpc_connection_avg_round_trip_latency",
    "rpc_connection_unhealthy",
)


# The package's SLO templates and OOTB alert rules all filter
# ``service.name == "cockroachdb"`` -- the fixed name the receiver pipeline
# stamps. Emit it verbatim; scenario uniqueness comes from host.name and
# service.instance.id (both ScenarioIdentity-scoped).
RECEIVER_SERVICE_NAME = "cockroachdb"


def node_resource(
    ns: str, scoped_service: str, node_id: str, host_base: str, cloud: dict,
) -> dict:
    """OTLP resource for one CockroachDB node (service.name fixed to the package's)."""
    return build_signal_resource(
        ns=ns, signal_type="metrics", dataset=otel_dataset("cockroachdb"),
        service_name=scoped_service, service_version="24.3.0",
        service_instance_id=f"{scoped_service}-{node_id}",
        host_name=f"{host_base}-{node_id}", cloud=cloud,
        extra_attrs={"service.name": RECEIVER_SERVICE_NAME},
    )


def _default_cloud() -> dict:
    return {"provider": "gcp", "platform": "gcp_compute_engine",
            "region": "us-central1", "availability_zone": "us-central1-a"}


def _sum(name: str, unit: str, counter: CounterState, attrs: dict) -> dict:
    return build_cumulative_sum(name, unit, counter.value, counter.start_time_ns, attrs)


def sql_query_count(state: "_NodeState") -> int:
    """Cumulative ``sql_query_count``: every DML statement executed, failed ones included.

    The package SLO "SQL statement success rate" computes
    ``(Δsql_query_count - Δsql_failure_count) / Δsql_query_count`` per host, so
    the total must contain the failures it subtracts.
    """
    return (
        state.sql_select.value + state.sql_insert.value + state.sql_update.value
        + state.sql_delete.value + state.sql_failure.value
    )


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


_METRICS_DATA_STREAM = "metrics-cockroachdb.otel-default"
_CAPACITY_DOUBLE_FIELDS = ("capacity", "capacity_used")


def _capacity_needs_mapping_reset(elastic_url: str, api_key: str) -> bool:
    """True when capacity fields are stuck as long (ES|QL int division)."""
    import httpx

    try:
        resp = httpx.get(
            f"{elastic_url.rstrip('/')}/{_METRICS_DATA_STREAM}/_field_caps",
            params={"fields": ",".join(f"metrics.{n}" for n in _CAPACITY_DOUBLE_FIELDS)},
            headers={"Authorization": f"ApiKey {api_key}"},
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
    for name in _CAPACITY_DOUBLE_FIELDS:
        caps = fields.get(f"metrics.{name}") or fields.get(name) or {}
        for meta in caps.values():
            mtype = meta.get("type")
            if mtype not in ("double", "gauge_double"):
                return True
    return False


def _reset_capacity_gauge_mapping() -> None:
    """Delete the metrics stream when capacity fields are stuck as long.

    OTLP gauge intake maps whole-number ``asDouble`` bytes as ``gauge_long``
    and wins the race against bulk ``dynamic_templates: gauge_double``. Fresh
    bulk-only capacity docs then rematerialize as doubles so
    ``usage_ratio = capacity_used / capacity`` is not truncated to 0.
    """
    try:
        from app.telemetry import ESBulkClient as _Bulk
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for mapping check: %s", exc)
        return

    client = _Bulk()
    if not client.configured:
        return

    if not _capacity_needs_mapping_reset(client.elastic_url, client.api_key):
        logger.info(
            "cockroachdb capacity fields are already double; leaving %s alone",
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
            "Deleted %s so capacity remaps as gauge_double (were long)",
            _METRICS_DATA_STREAM,
        )
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist yet", _METRICS_DATA_STREAM)
    else:
        logger.warning(
            "Could not delete %s (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )


def build_capacity_utilization_doc(
    resource: dict,
    *,
    node_id: str,
    store: str,
    capacity: float | int,
    capacity_used: float | int,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc with capacity + capacity_used co-located.

    Storage "Capacity Used %" and "Capacity Utilization by Store" panels
    AND-filter both metrics on the same document. OTLP stores one metric per
    document, so those filters never match OTLP-only data.

    Emit as **non-integral float** with bulk ``gauge_double``: OOTB
    ``[CockroachDB OTel] Storage capacity usage high`` does
    ``EVAL usage_ratio = capacity_used / capacity``. ES|QL long/long
    truncates to 0, so gauge_long never breaches ``usage_ratio > 0.6``.
    Do **not** also emit these fields via OTLP — whole-number ``asDouble``
    poisons the mapping as ``gauge_long`` before bulk templates apply.
    """
    # Tiny fraction keeps JSON/dynamic mapping on the double path if templates
    # are missed; negligible vs GiB-scale capacity.
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "By",
        "data_stream": {
            "type": "metrics",
            "dataset": "cockroachdb.otel",
            "namespace": "default",
        },
        "attributes": {"node_id": str(node_id), "store": str(store)},
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": COCKROACHDB_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "capacity": float(capacity) + 0.001,
            "capacity_used": float(capacity_used) + 0.001,
        },
    }


def build_block_cache_hit_rate_doc(
    resource: dict,
    *,
    node_id: str,
    store: str,
    hits: int,
    misses: int,
    timestamp_ms: int | None = None,
    start_timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc with block-cache hits + misses co-located.

    Storage "Block Cache Hit Rate" AND-filters both counters on the same
    document, then computes ``INCREASE(hits) / (INCREASE(hits)+INCREASE(misses))``.
    """
    doc: dict[str, Any] = {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "{hits}",
        "data_stream": {
            "type": "metrics",
            "dataset": "cockroachdb.otel",
            "namespace": "default",
        },
        "attributes": {"node_id": str(node_id), "store": str(store)},
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": COCKROACHDB_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "rocksdb_block_cache_hits": int(hits),
            "rocksdb_block_cache_misses": int(misses),
        },
    }
    if start_timestamp_ms is not None:
        doc["start_timestamp"] = int(start_timestamp_ms)
    return doc


def build_store_summary_doc(
    resource: dict,
    *,
    node_id: str,
    store: str,
    ranges: int,
    replicas: int,
    replicas_leaseholders: int,
    ranges_unavailable: int,
    ranges_underreplicated: int,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc with Store Summary metrics co-located.

    Store Summary filters ``WHERE ranges IS NOT NULL`` then takes
    ``MAX(replicas)`` / ``MAX(replicas_leaseholders)``. OTLP stores one
    metric per document, so those MAX fields are null without co-location.
    """
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "{ranges}",
        "data_stream": {
            "type": "metrics",
            "dataset": "cockroachdb.otel",
            "namespace": "default",
        },
        "attributes": {"node_id": str(node_id), "store": str(store)},
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": COCKROACHDB_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "ranges": int(ranges),
            "replicas": int(replicas),
            "replicas_leaseholders": int(replicas_leaseholders),
            "ranges_unavailable": int(ranges_unavailable),
            "ranges_underreplicated": int(ranges_underreplicated),
        },
    }


def build_sql_metrics_by_node_doc(
    resource: dict,
    *,
    node_id: str,
    sql_conns: int,
    sql_statements_active: int,
    sql_txns_open: int,
    sql_mem_root_current: int,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc with SQL Metrics by Node gauges co-located.

    The panel filters ``WHERE sql_conns IS NOT NULL OR sql_statements_active
    IS NOT NULL`` then ``MAX(sql_txns_open)`` / ``MAX(sql_mem_root_current)``.
    OTLP one-metric-per-doc leaves Open Txns / SQL Memory null without this.
    """
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "1",
        "data_stream": {
            "type": "metrics",
            "dataset": "cockroachdb.otel",
            "namespace": "default",
        },
        "attributes": {"node_id": str(node_id)},
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": COCKROACHDB_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "sql_conns": int(sql_conns),
            "sql_statements_active": int(sql_statements_active),
            "sql_txns_open": int(sql_txns_open),
            "sql_mem_root_current": int(sql_mem_root_current),
        },
    }


class _NodeState(CounterState):
    """Per-node cumulative counters + gauge baselines."""

    def __init__(self, rng: random.Random):
        # Parent .value is unused; keep it near zero so a mistaken emit cannot
        # create a RATE chart spike.
        super().__init__(rng, initial=0)
        self.conns = rng.randint(10, 80)
        self.ranges = rng.randint(800, 1400)
        self.capacity = 500 * 1024 * 1024 * 1024  # 500 GiB
        self.capacity_used = rng.randint(80, 220) * 1024 * 1024 * 1024
        self.block_cache_usage = rng.randint(256, 1024) * 1024 * 1024
        self.sql_mem = rng.randint(64, 512) * 1024 * 1024

        # Cumulative counters start near zero. CounterState's default initial
        # (50k–5M) makes the first ES|QL RATE/INCREASE bucket a huge spike and
        # the real per-tick rate looks like flatline 0 on autoscaled charts.
        self.sql_select = CounterState(rng, initial=rng.randint(0, 200))
        self.sql_insert = CounterState(rng, initial=rng.randint(0, 80))
        self.sql_update = CounterState(rng, initial=rng.randint(0, 60))
        self.sql_delete = CounterState(rng, initial=rng.randint(0, 20))
        self.sql_failure = CounterState(rng, initial=0)
        self.sql_conn_fail = CounterState(rng, initial=0)
        self.sql_txn_begin = CounterState(rng, initial=rng.randint(0, 100))
        self.sql_txn_commit = CounterState(rng, initial=rng.randint(0, 100))
        self.sql_txn_abort = CounterState(rng, initial=0)
        self.sql_contended = CounterState(rng, initial=0)
        self.txn_restart_serializable = CounterState(rng, initial=0)
        self.txn_restart_uncertainty = CounterState(rng, initial=0)
        self.deadlocks = CounterState(rng, initial=0)

        self.cache_hits = CounterState(rng, initial=rng.randint(0, 5_000))
        self.cache_misses = CounterState(rng, initial=rng.randint(0, 200))

        # Replication counters also need continuous drift (see tick) — starting
        # high with drift often 0 produced spike-then-flatline on that dashboard.
        self.lease_xfer_ok = CounterState(rng, initial=rng.randint(0, 50))
        self.lease_xfer_err = CounterState(rng, initial=0)
        self.rebalance_leases = CounterState(rng, initial=rng.randint(0, 50))
        self.rebalance_ranges = CounterState(rng, initial=rng.randint(0, 50))
        self.range_splits = CounterState(rng, initial=rng.randint(0, 30))
        self.range_merges = CounterState(rng, initial=rng.randint(0, 20))
        self.liveness_fail = CounterState(rng, initial=0)
        self.liveness_ok = CounterState(rng, initial=rng.randint(0, 100))

    def tick(self, rng: random.Random, load: float = 1.0,
             unavail_rate: float = 0.0, cpu_mult: float = 1.0) -> None:
        self.conns = max(1, self.conns + rng.randint(-3, 4))
        # Keep healthy utilization well under the OOTB ``usage_ratio > 0.6``
        # alert; chaos raises this via ``storage_capacity_usage_high``.
        self.capacity_used = min(
            int(self.capacity * 0.45),
            max(int(self.capacity * 0.1),
                self.capacity_used + rng.randint(-2, 5) * 1024 * 1024),
        )
        self.block_cache_usage = max(
            64 * 1024 * 1024,
            self.block_cache_usage + rng.randint(-8, 8) * 1024 * 1024,
        )
        self.sql_mem = max(
            32 * 1024 * 1024,
            self.sql_mem + rng.randint(-4, 4) * 1024 * 1024,
        )

        qps = max(1, int(rng.randint(40, 200) * load))
        self.sql_select.tick(rng, drift=int(qps * 0.60))
        self.sql_insert.tick(rng, drift=int(qps * 0.20))
        self.sql_update.tick(rng, drift=int(qps * 0.15))
        self.sql_delete.tick(rng, drift=int(qps * 0.05))
        fail = int(qps * max(unavail_rate, 0.001) * 0.05)
        self.sql_failure.tick(rng, drift=max(0, fail))
        self.sql_conn_fail.tick(rng, drift=rng.randint(0, max(1, fail)))
        self.sql_txn_begin.tick(rng, drift=int(qps * 0.30))
        self.sql_txn_commit.tick(rng, drift=int(qps * 0.28))
        self.sql_txn_abort.tick(rng, drift=max(0, int(qps * unavail_rate * 0.05)))
        self.sql_contended.tick(rng, drift=int(qps * max(0.01, unavail_rate) * 0.1))
        self.txn_restart_serializable.tick(rng, drift=rng.randint(0, max(1, int(3 * unavail_rate + 1))))
        self.txn_restart_uncertainty.tick(rng, drift=rng.randint(0, max(1, int(2 * unavail_rate + 1))))
        self.deadlocks.tick(rng, drift=1 if unavail_rate > 0.3 and rng.random() < 0.2 else 0)

        self.cache_hits.tick(rng, drift=rng.randint(1_000, 20_000))
        self.cache_misses.tick(rng, drift=rng.randint(10, 800))

        # Steady replication activity so RATE panels keep moving after start.
        self.lease_xfer_ok.tick(rng, drift=rng.randint(1, 6))
        self.lease_xfer_err.tick(rng, drift=1 if unavail_rate > 0.2 and rng.random() < 0.3 else 0)
        self.rebalance_leases.tick(rng, drift=rng.randint(1, 4))
        self.rebalance_ranges.tick(rng, drift=rng.randint(1, 4))
        self.range_splits.tick(rng, drift=rng.randint(1, 3))
        self.range_merges.tick(rng, drift=rng.randint(0, 2))
        self.liveness_ok.tick(rng, drift=rng.randint(2, 8))
        self.liveness_fail.tick(rng, drift=1 if unavail_rate > 0.4 and rng.random() < 0.3 else 0)

        # Stash chaos-influenced gauge inputs for emit
        self._unavail_rate = unavail_rate
        self._cpu_mult = cpu_mult
        self._load = load



from generators.cockroachdb_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("cockroachdb")
    node_count: int = int(cfg.get("node_count", 3))
    cloud = cfg.get("cloud") or _default_cloud()
    host_base = cfg.get("host_name") or ScenarioIdentity(ns).label("crdb")



    # Numeric node ids match CockroachDB's node_id label style.
    node_ids = [str(i) for i in range(1, node_count + 1)]
    node_states = MultiEntityState(node_ids, _NodeState, rng)
    scope = {"name": COCKROACHDB_SCOPE, "version": SCOPE_VERSION}
    # Secondary transport for AND-filtered co-located metric documents.
    try:
        es_bulk = ESBulkClient()
    except Exception as exc:  # pragma: no cover - missing http2/deps in some host envs
        logger.warning("ESBulkClient unavailable (%s); co-located docs disabled", exc)
        es_bulk = None

    _reset_capacity_gauge_mapping()

    logger.info("CockroachDB generator started (namespace=%s, nodes=%d)", ns, node_count)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="cockroachdb_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        chaos = active_chaos or {}
        unavail_rate = float(impact_value(chaos, "unavailable_ranges_detected", default=0.0) or 0.0)
        underrep_rate = float(impact_value(chaos, "under_replicated_ranges_detected", default=0.0) or 0.0)
        cpu_mult = float(impact_value(chaos, "cpu_utilization_high", default=1.0) or 1.0)
        deadlock_rate = float(impact_value(chaos, "deadlocks_detected", default=0.0) or 0.0)
        disk_iops = float(impact_value(chaos, "disk_i_o_operations_queued_high", default=0.0) or 0.0)
        goroutines_key = float(impact_value(chaos, "runnable_goroutines_per_cpu_high", default=0.0) or 0.0)
        io_overload_key = float(impact_value(chaos, "io_admission_control_overload", default=0.0) or 0.0)
        l0_files = int(impact_value(chaos, "l0_file_count_high", default=0) or 0)
        liveness_fail_extra = int(chaos.get("liveness_heartbeat_failures", 0) or 0)
        read_amp = float(impact_value(chaos, "lsm_read_amplification_high", default=0.0) or 0.0)
        rpc_unhealthy_key = int(impact_value(chaos, "unhealthy_rpc_connections", default=0) or 0)
        cap_ratio = float(impact_value(chaos, "storage_capacity_usage_high", default=0.0) or 0.0)
        write_stalls_key = int(impact_value(chaos, "storage_write_stalls_detected", default=0) or 0)
        load = business_load()

        live_nodes = node_count if unavail_rate < 0.7 else max(1, node_count - 1)
        bulk_docs: list[dict[str, Any]] = []
        ts_ms = int(time.time() * 1000)

        for node_id, state in node_states.items():
            state.tick(rng, load=load, unavail_rate=unavail_rate, cpu_mult=cpu_mult)
            if deadlock_rate > 0:
                state.deadlocks.tick(rng, drift=max(1, int(deadlock_rate)))
            if liveness_fail_extra > 0:
                state.liveness_fail.tick(rng, drift=liveness_fail_extra)
            if cap_ratio > 0:
                # GT may set percent (85.0) or ratio (0.85); alert needs > 0.6.
                ratio = cap_ratio / 100.0 if cap_ratio > 1.0 else cap_ratio
                ratio = min(max(ratio, 0.61), 0.99)
                state.capacity_used = float(state.capacity) * ratio

            node_attrs = {"node_id": node_id}
            store_id = "1"
            store_attrs = {"node_id": node_id, "store": store_id}

            unavail = int(state.ranges * unavail_rate)
            underrep = int(state.ranges * (underrep_rate if underrep_rate > 0 else min(unavail_rate * 0.5, 0.05)))
            cpu_norm = min(0.99, round(rng.uniform(0.05, 0.35) * cpu_mult * load, 4))
            if cpu_mult > 1.5:
                cpu_norm = min(0.99, max(cpu_norm, 0.81 * min(cpu_mult / 2.0, 1.2)))
            goroutines = goroutines_key if goroutines_key > 0 else round(rng.uniform(1.0, 12.0) * load, 2)
            io_overload = io_overload_key if io_overload_key > 0 else round(rng.uniform(0.05, 0.4), 3)
            write_stalls = write_stalls_key if write_stalls_key > 0 else 0
            iops_queued = disk_iops if disk_iops > 0 else round(rng.uniform(0.0, 4.0), 2)
            rpc_unhealthy = rpc_unhealthy_key if rpc_unhealthy_key > 0 else 0
            rpc_rtt_ns = rng.randint(200_000, 2_000_000) + int(unavail_rate * 5_000_000)
            active_stmts = max(0, int(state.conns * rng.uniform(0.1, 0.6) * load))
            open_txns = max(0, int(state.conns * rng.uniform(0.05, 0.4) * load))

            metrics: list[dict] = []

            # ── Node-level (attributes.store must be absent) ──────────
            metrics.append(build_gauge("sql_conns", "{conns}", state.conns, node_attrs))
            metrics.append(build_gauge("sql_statements_active", "{stmts}", active_stmts, node_attrs))
            metrics.append(build_gauge("sql_txns_open", "{txns}", open_txns, node_attrs))
            metrics.append(build_gauge("sql_mem_root_current", "By", state.sql_mem, node_attrs))

            metrics.append(_sum("sql_select_count", "{queries}", state.sql_select, node_attrs))
            metrics.append(_sum("sql_insert_count", "{queries}", state.sql_insert, node_attrs))
            metrics.append(_sum("sql_update_count", "{queries}", state.sql_update, node_attrs))
            metrics.append(_sum("sql_delete_count", "{queries}", state.sql_delete, node_attrs))
            metrics.append(_sum("sql_failure_count", "{failures}", state.sql_failure, node_attrs))
            metrics.append(build_cumulative_sum(
                "sql_query_count", "{queries}", sql_query_count(state),
                state.sql_select.start_time_ns, node_attrs,
            ))
            metrics.append(_sum("sql_conn_failures", "{failures}", state.sql_conn_fail, node_attrs))
            metrics.append(_sum("sql_txn_begin_count", "{txns}", state.sql_txn_begin, node_attrs))
            metrics.append(_sum("sql_txn_commit_count", "{txns}", state.sql_txn_commit, node_attrs))
            metrics.append(_sum("sql_txn_abort_count", "{txns}", state.sql_txn_abort, node_attrs))
            metrics.append(_sum("sql_distsql_contended_queries_count", "{queries}",
                                state.sql_contended, node_attrs))
            metrics.append(_sum("txn_restarts_serializable", "{restarts}",
                                state.txn_restart_serializable, node_attrs))
            metrics.append(_sum("txn_restarts_readwithinuncertainty", "{restarts}",
                                state.txn_restart_uncertainty, node_attrs))
            metrics.append(_sum("txnwaitqueue_deadlocks_total", "{deadlocks}",
                                state.deadlocks, node_attrs))

            metrics.append(build_gauge("liveness_livenodes", "{nodes}", live_nodes, node_attrs))
            metrics.append(_sum("liveness_heartbeatfailures", "{failures}",
                                state.liveness_fail, node_attrs))
            metrics.append(_sum("liveness_heartbeatsuccesses", "{successes}",
                                state.liveness_ok, node_attrs))

            metrics.append(_sum("leases_transfers_success", "{transfers}",
                                state.lease_xfer_ok, node_attrs))
            metrics.append(_sum("leases_transfers_error", "{transfers}",
                                state.lease_xfer_err, node_attrs))
            metrics.append(_sum("rebalancing_lease_transfers", "{transfers}",
                                state.rebalance_leases, node_attrs))
            metrics.append(_sum("rebalancing_range_rebalances", "{rebalances}",
                                state.rebalance_ranges, node_attrs))
            metrics.append(_sum("range_splits", "{splits}", state.range_splits, node_attrs))
            metrics.append(_sum("range_merges", "{merges}", state.range_merges, node_attrs))

            # Normalized 0–1 (alerts fire above 0.8)
            metrics.append(build_gauge("sys_cpu_combined_percent_normalized", "1",
                                       cpu_norm, node_attrs))
            metrics.append(build_gauge("sys_runnable_goroutines_per_cpu", "1",
                                       goroutines, node_attrs))
            metrics.append(build_gauge("sys_host_disk_iopsinprogress", "{iops}",
                                       iops_queued, node_attrs))
            metrics.append(build_gauge("rpc_connection_avg_round_trip_latency", "ns",
                                       rpc_rtt_ns, node_attrs))
            metrics.append(build_gauge("rpc_connection_unhealthy", "{conns}",
                                       rpc_unhealthy, node_attrs))

            # ── Store-level (node_id + store) ─────────────────────────
            # Used % panel (EVAL * 100 + percent format) double-scales in the
            # package; do not compensate via emit — values stay in bytes.
            # capacity / capacity_used: bulk-only (see build_capacity_utilization_doc).
            # OTLP asDouble of whole GiB values maps gauge_long and poisons
            # usage_ratio = used/capacity (ES|QL truncates long/long → 0).
            metrics.append(build_gauge("admission_io_overload", "1", io_overload, store_attrs))
            metrics.append(build_gauge("storage_l0_sublevels", "{sublevels}",
                                       rng.randint(1, 8), store_attrs))
            metrics.append(build_gauge("storage_l0_num_files", "{files}",
                                       l0_files if l0_files > 0 else rng.randint(20, 400), store_attrs))
            metrics.append(build_gauge("storage_write_stalls", "{stalls}",
                                       write_stalls, store_attrs))
            metrics.append(build_gauge("rocksdb_read_amplification", "{factor}",
                                       read_amp if read_amp > 0 else rng.randint(2, 12), store_attrs))
            metrics.append(build_gauge("rocksdb_block_cache_usage", "By",
                                       state.block_cache_usage, store_attrs))
            metrics.append(_sum("rocksdb_block_cache_hits", "{hits}",
                                state.cache_hits, store_attrs))
            metrics.append(_sum("rocksdb_block_cache_misses", "{misses}",
                                state.cache_misses, store_attrs))

            replicas = state.ranges * 3
            metrics.append(build_gauge("ranges", "{ranges}", state.ranges, store_attrs))
            metrics.append(build_gauge("ranges_unavailable", "{ranges}", unavail, store_attrs))
            metrics.append(build_gauge("ranges_underreplicated", "{ranges}", underrep, store_attrs))
            metrics.append(build_gauge("replicas", "{replicas}", replicas, store_attrs))
            metrics.append(build_gauge("replicas_leaseholders", "{replicas}",
                                       state.ranges, store_attrs))

            resource = node_resource(ns, service_name, node_id, host_base, cloud)
            client.send_metrics_scoped(resource, scope, metrics)

            bulk_docs.append(build_capacity_utilization_doc(
                resource,
                node_id=node_id,
                store=store_id,
                capacity=state.capacity,
                capacity_used=state.capacity_used,
                timestamp_ms=ts_ms,
            ))
            bulk_docs.append(build_block_cache_hit_rate_doc(
                resource,
                node_id=node_id,
                store=store_id,
                hits=state.cache_hits.value,
                misses=state.cache_misses.value,
                timestamp_ms=ts_ms,
                start_timestamp_ms=int(int(state.cache_hits.start_time_ns) / 1_000_000),
            ))
            bulk_docs.append(build_store_summary_doc(
                resource,
                node_id=node_id,
                store=store_id,
                ranges=state.ranges,
                replicas=replicas,
                replicas_leaseholders=state.ranges,
                ranges_unavailable=unavail,
                ranges_underreplicated=underrep,
                timestamp_ms=ts_ms,
            ))
            bulk_docs.append(build_sql_metrics_by_node_doc(
                resource,
                node_id=node_id,
                sql_conns=state.conns,
                sql_statements_active=active_stmts,
                sql_txns_open=open_txns,
                sql_mem_root_current=state.sql_mem,
                timestamp_ms=ts_ms,
            ))

        if es_bulk is not None and es_bulk.configured and bulk_docs:
            # Counters must map as counter_long so INCREASE() works — same
            # dynamic_templates mechanism the elasticsearch exporter uses.
            es_bulk.send_bulk(
                "metrics-cockroachdb.otel-default",
                bulk_docs,
                dynamic_templates={
                    "metrics.rocksdb_block_cache_hits": "counter_long",
                    "metrics.rocksdb_block_cache_misses": "counter_long",
                    # gauge_double required for OOTB usage_ratio = used/capacity
                    # (long/long truncates to 0 in ES|QL).
                    "metrics.capacity": "gauge_double",
                    "metrics.capacity_used": "gauge_double",
                    "metrics.ranges": "gauge_long",
                    "metrics.replicas": "gauge_long",
                    "metrics.replicas_leaseholders": "gauge_long",
                    "metrics.ranges_unavailable": "gauge_long",
                    "metrics.ranges_underreplicated": "gauge_long",
                    "metrics.sql_conns": "gauge_long",
                    "metrics.sql_statements_active": "gauge_long",
                    "metrics.sql_txns_open": "gauge_long",
                    "metrics.sql_mem_root_current": "gauge_long",
                },
            )

    run_tick_loop(stop_event, _emit_tick, interval=lambda: EMIT_INTERVAL_S)

    if es_bulk is not None:
        es_bulk.close()
    logger.info("CockroachDB generator stopped.")


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
