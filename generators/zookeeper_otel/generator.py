#!/usr/bin/env python3
"""ZooKeeper Metrics Generator — sends ZooKeeper OTel receiver metrics via OTLP.

Emits data matching the zookeeper_otel content package which populates the
bundled ZooKeeper dashboards.

IMPORTANT: zookeeper_otel manifest.yml declares an empty dataset list.
The dashboards read from metrics-generic.otel-default. Therefore this
generator passes dataset="generic" to build_signal_resource so the Elastic
OTLP endpoint routes to metrics-generic.otel-default.

OOTB FD utilization ES|QL AND-filters open+limit on the same document. OTLP
is one-metric-per-doc, so each tick also bulk-indexes a co-located FD doc via
``ESBulkClient``.

Metric names and types verified against:
  - open-telemetry-collector-contrib scraper/zookeeperscraper/documentation.md
  - elastic/integrations package zookeeper_otel 0.3.0 dashboard ES|QL

Usage (standalone):
    python3 -m generators.zookeeper_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/zookeeper_otel@0.3.0"

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
    build_non_monotonic_sum,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

# Metric names grounded from zookeeper_otel dashboard / alert ES|QL queries.
METRIC_NAMES = (
    "zookeeper.connection.active",
    "zookeeper.data_tree.ephemeral_node.count",
    "zookeeper.data_tree.size",
    "zookeeper.file_descriptor.limit",
    "zookeeper.file_descriptor.open",
    "zookeeper.follower.count",
    "zookeeper.fsync.exceeded_threshold.count",
    "zookeeper.latency.avg",
    "zookeeper.latency.max",
    "zookeeper.latency.min",
    "zookeeper.packet.count",
    "zookeeper.request.active",
    "zookeeper.ruok",
    "zookeeper.sync.pending",
    "zookeeper.watch.count",
    "zookeeper.znode.count",
)

ZOOKEEPER_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/zookeeperreceiver"
)
SCOPE_VERSION = "0.121.0"

logger = logging.getLogger("zookeeper-generator")
BATCH_INTERVAL_MIN = 10
BATCH_INTERVAL_MAX = 30
_METRICS_DATA_STREAM = "metrics-generic.otel-default"
_FD_ALERT_PCT = 80.0


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


def build_fd_utilization_doc(
    resource: dict,
    *,
    fd_open: int,
    fd_limit: int,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Co-locate open+limit FDs for OOTB FD utilization ES|QL (same-doc AND)."""
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "{file_descriptors}",
        "data_stream": {
            "type": "metrics",
            "dataset": "generic",
            "namespace": "default",
        },
        "attributes": {},
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": ZOOKEEPER_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "zookeeper.file_descriptor.open": int(fd_open),
            "zookeeper.file_descriptor.limit": int(fd_limit),
        },
    }



def build_outstanding_doc(
    resource: dict,
    *,
    outstanding: int,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Co-locate outstanding request tips for OOTB AVG(request.active)>10."""
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "{requests}",
        "data_stream": {
            "type": "metrics",
            "dataset": "generic",
            "namespace": "default",
        },
        "attributes": {},
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": ZOOKEEPER_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "zookeeper.request.active": int(outstanding),
        },
    }


def _default_cloud() -> dict:
    return {"provider": "gcp", "platform": "gcp_compute_engine",
            "region": "us-central1", "availability_zone": "us-central1-b"}


class ZookeeperNodeState(CounterState):
    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.packets_received = rng.randint(100_000, 50_000_000)
        self.packets_sent = rng.randint(100_000, 50_000_000)
        self.fsync_exceeded = 0

    def tick(self, rng: random.Random, active_chaos: dict | None = None,
             lat_mult: float = 1.0):
        load = business_load()
        delta = int(rng.randint(100, 10_000) * load)
        self.packets_received += delta
        self.packets_sent += delta
        if active_chaos and rng.random() < 0.2:
            self.fsync_exceeded += rng.randint(1, 5)



from generators.zookeeper_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("zookeeper")
    cloud = cfg.get("cloud") or _default_cloud()

    default_nodes = [ScenarioIdentity(ns).label("zk", i) for i in range(1, 4)]
    node_names: list[str] = cfg.get("node_names") or default_nodes

    node_states: dict[str, ZookeeperNodeState] = {
        n: ZookeeperNodeState(rng) for n in node_names
    }
    # Shared data-tree values (same across leader/follower from zk standpoint)
    znode_count = rng.randint(10_000, 500_000)
    ephemeral_count = rng.randint(100, 10_000)
    watch_count = rng.randint(500, 8_000)
    data_size = rng.randint(10_000_000, 1_000_000_000)


    # ZooKeeper sends one resource per node (server.state attribute)
    scope = {"name": ZOOKEEPER_SCOPE, "version": SCOPE_VERSION}
    fd_limit = 65536
    try:
        es_bulk = ESBulkClient()
    except Exception as exc:  # pragma: no cover
        logger.warning("ESBulkClient unavailable (%s); FD co-located docs disabled", exc)
        es_bulk = None
    logger.info("ZooKeeper metrics generator started (namespace=%s, nodes=%d)",
                ns, len(node_names))

    def _emit_tick() -> None:
        nonlocal data_size, ephemeral_count, exc, watch_count, znode_count
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="zookeeper_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        c = active_chaos or {}
        lat_mult = float(impact_value(c, "high_request_latency", default=1.0))
        election_delta = int(impact_value(c, "pending_syncs_buildup_on_leader", "unsynced_followers_detected", default=0))
        data_override = int(impact_value(c, "data_tree_size_excessive", default=0) or 0)
        watch_override = int(impact_value(c, "excessive_watch_count", default=0) or 0)
        znode_override = int(impact_value(c, "excessive_znode_count", default=0) or 0)
        fd_util = float(impact_value(c, "file_descriptor_utilization_high", default=0.0))
        outstanding_override = int(impact_value(c, "outstanding_requests_saturation", default=0) or 0)
        # SPEC: >0 activates breach (ruok=0). Channel probes like 0.8 are ON.
        ruok_fail = float(impact_value(c, "node_health_check_failed", default=0.0) or 0.0)
        ruok_ok = 0.0 if ruok_fail > 0 else 1.0

        load = business_load()
        # Drift the data tree — keep healthy under alert floors.
        znode_count = min(900_000, max(0, znode_count + int(rng.randint(-10, 50) * load)))
        ephemeral_count = max(0, ephemeral_count + int(rng.randint(-5, 20) * load))
        watch_count = min(9_000, max(0, watch_count + int(rng.randint(-20, 40) * load)))
        data_size = min(900_000_000, max(0, data_size + int(rng.randint(0, 100_000) * load)))

        emit_znode = znode_override if znode_override > 0 else znode_count
        emit_watch = watch_override if watch_override > 0 else watch_count
        # OOTB alert floors at >1GiB. Auto-calibrated channel YAMLs may pass a
        # small sentinel (e.g. 75); promote those to a clear breach value.
        _DATA_TREE_ALERT_FLOOR = 1_073_741_824
        _DATA_TREE_BREACH = 1_610_612_736  # 1.5 GiB
        if data_override > 0:
            emit_data = data_override if data_override > _DATA_TREE_ALERT_FLOOR else _DATA_TREE_BREACH
        else:
            emit_data = data_size

        n_followers = max(0, len(node_names) - 1)
        if election_delta > 0 and n_followers > 0:
            unsynced = min(n_followers, max(1, election_delta))
            synced = n_followers - unsynced
            pending_syncs = rng.randint(11, 40) + election_delta * 5
        else:
            synced = n_followers
            unsynced = 0
            pending_syncs = 0

        bulk_docs: list[dict[str, Any]] = []
        ts_ms = int(time.time() * 1000)
        # Impact may be percent (80/600) or fraction (0.95). OOTB alert is
        # fd_pct > 80 — force a clear breach when the channel is on.
        if fd_util > 0:
            ratio = fd_util / 100.0 if fd_util > 1.0 else fd_util
            if ratio * 100.0 <= _FD_ALERT_PCT:
                ratio = 0.95
        else:
            ratio = 0.0

        for i, (node_name, state) in enumerate(node_states.items()):
            state.tick(rng, active_chaos, lat_mult)
            is_leader = i == 0
            # Followers feel latency multiplier; leader stays closer to baseline.
            node_lat_mult = lat_mult if (lat_mult > 1.0 and not is_leader) else 1.0

            # ZooKeeper sends dataset="generic" (not "zookeeperreceiver")
            resource = build_signal_resource(
                ns=ns, signal_type="metrics",
                dataset=otel_dataset("generic"),
                service_name=service_name, service_version="3.8.2",
                service_instance_id=f"{service_name}-{node_name}",
                host_name=cfg.get("host_name") or node_name,
                cloud=cloud,
                extra_attrs={
                    "server.state": "leader" if is_leader else "follower",
                    "zk.version": "3.8.2",
                },
            )

            if node_lat_mult > 1.0:
                base_lat = max(500.0, rng.uniform(0.5, 5.0) * node_lat_mult * 20)
            else:
                base_lat = rng.uniform(0.5, 20.0)
            if ratio > 0:
                fd_open = int(fd_limit * min(1.0, ratio))
            else:
                fd_open = int(rng.randint(20, 200) * load)
            if outstanding_override > 0:
                # OOTB uses AVG(request.active)>10 over the lookback. Dilution
                # from healthy 0–8 tips keeps AVG under threshold — emit a hard
                # tip well above the GT probe so a few samples dominate.
                outstanding = max(int(outstanding_override), 500)
            else:
                outstanding = int(rng.randint(0, 8) * load)

            metrics: list[dict] = [
                build_non_monotonic_sum(
                    "zookeeper.connection.active", "{connections}",
                    int(rng.randint(1, 100) * load), state.start_time_ns),
                build_non_monotonic_sum(
                    "zookeeper.data_tree.ephemeral_node.count", "{nodes}",
                    ephemeral_count, state.start_time_ns),
                build_non_monotonic_sum(
                    "zookeeper.data_tree.size", "By",
                    emit_data, state.start_time_ns),
                build_gauge("zookeeper.file_descriptor.limit", "{file_descriptors}", fd_limit),
                build_non_monotonic_sum(
                    "zookeeper.file_descriptor.open", "{file_descriptors}",
                    fd_open, state.start_time_ns),
                build_cumulative_sum(
                    "zookeeper.fsync.exceeded_threshold.count", "{events}",
                    state.fsync_exceeded, state.start_time_ns),
                build_gauge("zookeeper.latency.avg", "ms", int(round(base_lat))),
                build_gauge("zookeeper.latency.max", "ms",
                            int(round(base_lat * rng.uniform(1.2, 2.0)))),
                build_gauge("zookeeper.latency.min", "ms",
                            max(0, int(round(base_lat * 0.1)))),
                build_cumulative_sum(
                    "zookeeper.packet.count", "{packets}",
                    state.packets_received, state.start_time_ns,
                    {"direction": "received"}),
                build_cumulative_sum(
                    "zookeeper.packet.count", "{packets}",
                    state.packets_sent, state.start_time_ns,
                    {"direction": "sent"}),
                build_non_monotonic_sum(
                    "zookeeper.request.active", "{requests}",
                    outstanding, state.start_time_ns),
                # ruok: 1 = imok (headline liveness signal for Healthy Nodes).
                build_gauge("zookeeper.ruok", "1", 1 if ruok_ok >= 0.5 else 0),
                build_non_monotonic_sum(
                    "zookeeper.watch.count", "{watches}",
                    emit_watch, state.start_time_ns),
                build_non_monotonic_sum(
                    "zookeeper.znode.count", "{znodes}",
                    emit_znode, state.start_time_ns),
            ]

            # Leader-only metrics (receiver only exposes these on leader).
            if is_leader:
                metrics.append(build_non_monotonic_sum(
                    "zookeeper.follower.count", "{followers}",
                    synced, state.start_time_ns, {"state": "synced"}))
                metrics.append(build_non_monotonic_sum(
                    "zookeeper.follower.count", "{followers}",
                    unsynced, state.start_time_ns, {"state": "unsynced"}))
                metrics.append(build_non_monotonic_sum(
                    "zookeeper.sync.pending", "{syncs}",
                    pending_syncs, state.start_time_ns))

            client.send_metrics_scoped(resource, scope, metrics)
            # Co-locate only while FD channel is active (OOTB AND-filter). Skip
            # healthy-path bulk to avoid ESBulk timeouts delaying OTLP ticks.
            if ratio > 0:
                bulk_docs.append(build_fd_utilization_doc(
                    resource,
                    fd_open=fd_open,
                    fd_limit=fd_limit,
                    timestamp_ms=ts_ms,
                ))
            # Outstanding saturation: also ESBulk so AVG tips land even if OTLP
            # ingest is delayed/diluted (channel trigger is often only 75).
            if outstanding_override > 0:
                bulk_docs.append(build_outstanding_doc(
                    resource,
                    outstanding=outstanding,
                    timestamp_ms=ts_ms,
                ))

        if es_bulk is not None and es_bulk.configured and bulk_docs:
            try:
                templates = {}
                if ratio > 0:
                    templates.update({
                        "metrics.zookeeper.file_descriptor.open": "gauge_long",
                        "metrics.zookeeper.file_descriptor.limit": "gauge_long",
                    })
                if outstanding_override > 0:
                    templates["metrics.zookeeper.request.active"] = "gauge_long"
                es_bulk.send_bulk(
                    _METRICS_DATA_STREAM,
                    bulk_docs,
                    dynamic_templates=templates or None,
                )
            except Exception as exc:
                logger.warning("ZK co-located bulk failed: %s", exc)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    if es_bulk is not None:
        es_bulk.close()
    logger.info("ZooKeeper metrics generator stopped.")


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
