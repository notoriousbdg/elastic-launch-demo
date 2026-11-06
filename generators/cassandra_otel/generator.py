#!/usr/bin/env python3
"""Cassandra Generator — sends JMX OTel metrics via OTLP.

Emits data matching the cassandra_otel content package
(discovery.datasets: cassandra.otel), which populates the bundled
Overview dashboard.

Signal types:
  - Metrics: cassandra.client.request.count,
             cassandra.client.request.error.count,
             cassandra.client.request.range_slice.latency.99p,
             cassandra.client.request.read.latency.99p,
             cassandra.client.request.write.latency.99p,
             cassandra.compaction.tasks.pending,
             cassandra.compaction.tasks.completed,
             cassandra.storage.load.count,
             cassandra.storage.total_hints.in_progress.count

Field names and attribute keys come from:
  - open-telemetry/opentelemetry-java-contrib (jmx-metrics cassandra.groovy)
  - elastic/integrations/packages/cassandra_otel/kibana/dashboard/*.json

Dashboard panels group heavily by ``server.address`` (Node control + every
"by Node" chart), so this generator emits one resource per Cassandra node.

Usage (standalone):
    python3 -m generators.cassandra_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/cassandra_otel@0.3.1"

import logging
import os
import random
import signal
import threading

from app.telemetry import OTLPClient
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

# ── Grounding constants — do not change names ─────────────────────────────────

METRIC_NAMES = (
    "cassandra.client.request.count",
    "cassandra.client.request.error.count",
    "cassandra.client.request.range_slice.latency.99p",
    "cassandra.client.request.read.latency.99p",
    "cassandra.client.request.write.latency.99p",
    "cassandra.compaction.tasks.pending",
    "cassandra.compaction.tasks.completed",
    "cassandra.storage.load.count",
    "cassandra.storage.total_hints.in_progress.count",
)

CASSANDRA_SCOPE = "io.opentelemetry.contrib.jmxmetrics"
SCOPE_VERSION = "1.0.0"

# Attribute values as declared in cassandra.groovy / dashboard controls
OPERATIONS = ("Read", "Write", "RangeSlice")
ERROR_STATUSES = ("Unavailable", "Timeout", "Failure")

logger = logging.getLogger("cassandra-generator")

BATCH_INTERVAL_MIN = 10
BATCH_INTERVAL_MAX = 20


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_ec2",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


class CassandraMetricState(CounterState):
    """Per-node cumulative counter state for Cassandra."""

    def __init__(self, rng: random.Random, storage_bytes: int):
        super().__init__(rng)
        self.request_count = {op: rng.randint(10_000, 10_000_000) for op in OPERATIONS}
        self.error_count = {
            (op, st): 0
            for op in OPERATIONS
            for st in ERROR_STATUSES
        }
        self.compaction_completed = rng.randint(0, 10_000)
        # UpDownCounter baseline for on-disk load (bytes).
        self.storage_bytes = int(storage_bytes)
        self.hints_in_progress = 0

    def tick(
        self,
        rng: random.Random,
        active_chaos: dict | None = None,
        error_rate_multiplier: float = 1.0,
        compaction_backlog: int = 0,
        open_file_error_rate: float | None = None,
    ) -> int:
        """Advance counters; return pending compaction tasks for this scrape.

        Parameters
        ----------
        open_file_error_rate:
            When set (0-1), probability that this tick generates an additional
            SSTable open-file-descriptor error burst on every operation, modelling
            a "Too many open files" / ulimit scenario on the Cassandra data tier.
        """
        load = business_load()
        for op in OPERATIONS:
            delta = int(rng.randint(50, 5_000) * load)
            if active_chaos:
                delta = int(delta * rng.uniform(0.5, 2.0))
            self.request_count[op] += max(1, delta)

        err_mult = max(1.0, float(error_rate_multiplier))
        # open_file_error_rate fires probabilistically and adds a large error burst
        # (SSTable open-descriptor failures) on top of the normal error rate.
        fd_burst = (open_file_error_rate is not None
                    and rng.random() < float(open_file_error_rate))
        for op in OPERATIONS:
            for st in ERROR_STATUSES:
                bump = int(rng.randint(0, 10) * err_mult) if err_mult > 1.0 else 0
                if fd_burst:
                    bump += rng.randint(20, 150)  # burst: "Too many open files"
                self.error_count[(op, st)] += max(0, bump)

        self.compaction_completed += rng.randint(0, 5)
        # Slight growth with occasional compaction reclaim.
        self.storage_bytes = max(
            1,
            self.storage_bytes + rng.randint(-500_000, 1_000_000),
        )
        if compaction_backlog > 0:
            self.hints_in_progress = rng.randint(50, 500)
            pending = rng.randint(10, 100) + int(compaction_backlog)
        else:
            self.hints_in_progress = 0
            pending = 0
        c = active_chaos or {}
        hints_key = float(c.get("hints_in_progress_replicas_unreachable", 0.0))
        if hints_key > 0:
            self.hints_in_progress = max(self.hints_in_progress, int(hints_key))
        return pending


def _generate_metrics(
    state: CassandraMetricState,
    rng: random.Random,
    active_chaos: dict | None = None,
    latency_multiplier: float = 1.0,
    error_rate_multiplier: float = 1.0,
    compaction_backlog: int = 0,
    open_file_error_rate: float | None = None,
) -> list[dict]:
    pending = state.tick(
        rng,
        active_chaos,
        error_rate_multiplier=error_rate_multiplier,
        compaction_backlog=compaction_backlog,
        open_file_error_rate=open_file_error_rate,
    )
    metrics: list[dict] = []

    # ── Request counts (cumulative monotonic) ──
    for op in OPERATIONS:
        metrics.append(build_cumulative_sum(
            "cassandra.client.request.count", "{request}",
            state.request_count[op], state.start_time_ns,
            {"operation": op},
        ))

    # ── Error counts (cumulative monotonic) ──
    for op in OPERATIONS:
        for st in ERROR_STATUSES:
            metrics.append(build_cumulative_sum(
                "cassandra.client.request.error.count", "{error}",
                state.error_count[(op, st)], state.start_time_ns,
                {"operation": op, "status": st},
            ))

    # ── Latency percentiles (gauges, microseconds, double — groovy doubleValueCallback)
    lat_mult = max(1.0, float(latency_multiplier))
    c = active_chaos or {}
    read_lat = float(c.get("high_read_latency_p99", 0.0))
    write_lat = float(c.get("high_write_latency_p99", 0.0))
    range_lat = float(c.get("high_range_slice_latency_p99", 0.0))

    base_read_us = read_lat if read_lat > 2000 else rng.uniform(200.0, 2_000.0) * lat_mult
    base_write_us = write_lat if write_lat > 800 else rng.uniform(100.0, 800.0) * lat_mult
    base_range_us = range_lat if range_lat > 10000 else rng.uniform(1_000.0, 10_000.0) * lat_mult

    metrics.append(build_gauge(
        "cassandra.client.request.read.latency.99p", "us", round(base_read_us, 3),
    ))
    metrics.append(build_gauge(
        "cassandra.client.request.write.latency.99p", "us", round(base_write_us, 3),
    ))
    metrics.append(build_gauge(
        "cassandra.client.request.range_slice.latency.99p", "us", round(base_range_us, 3),
    ))

    # ── Compaction tasks ──
    metrics.append(build_gauge("cassandra.compaction.tasks.pending", "{task}", pending))
    metrics.append(build_cumulative_sum(
        "cassandra.compaction.tasks.completed", "{task}",
        state.compaction_completed, state.start_time_ns,
    ))

    # ── Storage (UpDownCounters in cassandra.groovy) ──
    metrics.append(build_non_monotonic_sum(
        "cassandra.storage.load.count", "By",
        state.storage_bytes, state.start_time_ns,
    ))
    metrics.append(build_non_monotonic_sum(
        "cassandra.storage.total_hints.in_progress.count", "{hint}",
        state.hints_in_progress, state.start_time_ns,
    ))

    return metrics



from generators.cassandra_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run Cassandra generator loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {})
    else:
        ns = NAMESPACE
        cfg = {}

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("cassandra-node")
    storage_bytes: int = cfg.get("storage_bytes", 50 * 1024 * 1024 * 1024)  # 50 GiB default
    cloud = cfg.get("cloud") or _default_cloud()

    # Overview dashboard Node control + every "by Node" panel key on server.address.
    default_nodes = [ScenarioIdentity(ns).label("cassandra", i) for i in range(1, 4)]
    node_names: list[str] = cfg.get("node_names") or default_nodes
    # Back-compat: a single host_name override collapses to one node.
    if cfg.get("host_name") and not cfg.get("node_names"):
        node_names = [cfg["host_name"]]


    node_states = {
        name: CassandraMetricState(rng, storage_bytes=storage_bytes + rng.randint(0, 5_000_000_000))
        for name in node_names
    }
    scope = {"name": CASSANDRA_SCOPE, "version": SCOPE_VERSION}

    logger.info(
        "Cassandra generator started (namespace=%s, nodes=%d)",
        ns, len(node_names),
    )

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="cassandra_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        c = active_chaos or {}
        read_lat = float(c.get("high_read_latency_p99", 0.0))
        write_lat = float(c.get("high_write_latency_p99", 0.0))
        range_lat = float(c.get("high_range_slice_latency_p99", 0.0))
        lat_mult = max(1.0, read_lat / 2000.0, write_lat / 800.0, range_lat / 10000.0)
        if lat_mult == 1.0:
            lat_mult = float(impact_value(c, "high_range_slice_latency_p99", "high_read_latency_p99", "high_write_latency_p99", default=1.0))
        err_mult = float(c.get("high_error_rate_by_node", 0.0)) or float(impact_value(c, "high_error_rate_by_node", default=1.0))
        backlog = int(c.get("compaction_falling_behind_pending_tasks", 0) or impact_value(c, "compaction_falling_behind_pending_tasks", default=0))
        hints_key = float(c.get("hints_in_progress_replicas_unreachable", 0.0))
        timeout_key = float(c.get("request_errors_timeout_unavailable_failure", 0.0))
        fd_error_rate = timeout_key if timeout_key > 0 else None

        for idx, node_name in enumerate(node_names):
            resource = build_signal_resource(
                ns=ns,
                signal_type="metrics",
                dataset=otel_dataset("cassandra"),
                service_name=service_name,
                service_version="4.1.3",
                service_instance_id=f"{service_name}-{idx + 1:03d}",
                host_name=node_name,
                cloud=cloud,
                extra_attrs={"server.address": node_name},
            )
            metrics = _generate_metrics(
                node_states[node_name],
                rng,
                active_chaos,
                latency_multiplier=lat_mult,
                error_rate_multiplier=err_mult,
                compaction_backlog=backlog,
                open_file_error_rate=fd_error_rate,
            )
            client.send_metrics_scoped(resource, scope, metrics)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("Cassandra generator stopped.")


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
