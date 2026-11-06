#!/usr/bin/env python3
"""Envoy Proxy Generator — writes StatsD-shaped Envoy metrics via ES bulk.

Emits data matching the **stock** envoyproxy_otel content package dashboards
(Overview, Downstream & HTTP, Upstream & Proxy Health).

Authority:
  - elastic/integrations/packages/envoyproxy_otel/kibana/dashboard/*.json
  - elastic/integrations/packages/envoyproxy_otel/manifest.yml

The package declares ``statsdreceiver.otel``, but the shipped dashboards query
``FROM metrics-statsdreceiver-default`` with
``data_stream.dataset == "statsdreceiver"`` (and the same phrase filter on the
filter bar). OTLP intake cannot write that stream (it always appends
``.otel``), so this generator bulk-indexes pre-shaped docs into the stock
index/dataset the dashboards already expect. Dashboards stay unmodified.

Metric names match the bundled dashboard ES|QL queries exactly — including the
HTTP connection-manager prefix baked into the name
(``envoy.http.ingress_http.downstream_*``).

Panels call ``MAX`` / compare / divide on StatsD counter names. Those
operations reject ``counter_long``, and ``long / long`` integer-divides to 0,
so values are stored as plain numeric gauges (floats where panels divide).

Each tick writes one co-located document with every dashboard metric so
AND-filter panels (2xx success ratio, memory %) match.

Usage (standalone):
    python3 -m generators.envoyproxy_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/envoyproxy_otel@0.4.0"

import logging
import os
import random
import signal
import threading
import time
from typing import Any

from app.telemetry import ESBulkClient
from app.config import NAMESPACE
from generator_engine.db_common import (
    impact_value,
    CounterState,
    business_load,
    resolve_active_chaos,
)

DATA_STREAM = "metrics-statsdreceiver-default"
DATASET = "statsdreceiver"

logger = logging.getLogger("envoy-generator")
EMIT_INTERVAL_S = 15.0

# Metric names grounded from envoyproxy_otel dashboard ES|QL queries (28).
METRIC_NAMES = (
    # Server
    "envoy.server.live",
    "envoy.server.state",
    "envoy.server.uptime",
    "envoy.server.concurrency",
    "envoy.server.total_connections",
    "envoy.server.memory_allocated",
    "envoy.server.memory_heap_size",
    "envoy.server.memory_physical_size",
    "envoy.server.days_until_first_cert_expiring",
    # Cluster manager
    "envoy.cluster_manager.active_clusters",
    "envoy.cluster_manager.warming_clusters",
    "envoy.cluster_manager.cluster_added",
    # Listener manager
    "envoy.listener_manager.total_listeners_active",
    "envoy.listener_manager.total_listeners_warming",
    "envoy.listener_manager.workers_started",
    # Runtime
    "envoy.runtime.load_success",
    "envoy.runtime.override_dir_not_exists",
    # HTTP connection manager (prefix in metric name)
    "envoy.http.ingress_http.downstream_rq_total",
    "envoy.http.ingress_http.downstream_rq_completed",
    "envoy.http.ingress_http.downstream_rq_2xx",
    "envoy.http.ingress_http.downstream_rq_time",
    "envoy.http.ingress_http.downstream_cx_total",
    "envoy.http.ingress_http.downstream_cx_active",
    "envoy.http.ingress_http.downstream_cx_length_ms",
    "envoy.http.ingress_http.downstream_cx_rx_bytes_total",
    "envoy.http.ingress_http.downstream_cx_tx_bytes_total",
    # Admin HCM — OOTB error-rate alert COALESCE-sums ingress_http + admin.
    "envoy.http.admin.downstream_rq_completed",
    "envoy.http.admin.downstream_rq_2xx",
)

# Panels divide these — emit floats so ES|QL does not integer-truncate to 0.
_RATIO_FLOAT_FIELDS = frozenset({
    "envoy.http.ingress_http.downstream_rq_2xx",
    "envoy.http.ingress_http.downstream_rq_completed",
    "envoy.http.admin.downstream_rq_2xx",
    "envoy.http.admin.downstream_rq_completed",
    "envoy.server.memory_allocated",
    "envoy.server.memory_heap_size",
})


def _default_cloud() -> dict:
    return {"provider": "aws", "platform": "aws_ec2",
            "region": "us-east-1", "availability_zone": "us-east-1a"}


def build_metrics_doc(
    *,
    host_name: str,
    service_name: str,
    metrics: dict[str, int | float],
    cloud: dict[str, str] | None = None,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """One stock StatsD metrics doc with every dashboard field co-located."""
    cloud = cloud or _default_cloud()
    doc: dict[str, Any] = {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "data_stream": {
            "type": "metrics",
            "dataset": DATASET,
            "namespace": "default",
        },
        "host": {"name": host_name},
        "service": {"name": service_name},
        "cloud": {
            "provider": cloud.get("provider", "aws"),
            "region": cloud.get("region", "us-east-1"),
        },
    }
    for name, value in metrics.items():
        if name in _RATIO_FLOAT_FIELDS:
            doc[name] = float(value)
        else:
            doc[name] = value
    return doc


class _EnvoyState:
    """Cumulative counters + gauge baselines for one Envoy proxy."""

    def __init__(self, rng: random.Random, cluster_count: int = 3):
        self.cluster_count = cluster_count
        self.uptime = CounterState(rng, initial=rng.randint(86_400, 2_592_000))
        self.cluster_added = CounterState(rng, initial=cluster_count)
        self.rq_total = CounterState(rng, initial=rng.randint(50_000, 500_000))
        self.rq_completed = CounterState(rng, initial=0)
        self.rq_2xx = CounterState(rng, initial=0)
        self.cx_total = CounterState(rng, initial=rng.randint(5_000, 50_000))
        self.cx_rx_bytes = CounterState(rng, initial=rng.randint(10**8, 10**10))
        self.cx_tx_bytes = CounterState(rng, initial=rng.randint(10**8, 10**10))
        self.runtime_load_success = CounterState(rng, initial=rng.randint(1, 20))

        # Align completed/2xx with total so ratios start sane.
        self.rq_completed.value = self.rq_total.value
        self.rq_completed.start_time_ns = self.rq_total.start_time_ns
        self.rq_2xx.value = int(self.rq_total.value * 0.98)
        self.rq_2xx.start_time_ns = self.rq_total.start_time_ns

        self.live = 1
        self.state = 0  # 0 = LIVE in Envoy server.state
        self.concurrency = 4
        self.workers_started = 4
        self.listeners_active = 2
        self.listeners_warming = 0
        self.clusters_warming = 0
        self.cx_active = rng.randint(40, 200)
        self.cx_length_ms = round(rng.uniform(100.0, 5_000.0), 1)
        self.rq_time_ms = round(rng.uniform(5.0, 50.0), 1)
        self.total_connections = self.cx_active + rng.randint(0, 50)
        self.memory_heap = 256 * 1024 * 1024
        self.memory_allocated = int(self.memory_heap * rng.uniform(0.35, 0.55))
        self.memory_physical = self.memory_heap + 64 * 1024 * 1024
        self.cert_days = rng.randint(60, 365)
        self.override_dir_missing = 1  # typical when no override dir configured

    def tick(
        self,
        rng: random.Random,
        *,
        load: float = 1.0,
        error_rate: float = 0.0,
        cx_overflow_rate: float = 0.0,
        cert_days_remaining: int = 0,
        listeners_warming: int = 0,
        clusters_warming: int = 0,
        server_live: float = 1.0,
    ) -> None:
        self.uptime.tick(rng, drift=int(EMIT_INTERVAL_S))

        reqs = max(1, int(rng.randint(80, 400) * load))
        self.rq_total.tick(rng, drift=reqs)
        self.rq_completed.tick(rng, drift=reqs)
        # GT/SPEC may set percent (75.0) or fraction (0.75). Alert computes
        # ``(completed - 2xx) / completed * 100 > 5`` on absolute gauge values,
        # so a slow drift never catches up after a healthy cumulative base —
        # rebase 2xx under chaos so each doc's ratio breaches immediately.
        if error_rate > 1.0:
            err_frac = min(0.99, float(error_rate) / 100.0)
        else:
            err_frac = max(0.0, min(1.0, float(error_rate)))
        if err_frac >= 0.05:
            self.rq_2xx.value = max(0, int(self.rq_completed.value * (1.0 - err_frac)))
            self.rq_2xx.start_time_ns = self.rq_completed.start_time_ns
        else:
            # After chaos, absolute 2xx may still sit at the fault ratio; snap
            # back to a healthy cumulative so the 15m MAX window can clear.
            healthy_2xx = int(self.rq_completed.value * 0.98)
            if self.rq_2xx.value < int(self.rq_completed.value * 0.95):
                self.rq_2xx.value = healthy_2xx
                self.rq_2xx.start_time_ns = self.rq_completed.start_time_ns
            ok = max(0, int(reqs * (1.0 - err_frac)))
            self.rq_2xx.tick(rng, drift=ok)

        cxs = max(1, int(rng.randint(2, 20) * load))
        self.cx_total.tick(rng, drift=cxs)
        self.cx_rx_bytes.tick(rng, drift=reqs * rng.randint(500, 4_000))
        self.cx_tx_bytes.tick(rng, drift=reqs * rng.randint(500, 4_000))

        self.cx_active = max(5, int((40 + rng.randint(0, 160)) * load * (1 + cx_overflow_rate)))
        self.total_connections = self.cx_active + rng.randint(0, 80)
        self.cx_length_ms = round(rng.uniform(100.0, 5_000.0) * (1 + error_rate), 1)
        self.rq_time_ms = round(rng.uniform(5.0, 50.0) * (1 + error_rate * 3), 1)

        # Healthy memory util stays under 0.9; cx_overflow / errors push above.
        if cx_overflow_rate > 0 or error_rate > 0.5:
            util = min(0.98, 0.91 + cx_overflow_rate * 0.05 + error_rate * 0.05)
        else:
            util = min(0.85, 0.35 + rng.uniform(0, 0.15))
        self.memory_allocated = int(self.memory_heap * util)
        self.memory_physical = self.memory_heap + rng.randint(32, 96) * 1024 * 1024

        # Healthy baseline: no warming listeners/clusters (alerts fire on >0).
        self.clusters_warming = max(0, clusters_warming)
        self.listeners_warming = max(0, listeners_warming)
        if cert_days_remaining > 0:
            self.cert_days = cert_days_remaining
        elif self.cert_days < 7:
            self.cert_days = rng.randint(60, 365)

        if rng.random() < 0.02:
            self.runtime_load_success.tick(rng, drift=1)
            self.cluster_added.tick(rng, drift=1)

        if server_live < 0.5 or error_rate >= 0.95:
            self.live = 0
            self.state = 1
        else:
            self.live = 1
            self.state = 0

    def metric_values(self) -> dict[str, int | float]:
        return {
            "envoy.server.live": self.live,
            "envoy.server.state": self.state,
            "envoy.server.uptime": self.uptime.value,
            "envoy.server.concurrency": self.concurrency,
            "envoy.server.total_connections": self.total_connections,
            "envoy.server.memory_allocated": self.memory_allocated,
            "envoy.server.memory_heap_size": self.memory_heap,
            "envoy.server.memory_physical_size": self.memory_physical,
            "envoy.server.days_until_first_cert_expiring": self.cert_days,
            "envoy.cluster_manager.active_clusters": self.cluster_count,
            "envoy.cluster_manager.warming_clusters": self.clusters_warming,
            "envoy.cluster_manager.cluster_added": self.cluster_added.value,
            "envoy.listener_manager.total_listeners_active": self.listeners_active,
            "envoy.listener_manager.total_listeners_warming": self.listeners_warming,
            "envoy.listener_manager.workers_started": self.workers_started,
            "envoy.runtime.load_success": self.runtime_load_success.value,
            "envoy.runtime.override_dir_not_exists": self.override_dir_missing,
            "envoy.http.ingress_http.downstream_rq_total": self.rq_total.value,
            "envoy.http.ingress_http.downstream_rq_completed": self.rq_completed.value,
            "envoy.http.ingress_http.downstream_rq_2xx": self.rq_2xx.value,
            "envoy.http.ingress_http.downstream_rq_time": self.rq_time_ms,
            "envoy.http.ingress_http.downstream_cx_total": self.cx_total.value,
            "envoy.http.ingress_http.downstream_cx_active": self.cx_active,
            "envoy.http.ingress_http.downstream_cx_length_ms": self.cx_length_ms,
            "envoy.http.ingress_http.downstream_cx_rx_bytes_total": self.cx_rx_bytes.value,
            "envoy.http.ingress_http.downstream_cx_tx_bytes_total": self.cx_tx_bytes.value,
            # Admin traffic is a small share of ingress; keep columns present
            # so ES|QL COALESCE(admin, …) does not fail on unknown fields.
            "envoy.http.admin.downstream_rq_completed": max(1, int(self.rq_completed.value * 0.05)),
            "envoy.http.admin.downstream_rq_2xx": max(1, int(self.rq_2xx.value * 0.05)),
        }



from generators.envoyproxy_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def run(
    client: ESBulkClient,
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("envoy")
    cluster_names: list[str] = [
        ScenarioIdentity(ns).label(name)
        for name in (cfg.get("cluster_names") or ["backend-svc", "api-svc", "auth-svc"])
    ]
    cloud = cfg.get("cloud") or _default_cloud()
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("envoy")


    state = _EnvoyState(rng, cluster_count=len(cluster_names))

    if not client.configured:
        logger.error("ESBulkClient is not configured; envoyproxy_otel generator cannot emit")
        return

    logger.info(
        "Envoy generator started (namespace=%s, clusters=%s, stream=%s)",
        ns, cluster_names, DATA_STREAM,
    )

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="envoyproxy_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        c = active_chaos or {}
        error_rate = float(c.get("high_downstream_error_rate", 0.0))
        if error_rate <= 0:
            error_rate = float(impact_value(c, "high_downstream_error_rate", default=0.0))
        cx_overflow_rate = float(c.get("high_memory_pressure", 0.0))
        if cx_overflow_rate <= 0:
            cx_overflow_rate = float(impact_value(c, "high_memory_pressure", default=0.0))
        cert_days = int(c.get("certificate_expiring_soon", 0) or impact_value(c, "certificate_expiring_soon", default=0) or 0)
        listeners_warming = int(c.get("listeners_warming", 0) or 0)
        clusters_warming = int(c.get("warming_clusters_stuck", 0) or impact_value(c, "warming_clusters_stuck", default=0) or 0)
        server_live = float(impact_value(c, "server_not_live", "server_state_not_live", default=1.0))
        if float(c.get("server_not_live", 0.0)) > 0 or float(c.get("server_state_not_live", 0.0)) > 0:
            server_live = 0.0
        load = business_load()
        state.tick(
            rng, load=load, error_rate=error_rate, cx_overflow_rate=cx_overflow_rate,
            cert_days_remaining=cert_days, listeners_warming=listeners_warming,
            clusters_warming=clusters_warming, server_live=server_live,
        )
        ts_ms = int(time.time() * 1000)

        client.send_bulk(
            DATA_STREAM,
            [
                build_metrics_doc(
                    host_name=host_name,
                    service_name=service_name,
                    metrics=state.metric_values(),
                    cloud=cloud,
                    timestamp_ms=ts_ms,
                ),
            ],
        )

    run_tick_loop(stop_event, _emit_tick, interval=lambda: EMIT_INTERVAL_S)

    logger.info("Envoy generator stopped.")


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "60"))
    def _stop(sig, frame): stop_event.set()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    client = ESBulkClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()

if __name__ == "__main__":
    main()
