#!/usr/bin/env python3
"""Memcached Metrics Generator — sends Memcached OTel receiver metrics via OTLP.

Emits data matching the memcached_otel content package
(discovery.datasets: memcachedreceiver.otel), which populates the bundled
Memcached Overview dashboard.

Field names and attribute keys come from:
  - opentelemetry-collector-contrib/receiver/memcachedreceiver/metadata.yaml
  - elastic/integrations/packages/memcached_otel/kibana/dashboard/*.json

Semantic note — ``memcached.operation_hit_ratio``:
  The upstream memcachedreceiver historically swaps hit/miss args, so the
  metric is a *miss* ratio (0–100). The Elastic package compensates with
  ``hit_ratio = (100 - miss_ratio) / 100``. Emit the miss ratio so panels,
  alerts, and SLOs read correctly.

Usage (standalone):
    python3 -m generators.memcached_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/memcached_otel@0.3.0"

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
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

METRIC_NAMES = (
    "memcached.bytes",
    "memcached.commands",
    "memcached.connections.current",
    "memcached.connections.total",
    "memcached.cpu.usage",
    "memcached.current_items",
    "memcached.evictions",
    "memcached.network",
    "memcached.operation_hit_ratio",
    "memcached.operations",
    "memcached.threads",
)

COMMANDS = ("get", "set", "flush", "touch")
OPERATIONS = ("get", "increment", "decrement")
CPU_STATES = ("user", "system")

MEMCACHED_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/memcachedreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("memcached-generator")
BATCH_INTERVAL_MIN = 10
BATCH_INTERVAL_MAX = 30
MEMORY_LIMIT_BYTES = 1024 * 1024 * 1024  # 1 GiB default


def _default_cloud() -> dict:
    return {"provider": "gcp", "platform": "gcp_compute_engine",
            "region": "us-central1", "availability_zone": "us-central1-b"}


def miss_ratio_percent(hits: int, misses: int) -> float:
    """Miss ratio 0–100, matching the upstream receiver + Elastic package."""
    total = hits + misses
    if total <= 0:
        return 0.0
    return round((misses / total) * 100.0, 2)


class MemcachedState(CounterState):
    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.connections_total = rng.randint(1_000, 500_000)
        self.commands = {
            "get": rng.randint(100_000, 50_000_000),
            "set": rng.randint(10_000, 5_000_000),
            "flush": rng.randint(0, 1_000),
            "touch": rng.randint(1_000, 500_000),
        }
        # operations[operation][type] — cumulative counts.
        # OOTB hit-ratio alert uses AVG(operation_hit_ratio) where that gauge is
        # actually the miss ratio — keep get miss ratio well under 20%.
        get_hits = rng.randint(5_000_000, 50_000_000)
        get_misses = max(1, int(get_hits * rng.uniform(0.02, 0.08)))
        self.operations = {
            op: {
                "hit": get_hits if op == "get" else rng.randint(100, 50_000),
                "miss": get_misses if op == "get" else rng.randint(10, 10_000),
            }
            for op in OPERATIONS
        }
        self.evictions = 0
        self.net_rx = rng.randint(1_000_000_000, 100_000_000_000)
        self.net_tx = rng.randint(5_000_000_000, 500_000_000_000)
        self.cpu = {s: rng.uniform(10.0, 100.0) for s in CPU_STATES}

    def tick(
        self,
        rng: random.Random,
        active_chaos: dict | None = None,
        evict_per_s: float = 0.0,
        miss_mult: float = 1.0,
        cmd_mult: float = 1.0,
        cpu_cores_target: float = 0.0,
        interval_s: float = 15.0,
    ):
        load = business_load()
        delta = max(0, int(rng.randint(100, 10_000) * load * cmd_mult))
        self.connections_total += int(rng.randint(1, 20) * load)

        # miss_mult only skews hit/miss split within delta — never multiplies
        # total command volume (that broke low-throughput when sibling default=800).
        if miss_mult > 1.0:
            hit_rate = 0.05
            get_hits = int(delta * hit_rate)
            get_misses = max(0, delta - get_hits)
        else:
            hit_rate = 0.9
            get_hits = int(delta * hit_rate)
            get_misses = max(0, delta - get_hits)
        self.commands["get"] += get_hits + get_misses
        self.commands["set"] += int(delta * 0.1) if delta else 0
        self.commands["flush"] += rng.randint(0, 1 if (active_chaos and cmd_mult > 0) else 0)
        self.commands["touch"] += int(delta * rng.uniform(0.01, 0.05)) if delta else 0

        self.operations["get"]["hit"] += get_hits
        self.operations["get"]["miss"] += get_misses
        for op in ("increment", "decrement"):
            op_delta = int(delta * rng.uniform(0.005, 0.02)) if delta else 0
            op_hits = int(op_delta * hit_rate)
            self.operations[op]["hit"] += op_hits
            self.operations[op]["miss"] += max(0, op_delta - op_hits)

        # Eviction alert is RATE > 10/sec — only grow under chaos.
        self.evictions += int(evict_per_s * interval_s)
        self.net_rx += int(max(1, delta) * rng.randint(100, 1_000))
        self.net_tx += int(max(1, delta) * rng.randint(500, 5_000))
        for s in CPU_STATES:
            if cpu_cores_target > 0:
                # Split target cores across user/system so SUM(RATE) ≈ target.
                bump = (cpu_cores_target * interval_s) / max(1, len(CPU_STATES))
                bump *= rng.uniform(0.98, 1.02)
            else:
                # Healthy RATE stays well under 0.8 cores (alert threshold).
                bump = rng.uniform(0.002, 0.02) * load
            self.cpu[s] += bump



from generators.memcached_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("memcached")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("memcached")
    cloud = cfg.get("cloud") or _default_cloud()

    state = MemcachedState(rng)
    current_items = rng.randint(10_000, 1_000_000)


    resource = build_signal_resource(
        ns=ns, signal_type="metrics", dataset=otel_dataset("memcachedreceiver"),
        service_name=service_name, service_version="1.6.22",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name, cloud=cloud,
    )
    scope = {"name": MEMCACHED_SCOPE, "version": SCOPE_VERSION}
    logger.info("Memcached metrics generator started (namespace=%s)", ns)

    interval_s = None
    def _emit_tick() -> None:
        nonlocal current_items, interval_s
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="memcached_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        def _key(name: str, default: float) -> float:
            value = impact_value(active_chaos, name, default=default)
            return default if value is None else float(value)  # 0 is a real value

        # Each key moves only the signal its own package alert reads.
        evict_per_s = max(0.0, _key("high_eviction_rate", 0.0))  # alert: RATE > 10 /s
        # Target get hit ratio in percent; SPEC default 800 = healthy sentinel (NOT a miss
        # multiplier). The alert watches the miss ratio and fires above 20%, so a target
        # below 80 breaches; 100 and up leave the healthy baseline.
        hit_target = _key("low_hit_ratio_for_get_operations", 800.0)
        miss_mult = 12.0 if hit_target < 80.0 else 1.0
        conn_override = int(max(0.0, _key("high_connection_count", 0.0)))  # alert: > 800
        # Percent of one core; the alert is SUM(RATE(cpu.usage)) > 0.8 cores.
        cpu_cores = max(0.0, _key("high_cpu_usage", 0.0)) / 100.0
        # SPEC default 100 = healthy. Any lower value stops command traffic; the alert is
        # SUM(RATE(commands)) < 1/s.
        cmd_mult = 1.0 if _key("low_command_throughput", 100.0) >= 100.0 else 0.0
        interval_s = rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX)
        state.tick(
            rng, active_chaos, evict_per_s, miss_mult,
            cmd_mult=cmd_mult, cpu_cores_target=cpu_cores, interval_s=interval_s,
        )
        load = business_load()

        if evict_per_s > 3.0:
            current_items = max(0, current_items - int(rng.randint(1_000, 10_000)))
        else:
            current_items += int(rng.randint(-100, 500) * load)
        current_items = max(0, current_items)

        get_hits = state.operations["get"]["hit"]
        get_misses = state.operations["get"]["miss"]
        # Healthy connections stay under 800; chaos can force the alert.
        current_conns = (
            conn_override if conn_override > 0 else int(rng.randint(5, 200) * load)
        )

        # Upstream receiver reports miss ratio; package derives hit ratio.
        # OOTB alert: AVG(operation_hit_ratio) as miss_ratio_pct > 20.
        # Lifetime counters dilute slowly — force a breach gauge under chaos.
        hit_gauge = (
            100.0 - hit_target if hit_target < 100.0
            else miss_ratio_percent(get_hits, get_misses)
        )
        metrics = [
            build_gauge("memcached.bytes", "By",
                        int(MEMORY_LIMIT_BYTES * rng.uniform(0.3, 0.8) *
                            (1.0 if evict_per_s <= 1.0 else min(1.0, evict_per_s * 0.3)))),
            build_gauge("memcached.connections.current", "{connections}",
                        current_conns),
            build_cumulative_sum("memcached.connections.total", "{connections}",
                                 state.connections_total, state.start_time_ns),
            build_gauge("memcached.current_items", "{items}", current_items),
            build_cumulative_sum("memcached.evictions", "{evictions}",
                                 state.evictions, state.start_time_ns),
            build_cumulative_sum("memcached.network", "By",
                                 state.net_rx, state.start_time_ns,
                                 {"direction": "received"}),
            build_cumulative_sum("memcached.network", "By",
                                 state.net_tx, state.start_time_ns,
                                 {"direction": "sent"}),
            build_gauge("memcached.operation_hit_ratio", "%",
                        hit_gauge,
                        {"operation": "get"}),
            build_gauge("memcached.threads", "{threads}",
                        int(rng.randint(4, 16))),
        ]
        for command in COMMANDS:
            metrics.append(build_cumulative_sum(
                "memcached.commands", "{commands}",
                state.commands[command], state.start_time_ns,
                {"command": command},
            ))
        for op in OPERATIONS:
            for op_type in ("hit", "miss"):
                metrics.append(build_cumulative_sum(
                    "memcached.operations", "{operations}",
                    state.operations[op][op_type], state.start_time_ns,
                    {"operation": op, "type": op_type},
                ))
        for cpu_state in CPU_STATES:
            metrics.append(build_cumulative_sum(
                "memcached.cpu.usage", "s",
                round(state.cpu[cpu_state], 3), state.start_time_ns,
                {"state": cpu_state},
            ))

        client.send_metrics_scoped(resource, scope, metrics)
    run_tick_loop(stop_event, _emit_tick, interval=lambda: interval_s)

    logger.info("Memcached metrics generator stopped.")


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
