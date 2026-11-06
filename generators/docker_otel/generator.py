#!/usr/bin/env python3
"""Docker Stats Metrics Generator — sends Docker OTel receiver metrics via OTLP.

Emits data matching the docker_otel content package
(discovery.datasets: dockerstatsreceiver.otel), which populates the bundled
Docker container dashboards.

Note: docker_otel uses Lens dashboards. Field names verified from the
sourceField/field keys inside the dashboard JSON:
  container.cpu.utilization, container.cpu.usage.kernelmode,
  container.cpu.usage.usermode, container.memory.percent,
  container.memory.usage.total, container.memory.usage.limit,
  container.blockio.io_service_bytes_recursive,
  container.network.io.usage.rx_bytes, container.network.io.usage.tx_bytes

Datapoint attributes required by Disk/Network I/O panels
(dockerstatsreceiver semantic convention):
  blockio → device_major, device_minor, operation (read|write)
  network → interface (e.g. eth0)

Resource attribute keys:
  container.id, container.name, container.image.name,
  container.hostname, container.runtime

Usage (standalone):
    python3 -m generators.docker_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity
from generator_engine.scrape_clock import scrape_now_ns

SOURCE_INTEGRATION = "elastic/integrations packages/docker_otel@0.3.1"

import logging
import os
import random
import signal
import threading

from app.telemetry import OTLPClient
from app.config import NAMESPACE
from generator_engine.db_common import (
    CounterState,
    MultiEntityState,
    build_cumulative_sum,
    build_gauge,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

# ── Grounding constants — do not change names ─────────────────────────────────
# Field names verified from docker_otel Lens dashboard sourceField/field keys.

METRIC_NAMES = (
    "container.cpu.utilization",
    "container.cpu.usage.kernelmode",
    "container.cpu.usage.usermode",
    "container.memory.percent",
    "container.memory.usage.total",
    "container.memory.usage.limit",
    "container.blockio.io_service_bytes_recursive",
    "container.network.io.usage.rx_bytes",
    "container.network.io.usage.tx_bytes",
    # Additional stats receiver metrics
    "container.cpu.throttling_data.throttled_periods",
    "container.cpu.throttling_data.total_periods",
    "container.pids.count",
)

DOCKER_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/dockerstatsreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("docker-generator")
BATCH_INTERVAL_MIN = 10
BATCH_INTERVAL_MAX = 30

MEMORY_LIMIT = 2 * 1024 * 1024 * 1024  # 2 GiB per container

# Block device numbers matching dockerstatsreceiver sample data (virtio/mapper).
_BLKIO_DEVICES = (
    {"device_major": "254", "device_minor": "0"},
    {"device_major": "8", "device_minor": "0"},
)
_NETWORK_INTERFACES = ("eth0", "eth1")


def _default_cloud() -> dict:
    return {"provider": "gcp", "platform": "gcp_compute_engine",
            "region": "us-central1", "availability_zone": "us-central1-b"}


class ContainerState(CounterState):
    """Cumulative counter state for a single container.

    ``_cpu_util`` is the container's current CPU utilization (0–1), evolved each
    tick so that ``container.cpu.utilization`` and the ``kernelmode``/``usermode``
    cumulative counters are derived from the same driver — they are no longer
    independent random draws that can contradict each other.

    CPU throttling (``throttled_periods``) is gated on CPU pressure rather than
    on any active chaos, so it only climbs when utilization is actually high.
    """

    def __init__(self, rng: random.Random):
        # Unused parent .value — keep near zero (RATE spike guard).
        super().__init__(rng, initial=0)
        # Counters start near zero so RATE charts are not dominated by a spike.
        self.cpu_kernel = rng.randint(0, 50_000_000)
        self.cpu_user = rng.randint(0, 200_000_000)
        # Baseline util the random walk orbits; load scales the emitted value
        # without compounding into _cpu_util each tick (that decayed to 0.001).
        self._cpu_base: float = rng.uniform(0.12, 0.45)
        self._cpu_util: float = self._cpu_base
        # Per (device, operation) cumulative block I/O bytes
        self.blkio_read = {
            (d["device_major"], d["device_minor"]): rng.randint(0, 50_000_000)
            for d in _BLKIO_DEVICES
        }
        self.blkio_write = {
            (d["device_major"], d["device_minor"]): rng.randint(0, 25_000_000)
            for d in _BLKIO_DEVICES
        }
        # Per-interface cumulative network bytes
        self.net_rx = {iface: rng.randint(0, 20_000_000) for iface in _NETWORK_INTERFACES}
        self.net_tx = {iface: rng.randint(0, 10_000_000) for iface in _NETWORK_INTERFACES}
        self.throttled_periods = 0
        self.total_periods = rng.randint(100, 5_000)

    def tick(self, rng: random.Random, active_chaos: dict | None = None):
        load = business_load()
        cpu_mult = float((active_chaos or {}).get("cpu_utilization_multiplier", 1.0))
        # Random-walk the baseline; apply load/chaos as a scale on emit only.
        # (Previously util *= load every tick, which collapses to the 0.001 floor.)
        self._cpu_base += rng.uniform(-0.04, 0.04)
        self._cpu_base = max(0.05, min(0.85, self._cpu_base))
        self._cpu_util = max(0.01, min(0.99, self._cpu_base * load * max(1.0, cpu_mult)))
        # Advance CPU cumulative counters proportionally to utilization.
        # ~15s scrape: util 0.3 ≈ 0.3 core → ~4.5e9 ns total; split kernel/user.
        interval_ns = 15_000_000_000
        total_ns = int(interval_ns * self._cpu_util * rng.uniform(0.85, 1.15))
        kernel_share = rng.uniform(0.15, 0.35)
        self.cpu_kernel += max(1, int(total_ns * kernel_share))
        self.cpu_user += max(1, int(total_ns * (1.0 - kernel_share)))
        for key in self.blkio_read:
            self.blkio_read[key] += int(rng.randint(100_000, 5_000_000) * load)
            self.blkio_write[key] += int(rng.randint(50_000, 2_500_000) * load)
        for iface in self.net_rx:
            self.net_rx[iface] += int(rng.randint(100_000, 10_000_000) * load)
            self.net_tx[iface] += int(rng.randint(50_000, 5_000_000) * load)
        # Throttling:
        # - cpu_throttle_rate (explicit): target throttled/total ratio; ramp total_periods
        #   so the ratio converges to the target over a few ticks.
        # - Otherwise: CPU-pressure gated — only climbs when util > 0.7.
        periods_inc = rng.randint(100, 1_000)
        self.total_periods += periods_inc
        throttle_rate = (active_chaos or {}).get("cpu_throttle_rate")
        if throttle_rate is not None:
            # Drive throttled_periods so throttled/total ≈ cpu_throttle_rate
            target_throttled = int(self.total_periods * float(throttle_rate))
            deficit = max(0, target_throttled - self.throttled_periods)
            self.throttled_periods += min(deficit, int(periods_inc * float(throttle_rate)) + rng.randint(1, 10))
        elif self._cpu_util > 0.70:
            throttle_scale = (self._cpu_util - 0.70) / 0.30  # 0-1
            self.throttled_periods += int(rng.randint(5, 50) * throttle_scale)


def _generate_container_metrics(
    container_id: str,
    container_name: str,
    image_name: str,
    state: ContainerState,
    resource: dict,
    rng: random.Random,
    active_chaos: dict | None = None,
) -> list[dict]:
    mem_mult = float((active_chaos or {}).get("memory_utilization_multiplier", 1.0))
    # memory_limit_bytes: optional per-fault limit override (models "RAM cap too small")
    _limit_override = (active_chaos or {}).get("memory_limit_bytes")
    effective_limit = int(_limit_override) if _limit_override is not None else MEMORY_LIMIT
    state.tick(rng, active_chaos)

    metrics: list[dict] = []

    # CPU utilization (0–1 ratio) — read from state so it is coherent with the
    # cumulative kernelmode/usermode counters that were advanced in tick().
    cpu_util = state._cpu_util
    metrics.append(build_gauge("container.cpu.utilization", "1", round(cpu_util, 6)))

    # CPU usage counters (nanoseconds)
    metrics.append(build_cumulative_sum("container.cpu.usage.kernelmode", "ns",
                                        state.cpu_kernel, state.start_time_ns))
    metrics.append(build_cumulative_sum("container.cpu.usage.usermode", "ns",
                                        state.cpu_user, state.start_time_ns))

    # Memory — clamp mem_used to effective_limit so usage never exceeds the cgroup
    # limit (physically impossible in real containers).  mem_pct is derived, not
    # independently drawn, so it always equals usage / limit.
    # effective_limit may be overridden by memory_limit_bytes to model a too-small cap.
    mem_util = min(1.0, rng.uniform(0.1, 0.6) * mem_mult)
    mem_used = int(effective_limit * mem_util)
    mem_pct = mem_util  # derived: usage / limit, already clamped to [0, 1]
    metrics.append(build_gauge("container.memory.percent", "1", round(mem_pct, 6)))
    metrics.append(build_gauge("container.memory.usage.total", "By", mem_used))
    metrics.append(build_gauge("container.memory.usage.limit", "By", effective_limit))

    # Block I/O — dashboards break down by device_major/device_minor + operation
    for major, minor in state.blkio_read:
        device_attrs = {"device_major": major, "device_minor": minor}
        metrics.append(build_cumulative_sum(
            "container.blockio.io_service_bytes_recursive", "By",
            state.blkio_read[(major, minor)], state.start_time_ns,
            {**device_attrs, "operation": "read"},
        ))
        metrics.append(build_cumulative_sum(
            "container.blockio.io_service_bytes_recursive", "By",
            state.blkio_write[(major, minor)], state.start_time_ns,
            {**device_attrs, "operation": "write"},
        ))

    # Network I/O — dashboards break down by interface
    for iface in state.net_rx:
        iface_attrs = {"interface": iface}
        metrics.append(build_cumulative_sum(
            "container.network.io.usage.rx_bytes", "By",
            state.net_rx[iface], state.start_time_ns, iface_attrs,
        ))
        metrics.append(build_cumulative_sum(
            "container.network.io.usage.tx_bytes", "By",
            state.net_tx[iface], state.start_time_ns, iface_attrs,
        ))

    # CPU throttling
    metrics.append(build_cumulative_sum(
        "container.cpu.throttling_data.throttled_periods", "{period}",
        state.throttled_periods, state.start_time_ns,
    ))
    metrics.append(build_cumulative_sum(
        "container.cpu.throttling_data.total_periods", "{period}",
        state.total_periods, state.start_time_ns,
    ))

    # PID count — scaled by pids_multiplier when set (models thread leak / fork-bomb)
    _pids_mult = float((active_chaos or {}).get("pids_multiplier") or 1.0)
    metrics.append(build_gauge("container.pids.count", "{pid}",
                               int(rng.randint(1, 50) * _pids_mult)))

    return metrics



from generators.docker_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def _stamp(metrics: list[dict], ts_ns: str) -> None:
    """Give every data point in a scrape the same timestamp.

    The package dashboards take max(counter) across all containers per time bucket and
    differentiate it, so a scrape that straddles a bucket boundary leaves one container's
    much lower counter alone in a bucket and the next rate spikes 13-28x. One timestamp per
    cycle keeps a scrape whole.
    """
    for metric in metrics:
        for points in (metric.get("gauge") or metric.get("sum") or {}).get("dataPoints", []):
            points["timeUnixNano"] = ts_ns

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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("docker-stats")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("docker-host")
    cloud = cfg.get("cloud") or _default_cloud()

    default_container_names = [ScenarioIdentity(ns).label("api"), ScenarioIdentity(ns).label("worker"), ScenarioIdentity(ns).label("cache"), ScenarioIdentity(ns).label("db")]
    container_names: list[str] = cfg.get("container_names") or default_container_names

    # Build per-container state and synthetic IDs
    container_ids = {
        cn: ''.join(rng.choices('abcdef0123456789', k=12)) for cn in container_names
    }
    container_images = {
        cn: f"registry.example.com/{cn}:latest" for cn in container_names
    }
    container_states: dict[str, ContainerState] = {
        cn: ContainerState(rng) for cn in container_names
    }

    from generators.docker_otel import SPEC as _DOCKER_SPEC
    impact_keys = _DOCKER_SPEC.impact_keys

    scope = {"name": DOCKER_SCOPE, "version": SCOPE_VERSION}
    logger.info("Docker stats generator started (namespace=%s, containers=%d)",
                ns, len(container_names))

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="docker_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        cycle_ts = scrape_now_ns()
        for cn, state in container_states.items():
            cid = container_ids[cn]
            img = container_images[cn]

            # Each container gets its own resource with container.* attributes
            resource = build_signal_resource(
                ns=ns, signal_type="metrics",
                dataset=otel_dataset("dockerstatsreceiver"),
                service_name=service_name, service_version="24.0.0",
                service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
                host_name=host_name, cloud=cloud,
                extra_attrs={
                    "container.id": cid,
                    "container.name": cn,
                    "container.image.name": img,
                    "container.hostname": cn,
                    "container.runtime": "docker",
                },
            )
            metrics = _generate_container_metrics(
                cid, cn, img, state, resource, rng, active_chaos,
            )
            _stamp(metrics, cycle_ts)
            client.send_metrics_scoped(resource, scope, metrics)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("Docker stats generator stopped.")


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
