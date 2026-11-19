#!/usr/bin/env python3
"""GCP Compute Engine generator: Cloud Monitoring metrics via OTLP.

Emits what the OpenTelemetry ``googlecloudmonitoring`` receiver produces for
Compute Engine, shaped for the ``gcp_compute_otel`` content package
(``metrics-gcp.compute.otel-default``): the Compute Overview and Compute
Instance Detail dashboards.

Resources (one OTLP resource per monitored resource, like the receiver):
  - ``gce_instance``: ``gcp.resource_type``, ``project_id``, ``instance_id``,
    ``zone``. Datapoints carry the ``instance_name`` metric label; disk series
    add ``device_name``, ``device_type`` and ``storage_type``.
  - ``instance_group``: ``project_id``, ``location``, ``instance_group_name``,
    ``instance_group_id`` for ``compute.googleapis.com/instance_group/size``.

Every Cloud Monitoring value is one 60 s alignment period, so DELTA metrics
(``*_count``, ``usage_time``, ``scheduler_wait_time``) are emitted as gauges
holding the per-interval value: the dashboards read them with
``SUM_OVER_TIME`` / ``AVG_OVER_TIME`` / ``AVG(...) / 60``, never ``RATE``.
GAUGE metrics stay gauges. ``cpu/utilization`` is a 0-1 ratio and disk
``average_io_latency`` is in microseconds, as Cloud Monitoring reports them.

Usage (standalone):
    python3 -m generators.gcp_compute_otel.generator
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/gcp_compute_otel@0.3.0"

import hashlib
import logging
import os
import random
import signal
import threading
import time

from app.config import NAMESPACE
from app.telemetry import OTLPClient
from generator_engine.db_common import (
    CounterState,
    MultiEntityState,
    build_gauge,
    build_signal_resource,
    business_load,
    impact_value,
    otel_dataset,
    resolve_active_chaos,
)
from generator_engine.identity import ScenarioIdentity
from generator_engine.tick import run_tick_loop

# ── Grounding constants: names verified against both package dashboards ──────

_P = "compute.googleapis.com/"

METRIC_NAMES = (
    "compute.googleapis.com/instance/cpu/utilization",
    "compute.googleapis.com/instance/cpu/reserved_cores",
    "compute.googleapis.com/instance/cpu/guest_visible_vcpus",
    "compute.googleapis.com/instance/cpu/usage_time",
    "compute.googleapis.com/instance/cpu/scheduler_wait_time",
    "compute.googleapis.com/instance/memory/balloon/ram_used",
    "compute.googleapis.com/instance/memory/balloon/ram_size",
    "compute.googleapis.com/instance/memory/balloon/swap_in_bytes_count",
    "compute.googleapis.com/instance/memory/balloon/swap_out_bytes_count",
    "compute.googleapis.com/instance/network/received_bytes_count",
    "compute.googleapis.com/instance/network/sent_bytes_count",
    "compute.googleapis.com/instance/network/received_packets_count",
    "compute.googleapis.com/instance/network/sent_packets_count",
    "compute.googleapis.com/firewall/dropped_packets_count",
    "compute.googleapis.com/firewall/dropped_bytes_count",
    "compute.googleapis.com/instance/disk/read_bytes_count",
    "compute.googleapis.com/instance/disk/write_bytes_count",
    "compute.googleapis.com/instance/disk/read_ops_count",
    "compute.googleapis.com/instance/disk/write_ops_count",
    "compute.googleapis.com/instance/disk/average_io_latency",
    "compute.googleapis.com/instance/disk/average_io_queue_depth",
    "compute.googleapis.com/instance/disk/provisioning/size",
    "compute.googleapis.com/instance/disk/performance_status",
    "compute.googleapis.com/instance/uptime_total",
    "compute.googleapis.com/instance/interruption_count",
    "compute.googleapis.com/instance_group/size",
)

GCM_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/"
    "googlecloudmonitoringreceiver"
)
SCOPE_VERSION = "0.115.0"

PERFORMANCE_STATUSES = ("Healthy", "Degraded", "Severely Degraded")

GIB = 1024 ** 3
INTERVAL_S = 60

logger = logging.getLogger("gcp-compute-generator")


def _default_cloud() -> dict:
    return {
        "provider": "gcp",
        "platform": "gcp_compute_engine",
        "region": "us-central1",
        "availability_zone": "us-central1-a",
    }


def _stable_digits(seed: str, k: int) -> str:
    digest = hashlib.sha256(seed.encode()).hexdigest()
    return str(int(digest[:15], 16))[:k]


class VMState(CounterState):
    """Slow-moving per-VM baseline that drifts each interval."""

    def __init__(self, rng: random.Random):
        super().__init__(rng, initial=0)
        self.cpu = rng.uniform(0.20, 0.42)
        self.mem_frac = rng.uniform(0.45, 0.62)
        self.boot_ts = time.time() - rng.uniform(3, 30) * 86400
        self.interrupted = False

    def tick(self, rng: random.Random, drift: int = 0) -> None:
        self.cpu = min(0.55, max(0.12, self.cpu + rng.uniform(-0.03, 0.03)))
        self.mem_frac = min(0.70, max(0.40, self.mem_frac + rng.uniform(-0.01, 0.01)))


def build_fleet(ident: ScenarioIdentity, project_id: str, region: str,
                zones: list[str], count: int) -> tuple[list[dict], dict | None]:
    """Return (vms, instance_group) descriptors for the fleet."""
    count = max(1, int(count))
    group_name = ident.label("web-mig")
    vms: list[dict] = []
    for i in range(1, count + 1):
        spot = count > 1 and i == count
        if spot:
            name = ident.label("batch", 1)
            cores, ram = 4.0, 16 * GIB
            data_disk = ("data-disk-1", "pd-standard", 500 * GIB)
        else:
            name = ident.label("web", i)
            cores, ram = 2.0, 8 * GIB
            data_disk = ("data-disk-1", "pd-ssd", 200 * GIB)
        vms.append({
            "index": i,
            "name": name,
            "instance_id": ident.gcp_instance_id(i),
            "zone": zones[(i - 1) % len(zones)],
            "project_id": project_id,
            "spot": spot,
            "group": None if spot else group_name,
            "cores": cores,
            "ram": ram,
            "disks": [
                ("persistent-disk-0", "pd-balanced", 50 * GIB),
                data_disk,
            ],
        })
    members = [vm for vm in vms if vm["group"]]
    group = None
    if members:
        group = {
            "name": group_name,
            "id": _stable_digits(f"{group_name}:{project_id}", 19),
            "location": region,
            "project_id": project_id,
            "size": len(members),
        }
    return vms, group


def _f(value: float, places: int = 4) -> float:
    return float(round(value, places))


def generate_vm_metrics(
    vm: dict,
    state: VMState,
    rng: random.Random,
    active_chaos: dict | None = None,
    *,
    first_vm: bool = False,
    fleet_size: int = 1,
    now: float | None = None,
) -> list[dict]:
    """Metrics for one ``gce_instance`` resource for one 60 s interval."""
    now = time.time() if now is None else now
    load = business_load()
    base = {"instance_name": vm["name"]}
    out: list[dict] = []

    cpu_high = float(impact_value(active_chaos, "cpu_utilization_high", default=0.0) or 0.0)
    interruptions = float(impact_value(active_chaos, "instance_interruption", default=0.0) or 0.0)
    disk_sev = float(impact_value(active_chaos, "disk_throttled", default=0.0) or 0.0)
    fw_spike = float(impact_value(active_chaos, "firewall_drop_spike", default=0.0) or 0.0)

    # CPU
    if cpu_high > 0:
        cpu = min(1.0, cpu_high / 100.0 * rng.uniform(0.97, 1.0))
    else:
        cpu = min(0.9, state.cpu * (0.7 + 0.5 * load))
    cores = vm["cores"]
    pressure = max(0.0, cpu - 0.75) / 0.25
    sched_wait = cores * INTERVAL_S * (rng.uniform(0.001, 0.004) + 0.08 * pressure)
    out += [
        build_gauge(_P + "instance/cpu/utilization", "10^2.%", _f(cpu), base),
        build_gauge(_P + "instance/cpu/reserved_cores", "1", float(cores), base),
        build_gauge(_P + "instance/cpu/guest_visible_vcpus", "1", float(cores), base),
        build_gauge(_P + "instance/cpu/usage_time", "s{CPU}",
                    _f(cpu * cores * INTERVAL_S, 3), base),
        build_gauge(_P + "instance/cpu/scheduler_wait_time", "s{idle}",
                    _f(sched_wait, 4), base),
    ]

    # Memory (balloon driver)
    ram = int(vm["ram"])
    used = int(ram * min(0.97, state.mem_frac + 0.25 * pressure))
    swap_scale = 1.0 + 40.0 * pressure
    out += [
        build_gauge(_P + "instance/memory/balloon/ram_used", "By", used, base),
        build_gauge(_P + "instance/memory/balloon/ram_size", "By", ram, base),
        build_gauge(_P + "instance/memory/balloon/swap_in_bytes_count", "By",
                    int(rng.randint(0, 64 * 1024) * swap_scale), base),
        build_gauge(_P + "instance/memory/balloon/swap_out_bytes_count", "By",
                    int(rng.randint(0, 128 * 1024) * swap_scale), base),
    ]

    # Network (bytes and packets per interval)
    tx_bps = rng.uniform(1.5e6, 6e6) * cores / 2 * (0.4 + 0.6 * load)
    rx_bps = tx_bps * rng.uniform(0.6, 1.1)
    tx_bytes = int(tx_bps * INTERVAL_S)
    rx_bytes = int(rx_bps * INTERVAL_S)
    out += [
        build_gauge(_P + "instance/network/sent_bytes_count", "By", tx_bytes, base),
        build_gauge(_P + "instance/network/received_bytes_count", "By", rx_bytes, base),
        build_gauge(_P + "instance/network/sent_packets_count", "{packet}",
                    max(1, tx_bytes // 1100), base),
        build_gauge(_P + "instance/network/received_packets_count", "{packet}",
                    max(1, rx_bytes // 900), base),
    ]

    # Firewall drops: internet scan noise when healthy
    if fw_spike > 0:
        dropped = int(fw_spike / max(1, fleet_size) * rng.uniform(0.85, 1.15))
    else:
        dropped = rng.randint(1, 12)
    out += [
        build_gauge(_P + "firewall/dropped_packets_count", "{packet}", dropped, base),
        build_gauge(_P + "firewall/dropped_bytes_count", "By",
                    int(dropped * rng.uniform(54, 90)), base),
    ]

    # Disks
    for d_index, (device, storage_type, size) in enumerate(vm["disks"]):
        attrs = {**base, "device_name": device, "device_type": "permanent",
                 "storage_type": storage_type}
        throttled = first_vm and d_index == 1 and disk_sev > 0
        sev = disk_sev if throttled else 0.0
        boot = d_index == 0
        read_ops = int(rng.randint(20, 300 if boot else 2400) * (0.4 + 0.6 * load))
        write_ops = int(rng.randint(60, 600 if boot else 3600) * (0.4 + 0.6 * load))
        if throttled:
            read_ops = int(read_ops * (1.0 - 0.6 * sev))
            write_ops = int(write_ops * (1.0 - 0.6 * sev))
        base_lat = rng.uniform(400, 1400) if storage_type == "pd-ssd" else rng.uniform(900, 2600)
        latency = base_lat * (1.0 + 40.0 * sev)
        queue = rng.uniform(0.2, 2.5) + 60.0 * sev
        if sev >= 0.75:
            status = "Severely Degraded"
        elif sev > 0:
            status = "Degraded"
        else:
            status = "Healthy"
        out += [
            build_gauge(_P + "instance/disk/read_ops_count", "1", max(1, read_ops), attrs),
            build_gauge(_P + "instance/disk/write_ops_count", "1", max(1, write_ops), attrs),
            build_gauge(_P + "instance/disk/read_bytes_count", "By",
                        max(4096, read_ops * rng.randint(16, 64) * 1024), attrs),
            build_gauge(_P + "instance/disk/write_bytes_count", "By",
                        max(4096, write_ops * rng.randint(8, 48) * 1024), attrs),
            build_gauge(_P + "instance/disk/average_io_latency", "us", _f(latency, 1), attrs),
            build_gauge(_P + "instance/disk/average_io_queue_depth", "1", _f(queue, 3), attrs),
            build_gauge(_P + "instance/disk/provisioning/size", "By", int(size), attrs),
        ]
        for candidate in PERFORMANCE_STATUSES:
            out.append(build_gauge(
                _P + "instance/disk/performance_status", "1",
                1 if candidate == status else 0,
                {**attrs, "performance_status": candidate},
            ))

    # Availability
    if vm["spot"] and interruptions > 0:
        if not state.interrupted:
            state.boot_ts = now - rng.uniform(5, 45)
            state.interrupted = True
        out.append(build_gauge(
            _P + "instance/interruption_count", "1",
            max(1, int(round(interruptions))),
            {**base, "interruption_type": "Preemption",
             "interruption_reason": "Spot capacity reclaimed"},
        ))
    else:
        state.interrupted = False
        out.append(build_gauge(
            _P + "instance/interruption_count", "1", 0,
            {**base, "interruption_type": "none", "interruption_reason": "none"},
        ))
    out.append(build_gauge(_P + "instance/uptime_total", "s",
                           max(1, int(now - state.boot_ts)), base))
    return out


def generate_group_metrics(group: dict) -> list[dict]:
    return [build_gauge(_P + "instance_group/size", "1", int(group["size"]),
                        {"instance_group_name": group["name"]})]


from generators.gcp_compute_otel import SPEC as _SPEC  # noqa: E402


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Emit Compute Engine metrics until ``stop_event`` is set."""
    rng = random.Random()
    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {}) or {}
    else:
        ns = NAMESPACE
        cfg = {}
    channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    ident = ScenarioIdentity.from_scenario(scenario_data, ns)
    service_name = ident.service("gcp-compute")
    cloud = {**_default_cloud(), **(cfg.get("cloud") or {})}
    region = cloud.get("region", "us-central1")
    project_id = ident.gcp_project(cfg.get("project_id"))
    zones = cfg.get("zones") or [f"{region}-{z}" for z in ("a", "b", "c")]
    vms, group = build_fleet(ident, project_id, region, zones,
                             cfg.get("instance_count") or 3)
    states = MultiEntityState([vm["name"] for vm in vms], VMState, rng)
    dataset = otel_dataset("gcp.compute")
    scope = {"name": GCM_SCOPE, "version": SCOPE_VERSION}

    vm_resources = {}
    for vm in vms:
        vm_resources[vm["name"]] = build_signal_resource(
            identity=ident,
            signal_type="metrics",
            dataset=dataset,
            service_name=service_name,
            service_instance_id=ident.instance(service_name, vm["index"]),
            host_name=vm["name"],
            cloud={**cloud, "availability_zone": vm["zone"]},
            extra_attrs={
                "gcp.resource_type": "gce_instance",
                "project_id": vm["project_id"],
                "instance_id": vm["instance_id"],
                "zone": vm["zone"],
                "cloud.account.id": vm["project_id"],
                "host.id": vm["instance_id"],
            },
        )
    group_resource = None
    if group:
        group_resource = build_signal_resource(
            identity=ident,
            signal_type="metrics",
            dataset=dataset,
            service_name=service_name,
            host_name=group["name"],
            cloud=cloud,
            extra_attrs={
                "gcp.resource_type": "instance_group",
                "project_id": group["project_id"],
                "location": group["location"],
                "instance_group_name": group["name"],
                "instance_group_id": group["id"],
                "cloud.account.id": group["project_id"],
            },
        )

    logger.info("GCP Compute generator started (namespace=%s, vms=%d)", ns, len(vms))

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=channel_registry,
            generator_id="gcp_compute_otel",
            chaos_mode=chaos_mode,
            impact_keys=_SPEC.impact_keys,
        )
        states.tick_all(rng)
        now = time.time()
        for n, vm in enumerate(vms):
            metrics = generate_vm_metrics(
                vm, states[vm["name"]], rng, active_chaos,
                first_vm=(n == 0), fleet_size=len(vms), now=now,
            )
            client.send_metrics_scoped(vm_resources[vm["name"]], scope, metrics)
        if group and group_resource:
            client.send_metrics_scoped(group_resource, scope, generate_group_metrics(group))

    run_tick_loop(stop_event, _emit_tick, interval=INTERVAL_S)
    logger.info("GCP Compute generator stopped.")


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "120"))
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    client = OTLPClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
