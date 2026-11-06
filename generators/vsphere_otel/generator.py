#!/usr/bin/env python3
"""vSphere (vCenter) Metrics Generator — sends vCenter OTel receiver metrics via OTLP.

Emits data matching the vsphere_otel content package
(discovery.datasets: vcenterreceiver.otel), which populates the bundled
vSphere dashboards (overview, hosts, vms, storage).

Also bulk-indexes co-located documents each tick (OTLP is one-metric-per-doc):
  - Cluster CPU/memory effective+limit (headroom panels; bulk-only)
  - Per-host CPU/memory utilization+usage+capacity (host SUM panels; bulk-only)
  - Per-VM memory usage/ballooned/swapped + disk utilization (table panels)
  - Per-VM disk latency.max + disk.utilization with object=scsi0:0

Metric names, types, units, and attributes verified against:
  - open-telemetry-collector-contrib/receiver/vcenterreceiver/metadata.yaml
  - elastic/integrations/packages/vsphere_otel/kibana/dashboard/*.json

Dashboards filter ``attributes.object == "*"`` and group by ``direction``,
``disk_state``, and ``power_state``, so those attributes must be present.

Usage (standalone):
    python3 -m generators.vsphere_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/vsphere_otel@0.3.0"

import logging
import functools
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
    build_gauge,
    build_non_monotonic_sum,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
    stable_counter_start_ns,
)

METRIC_NAMES = (
    # Datacenter
    "vcenter.datacenter.cluster.count",
    "vcenter.datacenter.datastore.count",
    "vcenter.datacenter.disk.space",
    "vcenter.datacenter.host.count",
    "vcenter.datacenter.vm.count",
    # Cluster
    "vcenter.cluster.cpu.effective",
    "vcenter.cluster.cpu.limit",
    "vcenter.cluster.host.count",
    "vcenter.cluster.memory.effective",
    "vcenter.cluster.memory.limit",
    "vcenter.cluster.vm.count",
    # Datastore
    "vcenter.datastore.disk.usage",
    "vcenter.datastore.disk.utilization",
    # Host
    "vcenter.host.cpu.capacity",
    "vcenter.host.cpu.usage",
    "vcenter.host.cpu.utilization",
    "vcenter.host.disk.latency.max",
    "vcenter.host.disk.throughput",
    "vcenter.host.memory.usage",
    "vcenter.host.memory.utilization",
    "vcenter.host.network.packet.rate",
    "vcenter.host.network.throughput",
    # VM
    "vcenter.vm.cpu.readiness",
    "vcenter.vm.cpu.usage",
    "vcenter.vm.cpu.utilization",
    "vcenter.vm.disk.latency.max",
    "vcenter.vm.disk.throughput",
    "vcenter.vm.disk.utilization",
    "vcenter.vm.memory.ballooned",
    "vcenter.vm.memory.swapped",
    "vcenter.vm.memory.usage",
    "vcenter.vm.memory.utilization",
    "vcenter.vm.network.packet.rate",
    "vcenter.vm.network.throughput",
    "vcenter.vm.network.usage",
)

# Bulk-only: panels SUM these; dual-emit via OTLP would double counts.
_CLUSTER_HEADROOM_METRICS = (
    "vcenter.cluster.cpu.effective",
    "vcenter.cluster.cpu.limit",
    "vcenter.cluster.memory.effective",
    "vcenter.cluster.memory.limit",
)
_HOST_UTILIZATION_METRICS = (
    "vcenter.host.cpu.utilization",
    "vcenter.host.cpu.usage",
    "vcenter.host.cpu.capacity",
    "vcenter.host.memory.utilization",
    "vcenter.host.memory.usage",
)

VSPHERE_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/vcenterreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("vsphere-generator")
BATCH_INTERVAL_MIN = 20
BATCH_INTERVAL_MAX = 60

# Aggregate object name used by dashboard filters: attributes.object == "*"
_AGG_OBJECT = "*"
_DISK_OBJECT = "naa.6000*"

_METRICS_DATA_STREAM = "metrics-vcenterreceiver.otel-default"

# Co-located gauges must map as gauge_long / gauge_double. Plain ESBulk create
# (no dynamic_templates) maps ints as long without time_series_metric, and
# sibling-filter / SUM panels return nulls or "No results found".
_GAUGE_LONG_FIELDS = (
    *_CLUSTER_HEADROOM_METRICS,
    "vcenter.host.cpu.usage",
    "vcenter.host.cpu.capacity",
    "vcenter.host.memory.usage",
    "vcenter.vm.memory.usage",
    "vcenter.vm.memory.ballooned",
    "vcenter.vm.memory.swapped",
    "vcenter.vm.disk.latency.max",
)
_GAUGE_DOUBLE_FIELDS = (
    "vcenter.host.cpu.utilization",
    "vcenter.host.memory.utilization",
    "vcenter.vm.disk.utilization",
)
_SIBLING_GAUGE_FIELDS = _GAUGE_LONG_FIELDS + _GAUGE_DOUBLE_FIELDS
_GAUGE_DYNAMIC_TEMPLATES = {
    **{f"metrics.{name}": "gauge_long" for name in _GAUGE_LONG_FIELDS},
    **{f"metrics.{name}": "gauge_double" for name in _GAUGE_DOUBLE_FIELDS},
}


def _default_cloud() -> dict:
    return {"provider": "gcp", "platform": "gcp_compute_engine",
            "region": "us-central1", "availability_zone": "us-central1-b"}


def _pct(fraction: float) -> float:
    """Clamp a 0–1 fraction and convert to a 0–100 percent gauge value."""
    return round(min(100.0, max(0.0, fraction * 100.0)), 2)


def entity_resource(base_resource: dict, attrs: dict[str, str]) -> dict:
    """Copy ``base_resource`` with vCenter entity names added as RESOURCE attributes.

    The vcenter receiver emits each host / VM as its own resource carrying
    ``vcenter.host.name`` / ``vcenter.vm.name`` (and parents) as resource
    attributes; package SLOs group by ``resource.attributes.vcenter.*.name``.
    Data points keep the same names as attributes so bare-name ES|QL alerts
    and dashboards resolve unchanged.
    """
    replaced = set(attrs)
    kept = [a for a in base_resource.get("attributes", []) if a["key"] not in replaced]
    added = [{"key": k, "value": {"stringValue": str(v)}} for k, v in attrs.items()]
    return {**base_resource, "attributes": kept + added}


def vm_cpu_readiness_pct(rng: random.Random, host_cpu_key: float) -> int:
    """VM CPU readiness (%): healthy 0-3 (SLO < 5); rises with host CPU contention."""
    if host_cpu_key > 0:
        return rng.randint(8, 15)
    return rng.randint(0, 3)


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


def _base_bulk_doc(
    resource: dict,
    metrics: dict[str, Any],
    attributes: dict[str, Any],
    *,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "1",
        "data_stream": {
            "type": "metrics",
            "dataset": "vcenterreceiver.otel",
            "namespace": "default",
        },
        "attributes": attributes,
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": VSPHERE_SCOPE, "version": SCOPE_VERSION},
        "metrics": metrics,
    }


def build_cluster_headroom_doc(
    resource: dict,
    *,
    cluster_name: str,
    datacenter_name: str,
    cpu_effective: int,
    cpu_limit: int,
    memory_effective: int,
    memory_limit: int,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc with cluster CPU+memory effective/limit co-located.

    Headroom panels AND-filter effective+limit siblings. OTLP stores one
    metric per document, so those filters never match OTLP-only data.
    Bulk-only (not dual-emitted) because overview panels SUM these fields.
    """
    return _base_bulk_doc(
        resource,
        {
            "vcenter.cluster.cpu.effective": int(cpu_effective),
            "vcenter.cluster.cpu.limit": int(cpu_limit),
            "vcenter.cluster.memory.effective": int(memory_effective),
            "vcenter.cluster.memory.limit": int(memory_limit),
        },
        {
            "vcenter.cluster.name": cluster_name,
            "vcenter.datacenter.name": datacenter_name,
        },
        timestamp_ms=timestamp_ms,
    )


def build_host_utilization_doc(
    resource: dict,
    *,
    host_name: str,
    cluster_name: str,
    datacenter_name: str,
    cpu_utilization: float,
    cpu_usage: int,
    cpu_capacity: int,
    memory_utilization: float,
    memory_usage: int,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc with host CPU/memory utilization siblings co-located.

    Host dashboard SUM panels AND-filter usage+capacity / utilization pairs.
    Bulk-only for these five fields (disk/network stay on OTLP).
    """
    return _base_bulk_doc(
        resource,
        {
            "vcenter.host.cpu.utilization": float(cpu_utilization),
            "vcenter.host.cpu.usage": int(cpu_usage),
            "vcenter.host.cpu.capacity": int(cpu_capacity),
            "vcenter.host.memory.utilization": float(memory_utilization),
            "vcenter.host.memory.usage": int(memory_usage),
        },
        {
            "vcenter.host.name": host_name,
            "vcenter.cluster.name": cluster_name,
            "vcenter.datacenter.name": datacenter_name,
        },
        timestamp_ms=timestamp_ms,
    )


def build_vm_memory_table_doc(
    resource: dict,
    *,
    vm_name: str,
    host_name: str,
    cluster_name: str,
    datacenter_name: str,
    memory_usage: int,
    ballooned: int,
    swapped: int,
    disk_utilization: float,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc for VM memory table panels (usage+ballooned+swapped+disk %).

    Healthy ballooned/swapped stay 0; zeros must still share the doc with usage.
    No ``object`` attribute (table rows are per-VM, not per-disk).
    """
    return _base_bulk_doc(
        resource,
        {
            "vcenter.vm.memory.usage": int(memory_usage),
            "vcenter.vm.memory.ballooned": int(ballooned),
            "vcenter.vm.memory.swapped": int(swapped),
            "vcenter.vm.disk.utilization": float(disk_utilization),
        },
        {
            "vcenter.vm.name": vm_name,
            "vcenter.host.name": host_name,
            "vcenter.cluster.name": cluster_name,
            "vcenter.datacenter.name": datacenter_name,
        },
        timestamp_ms=timestamp_ms,
    )


def build_vm_disk_latency_doc(
    resource: dict,
    *,
    vm_name: str,
    host_name: str,
    cluster_name: str,
    datacenter_name: str,
    disk_latency_max: int,
    disk_utilization: float,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build companion doc co-locating disk latency.max with disk.utilization.

    Latency panels that also read utilization need both on the same document
    with ``object: scsi0:0``.
    """
    return _base_bulk_doc(
        resource,
        {
            "vcenter.vm.disk.latency.max": int(disk_latency_max),
            "vcenter.vm.disk.utilization": float(disk_utilization),
        },
        {
            "vcenter.vm.name": vm_name,
            "vcenter.host.name": host_name,
            "vcenter.cluster.name": cluster_name,
            "vcenter.datacenter.name": datacenter_name,
            "object": "scsi0:0",
        },
        timestamp_ms=timestamp_ms,
    )


def _sibling_gauges_need_mapping_reset(elastic_url: str, api_key: str) -> bool:
    """True when any co-located sibling lacks time_series_metric=gauge.

    Plain ``long`` / ``double`` from older bulk docs poisons sibling-filter
    and SUM panels even when OTLP gauges are healthy.
    """
    import httpx

    field_list = ",".join(f"metrics.{name}" for name in _SIBLING_GAUGE_FIELDS)
    try:
        resp = httpx.get(
            f"{elastic_url.rstrip('/')}/{_METRICS_DATA_STREAM}/_field_caps",
            params={"fields": field_list},
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

    fields = resp.json().get("fields") or {}
    for name in _SIBLING_GAUGE_FIELDS:
        caps = fields.get(f"metrics.{name}") or fields.get(name) or {}
        for type_meta in caps.values():
            if not isinstance(type_meta, dict):
                continue
            mtype = type_meta.get("type")
            tsm = type_meta.get("time_series_metric")
            if tsm == "counter" or mtype in ("counter_long", "counter_double"):
                return True
            if tsm != "gauge":
                return True
    return False


def _reset_poisoned_sibling_gauge_mapping() -> None:
    """Delete the metrics data stream when bulk docs poisoned gauge mappings."""
    try:
        from app.telemetry import ESBulkClient as _ESBulk
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for vsphere mapping check: %s", exc)
        return

    client = _ESBulk()
    if not client.configured:
        return
    if not _sibling_gauges_need_mapping_reset(client.elastic_url, client.api_key):
        logger.info(
            "vcenter.* sibling gauges already have time_series_metric=gauge; leaving %s alone",
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
            "Deleted %s so vcenter.* sibling gauges remap with time_series_metric",
            _METRICS_DATA_STREAM,
        )
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist", _METRICS_DATA_STREAM)
    else:
        logger.warning(
            "Could not delete %s: %s %s",
            _METRICS_DATA_STREAM,
            resp.status_code,
            resp.text[:200],
        )


class VMState(CounterState):
    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.disk_read = rng.randint(1_000, 50_000)
        self.disk_write = rng.randint(1_000, 80_000)
        self.net_rx = rng.randint(100, 20_000)
        self.net_tx = rng.randint(100, 15_000)
        self.net_pps_rx = rng.randint(10, 5_000)
        self.net_pps_tx = rng.randint(10, 4_000)

    def tick(self, rng: random.Random, load: float = 1.0) -> None:
        self.disk_read = int(rng.randint(500, 40_000) * load)
        self.disk_write = int(rng.randint(1_000, 60_000) * load)
        self.net_rx = int(rng.randint(100, 15_000) * load)
        self.net_tx = int(rng.randint(50, 10_000) * load)
        self.net_pps_rx = int(rng.randint(10, 4_000) * load)
        self.net_pps_tx = int(rng.randint(10, 3_000) * load)


def _dir_metrics(
    build_fn,
    name: str,
    unit: str,
    read_or_rx: int | float,
    write_or_tx: int | float,
    base_attrs: dict,
    *,
    read_dir: str,
    write_dir: str,
    as_gauge: bool = False,
) -> list[dict]:
    """Emit a metric for both directions with object='*' (dashboard aggregate)."""
    out: list[dict] = []
    for direction, value in ((read_dir, read_or_rx), (write_dir, write_or_tx)):
        attrs = {**base_attrs, "direction": direction, "object": _AGG_OBJECT}
        if as_gauge:
            out.append(build_gauge(name, unit, value, attrs))
        else:
            out.append(build_fn(name, unit, value, attributes=attrs))
    return out



from generators.vsphere_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("vsphere")
    host_name = cfg.get("host_name") or ScenarioIdentity(ns).domain("vcenter")
    cloud = cfg.get("cloud") or _default_cloud()
    dc_name = cfg.get("datacenter_name") or ScenarioIdentity(ns).label("dc")
    cluster_name = cfg.get("cluster_name") or ScenarioIdentity(ns).label("cluster")

    default_vm_names = [ScenarioIdentity(ns).label("vm", i) for i in range(1, 5)]
    vm_names: list[str] = cfg.get("vm_names") or default_vm_names
    esxi_host_names = cfg.get("host_names") or [ScenarioIdentity(ns).label("esxi", i) for i in range(1, 3)]
    datastore_names = cfg.get("datastore_names") or [ScenarioIdentity(ns).label("ds", i) for i in range(1, 3)]

    vm_states: dict[str, VMState] = {vm: VMState(rng) for vm in vm_names}
    # Pin each VM to a host so BY vcenter.host.name panels resolve.
    vm_host: dict[str, str] = {
        vm: esxi_host_names[i % len(esxi_host_names)] for i, vm in enumerate(vm_names)
    }


    base_resource = build_signal_resource(
        ns=ns, signal_type="metrics", dataset=otel_dataset("vcenterreceiver"),
        service_name=service_name, service_version="8.0.0",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name, cloud=cloud,
        extra_attrs={"vcenter.datacenter.name": dc_name},
    )
    scope = {"name": VSPHERE_SCOPE, "version": SCOPE_VERSION}
    # Secondary transport for co-located multi-metric panel docs.
    es_bulk = ESBulkClient()
    _reset_poisoned_sibling_gauge_mapping()
    # Mapping reset may have deleted the stream; clear any prior bulk backoff.
    es_bulk.consecutive_failures = 0
    logger.info(
        "vSphere metrics generator started (namespace=%s, hosts=%d, vms=%d, datastores=%d)",
        ns, len(esxi_host_names), len(vm_names), len(datastore_names),
    )

    def _emit_tick() -> None:
        # One stable start for every sum keeps each series continuous (GUARDRAILS #23).
        sum_start = stable_counter_start_ns()
        dir_sum = functools.partial(build_non_monotonic_sum, start_time_ns=sum_start)
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="vsphere_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        c = active_chaos or {}
        ds_util_pct = float(c.get("datastore_utilization_high", 0.0))
        entity_unhealthy = float(c.get("entity_status_unhealthy", 0.0))
        host_cpu_key = float(c.get("host_cpu_utilization_high", 0.0))
        host_mem_key = float(c.get("host_memory_utilization_high", 0.0))
        host_disk_lat_key = float(c.get("host_disk_latency_high", 0.0))
        host_power_off = int(c.get("host_power_state_unhealthy", 0.0))
        vm_disk_lat_key = float(c.get("vm_disk_latency_high", 0.0))
        vm_balloon = float(c.get("vm_memory_ballooning", 0.0))
        vm_swap = float(c.get("vm_memory_swapping", 0.0))
        load = business_load()

        for state in vm_states.values():
            state.tick(rng, load)

        powered_off: set[str] = set()
        vms_on = len(vm_names) - len(powered_off)
        vms_off = len(powered_off)
        hosts_off = min(len(esxi_host_names), max(0, host_power_off))
        hosts_on = len(esxi_host_names) - hosts_off
        status_ok = "green" if entity_unhealthy <= 0 else "red"
        status_bad = "yellow" if entity_unhealthy < 0.7 else "red"
        ts_ms = int(time.time() * 1000)

        metrics: list[dict] = []
        # Per-host / per-VM metrics ride on their own entity resource (as the
        # vcenter receiver does) so resource.attributes.vcenter.*.name is set.
        entity_batches: list[tuple[dict, list[dict]]] = []
        bulk_docs: list[dict] = []
        dc_attrs = {"vcenter.datacenter.name": dc_name}

        # ── Datacenter ────────────────────────────────────────────────────
        # Counts use recommended attributes; emit one primary series so
        # overview MAX/SUM panels resolve to the inventory totals.
        metrics.extend([
            build_non_monotonic_sum(
                "vcenter.datacenter.cluster.count", "{clusters}", 1, start_time_ns=sum_start,
                attributes={**dc_attrs, "status": status_ok},
            ),
            build_non_monotonic_sum(
                "vcenter.datacenter.datastore.count", "{datastores}",
                len(datastore_names), start_time_ns=sum_start, attributes=dc_attrs,
            ),
            build_non_monotonic_sum(
                "vcenter.datacenter.host.count", "{hosts}",
                hosts_on, start_time_ns=sum_start,
                attributes={**dc_attrs, "status": status_ok, "power_state": "on"},
            ),
            build_non_monotonic_sum(
                "vcenter.datacenter.host.count", "{hosts}",
                hosts_off, start_time_ns=sum_start,
                attributes={
                    **dc_attrs,
                    "status": status_bad if hosts_off else status_ok,
                    "power_state": "off",
                },
            ),
            build_non_monotonic_sum(
                "vcenter.datacenter.vm.count", "{virtual_machines}", vms_on, start_time_ns=sum_start,
                attributes={**dc_attrs, "status": status_ok, "power_state": "on"},
            ),
            build_non_monotonic_sum(
                "vcenter.datacenter.vm.count", "{virtual_machines}", vms_off, start_time_ns=sum_start,
                attributes={
                    **dc_attrs,
                    "status": status_bad if (vms_off or entity_unhealthy > 0) else status_ok,
                    "power_state": "off",
                },
            ),
        ])
        if entity_unhealthy > 0:
            metrics.append(build_non_monotonic_sum(
                "vcenter.datacenter.cluster.count", "{clusters}",
                max(1, int(entity_unhealthy * 3)), start_time_ns=sum_start,
                attributes={**dc_attrs, "status": status_bad},
            ))

        # Datacenter disk space by disk_state (used | available)
        dc_total_bytes = len(datastore_names) * 2_000_000_000_000  # ~2 TiB each
        dc_used_frac = rng.uniform(0.35, 0.65)
        dc_used = int(dc_total_bytes * dc_used_frac)
        dc_avail = dc_total_bytes - dc_used
        metrics.append(build_non_monotonic_sum(
            "vcenter.datacenter.disk.space", "By", dc_used, start_time_ns=sum_start,
            attributes={**dc_attrs, "disk_state": "used"},
        ))
        metrics.append(build_non_monotonic_sum(
            "vcenter.datacenter.disk.space", "By", dc_avail, start_time_ns=sum_start,
            attributes={**dc_attrs, "disk_state": "available"},
        ))

        # ── Cluster ───────────────────────────────────────────────────────
        cl_attrs = {"vcenter.cluster.name": cluster_name, "vcenter.datacenter.name": dc_name}
        host_cpu_mhz = 40_000
        host_mem_bytes = 128 * 1024 * 1024 * 1024  # 128 GiB
        cluster_cpu_mhz = len(esxi_host_names) * host_cpu_mhz
        cluster_mem_bytes = len(esxi_host_names) * host_mem_bytes
        cluster_cpu_effective = int(cluster_cpu_mhz * 0.85)
        cluster_mem_effective = int(cluster_mem_bytes * 0.85)
        metrics.extend([
            build_non_monotonic_sum(
                "vcenter.cluster.host.count", "{hosts}", len(esxi_host_names), start_time_ns=sum_start,
                attributes={**cl_attrs, "effective": True},
            ),
            build_non_monotonic_sum(
                "vcenter.cluster.vm.count", "{virtual_machines}", vms_on, start_time_ns=sum_start,
                attributes={**cl_attrs, "power_state": "on"},
            ),
            build_non_monotonic_sum(
                "vcenter.cluster.vm.count", "{virtual_machines}", vms_off, start_time_ns=sum_start,
                attributes={**cl_attrs, "power_state": "off"},
            ),
            # CPU/memory effective+limit: bulk-only (SUM panels; no dual-emit).
        ])
        bulk_docs.append(build_cluster_headroom_doc(
            base_resource,
            cluster_name=cluster_name,
            datacenter_name=dc_name,
            cpu_effective=cluster_cpu_effective,
            cpu_limit=cluster_cpu_mhz,
            memory_effective=cluster_mem_effective,
            memory_limit=cluster_mem_bytes,
            timestamp_ms=ts_ms,
        ))

        # ── Datastores ────────────────────────────────────────────────────
        for ds in datastore_names:
            ds_attrs = {
                "vcenter.datastore.name": ds,
                "vcenter.datacenter.name": dc_name,
            }
            ds_total = 2_000_000_000_000
            if ds_util_pct > 0:
                ds_util_frac = min(0.99, ds_util_pct / 100.0)
            else:
                # Healthy stays under the 80% datastore alert.
                ds_util_frac = rng.uniform(0.3, 0.70)
            ds_used = int(ds_total * ds_util_frac)
            ds_avail = ds_total - ds_used
            metrics.append(build_non_monotonic_sum(
                "vcenter.datastore.disk.usage", "By", ds_used, start_time_ns=sum_start,
                attributes={**ds_attrs, "disk_state": "used"},
            ))
            metrics.append(build_non_monotonic_sum(
                "vcenter.datastore.disk.usage", "By", ds_avail, start_time_ns=sum_start,
                attributes={**ds_attrs, "disk_state": "available"},
            ))
            metrics.append(build_gauge(
                "vcenter.datastore.disk.utilization", "%",
                _pct(ds_util_frac), ds_attrs,
            ))

        # ── Hosts (ESXi) ──────────────────────────────────────────────────
        for esxi in esxi_host_names:
            h_attrs = {
                "vcenter.host.name": esxi,
                "vcenter.cluster.name": cluster_name,
                "vcenter.datacenter.name": dc_name,
            }
            host_metrics: list[dict] = []
            entity_batches.append((entity_resource(base_resource, h_attrs), host_metrics))
            host_cpu_util = min(1.0, rng.uniform(0.2, 0.55) * load)
            if host_cpu_key > 0:
                host_cpu_util = min(1.0, max(host_cpu_util, host_cpu_key / 100.0))
            host_mem_util = min(1.0, rng.uniform(0.3, 0.70))
            if host_mem_key > 0:
                host_mem_util = min(1.0, max(host_mem_util, host_mem_key / 100.0))
            host_cpu_usage = int(host_cpu_mhz * host_cpu_util)
            host_mem_mib = int(128 * 1024 * host_mem_util)
            host_disk_lat = int(rng.uniform(1.0, 10.0))
            if host_disk_lat_key > 0:
                host_disk_lat = max(host_disk_lat, int(host_disk_lat_key))

            # CPU/memory utilization siblings: bulk-only (host SUM panels).
            bulk_docs.append(build_host_utilization_doc(
                base_resource,
                host_name=esxi,
                cluster_name=cluster_name,
                datacenter_name=dc_name,
                cpu_utilization=_pct(host_cpu_util),
                cpu_usage=host_cpu_usage,
                cpu_capacity=host_cpu_mhz,
                memory_utilization=_pct(host_mem_util),
                memory_usage=host_mem_mib,
                timestamp_ms=ts_ms,
            ))

            host_metrics.append(build_gauge(
                "vcenter.host.disk.latency.max", "ms",
                host_disk_lat,
                {**h_attrs, "object": _DISK_OBJECT},
            ))

            disk_read = int(rng.randint(10_000, 200_000) * load)
            disk_write = int(rng.randint(10_000, 300_000) * load)
            host_metrics.extend(_dir_metrics(
                dir_sum, "vcenter.host.disk.throughput", "{KiBy/s}",
                disk_read, disk_write, h_attrs,
                read_dir="read", write_dir="write",
            ))

            net_rx = int(rng.randint(1_000, 80_000) * load)
            net_tx = int(rng.randint(1_000, 60_000) * load)
            host_metrics.extend(_dir_metrics(
                dir_sum, "vcenter.host.network.throughput", "{KiBy/s}",
                net_rx, net_tx, h_attrs,
                read_dir="received", write_dir="transmitted",
            ))

            pps_rx = float(rng.randint(100, 20_000) * load)
            pps_tx = float(rng.randint(100, 15_000) * load)
            host_metrics.extend(_dir_metrics(
                build_gauge, "vcenter.host.network.packet.rate", "{packets/s}",
                pps_rx, pps_tx, h_attrs,
                read_dir="received", write_dir="transmitted",
                as_gauge=True,
            ))

        # ── VMs ───────────────────────────────────────────────────────────
        for vmn, vms in vm_states.items():
            if vmn in powered_off:
                continue
            vm_attrs = {
                "vcenter.vm.name": vmn,
                "vcenter.host.name": vm_host[vmn],
                "vcenter.cluster.name": cluster_name,
                "vcenter.datacenter.name": dc_name,
            }
            vm_metrics: list[dict] = []
            entity_batches.append((entity_resource(base_resource, vm_attrs), vm_metrics))
            vm_cpu_util = min(1.0, rng.uniform(0.05, 0.6) * load)
            if host_cpu_key > 0:
                vm_cpu_util = min(1.0, max(vm_cpu_util, host_cpu_key / 100.0))
            vm_mem_util = min(1.0, rng.uniform(0.2, 0.7))
            vm_mem_mib = 16 * 1024
            mem_usage = int(vm_mem_mib * vm_mem_util)
            balloon = int(vm_balloon) if vm_balloon > 0 else 0
            swapped = int(vm_swap) if vm_swap > 0 else 0
            disk_util = min(1.0, rng.uniform(0.2, 0.8))
            disk_util_pct = _pct(disk_util)
            vm_disk_lat = int(rng.uniform(1.0, 12.0))
            if vm_disk_lat_key > 0:
                vm_disk_lat = max(vm_disk_lat, int(vm_disk_lat_key))

            vm_metrics.extend([
                build_non_monotonic_sum(
                    "vcenter.vm.cpu.usage", "MHz",
                    int(4_000 * vm_cpu_util), start_time_ns=sum_start, attributes=vm_attrs,
                ),
                build_gauge(
                    "vcenter.vm.cpu.utilization", "%", _pct(vm_cpu_util), vm_attrs,
                ),
                build_gauge(
                    "vcenter.vm.cpu.readiness", "%",
                    vm_cpu_readiness_pct(rng, host_cpu_key), vm_attrs,
                ),
                # Dual-emit memory/disk for standalone MAX charts; bulk co-locates
                # siblings for table / AND-filter panels.
                build_non_monotonic_sum(
                    "vcenter.vm.memory.usage", "MiBy",
                    mem_usage, start_time_ns=sum_start, attributes=vm_attrs,
                ),
                build_gauge(
                    "vcenter.vm.memory.utilization", "%", _pct(vm_mem_util), vm_attrs,
                ),
                build_non_monotonic_sum(
                    "vcenter.vm.memory.ballooned", "MiBy", balloon, start_time_ns=sum_start, attributes=vm_attrs,
                ),
                build_non_monotonic_sum(
                    "vcenter.vm.memory.swapped", "MiBy", swapped, start_time_ns=sum_start, attributes=vm_attrs,
                ),
                build_gauge(
                    "vcenter.vm.disk.utilization", "%", disk_util_pct, vm_attrs,
                ),
                build_gauge(
                    "vcenter.vm.disk.latency.max", "ms",
                    vm_disk_lat,
                    {**vm_attrs, "object": "scsi0:0"},
                ),
                build_non_monotonic_sum(
                    "vcenter.vm.network.usage", "{KiBy/s}",
                    vms.net_rx + vms.net_tx, start_time_ns=sum_start,
                    attributes={**vm_attrs, "object": _AGG_OBJECT},
                ),
            ])

            bulk_docs.append(build_vm_memory_table_doc(
                base_resource,
                vm_name=vmn,
                host_name=vm_host[vmn],
                cluster_name=cluster_name,
                datacenter_name=dc_name,
                memory_usage=mem_usage,
                ballooned=balloon,
                swapped=swapped,
                disk_utilization=disk_util_pct,
                timestamp_ms=ts_ms,
            ))
            bulk_docs.append(build_vm_disk_latency_doc(
                base_resource,
                vm_name=vmn,
                host_name=vm_host[vmn],
                cluster_name=cluster_name,
                datacenter_name=dc_name,
                disk_latency_max=vm_disk_lat,
                disk_utilization=disk_util_pct,
                timestamp_ms=ts_ms,
            ))

            # VM disk throughput is a gauge in metadata.yaml
            vm_metrics.extend(_dir_metrics(
                build_gauge, "vcenter.vm.disk.throughput", "{KiBy/s}",
                vms.disk_read, vms.disk_write, vm_attrs,
                read_dir="read", write_dir="write",
                as_gauge=True,
            ))
            # VM network throughput is a non-monotonic sum, unit By/s
            vm_metrics.extend(_dir_metrics(
                dir_sum, "vcenter.vm.network.throughput", "By/s",
                vms.net_rx * 1024, vms.net_tx * 1024, vm_attrs,
                read_dir="received", write_dir="transmitted",
            ))
            vm_metrics.extend(_dir_metrics(
                build_gauge, "vcenter.vm.network.packet.rate", "{packets/s}",
                float(vms.net_pps_rx), float(vms.net_pps_tx), vm_attrs,
                read_dir="received", write_dir="transmitted",
                as_gauge=True,
            ))

        client.send_metrics_scoped(base_resource, scope, metrics)
        for entity_res, entity_metrics in entity_batches:
            client.send_metrics_scoped(entity_res, scope, entity_metrics)

        if es_bulk.configured and bulk_docs:
            if es_bulk.consecutive_failures >= es_bulk.max_failures_before_backoff:
                logger.warning(
                    "Clearing ESBulk backoff so vsphere co-location can retry"
                )
                es_bulk.consecutive_failures = 0
            es_bulk.send_bulk(
                _METRICS_DATA_STREAM,
                bulk_docs,
                dynamic_templates=_GAUGE_DYNAMIC_TEMPLATES,
            )

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    es_bulk.close()
    logger.info("vSphere metrics generator stopped.")


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
