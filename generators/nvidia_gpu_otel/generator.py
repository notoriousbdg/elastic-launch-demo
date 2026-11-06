#!/usr/bin/env python3
"""NVIDIA GPU Generator — sends DCGM exporter metrics via OTLP.

Emits data matching the nvidia_gpu_otel content package
(discovery.datasets: nvidia_gpu.otel).

Metric names use the DCGM_FI_DEV_* naming convention from the
NVIDIA Data Center GPU Manager Prometheus exporter.  Attribute keys are
UUID, Hostname, modelName, and DCGM_FI_DRIVER_VERSION (Prometheus labels).

Also bulk-indexes co-located inventory documents each tick so the
GPU Inventory panel (AND-filter on GPU_TEMP then AVG of util/power/FB)
is populated — OTLP intake stores one metric per document.

XID Error Details always carries err_code/err_msg attributes so the
ES|QL BY columns exist in the mapping. Healthy ticks emit 0 (no XID
occurrence) by default; chaos forces a sticky XID code.

Usage (standalone):
    python3 -m generators.nvidia_gpu_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/nvidia_gpu_otel@0.3.0"

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
    otel_dataset,
    resolve_active_chaos,
)

NVIDIA_GPU_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
)
SCOPE_VERSION = "0.115.0"
_METRICS_DATA_STREAM = "metrics-nvidia_gpu.otel-default"

logger = logging.getLogger("nvidia-gpu-generator")
EMIT_INTERVAL_S = 15.0

# Metrics queried by nvidia_gpu_otel-overview (plus extras covered by the package docs).
METRIC_NAMES = (
    "DCGM_FI_DEV_GPU_UTIL",
    "DCGM_FI_DEV_MEM_COPY_UTIL",
    "DCGM_FI_DEV_ENC_UTIL",
    "DCGM_FI_DEV_DEC_UTIL",
    "DCGM_FI_DEV_FB_FREE",
    "DCGM_FI_DEV_FB_USED",
    "DCGM_FI_DEV_POWER_USAGE",
    "DCGM_FI_DEV_TOTAL_ENERGY_CONSUMPTION",
    "DCGM_FI_DEV_GPU_TEMP",
    "DCGM_FI_DEV_MEMORY_TEMP",
    "DCGM_FI_DEV_SM_CLOCK",
    "DCGM_FI_DEV_MEM_CLOCK",
    "DCGM_FI_DEV_NVLINK_BANDWIDTH_TOTAL",
    "DCGM_FI_DEV_XID_ERRORS",
)

# Common NVIDIA XID codes → short messages for the overview error table.
_XID_MESSAGES: dict[int, str] = {
    13: "Graphics Engine Exception",
    31: "GPU memory page fault",
    43: "GPU stopped processing",
    45: "Preemptive cleanup due to previous errors",
    48: "Double Bit ECC Error",
    63: "GPU Row Remapping Event",
    74: "NVLink Error",
    79: "GPU has fallen off the bus",
}

# A100-SXM4-40GB has 40 GB HBM2e
_FB_TOTAL_MIB = 40536
_DEFAULT_DRIVER_VERSION = "550.54.15"


def _make_uuid(rng: random.Random, idx: int) -> str:
    hex_digits = "0123456789ABCDEF"
    return "GPU-" + "".join(rng.choice(hex_digits) for _ in range(8)) + f"-{idx:04d}"


def _default_cloud() -> dict:
    return {"provider": "aws", "platform": "aws_ec2",
            "region": "us-east-1", "availability_zone": "us-east-1a"}


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


def build_gpu_inventory_doc(
    resource: dict,
    *,
    uuid: str,
    hostname: str,
    model_name: str,
    driver_version: str,
    gpu: str,
    gpu_temp: float,
    gpu_util: float,
    power_w: float,
    fb_used: int,
    fb_free: int,
    sm_clock: int | None = None,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc with inventory gauges co-located.

    GPU Inventory filters ``DCGM_FI_DEV_GPU_TEMP IS NOT NULL`` then averages
    util/power/FB on the same document. OTLP stores one metric per document,
    so those sibling averages are null unless co-located via bulk.

    Clock-throttle OOTB also requires SM_CLOCK + UTIL + TEMP on one doc.
    """
    metrics: dict[str, Any] = {
        "DCGM_FI_DEV_GPU_TEMP": float(gpu_temp),
        "DCGM_FI_DEV_GPU_UTIL": float(gpu_util),
        "DCGM_FI_DEV_POWER_USAGE": float(power_w),
        # Doubles so OOTB ``USED / total * 100`` is not long/long → 0.
        "DCGM_FI_DEV_FB_USED": float(fb_used),
        "DCGM_FI_DEV_FB_FREE": float(fb_free),
    }
    if sm_clock is not None:
        metrics["DCGM_FI_DEV_SM_CLOCK"] = int(sm_clock)
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "1",
        "data_stream": {
            "type": "metrics",
            "dataset": "nvidia_gpu.otel",
            "namespace": "default",
        },
        "attributes": {
            "UUID": uuid,
            "Hostname": hostname,
            "modelName": model_name,
            "DCGM_FI_DRIVER_VERSION": driver_version,
            "gpu": gpu,
        },
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": NVIDIA_GPU_SCOPE, "version": SCOPE_VERSION},
        "metrics": metrics,
    }


class _GpuState(CounterState):
    def __init__(self, rng: random.Random, uuid: str, model: str, host: str):
        super().__init__(rng)
        self.uuid = uuid
        self.model = model
        self.host = host
        self.energy_total = rng.randint(0, 1_000_000)  # mJ
        self.util = rng.uniform(10.0, 80.0)
        self.xid = 0



from generators.nvidia_gpu_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("nvidia-gpu")
    gpu_count: int = max(1, cfg.get("gpu_count", 2))
    model_name: str = cfg.get("model_name", "A100-SXM4-40GB")
    driver_version: str = cfg.get("driver_version", _DEFAULT_DRIVER_VERSION)
    # Probability per tick that a healthy GPU starts a transient XID (demo panel fill).
    baseline_xid_rate = float(cfg.get("baseline_xid_rate", 0.0))
    cloud = cfg.get("cloud") or _default_cloud()
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("gpu-node")


    gpu_ids = [str(i) for i in range(gpu_count)]
    uuids = {i: _make_uuid(rng, int(i)) for i in gpu_ids}
    gpu_states = {i: _GpuState(rng, uuids[i], model_name, host_name) for i in gpu_ids}

    resource = build_signal_resource(
        ns=ns, signal_type="metrics", dataset=otel_dataset("nvidia_gpu"),
        service_name=service_name, service_version="3.3.5",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name, cloud=cloud,
    )
    scope = {"name": NVIDIA_GPU_SCOPE, "version": SCOPE_VERSION}
    try:
        es_bulk = ESBulkClient()
    except Exception as exc:  # pragma: no cover - missing http2/deps in some host envs
        logger.warning("ESBulkClient unavailable (%s); co-located docs disabled", exc)
        es_bulk = None

    logger.info("NVIDIA GPU generator started (namespace=%s, gpus=%d, model=%s)",
                ns, gpu_count, model_name)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="nvidia_gpu_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        c = active_chaos or {}
        temp_spike = float(c.get("gpu_temperature_critical", 0.0)) > 0
        util_sat = float(c.get("gpu_utilization_saturated", 0.0))
        util_mult = util_sat if util_sat > 0 else 1.0
        xid_fault = float(c.get("xid_errors_detected", 0.0)) > 0
        clock_throttle = float(c.get("clock_frequency_throttled_under_load", 0.0))
        fb_mem = float(c.get("framebuffer_memory_utilization_high", 0.0))
        # SPEC default for idle is 100.0 (healthy). resolve_active_chaos injects
        # that default on every channel — only treat low values as the idle probe.
        gpu_idle = float(c.get("gpu_idle_during_expected_workload", 0.0))
        load = business_load()

        metrics: list[dict] = []
        inventory_docs: list[dict[str, Any]] = []
        ts_ms = int(time.time() * 1000)

        for gpu_id, state in gpu_states.items():
            attrs = {
                "UUID": state.uuid,
                "Hostname": state.host,
                "modelName": state.model,
                "DCGM_FI_DRIVER_VERSION": driver_version,
                "gpu": gpu_id,
            }
            state.util = max(0.0, min(100.0, state.util + rng.uniform(-5.0, 5.0)))
            if 0 < gpu_idle < 50.0:
                # OOTB: max_util < 1. GT probe is often 5.0 (not the util itself);
                # SPEC healthy default is 100.0 (ignored here).
                util = 0.0
            elif clock_throttle > 0:
                # OOTB clock-throttle requires avg_util > 50 while clocks sag.
                util = max(state.util * load, 75.0)
            elif util_sat > 0:
                # GT probe is often 75; OOTB saturates above ~95% avg util.
                util = max(95.0, min(100.0, util_sat if util_sat <= 100 else 98.0))
            else:
                util = state.util * util_mult * load
            # Framebuffer alert is used/(used+free) > 0.9 — GT percent probes
            # (e.g. 75) must be lifted into the breach band.
            if fb_mem > 0:
                mem_util = fb_mem if fb_mem > 1.0 else fb_mem * 100.0
                mem_util = max(mem_util, 95.0)
            else:
                mem_util = util * rng.uniform(0.6, 1.1)
            fb_used = int((_FB_TOTAL_MIB * mem_util / 100.0))
            fb_free = _FB_TOTAL_MIB - fb_used

            metrics.append(build_gauge("DCGM_FI_DEV_GPU_UTIL", "%", round(util, 1), attrs))
            metrics.append(build_gauge("DCGM_FI_DEV_MEM_COPY_UTIL", "%", round(mem_util, 1), attrs))
            metrics.append(build_gauge("DCGM_FI_DEV_ENC_UTIL", "%", round(rng.uniform(0, util * 0.3), 1), attrs))
            metrics.append(build_gauge("DCGM_FI_DEV_DEC_UTIL", "%", round(rng.uniform(0, util * 0.3), 1), attrs))
            metrics.append(build_gauge("DCGM_FI_DEV_FB_FREE", "MiB", float(fb_free), attrs))
            metrics.append(build_gauge("DCGM_FI_DEV_FB_USED", "MiB", float(fb_used), attrs))
            power_w = round(util * 4.0 + rng.uniform(50.0, 100.0), 1)  # A100 max 400W
            metrics.append(build_gauge("DCGM_FI_DEV_POWER_USAGE", "W", power_w, attrs))
            state.energy_total += int(power_w * EMIT_INTERVAL_S * 1000)
            metrics.append(build_cumulative_sum("DCGM_FI_DEV_TOTAL_ENERGY_CONSUMPTION", "mJ",
                                                state.energy_total, state.start_time_ns, attrs))
            # OOTB critical temp is >83°C; random 70–85 can miss the floor.
            gpu_temp = round(rng.uniform(84.0, 92.0) if temp_spike else rng.uniform(40.0, 65.0), 1)
            mem_temp = round(gpu_temp * rng.uniform(0.85, 0.95), 1)
            metrics.append(build_gauge("DCGM_FI_DEV_GPU_TEMP", "C", gpu_temp, attrs))
            metrics.append(build_gauge("DCGM_FI_DEV_MEMORY_TEMP", "C", mem_temp, attrs))
            sm_clock = int(1593 * (util / 100.0) + rng.randint(100, 300))  # MHz
            if clock_throttle > 0:
                # OOTB: avg_util>50 AND avg_sm_clock/max_sm_clock*100 < 70.
                # Do NOT use the GT probe MHz as the only clock (avg==max →
                # ratio 100%). Pair boost + throttled; stagger timestamps so
                # TSDB dimension+_timestamp create doesn't version-conflict.
                boost_mhz = 1593
                throttled_mhz = 400
                metrics.append(build_gauge("DCGM_FI_DEV_SM_CLOCK", "MHz", boost_mhz, attrs))
                sm_clock = throttled_mhz
            metrics.append(build_gauge("DCGM_FI_DEV_SM_CLOCK", "MHz", min(sm_clock, 1593), attrs))
            metrics.append(build_gauge("DCGM_FI_DEV_MEM_CLOCK", "MHz",
                                       rng.choice([1215, 1593]), attrs))
            nvlink_bw = int(util * 50.0 * load)  # MB/s proxy
            metrics.append(build_gauge("DCGM_FI_DEV_NVLINK_BANDWIDTH_TOTAL", "MB/s", nvlink_bw, attrs))

            # XID: chaos forces a sticky code; healthy baseline emits sparse
            # transient errors so XID Error Details / GPUs-with-Errors panels
            # have demo data. Always attach err_code/err_msg (non-empty) so the
            # ES|QL BY columns exist in the mapping.
            if xid_fault:
                if not state.xid:
                    state.xid = rng.choice(list(_XID_MESSAGES))
            elif state.xid:
                # Clear transient baseline errors after a few ticks.
                if rng.random() < 0.35:
                    state.xid = 0
            elif baseline_xid_rate > 0 and rng.random() < baseline_xid_rate:
                state.xid = rng.choice(list(_XID_MESSAGES))
            xid_attrs = dict(attrs)
            if state.xid:
                xid_attrs["err_code"] = str(state.xid)
                xid_attrs["err_msg"] = _XID_MESSAGES.get(state.xid, f"XID {state.xid}")
            else:
                xid_attrs["err_code"] = "0"
                xid_attrs["err_msg"] = "None"
            metrics.append(build_gauge("DCGM_FI_DEV_XID_ERRORS", "1", state.xid, xid_attrs))

            inventory_docs.append(build_gpu_inventory_doc(
                resource,
                uuid=state.uuid,
                hostname=state.host,
                model_name=state.model,
                driver_version=driver_version,
                gpu=gpu_id,
                gpu_temp=gpu_temp,
                gpu_util=round(util, 1),
                power_w=power_w,
                fb_used=fb_used,
                fb_free=fb_free,
                sm_clock=min(sm_clock, 1593),
                timestamp_ms=ts_ms,
            ))
            if clock_throttle > 0:
                inventory_docs.append(build_gpu_inventory_doc(
                    resource,
                    uuid=state.uuid,
                    hostname=state.host,
                    model_name=state.model,
                    driver_version=driver_version,
                    gpu=gpu_id,
                    gpu_temp=gpu_temp,
                    gpu_util=round(util, 1),
                    power_w=power_w,
                    fb_used=fb_used,
                    fb_free=fb_free,
                    sm_clock=1593,
                    timestamp_ms=ts_ms + 1000,
                ))

        client.send_metrics_scoped(resource, scope, metrics)
        if es_bulk is not None and es_bulk.configured and inventory_docs:
            es_bulk.send_bulk(_METRICS_DATA_STREAM, inventory_docs)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: EMIT_INTERVAL_S)

    if es_bulk is not None:
        es_bulk.close()
    logger.info("NVIDIA GPU generator stopped.")


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
