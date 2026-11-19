"""NVIDIA GPU-specific tests for the nvidia_gpu generator."""

from __future__ import annotations

import threading
from typing import Any

from generator_engine.db_common import build_signal_resource
from generators.nvidia_gpu_otel.generator import (
    METRIC_NAMES,
    build_gpu_inventory_doc,
    run,
)


class _FakeClient:
    def __init__(self) -> None:
        self.batches: list[tuple[dict, dict, list[dict]]] = []

    def send_metrics_scoped(self, resource: dict, scope: dict, metrics: list[dict]) -> None:
        self.batches.append((resource, scope, metrics))


def _metric_names(metrics: list[dict]) -> set[str]:
    return {m["name"] for m in metrics}


def _attrs_for(metrics: list[dict], name: str) -> dict[str, Any]:
    for m in metrics:
        if m["name"] != name:
            continue
        dps = m.get("gauge", {}).get("dataPoints") or m.get("sum", {}).get("dataPoints") or []
        if not dps:
            continue
        out: dict[str, Any] = {}
        for a in dps[0].get("attributes", []):
            key = a["key"]
            val = a["value"]
            out[key] = next(iter(val.values()))
        return out
    return {}


def test_emit_matches_dashboard_contract() -> None:
    client = _FakeClient()
    stop = threading.Event()

    def _stop_after_one(*_a, **_k):
        stop.set()
        return True

    stop.wait = _stop_after_one  # type: ignore[method-assign]

    run(
        client,
        stop,
        scenario_data={
            "namespace": "testns",
            # Disable sparse baseline XIDs so this assertion is deterministic.
            "generator_config": {"baseline_xid_rate": 0},
        },
    )
    assert client.batches, "expected at least one metrics batch"
    resource, scope, metrics = client.batches[0]

    assert "prometheusreceiver" in scope["name"]
    ds = {a["key"]: next(iter(a["value"].values())) for a in resource["attributes"]}
    assert ds["data_stream.dataset"] == "nvidia_gpu"

    names = _metric_names(metrics)
    assert "DCGM_FI_DEV_GPU_TEMP" in names
    assert "DCGM_FI_DEV_XID_ERRORS" in names
    assert names <= set(METRIC_NAMES)

    gpu_attrs = _attrs_for(metrics, "DCGM_FI_DEV_GPU_TEMP")
    assert gpu_attrs.get("UUID")
    assert gpu_attrs.get("Hostname")
    assert gpu_attrs.get("modelName")
    assert gpu_attrs.get("DCGM_FI_DRIVER_VERSION")

    xid_attrs = _attrs_for(metrics, "DCGM_FI_DEV_XID_ERRORS")
    # Healthy baseline still emits err_code/err_msg so ES|QL mapping exists.
    assert xid_attrs.get("err_code") == "0"
    assert xid_attrs.get("err_msg") == "None"


def test_baseline_xid_rate_emits_nonzero_errors() -> None:
    client = _FakeClient()
    stop = threading.Event()
    ticks = {"n": 0}

    def _stop_after_many(*_a, **_k):
        ticks["n"] += 1
        if ticks["n"] >= 40:
            stop.set()
            return True
        return False

    stop.wait = _stop_after_many  # type: ignore[method-assign]

    run(
        client,
        stop,
        scenario_data={
            "namespace": "testns",
            "generator_config": {"baseline_xid_rate": 1.0, "gpu_count": 1},
        },
    )
    assert client.batches
    # First tick with rate=1.0 must start an XID on the sole GPU.
    xid_attrs = _attrs_for(client.batches[0][2], "DCGM_FI_DEV_XID_ERRORS")
    assert xid_attrs["err_code"] not in ("", "0")
    assert xid_attrs["err_msg"] not in ("", "None")


def test_xid_chaos_sets_err_code_and_msg() -> None:
    client = _FakeClient()
    stop = threading.Event()

    def _stop_after_one(*_a, **_k):
        stop.set()
        return True

    stop.wait = _stop_after_one  # type: ignore[method-assign]

    class _Chaos:
        def get_active_channels(self):
            return ["gpu-xid"]

    run(
        client,
        stop,
        scenario_data={
            "namespace": "testns",
            "generator_config": {"chaos": "explicit"},
            "channel_registry": {
                "gpu-xid": {
                    "name": "XID errors",
                    "generator_impact": {
                        "nvidia_gpu_otel": {"xid_errors_detected": 1.0},
                    },
                },
            },
        },
        chaos_controller=_Chaos(),
    )
    assert client.batches
    _, _, metrics = client.batches[0]
    xid_attrs = _attrs_for(metrics, "DCGM_FI_DEV_XID_ERRORS")
    assert xid_attrs["err_code"] not in ("", "0")
    assert xid_attrs["err_msg"] not in ("", "None")


def test_gpu_inventory_doc_colocates_temp_util_power_fb() -> None:
    """Inventory panel filters on TEMP then averages util/power/FB on one doc."""
    resource = build_signal_resource(
        ns="testns",
        signal_type="metrics",
        dataset="nvidia_gpu",
        service_name="nvidia-gpu",
        service_version="3.3.5",
        service_instance_id="nvidia-gpu-001",
        host_name="testns-gpu-node",
        cloud={
            "provider": "aws",
            "platform": "aws_ec2",
            "region": "us-east-1",
            "availability_zone": "us-east-1a",
        },
    )
    doc = build_gpu_inventory_doc(
        resource,
        uuid="GPU-ABCD1234-0000",
        hostname="testns-gpu-node",
        model_name="A100-SXM4-40GB",
        driver_version="550.54.15",
        gpu="0",
        gpu_temp=52.5,
        gpu_util=41.2,
        power_w=210.0,
        fb_used=12000,
        fb_free=28536,
    )

    assert doc["attributes"]["UUID"] == "GPU-ABCD1234-0000"
    assert doc["attributes"]["Hostname"] == "testns-gpu-node"
    assert doc["attributes"]["modelName"] == "A100-SXM4-40GB"
    assert doc["attributes"]["DCGM_FI_DRIVER_VERSION"] == "550.54.15"
    assert doc["metrics"]["DCGM_FI_DEV_GPU_TEMP"] == 52.5
    assert doc["metrics"]["DCGM_FI_DEV_GPU_UTIL"] == 41.2
    assert doc["metrics"]["DCGM_FI_DEV_POWER_USAGE"] == 210.0
    assert doc["metrics"]["DCGM_FI_DEV_FB_USED"] == 12000
    assert doc["metrics"]["DCGM_FI_DEV_FB_FREE"] == 28536
    assert doc["data_stream"]["dataset"] == "nvidia_gpu.otel"
    assert "data_stream.type" not in doc["resource"]["attributes"]

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.nvidia_gpu_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['clock_frequency_throttled_under_load', 'framebuffer_memory_utilization_high', 'gpu_idle_during_expected_workload', 'gpu_temperature_critical', 'gpu_utilization_saturated', 'xid_errors_detected']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
