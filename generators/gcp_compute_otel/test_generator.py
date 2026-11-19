"""Tests for the gcp_compute_otel generator."""

from __future__ import annotations

import random

from generator_engine.identity import ScenarioIdentity
from generators.gcp_compute_otel import SPEC
from generators.gcp_compute_otel.generator import (
    METRIC_NAMES,
    VMState,
    build_fleet,
    generate_group_metrics,
    generate_vm_metrics,
)

_P = "compute.googleapis.com/"
NOW = 1_700_000_000.0


def _attrs(dp: dict) -> dict:
    return {a["key"]: next(iter(a["value"].values())) for a in dp.get("attributes", [])}


def _points(metrics: list[dict], suffix: str) -> list[tuple[dict, float]]:
    out = []
    for m in metrics:
        if m["name"] != _P + suffix:
            continue
        for dp in m["gauge"]["dataPoints"]:
            value = float(dp["asDouble"]) if "asDouble" in dp else float(dp["asInt"])
            out.append((_attrs(dp), value))
    return out


def _fleet():
    ident = ScenarioIdentity("gtgcpcompute")
    return build_fleet(ident, ident.gcp_project_id, "us-central1",
                       ["us-central1-a", "us-central1-b", "us-central1-c"], 3)


def _emit(chaos=None, seed=7):
    rng = random.Random(seed)
    vms, _ = _fleet()
    out = []
    for n, vm in enumerate(vms):
        state = VMState(rng)
        out.extend(generate_vm_metrics(vm, state, rng, chaos, first_vm=(n == 0),
                                       fleet_size=len(vms), now=NOW))
    return out


def test_spec_shape():
    assert SPEC.client == "otlp"
    assert SPEC.datasets == ("gcp.compute.otel",)
    assert SPEC.integrations == ("gcp_compute_otel",)
    assert SPEC.wants_chaos is True
    assert SPEC.chaos_mode == "explicit"
    assert len(SPEC.impact_keys) >= 3
    assert all(not f["required"] for f in SPEC.config_fields.values())


def test_fleet_identity_and_group():
    vms, group = _fleet()
    ident = ScenarioIdentity("gtgcpcompute")
    assert [vm["instance_id"] for vm in vms] == [ident.gcp_instance_id(i) for i in (1, 2, 3)]
    assert {vm["zone"] for vm in vms} == {"us-central1-a", "us-central1-b", "us-central1-c"}
    assert all(vm["project_id"] == "gtgcpcompute-project" for vm in vms)
    assert group and group["size"] == 2
    size = generate_group_metrics(group)
    assert size[0]["name"] == _P + "instance_group/size"


def test_every_dashboard_metric_is_emitted_as_gauge():
    metrics = _emit() + generate_group_metrics(_fleet()[1])
    assert {m["name"] for m in metrics} == set(METRIC_NAMES)
    assert all("gauge" in m for m in metrics)
    for m in metrics:
        for dp in m["gauge"]["dataPoints"]:
            if m["name"] != _P + "instance_group/size":
                assert "instance_name" in _attrs(dp)


def test_healthy_baseline():
    metrics = _emit()
    cpu = [v for _, v in _points(metrics, "instance/cpu/utilization")]
    assert all(0.05 < v < 0.7 for v in cpu)
    status = _points(metrics, "instance/disk/performance_status")
    active = {a["performance_status"] for a, v in status if v == 1}
    assert active == {"Healthy"}
    interruptions = _points(metrics, "instance/interruption_count")
    assert all(a["interruption_type"] == "none" and v == 0 for a, v in interruptions)
    assert max(v for _, v in _points(metrics, "firewall/dropped_packets_count")) < 50
    assert max(v for _, v in _points(metrics, "instance/disk/average_io_latency")) < 5000
    used = sum(v for _, v in _points(metrics, "instance/memory/balloon/ram_used"))
    size = sum(v for _, v in _points(metrics, "instance/memory/balloon/ram_size"))
    assert 0.3 < used / size < 0.8


def test_cpu_utilization_high():
    metrics = _emit({"cpu_utilization_high": 95.0})
    cpu = [v for _, v in _points(metrics, "instance/cpu/utilization")]
    assert min(cpu) > 0.9
    healthy_wait = sum(v for _, v in _points(_emit(), "instance/cpu/scheduler_wait_time"))
    hot_wait = sum(v for _, v in _points(metrics, "instance/cpu/scheduler_wait_time"))
    assert hot_wait > healthy_wait * 5


def test_instance_interruption():
    metrics = _emit({"instance_interruption": 3.0})
    events = [(a, v) for a, v in _points(metrics, "instance/interruption_count")
              if a["interruption_type"] != "none"]
    assert len(events) == 1
    attrs, value = events[0]
    assert value == 3 and attrs["interruption_type"] == "Preemption"
    assert attrs["instance_name"].endswith("batch-01")
    uptime = {a["instance_name"]: v for a, v in _points(metrics, "instance/uptime_total")}
    assert uptime[attrs["instance_name"]] < 60


def test_disk_throttled():
    healthy = _emit()
    metrics = _emit({"disk_throttled": 1.0})
    lat = max(v for _, v in _points(metrics, "instance/disk/average_io_latency"))
    assert lat > 10 * max(v for _, v in _points(healthy, "instance/disk/average_io_latency"))
    assert max(v for _, v in _points(metrics, "instance/disk/average_io_queue_depth")) > 33
    active = [a for a, v in _points(metrics, "instance/disk/performance_status") if v == 1]
    assert sum(a["performance_status"] == "Severely Degraded" for a in active) == 1
    degraded = _emit({"disk_throttled": 0.4})
    active = [a for a, v in _points(degraded, "instance/disk/performance_status") if v == 1]
    assert sum(a["performance_status"] == "Degraded" for a in active) == 1


def test_firewall_drop_spike():
    metrics = _emit({"firewall_drop_spike": 20000.0})
    total = sum(v for _, v in _points(metrics, "firewall/dropped_packets_count"))
    assert total > 15000
    assert sum(v for _, v in _points(metrics, "firewall/dropped_bytes_count")) > total * 50
