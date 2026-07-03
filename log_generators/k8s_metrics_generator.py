#!/usr/bin/env python3
"""Kubernetes Metrics Generator — sends k8s node/pod/container/deployment metrics via OTLP.

Ported from otel-demo-gen/backend/k8s_metrics_generator.py, adapted to NOVA-7 patterns.
Generates metrics that populate the [OTEL][Metrics Kubernetes] Cluster Overview dashboard.

Each metric is routed to the index that the real OTel receiver would produce:
  - kubeletstatsreceiver scope + data_stream.dataset="kubeletstatsreceiver"
    → metrics-kubeletstatsreceiver.otel-default (queried by Kubelet panels)
  - k8sclusterreceiver scope + data_stream.dataset="k8sclusterreceiver"
    → metrics-k8sclusterreceiver.otel-default (queried by Cluster panels)

Usage (standalone):
    python3 -m log_generators.k8s_metrics_generator
"""

from __future__ import annotations

import logging
import os
import random
import secrets
import signal
import threading
import time
import uuid
import zlib
from datetime import datetime, timezone

from app.telemetry import OTLPClient, _format_attributes, SCHEMA_URL, _now_ns
from app.config import ACTIVE_SCENARIO, NAMESPACE

logger = logging.getLogger("k8s-metrics-generator")

METRICS_INTERVAL = int(os.getenv("K8S_METRICS_INTERVAL", "15"))

# Scope names matching real OTel K8s receivers
KUBELET_SCOPE = "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kubeletstatsreceiver"
CLUSTER_SCOPE = "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver"
K8S_OBJECTS_SCOPE = "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sobjectsreceiver"
SCOPE_VERSION = "0.115.0"

# ── Load from active scenario ────────────────────────────────────────────────
def _load_scenario_data():
    from scenario_engine import get_scenario
    scenario = get_scenario(ACTIVE_SCENARIO)
    return list(scenario.services.keys()), scenario.k8s_clusters

SERVICES, CLUSTERS = _load_scenario_data()

# Legacy single-cluster config (used by warning event logs) — use first cluster
CLOUD_CONFIG = {
    "provider": CLUSTERS[0]["provider"] if CLUSTERS else "aws",
    "platform": CLUSTERS[0]["platform"] if CLUSTERS else "aws_eks",
    "region": CLUSTERS[0]["region"] if CLUSTERS else "us-east-1",
    "zones": CLUSTERS[0]["zones"] if CLUSTERS else ["us-east-1a"],
    "os_description": CLUSTERS[0].get("os_description", "Linux") if CLUSTERS else "Linux",
}


def _init_pod_data(cluster: dict, seed_offset: int = 0, namespace: str = NAMESPACE) -> dict:
    """Initialize static K8s pod/node/deployment data for a cluster's services.

    Uses a namespace-scoped seed so pod/node names are deterministic across restarts
    and unique per scenario even when scenarios share the same cluster-index/region layout.
    Seed expression mirrors infra_topology.build_topology exactly so IDs are identical
    between the trace generator and the k8s metrics generator.
    seed_offset differentiates clusters (0, 1, 2).
    """
    # Namespace-scoped seed: random.Random(str) uses sha512, stable across processes.
    # Must mirror build_topology in infra_topology.py exactly.
    stable = random.Random(f"{namespace}:{seed_offset}")

    region = cluster["region"]
    # Per-namespace subnet octet — ensures node names never collide across scenarios.
    ns_octet = zlib.crc32(namespace.encode()) % 256
    node_names = [
        f"ip-10-{ns_octet}-{stable.randint(10, 200)}-{stable.randint(10, 200)}.{region}.compute.internal"
        for _ in range(3)
    ]

    pods = {}
    for svc in cluster["services"]:
        node_name = stable.choice(node_names)
        pod_hex1 = f"{stable.getrandbits(32):08x}"
        pod_hex2 = f"{stable.getrandbits(24):06x}"
        pods[svc] = {
            "pod_name": f"{svc}-{pod_hex1}-{pod_hex2}",
            "pod_uid": str(uuid.UUID(int=stable.getrandbits(128))),
            "pod_ip": f"10.{stable.randint(100, 120)}.{stable.randint(1, 10)}.{stable.randint(2, 250)}",
            "node_name": node_name,
            "node_uid": str(uuid.UUID(int=stable.getrandbits(128))),
            "deployment_name": f"{svc}-deployment",
            "replicaset_name": f"{svc}-{stable.getrandbits(32):08x}",
            "container_id": f"containerd://{stable.getrandbits(256):064x}",
        }

    return {"pods": pods, "node_names": list(set(node_names))}


class K8sState:
    """Tracks cumulative counters per service."""

    def __init__(self, rng: random.Random, services: list[str] | None = None):
        self._rng = rng
        self._services = services or list(SERVICES)
        self.net_rx = {svc: rng.randint(50_000_000, 100_000_000) for svc in self._services}
        self.net_tx = {svc: rng.randint(70_000_000, 120_000_000) for svc in self._services}
        self.restarts = {svc: 0 for svc in self._services}

    def tick(self):
        rng = self._rng
        for svc in self._services:
            self.net_rx[svc] += rng.randint(10_000, 100_000)
            self.net_tx[svc] += rng.randint(15_000, 120_000)
            if rng.random() < 0.05:
                self.restarts[svc] += 1


def _gauge(name: str, unit: str, value, is_int: bool = False, attributes: dict | None = None, ts: str | None = None) -> dict:
    now = ts or _now_ns()
    dp: dict = {"timeUnixNano": now}
    if is_int:
        dp["asInt"] = str(int(value))
    else:
        dp["asDouble"] = float(value)
    if attributes:
        dp["attributes"] = _format_attributes(attributes)
    return {"name": name, "unit": unit, "gauge": {"dataPoints": [dp]}}


def _cumulative_sum(name: str, unit: str, value, is_int: bool = True, attributes: dict | None = None, ts: str | None = None) -> dict:
    now = ts or _now_ns()
    dp: dict = {"timeUnixNano": now}
    if is_int:
        dp["asInt"] = str(int(value))
    else:
        dp["asDouble"] = float(value)
    if attributes:
        dp["attributes"] = _format_attributes(attributes)
    return {
        "name": name, "unit": unit,
        "sum": {"dataPoints": [dp], "aggregationTemporality": 2, "isMonotonic": True},
    }


def _build_pod_resource(svc: str, pod_data: dict, cluster: dict, namespace: str = NAMESPACE) -> dict:
    """Build OTLP resource for a pod (kubeletstatsreceiver).

    Carries kubelet-owned pod and container metrics.
    Note: k8s.container.status.last_terminated_reason is a k8sclusterreceiver
    resource attribute — it lives on _build_container_resource instead.
    """
    p = pod_data["pods"][svc]
    attrs = {
        "k8s.namespace.name": namespace,
        "k8s.deployment.name": p["deployment_name"],
        "k8s.replicaset.name": p["replicaset_name"],
        "k8s.node.name": p["node_name"],
        "k8s.node.uid": p["node_uid"],
        "k8s.pod.name": p["pod_name"],
        "k8s.pod.ip": p["pod_ip"],
        "k8s.pod.uid": p["pod_uid"],
        "k8s.cluster.name": cluster["name"],
        "container.name": f"{svc}-container",
        "container.id": p["container_id"],
        "container.image.name": f"{svc}:latest",
        "service.name": svc,
        "service.namespace": namespace,
        "host.name": p["node_name"],
        "host.architecture": "amd64",
        "os.type": "linux",
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "cloud.region": cluster["region"],
        "telemetry.sdk.name": "opentelemetry",
        "telemetry.sdk.version": "1.24.0",
        "telemetry.sdk.language": "python",
        "data_stream.type": "metrics",
        "data_stream.dataset": "kubeletstatsreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _build_node_resource(node_name: str, pod_data: dict, cluster: dict) -> dict:
    """Build OTLP resource for a node (k8sclusterreceiver).

    Carries cluster-owned node metrics: allocatable_*, condition_*.
    """
    # Find a pod on this node for its node_uid
    node_uid = ""
    container_id = ""
    for svc in cluster["services"]:
        p = pod_data["pods"][svc]
        if p["node_name"] == node_name:
            node_uid = p["node_uid"]
            container_id = p["container_id"]
            break
    attrs = {
        "k8s.node.name": node_name,
        "k8s.node.uid": node_uid,
        "k8s.cluster.name": cluster["name"],
        "host.name": node_name,
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "cloud.region": cluster["region"],
        "os.type": "linux",
        "os.description": cluster["os_description"],
        "container.id": container_id,
        "data_stream.type": "metrics",
        "data_stream.dataset": "k8sclusterreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _build_node_resource_kubelet(node_name: str, pod_data: dict, cluster: dict) -> dict:
    """Build OTLP resource for a node (kubeletstatsreceiver).

    Carries kubelet-owned node metrics: cpu.usage, memory.*, filesystem.*, network.*.
    """
    node_uid = ""
    for svc in cluster["services"]:
        p = pod_data["pods"][svc]
        if p["node_name"] == node_name:
            node_uid = p["node_uid"]
            break
    attrs = {
        "k8s.node.name": node_name,
        "k8s.node.uid": node_uid,
        "k8s.cluster.name": cluster["name"],
        "host.name": node_name,
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "cloud.region": cluster["region"],
        "os.type": "linux",
        "os.description": cluster["os_description"],
        "data_stream.type": "metrics",
        "data_stream.dataset": "kubeletstatsreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _build_deployment_resource(svc: str, pod_data: dict, cluster: dict, namespace: str = NAMESPACE) -> dict:
    """Build OTLP resource for a deployment (k8sclusterreceiver)."""
    p = pod_data["pods"][svc]
    attrs = {
        "k8s.deployment.name": p["deployment_name"],
        "k8s.namespace.name": namespace,
        "k8s.cluster.name": cluster["name"],
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "container.id": p["container_id"],
        "data_stream.type": "metrics",
        "data_stream.dataset": "k8sclusterreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _generate_pod_metrics(svc: str, state: K8sState, rng: random.Random) -> list:
    """Generate pod + kubelet-owned container metrics for one service.

    kubeletstatsreceiver owns: pod cpu/memory/network/filesystem,
    and per-container cpu.usage + memory.working_set.
    k8sclusterreceiver-owned container metrics (restarts, requests, limits)
    are emitted separately via _generate_container_metrics.
    """
    metrics = []
    ts = _now_ns()  # shared timestamp — all metrics go into one TSDB document

    # Pod CPU
    metrics.append(_gauge("k8s.pod.cpu.usage", "ns", rng.randint(10_000_000, 500_000_000), is_int=True, ts=ts))
    metrics.append(_gauge("k8s.pod.cpu_limit_utilization", "1", rng.uniform(0.05, 0.85), ts=ts))
    metrics.append(_gauge("k8s.pod.cpu.node.utilization", "1", rng.uniform(0.05, 0.45), ts=ts))

    # Pod Memory
    metrics.append(_gauge("k8s.pod.memory.usage", "By", rng.randint(100_000_000, 800_000_000), is_int=True, ts=ts))
    metrics.append(_gauge("k8s.pod.memory_limit_utilization", "1", rng.uniform(0.25, 0.85), ts=ts))
    metrics.append(_gauge("k8s.pod.memory.node.utilization", "1", rng.uniform(0.001, 0.05), ts=ts))
    metrics.append(_gauge("k8s.pod.memory.working_set", "By", rng.randint(80_000_000, 600_000_000), is_int=True, ts=ts))

    # Pod Network (cumulative)
    metrics.append(_cumulative_sum("k8s.pod.network.rx", "By", state.net_rx[svc], ts=ts))
    metrics.append(_cumulative_sum("k8s.pod.network.tx", "By", state.net_tx[svc], ts=ts))

    # Pod Filesystem
    metrics.append(_gauge("k8s.pod.filesystem.usage", "By", rng.randint(100_000_000, 500_000_000), is_int=True, ts=ts))

    # Container metrics owned by kubeletstatsreceiver
    container_attrs = {"container.name": f"{svc}-container"}
    metrics.append(_gauge("k8s.container.cpu.usage", "ns", rng.randint(10_000_000, 600_000_000), is_int=True, attributes=container_attrs, ts=ts))
    metrics.append(_gauge("k8s.container.memory.working_set", "By", rng.randint(100_000_000, 400_000_000), is_int=True, attributes=container_attrs, ts=ts))

    return metrics


def _generate_node_metrics_cluster(rng: random.Random) -> list:
    """Generate k8sclusterreceiver-owned node metrics.

    k8sclusterreceiver owns: allocatable_*, condition_*.
    """
    ts = _now_ns()
    return [
        _gauge("k8s.node.allocatable_cpu", "1", rng.uniform(2.0, 8.0), ts=ts),
        _gauge("k8s.node.allocatable_memory", "By", rng.randint(8_000_000_000, 16_000_000_000), is_int=True, ts=ts),
        _gauge("k8s.node.condition_ready", "1", 1, is_int=True, ts=ts),
        _gauge("k8s.node.condition_memory_pressure", "1", 1 if rng.random() < 0.1 else 0, is_int=True, ts=ts),
        _gauge("k8s.node.condition_disk_pressure", "1", 1 if rng.random() < 0.05 else 0, is_int=True, ts=ts),
    ]


def _generate_node_metrics_kubelet(rng: random.Random) -> list:
    """Generate kubeletstatsreceiver-owned node metrics.

    kubeletstatsreceiver owns: cpu.usage, cpu.utilization, memory.*, filesystem.*, network.*.
    These metrics land in metrics-kubeletstatsreceiver.otel-* and are referenced by
    the k8s_otel Node dashboards (filesystem usage/capacity panels, etc.).
    """
    allocatable_cores = rng.uniform(2.0, 8.0)
    utilization = rng.uniform(0.1, 0.8)
    cpu_usage_ns = int(allocatable_cores * utilization * 10)
    fs_capacity = rng.randint(100_000_000_000, 200_000_000_000)
    fs_usage = rng.randint(20_000_000_000, 80_000_000_000)
    ts = _now_ns()

    return [
        _gauge("k8s.node.cpu.usage", "ns", cpu_usage_ns, is_int=True, ts=ts),
        _gauge("k8s.node.cpu.utilization", "1", utilization, ts=ts),
        _gauge("k8s.node.memory.usage", "By", rng.randint(2_000_000_000, 8_000_000_000), is_int=True, ts=ts),
        _gauge("k8s.node.memory.working_set", "By", rng.randint(1_500_000_000, 6_000_000_000), is_int=True, ts=ts),
        _gauge("k8s.node.memory.available", "By", rng.randint(1_000_000_000, 4_000_000_000), is_int=True, ts=ts),
        _gauge("k8s.node.memory.utilization", "1", rng.uniform(0.2, 0.7), ts=ts),
        _gauge("k8s.node.filesystem.usage", "By", fs_usage, is_int=True, ts=ts),
        _gauge("k8s.node.filesystem.capacity", "By", fs_capacity, is_int=True, ts=ts),
        _gauge("k8s.node.filesystem.available", "By", fs_capacity - fs_usage, is_int=True, ts=ts),
        _gauge("k8s.node.filesystem.utilization", "1", fs_usage / fs_capacity, ts=ts),
        _cumulative_sum("k8s.node.network.rx", "By", rng.randint(1_000_000_000, 10_000_000_000), ts=ts),
        _cumulative_sum("k8s.node.network.tx", "By", rng.randint(1_000_000_000, 10_000_000_000), ts=ts),
    ]


def _generate_deployment_metrics(rng: random.Random) -> list:
    """Generate deployment-level metrics."""
    ts = _now_ns()
    desired = rng.randint(2, 5)
    available = min(desired, rng.randint(1, desired))
    return [
        _gauge("k8s.deployment.desired", "1", desired, is_int=True, ts=ts),
        _gauge("k8s.deployment.available", "1", available, is_int=True, ts=ts),
    ]


# ── Additional workload resources + metrics for donut charts ─────────────────

# DaemonSets: 2 system-level daemonsets
DAEMONSETS = [f"{NAMESPACE}-log-collector", f"{NAMESPACE}-node-exporter"]
# StatefulSets: 2 stateful services
STATEFULSETS = [f"{NAMESPACE}-redis", f"{NAMESPACE}-postgres"]
# Jobs: 2 periodic batch jobs
JOBS = [f"{NAMESPACE}-db-backup", f"{NAMESPACE}-report-gen"]


def _build_daemonset_resource(ds_name: str, cluster: dict, namespace: str = NAMESPACE) -> dict:
    attrs = {
        "k8s.daemonset.name": ds_name,
        "k8s.namespace.name": namespace,
        "k8s.cluster.name": cluster["name"],
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "data_stream.type": "metrics",
        "data_stream.dataset": "k8sclusterreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _generate_daemonset_metrics(rng: random.Random, num_nodes: int) -> list:
    ts = _now_ns()
    desired = num_nodes
    ready = desired if rng.random() > 0.05 else desired - 1
    return [
        _gauge("k8s.daemonset.desired_scheduled_nodes", "1", desired, is_int=True, ts=ts),
        _gauge("k8s.daemonset.ready_nodes", "1", ready, is_int=True, ts=ts),
        _gauge("k8s.daemonset.current_scheduled_nodes", "1", desired, is_int=True, ts=ts),
    ]


def _build_statefulset_resource(ss_name: str, cluster: dict, namespace: str = NAMESPACE) -> dict:
    attrs = {
        "k8s.statefulset.name": ss_name,
        "k8s.namespace.name": namespace,
        "k8s.cluster.name": cluster["name"],
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "data_stream.type": "metrics",
        "data_stream.dataset": "k8sclusterreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _generate_statefulset_metrics(rng: random.Random) -> list:
    ts = _now_ns()
    desired = rng.randint(2, 3)
    ready = desired if rng.random() > 0.05 else desired - 1
    return [
        _gauge("k8s.statefulset.desired_pods", "1", desired, is_int=True, ts=ts),
        _gauge("k8s.statefulset.ready_pods", "1", ready, is_int=True, ts=ts),
        _gauge("k8s.statefulset.current_pods", "1", desired, is_int=True, ts=ts),
    ]


def _build_replicaset_resource(svc: str, pod_data: dict, cluster: dict, namespace: str = NAMESPACE) -> dict:
    p = pod_data["pods"][svc]
    attrs = {
        "k8s.replicaset.name": p["replicaset_name"],
        "k8s.namespace.name": namespace,
        "k8s.cluster.name": cluster["name"],
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "data_stream.type": "metrics",
        "data_stream.dataset": "k8sclusterreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _generate_replicaset_metrics(rng: random.Random) -> list:
    ts = _now_ns()
    desired = rng.randint(1, 5)
    available = min(desired, rng.randint(1, desired))
    return [
        _gauge("k8s.replicaset.desired", "1", desired, is_int=True, ts=ts),
        _gauge("k8s.replicaset.available", "1", available, is_int=True, ts=ts),
    ]


def _build_pod_phase_resource(svc: str, pod_data: dict, cluster: dict, namespace: str = NAMESPACE) -> dict:
    """Resource for pod-phase metrics (k8sclusterreceiver scope)."""
    p = pod_data["pods"][svc]
    attrs = {
        "k8s.pod.name": p["pod_name"],
        "k8s.pod.uid": p["pod_uid"],
        "k8s.namespace.name": namespace,
        "k8s.cluster.name": cluster["name"],
        "k8s.node.name": p["node_name"],
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "data_stream.type": "metrics",
        "data_stream.dataset": "k8sclusterreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _generate_pod_phase_metric(rng: random.Random) -> list:
    """Generate k8s.pod.phase gauge."""
    ts = _now_ns()
    # Phase values: 1=Pending, 2=Running, 3=Succeeded, 4=Failed, 5=Unknown
    phase = 2 if rng.random() > 0.05 else rng.choice([1, 3, 4])
    return [
        _gauge("k8s.pod.phase", "1", phase, is_int=True, ts=ts),
    ]


# ── Container resource (k8sclusterreceiver) ──────────────────────────────────

def _build_container_resource(svc: str, pod_data: dict, cluster: dict, namespace: str = NAMESPACE) -> dict:
    """Build OTLP resource for a container (k8sclusterreceiver).

    k8sclusterreceiver owns: k8s.container.restarts, cpu/memory requests+limits.
    k8s.container.status.last_terminated_reason is a resource attribute on this
    resource (it is disabled by default in the real receiver but is explicitly
    enabled in the edot-values.yaml kube-stack config).
    """
    p = pod_data["pods"][svc]
    attrs = {
        "k8s.container.name": f"{svc}-container",
        "k8s.pod.name": p["pod_name"],
        "k8s.pod.uid": p["pod_uid"],
        "k8s.namespace.name": namespace,
        "k8s.node.name": p["node_name"],
        "k8s.cluster.name": cluster["name"],
        "container.id": p["container_id"],
        "k8s.container.status.last_terminated_reason": "Completed",
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "data_stream.type": "metrics",
        "data_stream.dataset": "k8sclusterreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _generate_container_metrics(svc: str, state: K8sState, rng: random.Random) -> list:
    """Generate k8sclusterreceiver-owned container metrics.

    k8sclusterreceiver owns: restarts (gauge), cpu/memory request+limit.
    """
    ts = _now_ns()
    return [
        _cumulative_sum("k8s.container.restarts", "{restart}", state.restarts[svc], ts=ts),
        _gauge("k8s.container.cpu_request", "{cpu}", rng.uniform(0.1, 1.0), ts=ts),
        _gauge("k8s.container.cpu_limit", "{cpu}", rng.uniform(0.5, 2.0), ts=ts),
        _gauge("k8s.container.memory_request", "By", rng.randint(128 * 2**20, 512 * 2**20), is_int=True, ts=ts),
        _gauge("k8s.container.memory_limit", "By", rng.randint(256 * 2**20, 1024 * 2**20), is_int=True, ts=ts),
    ]


# ── Namespace resource (k8sclusterreceiver) ───────────────────────────────────

def _build_namespace_resource(namespace: str, cluster: dict) -> dict:
    """Build OTLP resource for a namespace (k8sclusterreceiver).

    k8sclusterreceiver owns k8s.namespace.phase.
    """
    attrs = {
        "k8s.namespace.name": namespace,
        "k8s.cluster.name": cluster["name"],
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "data_stream.type": "metrics",
        "data_stream.dataset": "k8sclusterreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _generate_namespace_metrics(rng: random.Random) -> list:
    """Generate k8s.namespace.phase gauge.

    1 = Active (the only real value for a running namespace).
    """
    ts = _now_ns()
    return [
        _gauge("k8s.namespace.phase", "1", 1, is_int=True, ts=ts),
    ]


# ── Job resource (k8sclusterreceiver) ─────────────────────────────────────────

def _build_job_resource(job_name: str, cluster: dict, namespace: str = NAMESPACE) -> dict:
    """Build OTLP resource for a k8s Job (k8sclusterreceiver)."""
    attrs = {
        "k8s.job.name": job_name,
        "k8s.namespace.name": namespace,
        "k8s.cluster.name": cluster["name"],
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "data_stream.type": "metrics",
        "data_stream.dataset": "k8sclusterreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _generate_job_metrics(rng: random.Random) -> list:
    """Generate k8sclusterreceiver-owned Job metrics."""
    ts = _now_ns()
    desired = 1
    successful = desired if rng.random() > 0.1 else 0
    active = 0 if successful else 1
    failed = 1 if rng.random() < 0.05 else 0
    return [
        _gauge("k8s.job.active_pods", "{pod}", active, is_int=True, ts=ts),
        _gauge("k8s.job.desired_successful_pods", "{pod}", desired, is_int=True, ts=ts),
        _gauge("k8s.job.successful_pods", "{pod}", successful, is_int=True, ts=ts),
        _gauge("k8s.job.failed_pods", "{pod}", failed, is_int=True, ts=ts),
        _gauge("k8s.job.max_parallel_pods", "{pod}", 1, is_int=True, ts=ts),
    ]


# ── K8s Warning Events (logs) ───────────────────────────────────────────────

WARNING_EVENTS = [
    {"reason": "FailedScheduling", "message": "0/3 nodes are available: 3 Insufficient memory."},
    {"reason": "Unhealthy", "message": "Readiness probe failed: HTTP probe failed with statuscode: 503"},
    {"reason": "BackOff", "message": "Back-off restarting failed container"},
    {"reason": "FailedMount", "message": "MountVolume.SetUp failed for volume \"pvc-data\": mount failed: exit status 32"},
    {"reason": "Failed", "message": "Error: container failed to start"},
]


def _generate_k8s_warning_logs(client: OTLPClient, pod_data: dict, cluster: dict, rng: random.Random, namespace: str = NAMESPACE) -> None:
    """Generate occasional K8s Warning event logs for the dashboard's Warning Events panel."""
    # Only emit ~20% of the time
    if rng.random() > 0.20:
        return

    svc = rng.choice(cluster["services"])
    p = pod_data["pods"][svc]
    evt = rng.choice(WARNING_EVENTS)
    now_ns = _now_ns()

    event_name = f"{p['pod_name']}.{secrets.token_hex(8)}"
    event_time_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    structured_body = {
        "object.kind": "Event",
        "object.type": "Warning",
        "object.reason": evt["reason"],
        "object.note": evt["message"],
        "object.regarding.kind": "Pod",
        "object.regarding.name": p["pod_name"],
        "object.regarding.namespace": namespace,
        "object.metadata.name": event_name,
        "object.metadata.namespace": namespace,
        "object.metadata.creationTimestamp": event_time_iso,
        "object.deprecatedSource.component": rng.choice(["kubelet", "scheduler", "controller-manager"]),
        "object.deprecatedSource.host": p["node_name"],
        "type": "MODIFIED",
    }

    object_kind = rng.choice(["Pod", "Node", "ReplicaSet", "Deployment"])

    log_record = {
        "timeUnixNano": now_ns,
        "severityText": "Warning",
        "severityNumber": 13,
        "body": {
            "kvlistValue": {
                "values": _format_attributes(structured_body),
            }
        },
        "attributes": [
            {"key": "event.name", "value": {"stringValue": event_name}},
            {"key": "event.domain", "value": {"stringValue": "k8s"}},
            {"key": "k8s.event.type", "value": {"stringValue": "Warning"}},
            {"key": "k8s.event.reason", "value": {"stringValue": evt["reason"]}},
            {"key": "k8s.event.start_time", "value": {"stringValue": event_time_iso}},
            {"key": "k8s.object.kind", "value": {"stringValue": object_kind}},
            {"key": "k8s.object.name", "value": {"stringValue": p["pod_name"]}},
            {"key": "k8s.event.object.kind", "value": {"stringValue": "Pod"}},
            {"key": "k8s.event.object.name", "value": {"stringValue": p["pod_name"]}},
            {"key": "k8s.event.object.namespace", "value": {"stringValue": namespace}},
            {"key": "k8s.namespace.name", "value": {"stringValue": namespace}},
        ],
    }

    resource_attrs = {
        "k8s.cluster.name": cluster["name"],
        "k8s.namespace.name": namespace,
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "cloud.region": cluster["region"],
        "data_stream.type": "logs",
        "data_stream.dataset": "k8seventsreceiver",
        "data_stream.namespace": "default",
    }

    payload = {
        "resourceLogs": [{
            "resource": {"attributes": _format_attributes(resource_attrs), "schemaUrl": SCHEMA_URL},
            "scopeLogs": [{
                "scope": {"name": K8S_OBJECTS_SCOPE, "version": SCOPE_VERSION},
                "logRecords": [log_record],
            }],
        }]
    }
    client._send(f"{client.endpoint}/v1/logs", payload, "k8s-events")


# ── Run loop ─────────────────────────────────────────────────────────────────

def _generate_oom_killed_log(client: OTLPClient, svc: str, pod_data: dict, cluster: dict, rng: random.Random, namespace: str = NAMESPACE) -> None:
    """Emit an OOMKilled event log for a targeted pod."""
    p = pod_data["pods"][svc]
    now_ns = _now_ns()
    event_name = f"{p['pod_name']}.oomkill.{secrets.token_hex(4)}"
    event_time_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    structured_body = {
        "object.kind": "Event",
        "object.type": "Warning",
        "object.reason": "OOMKilling",
        "object.note": f"Container {svc}-container in pod {p['pod_name']} was OOMKilled (memory limit exceeded)",
        "object.regarding.kind": "Pod",
        "object.regarding.name": p["pod_name"],
        "object.regarding.namespace": namespace,
        "object.metadata.name": event_name,
        "object.metadata.namespace": namespace,
        "object.metadata.creationTimestamp": event_time_iso,
        "object.deprecatedSource.component": "kubelet",
        "object.deprecatedSource.host": p["node_name"],
        "type": "MODIFIED",
    }

    log_record = {
        "timeUnixNano": now_ns,
        "severityText": "Warning",
        "severityNumber": 13,
        "body": {
            "kvlistValue": {
                "values": _format_attributes(structured_body),
            }
        },
        "attributes": [
            {"key": "event.name", "value": {"stringValue": event_name}},
            {"key": "event.domain", "value": {"stringValue": "k8s"}},
            {"key": "k8s.event.type", "value": {"stringValue": "Warning"}},
            {"key": "k8s.event.reason", "value": {"stringValue": "OOMKilling"}},
            {"key": "k8s.event.start_time", "value": {"stringValue": event_time_iso}},
            {"key": "k8s.object.kind", "value": {"stringValue": "Pod"}},
            {"key": "k8s.object.name", "value": {"stringValue": p["pod_name"]}},
            {"key": "k8s.event.object.kind", "value": {"stringValue": "Pod"}},
            {"key": "k8s.event.object.name", "value": {"stringValue": p["pod_name"]}},
            {"key": "k8s.event.object.namespace", "value": {"stringValue": namespace}},
            {"key": "k8s.namespace.name", "value": {"stringValue": namespace}},
        ],
    }

    resource_attrs = {
        "k8s.cluster.name": cluster["name"],
        "k8s.namespace.name": namespace,
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "cloud.region": cluster["region"],
        "data_stream.type": "logs",
        "data_stream.dataset": "k8seventsreceiver",
        "data_stream.namespace": "default",
    }

    payload = {
        "resourceLogs": [{
            "resource": {"attributes": _format_attributes(resource_attrs), "schemaUrl": SCHEMA_URL},
            "scopeLogs": [{
                "scope": {"name": K8S_OBJECTS_SCOPE, "version": SCOPE_VERSION},
                "logRecords": [log_record],
            }],
        }]
    }
    client._send(f"{client.endpoint}/v1/logs", payload, "k8s-oomkill-events")


def run(client: OTLPClient, stop_event: threading.Event, scenario_data: dict | None = None,
        chaos_controller=None) -> None:
    """Run K8s metrics generator loop until stop_event is set."""
    rng = random.Random()
    clusters = scenario_data["k8s_clusters"] if scenario_data else CLUSTERS
    # Prefer per-deployment namespace; fall back to module global for standalone mode.
    _namespace = scenario_data["namespace"] if scenario_data else NAMESPACE
    # Per-namespace workload names (avoid frozen module-global NAMESPACE).
    _daemonsets = [f"{_namespace}-log-collector", f"{_namespace}-node-exporter"]
    _statefulsets = [f"{_namespace}-redis", f"{_namespace}-postgres"]
    _jobs = [f"{_namespace}-db-backup", f"{_namespace}-report-gen"]

    # Build service -> cloud_provider mapping for targeted spikes
    _service_cloud: dict[str, str] = {}
    _channel_registry = {}
    if scenario_data:
        for svc_name, svc_cfg in scenario_data.get("services", {}).items():
            _service_cloud[svc_name] = svc_cfg.get("cloud_provider", "")
        _channel_registry = scenario_data.get("channel_registry", {})

    # Filter out database services — they're managed instances, not K8s deployments
    if scenario_data:
        db_services = {name for name, cfg in scenario_data.get("services", {}).items()
                       if cfg.get("subsystem") == "database"}
        for c in clusters:
            c["services"] = [s for s in c["services"] if s not in db_services]

    # Collect all service names across clusters for state tracking
    all_services = []
    for c in clusters:
        all_services.extend(c["services"])
    state = K8sState(rng, services=all_services)

    # Initialize per-cluster pod data — use shared topology when scenario_data is available
    # so pod/container IDs match the trace generator (enabling APM Service Map infra correlation)
    _infra_topology = None
    if scenario_data:
        from log_generators.infra_topology import build_topology as _build_infra_topology, to_pod_data as _to_pod_data
        _infra_topology = _build_infra_topology(scenario_data)

    cluster_data = []
    total_services = 0
    total_nodes = 0
    for idx, cluster in enumerate(clusters):
        if _infra_topology is not None:
            pod_data = _to_pod_data(_infra_topology, cluster)
        else:
            pod_data = _init_pod_data(cluster, seed_offset=idx, namespace=_namespace)
        cluster_data.append((cluster, pod_data))
        total_services += len(cluster["services"])
        total_nodes += len(pod_data["node_names"])

    logger.info("K8s metrics generator started (interval=%ds, clusters=%d, services=%d, nodes=%d)",
                METRICS_INTERVAL, len(clusters), total_services, total_nodes)

    scrape_count = 0
    while not stop_event.is_set():
        state.tick()
        resource_metrics = []

        # Determine OOM spike targets from chaos_controller
        spikes = chaos_controller.get_infra_spikes() if chaos_controller else {}
        oom_intensity = spikes.get("k8s_oom_intensity", 0)

        # Build set of spiked services (affected by active faults)
        spiked_services: set[str] = set()
        has_active_faults = False
        if chaos_controller and oom_intensity > 0:
            active_channels = chaos_controller.get_active_channels()
            if active_channels:
                has_active_faults = True
                for ch_id in active_channels:
                    ch = _channel_registry.get(ch_id, {})
                    spiked_services.update(ch.get("affected_services", []))

        for cluster, pod_data in cluster_data:
            svcs = cluster["services"]

            # ── Pod-level metrics (kubeletstatsreceiver) ─────────────────────
            # One resource per service; carries pod cpu/memory/network/filesystem
            # and the two kubelet-owned container metrics (cpu.usage, memory.working_set).
            for svc in svcs:
                # Check if this service should be OOM-spiked
                is_spiked = oom_intensity > 0 and (not has_active_faults or svc in spiked_services)
                intensity_ratio = oom_intensity / 100.0

                pod_res = _build_pod_resource(svc, pod_data, cluster, _namespace)
                pod_metrics = _generate_pod_metrics(svc, state, rng)

                if is_spiked:
                    # Override memory utilization for spiked pods
                    for m in pod_metrics:
                        if m["name"] == "k8s.pod.memory_limit_utilization":
                            m["gauge"]["dataPoints"][0]["asDouble"] = (
                                rng.uniform(0.92, 1.0) * intensity_ratio
                                + (1 - intensity_ratio) * rng.uniform(0.25, 0.85)
                            )

                resource_metrics.append({
                    "resource": pod_res,
                    "scopeMetrics": [{"scope": {"name": KUBELET_SCOPE, "version": SCOPE_VERSION}, "metrics": pod_metrics}],
                })

                # ── Container-level metrics (k8sclusterreceiver) ─────────────
                # Separate resource for k8sclusterreceiver-owned container fields:
                # restarts, cpu/memory requests+limits, last_terminated_reason attr.
                container_res = _build_container_resource(svc, pod_data, cluster, _namespace)
                container_metrics = _generate_container_metrics(svc, state, rng)

                if is_spiked:
                    # Increase restart probability: baseline 5% → up to 75%
                    restart_chance = 0.05 + 0.70 * intensity_ratio
                    if rng.random() < restart_chance:
                        state.restarts[svc] += 1
                        for m in container_metrics:
                            if m["name"] == "k8s.container.restarts":
                                m["sum"]["dataPoints"][0]["asInt"] = str(state.restarts[svc])

                resource_metrics.append({
                    "resource": container_res,
                    "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": container_metrics}],
                })

                # Emit OOMKilled event log for spiked pods (probabilistic)
                if is_spiked and rng.random() < 0.15 * intensity_ratio:
                    _generate_oom_killed_log(client, svc, pod_data, cluster, rng, _namespace)

            # Determine if any node in this cluster has spiked services
            cluster_has_spike = oom_intensity > 0 and (not has_active_faults or any(s in spiked_services for s in svcs))

            # ── Node-level metrics — split across two receivers ───────────────
            for node_name in pod_data["node_names"]:
                # kubeletstatsreceiver: cpu.usage, memory.*, filesystem.*, network.*
                kubelet_node_res = _build_node_resource_kubelet(node_name, pod_data, cluster)
                kubelet_node_metrics = _generate_node_metrics_kubelet(rng)
                resource_metrics.append({
                    "resource": kubelet_node_res,
                    "scopeMetrics": [{"scope": {"name": KUBELET_SCOPE, "version": SCOPE_VERSION}, "metrics": kubelet_node_metrics}],
                })

                # k8sclusterreceiver: allocatable_*, condition_*
                cluster_node_res = _build_node_resource(node_name, pod_data, cluster)
                cluster_node_metrics = _generate_node_metrics_cluster(rng)

                if cluster_has_spike:
                    # Set memory_pressure condition on affected nodes
                    for m in cluster_node_metrics:
                        if m["name"] == "k8s.node.condition_memory_pressure":
                            m["gauge"]["dataPoints"][0]["asInt"] = str(1)

                resource_metrics.append({
                    "resource": cluster_node_res,
                    "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": cluster_node_metrics}],
                })

            # ── Deployment-level metrics ──────────────────────────────────────
            for svc in svcs:
                dep_res = _build_deployment_resource(svc, pod_data, cluster, _namespace)
                metrics = _generate_deployment_metrics(rng)
                resource_metrics.append({
                    "resource": dep_res,
                    "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": metrics}],
                })

            # ── DaemonSet metrics ─────────────────────────────────────────────
            num_nodes = len(pod_data["node_names"])
            for ds_name in _daemonsets:
                ds_res = _build_daemonset_resource(ds_name, cluster, _namespace)
                metrics = _generate_daemonset_metrics(rng, num_nodes)
                resource_metrics.append({
                    "resource": ds_res,
                    "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": metrics}],
                })

            # ── StatefulSet metrics ───────────────────────────────────────────
            for ss_name in _statefulsets:
                ss_res = _build_statefulset_resource(ss_name, cluster, _namespace)
                metrics = _generate_statefulset_metrics(rng)
                resource_metrics.append({
                    "resource": ss_res,
                    "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": metrics}],
                })

            # ── ReplicaSet metrics ────────────────────────────────────────────
            for svc in svcs:
                rs_res = _build_replicaset_resource(svc, pod_data, cluster, _namespace)
                metrics = _generate_replicaset_metrics(rng)
                resource_metrics.append({
                    "resource": rs_res,
                    "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": metrics}],
                })

            # ── Pod phase metrics ─────────────────────────────────────────────
            for svc in svcs:
                phase_res = _build_pod_phase_resource(svc, pod_data, cluster, _namespace)
                metrics = _generate_pod_phase_metric(rng)
                resource_metrics.append({
                    "resource": phase_res,
                    "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": metrics}],
                })

            # ── Namespace metrics ─────────────────────────────────────────────
            ns_res = _build_namespace_resource(_namespace, cluster)
            ns_metrics = _generate_namespace_metrics(rng)
            resource_metrics.append({
                "resource": ns_res,
                "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": ns_metrics}],
            })

            # ── Job metrics ───────────────────────────────────────────────────
            for job_name in _jobs:
                job_res = _build_job_resource(job_name, cluster, _namespace)
                metrics = _generate_job_metrics(rng)
                resource_metrics.append({
                    "resource": job_res,
                    "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": metrics}],
                })

        payload = {"resourceMetrics": resource_metrics}
        client._send(f"{client.endpoint}/v1/metrics", payload, "k8s-metrics")

        # Occasional K8s warning event logs (pick a random cluster)
        cluster, pod_data = rng.choice(cluster_data)
        _generate_k8s_warning_logs(client, pod_data, cluster, rng, _namespace)

        scrape_count += 1
        if scrape_count % 4 == 0:
            logger.info("K8s metrics scrape %d complete", scrape_count)

        stop_event.wait(METRICS_INTERVAL)

    logger.info("K8s metrics generator stopped after %d scrapes", scrape_count)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = OTLPClient()
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    duration = int(os.environ.get("RUN_DURATION", "60"))
    timer = threading.Timer(duration, stop_event.set)
    timer.daemon = True
    timer.start()
    logger.info("Running for %ds (standalone)", duration)
    run(client, stop_event)
    timer.cancel()
    client.close()


if __name__ == "__main__":
    main()
