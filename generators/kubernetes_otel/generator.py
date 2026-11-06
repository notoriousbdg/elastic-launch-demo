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
    python3 -m generators.kubernetes_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/kubernetes_otel@2.6.1"

import logging
import os
import random
import secrets
import signal
import threading
import time
import uuid
from datetime import datetime, timezone

from app.telemetry import OTLPClient, ESBulkClient, _format_attributes, SCHEMA_URL, _now_ns
from app.config import ACTIVE_SCENARIO, NAMESPACE
from generator_engine.db_common import impact_value, resolve_active_chaos as _resolve_active_chaos
from generator_engine.db_common import stable_counter_start_ns
from generator_engine.scrape_clock import scrape_now_ns
from typing import Any

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

# First-cluster fallback for warning event logs when multi-cluster config is absent.
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
    # Same node-name sequence as generator_engine.infra_topology.build_topology.
    _ident = ScenarioIdentity(namespace)
    node_names = [_ident.k8s_node(region, i, stable) for i in range(3)]

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


def _build_profiles(cluster_data: list, ns: str) -> dict:
    """Build stable resource profiles for all services, nodes, and volumes.

    All limits / capacities are drawn once from a deterministic seed so they
    stay constant across scrapes. Enforces request <= limit for CPU and memory,
    gives each mountpoint a distinct total, and gives each volume its own capacity.

    Returns a dict with keys 'svc', 'node', 'vol'.
    """
    stable = random.Random(f"{ns}:resource-profiles")

    svc_profiles: dict[str, dict] = {}
    node_profiles: dict[str, dict] = {}
    vol_capacities: dict[str, int] = {}

    for cluster, pod_data in cluster_data:
        for svc in cluster["services"]:
            if svc not in svc_profiles:
                cpu_req = stable.uniform(0.1, 1.0)
                cpu_lim = cpu_req * stable.uniform(1.5, 3.0)
                mem_req = stable.randint(128 * 2**20, 512 * 2**20)
                mem_lim = int(mem_req * stable.uniform(1.5, 3.0))
                svc_profiles[svc] = {
                    "cpu_request": cpu_req,
                    "cpu_limit": cpu_lim,
                    "memory_request": mem_req,
                    "memory_limit": mem_lim,
                }
            for vol_suffix in ["data-pvc", "config-vol"]:
                key = f"{svc}-{vol_suffix}"
                if key not in vol_capacities:
                    vol_capacities[key] = stable.randint(10_000_000_000, 50_000_000_000)

        for node_name in pod_data["node_names"]:
            if node_name not in node_profiles:
                alloc_cpu = stable.uniform(2.0, 8.0)
                alloc_mem = stable.randint(8_000_000_000, 32_000_000_000)
                fs_cap = stable.randint(100_000_000_000, 200_000_000_000)
                node_profiles[node_name] = {
                    "allocatable_cpu": alloc_cpu,
                    "allocatable_memory": alloc_mem,
                    "fs_capacity": fs_cap,
                }

    # Stable replica counts and HPA ceilings. A separate RNG keeps the draws above
    # unchanged. Previously deployment `desired` and the HPA `max` were re-randomized
    # on every scrape, so counts could never hold a fault state.
    wl = random.Random(f"{ns}:workload-profiles")
    replicas = {svc: wl.randint(2, 5) for svc in svc_profiles}
    hpa_max = {svc: n + wl.randint(2, 3) for svc, n in replicas.items()}

    return {
        "svc": svc_profiles, "node": node_profiles, "vol": vol_capacities,
        "replicas": replicas, "hpa_max": hpa_max,
    }


class K8sState:
    """Tracks cumulative counters and slow-walk fill levels per entity.

    Disk fill levels (node and volume) are walked in tick() with a small upward bias
    so that disk-filling charts show a realistic ramp rather than per-scrape noise.
    Fault injection sets a *target* fill level; tick() ramps toward it at ~2%/cycle.
    """

    def __init__(
        self,
        rng: random.Random,
        services: list[str] | None = None,
        node_names: list[str] | None = None,
        vol_keys: list[str] | None = None,
    ):
        self._rng = rng
        self._services = services or list(SERVICES)
        self.net_rx = {svc: rng.randint(50_000_000, 100_000_000) for svc in self._services}
        self.net_tx = {svc: rng.randint(70_000_000, 120_000_000) for svc in self._services}
        self.restarts = {svc: 0 for svc in self._services}
        # Cumulative node network bytes (k8s.node.network.io, per direction); monotonic so
        # the Node Detail SUM(RATE(...)) panel has a rate to draw.
        self.node_net_rx = {n: rng.randint(1_000_000_000, 10_000_000_000) for n in (node_names or [])}
        self.node_net_tx = {n: rng.randint(1_000_000_000, 10_000_000_000) for n in (node_names or [])}
        # Cumulative container major page faults (kubeletstatsreceiver). The
        # Namespaces dashboard "Top 10 by OOM events" panel uses
        # MAX(faults)-MIN(faults) per time bucket as an OOM proxy.
        self.major_page_faults = {svc: rng.randint(0, 100) for svc in self._services}

        # Disk fill levels (0.0–1.0) per node and volume — distinct baselines,
        # walked slowly in tick() to avoid the identical-bytes tell.
        self.node_fs_fill: dict[str, float] = {
            n: rng.uniform(0.25, 0.60) for n in (node_names or [])
        }
        self.vol_fill: dict[str, float] = {
            k: rng.uniform(0.10, 0.50) for k in (vol_keys or [])
        }
        # Fault targets — None = free walk, float = ramp toward this value
        self.node_fs_target: dict[str, float | None] = {}
        self.vol_target: dict[str, float | None] = {}

    def tick(self):
        rng = self._rng
        for svc in self._services:
            self.net_rx[svc] += rng.randint(10_000, 100_000)
            self.net_tx[svc] += rng.randint(15_000, 120_000)
            # Quiet baseline — real OOM shows up as a sharp jump on top of this.
            # Restarts stay at 0 unless chaos raises container_restart_rate
            # (CrashLoopBackOff alert fires on any restarts > 0).
            self.major_page_faults[svc] += rng.randint(0, 2)

        for node in self.node_net_rx:
            self.node_net_rx[node] += rng.randint(100_000, 2_000_000)
            self.node_net_tx[node] += rng.randint(100_000, 2_000_000)

        # Advance node filesystem fill levels.
        # Baseline: zero-mean random walk capped well under the 0.85 disk-
        # pressure / saturation alert floor (0.70) so it never drifts into
        # breach territory on its own. Fault target: ramp at up to 2%/cycle.
        for node in list(self.node_fs_fill.keys()):
            target = self.node_fs_target.get(node)
            if target is not None:
                diff = target - self.node_fs_fill[node]
                step = min(abs(diff), 0.02) * (1.0 if diff > 0 else -1.0)
                self.node_fs_fill[node] = max(0.01, min(0.99, self.node_fs_fill[node] + step))
            else:
                self.node_fs_fill[node] = max(0.01, min(0.70,
                    self.node_fs_fill[node] + rng.uniform(-0.006, 0.006)))

        # Advance volume fill levels (same logic, slightly slower baseline drift).
        for key in list(self.vol_fill.keys()):
            target = self.vol_target.get(key)
            if target is not None:
                diff = target - self.vol_fill[key]
                step = min(abs(diff), 0.02) * (1.0 if diff > 0 else -1.0)
                self.vol_fill[key] = max(0.01, min(0.99, self.vol_fill[key] + step))
            else:
                self.vol_fill[key] = max(0.01, min(0.70,
                    self.vol_fill[key] + rng.uniform(-0.004, 0.004)))


# Chaos UI "channel augmentation" timings. NotReady cascade: the node's pods go Unknown
# after the kubelet stops reporting, then are evicted after the default 5 minute toleration.
NODE_UNKNOWN_AFTER_S = 40.0
NODE_EVICT_AFTER_S = 300.0
CRASH_BACKOFF_START_S = 10.0
CRASH_BACKOFF_MAX_S = 300.0
# Disk fill at which one pod on the node is evicted (disk pressure itself is at 0.85).
EVICT_FILL = 0.90
# Pod phase codes (k8sclusterreceiver k8s.pod.phase)
PHASE_PENDING, PHASE_RUNNING, PHASE_SUCCEEDED, PHASE_FAILED, PHASE_UNKNOWN = 1, 2, 3, 4, 5


def _not_ready_stage(elapsed: float | None, scheduling_blocked: bool) -> str:
    """Stage of the node-NotReady cascade: ready | down | unknown | pending | rescheduled.

    ``elapsed`` is seconds since the node went NotReady (None = node is fine). After
    NODE_EVICT_AFTER_S the evicted pods are rescheduled on the healthy nodes, or stay
    Pending when those nodes have no headroom (``scheduling_blocked``).
    """
    if elapsed is None:
        return "ready"
    if elapsed < NODE_UNKNOWN_AFTER_S:
        return "down"
    if elapsed < NODE_EVICT_AFTER_S:
        return "unknown"
    return "pending" if scheduling_blocked else "rescheduled"


class _Augment:
    """State the Chaos UI channel augmentations need to carry across scrapes."""

    def __init__(self) -> None:
        self.crash: dict[str, dict[str, float]] = {}  # svc -> {"next_at", "backoff"}
        self.hpa_cur: dict[str, int] = {}             # svc -> current replicas while capped
        self.nr_node: str | None = None               # node held NotReady
        self.nr_since: float | None = None
        self.nr_evicted: set[str] = set()             # svcs whose eviction event was logged

    def crash_restart_due(self, svc: str, now: float) -> bool:
        """Kubelet restart back-off: 10s, 20s, 40s ... capped at 5m. True on a restart."""
        st = self.crash.setdefault(svc, {"next_at": now, "backoff": CRASH_BACKOFF_START_S})
        if now < st["next_at"]:
            return False
        st["next_at"] = now + st["backoff"]
        st["backoff"] = min(CRASH_BACKOFF_MAX_S, st["backoff"] * 2)
        return True


def _gauge(name: str, unit: str, value, is_int: bool = False, attributes: dict | None = None, ts: str | None = None) -> dict:
    now = ts or scrape_now_ns()
    dp: dict = {"timeUnixNano": now}
    if is_int:
        dp["asInt"] = str(int(value))
    else:
        dp["asDouble"] = float(value)
    if attributes:
        dp["attributes"] = _format_attributes(attributes)
    return {"name": name, "unit": unit, "gauge": {"dataPoints": [dp]}}


def _cumulative_sum(name: str, unit: str, value, is_int: bool = True, attributes: dict | None = None, ts: str | None = None) -> dict:
    now = ts or scrape_now_ns()
    # A stable start keeps one counter series across ticks (GUARDRAILS #23).
    dp: dict = {"startTimeUnixNano": stable_counter_start_ns(), "timeUnixNano": now}
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


def _resource_attr_map(resource: dict) -> dict[str, Any]:
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


def _gauge_value(metric: dict) -> float | int | None:
    dps = (metric.get("gauge") or {}).get("dataPoints") or []
    if not dps:
        return None
    dp = dps[0]
    if "asDouble" in dp:
        return float(dp["asDouble"])
    if "asInt" in dp:
        return int(dp["asInt"])
    return None


def build_node_util_doc(
    resource: dict,
    *,
    cpu_util: float,
    mem_util: float,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """ES bulk companion for Systems Ops CPU/Mem tiles when OTLP fails."""
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "1",
        "data_stream": {
            "type": "metrics",
            "dataset": "kubeletstatsreceiver.otel",
            "namespace": "default",
        },
        "resource": {"attributes": _resource_attr_map(resource)},
        "metrics": {
            "k8s.node.cpu.utilization": float(cpu_util),
            "k8s.node.memory.utilization": float(mem_util),
        },
    }


def build_pod_memory_doc(
    resource: dict,
    mem_usage: int,
    *,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "By",
        "data_stream": {
            "type": "metrics",
            "dataset": "kubeletstatsreceiver.otel",
            "namespace": "default",
        },
        "resource": {"attributes": _resource_attr_map(resource)},
        "metrics": {"k8s.pod.memory.usage": int(mem_usage)},
    }


def build_container_status_doc(
    resource: dict,
    restarts: int,
    *,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """ES bulk companion for OOMKilled / CrashLoopBackOff when OTLP batches fail.

    OOTB ``[Kubernetes OTel] OOMKilled containers`` queries
    ``metrics-k8sclusterreceiver.otel-*`` filtering
    ``k8s.container.status.last_terminated_reason == "OOMKilled"``. That reason
    lives on the container resource; dual-write it with ``k8s.container.restarts``
    so the alert still works when the giant OTLP ``resourceMetrics`` scrape is
    dropped or back-pressured.
    """
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "{restart}",
        "data_stream": {
            "type": "metrics",
            "dataset": "k8sclusterreceiver.otel",
            "namespace": "default",
        },
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION},
        "metrics": {"k8s.container.restarts": int(restarts)},
    }


def build_pod_util_doc(
    resource: dict,
    *,
    cpu_node_util: float | None = None,
    mem_working_set: int | None = None,
    cpu_limit_util: float | None = None,
    mem_limit_util: float | None = None,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """ES bulk companion for pod CPU/memory fields used by ML and OOTB alerts."""
    metrics: dict[str, Any] = {}
    if cpu_node_util is not None:
        metrics["k8s.pod.cpu.node.utilization"] = float(cpu_node_util)
    if mem_working_set is not None:
        metrics["k8s.pod.memory.working_set"] = int(mem_working_set)
    if cpu_limit_util is not None:
        metrics["k8s.container.cpu_limit_utilization"] = float(cpu_limit_util)
    if mem_limit_util is not None:
        metrics["k8s.container.memory_limit_utilization"] = float(mem_limit_util)
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "data_stream": {
            "type": "metrics",
            "dataset": "kubeletstatsreceiver.otel",
            "namespace": "default",
        },
        "resource": {"attributes": _resource_attr_map(resource)},
        "metrics": metrics,
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
        # Both names: real kubeletstats emits container.name; the Namespaces
        # OOM panel filters on k8s.container.name and drops rows where it is null.
        "container.name": f"{svc}-container",
        "k8s.container.name": f"{svc}-container",
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


def _generate_pod_metrics(
    svc: str,
    state: K8sState,
    rng: random.Random,
    profiles: dict | None = None,
    mem_util_override: float | None = None,
    cpu_util_override: float | None = None,
    container_name: str | None = None,
) -> list:
    """Generate pod + kubelet-owned container metrics for one service.

    ``svc`` keys the profile and K8sState counters; StatefulSet / DaemonSet pods pass
    their pod key plus ``container_name`` (default ``<svc>-container``).

    kubeletstatsreceiver owns: pod cpu/memory/network/filesystem,
    and per-container cpu.usage + memory.working_set + *_limit_utilization.
    k8sclusterreceiver-owned container metrics (restarts, requests, limits)
    are emitted separately via _generate_container_metrics.

    All memory metrics are derived from a single utilization draw so that
    the invariant rss <= working_set <= usage <= limit always holds, and
    memory.available == limit - usage.  CPU utilization is the sole driver
    of cpu.usage and cpu_limit_utilization (they stay consistent across scrapes).
    """
    metrics = []
    ts = scrape_now_ns()  # shared timestamp — all metrics go into one TSDB document

    prof = (profiles or {}).get("svc", {}).get(svc, {})
    cpu_limit = prof.get("cpu_limit", 1.0)
    mem_limit = prof.get("memory_limit", 512 * 2**20)

    # CPU: healthy stays under 0.9 throttling alert; chaos can override.
    cpu_util = (
        float(cpu_util_override)
        if cpu_util_override is not None
        else rng.uniform(0.05, 0.80)
    )
    cpu_util = min(1.0, max(0.0, cpu_util))
    # k8s.pod.cpu.usage: Elastic's OTLP ingest normalizes the OTel nanosecond
    # counter to cores before storage. The OOTB dashboard divides avg_usage by
    # avg_limit (both in cores) to get utilization (0–1). Send as gauge in cores.
    cpu_usage_cores = cpu_limit * cpu_util
    metrics.append(_gauge("k8s.pod.cpu.usage", "1", round(cpu_usage_cores, 6), ts=ts))
    metrics.append(_gauge("k8s.pod.cpu_limit_utilization", "1", round(cpu_util, 6), ts=ts))
    # Track limit util closely so pod ML (cpu.node.utilization) sees the same intensity.
    metrics.append(_gauge("k8s.pod.cpu.node.utilization", "1", round(cpu_util, 6), ts=ts))

    # Memory: draw one utilization (or accept an override from the OOM spike path),
    # then derive the full chain: usage → working_set → rss → available.
    # Invariant: rss <= working_set <= usage <= limit; available = limit - usage.
    # Healthy stays under 0.9 near-limit alert.
    mem_util = mem_util_override if mem_util_override is not None else rng.uniform(0.25, 0.80)
    mem_util = min(1.0, max(0.0, mem_util))
    mem_usage = int(mem_limit * mem_util)
    mem_working_set = int(mem_usage * rng.uniform(0.85, 0.98))
    mem_rss = int(mem_working_set * rng.uniform(0.70, 0.95))
    mem_available = max(0, mem_limit - mem_usage)
    metrics.append(_gauge("k8s.pod.memory.usage", "By", mem_usage, is_int=True, ts=ts))
    metrics.append(_gauge("k8s.pod.memory_limit_utilization", "1", round(mem_util, 6), ts=ts))
    metrics.append(_gauge("k8s.pod.memory.node.utilization", "1", round(mem_util * 0.04, 6), ts=ts))
    metrics.append(_gauge("k8s.pod.memory.working_set", "By", mem_working_set, is_int=True, ts=ts))
    metrics.append(_gauge("k8s.pod.memory.rss", "By", mem_rss, is_int=True, ts=ts))
    metrics.append(_gauge("k8s.pod.memory.available", "By", mem_available, is_int=True, ts=ts))

    # Pod Network (cumulative) — dashboard queries k8s.pod.network.io with
    # direction attribute; emit two time series, one per direction.
    metrics.append(_cumulative_sum("k8s.pod.network.io", "By", state.net_rx[svc],
                                   attributes={"direction": "receive"}, ts=ts))
    metrics.append(_cumulative_sum("k8s.pod.network.io", "By", state.net_tx[svc],
                                   attributes={"direction": "transmit"}, ts=ts))

    # Pod Filesystem (no capacity metric in kubeletstats; express as raw bytes)
    metrics.append(_gauge("k8s.pod.filesystem.usage", "By", rng.randint(100_000_000, 500_000_000), is_int=True, ts=ts))

    # Container metrics owned by kubeletstatsreceiver.
    # Include k8s.container.name — the Namespaces "Top 10 by OOM events" panel
    # filters on that field (container.name alone is not enough).
    # cpu.usage and memory.working_set on the container match the pod values since
    # each pod is modelled as single-container here.
    _cname = container_name or f"{svc}-container"
    container_attrs = {
        "container.name": _cname,
        "k8s.container.name": _cname,
    }
    # kubeletstatsreceiver names these container.* (no k8s. prefix); Pod Detail reads them so.
    metrics.append(_gauge("container.cpu.usage", "1", round(cpu_usage_cores, 6), attributes=container_attrs, ts=ts))
    metrics.append(_gauge(
        "k8s.container.cpu_limit_utilization", "1", round(cpu_util, 6),
        attributes=container_attrs, ts=ts,
    ))
    metrics.append(_gauge("container.memory.working_set", "By", mem_working_set, is_int=True, attributes=container_attrs, ts=ts))
    metrics.append(_gauge(
        "k8s.container.memory_limit_utilization", "1", round(mem_util, 6),
        attributes=container_attrs, ts=ts,
    ))
    # Cumulative counter so MAX-MIN per time bucket reflects new faults.
    metrics.append(_cumulative_sum(
        "container.memory.major_page_faults", "1",
        state.major_page_faults[svc],
        attributes=container_attrs, ts=ts,
    ))

    return metrics


def _generate_node_metrics_cluster(
    rng: random.Random,
    *,
    condition_ready: int = 1,
    memory_pressure: int = 0,
    disk_pressure: int = 0,
    allocatable_cpu: float | None = None,
    allocatable_memory: int | None = None,
) -> list:
    """Generate k8sclusterreceiver-owned node metrics.

    k8sclusterreceiver owns: allocatable_*, condition_*.
    Healthy baseline: ready=1, no pressure conditions.
    """
    ts = scrape_now_ns()
    return [
        _gauge(
            "k8s.node.allocatable_cpu", "1",
            allocatable_cpu if allocatable_cpu is not None else rng.uniform(2.0, 8.0),
            ts=ts,
        ),
        _gauge(
            "k8s.node.allocatable_memory", "By",
            allocatable_memory if allocatable_memory is not None else rng.randint(8_000_000_000, 16_000_000_000),
            is_int=True, ts=ts,
        ),
        _gauge("k8s.node.condition_ready", "1", condition_ready, is_int=True, ts=ts),
        _gauge("k8s.node.condition_memory_pressure", "1", memory_pressure, is_int=True, ts=ts),
        _gauge("k8s.node.condition_disk_pressure", "1", disk_pressure, is_int=True, ts=ts),
    ]


def _generate_node_metrics_kubelet(
    rng: random.Random,
    node_name: str | None = None,
    profiles: dict | None = None,
    state: "K8sState | None" = None,
    cpu_util_override: float | None = None,
    mem_util_override: float | None = None,
) -> list:
    """Generate kubeletstatsreceiver-owned node metrics.

    kubeletstatsreceiver owns: cpu.usage, cpu.utilization, memory.*, filesystem.*, network.*.
    These metrics land in metrics-kubeletstatsreceiver.otel-* and are referenced by
    the k8s_otel Node dashboards (filesystem usage/capacity panels, etc.).

    Coherence guarantees:
    - fs_capacity is stable across scrapes (from profile, not re-drawn)
    - fs_available == fs_capacity - fs_usage
    - fs_utilization == fs_usage / fs_capacity
    - memory.available == allocatable_memory - memory.usage
    - memory.working_set <= memory.usage
    """
    ts = scrape_now_ns()
    node_prof = (profiles or {}).get("node", {}).get(node_name or "", {})
    allocatable_cores = node_prof.get("allocatable_cpu", 4.0)
    allocatable_mem = node_prof.get("allocatable_memory", 16_000_000_000)
    fs_cap = node_prof.get("fs_capacity", 150_000_000_000)

    # CPU: OOTB "Node CPU saturation" alert fires on avg_cpu (cores) > 3.6,
    # not a ratio. allocatable_cores varies per node (2.0-8.0), so a fixed
    # healthy utilization ratio can still push larger nodes' absolute core
    # usage over the threshold. Cap the healthy core count directly.
    utilization = (
        float(cpu_util_override)
        if cpu_util_override is not None
        else rng.uniform(0.1, 0.75)
    )
    utilization = min(1.0, max(0.0, utilization))
    if cpu_util_override is None:
        max_healthy_cores = 2.5
        utilization = min(utilization, max_healthy_cores / max(allocatable_cores, 0.1))
    # OOTB "Node CPU saturation" does avg_cpu / allocatable_cpu > 0.9 — both
    # must be in cores (not nanoseconds) for the ratio to be meaningful.
    cpu_usage_cores = allocatable_cores * utilization

    # Memory: OOTB "Node memory saturation" alert fires on avg_memory (bytes)
    # > 13743895347 (~12.8 GiB), not a ratio. allocatable_mem varies per node
    # (8-32 GiB), so cap the healthy byte usage directly, same as CPU above.
    mem_util = (
        float(mem_util_override)
        if mem_util_override is not None
        else rng.uniform(0.2, 0.70)
    )
    mem_util = min(1.0, max(0.0, mem_util))
    if mem_util_override is None:
        max_healthy_mem_bytes = 10_000_000_000
        mem_util = min(mem_util, max_healthy_mem_bytes / max(allocatable_mem, 1))
    # OOTB Node memory saturation uses working_set / allocatable (>0.85).
    # When a slider/chaos override is set, set working_set from the same util
    # fraction so slider % lines up with the alert ratio.
    if mem_util_override is not None:
        mem_working_set = int(allocatable_mem * mem_util)
        mem_usage = max(mem_working_set, int(mem_working_set * rng.uniform(1.0, 1.03)))
    else:
        mem_usage = int(allocatable_mem * mem_util)
        mem_working_set = int(mem_usage * rng.uniform(0.85, 0.97))
    mem_available = max(0, allocatable_mem - mem_usage)
    mem_rss = int(mem_working_set * rng.uniform(0.70, 0.95))  # rss <= working_set <= usage

    # Network: cumulative bytes per direction, grown in K8sState.tick() so RATE is positive.
    if state is not None and node_name and node_name in state.node_net_rx:
        net_rx, net_tx = state.node_net_rx[node_name], state.node_net_tx[node_name]
    else:
        net_rx = rng.randint(1_000_000_000, 10_000_000_000)
        net_tx = rng.randint(1_000_000_000, 10_000_000_000)

    # Filesystem: use slow-walk fill from state; derive available and utilization
    if state is not None and node_name and node_name in state.node_fs_fill:
        fill = state.node_fs_fill[node_name]
    else:
        fill = rng.uniform(0.25, 0.60)
    fs_usage = int(fs_cap * fill)
    fs_available = max(0, fs_cap - fs_usage)

    return [
        _gauge("k8s.node.cpu.usage", "1", round(cpu_usage_cores, 6), ts=ts),
        _gauge("k8s.node.cpu.utilization", "1", round(utilization, 6), ts=ts),
        _gauge("k8s.node.memory.usage", "By", mem_usage, is_int=True, ts=ts),
        _gauge("k8s.node.memory.working_set", "By", mem_working_set, is_int=True, ts=ts),
        _gauge("k8s.node.memory.rss", "By", mem_rss, is_int=True, ts=ts),
        _gauge("k8s.node.memory.available", "By", mem_available, is_int=True, ts=ts),
        _gauge("k8s.node.memory.utilization", "1", round(mem_util, 6), ts=ts),
        _gauge("k8s.node.filesystem.usage", "By", fs_usage, is_int=True, ts=ts),
        _gauge("k8s.node.filesystem.capacity", "By", fs_cap, is_int=True, ts=ts),
        _gauge("k8s.node.filesystem.available", "By", fs_available, is_int=True, ts=ts),
        _gauge("k8s.node.filesystem.utilization", "1", round(fill, 6), ts=ts),
        # Node Detail reads k8s.node.network.io with a direction attribute (not .rx/.tx).
        _cumulative_sum("k8s.node.network.io", "By", net_rx, attributes={"direction": "receive"}, ts=ts),
        _cumulative_sum("k8s.node.network.io", "By", net_tx, attributes={"direction": "transmit"}, ts=ts),
    ]


def _generate_deployment_metrics(
    rng: random.Random,
    ready_fraction: float | None = None,
    desired: int | None = None,
    unavailable: int = 0,
) -> list:
    """Generate deployment-level metrics.

    ``desired`` is the stable replica count from the workload profile (random when
    omitted). ``unavailable`` removes that many replicas from ``available``.
    """
    ts = scrape_now_ns()
    desired = desired if desired is not None else rng.randint(2, 5)
    if ready_fraction is None or ready_fraction >= 1.0:
        available = desired
    else:
        available = max(0, int(desired * ready_fraction))
        if available >= desired and desired > 0:
            available = desired - 1
    if unavailable > 0:
        available = max(0, min(available, desired - unavailable))
    return [
        _gauge("k8s.deployment.desired", "1", desired, is_int=True, ts=ts),
        _gauge("k8s.deployment.available", "1", available, is_int=True, ts=ts),
    ]


# ── Additional workload resources + metrics for donut charts ─────────────────

# Non-Deployment workloads: (role, container name, public image, cpu limit cores,
# memory limit bytes). Object names come from ScenarioIdentity.label(role) in run().
# StatefulSets: a cache and a database, N ordinal pods each (<name>-0, <name>-1 ...).
STATEFULSET_ROLES: tuple[tuple[str, str, str, float, int], ...] = (
    ("redis", "redis", "redis:7.2", 1.0, 1 * 2**30),
    ("postgres", "postgres", "postgres:16.4", 2.0, 4 * 2**30),
)
# DaemonSets: node agents, one pod per node.
DAEMONSET_ROLES: tuple[tuple[str, str, str, float, int], ...] = (
    ("log-collector", "fluent-bit", "fluent/fluent-bit:3.1", 0.5, 256 * 2**20),
    ("node-exporter", "node-exporter", "prom/node-exporter:v1.8.2", 0.25, 128 * 2**20),
)
# Characters Kubernetes uses for generated pod-name suffixes.
_K8S_SUFFIX_CHARS = "bcdfghjklmnpqrstvwxz2456789"


def _statefulset_replicas(names: list[str], namespace: str) -> dict[str, int]:
    """Stable desired replica count per StatefulSet (drawn once per namespace)."""
    wl = random.Random(f"{namespace}:statefulset-replicas")
    return {name: wl.randint(2, 3) for name in names}


def _init_workload_pods(
    cluster: dict,
    pod_data: dict,
    namespace: str,
    statefulsets: list[str],
    daemonsets: list[str],
    ss_replicas: dict[str, int],
) -> list[dict]:
    """Stable StatefulSet and DaemonSet pods for one cluster.

    StatefulSet pods are ``<statefulset>-<ordinal>`` spread over the nodes; DaemonSet
    pods run once on every node. Each dict carries the pod identity plus a stable
    resource profile (limits/requests), keyed by ``key`` for K8sState counters.
    """
    stable = random.Random(f"{namespace}:workload-pods:{cluster['name']}")
    nodes = sorted(pod_data["node_names"])
    node_uids: dict[str, str] = {}
    for svc in cluster["services"]:
        p = pod_data["pods"].get(svc)
        if p and p["node_name"] not in node_uids:
            node_uids[p["node_name"]] = p["node_uid"]
    for n in nodes:
        node_uids.setdefault(n, str(uuid.UUID(int=stable.getrandbits(128))))

    def _pod(kind: str, owner: str, pod_name: str, node: str, role: tuple) -> dict:
        _role, container, image, cpu_lim, mem_lim = role
        cpu_req = cpu_lim * stable.uniform(0.3, 0.6)
        mem_req = int(mem_lim * stable.uniform(0.4, 0.7))
        return {
            "key": f"{cluster['name']}/{pod_name}",
            "kind": kind,
            "owner": owner,
            "pod_name": pod_name,
            "pod_uid": str(uuid.UUID(int=stable.getrandbits(128))),
            "pod_ip": f"10.{stable.randint(100, 120)}.{stable.randint(1, 10)}.{stable.randint(2, 250)}",
            "node_name": node,
            "node_uid": node_uids[node],
            "container_name": container,
            "container_id": f"containerd://{stable.getrandbits(256):064x}",
            "image": image,
            "profile": {
                "cpu_request": cpu_req, "cpu_limit": cpu_lim,
                "memory_request": mem_req, "memory_limit": mem_lim,
            },
        }

    pods: list[dict] = []
    for name, role in zip(statefulsets, STATEFULSET_ROLES):
        for ordinal in range(ss_replicas.get(name, 2)):
            node = nodes[(ordinal + stable.randrange(len(nodes))) % len(nodes)] if nodes else ""
            pods.append(_pod("statefulset", name, f"{name}-{ordinal}", node, role))
    for name, role in zip(daemonsets, DAEMONSET_ROLES):
        for node in nodes:
            suffix = "".join(stable.choice(_K8S_SUFFIX_CHARS) for _ in range(5))
            pods.append(_pod("daemonset", name, f"{name}-{suffix}", node, role))
    return pods


def _build_workload_pod_resource(wp: dict, cluster: dict, namespace: str = NAMESPACE) -> dict:
    """kubeletstatsreceiver resource for a StatefulSet / DaemonSet pod."""
    attrs = {
        "k8s.namespace.name": namespace,
        f"k8s.{wp['kind']}.name": wp["owner"],
        "k8s.node.name": wp["node_name"],
        "k8s.node.uid": wp["node_uid"],
        "k8s.pod.name": wp["pod_name"],
        "k8s.pod.ip": wp["pod_ip"],
        "k8s.pod.uid": wp["pod_uid"],
        "k8s.cluster.name": cluster["name"],
        "container.name": wp["container_name"],
        "k8s.container.name": wp["container_name"],
        "container.id": wp["container_id"],
        "container.image.name": wp["image"],
        "host.name": wp["node_name"],
        "host.architecture": "amd64",
        "os.type": "linux",
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "cloud.region": cluster["region"],
        "data_stream.type": "metrics",
        "data_stream.dataset": "kubeletstatsreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _build_workload_cluster_resource(
    wp: dict, cluster: dict, namespace: str = NAMESPACE, *, with_container: bool = False,
) -> dict:
    """k8sclusterreceiver resource for a StatefulSet / DaemonSet pod (phase) or its container."""
    attrs = {
        "k8s.pod.name": wp["pod_name"],
        "k8s.pod.uid": wp["pod_uid"],
        "k8s.namespace.name": namespace,
        f"k8s.{wp['kind']}.name": wp["owner"],
        "k8s.node.name": wp["node_name"],
        "k8s.cluster.name": cluster["name"],
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "data_stream.type": "metrics",
        "data_stream.dataset": "k8sclusterreceiver",
        "data_stream.namespace": "default",
    }
    if with_container:
        attrs.update({
            "k8s.container.name": wp["container_name"],
            "container.id": wp["container_id"],
            "container.image.name": wp["image"],
            "k8s.container.status.last_terminated_reason": "Completed",
        })
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


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


def _generate_daemonset_metrics(
    rng: random.Random,
    num_nodes: int,
    *,
    ready_fraction: float | None = None,
    misscheduled: int = 0,
) -> list:
    ts = scrape_now_ns()
    desired = num_nodes
    if ready_fraction is None or ready_fraction >= 1.0:
        ready = desired
    else:
        ready = max(0, int(desired * ready_fraction))
        if ready >= desired and desired > 0:
            ready = desired - 1
    return [
        _gauge("k8s.daemonset.desired_scheduled_nodes", "1", desired, is_int=True, ts=ts),
        _gauge("k8s.daemonset.ready_nodes", "1", ready, is_int=True, ts=ts),
        _gauge("k8s.daemonset.current_scheduled_nodes", "1", desired, is_int=True, ts=ts),
        _gauge("k8s.daemonset.misscheduled_nodes", "1", max(0, misscheduled), is_int=True, ts=ts),
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


def _statefulset_ready(desired: int, ready_fraction: float | None) -> int:
    """Ready pods for a StatefulSet: all of them unless a fault sets a fraction."""
    if ready_fraction is None or ready_fraction >= 1.0:
        return desired
    ready = max(0, int(desired * ready_fraction))
    if ready >= desired and desired > 0:
        ready = desired - 1
    return ready


def _generate_statefulset_metrics(
    rng: random.Random,
    ready_fraction: float | None = None,
    desired: int | None = None,
) -> list:
    """StatefulSet replica gauges. ``desired`` is the stable replica count (the
    number of ordinal pods emitted); random only when omitted."""
    ts = scrape_now_ns()
    desired = desired if desired is not None else rng.randint(2, 3)
    ready = _statefulset_ready(desired, ready_fraction)
    return [
        _gauge("k8s.statefulset.desired_pods", "1", desired, is_int=True, ts=ts),
        _gauge("k8s.statefulset.ready_pods", "1", ready, is_int=True, ts=ts),
        _gauge("k8s.statefulset.current_pods", "1", desired, is_int=True, ts=ts),
        _gauge("k8s.statefulset.updated_pods", "1", desired, is_int=True, ts=ts),
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
    ts = scrape_now_ns()
    desired = rng.randint(1, 5)
    # Healthy: fully available (no package alert, but keep coherent).
    available = desired
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


def _generate_pod_phase_metric(
    rng: random.Random,
    ready_fraction: float | None = None,
    phase_override: int | None = None,
) -> list:
    """Generate k8s.pod.phase gauge.

    Parameters
    ----------
    ready_fraction:
        When set (0-1), overrides the default Running probability.
        Used by generic ready-fraction impact keys.
    phase_override:
        Force a specific phase (1=Pending, 4=Failed). Required for OOTB alerts
        that need MAX(phase)==1 (Pending) or VALUES containing 4 (Failed).
    """
    ts = scrape_now_ns()
    # Phase values: 1=Pending, 2=Running, 3=Succeeded, 4=Failed, 5=Unknown
    if phase_override is not None:
        phase = int(phase_override)
    elif ready_fraction is not None:
        phase = 2 if rng.random() < ready_fraction else rng.choice([1, 4])
    else:
        # Healthy baseline: always Running so Pending/Failed alerts stay quiet.
        phase = 2
    return [
        _gauge("k8s.pod.phase", "1", phase, is_int=True, ts=ts),
    ]


# ── Container resource (k8sclusterreceiver) ──────────────────────────────────

def _build_job_pod_phase_resource(
    job_name: str,
    cluster: dict,
    namespace: str = NAMESPACE,
) -> dict:
    """Pod-phase resource for a completed Job pod (k8sclusterreceiver).

    Overview "Top 10 clusters by non-running pods" keeps only buckets with
    ``non_running_pods > 0``. Healthy service pods stay Running (phase=2) so
    Pending/Failed alerts stay quiet; emit one Succeeded (phase=3) pod per Job
    instead — non-running without tripping those alerts.
    """
    stable = random.Random(f"{namespace}:job-pod:{cluster['name']}:{job_name}")
    pod_hex = f"{stable.getrandbits(32):08x}"
    attrs = {
        "k8s.pod.name": f"{job_name}-{pod_hex}",
        "k8s.pod.uid": str(uuid.UUID(int=stable.getrandbits(128))),
        "k8s.namespace.name": namespace,
        "k8s.cluster.name": cluster["name"],
        "k8s.node.name": ScenarioIdentity(namespace).k8s_node(cluster["region"], 0, stable),
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "data_stream.type": "metrics",
        "data_stream.dataset": "k8sclusterreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _generate_succeeded_pod_phase_metric() -> list:
    """Always-Succeeded pod phase for Job breadcrumb pods."""
    return [
        _gauge("k8s.pod.phase", "1", 3, is_int=True, ts=scrape_now_ns()),  # Succeeded
    ]


def _build_container_resource(
    svc: str,
    pod_data: dict,
    cluster: dict,
    namespace: str = NAMESPACE,
    last_terminated_reason: str = "Completed",
) -> dict:
    """Build OTLP resource for a container (k8sclusterreceiver).

    k8sclusterreceiver owns: k8s.container.restarts, cpu/memory requests+limits.
    k8s.container.status.last_terminated_reason is a resource attribute on this
    resource (it is disabled by default in the real receiver but is explicitly
    enabled in the edot-values.yaml kube-stack config).

    The Kubernetes OTel Overview dashboard's "Top 10 namespaces by OOM events"
    panel filters on last_terminated_reason == \"OOMKilled\", so chaos OOM
    spikes must set this attribute — event logs alone are not enough.
    """
    p = pod_data["pods"][svc]
    attrs = {
        "k8s.container.name": f"{svc}-container",
        "k8s.pod.name": p["pod_name"],
        "k8s.pod.uid": p["pod_uid"],
        # Overview/Workloads "Top 10 deployments by container restarts" filters
        # on k8s.deployment.name IS NOT NULL on the same docs as restarts.
        "k8s.deployment.name": p["deployment_name"],
        "k8s.replicaset.name": p["replicaset_name"],
        "k8s.namespace.name": namespace,
        "k8s.node.name": p["node_name"],
        "k8s.cluster.name": cluster["name"],
        "container.id": p["container_id"],
        "container.image.name": f"{svc}:latest",
        "k8s.container.status.last_terminated_reason": last_terminated_reason,
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "data_stream.type": "metrics",
        "data_stream.dataset": "k8sclusterreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _generate_container_metrics(
    svc: str,
    state: K8sState,
    rng: random.Random,
    profiles: dict | None = None,
    ready: bool = True,
) -> list:
    """Generate k8sclusterreceiver-owned container metrics.

    k8sclusterreceiver owns: restarts (gauge), ready (gauge, 1/0), cpu/memory request+limit.
    Requests and limits are stable (from profiles) — they represent static pod spec
    and must not jitter between scrapes. request <= limit is enforced at profile build.
    """
    ts = scrape_now_ns()
    prof = (profiles or {}).get("svc", {}).get(svc, {})
    cpu_req = prof.get("cpu_request", 0.25)
    cpu_lim = prof.get("cpu_limit", 1.0)
    mem_req = prof.get("memory_request", 256 * 2**20)
    mem_lim = prof.get("memory_limit", 512 * 2**20)
    return [
        _gauge("k8s.container.restarts", "{restart}", state.restarts[svc], is_int=True, ts=ts),
        _gauge("k8s.container.ready", "1", 1 if ready else 0, is_int=True, ts=ts),
        _gauge("k8s.container.cpu_request", "{cpu}", round(cpu_req, 4), ts=ts),
        _gauge("k8s.container.cpu_limit", "{cpu}", round(cpu_lim, 4), ts=ts),
        _gauge("k8s.container.memory_request", "By", mem_req, is_int=True, ts=ts),
        _gauge("k8s.container.memory_limit", "By", mem_lim, is_int=True, ts=ts),
    ]


# ── Volume resource (kubeletstatsreceiver) ────────────────────────────────────

def _build_volume_resource(svc: str, vol_name: str, pod_data: dict, cluster: dict, namespace: str = NAMESPACE) -> dict:
    """Build OTLP resource for a pod volume (kubeletstatsreceiver).

    k8s.volume.name is a resource attribute — the real receiver creates one
    resource per volume, enabling the dashboard to filter by volume name.
    """
    p = pod_data["pods"][svc]
    attrs = {
        "k8s.volume.name": vol_name,
        "k8s.volume.type": "persistentVolumeClaim",
        "k8s.pod.name": p["pod_name"],
        "k8s.pod.uid": p["pod_uid"],
        "k8s.namespace.name": namespace,
        "k8s.node.name": p["node_name"],
        "k8s.cluster.name": cluster["name"],
        "k8s.deployment.name": p.get("deployment_name", f"{svc}-deployment"),
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "cloud.region": cluster["region"],
        "data_stream.type": "metrics",
        "data_stream.dataset": "kubeletstatsreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _generate_volume_metrics(
    rng: random.Random,
    capacity: int | None = None,
    fill: float | None = None,
) -> list:
    """Generate kubeletstatsreceiver volume capacity / available metrics.

    Coherence guarantees:
    - capacity is stable across scrapes (passed from the once-built profile)
    - available == capacity - used  (derived, not independently drawn)
    - fill (used/capacity ratio) comes from the slow-walk state
    """
    ts = scrape_now_ns()
    cap = capacity if capacity is not None else rng.randint(10_000_000_000, 50_000_000_000)
    f = fill if fill is not None else rng.uniform(0.10, 0.50)
    f = min(1.0, max(0.0, f))
    used = int(cap * f)
    available = max(0, cap - used)
    return [
        _gauge("k8s.volume.capacity", "By", cap, is_int=True, ts=ts),
        _gauge("k8s.volume.available", "By", available, is_int=True, ts=ts),
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
    ts = scrape_now_ns()
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


def _generate_job_metrics(rng: random.Random, failed_pods: int = 0) -> list:
    """Generate k8sclusterreceiver-owned Job metrics."""
    ts = scrape_now_ns()
    desired = 1
    failed = max(0, int(failed_pods))
    successful = 0 if failed > 0 else desired
    active = 0 if successful else 1
    return [
        _gauge("k8s.job.active_pods", "{pod}", active, is_int=True, ts=ts),
        _gauge("k8s.job.desired_successful_pods", "{pod}", desired, is_int=True, ts=ts),
        _gauge("k8s.job.successful_pods", "{pod}", successful, is_int=True, ts=ts),
        _gauge("k8s.job.failed_pods", "{pod}", failed, is_int=True, ts=ts),
        _gauge("k8s.job.max_parallel_pods", "{pod}", 1, is_int=True, ts=ts),
    ]


def _build_hpa_resource(hpa_name: str, cluster: dict, namespace: str = NAMESPACE) -> dict:
    attrs = {
        "k8s.hpa.name": hpa_name,
        "k8s.namespace.name": namespace,
        "k8s.cluster.name": cluster["name"],
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "data_stream.type": "metrics",
        "data_stream.dataset": "k8sclusterreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _generate_hpa_metrics(
    rng: random.Random,
    at_max: bool = False,
    max_replicas: int | None = None,
    current: int | None = None,
) -> list:
    """HPA metrics. ``max_replicas`` / ``current`` come from the stable workload profile
    (random when omitted); ``current`` also fixes ``desired`` so the HPA agrees with the
    deployment it scales."""
    ts = scrape_now_ns()
    max_replicas = max_replicas if max_replicas is not None else rng.randint(5, 20)
    if at_max:
        desired = max_replicas
    elif current is not None:
        desired = max(1, min(current, max_replicas))
    else:
        desired = rng.randint(1, max(1, max_replicas - 1))
    return [
        _gauge("k8s.hpa.max_replicas", "1", max_replicas, is_int=True, ts=ts),
        _gauge("k8s.hpa.min_replicas", "1", 1, is_int=True, ts=ts),
        _gauge("k8s.hpa.desired_replicas", "1", desired, is_int=True, ts=ts),
        _gauge("k8s.hpa.current_replicas", "1", desired, is_int=True, ts=ts),
    ]


# ── K8s Warning Events (logs) ───────────────────────────────────────────────

WARNING_EVENTS = [
    {"reason": "FailedScheduling", "message": "0/3 nodes are available: 3 Insufficient memory."},
    {"reason": "Unhealthy", "message": "Readiness probe failed: HTTP probe failed with statuscode: 503"},
    {"reason": "BackOff", "message": "Back-off restarting failed container"},
    {"reason": "FailedMount", "message": "MountVolume.SetUp failed for volume \"pvc-data\": mount failed: exit status 32"},
    {"reason": "Failed", "message": "Error: container failed to start"},
    # --- Extended catalog for infra / OpenShift scenarios ---
    # Port conflict / CrashLoopBackOff after bind failure
    {"reason": "BackOff", "message": "Back-off restarting failed container; last exit code 1 (bind: address already in use :8443)"},
    # OOMKill
    {"reason": "OOMKilling", "message": "Memory limit exceeded: container was OOM killed (memory limit exceeded). Limit: 512Mi"},
    # Eviction
    {"reason": "Evicted", "message": "The node was low on resource: ephemeral-storage. Threshold quantity: 10%, available: 4932372Ki."},
    # Image pull failure
    {"reason": "ErrImagePull", "message": "rpc error: code = Unknown desc = failed to pull image: authentication required"},
    {"reason": "ImagePullBackOff", "message": "Back-off pulling image; waiting for next pull attempt"},
    # Node conditions
    {"reason": "NodeHasDiskPressure", "message": "Node disk usage is above threshold, pods will not be scheduled to this node."},
    {"reason": "NodeNotReady", "message": "Node controller detected node is not Ready; kubelet stopped posting node status."},
    # OpenShift SecurityContextConstraints denial (FailedCreate reason, SCC vocabulary)
    {"reason": "FailedCreate", "message": "Error creating: pods \"app-\" is forbidden: unable to validate against any security context constraint: [provider \"restricted\": Forbidden: not usable by user or serviceaccount; spec.containers[0].securityContext.runAsUser: Invalid value: 0: must be in the ranges: [1000620000, 1000629999]]"},
]


def _build_k8s_event_log_record(
    *,
    pod_name: str,
    node_name: str,
    namespace: str,
    reason: str,
    message: str,
    object_kind: str = "Pod",
    event_name: str | None = None,
    source_component: str = "kubelet",
    object_name: str | None = None,
) -> tuple[dict, str]:
    """Build one OTLP log record for Cluster Detail ``Cluster logs``.

    That panel KEEP's ``body.*``, ``k8s.pod.name``, and ``k8s.node.name``.
    Emitting only a structured ``body.kvlistValue`` leaves ``body.text`` null,
    and using ``k8s.event.object.name`` alone leaves pod/node columns empty.
    """
    now_ns = _now_ns()
    event_time_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    event_name = event_name or f"{pod_name}.{secrets.token_hex(8)}"

    log_record = {
        "timeUnixNano": now_ns,
        "severityText": "Warning",
        "severityNumber": 13,
        # Cluster logs KEEP body.* — stringValue maps to body.text.
        "body": {"stringValue": message},
        "attributes": [
            {"key": "event.name", "value": {"stringValue": event_name}},
            {"key": "event.domain", "value": {"stringValue": "k8s"}},
            {"key": "k8s.event.type", "value": {"stringValue": "Warning"}},
            {"key": "k8s.event.reason", "value": {"stringValue": reason}},
            {"key": "k8s.event.start_time", "value": {"stringValue": event_time_iso}},
            {"key": "k8s.object.kind", "value": {"stringValue": object_kind}},
            {"key": "k8s.object.name", "value": {"stringValue": object_name or pod_name}},
            {"key": "k8s.event.object.kind", "value": {"stringValue": object_kind if object_name else "Pod"}},
            {"key": "k8s.event.object.name", "value": {"stringValue": object_name or pod_name}},
            {"key": "k8s.event.object.namespace", "value": {"stringValue": namespace}},
            {"key": "k8s.namespace.name", "value": {"stringValue": namespace}},
            # Cluster logs KEEP columns (flattened log attributes).
            {"key": "k8s.pod.name", "value": {"stringValue": pod_name}},
            {"key": "k8s.node.name", "value": {"stringValue": node_name}},
            {"key": "k8s.event.message", "value": {"stringValue": message}},
            {"key": "k8s.event.note", "value": {"stringValue": message}},
            {"key": "k8s.event.source.component", "value": {"stringValue": source_component}},
            {"key": "k8s.event.source.host", "value": {"stringValue": node_name}},
        ],
    }
    return log_record, event_name


def _k8s_event_resource_attrs(cluster: dict, namespace: str, *, pod_name: str, node_name: str) -> dict:
    return {
        "k8s.cluster.name": cluster["name"],
        "k8s.namespace.name": namespace,
        "k8s.pod.name": pod_name,
        "k8s.node.name": node_name,
        "cloud.provider": cluster["provider"],
        "cloud.platform": cluster["platform"],
        "cloud.region": cluster["region"],
        "data_stream.type": "logs",
        "data_stream.dataset": "k8seventsreceiver",
        "data_stream.namespace": "default",
    }


def _generate_k8s_warning_logs(
    client: OTLPClient,
    pod_data: dict,
    cluster: dict,
    rng: random.Random,
    namespace: str = NAMESPACE,
    event_reason: str | None = None,
    event_rate: float | None = None,
) -> None:
    """Generate occasional K8s Warning event logs for the dashboard's Warning Events panel.

    Parameters
    ----------
    event_reason:
        When set, pins the ``k8s.event.reason`` field to this value instead of
        picking randomly from WARNING_EVENTS.  Used by ``warning_event_reason``
        impact key.
    event_rate:
        Probability that an event is emitted (0-1).  Defaults to 0.20.
        Used by ``warning_event_rate`` impact key.
    """
    gate = event_rate if event_rate is not None else 0.20
    if rng.random() > gate:
        return

    svc = rng.choice(cluster["services"])
    p = pod_data["pods"][svc]
    if event_reason is not None:
        # Pick from WARNING_EVENTS matching the pinned reason, or synthesize one.
        candidates = [e for e in WARNING_EVENTS if e["reason"] == event_reason]
        evt = rng.choice(candidates) if candidates else {
            "reason": event_reason,
            "message": f"{event_reason}: event generated by impact key",
        }
    else:
        evt = rng.choice(WARNING_EVENTS)

    _send_k8s_event(
        client, cluster, namespace,
        pod_name=p["pod_name"], node_name=p["node_name"],
        reason=evt["reason"], message=evt["message"],
        object_kind=rng.choice(["Pod", "Node", "ReplicaSet", "Deployment"]),
        source_component=rng.choice(["kubelet", "scheduler", "controller-manager"]),
    )


def _send_k8s_event(
    client: OTLPClient,
    cluster: dict,
    namespace: str,
    *,
    pod_name: str,
    node_name: str,
    reason: str,
    message: str,
    object_kind: str = "Pod",
    object_name: str | None = None,
    source_component: str = "kubelet",
) -> None:
    """Emit one k8sobjectsreceiver Warning event.

    ``pod_name``/``node_name`` fill the Cluster logs pod and node columns;
    ``object_name`` is the object the event is about when that is not the pod
    (a Node, an HPA, a DaemonSet).
    """
    log_record, _event_name = _build_k8s_event_log_record(
        pod_name=pod_name,
        node_name=node_name,
        namespace=namespace,
        reason=reason,
        message=message,
        object_kind=object_kind,
        object_name=object_name,
        source_component=source_component,
    )
    resource_attrs = _k8s_event_resource_attrs(
        cluster, namespace, pod_name=pod_name, node_name=node_name,
    )
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
    message = (
        f"Container {svc}-container in pod {p['pod_name']} was OOMKilled "
        f"(memory limit exceeded)"
    )
    log_record, _event_name = _build_k8s_event_log_record(
        pod_name=p["pod_name"],
        node_name=p["node_name"],
        namespace=namespace,
        reason="OOMKilling",
        message=message,
        object_kind="Pod",
        event_name=f"{p['pod_name']}.oomkill.{secrets.token_hex(4)}",
        source_component="kubelet",
    )

    resource_attrs = _k8s_event_resource_attrs(
        cluster, namespace, pod_name=p["pod_name"], node_name=p["node_name"],
    )

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



from generators.kubernetes_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run K8s metrics generator loop until stop_event is set."""
    rng = random.Random()
    clusters = scenario_data["k8s_clusters"] if scenario_data else CLUSTERS
    # Prefer per-deployment namespace; fall back to module global for standalone mode.
    _namespace = scenario_data["namespace"] if scenario_data else NAMESPACE
    # Per-namespace workload names (avoid frozen module-global NAMESPACE).
    _ident = ScenarioIdentity(_namespace)
    _daemonsets = [_ident.label(role[0]) for role in DAEMONSET_ROLES]
    _statefulsets = [_ident.label(role[0]) for role in STATEFULSET_ROLES]
    _jobs = [_ident.label("db-backup"), _ident.label("report-gen")]
    _ss_replicas = _statefulset_replicas(_statefulsets, _namespace)

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

    # Initialize per-cluster pod data — use shared topology when scenario_data is available
    # so pod/container IDs match the trace generator (enabling APM Service Map infra correlation)
    _infra_topology = None
    if scenario_data:
        from generator_engine.infra_topology import build_topology as _build_infra_topology, to_pod_data as _to_pod_data
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

    # Build stable resource profiles (requests/limits/capacities, drawn once)
    _profiles = _build_profiles(cluster_data, _namespace)

    # StatefulSet / DaemonSet pods per cluster (ML k8s_workload_memory_anomaly splits
    # pod memory by deployment, statefulset and daemonset). Their profiles join the
    # service profiles so the pod-metric path is shared.
    _workload_pods: dict[str, list[dict]] = {}
    for cluster, pod_data in cluster_data:
        _wps = _init_workload_pods(
            cluster, pod_data, _namespace, _statefulsets, _daemonsets, _ss_replicas,
        )
        _workload_pods[cluster["name"]] = _wps
        for _wp in _wps:
            _profiles["svc"][_wp["key"]] = _wp["profile"]

    # Collect all service names, node names, and volume keys for state initialisation
    all_services: list[str] = []
    all_node_names: list[str] = []
    all_vol_keys: list[str] = []
    for cluster, pod_data in cluster_data:
        for svc in cluster["services"]:
            if svc not in all_services:
                all_services.append(svc)
            for vol_suffix in ["data-pvc", "config-vol"]:
                key = f"{svc}-{vol_suffix}"
                if key not in all_vol_keys:
                    all_vol_keys.append(key)
        for node_name in pod_data["node_names"]:
            if node_name not in all_node_names:
                all_node_names.append(node_name)

    _wp_keys = [wp["key"] for wps in _workload_pods.values() for wp in wps]
    state = K8sState(
        rng, services=all_services + _wp_keys, node_names=all_node_names, vol_keys=all_vol_keys,
    )
    aug = _Augment()
    # service -> (cluster, pod_data) for scoping Chaos UI augmentations to the pods they hit
    _svc_pod: dict[str, tuple[dict, dict]] = {}
    for _c, _pd in cluster_data:
        for _svc in _c["services"]:
            _svc_pod.setdefault(_svc, (_c, _pd))
    _replicas = _profiles.get("replicas", {})
    _hpa_max = _profiles.get("hpa_max", {})

    logger.info("K8s metrics generator started (interval=%ds, clusters=%d, services=%d, nodes=%d)",
                METRICS_INTERVAL, len(clusters), total_services, total_nodes)

    scrape_count = 0
    es_bulk = ESBulkClient()
    def _emit_tick() -> None:
        nonlocal cluster, node_name, pod_data, scrape_count, svc, vol_suffix
        resource_metrics = []
        es_bulk_docs: list[dict[str, Any]] = []
        es_cluster_bulk_docs: list[dict[str, Any]] = []
        ts_ms = int(time.time() * 1000)

        # Infra spike sliders (chaos UI) — 1:1 util intensity for host/pod CPU/mem;
        # OOM is a duty-cycle for OOMKilled emission (presence-based alert).
        spikes = chaos_controller.get_infra_spikes() if chaos_controller else {}
        host_cpu_pct = float(spikes.get("cpu_pct", 0) or 0)
        host_mem_pct = float(spikes.get("memory_pct", 0) or 0)
        pod_cpu_pct = float(spikes.get("k8s_pod_cpu_intensity", 0) or 0)
        pod_mem_pct = float(spikes.get("k8s_pod_memory_intensity", 0) or 0)
        oom_intensity = float(spikes.get("k8s_oom_intensity", 0) or 0)
        any_infra_spike = (
            host_cpu_pct > 0
            or host_mem_pct > 0
            or pod_cpu_pct > 0
            or pod_mem_pct > 0
            or oom_intensity > 0
        )

        # ── Channel augmentations (Chaos UI) ─────────────────────────────────
        # Unlike the six sliders above, these only act on the active channels' services
        # (and the nodes they run on), so they are scoped even with no channel active.
        now = time.time()
        aug_node_disk = float(spikes.get("node_disk_fill_pct", 0) or 0)
        aug_node_down = float(spikes.get("k8s_node_not_ready", 0) or 0) > 0
        aug_pvc = float(spikes.get("k8s_pvc_fill_pct", 0) or 0)
        aug_crash = float(spikes.get("k8s_crashloop", 0) or 0) > 0
        aug_ready_fail = float(spikes.get("k8s_readiness_fail", 0) or 0) > 0
        aug_live_fail = float(spikes.get("k8s_liveness_fail", 0) or 0) > 0
        aug_hpa = float(spikes.get("k8s_hpa_capped", 0) or 0) > 0
        aug_ds_miss = int(float(spikes.get("k8s_daemonset_misscheduled", 0) or 0))

        aug_services: set[str] = set()
        aug_target_node: str | None = None
        _active_ids = list(chaos_controller.get_active_channels()) if chaos_controller else []
        if _active_ids and _channel_registry:
            try:
                _status = chaos_controller.get_status()
                _active_ids.sort(key=lambda i: ((_status.get(i) or {}).get("triggered_at") or 0, i))
            except Exception:
                _active_ids.sort()
            for _ch_id in _active_ids:
                for _svc in _channel_registry.get(_ch_id, {}).get("affected_services", []):
                    if _svc in _svc_pod:
                        aug_services.add(_svc)
                        if aug_target_node is None:
                            _c, _pd = _svc_pod[_svc]
                            aug_target_node = _pd["pods"][_svc]["node_name"]
        aug_nodes: set[str] = {
            _svc_pod[svc][1]["pods"][svc]["node_name"] for svc in aug_services
        }

        # Exactly one node goes NotReady: the one hosting the oldest channel's first service.
        if aug_node_down and aug_target_node:
            if aug.nr_node != aug_target_node or aug.nr_since is None:
                aug.nr_node, aug.nr_since = aug_target_node, now
                aug.nr_evicted.clear()
            nr_elapsed: float | None = now - aug.nr_since
        else:
            aug.nr_node = aug.nr_since = None
            aug.nr_evicted.clear()
            nr_elapsed = None
        # Evicted pods reschedule onto the other nodes unless a host slider says they're full.
        nr_stage = _not_ready_stage(nr_elapsed, host_cpu_pct >= 80 or host_mem_pct >= 80)

        # Events that explain the fault: (cluster, pod, kind, object, reason, message, component)
        aug_events: list[tuple[dict, dict, str, str | None, str, str, str]] = []
        aug_pod: dict[str, dict] = {}  # per-service fault state, read by the later loops

        # Generator-level impact keys from generator_impact: blocks in channel YAML.
        # Resolved separately from infra spikes so each key has a clean default path.
        _active_chaos: dict | None = None
        if chaos_controller and _channel_registry:
            from generators.kubernetes_otel import SPEC as _K8S_SPEC
            # explicit: only channels that declare generator_impact.kubernetes_otel
            # (GT packs for other packages must not skew k8s healthy baselines).
            _active_chaos = _resolve_active_chaos(
                chaos_controller, _channel_registry, "kubernetes_otel",
                chaos_mode="explicit", impact_keys=_K8S_SPEC.impact_keys,
            )
        # Extract per-cycle impact values from SPEC alert-slug keys.
        # container_cpu_throttling drives cpu_limit_utilization (>0.9), NOT restarts.
        _chaos_container_cpu: float | None = impact_value(_active_chaos, "container_cpu_throttling")
        _chaos_restart_rate: float | None = impact_value(_active_chaos, "pod_crashloopbackoff")
        # Ready-fraction overrides only when replica keys are actively >0.
        # Phase keys (Failed/Pending) use explicit phase_override below — do not
        # fold them into ready_fraction (Pending needs MAX(phase)==1 exclusively).
        #
        # resolve_active_chaos fills EVERY SPEC key with its default (often 0.0).
        # impact_value() returns the first *present* key, so a leading 0.0 default
        # would mask a later positive key (e.g. statefulset_replicas_not_ready=50).
        def _positive_impact(*keys: str) -> float:
            if not _active_chaos:
                return 0.0
            for k in keys:
                v = _active_chaos.get(k)
                if v is None:
                    continue
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    continue
                if fv > 0:
                    return fv
            return 0.0

        _ready_breach = _positive_impact(
            "deployment_unavailable_replicas",
            "statefulset_replicas_not_ready",
            "pod_crashloopbackoff",
        )
        _chaos_pod_ready_fraction: float | None = (
            max(0.0, min(0.5, 1.0 - min(_ready_breach, 1.0))) if _ready_breach > 0 else None
        )
        _ss_breach = _positive_impact("statefulset_replicas_not_ready")
        _chaos_ss_ready_fraction: float | None = (
            max(0.0, min(0.5, 1.0 - min(_ss_breach, 1.0))) if _ss_breach > 0 else None
        )
        _failed_phase = float(impact_value(_active_chaos, "pods_in_failed_phase", default=0.0) or 0.0)
        _pending_phase = float(impact_value(_active_chaos, "pods_stuck_in_pending_phase", default=0.0) or 0.0)
        _chaos_pod_phase_override: int | None = None
        if _failed_phase > 0:
            _chaos_pod_phase_override = 4  # Failed
        elif _pending_phase > 0:
            _chaos_pod_phase_override = 1  # Pending
        # Utilization keys count only when a channel sets them above 0. Unset keys
        # arrive as their 0.0 default and must leave the infra sliders and healthy
        # baselines in charge, not pin every pod and node. A set key is a breach
        # request, so clamp it just past the alert line.
        def _as_fraction(v: float) -> float:
            # GT may pass fraction, percent, or absolute bytes (legacy probe values).
            if v > 100:
                return 0.95
            return v / 100.0 if v > 1.0 else v

        # OOTB container memory near limit is AVG(util) > 0.9 (strict), so the
        # floor must sit above 0.9, not on it.
        _container_mem = _positive_impact("container_memory_near_limit")
        _chaos_mem_util: float | None = (
            max(0.92, min(1.0, _as_fraction(_container_mem))) if _container_mem > 0 else None
        )
        # OOTB node memory saturation is working_set/allocatable > 0.85 — emit util.
        _node_mem = _positive_impact("node_memory_saturation")
        _chaos_node_mem_util: float | None = (
            max(0.9, min(1.0, _as_fraction(_node_mem))) if _node_mem > 0 else None
        )
        # node_memory_pressure is a condition flag (GT often passes 0.8), not util %.
        _pressure = _positive_impact("node_memory_pressure")
        _chaos_memory_pressure_flag: float | None = _pressure if _pressure > 0 else None
        if _chaos_memory_pressure_flag is not None and _chaos_node_mem_util is None:
            # Keep node mem util elevated while the condition flag is active.
            _chaos_node_mem_util = max(_as_fraction(_pressure), 0.9)
        # OOTB node CPU saturation is avg cpu.usage > 0.9 of allocatable cores.
        _node_cpu = _positive_impact("node_cpu_saturation")
        _chaos_cpu_util: float | None = (
            max(0.95, min(1.0, _as_fraction(_node_cpu))) if _node_cpu > 0 else None
        )
        _node_fs = _positive_impact("node_filesystem_saturation")
        _chaos_node_fs_pct: float | None = (
            max(0.9, min(0.99, _as_fraction(_node_fs))) if _node_fs > 0 else None
        )
        # node_disk_pressure is a condition flag (0/1), not filesystem fill %.
        _disk_pressure = _positive_impact("node_disk_pressure")
        _chaos_disk_pressure_flag: float | None = _disk_pressure if _disk_pressure > 0 else None
        # persistent_volume_space_low is the available % to drive toward (breach
        # below 20); 100 or more, including the 200 default, means healthy.
        _vol_available = impact_value(_active_chaos, "persistent_volume_space_low")
        _chaos_vol_fs_pct: float | None = (
            min(0.99, 1.0 - float(_vol_available) / 100.0)
            if _vol_available is not None and 0 <= float(_vol_available) < 100
            else None
        )
        _chaos_node_not_ready = float(
            impact_value(
                _active_chaos,
                "node_not_ready",
                default=0.0,
            )
            or 0.0
        )
        _chaos_job_failed = int(
            impact_value(
                _active_chaos,
                "job_failures",
                default=0,
            )
            or 0
        )
        _chaos_hpa_at_max = float(impact_value(_active_chaos, "hpa_at_max_replicas", default=0.0) or 0.0)
        _chaos_ds_miss = int(
            impact_value(_active_chaos, "daemonset_mis_scheduled_or_not_ready", default=0) or 0
        )

        # Derive event reason strings from active SPEC keys (no string impact knobs).
        _chaos_terminated_reason: str | None = None
        _chaos_warning_reason: str | None = None
        _chaos_warning_rate: float | None = None
        _c = _active_chaos or {}
        _reason_from_keys = (
            ("oomkilled_containers", "OOMKilled", "OOMKilling"),
            ("node_memory_pressure", "Evicted", "Evicted"),
            ("node_not_ready", None, "NodeNotReady"),
            ("node_disk_pressure", None, "NodeHasDiskPressure"),
            ("pods_stuck_in_pending_phase", "Error", "ImagePullBackOff"),
            ("deployment_unavailable_replicas", "Error", "FailedCreate"),
            ("pods_in_failed_phase", "Error", "Error"),
            ("pod_crashloopbackoff", "Error", "BackOff"),
            ("container_cpu_throttling", "Error", "BackOff"),
        )
        for _key, _term, _warn in _reason_from_keys:
            _val = _c.get(_key)
            try:
                _num = float(_val) if _val is not None else 0.0
            except (TypeError, ValueError):
                _num = 0.0
            if _num <= 0:
                continue
            if _term and _chaos_terminated_reason is None:
                _chaos_terminated_reason = _term
            if _warn and _chaos_warning_reason is None:
                _chaos_warning_reason = _warn
                _chaos_warning_rate = _num if _num <= 1.0 else min(1.0, _num / 100.0)

        # Channel generator_impact applies to the services its channels affect and to
        # the nodes they run on. When none of them runs on k8s, fall back to every
        # service so the channel's alert can still fire.
        _impact_services: set[str] = set()
        if _active_chaos:
            for ch_id in _active_chaos.get("channel_ids") or [_active_chaos.get("channel_id")]:
                _impact_services.update(_channel_registry.get(ch_id, {}).get("affected_services", []))
            if not any(svc in _impact_services for c, _ in cluster_data for svc in c["services"]):
                _impact_services = {svc for c, _ in cluster_data for svc in c["services"]}
        _impact_nodes: set[str] = {
            pd["pods"][svc]["node_name"]
            for _, pd in cluster_data
            for svc in _impact_services
            if svc in pd.get("pods", {})
        }

        # APPLY node_filesystem_pct / volume_filesystem_pct before tick so the
        # ramp advances toward the target in this same scrape.
        for _nk in all_node_names:
            _fs_target = _chaos_node_fs_pct if _nk in _impact_nodes else None
            if _fs_target is None and aug_node_disk > 0 and _nk in aug_nodes:
                _fs_target = min(0.99, aug_node_disk / 100.0)
            state.node_fs_target[_nk] = _fs_target
        for _vk in all_vol_keys:
            in_impact = any(_vk.startswith(f"{svc}-") for svc in _impact_services)
            _vol_target = _chaos_vol_fs_pct if in_impact else None
            if (_vol_target is None and aug_pvc > 0 and _vk.endswith("-data-pvc")
                    and _vk[: -len("-data-pvc")] in aug_services):
                _vol_target = min(0.99, aug_pvc / 100.0)
            state.vol_target[_vk] = _vol_target
        state.tick()

        # One pod per full node is evicted once the disk fill slider pushes it past EVICT_FILL.
        evicted_svcs: set[str] = set()
        if aug_node_disk > 0:
            for _n in aug_nodes:
                if state.node_fs_fill.get(_n, 0.0) < EVICT_FILL:
                    continue
                _cands = sorted(
                    svc for svc, (_c, _pd) in _svc_pod.items()
                    if _pd["pods"][svc]["node_name"] == _n
                )
                _pick = [svc for svc in _cands if svc in aug_services] or _cands
                if _pick:
                    evicted_svcs.add(_pick[0])

        # Build set of spiked services (affected by active faults) for any infra spike.
        spiked_services: set[str] = set()
        has_active_faults = False
        if chaos_controller and any_infra_spike:
            active_channels = chaos_controller.get_active_channels()
            if active_channels:
                has_active_faults = True
                for ch_id in active_channels:
                    ch = _channel_registry.get(ch_id, {})
                    spiked_services.update(ch.get("affected_services", []))

        for cluster, pod_data in cluster_data:
            svcs = cluster["services"]
            spiked_nodes: set[str] = set()
            if has_active_faults:
                for svc in spiked_services:
                    pod = pod_data.get("pods", {}).get(svc)
                    if pod:
                        spiked_nodes.add(pod["node_name"])

            # ── Pod-level metrics (kubeletstatsreceiver) ─────────────────────
            # One resource per service; carries pod cpu/memory/network/filesystem
            # and the two kubelet-owned container metrics (cpu.usage, memory.working_set).
            for svc in svcs:
                in_scope = not has_active_faults or svc in spiked_services
                impacted = svc in _impact_services
                pod = pod_data["pods"][svc]
                aug_here = svc in aug_services
                crash_svc = aug_here and (aug_crash or aug_live_fail)
                ready_fail_svc = aug_here and aug_ready_fail
                on_down_node = nr_elapsed is not None and pod["node_name"] == aug.nr_node
                # Pod phase and kubelet visibility under node eviction / NotReady.
                aug_phase: int | None = None
                if svc in evicted_svcs:
                    aug_phase = PHASE_FAILED
                elif on_down_node and nr_stage == "unknown":
                    aug_phase = PHASE_UNKNOWN
                elif on_down_node and nr_stage == "pending":
                    aug_phase = PHASE_PENDING
                kubelet_dark = on_down_node and nr_stage in ("down", "unknown", "pending")
                aug_pod[svc] = {
                    "phase": aug_phase,
                    "unavailable": 1 if aug_phase is not None else 0,
                    "degraded": crash_svc or ready_fail_svc,
                }
                if aug_phase == PHASE_FAILED and scrape_count % 2 == 0:
                    aug_events.append((cluster, pod, "Pod", None, "Evicted",
                        "The node was low on resource: ephemeral-storage. Threshold quantity: 10%, "
                        f"available: {int((1 - state.node_fs_fill.get(pod['node_name'], 0.9)) * 100)}%.",
                        "kubelet"))
                if on_down_node and nr_stage in ("pending", "rescheduled") and svc not in aug.nr_evicted:
                    aug.nr_evicted.add(svc)
                    aug_events.append((cluster, pod, "Pod", None, "TaintManagerEviction",
                        f"Marking for deletion Pod {pod['pod_name']}: node {pod['node_name']} "
                        "is unreachable (NoExecute taint tolerationSeconds elapsed)",
                        "taint-controller"))
                if aug_phase == PHASE_PENDING:  # the scheduler retries (and logs) every cycle
                    aug_events.append((cluster, pod, "Pod", None, "FailedScheduling",
                        "0/3 nodes are available: 1 node(s) had untolerated taint "
                        "{node.kubernetes.io/unreachable}, 2 Insufficient memory.",
                        "scheduler"))
                if aug_pvc > 0 and aug_here:
                    vol_fill = state.vol_fill.get(f"{svc}-data-pvc", 0.0)
                    if vol_fill >= 0.95 and scrape_count % 2 == 0:
                        aug_events.append((cluster, pod, "Pod", None, "VolumeResizeFailed",
                            f'Volume "{svc}-data-pvc": write failed: no space left on device',
                            "kubelet"))
                oom_ratio = oom_intensity / 100.0
                # Duty cycle: fraction of scrapes that emit OOMKilled for in-scope pods.
                oom_this_scrape = bool(
                    in_scope and oom_intensity > 0 and rng.random() < oom_ratio
                )

                if oom_this_scrape:
                    # Sharp jump in major page faults — drives the Namespaces
                    # dashboard "Top 10 by OOM events" (MAX-MIN per bucket).
                    bump = int(rng.randint(40, 120) * max(oom_ratio, 0.1))
                    state.major_page_faults[svc] += bump

                # Resolve memory/CPU utilization overrides.
                # Priority: pod spike sliders > OOM duty scrape > channel impact > healthy.
                mem_util_override: float | None = None
                cpu_util_override: float | None = None
                if in_scope and pod_mem_pct > 0:
                    mem_util_override = min(1.0, max(0.0, pod_mem_pct / 100.0))
                elif oom_this_scrape:
                    # Mild elevates with duty cycle; does not force alert unless high %.
                    mem_util_override = min(1.0, max(0.0, oom_ratio))
                elif impacted and _chaos_mem_util is not None:
                    mem_util_override = float(_chaos_mem_util)
                elif aug_hpa and aug_here and aug.hpa_cur.get(svc, 0) >= _hpa_max.get(svc, 0) > 0:
                    mem_util_override = 0.85  # capped replicas starve: near the limit, below OOM

                if in_scope and pod_cpu_pct > 0:
                    cpu_util_override = min(1.0, max(0.0, pod_cpu_pct / 100.0))
                elif impacted and _chaos_container_cpu is not None and float(_chaos_container_cpu) > 0:
                    # Container CPU throttling OOTB: AVG(cpu_limit_utilization) > 0.9.
                    v = float(_chaos_container_cpu)
                    if v > 1.0:
                        v = v / 100.0
                    cpu_util_override = max(0.95, min(1.0, v))
                elif aug_hpa and aug_here:
                    # Hot while scaling out; past the 0.9 throttling alert once capped.
                    at_cap = aug.hpa_cur.get(svc, 0) >= _hpa_max.get(svc, 0) > 0
                    cpu_util_override = 0.95 if at_cap else 0.85

                if not kubelet_dark:  # NotReady node: its kubelet stops reporting
                    pod_res = _build_pod_resource(svc, pod_data, cluster, _namespace)
                    pod_metrics = _generate_pod_metrics(
                        svc, state, rng, _profiles, mem_util_override, cpu_util_override,
                    )

                    resource_metrics.append({
                        "resource": pod_res,
                        "scopeMetrics": [{"scope": {"name": KUBELET_SCOPE, "version": SCOPE_VERSION}, "metrics": pod_metrics}],
                    })
                    for m in pod_metrics:
                        if m["name"] == "k8s.pod.memory.usage":
                            mem_v = _gauge_value(m)
                            if mem_v is not None:
                                es_bulk_docs.append(
                                    build_pod_memory_doc(pod_res, int(mem_v), timestamp_ms=ts_ms)
                                )
                            break
                    cpu_node = mem_ws = cpu_lim = mem_lim = None
                    for m in pod_metrics:
                        name = m["name"]
                        val = _gauge_value(m)
                        if val is None:
                            continue
                        if name == "k8s.pod.cpu.node.utilization":
                            cpu_node = float(val)
                        elif name == "k8s.pod.memory.working_set":
                            mem_ws = int(val)
                        elif name == "k8s.container.cpu_limit_utilization":
                            cpu_lim = float(val)
                        elif name == "k8s.container.memory_limit_utilization":
                            mem_lim = float(val)
                    if any(v is not None for v in (cpu_node, mem_ws, cpu_lim, mem_lim)):
                        es_bulk_docs.append(
                            build_pod_util_doc(
                                pod_res,
                                cpu_node_util=cpu_node,
                                mem_working_set=mem_ws,
                                cpu_limit_util=cpu_lim,
                                mem_limit_util=mem_lim,
                                timestamp_ms=ts_ms,
                            )
                        )

                # ── Container-level metrics (k8sclusterreceiver) ─────────────
                # OOM duty cycle sets OOMKilled; else channel impact key; else Completed.
                if oom_this_scrape:
                    terminated_reason = "OOMKilled"
                elif impacted and _chaos_terminated_reason is not None:
                    terminated_reason = str(_chaos_terminated_reason)
                elif crash_svc:
                    terminated_reason = "Error"
                else:
                    terminated_reason = "Completed"

                container_res = _build_container_resource(
                    svc, pod_data, cluster, _namespace,
                    last_terminated_reason=terminated_reason,
                )
                container_metrics = _generate_container_metrics(svc, state, rng, _profiles)

                # Resolve restart probability.
                # Priority: OOM duty scrape → pod_crashloopbackoff → 0 (healthy).
                # CrashLoopBackOff alert: MAX(restarts) > 0 over 15m — must snap to 0
                # when chaos is off (gauge is sticky; state must not keep climbing).
                if oom_this_scrape:
                    restart_chance = 0.05 + 0.70 * oom_ratio
                elif impacted and _chaos_restart_rate is not None and float(_chaos_restart_rate) > 0:
                    restart_chance = float(_chaos_restart_rate)
                    # GT often sends percent (75.0) — normalize to 0–1 probability.
                    if restart_chance > 1.0:
                        restart_chance = min(0.95, restart_chance / 100.0)
                elif crash_svc:
                    # CrashLoopBackOff / failing liveness probe: restarts follow the
                    # kubelet back-off (10s, 20s, 40s ... 5m), not a probability.
                    restart_chance = 0.0
                    if aug.crash_restart_due(svc, now):
                        state.restarts[svc] += 1
                        aug_events.append((cluster, pod, "Pod", None, "BackOff",
                            f"Back-off restarting failed container {svc}-container in pod "
                            f"{pod['pod_name']}_{_namespace}", "kubelet"))
                        if aug_live_fail:
                            aug_events.append((cluster, pod, "Pod", None, "Unhealthy",
                                "Liveness probe failed: HTTP probe failed with statuscode: 503; "
                                f"container {svc}-container will be restarted", "kubelet"))
                else:
                    restart_chance = 0.0
                    state.restarts[svc] = 0
                    aug.crash.pop(svc, None)
                if restart_chance > 0 and rng.random() < restart_chance:
                    state.restarts[svc] += 1
                if ready_fail_svc and scrape_count % 2 == 0:
                    aug_events.append((cluster, pod, "Pod", None, "Unhealthy",
                        "Readiness probe failed: HTTP probe failed with statuscode: 503", "kubelet"))
                # A crashing, OOM-killed, pending or failed container is not ready.
                container_ready = not (
                    oom_this_scrape or restart_chance > 0
                    or (impacted and (_failed_phase > 0 or _pending_phase > 0))
                    or crash_svc or ready_fail_svc or aug_phase is not None
                )
                for m in container_metrics:
                    if m["name"] == "k8s.container.restarts":
                        m["gauge"]["dataPoints"][0]["asInt"] = str(state.restarts[svc])
                    elif m["name"] == "k8s.container.ready":
                        m["gauge"]["dataPoints"][0]["asInt"] = "1" if container_ready else "0"

                resource_metrics.append({
                    "resource": container_res,
                    "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": container_metrics}],
                })
                es_cluster_bulk_docs.append(
                    build_container_status_doc(
                        container_res,
                        state.restarts[svc],
                        timestamp_ms=ts_ms,
                    )
                )

                # ── Volume metrics (kubeletstatsreceiver) ────────────────────
                for vol_suffix in ["data-pvc", "config-vol"]:
                    vol_key = f"{svc}-{vol_suffix}"
                    vol_cap = _profiles["vol"].get(vol_key)
                    vol_fill = state.vol_fill.get(vol_key)
                    vol_res = _build_volume_resource(svc, vol_key, pod_data, cluster, _namespace)
                    vol_metrics = _generate_volume_metrics(rng, capacity=vol_cap, fill=vol_fill)
                    resource_metrics.append({
                        "resource": vol_res,
                        "scopeMetrics": [{"scope": {"name": KUBELET_SCOPE, "version": SCOPE_VERSION}, "metrics": vol_metrics}],
                    })

                # Emit OOMKilled event log when this scrape's duty cycle fires
                if oom_this_scrape and rng.random() < 0.15 * max(oom_ratio, 0.1):
                    _generate_oom_killed_log(client, svc, pod_data, cluster, rng, _namespace)

            # ── Node-level metrics — split across two receivers ───────────────
            # Channel generator_impact wins; else host CPU/Memory sliders (1:1 util).
            for node_name in pod_data["node_names"]:
                node_in_scope = not has_active_faults or node_name in spiked_nodes
                node_impacted = node_name in _impact_nodes
                node_cpu_override = _chaos_cpu_util if node_impacted else None
                node_mem_override = _chaos_node_mem_util if node_impacted else None
                if node_cpu_override is None and host_cpu_pct > 0 and node_in_scope:
                    node_cpu_override = min(1.0, max(0.0, host_cpu_pct / 100.0))
                if node_mem_override is None and host_mem_pct > 0 and node_in_scope:
                    node_mem_override = min(1.0, max(0.0, host_mem_pct / 100.0))

                node_prof = _profiles.get("node", {}).get(node_name, {})
                alloc_cpu = node_prof.get("allocatable_cpu", 4.0)
                alloc_mem = node_prof.get("allocatable_memory", 16_000_000_000)
                fill = state.node_fs_fill.get(node_name, 0.3)

                # kubeletstatsreceiver: cpu.usage, memory.*, filesystem.*, network.*
                # A NotReady node's kubelet stops posting, so its node stats go dark.
                node_down = nr_elapsed is not None and node_name == aug.nr_node
                kubelet_node_res = _build_node_resource_kubelet(node_name, pod_data, cluster)
                kubelet_node_metrics = _generate_node_metrics_kubelet(
                    rng, node_name=node_name, profiles=_profiles, state=state,
                    cpu_util_override=node_cpu_override,
                    mem_util_override=node_mem_override,
                )
                if not node_down:
                    resource_metrics.append({
                        "resource": kubelet_node_res,
                        "scopeMetrics": [{"scope": {"name": KUBELET_SCOPE, "version": SCOPE_VERSION}, "metrics": kubelet_node_metrics}],
                    })
                cpu_v = mem_v = None
                for m in kubelet_node_metrics:
                    if m["name"] == "k8s.node.cpu.utilization":
                        cpu_v = _gauge_value(m)
                    elif m["name"] == "k8s.node.memory.utilization":
                        mem_v = _gauge_value(m)
                if cpu_v is not None and mem_v is not None and not node_down:
                    es_bulk_docs.append(
                        build_node_util_doc(
                            kubelet_node_res,
                            cpu_util=float(cpu_v),
                            mem_util=float(mem_v),
                            timestamp_ms=ts_ms,
                        )
                    )
                _node_pod = next(iter(pod_data["pods"].values()), None)
                if _node_pod is not None and scrape_count % 2 == 0:
                    _on_node = next(
                        (p for p in pod_data["pods"].values() if p["node_name"] == node_name), _node_pod,
                    )
                    if node_down:
                        aug_events.append((cluster, _on_node, "Node", node_name, "NodeNotReady",
                            "Node controller detected node is not Ready; kubelet stopped posting node status.",
                            "node-controller"))
                    if fill >= 0.85 and aug_node_disk > 0 and node_name in aug_nodes:
                        aug_events.append((cluster, _on_node, "Node", node_name, "NodeHasDiskPressure",
                            "Node disk usage is above threshold, pods will not be scheduled to this node.",
                            "kubelet"))

                # APPLY node_disk_pressure flag OR filesystem fill → condition_disk_pressure;
                # host mem spike / channel at or above node memory saturation (0.85) → pressure.
                disk_pressure = 1 if (
                    (node_impacted and _chaos_disk_pressure_flag is not None)
                    or (node_impacted and _chaos_node_fs_pct is not None and _chaos_node_fs_pct >= 0.85)
                    or fill >= 0.85
                ) else 0
                memory_pressure = 1 if (
                    (node_mem_override is not None and float(node_mem_override) >= 0.85)
                    or (node_impacted and _chaos_memory_pressure_flag is not None)
                ) else 0
                condition_ready = 0 if (
                    (node_impacted and _chaos_node_not_ready > 0) or node_down
                ) else 1

                # k8sclusterreceiver: allocatable_*, condition_*
                cluster_node_res = _build_node_resource(node_name, pod_data, cluster)
                cluster_node_metrics = _generate_node_metrics_cluster(
                    rng,
                    condition_ready=condition_ready,
                    memory_pressure=memory_pressure,
                    disk_pressure=disk_pressure,
                    allocatable_cpu=alloc_cpu,
                    allocatable_memory=alloc_mem,
                )

                resource_metrics.append({
                    "resource": cluster_node_res,
                    "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": cluster_node_metrics}],
                })

            # ── Deployment-level metrics ──────────────────────────────────────
            for svc in svcs:
                dep_res = _build_deployment_resource(svc, pod_data, cluster, _namespace)
                _aug = aug_pod.get(svc, {})
                _dep_fraction = _chaos_pod_ready_fraction if svc in _impact_services else None
                if _dep_fraction is None and _aug.get("degraded"):
                    _dep_fraction = 0.5  # crash-looping / not-Ready replicas are out of service
                metrics = _generate_deployment_metrics(
                    rng, ready_fraction=_dep_fraction,
                    desired=aug.hpa_cur.get(svc, _replicas.get(svc)),
                    unavailable=_aug.get("unavailable", 0),
                )
                resource_metrics.append({
                    "resource": dep_res,
                    "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": metrics}],
                })

            # ── DaemonSet metrics ─────────────────────────────────────────────
            num_nodes = len(pod_data["node_names"])
            _ds_miss = max(_chaos_ds_miss, aug_ds_miss)
            for ds_name in _daemonsets:
                ds_res = _build_daemonset_resource(ds_name, cluster, _namespace)
                metrics = _generate_daemonset_metrics(
                    rng, num_nodes,
                    ready_fraction=0.5 if _ds_miss > 0 else None,
                    misscheduled=_ds_miss,
                )
                if aug_ds_miss > 0 and scrape_count % 4 == 0 and pod_data["pods"]:
                    aug_events.append((cluster, next(iter(pod_data["pods"].values())), "DaemonSet", ds_name,
                        "FailedDaemonPod",
                        f"Found failed daemon pod {_namespace}/{ds_name} on node, will try to kill it",
                        "daemonset-controller"))
                resource_metrics.append({
                    "resource": ds_res,
                    "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": metrics}],
                })

            # ── StatefulSet / DaemonSet pods ──────────────────────────────────
            # kubeletstats pod + container metrics, and the cluster receiver's pod
            # phase and container status. Healthy: Running, ready, no restarts. A
            # StatefulSet fault leaves its highest ordinals not ready; a DaemonSet
            # fault leaves pods on the later nodes not ready.
            _ds_miss_now = max(_chaos_ds_miss, aug_ds_miss)
            _ds_ready = len(pod_data["node_names"]) if _ds_miss_now <= 0 else max(
                0, min(len(pod_data["node_names"]) - 1, int(len(pod_data["node_names"]) * 0.5)),
            )
            _ds_seen: dict[str, int] = {}
            for wp in _workload_pods.get(cluster["name"], []):
                if wp["kind"] == "statefulset":
                    ordinal = int(wp["pod_name"].rsplit("-", 1)[1])
                    wp_ready = ordinal < _statefulset_ready(
                        _ss_replicas.get(wp["owner"], 2), _chaos_ss_ready_fraction,
                    )
                else:
                    idx = _ds_seen.get(wp["owner"], 0)
                    _ds_seen[wp["owner"]] = idx + 1
                    wp_ready = idx < _ds_ready
                wp_dark = (
                    nr_elapsed is not None and wp["node_name"] == aug.nr_node
                    and nr_stage in ("down", "unknown", "pending")
                )
                if not wp_dark:
                    wp_res = _build_workload_pod_resource(wp, cluster, _namespace)
                    wp_metrics = _generate_pod_metrics(
                        wp["key"], state, rng, _profiles, container_name=wp["container_name"],
                    )
                    resource_metrics.append({
                        "resource": wp_res,
                        "scopeMetrics": [{"scope": {"name": KUBELET_SCOPE, "version": SCOPE_VERSION}, "metrics": wp_metrics}],
                    })
                    _vals = {m["name"]: _gauge_value(m) for m in wp_metrics if "gauge" in m}
                    es_bulk_docs.append(build_pod_memory_doc(
                        wp_res, int(_vals["k8s.pod.memory.usage"]), timestamp_ms=ts_ms,
                    ))
                    es_bulk_docs.append(build_pod_util_doc(
                        wp_res,
                        cpu_node_util=_vals.get("k8s.pod.cpu.node.utilization"),
                        mem_working_set=_vals.get("k8s.pod.memory.working_set"),
                        cpu_limit_util=_vals.get("k8s.container.cpu_limit_utilization"),
                        mem_limit_util=_vals.get("k8s.container.memory_limit_utilization"),
                        timestamp_ms=ts_ms,
                    ))
                resource_metrics.append({
                    "resource": _build_workload_cluster_resource(wp, cluster, _namespace),
                    "scopeMetrics": [{
                        "scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION},
                        "metrics": _generate_pod_phase_metric(rng),
                    }],
                })
                wp_container_res = _build_workload_cluster_resource(
                    wp, cluster, _namespace, with_container=True,
                )
                resource_metrics.append({
                    "resource": wp_container_res,
                    "scopeMetrics": [{
                        "scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION},
                        "metrics": _generate_container_metrics(
                            wp["key"], state, rng, _profiles, ready=wp_ready,
                        ),
                    }],
                })
                es_cluster_bulk_docs.append(
                    build_container_status_doc(wp_container_res, state.restarts[wp["key"]], timestamp_ms=ts_ms)
                )

            # ── StatefulSet metrics ───────────────────────────────────────────
            for ss_name in _statefulsets:
                ss_res = _build_statefulset_resource(ss_name, cluster, _namespace)
                # Only the StatefulSet key degrades StatefulSets: a crashlooping
                # Deployment must not also fire the StatefulSet alert.
                metrics = _generate_statefulset_metrics(
                    rng, ready_fraction=_chaos_ss_ready_fraction,
                    desired=_ss_replicas.get(ss_name),
                )
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
                phase_impacted = svc in _impact_services
                _phase_override = _chaos_pod_phase_override if phase_impacted else None
                if _phase_override is None:
                    _phase_override = aug_pod.get(svc, {}).get("phase")
                metrics = _generate_pod_phase_metric(
                    rng,
                    ready_fraction=_chaos_pod_ready_fraction if phase_impacted else None,
                    phase_override=_phase_override,
                )
                resource_metrics.append({
                    "resource": phase_res,
                    "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": metrics}],
                })

            # Succeeded Job pods → Overview "non-running pods" panel (phase != 2)
            # without firing Pending/Failed OOTB alerts.
            # Succeeded (3) pods would mask the Pending alert (MAX(phase)==1), so they
            # also stay quiet while an augmentation holds pods in a non-Running phase.
            if (
                _chaos_pod_phase_override is None and _chaos_pod_ready_fraction is None
                and not any(v["phase"] is not None for v in aug_pod.values())
            ):
                for job_name in _jobs:
                    job_phase_res = _build_job_pod_phase_resource(
                        job_name, cluster, _namespace,
                    )
                    resource_metrics.append({
                        "resource": job_phase_res,
                        "scopeMetrics": [{
                            "scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION},
                            "metrics": _generate_succeeded_pod_phase_metric(),
                        }],
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
                metrics = _generate_job_metrics(rng, failed_pods=_chaos_job_failed)
                resource_metrics.append({
                    "resource": job_res,
                    "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": metrics}],
                })

            # ── HPA metrics ───────────────────────────────────────────────────
            for svc in svcs:
                hpa_name = f"{svc}-hpa"
                hpa_res = _build_hpa_resource(hpa_name, cluster, _namespace)
                _base = _replicas.get(svc)
                _max = _hpa_max.get(svc)
                if aug_hpa and svc in aug_services and _base and _max:
                    # Scale out one replica per scrape until the HPA's maxReplicas, then hold.
                    _cur = aug.hpa_cur.get(svc, _base)
                    if _cur < _max:
                        _cur += 1
                        aug_events.append((cluster, pod_data["pods"][svc], "HorizontalPodAutoscaler", hpa_name,
                            "SuccessfulRescale",
                            f"New size: {_cur}; reason: cpu resource utilization (percentage of request) above target",
                            "horizontal-pod-autoscaler"))
                    elif scrape_count % 4 == 0:
                        aug_events.append((cluster, pod_data["pods"][svc], "HorizontalPodAutoscaler", hpa_name,
                            "ScalingLimited",
                            "the desired replica count is more than the maximum replica count",
                            "horizontal-pod-autoscaler"))
                    aug.hpa_cur[svc] = _cur
                elif svc in aug.hpa_cur:
                    aug.hpa_cur.pop(svc)
                metrics = _generate_hpa_metrics(
                    rng, at_max=_chaos_hpa_at_max > 0 and svc in _impact_services,
                    max_replicas=_max, current=aug.hpa_cur.get(svc, _base),
                )
                resource_metrics.append({
                    "resource": hpa_res,
                    "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": metrics}],
                })

        # Split the scrape — a single resourceMetrics payload for all clusters/
        # pods/nodes regularly exceeds ingest limits and trips OTLP backoff,
        # which starves k8sclusterreceiver (OOM / CrashLoop / replica alerts)
        # while kubeletstats bulk companions keep flowing.
        _OTLP_CHUNK = 25
        for _i in range(0, len(resource_metrics), _OTLP_CHUNK):
            chunk = resource_metrics[_i : _i + _OTLP_CHUNK]
            client._send(
                f"{client.endpoint}/v1/metrics",
                {"resourceMetrics": chunk},
                "k8s-metrics",
            )
        if es_bulk.configured and es_bulk_docs:
            es_bulk.send_bulk("metrics-kubeletstatsreceiver.otel-default", es_bulk_docs)
        if es_bulk.configured and es_cluster_bulk_docs:
            es_bulk.send_bulk(
                "metrics-k8sclusterreceiver.otel-default",
                es_cluster_bulk_docs,
                dynamic_templates={"metrics.k8s.container.restarts": "gauge_long"},
            )

        # Events that explain the active augmentations, on the faulted pod / node / HPA.
        for _cl, _pod, _kind, _obj, _reason, _msg, _comp in aug_events:
            _send_k8s_event(
                client, _cl, _namespace,
                pod_name=_pod["pod_name"], node_name=_pod["node_name"],
                reason=_reason, message=_msg, object_kind=_kind,
                object_name=_obj, source_component=_comp,
            )

        # Occasional K8s warning event logs (pick a random cluster)
        cluster, pod_data = rng.choice(cluster_data)
        _generate_k8s_warning_logs(
            client, pod_data, cluster, rng, _namespace,
            event_reason=_chaos_warning_reason,
            event_rate=_chaos_warning_rate,
        )

        scrape_count += 1
        if scrape_count % 4 == 0:
            logger.info("K8s metrics scrape %d complete", scrape_count)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: METRICS_INTERVAL)

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
