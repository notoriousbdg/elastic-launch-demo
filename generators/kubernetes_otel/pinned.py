"""Kubernetes metrics for scenarios that pin their platform (schema 1.3 ``k8s_topology``).

``generator.run`` hands off here when the scenario declares ``k8s_topology``; every
other scenario keeps the generated three-node layout in ``generator.py`` unchanged.

Differences from the generated layout:

* Nodes, node pools, workloads and pods come from ``generator_engine.platform_topology``,
  so node counts, names, IPs, instance types and kubelet versions agree with every log
  line that quotes them.
* Faults honour ``fault_scope`` (cluster, node pool, nodes, AZ, workloads): only the
  pods and nodes inside the scope degrade, and the rest of the platform stays healthy
  as a comparison.
* Fault impacts ramp in over ``ramp_seconds`` and fix effects play out over the fix's
  ``transition_seconds`` (node joins, rolling updates, the HPA scale-down window).
* Related signals are coupled instead of set independently (the coherence contract,
  generator schema 1.1): an HPA at max means replicas = max and per-pod CPU above
  target; throttling means CPU usage at the limit; an OOMKilled container ramps its
  memory to the limit before each kill and its restart count rises; a NotReady node
  stops reporting, its pods go Unknown and are evicted after 300 s; disk pressure means
  a full filesystem and evicted pods; node CPU and memory are the sum of their pods.
* Platform state (``fault_state`` / ``remediation_effects.state``) changes the model
  itself: a raised memory limit shows up as ``k8s.container.memory_limit``, added Spot
  instance types as new nodes with ``host.type``, a rollback as the old image tag.
"""

from __future__ import annotations

import copy
import logging
import math
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from app.telemetry import ESBulkClient, OTLPClient, SCHEMA_URL, _format_attributes
from generator_engine.platform_topology import (
    pod_template,
    Cluster,
    Node,
    Platform,
    Pod,
    Workload,
    effective_platform,
    get_platform,
    parse_bytes,
    pending_template_hash,
)
from generator_engine.tick import run_tick_loop
from generators.kubernetes_otel.generator import (
    CLUSTER_SCOPE,
    KUBELET_SCOPE,
    METRICS_INTERVAL,
    NODE_EVICT_AFTER_S,
    NODE_UNKNOWN_AFTER_S,
    CRASH_BACKOFF_MAX_S,
    CRASH_BACKOFF_START_S,
    PHASE_FAILED,
    PHASE_PENDING,
    PHASE_RUNNING,
    PHASE_UNKNOWN,
    SCOPE_VERSION,
    _cumulative_sum,
    _gauge,
    _send_k8s_event,
    build_container_status_doc,
    build_node_util_doc,
    build_pod_memory_doc,
    build_pod_util_doc,
)

logger = logging.getLogger("k8s-metrics-generator.pinned")

HPA_SCALE_DOWN_STABILIZATION_S = 300.0
HPA_SCALE_DOWN_DURATION_S = 60.0
OOM_CYCLE_S = (60.0, 95.0)
NODE_SYSTEM_CPU = 0.06       # kubelet, containerd, kube-proxy overhead (fraction)
NODE_SYSTEM_MEM = 0.10
DISK_PRESSURE_FILL = 0.85
# generator_impact keys that start before onset_delay_seconds (see _collect_faults)
_PRE_ONSET_IMPACTS = frozenset({"node_disk_pressure", "node_filesystem_saturation",
                                "persistent_volume_space_low"})
_OTLP_CHUNK = 25


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * _clamp(t)


# ── Faults ────────────────────────────────────────────────────────────────────


@dataclass
class _Fault:
    channel: int
    impact: dict[str, float]
    scope: dict[str, Any]
    ramp: float
    elapsed: float
    onset_at: float
    k8s_events: list[dict]
    workloads: set[str]
    pods: list[Pod]
    nodes: list[Node]
    cluster: Cluster


def _positive(impact: dict, key: str) -> float:
    try:
        v = float(impact.get(key) or 0)
    except (TypeError, ValueError):
        return 0.0
    return v if v > 0 else 0.0


def _collect_faults(controller, registry: dict, platform: Platform, now: float) -> list[_Fault]:
    if controller is None:
        return []
    from scenario_engine.fault_spec import resolve_dict

    elapsed_of = getattr(controller, "get_channel_elapsed", None)
    faults: list[_Fault] = []
    for ch_id in controller.get_active_channels():
        ch = registry.get(ch_id) or {}
        raw_impact = (ch.get("generator_impact") or {}).get("kubernetes_otel")
        events = ch.get("k8s_events") or []
        if raw_impact is None and not events:
            continue
        elapsed = elapsed_of(ch_id) if elapsed_of else 0.0
        if elapsed is None:
            continue
        onset = float(ch.get("onset_delay_seconds", 0) or 0)
        try:
            impact = resolve_dict(raw_impact or {})
        except Exception:
            impact = dict(raw_impact or {})
        if elapsed < onset:
            # A disk fills up before the kubelet starts evicting: disk-fill impacts
            # ramp in from the trigger and reach their target just before onset; nothing else
            # (events, pod effects) starts before onset.
            impact = {k: v for k, v in impact.items() if k in _PRE_ONSET_IMPACTS}
            if not impact or onset <= 0:
                continue
            events = []
            ramp = _clamp(elapsed / (0.75 * onset))  # full a little before the first eviction
        else:
            ramp_s = float(ch.get("ramp_seconds", 0) or 0)
            ramp = 1.0 if ramp_s <= 0 else _clamp((elapsed - onset) / ramp_s)
        scope = dict(ch.get("fault_scope") or {})
        cluster = platform.cluster(scope.get("cluster"))
        wls = set(scope.get("workloads") or [])
        if not wls:
            for svc in ch.get("affected_services", []):
                found = platform.workload_for_service(svc)
                if found:
                    wls.add(found[1].name)
        has_node_selector = any(scope.get(k) for k in ("nodes", "node_pool", "az"))
        if has_node_selector:
            nodes = platform.nodes_in_scope(scope)
            node_names = {n.name for n in nodes}
            pods = [p for p in cluster.pods if p.workload in wls and p.node in node_names]
        else:
            pods = [p for p in cluster.pods if p.workload in wls]
            node_names = {p.node for p in pods}
            nodes = [n for n in cluster.nodes if n.name in node_names]
        faults.append(_Fault(
            channel=ch_id, impact={k: v for k, v in impact.items()}, scope=scope,
            ramp=ramp, elapsed=elapsed, onset_at=now - elapsed + onset,
            k8s_events=list(events), workloads=wls, pods=pods, nodes=nodes, cluster=cluster,
        ))
    return faults


# ── Per-entity state carried across scrapes ──────────────────────────────────


@dataclass
class _PodState:
    net_rx: int
    net_tx: int
    faults: int
    restarts: int = 0
    cpu_base: float = 0.4
    mem_base: float = 0.5
    oom_phase_start: float | None = None
    oom_period: float = 75.0
    last_terminated: str = "Completed"
    last_terminated_at: float = 0.0
    crash_next_at: float = 0.0
    crash_backoff: float = CRASH_BACKOFF_START_S


@dataclass
class _NodeState:
    net_rx: int
    net_tx: int
    fs_fill: float
    fs_cap: int
    down_since: float | None = None
    evicted_logged: bool = False


@dataclass
class _State:
    rng: random.Random
    pods: dict[str, _PodState] = field(default_factory=dict)
    nodes: dict[str, _NodeState] = field(default_factory=dict)
    pvc_used: dict[str, int] = field(default_factory=dict)
    # keyed by (cluster, workload): one state object serves every cluster
    hpa_current: dict[tuple[str, str], int] = field(default_factory=dict)
    hpa_capped_until: dict[tuple[str, str], float] = field(default_factory=dict)
    hpa_load: dict[tuple[str, str], float] = field(default_factory=dict)
    event_last: dict[tuple, float] = field(default_factory=dict)
    event_count: dict[tuple, int] = field(default_factory=dict)

    def pod(self, pod: Pod, wl: Workload) -> _PodState:
        st = self.pods.get(pod.uid)
        if st is None:
            r = random.Random(pod.uid)
            st = _PodState(
                net_rx=r.randint(50_000_000, 900_000_000),
                net_tx=r.randint(50_000_000, 900_000_000),
                faults=r.randint(0, 50),
                cpu_base=r.uniform(*wl.cpu_util),
                mem_base=r.uniform(*wl.memory_util),
                oom_period=r.uniform(*OOM_CYCLE_S),
            )
            self.pods[pod.uid] = st
        return st

    def node(self, node: Node) -> _NodeState:
        st = self.nodes.get(node.name)
        if st is None:
            r = random.Random(node.uid)
            st = _NodeState(
                net_rx=r.randint(5_000_000_000, 90_000_000_000),
                net_tx=r.randint(5_000_000_000, 90_000_000_000),
                fs_fill=r.uniform(0.22, 0.48),
                fs_cap=r.choice([100, 120, 150]) * 2**30,
            )
            self.nodes[node.name] = st
        return st


# ── Resources ────────────────────────────────────────────────────────────────


def _cloud(cluster: Cluster) -> dict[str, Any]:
    return {
        "k8s.cluster.name": cluster.name,
        "cloud.provider": cluster.provider,
        "cloud.platform": cluster.platform,
        "cloud.region": cluster.region,
        "cloud.account.id": cluster.account_id,
    }


def _ds(dataset: str, kind: str = "metrics") -> dict[str, str]:
    return {
        "data_stream.type": kind,
        "data_stream.dataset": dataset,
        "data_stream.namespace": "default",
    }


def _res(attrs: dict[str, Any]) -> dict:
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _node_attrs(cluster: Cluster, node: Node) -> dict[str, Any]:
    return {
        **_cloud(cluster),
        "k8s.node.name": node.name,
        "k8s.node.uid": node.uid,
        "k8s.kubelet.version": node.kubelet_version,
        "host.name": node.name,
        "host.id": node.instance_id,
        "host.type": node.instance_type,
        "host.arch": node.arch,
        "host.ip": node.ip,
        "cloud.availability_zone": node.az,
        "cloud.instance.id": node.instance_id,
        "os.type": "linux",
        "os.description": node.os_image,
        "k8s.node.label.node.kubernetes.io/instance-type": node.instance_type,
        "k8s.node.label.eks.amazonaws.com/nodegroup": node.pool,
        "k8s.node.label.eks.amazonaws.com/capacityType": node.capacity_type,
        "k8s.node.label.kubernetes.io/arch": node.arch,
        "k8s.node.label.topology.kubernetes.io/zone": node.az,
    }


def _pod_attrs(cluster: Cluster, pod: Pod, wl: Workload, node: Node | None) -> dict[str, Any]:
    from generator_engine.platform_topology import pod_resource_attrs

    attrs = pod_resource_attrs(cluster, pod, wl)
    attrs.update(_cloud(cluster))
    if node is not None:
        attrs["cloud.availability_zone"] = node.az
    if wl.kind == "Deployment":
        # The pod's own ReplicaSet (old or new during a rolling update).
        attrs["k8s.replicaset.name"] = pod.name.rsplit("-", 1)[0]
    return attrs


# ── Tick ─────────────────────────────────────────────────────────────────────


class _Tick:
    def __init__(self, client: OTLPClient, scenario_data: dict, controller) -> None:
        self.client = client
        self.controller = controller
        self.registry = scenario_data.get("channel_registry") or {}
        self.namespace = scenario_data["namespace"]
        self.base = get_platform(scenario_data["k8s_topology"], self.namespace)
        self.state = _State(rng=random.Random())
        self.es_bulk = ESBulkClient()
        self.scrape = 0

    # -- helpers ---------------------------------------------------------------

    def _platform_state(self) -> dict | None:
        getter = getattr(self.controller, "get_platform_state", None)
        return getter() if getter else None

    def _event(self, cluster: Cluster, *, key: tuple, every: float, pod: Pod | None,
               node_name: str, reason: str, message: str, kind: str, obj: str | None,
               namespace: str, component: str, now: float) -> None:
        last = self.state.event_last.get(key, 0.0)
        if now - last < every:
            return
        self.state.event_last[key] = now
        self.state.event_count[key] = self.state.event_count.get(key, 0) + 1
        try:
            _send_k8s_event(
                self.client,
                {"name": cluster.name, "provider": cluster.provider,
                 "platform": cluster.platform, "region": cluster.region},
                namespace,
                pod_name=pod.name if pod else (obj or ""),
                node_name=node_name,
                reason=reason,
                message=message,
                object_kind=kind,
                object_name=obj,
                source_component=component,
            )
        except Exception:
            logger.debug("k8s event send failed", exc_info=True)

    # -- main ------------------------------------------------------------------

    def __call__(self) -> None:
        now = time.time()
        platform = effective_platform(self.base, self._platform_state(), now)
        faults = _collect_faults(self.controller, self.registry, platform, now)
        rm: list[dict] = []
        kubelet_docs: list[dict] = []
        cluster_docs: list[dict] = []
        ts_ms = int(now * 1000)
        for cluster in platform.clusters.values():
            cf = [f for f in faults if f.cluster.name == cluster.name]
            self._emit_cluster(cluster, cf, now, rm, kubelet_docs, cluster_docs, ts_ms)
        for i in range(0, len(rm), _OTLP_CHUNK):
            self.client._send(
                f"{self.client.endpoint}/v1/metrics",
                {"resourceMetrics": rm[i:i + _OTLP_CHUNK]},
                "k8s-metrics",
            )
        if self.es_bulk.configured and kubelet_docs:
            self.es_bulk.send_bulk("metrics-kubeletstatsreceiver.otel-default", kubelet_docs)
        if self.es_bulk.configured and cluster_docs:
            self.es_bulk.send_bulk(
                "metrics-k8sclusterreceiver.otel-default", cluster_docs,
                dynamic_templates={"metrics.k8s.container.restarts": "gauge_long"},
            )
        self.scrape += 1

    def _emit_cluster(self, cluster: Cluster, faults: list[_Fault], now: float,
                      rm: list, kubelet_docs: list, cluster_docs: list, ts_ms: int) -> None:
        st = self.state
        rng = st.rng

        # ---- aggregate fault effects ------------------------------------------
        pod_cpu: dict[str, float] = {}          # pod uid -> throttling ramp (0..1)
        wl_throttle: dict[str, float] = {}      # workload -> throttling ramp
        pod_mem: dict[str, float] = {}          # pod uid -> util of limit
        pod_oom: set[str] = set()
        pod_crash: set[str] = set()
        pod_not_ready: set[str] = set()
        pod_failed: set[str] = set()
        node_cpu: dict[str, float] = {}
        node_mem: dict[str, float] = {}
        node_fs_target: dict[str, float] = {}
        node_down: set[str] = set()
        node_mem_pressure: set[str] = set()
        wl_hpa_load: dict[str, float] = {}
        wl_unavailable: dict[str, int] = {}
        wl_pending: dict[str, int] = {}
        wl_pending_tpl: dict[str, dict] = {}
        wl_job_failed: dict[str, int] = {}
        pvc_target: dict[str, float] = {}
        ss_not_ready: dict[str, int] = {}

        for f in faults:
            imp, r = f.impact, f.ramp
            uids = [p.uid for p in f.pods]
            v = _positive(imp, "container_cpu_throttling")
            if v:
                for u in uids:
                    pod_cpu[u] = max(pod_cpu.get(u, 0.0), r)  # throttling ramp 0..1
                for wl in f.workloads:  # includes replicas the HPA adds later
                    wl_throttle[wl] = max(wl_throttle.get(wl, 0.0), r)
            v = _positive(imp, "container_memory_near_limit")
            if v:
                for u in uids:
                    pod_mem[u] = max(pod_mem.get(u, 0.0), _lerp(0.6, 0.95, r))
            if _positive(imp, "oomkilled_containers"):
                pod_oom.update(uids)
            if _positive(imp, "pod_crashloopbackoff"):
                pod_crash.update(uids)
            if _positive(imp, "daemonset_mis_scheduled_or_not_ready"):
                for p in f.pods:
                    if p.kind == "DaemonSet":
                        pod_not_ready.add(p.uid)
            v = _positive(imp, "pods_in_failed_phase")
            if v:
                n = int(v) if v >= 1 else max(1, round(len(uids) * v))
                pod_failed.update(uids[:n])
            v = _positive(imp, "pods_stuck_in_pending_phase")
            if v:
                fix_wls = ((((self.registry.get(f.channel) or {}).get("remediation_effects") or {})
                            .get("state") or {}).get("kubernetes_otel") or {}).get("workloads") or {}
                for wl in f.workloads:
                    n = int(v) if v >= 1 else max(1, round(len(f.pods) * v))
                    wl_pending[wl] = max(wl_pending.get(wl, 0), max(1, int(round(n * max(r, 0.34)))))
                    # the stuck pods belong to the ReplicaSet the fix lets finish rolling out
                    if isinstance(fix_wls.get(wl), dict):
                        wl_pending_tpl[wl] = fix_wls[wl]
            v = _positive(imp, "deployment_unavailable_replicas")
            if v:
                for wl in f.workloads:
                    n = int(v) if v >= 1 else max(1, round(len(f.pods) * v))
                    wl_unavailable[wl] = max(wl_unavailable.get(wl, 0), n)
            v = _positive(imp, "hpa_at_max_replicas")
            if v:
                load = v if v > 1 else 4.0
                for wl in f.workloads:
                    wl_hpa_load[wl] = max(wl_hpa_load.get(wl, 1.0), _lerp(1.0, load, r))
            v = _positive(imp, "job_failures")
            if v:
                for wl in f.workloads:
                    wl_job_failed[wl] = max(wl_job_failed.get(wl, 0), int(max(1, v)))
            v = _positive(imp, "statefulset_replicas_not_ready")
            if v:
                for wl in f.workloads:
                    ss_not_ready[wl] = max(ss_not_ready.get(wl, 0), int(max(1, v if v >= 1 else 1)))
            if "persistent_volume_space_low" in imp:
                try:
                    avail = float(imp["persistent_volume_space_low"])
                except (TypeError, ValueError):
                    avail = 200.0
                if 0 <= avail < 100:
                    for wl in f.workloads:
                        pvc_target[wl] = _lerp(pvc_target.get(wl, 0.6), 1.0 - avail / 100.0, r)
            node_names = [n.name for n in f.nodes]
            if _positive(imp, "node_not_ready"):
                node_down.update(node_names)
            v = max(_positive(imp, "node_disk_pressure"), _positive(imp, "node_filesystem_saturation"))
            if v:
                tgt = 0.93 if v <= 1 else _clamp(v / 100.0, 0.86, 0.99)
                for n in node_names:
                    node_fs_target[n] = max(node_fs_target.get(n, 0.0), _lerp(0.5, tgt, r))
            if _positive(imp, "node_cpu_saturation"):
                for n in node_names:
                    node_cpu[n] = max(node_cpu.get(n, 0.0), _lerp(0.5, 0.96, r))
            v = max(_positive(imp, "node_memory_saturation"), _positive(imp, "node_memory_pressure"))
            if v:
                for n in node_names:
                    node_mem[n] = max(node_mem.get(n, 0.0), _lerp(0.5, 0.93, r))
                if _positive(imp, "node_memory_pressure"):
                    node_mem_pressure.update(node_names)

        # ---- node lifecycle (NotReady cascade) ----------------------------------
        for node in cluster.nodes:
            ns = st.node(node)
            if node.name in node_down:
                if ns.down_since is None:
                    ns.down_since = now
            else:
                ns.down_since = None
                ns.evicted_logged = False

        def node_stage(node_name: str) -> str:  # noqa: E306
            ns_ = st.nodes.get(node_name)
            if not ns_ or ns_.down_since is None:
                return "ready"
            el = now - ns_.down_since
            if el < NODE_UNKNOWN_AFTER_S:
                return "down"
            if el < NODE_EVICT_AFTER_S:
                return "unknown"
            return "evicted"

        # ---- workloads: HPA coupling + replicas ----------------------------------
        node_by_name = {n.name: n for n in cluster.nodes}
        pod_usage_cpu: dict[str, float] = {}   # node -> cores used by pods
        pod_usage_mem: dict[str, int] = {}
        wl_ready: dict[str, int] = {}
        hpa_view: dict[str, tuple[int, int, int]] = {}  # wl -> (min, max, current)

        for wl in cluster.workloads.values():
            if wl.hpa:
                base = wl.replicas
                hmax = int(wl.hpa.get("max", base))
                target = float(wl.hpa.get("target_cpu", 0.7))
                load = wl_hpa_load.get(wl.name)
                hkey = (cluster.name, wl.name)
                cur = st.hpa_current.get(hkey, base)
                if load and load > 1.0:
                    mid = (wl.cpu_util[0] + wl.cpu_util[1]) / 2
                    need = math.ceil(base * mid * load / target)
                    # Scale out a few replicas per scrape up to the ceiling.
                    cur = min(hmax, max(cur, min(need, cur + 4)))
                    st.hpa_capped_until[hkey] = now + HPA_SCALE_DOWN_STABILIZATION_S
                    st.hpa_load[hkey] = load
                elif cur > base:
                    hold_until = st.hpa_capped_until.get(hkey, 0.0)
                    if now >= hold_until:
                        step = max(1, math.ceil((cur - base) * (METRICS_INTERVAL / HPA_SCALE_DOWN_DURATION_S)))
                        cur = max(base, cur - step)
                    st.hpa_load[hkey] = 1.0
                else:
                    st.hpa_load.pop(hkey, None)
                st.hpa_current[hkey] = cur
                hpa_view[wl.name] = (int(wl.hpa.get("min", 1)), hmax, cur)

        # ---- pods -----------------------------------------------------------------
        pods_by_wl: dict[str, list[Pod]] = {}
        for pod in cluster.pods:
            pods_by_wl.setdefault(pod.workload, []).append(pod)

        for wl in cluster.workloads.values():
            if wl.kind == "Job":
                continue
            pods = list(pods_by_wl.get(wl.name, []))
            if wl.hpa:
                cur = hpa_view[wl.name][2]
                # Extra replicas beyond the pinned set land on the pool's nodes.
                pool_nodes = [n for n in cluster.nodes if not wl.node_pool or n.pool == wl.node_pool]
                for i in range(len(pods), cur):
                    if not pool_nodes:
                        break
                    node = pool_nodes[(i * 5) % len(pool_nodes)]
                    r2 = random.Random(f"{wl.name}:{i}:{wl.replicaset_hash}")
                    pods.append(Pod(
                        name=f"{wl.name}-{wl.replicaset_hash}-{''.join(r2.choice('bcdfghjklmnpqrstvwxz2456789') for _ in range(5))}",
                        uid=f"{r2.getrandbits(128):032x}",
                        ip=f"{node.ip.rsplit('.', 1)[0]}.{r2.randint(10, 250)}",
                        node=node.name, workload=wl.name, kind=wl.kind,
                        namespace=wl.namespace, cluster=cluster.name,
                        container=wl.container,
                        container_id=f"containerd://{r2.getrandbits(256):064x}",
                        ordinal=i,
                    ))
                pods = pods[:cur]
            ready = 0
            load = st.hpa_load.get((cluster.name, wl.name))
            wl_all = wl
            for pod in pods:
                # During a rolling update a pod runs the template it was created with.
                tpl = pod_template(wl_all, pod, len(pods))
                wl = copy.copy(wl_all)
                for _k, _v in tpl.items():
                    setattr(wl, _k, _v)
                node = node_by_name.get(pod.node)
                stage = node_stage(pod.node)
                pst = st.pod(pod, wl)
                pst.net_rx += rng.randint(200_000, 4_000_000)
                pst.net_tx += rng.randint(200_000, 4_000_000)
                pst.faults += rng.randint(0, 2)

                # CPU (fraction of request)
                cpu_frac = _clamp(pst.cpu_base + rng.uniform(-0.04, 0.04), 0.01, 3.0)
                if load is not None and wl.hpa:
                    cur = max(1, hpa_view[wl.name][2])
                    cpu_frac = _clamp(wl.replicas * pst.cpu_base * load / cur + rng.uniform(-0.02, 0.02), 0.02, 3.0)
                throttle = pod_cpu.get(pod.uid)
                if throttle is None:
                    throttle = wl_throttle.get(wl.name)
                limit_frac = (wl.cpu_limit / wl.cpu_request) if (wl.cpu_limit and wl.cpu_request) else None
                if throttle is not None:
                    ramp = throttle
                    if limit_frac:
                        cpu_frac = _lerp(cpu_frac, limit_frac * rng.uniform(0.96, 1.0), ramp)
                    else:
                        cpu_frac = _lerp(cpu_frac, 1.6, ramp)
                if limit_frac and cpu_frac > limit_frac * 0.95:
                    # CFS throttling holds usage just under the limit, not exactly on it.
                    cpu_frac = limit_frac * rng.uniform(0.95, 0.995)
                cpu_cores = wl.cpu_request * cpu_frac

                # Memory (fraction of limit, or of 2x request when unlimited)
                mem_cap = wl.memory_limit or int(wl.memory_request * 2)
                mem_frac = _clamp(pst.mem_base + rng.uniform(-0.03, 0.03), 0.05, 0.99)
                if pod.uid in pod_mem:
                    mem_frac = max(mem_frac, pod_mem[pod.uid])
                oom_now = False
                if pod.uid in pod_oom:
                    if pst.oom_phase_start is None:
                        pst.oom_phase_start = now
                    cyc = (now - pst.oom_phase_start) / pst.oom_period
                    mem_frac = _lerp(0.62, 1.0, cyc)
                    if cyc >= 1.0:
                        oom_now = True
                        pst.oom_phase_start = now
                        pst.restarts += 1
                        pst.last_terminated = "OOMKilled"
                        pst.last_terminated_at = now
                        mem_frac = 0.99
                else:
                    pst.oom_phase_start = None

                crash_restart = False
                if pod.uid in pod_crash:
                    if now >= pst.crash_next_at:
                        crash_restart = pst.crash_next_at > 0
                        if crash_restart:
                            pst.restarts += 1
                            pst.last_terminated = "Error"
                            pst.last_terminated_at = now
                        pst.crash_next_at = now + pst.crash_backoff
                        pst.crash_backoff = min(CRASH_BACKOFF_MAX_S, pst.crash_backoff * 2)
                else:
                    pst.crash_next_at = 0.0
                    pst.crash_backoff = CRASH_BACKOFF_START_S

                if stage == "evicted" and wl.kind != "DaemonSet":
                    # Taint manager deleted it after 300 s; its replacement runs on a
                    # healthy node, so it counts as ready and this pod stops reporting.
                    ready += 1
                    if not st.nodes[pod.node].evicted_logged:
                        self._event(cluster, key=("taint-evict", pod.uid), every=3600, pod=pod,
                                    node_name=pod.node, reason="TaintManagerEviction",
                                    message=f"Marking for deletion Pod {pod.namespace}/{pod.name}",
                                    kind="Pod", obj=pod.name, namespace=pod.namespace,
                                    component="taint-eviction-controller", now=now)
                    continue
                dark = stage in ("down", "unknown", "evicted")
                evicted = pod.uid in pod_failed
                phase = PHASE_RUNNING
                if stage in ("unknown", "evicted"):
                    phase = PHASE_UNKNOWN
                if evicted:
                    phase = PHASE_FAILED
                container_ready = not (
                    dark or evicted or pod.uid in pod_crash or pod.uid in pod_not_ready
                    or (oom_now or (now - pst.last_terminated_at < 20 and pst.last_terminated == "OOMKilled"))
                )
                if container_ready:
                    ready += 1

                attrs = _pod_attrs(cluster, pod, wl, node)
                if not dark and not evicted and pod.uid not in pod_crash:
                    pod_usage_cpu[pod.node] = pod_usage_cpu.get(pod.node, 0.0) + cpu_cores
                    pod_usage_mem[pod.node] = pod_usage_mem.get(pod.node, 0) + int(mem_cap * mem_frac)
                    self._pod_kubelet(rm, kubelet_docs, attrs, wl, node, pst, cpu_cores,
                                      mem_cap, mem_frac, ts_ms)
                self._pod_cluster(rm, cluster_docs, attrs, wl, pst, container_ready, phase, ts_ms)

                if oom_now:
                    self._event(cluster, key=("oom", pod.uid, pst.restarts), every=0, pod=pod,
                                node_name=pod.node, reason="OOMKilling",
                                message=(f"Memory cgroup out of memory: Killed process {rng.randint(1000, 60000)} "
                                         f"({wl.container}) total-vm:{rng.randint(1_500_000, 4_000_000)}kB, "
                                         f"anon-rss:{int(mem_cap / 1024)}kB"),
                                kind="Node", obj=pod.node, namespace=pod.namespace,
                                component="kernel-monitor", now=now)
                if crash_restart or (oom_now and pst.restarts > 2):
                    self._event(cluster, key=("backoff", pod.uid), every=30, pod=pod,
                                node_name=pod.node, reason="BackOff",
                                message=f"Back-off restarting failed container {wl.container} in pod {pod.name}_{pod.namespace}({pod.uid})",
                                kind="Pod", obj=pod.name, namespace=pod.namespace,
                                component="kubelet", now=now)
                if evicted and pod.uid in pod_failed:
                    self._event(cluster, key=("evicted", pod.uid), every=120, pod=pod,
                                node_name=pod.node, reason="Evicted",
                                message="The node was low on resource: ephemeral-storage.",
                                kind="Pod", obj=pod.name, namespace=pod.namespace,
                                component="kubelet", now=now)

            wl = wl_all
            # Pending pods (new pods that cannot start: image pull, IPs, capacity)
            pending_hash = (pending_template_hash(wl, wl_pending_tpl[wl.name])
                            if wl.name in wl_pending_tpl
                            else f"{random.Random(f'pending-rs:{wl.name}').getrandbits(40):010x}")
            for i in range(wl_pending.get(wl.name, 0)):
                r2 = random.Random(f"pending:{wl.name}:{i}")
                suffix = "".join(r2.choice("bcdfghjklmnpqrstvwxz2456789") for _ in range(5))
                pname = f"{wl.name}-{pending_hash}-{suffix}" if wl.kind == "Deployment" else f"{wl.name}-{suffix}"
                attrs = {
                    **_cloud(cluster), **_ds("k8sclusterreceiver"),
                    "k8s.namespace.name": wl.namespace, "k8s.pod.name": pname,
                    "k8s.pod.uid": f"{r2.getrandbits(128):032x}",
                    f"k8s.{wl.kind.lower()}.name": wl.name,
                }
                rm.append({"resource": _res(attrs), "scopeMetrics": [{
                    "scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION},
                    "metrics": [_gauge("k8s.pod.phase", "1", PHASE_PENDING, is_int=True)],
                }]})

            wl_ready[wl.name] = ready
            self._workload_metrics(rm, cluster, wl, pods, ready, hpa_view.get(wl.name),
                                   wl_unavailable.get(wl.name, 0), ss_not_ready.get(wl.name, 0))
            if wl.pvc:
                self._pvc_metrics(rm, cluster, wl, pods, pvc_target.get(wl.name))

        # Jobs
        for wl in cluster.workloads.values():
            if wl.kind != "Job":
                continue
            failed = wl_job_failed.get(wl.name, 0)
            attrs = {**_cloud(cluster), **_ds("k8sclusterreceiver"),
                     "k8s.namespace.name": wl.namespace, "k8s.job.name": wl.name}
            succ = 0 if failed else 1
            rm.append({"resource": _res(attrs), "scopeMetrics": [{
                "scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION},
                "metrics": [
                    _gauge("k8s.job.active_pods", "{pod}", 0 if succ else 1, is_int=True),
                    _gauge("k8s.job.desired_successful_pods", "{pod}", 1, is_int=True),
                    _gauge("k8s.job.successful_pods", "{pod}", succ, is_int=True),
                    _gauge("k8s.job.failed_pods", "{pod}", failed, is_int=True),
                    _gauge("k8s.job.max_parallel_pods", "{pod}", 1, is_int=True),
                ],
            }]})

        # ---- nodes ----------------------------------------------------------------
        for node in cluster.nodes:
            ns = st.node(node)
            stage = node_stage(node.name)
            ns.net_rx += rng.randint(2_000_000, 40_000_000)
            ns.net_tx += rng.randint(2_000_000, 40_000_000)
            target = node_fs_target.get(node.name)
            if target is not None:
                diff = target - ns.fs_fill
                # filling follows the writer closely; freeing space is slower
                step = max(0.02, 0.6 * diff) if diff > 0 else min(abs(diff), 0.02)
                ns.fs_fill = _clamp(ns.fs_fill + math.copysign(min(abs(diff), step), diff), 0.01, 0.99)
            else:
                drift = rng.uniform(-0.004, 0.004)
                if ns.fs_fill > 0.6:
                    drift = -0.02  # images garbage-collected / evicted pods cleaned up
                ns.fs_fill = _clamp(ns.fs_fill + drift, 0.15, 0.65)
            alloc_cpu = max(0.5, node.cpu - 0.17 - 0.06 * node.cpu)
            alloc_mem = int(node.memory_bytes * 0.88)
            cpu_util = _clamp(pod_usage_cpu.get(node.name, 0.0) / alloc_cpu + NODE_SYSTEM_CPU + rng.uniform(0, 0.03), 0.02, 1.0)
            if node.name in node_cpu:
                cpu_util = max(cpu_util, node_cpu[node.name])
            mem_used = pod_usage_mem.get(node.name, 0) + int(node.memory_bytes * NODE_SYSTEM_MEM)
            mem_util = _clamp(mem_used / alloc_mem, 0.05, 1.0)
            if node.name in node_mem:
                mem_util = max(mem_util, node_mem[node.name])
            disk_pressure = 1 if ns.fs_fill >= DISK_PRESSURE_FILL else 0
            mem_pressure = 1 if (mem_util >= 0.9 or node.name in node_mem_pressure) else 0
            ready = 0 if stage != "ready" else 1
            attrs = _node_attrs(cluster, node)

            if stage == "ready":
                fs_usage = int(ns.fs_cap * ns.fs_fill)
                mem_ws = int(alloc_mem * mem_util)
                k_metrics = [
                    _gauge("k8s.node.cpu.usage", "1", round(alloc_cpu * cpu_util, 6)),
                    _gauge("k8s.node.cpu.utilization", "1", round(cpu_util, 6)),
                    _gauge("k8s.node.memory.usage", "By", int(mem_ws * 1.04), is_int=True),
                    _gauge("k8s.node.memory.working_set", "By", mem_ws, is_int=True),
                    _gauge("k8s.node.memory.rss", "By", int(mem_ws * 0.82), is_int=True),
                    _gauge("k8s.node.memory.available", "By", max(0, alloc_mem - int(mem_ws * 1.04)), is_int=True),
                    _gauge("k8s.node.memory.utilization", "1", round(mem_util, 6)),
                    _gauge("k8s.node.filesystem.usage", "By", fs_usage, is_int=True),
                    _gauge("k8s.node.filesystem.capacity", "By", ns.fs_cap, is_int=True),
                    _gauge("k8s.node.filesystem.available", "By", max(0, ns.fs_cap - fs_usage), is_int=True),
                    _gauge("k8s.node.filesystem.utilization", "1", round(ns.fs_fill, 6)),
                    _cumulative_sum("k8s.node.network.io", "By", ns.net_rx, attributes={"direction": "receive"}),
                    _cumulative_sum("k8s.node.network.io", "By", ns.net_tx, attributes={"direction": "transmit"}),
                ]
                k_res = _res({**attrs, **_ds("kubeletstatsreceiver")})
                rm.append({"resource": k_res, "scopeMetrics": [{
                    "scope": {"name": KUBELET_SCOPE, "version": SCOPE_VERSION}, "metrics": k_metrics}]})
                kubelet_docs.append(build_node_util_doc(k_res, cpu_util=cpu_util, mem_util=mem_util, timestamp_ms=ts_ms))
            c_metrics = [
                _gauge("k8s.node.allocatable_cpu", "1", round(alloc_cpu, 3)),
                _gauge("k8s.node.allocatable_memory", "By", alloc_mem, is_int=True),
                _gauge("k8s.node.condition_ready", "1", ready, is_int=True),
                _gauge("k8s.node.condition_memory_pressure", "1", mem_pressure, is_int=True),
                _gauge("k8s.node.condition_disk_pressure", "1", disk_pressure, is_int=True),
            ]
            rm.append({"resource": _res({**attrs, **_ds("k8sclusterreceiver")}), "scopeMetrics": [{
                "scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": c_metrics}]})

            if stage != "ready":
                self._event(cluster, key=("nodenotready", node.name), every=60, pod=None,
                            node_name=node.name, reason="NodeNotReady",
                            message=f"Node {node.name} status is now: NodeNotReady",
                            kind="Node", obj=node.name, namespace="default",
                            component="node-controller", now=now)
            if disk_pressure:
                self._event(cluster, key=("diskpressure", node.name), every=120, pod=None,
                            node_name=node.name, reason="NodeHasDiskPressure",
                            message=f"Node {node.name} status is now: NodeHasDiskPressure",
                            kind="Node", obj=node.name, namespace="default",
                            component="kubelet", now=now)

        # ---- namespaces -----------------------------------------------------------
        for ns_name in sorted({wl.namespace for wl in cluster.workloads.values()}):
            rm.append({"resource": _res({**_cloud(cluster), **_ds("k8sclusterreceiver"),
                                         "k8s.namespace.name": ns_name}),
                       "scopeMetrics": [{"scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION},
                                         "metrics": [_gauge("k8s.namespace.phase", "1", 1, is_int=True)]}]})

        # ---- channel-declared k8s events -------------------------------------------
        for f in faults:
            for idx, evt in enumerate(f.k8s_events):
                self._channel_event(cluster, f, idx, evt, now)

        # ---- background noise: the odd readiness blip on a healthy pod --------------
        if cluster.pods and rng.random() < 0.08:
            pod = rng.choice(cluster.pods)
            self._event(cluster, key=("noise", pod.uid), every=600, pod=pod, node_name=pod.node,
                        reason="Unhealthy",
                        message="Readiness probe failed: Get \"http://%s:8080/healthz\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)" % pod.ip,
                        kind="Pod", obj=pod.name, namespace=pod.namespace,
                        component="kubelet", now=now)

    # -- emit helpers ----------------------------------------------------------

    def _pod_kubelet(self, rm, docs, attrs, wl: Workload, node: Node | None, pst: _PodState,
                     cpu_cores: float, mem_cap: int, mem_frac: float, ts_ms: int) -> None:
        mem_usage = int(mem_cap * mem_frac)
        mem_ws = int(mem_usage * 0.94)
        alloc_cpu = node.cpu if node else 4.0
        node_mem = node.memory_bytes if node else 16 * 2**30
        metrics = [
            _gauge("k8s.pod.cpu.usage", "1", round(cpu_cores, 6)),
            _gauge("k8s.pod.cpu.node.utilization", "1", round(cpu_cores / alloc_cpu, 6)),
            _gauge("k8s.pod.memory.usage", "By", mem_usage, is_int=True),
            _gauge("k8s.pod.memory.working_set", "By", mem_ws, is_int=True),
            _gauge("k8s.pod.memory.rss", "By", int(mem_ws * 0.85), is_int=True),
            _gauge("k8s.pod.memory.node.utilization", "1", round(mem_usage / node_mem, 6)),
            _cumulative_sum("k8s.pod.network.io", "By", pst.net_rx, attributes={"direction": "receive"}),
            _cumulative_sum("k8s.pod.network.io", "By", pst.net_tx, attributes={"direction": "transmit"}),
            _gauge("k8s.pod.filesystem.usage", "By", 120_000_000 + (pst.faults % 50) * 1_000_000, is_int=True),
        ]
        c_attrs = {"container.name": wl.container, "k8s.container.name": wl.container}
        cpu_lim_util = None
        if wl.cpu_limit:
            cpu_lim_util = cpu_cores / wl.cpu_limit
            metrics.append(_gauge("k8s.pod.cpu_limit_utilization", "1", round(cpu_lim_util, 6)))
            metrics.append(_gauge("k8s.container.cpu_limit_utilization", "1", round(cpu_lim_util, 6), attributes=c_attrs))
        mem_lim_util = None
        if wl.memory_limit:
            mem_lim_util = mem_usage / wl.memory_limit
            metrics.append(_gauge("k8s.pod.memory_limit_utilization", "1", round(mem_lim_util, 6)))
            metrics.append(_gauge("k8s.pod.memory.available", "By", max(0, wl.memory_limit - mem_usage), is_int=True))
            metrics.append(_gauge("k8s.container.memory_limit_utilization", "1", round(mem_lim_util, 6), attributes=c_attrs))
        metrics += [
            _gauge("container.cpu.usage", "1", round(cpu_cores, 6), attributes=c_attrs),
            _gauge("container.memory.working_set", "By", mem_ws, is_int=True, attributes=c_attrs),
            _cumulative_sum("container.memory.major_page_faults", "1", pst.faults, attributes=c_attrs),
        ]
        res = _res({**attrs, **_ds("kubeletstatsreceiver")})
        rm.append({"resource": res, "scopeMetrics": [{
            "scope": {"name": KUBELET_SCOPE, "version": SCOPE_VERSION}, "metrics": metrics}]})
        docs.append(build_pod_memory_doc(res, mem_usage, timestamp_ms=ts_ms))
        docs.append(build_pod_util_doc(
            res, cpu_node_util=cpu_cores / alloc_cpu, mem_working_set=mem_ws,
            cpu_limit_util=cpu_lim_util, mem_limit_util=mem_lim_util, timestamp_ms=ts_ms,
        ))

    def _pod_cluster(self, rm, docs, attrs, wl: Workload, pst: _PodState,
                     ready: bool, phase: int, ts_ms: int) -> None:
        c_attrs = {**attrs, **_ds("k8sclusterreceiver"),
                   "k8s.container.status.last_terminated_reason": pst.last_terminated}
        metrics = [
            _gauge("k8s.container.restarts", "{restart}", pst.restarts, is_int=True),
            _gauge("k8s.container.ready", "1", 1 if ready else 0, is_int=True),
            _gauge("k8s.container.cpu_request", "{cpu}", round(wl.cpu_request, 4)),
            _gauge("k8s.container.memory_request", "By", wl.memory_request, is_int=True),
        ]
        if wl.cpu_limit:
            metrics.append(_gauge("k8s.container.cpu_limit", "{cpu}", round(wl.cpu_limit, 4)))
        if wl.memory_limit:
            metrics.append(_gauge("k8s.container.memory_limit", "By", wl.memory_limit, is_int=True))
        res = _res(c_attrs)
        rm.append({"resource": res, "scopeMetrics": [{
            "scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION}, "metrics": metrics}]})
        docs.append(build_container_status_doc(res, pst.restarts, timestamp_ms=ts_ms))
        p_attrs = {k: v for k, v in attrs.items() if not k.startswith("container.")}
        rm.append({"resource": _res({**p_attrs, **_ds("k8sclusterreceiver")}), "scopeMetrics": [{
            "scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION},
            "metrics": [_gauge("k8s.pod.phase", "1", phase, is_int=True)]}]})

    def _workload_metrics(self, rm, cluster: Cluster, wl: Workload, pods: list[Pod], ready: int,
                          hpa: tuple[int, int, int] | None, unavailable: int, ss_not_ready: int) -> None:
        base = {**_cloud(cluster), **_ds("k8sclusterreceiver"), "k8s.namespace.name": wl.namespace}
        if wl.kind == "Deployment":
            desired = len(pods)
            available = max(0, min(ready, desired - unavailable))
            rm.append({"resource": _res({**base, "k8s.deployment.name": wl.name}), "scopeMetrics": [{
                "scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION},
                "metrics": [
                    _gauge("k8s.deployment.desired", "1", desired, is_int=True),
                    _gauge("k8s.deployment.available", "1", available, is_int=True),
                ]}]})
            rm.append({"resource": _res({**base, "k8s.replicaset.name": f"{wl.name}-{wl.replicaset_hash}",
                                         "k8s.deployment.name": wl.name}), "scopeMetrics": [{
                "scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION},
                "metrics": [
                    _gauge("k8s.replicaset.desired", "1", desired, is_int=True),
                    _gauge("k8s.replicaset.available", "1", available, is_int=True),
                ]}]})
        elif wl.kind == "DaemonSet":
            desired = len(pods)
            rm.append({"resource": _res({**base, "k8s.daemonset.name": wl.name}), "scopeMetrics": [{
                "scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION},
                "metrics": [
                    _gauge("k8s.daemonset.desired_scheduled_nodes", "1", desired, is_int=True),
                    _gauge("k8s.daemonset.current_scheduled_nodes", "1", desired, is_int=True),
                    _gauge("k8s.daemonset.ready_nodes", "1", ready, is_int=True),
                    _gauge("k8s.daemonset.misscheduled_nodes", "1", 0, is_int=True),
                ]}]})
        elif wl.kind == "StatefulSet":
            desired = len(pods)
            rdy = max(0, min(ready, desired - ss_not_ready))
            rm.append({"resource": _res({**base, "k8s.statefulset.name": wl.name}), "scopeMetrics": [{
                "scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION},
                "metrics": [
                    _gauge("k8s.statefulset.desired_pods", "1", desired, is_int=True),
                    _gauge("k8s.statefulset.ready_pods", "1", rdy, is_int=True),
                    _gauge("k8s.statefulset.current_pods", "1", desired, is_int=True),
                    _gauge("k8s.statefulset.updated_pods", "1", desired, is_int=True),
                ]}]})
        if hpa is not None:
            hmin, hmax, cur = hpa
            rm.append({"resource": _res({**base, "k8s.hpa.name": wl.name}), "scopeMetrics": [{
                "scope": {"name": CLUSTER_SCOPE, "version": SCOPE_VERSION},
                "metrics": [
                    _gauge("k8s.hpa.max_replicas", "1", hmax, is_int=True),
                    _gauge("k8s.hpa.min_replicas", "1", hmin, is_int=True),
                    _gauge("k8s.hpa.desired_replicas", "1", cur, is_int=True),
                    _gauge("k8s.hpa.current_replicas", "1", cur, is_int=True),
                ]}]})

    def _pvc_metrics(self, rm, cluster: Cluster, wl: Workload, pods: list[Pod],
                     fault_target: float | None) -> None:
        pvc = wl.pvc or {}
        cap = parse_bytes(pvc.get("capacity", "100Gi"))
        name = pvc.get("name", f"{wl.name}-data")
        key = f"{cluster.name}/{wl.namespace}/{name}"
        used = self.state.pvc_used.get(key)
        lo, hi = (pvc.get("used") or [0.4, 0.5])[:2] if isinstance(pvc.get("used"), list) else (0.4, 0.5)
        if isinstance(pvc.get("used"), (int, float)):
            lo = hi = float(pvc["used"])
        if used is None:
            used = int(cap * (lo + hi) / 2)
        target_frac = fault_target if fault_target is not None else (lo + hi) / 2
        target = int(cap * target_frac)
        if pvc.get("hold_used") and fault_target is None:
            target = used  # waiting for the fix's cleanup to free space
        step = int(cap * 0.02)
        if target < used:
            step = max(step, (used - target) // 2)  # freed in bulk (GC, deleted data)
        elif fault_target is not None:
            # a volume filling up keeps pace with the fault's errors (writes fail when it
            # is full), like the node disk fill
            step = max(step, int(0.6 * (target - used)))
        if abs(target - used) <= step:
            used = target
        else:
            used += step if target > used else -step
        used = max(0, min(used, cap))
        self.state.pvc_used[key] = used
        pod = pods[0] if pods else None
        attrs = {**_cloud(cluster), **_ds("kubeletstatsreceiver"),
                 "k8s.namespace.name": wl.namespace,
                 "k8s.volume.name": name, "k8s.volume.type": "persistentVolumeClaim",
                 "k8s.persistentvolumeclaim.name": name}
        if pod:
            attrs.update({"k8s.pod.name": pod.name, "k8s.pod.uid": pod.uid, "k8s.node.name": pod.node})
        rm.append({"resource": _res(attrs), "scopeMetrics": [{
            "scope": {"name": KUBELET_SCOPE, "version": SCOPE_VERSION},
            "metrics": [
                _gauge("k8s.volume.capacity", "By", cap, is_int=True),
                _gauge("k8s.volume.available", "By", cap - used, is_int=True),
            ]}]})

    def _channel_event(self, cluster: Cluster, f: _Fault, idx: int, evt: dict, now: float) -> None:
        obj = dict(evt.get("object") or {})
        kind = obj.get("kind", "Pod")
        pod = f.pods[(self.scrape + idx) % len(f.pods)] if f.pods else None
        node = None
        if f.nodes:
            node = f.nodes[(self.scrape + idx) % len(f.nodes)]
        elif pod:
            node = cluster.node(pod.node)
        params = {
            "self_pod": pod.name if pod else "",
            "self_node": node.name if node else "",
            "node_count": len(cluster.nodes),
            "scope_node_count": len(f.nodes),
            "other_node_count": len(cluster.nodes) - len(f.nodes),
        }
        for pool in cluster.pools:
            params[f"pool_{pool.replace('-', '_')}_count"] = len(cluster.nodes_in_pool(pool))
        name = str(obj.get("name") or "").format(**params) if obj.get("name") else None
        if name in (None, "", "{self_pod}") and kind == "Pod" and pod:
            name = pod.name
        if kind == "Node" and not name and node:
            name = node.name
        try:
            message = str(evt.get("message", "")).format(**params)
        except (KeyError, IndexError, ValueError):
            message = str(evt.get("message", ""))
        self._event(
            cluster, key=("ch", f.channel, idx, name if evt.get("per_object") else ""),
            every=float(evt.get("every_seconds", 30)),
            pod=pod if kind == "Pod" else None,
            node_name=node.name if node else "",
            reason=evt.get("reason", "Warning"), message=message, kind=kind, obj=name,
            namespace=obj.get("namespace") or (pod.namespace if pod else "default"),
            component=evt.get("component", "kubelet"), now=now,
        )


def run_pinned(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict,
    chaos_controller=None,
) -> None:
    tick = _Tick(client, scenario_data, chaos_controller)
    if tick.base is None:
        logger.error("k8s_topology declared but produced no platform; generator idle")
        return
    logger.info(
        "K8s metrics generator (pinned topology) started: clusters=%d nodes=%d pods=%d",
        len(tick.base.clusters),
        sum(len(c.nodes) for c in tick.base.clusters.values()),
        sum(len(c.pods) for c in tick.base.clusters.values()),
    )
    run_tick_loop(stop_event, tick, interval=lambda: METRICS_INTERVAL)
