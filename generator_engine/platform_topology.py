"""Pinned Kubernetes platform model (scenario schema 1.3 ``k8s_topology``).

The default ``infra_topology`` gives every cluster three seeded nodes and one pod per
service. That is fine for application scenarios, but an infrastructure story needs the
numbers to agree everywhere: a scheduler message that says ``0/51 nodes are available``
must match the node count in the metrics, a CoreDNS log that reads from ``10.42.33.12``
must come from a pod with that IP, and a node named ``ip-10-42-36-17.ec2.internal``
must sit in the us-east-1b subnet.

``build_platform`` turns the declarative ``k8s_topology`` block into that model:
clusters, node pools spread across AZ subnets, nodes with EKS-style names, workloads
(Deployments, DaemonSets, StatefulSets) and their pods with IPs from their node's
subnet. Everything is deterministic per scenario namespace.

``apply_state`` derives a new model from platform-state overrides (a fix that raised a
memory limit, added Spot instance types or replaced a node), so the k8s generator, the
services' log identity and the deployer all read the same post-remediation values.

Only scenarios that declare ``k8s_topology`` use this module; every other scenario keeps
the generated layout from ``infra_topology``.
"""

from __future__ import annotations

import copy
import ipaddress
import random
import re
import uuid
from dataclasses import dataclass, field
from typing import Any

# ── Units ────────────────────────────────────────────────────────────────────

_MEM_UNITS = {
    "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40,
    "K": 10**3, "M": 10**6, "G": 10**9, "T": 10**12,
}


def parse_bytes(value: Any) -> int:
    """``"3Gi"`` → 3221225472. Plain numbers pass through as bytes."""
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    m = re.fullmatch(r"\s*([0-9.]+)\s*([KMGT]i?)?\s*", str(value))
    if not m:
        raise ValueError(f"cannot parse quantity {value!r}")
    num, unit = m.groups()
    return int(float(num) * (_MEM_UNITS[unit] if unit else 1))


def format_bytes(n: int) -> str:
    for unit in ("Ti", "Gi", "Mi", "Ki"):
        size = _MEM_UNITS[unit]
        if n >= size and n % size == 0:
            return f"{n // size}{unit}"
    return str(n)


def parse_cpu(value: Any) -> float:
    """``"500m"`` → 0.5, ``2`` → 2.0."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if s.endswith("m"):
        return float(s[:-1]) / 1000.0
    return float(s)


# ── Model ────────────────────────────────────────────────────────────────────


@dataclass
class Node:
    name: str
    ip: str
    az: str
    pool: str
    cluster: str
    instance_type: str
    capacity_type: str
    arch: str
    os_image: str
    kubelet_version: str
    cpu: float
    memory_bytes: int
    uid: str
    instance_id: str
    labels: dict[str, str] = field(default_factory=dict)
    taints: list[str] = field(default_factory=list)
    joined_at_offset: float = 0.0  # seconds after a fix; >0 means still joining

    @property
    def taint_keys(self) -> set[str]:
        return {t.split("=", 1)[0].split(":", 1)[0] for t in self.taints}


@dataclass
class Workload:
    name: str
    kind: str
    namespace: str
    cluster: str
    service: str | None
    node_pool: str | None
    replicas: int
    image: str
    container: str
    cpu_request: float
    cpu_limit: float | None
    memory_request: int
    memory_limit: int | None
    cpu_util: tuple[float, float]
    memory_util: tuple[float, float]
    hpa: dict[str, Any] | None
    pvc: dict[str, Any] | None
    tolerations: list[str]
    replicaset_hash: str
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class Pod:
    name: str
    uid: str
    ip: str
    node: str
    workload: str
    kind: str
    namespace: str
    cluster: str
    container: str
    container_id: str
    ordinal: int


@dataclass
class Cluster:
    name: str
    provider: str
    platform: str
    region: str
    account_id: str
    vpc_cidr: str
    service_cidr: str
    kube_dns_ip: str
    subnets: dict[str, str]
    node_name_format: str
    pools: dict[str, dict[str, Any]]
    nodes: list[Node]
    workloads: dict[str, Workload]
    pods: list[Pod]
    uid: str

    @property
    def zones(self) -> list[str]:
        return list(self.subnets)

    def nodes_in_pool(self, pool: str) -> list[Node]:
        return [n for n in self.nodes if n.pool == pool]

    def pods_of(self, workload: str) -> list[Pod]:
        return [p for p in self.pods if p.workload == workload]

    def node(self, name: str) -> Node | None:
        return next((n for n in self.nodes if n.name == name), None)


@dataclass
class Platform:
    namespace: str
    clusters: dict[str, Cluster]
    default_cluster: str

    @property
    def default(self) -> Cluster:
        return self.clusters[self.default_cluster]

    def cluster(self, name: str | None) -> Cluster:
        return self.clusters.get(name or self.default_cluster, self.default)

    def workload_for_service(self, service: str) -> tuple[Cluster, Workload] | None:
        c = self.default
        for wl in c.workloads.values():
            if wl.service == service:
                return c, wl
        return None

    # Scope resolution (schema 1.3 fault_scope)
    def nodes_in_scope(self, scope: dict[str, Any] | None) -> list[Node]:
        scope = scope or {}
        c = self.cluster(scope.get("cluster"))
        nodes = list(c.nodes)
        if scope.get("nodes"):
            wanted = set(scope["nodes"])
            nodes = [n for n in nodes if n.name in wanted]
        if scope.get("node_pool"):
            pools = scope["node_pool"]
            pools = {pools} if isinstance(pools, str) else set(pools)
            nodes = [n for n in nodes if n.pool in pools]
        if scope.get("az"):
            azs = scope["az"]
            azs = {azs} if isinstance(azs, str) else set(azs)
            nodes = [n for n in nodes if n.az in azs]
        return nodes

    def pods_in_scope(self, scope: dict[str, Any] | None, workload: str | None = None) -> list[Pod]:
        scope = scope or {}
        c = self.cluster(scope.get("cluster"))
        node_names = {n.name for n in self.nodes_in_scope(scope)}
        pods = [p for p in c.pods if p.node in node_names]
        wls = scope.get("workloads")
        if workload:
            pods = [p for p in pods if p.workload == workload]
        elif wls:
            pods = [p for p in pods if p.workload in set(wls)]
        if scope.get("namespaces"):
            pods = [p for p in pods if p.namespace in set(scope["namespaces"])]
        return pods


# ── Builders ─────────────────────────────────────────────────────────────────


def _host_pool(subnet: str, rng: random.Random, reserved: set[str]) -> "list[str]":
    net = ipaddress.ip_network(subnet, strict=False)
    hosts = [str(h) for h in net.hosts()][4:-1]  # AWS reserves .0-.3 and the last
    rng.shuffle(hosts)
    return [h for h in hosts if h not in reserved]


def _node_name(fmt: str, ip: str, region: str) -> str:
    return fmt.format(ip_dashed=ip.replace(".", "-"), region=region)


def _suffix(rng: random.Random, n: int = 5) -> str:
    alphabet = "bcdfghjklmnpqrstvwxz2456789"
    return "".join(rng.choice(alphabet) for _ in range(n))


def _image_container(image: str, name: str) -> str:
    base = image.rsplit("/", 1)[-1].split(":", 1)[0].split("@", 1)[0]
    return base or name


def _make_node(
    *, cluster: dict, pool: dict, ip: str, az: str, rng: random.Random,
    instance_type: str | None = None, name: str | None = None,
    instance_id: str | None = None,
) -> Node:
    fmt = cluster.get("node_name_format", "ip-{ip_dashed}.ec2.internal")
    region = cluster["region"]
    node_name = name or _node_name(fmt, ip, region)
    return Node(
        name=node_name,
        ip=ip,
        az=az,
        pool=pool["name"],
        cluster=cluster["name"],
        instance_type=instance_type or (pool.get("instance_types") or ["m6i.large"])[0],
        capacity_type=pool.get("capacity_type", "ON_DEMAND"),
        arch=pool.get("arch", "amd64"),
        os_image=pool.get("os_image", "Amazon Linux 2023"),
        kubelet_version=pool.get("kubelet_version", "v1.30.4-eks-a737599"),
        cpu=float(pool.get("cpu", 4)),
        memory_bytes=int(float(pool.get("memory_gib", 16)) * 2**30),
        uid=str(uuid.UUID(int=rng.getrandbits(128))),
        instance_id=instance_id or ("i-0" + f"{rng.getrandbits(64):016x}"),
        labels=dict(pool.get("labels") or {}),
        taints=list(pool.get("taints") or []),
    )


def _build_cluster(cfg: dict, namespace: str, host_ids: dict | None = None) -> Cluster:
    from generator_engine.identity import ScenarioIdentity

    rng = random.Random(f"{namespace}:platform:{cfg['name']}")
    ident = ScenarioIdentity(namespace)
    host_ids = host_ids or {}
    subnets: dict[str, str] = dict(cfg.get("subnets") or {})
    if not subnets:
        # Minimal declaration (comparison cluster): one /20 per listed zone.
        base = ipaddress.ip_network(cfg.get("vpc_cidr", "10.60.0.0/16"))
        subs = list(base.subnets(new_prefix=20))
        subnets = {z: str(subs[i]) for i, z in enumerate(cfg.get("zones") or [cfg["region"] + "a"])}
    zones = list(subnets)
    reserved: set[str] = set()
    for pool in cfg.get("node_pools") or []:
        for n in pool.get("nodes") or []:
            reserved.add(n["ip"] if isinstance(n, dict) else _ip_from_name(n))
    free = {z: _host_pool(s, rng, reserved) for z, s in subnets.items()}

    pools: dict[str, dict] = {}
    nodes: list[Node] = []
    for pool in cfg.get("node_pools") or []:
        pools[pool["name"]] = pool
        pinned = list(pool.get("nodes") or [])
        pool_zones = pool.get("zones") or zones
        for i in range(int(pool.get("count", 0))):
            if i < len(pinned):
                p = pinned[i]
                ip = p["ip"] if isinstance(p, dict) else _ip_from_name(p)
                az = _az_for_ip(ip, subnets) or pool_zones[i % len(pool_zones)]
                name = p.get("name") if isinstance(p, dict) else p
            else:
                az = pool_zones[i % len(pool_zones)]
                ip = free[az].pop()
                name = None
            node_name = name or _node_name(
                cfg.get("node_name_format", "ip-{ip_dashed}.ec2.internal"), ip, cfg["region"]
            )
            iid = host_ids.get(node_name) or ident.aws_id(
                "instance", f"{cfg['name']}:{pool['name']}", i + 1
            )
            nodes.append(_make_node(cluster=cfg, pool=pool, ip=ip, az=az, rng=rng, name=name,
                                    instance_id=iid))

    cluster = Cluster(
        name=cfg["name"],
        provider=cfg.get("provider", "aws"),
        platform=cfg.get("platform", "aws_eks"),
        region=cfg["region"],
        account_id=str(cfg.get("account_id", "")),
        vpc_cidr=cfg.get("vpc_cidr", ""),
        service_cidr=cfg.get("service_cidr", "172.20.0.0/16"),
        kube_dns_ip=cfg.get("kube_dns_ip", "172.20.0.10"),
        subnets=subnets,
        node_name_format=cfg.get("node_name_format", "ip-{ip_dashed}.ec2.internal"),
        pools=pools,
        nodes=nodes,
        workloads={},
        pods=[],
        uid=str(uuid.UUID(int=rng.getrandbits(128))),
    )
    cluster._free_ips = free  # type: ignore[attr-defined]
    cluster._rng = rng  # type: ignore[attr-defined]

    for wcfg in cfg.get("workloads") or []:
        wl = _make_workload(wcfg, cluster, rng)
        cluster.workloads[wl.name] = wl
    _place_pods(cluster)
    return cluster


def _ip_from_name(name: str) -> str:
    m = re.match(r"ip-(\d+)-(\d+)-(\d+)-(\d+)", name)
    if not m:
        raise ValueError(f"pinned node {name!r} must look like ip-a-b-c-d...")
    return ".".join(m.groups())


def _az_for_ip(ip: str, subnets: dict[str, str]) -> str | None:
    addr = ipaddress.ip_address(ip)
    for az, cidr in subnets.items():
        if addr in ipaddress.ip_network(cidr, strict=False):
            return az
    return None


def _make_workload(wcfg: dict, cluster: Cluster, rng: random.Random) -> Workload:
    image = wcfg.get("image", f"{wcfg['name']}:latest")
    cpu_limit = wcfg.get("cpu_limit")
    mem_limit = wcfg.get("memory_limit")
    return Workload(
        name=wcfg["name"],
        kind=wcfg.get("kind", "Deployment"),
        namespace=wcfg.get("namespace", "default"),
        cluster=cluster.name,
        service=wcfg.get("service"),
        node_pool=wcfg.get("node_pool"),
        replicas=int(wcfg.get("replicas", 1)),
        image=image,
        container=wcfg.get("container") or _image_container(image, wcfg["name"]),
        cpu_request=parse_cpu(wcfg.get("cpu_request", 0.1)),
        cpu_limit=parse_cpu(cpu_limit) if cpu_limit is not None else None,
        memory_request=parse_bytes(wcfg.get("memory_request", "128Mi")),
        memory_limit=parse_bytes(mem_limit) if mem_limit is not None else None,
        cpu_util=tuple(wcfg.get("cpu_util", [0.25, 0.45])),  # type: ignore[arg-type]
        memory_util=tuple(wcfg.get("memory_util", [0.40, 0.60])),  # type: ignore[arg-type]
        hpa=dict(wcfg["hpa"]) if wcfg.get("hpa") else None,
        pvc=dict(wcfg["pvc"]) if wcfg.get("pvc") else None,
        tolerations=list(wcfg.get("tolerations") or []),
        replicaset_hash=f"{rng.getrandbits(40):010x}"[:10],
        extra={k: v for k, v in wcfg.items() if k in ("arch_images", "selector", "labels", "placement")},
    )


def _eligible_nodes(cluster: Cluster, wl: Workload) -> list[Node]:
    if wl.kind == "DaemonSet":
        nodes = [n for n in cluster.nodes if not wl.node_pool or n.pool == wl.node_pool]
        # A DaemonSet only targets nodes whose NoSchedule taints it tolerates.
        tol = set(wl.tolerations)
        return [n for n in nodes if not (n.taint_keys - tol)]
    pool = wl.node_pool
    nodes = [n for n in cluster.nodes if not pool or n.pool == pool]
    return nodes or list(cluster.nodes)


def _alloc_pod_ip(cluster: Cluster, node: Node, prng: random.Random | None = None) -> str:
    free = cluster._free_ips[node.az]  # type: ignore[attr-defined]
    if prng is None:
        return free.pop()
    return free.pop(prng.randrange(len(free)))


def _place_pods(cluster: Cluster, *, stable: bool = False) -> None:
    rng: random.Random = cluster._rng  # type: ignore[attr-defined]
    existing = {(p.workload, p.ordinal): p for p in cluster.pods}
    pods: list[Pod] = []
    for wl in cluster.workloads.values():
        if wl.kind in ("Job", "CronJob"):
            continue  # run-to-completion: no long-lived pods to place
        nodes = _eligible_nodes(cluster, wl)
        if not nodes:
            continue
        placement = wl.extra.get("placement")
        if wl.kind == "DaemonSet":
            targets = list(enumerate(nodes))
        elif placement:
            # Explicit per-replica pools (e.g. one CoreDNS replica on a new pool).
            targets = []
            for i in range(wl.replicas):
                pool = placement[i % len(placement)]
                pool_nodes = [n for n in cluster.nodes if n.pool == pool] or nodes
                targets.append((i, pool_nodes[(i * 7) % len(pool_nodes)]))
        else:
            # Spread replicas across nodes round-robin, offset per workload for variety.
            start = (sum(ord(ch) for ch in wl.name)) % len(nodes)
            targets = [(i, nodes[(start + i * 7) % len(nodes)]) for i in range(wl.replicas)]
        for i, node in targets:
            prev = existing.get((wl.name, i))
            ro = wl.extra.get("rollout")
            rs_hash = wl.replicaset_hash
            if (ro and i >= int(round(float(ro["progress"]) * max(1, len(targets))))
                    and prev is not None and prev.node == node.name
                    and prev.name.startswith(f"{wl.name}-{ro['old_hash']}-")):
                rs_hash = ro["old_hash"]  # not replaced yet: still the old ReplicaSet's pod
            # (a pod that has to be created, e.g. because the replica moved node, comes
            # from the current template: an old ReplicaSet never creates pods mid-rollout)
            same_rs = wl.kind != "Deployment" or prev is None or prev.name.startswith(
                f"{wl.name}-{rs_hash}-"
            )
            if prev and prev.node == node.name and same_rs:
                pods.append(prev)
                continue
            # Pods created by a state override take their identity from who they are,
            # not from the order they were created in: the same pod keeps its name, uid
            # and IP from one tick to the next while a rollout progresses.
            prng = (random.Random(f"{cluster.name}:{wl.name}:{rs_hash}:{i}:{node.name}")
                    if stable else rng)
            if wl.kind == "StatefulSet":
                pname = f"{wl.name}-{i}"
            elif wl.kind == "DaemonSet":
                pname = f"{wl.name}-{_suffix(prng)}"
            else:
                pname = f"{wl.name}-{rs_hash}-{_suffix(prng)}"
            pods.append(Pod(
                name=pname,
                uid=str(uuid.UUID(int=prng.getrandbits(128))),
                ip=_alloc_pod_ip(cluster, node, prng if stable else None),
                node=node.name,
                workload=wl.name,
                kind=wl.kind,
                namespace=wl.namespace,
                cluster=cluster.name,
                container=wl.container,
                container_id=f"containerd://{prng.getrandbits(256):064x}",
                ordinal=i,
            ))
    cluster.pods = pods


def build_platform(topology: dict[str, Any], namespace: str) -> Platform:
    clusters = {}
    for cfg in topology.get("clusters") or []:
        c = _build_cluster(cfg, namespace, topology.get("_host_ids"))
        clusters[c.name] = c
    default = topology.get("default_cluster") or next(iter(clusters))
    return Platform(namespace=namespace, clusters=clusters, default_cluster=default)


_cache: dict[tuple[str, int], Platform] = {}


def get_platform(topology: dict[str, Any] | None, namespace: str) -> Platform | None:
    """Cached baseline model for a scenario (None when it declares no topology)."""
    if not topology:
        return None
    key = (namespace, id(topology))
    p = _cache.get(key)
    if p is None:
        p = build_platform(topology, namespace)
        _cache[key] = p
    return p


# ── State overrides (fault_state / remediation_effects.state) ────────────────


_TEMPLATE_FIELDS = ("image", "cpu_request", "cpu_limit", "memory_request", "memory_limit")
# Pod-template settings kept in Workload.extra (set by fault_state / fixes): node
# affinity and the pod dnsConfig. ``placement`` is not one: it records where the
# scheduler put the replicas, which does not change the template.
_TEMPLATE_EXTRAS = ("affinity", "dns_ndots")


def _template(wl: Workload) -> dict[str, Any]:
    """The pod-template fields whose change makes Kubernetes roll the pods."""
    tpl = {f: getattr(wl, f) for f in _TEMPLATE_FIELDS}
    for k in _TEMPLATE_EXTRAS:
        if k in wl.extra:
            tpl[k] = wl.extra[k]
    return tpl


def template_hash(name: str, template: dict[str, Any]) -> str:
    """The ReplicaSet hash for a pod template: same template, same ReplicaSet."""
    import hashlib

    return hashlib.sha1(f"{name}:{sorted(template.items(), key=lambda kv: kv[0])!r}".encode()).hexdigest()[:10]


def pending_template_hash(wl: Workload, overrides: dict[str, Any] | None) -> str:
    """Hash of the template new (still Pending) pods of *wl* would run.

    *overrides* is the workload's entry in the fix state that completes the rollout
    (e.g. ``{image: redis:7.4.1}``); the stuck pods already belong to that ReplicaSet.
    """
    probe = copy.deepcopy(wl)
    for k, v in (overrides or {}).items():
        if k == "image":
            probe.image = v
        elif k in ("memory_limit", "memory_request"):
            setattr(probe, k, parse_bytes(v))
        elif k in ("cpu_limit", "cpu_request"):
            setattr(probe, k, parse_cpu(v))
        elif k in _TEMPLATE_EXTRAS:
            probe.extra[k] = v
    return template_hash(wl.name, _template(probe))


def _start_rollout(wl: Workload, old_tpl: dict, old_hash: str, progress: float) -> None:
    """A changed pod template rolls out like a Deployment update.

    The new template gets a new ReplicaSet hash, so new pods get new names. While
    ``progress`` < 1, the first share of pods (by ordinal) run the new template and the
    rest keep the old pods, names and values; at 1 only new pods remain.
    """
    new_tpl = _template(wl)
    if new_tpl == old_tpl:
        return
    wl.replicaset_hash = template_hash(wl.name, new_tpl)
    if progress < 1.0:
        wl.extra["rollout"] = {"old": old_tpl, "old_hash": old_hash, "progress": progress}
    else:
        wl.extra.pop("rollout", None)


def pod_rolled(wl: Workload, pod: Pod, count: int) -> bool:
    """True when the rolling update has replaced *pod* (pods roll in ordinal order)."""
    ro = wl.extra.get("rollout")
    if not ro:
        return True
    return pod.ordinal < int(round(float(ro["progress"]) * max(1, count)))


def pod_template(wl: Workload, pod: Pod, count: int) -> dict[str, Any]:
    """Image, requests and limits *pod* actually runs with (old ones until it is rolled)."""
    ro = wl.extra.get("rollout")
    if ro and not pod_rolled(wl, pod, count):
        return dict(ro["old"])
    return _template(wl)


def apply_state(base: Platform, overrides: dict[str, Any] | None, *, progress: float = 1.0) -> Platform:
    """Return a copy of *base* with ``kubernetes_otel`` overrides applied.

    ``overrides`` shape (any key optional)::

        cluster: prod-use1            # default: platform default cluster
        workloads:
          ingress-nginx-controller: {memory_limit: 3Gi, image: ..., replicas: 8,
                                     hpa_max: 20, pvc_capacity: 2Ti, node_pool: ...}
        node_pools:
          batch-spot: {instance_types: [...], add_nodes: 6 | [{name, az, instance_type}],
                       kubelet_version: v1.31.2-eks-..., count_on_new_version: 6}
        remove_nodes: [ip-10-42-36-17.ec2.internal]

    ``progress`` (0..1) scales node additions and version roll-outs so a fix plays out
    over ``transition_seconds`` instead of snapping.
    """
    if not overrides:
        return base
    p = copy.deepcopy(base)
    c = p.cluster(overrides.get("cluster"))
    rng = random.Random(f"{base.namespace}:state:{c.name}:{sorted(overrides)!r}")

    for name, wov in (overrides.get("workloads") or {}).items():
        wl = c.workloads.get(name)
        if not wl or not isinstance(wov, dict):
            continue
        old_tpl = _template(wl)
        old_hash = wl.replicaset_hash
        if "image" in wov:
            wl.image = wov["image"]
        if "memory_limit" in wov:
            wl.memory_limit = parse_bytes(wov["memory_limit"])
        if "memory_request" in wov:
            wl.memory_request = parse_bytes(wov["memory_request"])
        if "cpu_limit" in wov:
            wl.cpu_limit = parse_cpu(wov["cpu_limit"])
        if "cpu_request" in wov:
            wl.cpu_request = parse_cpu(wov["cpu_request"])
        if "replicas" in wov:
            wl.replicas = int(wov["replicas"])
        if "hpa_max" in wov and wl.hpa is not None:
            wl.hpa["max"] = int(wov["hpa_max"])
        if "hpa_target_cpu" in wov and wl.hpa is not None:
            wl.hpa["target_cpu"] = float(wov["hpa_target_cpu"])
        if "pvc_capacity" in wov and wl.pvc is not None:
            wl.pvc["capacity"] = wov["pvc_capacity"]
        if "pvc_used" in wov and wl.pvc is not None:
            if progress >= 1.0:
                wl.pvc["used"] = wov["pvc_used"]
                wl.pvc.pop("hold_used", None)
            else:
                # space is freed by the work the fix starts (e.g. garbage collection):
                # until it finishes the bytes in use stay put, even if capacity grew
                wl.pvc["hold_used"] = True
        if "node_pool" in wov:
            wl.node_pool = wov["node_pool"]
        if "tolerations" in wov:
            wl.tolerations = list(wov["tolerations"])
        if "cpu_util" in wov:
            wl.cpu_util = tuple(wov["cpu_util"])  # type: ignore[assignment]
        if "memory_util" in wov:
            wl.memory_util = tuple(wov["memory_util"])  # type: ignore[assignment]
        for k in ("selector", "labels", "ingress_count", "config", "placement", *_TEMPLATE_EXTRAS):
            if k in wov:
                wl.extra[k] = wov[k]
        _start_rollout(wl, old_tpl, old_hash, progress)

    remove = set(overrides.get("remove_nodes") or [])
    if remove:
        c.nodes = [n for n in c.nodes if n.name not in remove]

    for pool_name, pov in (overrides.get("node_pools") or {}).items():
        pool = c.pools.get(pool_name)
        if pool is None or not isinstance(pov, dict):
            continue
        pool = dict(pool)
        if "instance_types" in pov:
            pool["instance_types"] = list(pov["instance_types"])
        if "kubelet_resolv_conf" in pov:
            pool["kubelet_resolv_conf"] = pov["kubelet_resolv_conf"]
        c.pools[pool_name] = pool
        add = pov.get("add_nodes")
        if add:
            specs = add if isinstance(add, list) else [{} for _ in range(int(add))]
            n_now = int(round(len(specs) * max(0.0, min(1.0, progress))))
            types = pool.get("instance_types") or ["m6i.large"]
            zones = pool.get("zones") or c.zones
            for i, spec in enumerate(specs[:n_now]):
                spec = spec or {}
                az = spec.get("az") or zones[i % len(zones)]
                if spec.get("name"):
                    ip = _ip_from_name(spec["name"])
                else:
                    ip = c._free_ips[az].pop()  # type: ignore[attr-defined]
                itype = spec.get("instance_type") or types[(i + 1) % len(types)]
                from generator_engine.identity import ScenarioIdentity

                iid = ScenarioIdentity(base.namespace).aws_id(
                    "instance", f"{c.name}:{pool_name}:added", i + 1
                )
                c.nodes.append(_make_node(instance_id=iid,
                    cluster={"name": c.name, "region": c.region,
                             "node_name_format": c.node_name_format},
                    pool=pool, ip=ip, az=az, rng=rng, instance_type=itype,
                    name=spec.get("name"),
                ))
        if "kubelet_version" in pov:
            pool_nodes = [n for n in c.nodes if n.pool == pool_name]
            total = int(pov.get("upgraded_count", len(pool_nodes)))
            n_up = int(round(total * max(0.0, min(1.0, progress)))) if "upgraded_count" not in pov else total
            for n in pool_nodes[:n_up]:
                n.kubelet_version = pov["kubelet_version"]
        if "cordoned" in pov:
            for n in c.nodes:
                if n.pool == pool_name and n.name in set(pov["cordoned"]):
                    n.labels["node.kubernetes.io/unschedulable"] = "true"

    _place_pods(c, stable=True)
    return p


def merge_overrides(*layers: dict[str, Any] | None) -> dict[str, Any]:
    """Deep-merge override dicts left to right (later wins; lists replace)."""
    out: dict[str, Any] = {}
    for layer in layers:
        if not layer:
            continue
        _deep_merge(out, layer)
    return out


def _deep_merge(dst: dict, src: dict) -> None:
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_merge(dst[k], v)
        else:
            dst[k] = copy.deepcopy(v)


# ── Identity helpers for logs ────────────────────────────────────────────────


def node_resource_attrs(cluster: Cluster, node: Node) -> dict[str, Any]:
    return {
        "host.name": node.name,
        "host.id": node.instance_id,
        "host.type": node.instance_type,
        "host.arch": node.arch,
        "host.ip": node.ip,
        "k8s.node.name": node.name,
        "k8s.node.uid": node.uid,
        "k8s.cluster.name": cluster.name,
        "cloud.provider": cluster.provider,
        "cloud.platform": cluster.platform,
        "cloud.region": cluster.region,
        "cloud.availability_zone": node.az,
        "cloud.account.id": cluster.account_id,
        "cloud.instance.id": node.instance_id,
        "os.type": "linux",
        "os.description": node.os_image,
    }


def pod_resource_attrs(cluster: Cluster, pod: Pod, workload: Workload | None = None) -> dict[str, Any]:
    node = cluster.node(pod.node)
    attrs = node_resource_attrs(cluster, node) if node else {"k8s.cluster.name": cluster.name}
    attrs.update({
        "k8s.namespace.name": pod.namespace,
        "k8s.pod.name": pod.name,
        "k8s.pod.uid": pod.uid,
        "k8s.pod.ip": pod.ip,
        "k8s.container.name": pod.container,
        "container.id": pod.container_id.split("://", 1)[-1],
        "container.name": pod.container,
    })
    if workload is not None:
        kind_key = {
            "Deployment": "k8s.deployment.name",
            "DaemonSet": "k8s.daemonset.name",
            "StatefulSet": "k8s.statefulset.name",
        }.get(workload.kind)
        if kind_key:
            attrs[kind_key] = workload.name
        image = pod_template(workload, pod, len(cluster.pods_of(workload.name)))["image"]
        name, _, tag = image.rpartition(":")
        attrs["container.image.name"] = name or image
        attrs["container.image.tag"] = tag if name else "latest"
    return attrs


def identity_placeholders(attrs: dict[str, Any]) -> dict[str, Any]:
    """``self_*`` placeholders a fault message can use to quote its own source."""
    return {
        "self_node": attrs.get("k8s.node.name", ""),
        "self_node_ip": attrs.get("host.ip", ""),
        "self_az": attrs.get("cloud.availability_zone", ""),
        "self_instance_type": attrs.get("host.type", ""),
        "self_instance_id": attrs.get("cloud.instance.id", ""),
        "self_pod": attrs.get("k8s.pod.name", ""),
        "self_pod_ip": attrs.get("k8s.pod.ip", ""),
        "self_cluster": attrs.get("k8s.cluster.name", ""),
    }


# ── Effective model from controller state ────────────────────────────────────

_effective_cache: dict[tuple, Platform] = {}
_EFFECTIVE_CACHE_MAX = 64


def fix_progress(row: dict[str, Any], now: float) -> float:
    transition = float(row.get("transition_seconds") or 0)
    if transition <= 0:
        return 1.0
    return max(0.0, min(1.0, (now - float(row.get("applied_at") or now)) / transition))


def _without_node_changes(state: dict[str, Any]) -> dict[str, Any]:
    """*state* minus node additions and removals (copy)."""
    out = copy.deepcopy(state)
    out.pop("remove_nodes", None)
    for pov in (out.get("node_pools") or {}).values():
        if isinstance(pov, dict):
            pov.pop("add_nodes", None)
    return out


def effective_platform(
    base: Platform,
    controller_state: dict[str, Any] | None,
    now: float,
    generator_id: str = "kubernetes_otel",
) -> Platform:
    """Baseline ⊕ applied fixes (oldest first, with transition progress) ⊕ active faults.

    *controller_state* is ``ChaosController.get_platform_state()``. Only the
    *generator_id* slice of each state dict is applied.
    """
    if not controller_state:
        return base
    layers: list[tuple[dict, float]] = []
    key: list = []
    for row in controller_state.get("fixes") or []:
        st = (row.get("state") or {}).get(generator_id)
        if not st:
            continue
        prog = round(fix_progress(row, now), 2)
        if prog < 1.0:
            # While the fix plays out it starts from where the fault left the platform
            # (6 of 24 nodes already upgraded stay upgraded), not from the baseline.
            # Node additions/removals are left out so they are not counted twice.
            start = _without_node_changes(((row.get("from_state") or {}).get(generator_id)) or {})
            if start:
                layers.append((start, 1.0))
        layers.append((st, prog))
        key.append(("fix", row.get("channel"), row.get("applied_at"), prog))
    for f in controller_state.get("faults") or []:
        if now < float(f.get("onset_at") or 0):
            continue
        st = (f.get("state") or {}).get(generator_id)
        if not st:
            continue
        layers.append((st, 1.0))
        key.append(("fault", f.get("channel"), f.get("triggered_at")))
    if not layers:
        return base
    cache_key = (id(base), tuple(key))
    hit = _effective_cache.get(cache_key)
    if hit is not None:
        return hit
    model = base
    for st, prog in layers:
        model = apply_state(model, st, progress=prog)
    if len(_effective_cache) >= _EFFECTIVE_CACHE_MAX:
        _effective_cache.clear()
    _effective_cache[cache_key] = model
    return model


def platform_placeholders(platform: Platform, cluster: str | None = None) -> dict[str, Any]:
    """Live counts a message may quote: ``{node_count}``, ``{pool_edge_c6i_count}``..."""
    c = platform.cluster(cluster)
    out: dict[str, Any] = {"node_count": len(c.nodes), "cluster_name": c.name}
    for pool in c.pools:
        n = len(c.nodes_in_pool(pool))
        out[f"pool_{pool.replace('-', '_')}_count"] = n
        out[f"pool_{pool.replace('-', '_')}_others_count"] = len(c.nodes) - n
    return out
