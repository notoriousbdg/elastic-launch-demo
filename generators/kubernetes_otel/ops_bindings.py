"""How opsctl commands change kubernetes_otel state (``SPEC.ops_bindings``).

Called by :func:`app.ops.change_state` for every applied command in a scenario that
enables this generator and pins a ``k8s_topology``. Patching a workload's resources,
image or replicas rolls its pods; expanding a PVC grows the owning workload's volume;
patching a node group's instance types changes what new nodes launch as.
"""

from __future__ import annotations

from typing import Any

# opsctl flag -> workload field it changes in the pinned platform.
WORKLOAD_FLAGS = {
    "memory-limit": "memory_limit", "memory-request": "memory_request",
    "cpu-limit": "cpu_limit", "cpu-request": "cpu_request",
    "image": "image", "replicas": "replicas",
}
WORKLOAD_KINDS = {"deployment", "daemonset", "statefulset"}
ROLLOUT_SECONDS = 90.0   # a rolling update of a few pods (maxSurge 25%)
PVC_RESIZE_SECONDS = 45.0


def ops_state(scenario: Any, cmd: Any) -> tuple[dict[str, Any], float]:
    """(state, transition_seconds) for *cmd*; ({}, 0) when it changes nothing here."""
    topo = getattr(scenario, "k8s_topology", None)
    if not topo:
        return {}, 0.0
    default = topo.get("default_cluster") or (topo.get("clusters") or [{}])[0].get("name")
    cluster = next((c for c in topo.get("clusters") or [] if c.get("name") == default), {})
    workloads = {w.get("name"): w for w in cluster.get("workloads") or []}
    pools = {p.get("name") for p in cluster.get("node_pools") or []}

    if cmd.kind in WORKLOAD_KINDS and cmd.name in workloads and cmd.verb in ("patch", "apply", "scale"):
        changes = {}
        for flag, fld in WORKLOAD_FLAGS.items():
            val = cmd.flags.get(flag)
            if val is None or val is True:
                continue
            changes[fld] = int(val) if fld == "replicas" else val
        if changes:
            return {"kubernetes_otel": {"workloads": {cmd.name: changes}}}, ROLLOUT_SECONDS
    if cmd.kind == "pvc" and cmd.verb == "expand" and isinstance(cmd.flags.get("size"), str):
        owner = next((n for n, w in workloads.items() if (w.get("pvc") or {}).get("name") == cmd.name), None)
        if owner:
            return ({"kubernetes_otel": {"workloads": {owner: {"pvc_capacity": cmd.flags["size"]}}}},
                    PVC_RESIZE_SECONDS)
    if cmd.kind == "nodegroup" and cmd.name in pools and isinstance(cmd.flags.get("instance-types"), str):
        types = [t.strip() for t in cmd.flags["instance-types"].split(",") if t.strip()]
        return {"kubernetes_otel": {"node_pools": {cmd.name: {"instance_types": types}}}}, 0.0
    return {}, 0.0
