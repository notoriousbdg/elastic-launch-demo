"""GeneratorSpec for the ``kubernetes_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="kubernetes_otel",
    thread_name="gen-kubernetes-otel",
    module="generators.kubernetes_otel.generator",
    client='otlp',
    signals=('metrics',),
    datasets=('kubeletstatsreceiver', 'k8sclusterreceiver', 'k8seventsreceiver'),
    integrations=('kubernetes_otel',),
    always_install=True,
    data_views=(),
    wants_chaos=True,
    default_enabled=True,
    core=True,
    config_fields={},
        impact_keys={
        "container_cpu_throttling": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Kubernetes OTel] Container CPU throttling\": set above 0.9 (avg_util) to breach; 0=healthy.",
        },
        "container_memory_near_limit": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Kubernetes OTel] Container memory near limit\": set above 0.9 (avg_util) to breach; 0=healthy.",
        },
        "daemonset_mis_scheduled_or_not_ready": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Kubernetes OTel] DaemonSet mis-scheduled or not ready\": set above 0.0 (misscheduled) to breach; 0=healthy.",
        },
        "deployment_unavailable_replicas": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Kubernetes OTel] Deployment unavailable replicas\": set above 0.0 (desired) to breach; 0=healthy.",
        },
        "hpa_at_max_replicas": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Kubernetes OTel] HPA at max replicas\": set above 0.0 (max_replicas) to breach; 0=healthy.",
        },
        "job_failures": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Kubernetes OTel] Job failures\": set above 0.0 (failed) to breach; 0=healthy.",
        },
        "node_cpu_saturation": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Kubernetes OTel] Node CPU saturation\": set above 3.6 (avg_cpu) to breach; 0=healthy.",
        },
        "node_disk_pressure": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[Kubernetes OTel] Node disk pressure\": >0 activates breach emit for this rule; 0=healthy.",
        },
        "node_filesystem_saturation": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Kubernetes OTel] Node filesystem saturation\": set above 0.0 (k8s.node.filesystem.capacity) to breach; 0=healthy.",
        },
        "node_memory_pressure": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[Kubernetes OTel] Node memory pressure\": >0 activates breach emit for this rule; 0=healthy.",
        },
        "node_memory_saturation": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 137438953470.0],
            "doc": "Drive alert \"[Kubernetes OTel] Node memory saturation\": set above 13743895347.0 (avg_memory) to breach; 0=healthy.",
        },
        "node_not_ready": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "When >0, set k8s.node.condition_ready=0. Alert: [Kubernetes OTel] Node not ready.",
        },
        "oomkilled_containers": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[Kubernetes OTel] OOMKilled containers\": >0 activates breach emit for this rule; 0=healthy.",
        },
        "persistent_volume_space_low": {
            "type": "float",
            "default": 200.0,
            "range": [0.0, 200.0],
            "doc": "Drive alert \"[Kubernetes OTel] Persistent volume space low\": set below 20.0 (available_pct) to breach; default 200.0=healthy.",
        },
        "pod_crashloopbackoff": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Kubernetes OTel] Pod CrashLoopBackOff\": set above 0.0 (restarts) to breach; 0=healthy.",
        },
        "pods_in_failed_phase": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[Kubernetes OTel] Pods in Failed phase\": >0 activates breach emit for this rule; 0=healthy.",
        },
        "pods_stuck_in_pending_phase": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[Kubernetes OTel] Pods stuck in Pending phase\": >0 activates breach emit for this rule; 0=healthy.",
        },
        "statefulset_replicas_not_ready": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Kubernetes OTel] StatefulSet replicas not ready\": set above 0.0 (desired) to breach; 0=healthy.",
        },
    },
    chaos_mode='explicit',
    family='core',
    schema_version="1.1",
    scope_aware=True,
    # Generator schema 1.1: what fault_state / remediation_effects.state may change.
    # Read by the pinned-topology path (scenarios that declare k8s_topology).
    state_keys={
        "workloads": {"type": "dict", "doc": "Per-workload overrides: image, replicas, cpu_request, cpu_limit, memory_request, memory_limit, cpu_util, memory_util, hpa_max, hpa_target_cpu, pvc_capacity, pvc_used, node_pool, placement, tolerations."},
        "node_pools": {"type": "dict", "doc": "Per-pool overrides: instance_types, add_nodes (count or [{name, az, instance_type}]), kubelet_version (+ upgraded_count), kubelet_resolv_conf, cordoned."},
        "remove_nodes": {"type": "list", "doc": "Node names that leave the cluster (drained and replaced)."},
        "cluster": {"type": "str", "doc": "Cluster the overrides apply to (default: the topology's default cluster)."},
    },
    coherence=(
        "hpa_at_max_replicas => deployment replicas = HPA current = max; per-pod CPU above the HPA target",
        "container_cpu_throttling => CPU usage just under the limit",
        "oomkilled_containers => memory ramps to the limit before each kill; restarts increment",
        "node_not_ready => kubelet stops reporting; pods Unknown, evicted after 300s",
        "node_disk_pressure => filesystem utilisation above 0.85; disk-pressure condition",
        "pod_crashloopbackoff => restarts with kubelet back-off spacing; container not ready",
        "pods_stuck_in_pending_phase => extra Pending pods; running pods unaffected",
    ),
)
