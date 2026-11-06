"""GeneratorSpec for the ``gcp_compute_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="gcp_compute_otel",
    thread_name="gen-gcp-compute-otel",
    module="generators.gcp_compute_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("gcp.compute.otel",),
    integrations=("gcp_compute_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "gcp-compute",
            "doc": "service.name for the Cloud Monitoring collector resource.",
        },
        "cloud": {
            "type": "dict",
            "required": False,
            "default": {
                "provider": "gcp",
                "platform": "gcp_compute_engine",
                "region": "us-central1",
                "availability_zone": "us-central1-a",
            },
            "doc": "Cloud provider/platform/region/zone for resource attributes.",
        },
        "project_id": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "GCP project id.  Defaults to ScenarioIdentity(ns).gcp_project_id.",
        },
        "instance_count": {
            "type": "int",
            "required": False,
            "default": 3,
            "doc": "Number of GCE instances. All but the last are members of one "
                   "managed instance group; the last is a Spot batch VM.",
        },
        "zones": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "Zones to spread the instances over.  Defaults to <region>-a/-b/-c.",
        },
    },
    impact_keys={
        "cpu_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Pin compute.googleapis.com/instance/cpu/utilization on every VM to "
                   "this percent (scheduler wait and CPU core-seconds rise with it). "
                   "Set 95.0 to saturate the fleet; 0=healthy (20-45% per VM).",
        },
        "instance_interruption": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 20.0],
            "doc": "Spot VM preemptions per interval: emits instance/interruption_count "
                   "with interruption_type=Preemption on the batch VM and resets its "
                   "uptime_total. Set 3.0 to breach; 0=healthy (type 'none', count 0).",
        },
        "disk_throttled": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Severity of a throttled data disk on the first VM: I/O latency rises "
                   "up to ~40x, queue depth up to ~60, and disk/performance_status moves "
                   "to Degraded (<0.75) or Severely Degraded. Set 1.0 to breach; 0=healthy.",
        },
        "firewall_drop_spike": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100000.0],
            "doc": "Firewall dropped packets per interval across the fleet "
                   "(firewall/dropped_packets_count and dropped_bytes_count). Set "
                   "20000.0 to breach; 0=healthy (a few dropped scan packets per VM).",
        },
    },
    chaos_mode="explicit",
    family="cloud_compute",
    schema_version="1.0",
)
