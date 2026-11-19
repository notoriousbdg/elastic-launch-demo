"""GeneratorSpec for the ``vsphere`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="vsphere_otel",
    thread_name="gen-vsphere-otel",
    module="generators.vsphere_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("vcenterreceiver.otel",),
    integrations=("vsphere_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="platform",
        impact_keys={
        "datastore_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 800.0],
            "doc": "Drive alert \"[vSphere OTel] Datastore utilization high\": set above 80.0 (vcenter.datastore.disk.utilization) to breach; 0=healthy.",
        },
        "entity_status_unhealthy": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[vSphere OTel] Entity status unhealthy\": set above 0.0 (unhealthy_count) to breach; 0=healthy.",
        },
        "host_cpu_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 800.0],
            "doc": "Drive alert \"[vSphere OTel] Host CPU utilization high\": set above 80.0 (vcenter.host.cpu.utilization) to breach; 0=healthy.",
        },
        "host_disk_latency_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 150.0],
            "doc": "Drive alert \"[vSphere OTel] Host disk latency high\": set above 15.0 (vcenter.host.disk.latency.max) to breach; 0=healthy.",
        },
        "host_memory_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 850.0],
            "doc": "Drive alert \"[vSphere OTel] Host memory utilization high\": set above 85.0 (vcenter.host.memory.utilization) to breach; 0=healthy.",
        },
        "host_power_state_unhealthy": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[vSphere OTel] Host power state unhealthy\": set above 0.0 (vcenter.datacenter.host.count) to breach; 0=healthy.",
        },
        "vm_disk_latency_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 200.0],
            "doc": "Drive alert \"[vSphere OTel] VM disk latency high\": set above 20.0 (vcenter.vm.disk.latency.max) to breach; 0=healthy.",
        },
        "vm_memory_ballooning": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[vSphere OTel] VM memory ballooning\": set above 0.0 (vcenter.vm.memory.ballooned) to breach; 0=healthy.",
        },
        "vm_memory_swapping": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[vSphere OTel] VM memory swapping\": set above 0.0 (vcenter.vm.memory.swapped) to breach; 0=healthy.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "vsphere",
            "doc": "service.name for the vCenter endpoint.",
        },
        "host_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "host.name override (vCenter FQDN).",
        },
        "cloud": {
            "type": "dict",
            "required": False,
            "default": None,
            "doc": "Cloud configuration dict.",
        },
        "datacenter_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "vSphere datacenter name.  Defaults to <namespace>-dc.",
        },
        "cluster_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "vSphere cluster name.  Defaults to <namespace>-cluster.",
        },
        "vm_names": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "VM names to simulate.  Defaults to 4 synthetic names.",
        },
        "host_names": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "ESXi host names.  Defaults to 2 synthetic names.",
        },
        "datastore_names": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "Datastore names.  Defaults to 2 synthetic names.",
        },
    },
    schema_version="1.0"
)
