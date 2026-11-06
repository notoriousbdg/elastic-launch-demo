"""GeneratorSpec for the ``cockroachdb`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id='cockroachdb_otel',
    thread_name='gen-cockroachdb-otel',
    module="generators.cockroachdb_otel.generator",
    client='otlp',
    signals=('metrics',),
    datasets=('cockroachdb.otel',),
    integrations=('cockroachdb_otel',),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode='explicit',
    family='database',
        impact_keys={
        "cpu_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 300.0],
            "doc": "Drive alert \"[CockroachDB OTel] CPU utilization high\": set above 30.0 (values) to breach; 0=healthy.",
        },
        "deadlocks_detected": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[CockroachDB OTel] Deadlocks detected\": set above 0.0 (deadlocks) to breach; 0=healthy.",
        },
        "disk_i_o_operations_queued_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[CockroachDB OTel] Disk I/O operations queued high\": set above 10.0 (sys_host_disk_iopsinprogress) to breach; 0=healthy.",
        },
        "runnable_goroutines_per_cpu_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 300.0],
            "doc": "Drive alert \"[CockroachDB OTel] Runnable goroutines per CPU high\": set above 30.0 (sys_runnable_goroutines_per_cpu) to breach; 0=healthy.",
        },
        "io_admission_control_overload": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[CockroachDB OTel] IO admission control overload\": set above 1.0 (admission_io_overload) to breach; 0=healthy.",
        },
        "l0_file_count_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10000.0],
            "doc": "Drive alert \"[CockroachDB OTel] L0 file count high\": set above 1000.0 (storage_l0_num_files) to breach; 0=healthy.",
        },
        "liveness_heartbeat_failures": {
            "type": "int",
            "default": 0,
            "range": [0, 1000],
            "doc": "Extra liveness heartbeat failures. Alert: [CockroachDB OTel] Liveness heartbeat failures.",
        },
        "lsm_read_amplification_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 500.0],
            "doc": "Drive alert \"[CockroachDB OTel] LSM read amplification high\": set above 50.0 (rocksdb_read_amplification) to breach; 0=healthy.",
        },
        "unhealthy_rpc_connections": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[CockroachDB OTel] Unhealthy RPC connections\": set above 0.0 (rpc_connection_unhealthy) to breach; 0=healthy.",
        },
        "storage_capacity_usage_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[CockroachDB OTel] Storage capacity usage high\": set above 0.6 (capacity) to breach; 0=healthy.",
        },
        "storage_write_stalls_detected": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[CockroachDB OTel] Storage write stalls detected\": set above 0.0 (storage_write_stalls) to breach; 0=healthy.",
        },
        "unavailable_ranges_detected": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[CockroachDB OTel] Unavailable ranges detected\": set above 0.0 (ranges_unavailable) to breach; 0=healthy.",
        },
        "under_replicated_ranges_detected": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[CockroachDB OTel] Under-replicated ranges detected\": set above 0.0 (ranges_underreplicated) to breach; 0=healthy.",
        },
    },
    config_fields={
        'service_name': {'type': 'str', 'required': False, 'default': 'cockroachdb', 'doc': 'service.name'},
        'node_count': {'type': 'int', 'required': False, 'default': 3, 'doc': 'Number of synthetic nodes.'},
        'host_name': {'type': 'str', 'required': False, 'default': None, 'doc': 'host basename.'},
        'cloud': {'type': 'dict', 'required': False, 'default': None, 'doc': 'Cloud attrs.'},
    },
    schema_version="1.0",
)
