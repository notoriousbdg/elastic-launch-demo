"""GeneratorSpec for the ``aws_rds_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="aws_rds_otel",
    thread_name="gen-aws-rds-otel",
    module="generators.aws_rds_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("aws.rds.otel",),
    integrations=("aws_rds_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "rds-instance",
            "doc": "service.name for the RDS instance.",
        },
        "host_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "host.name.  Defaults to '<ns>-rds'.",
        },
        "instance_id": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "DBInstanceIdentifier (scoped to the scenario: 'orders-db' becomes '<ns>-orders-db').  Defaults to '<ns>-rds-01'.",
        },
        "engine": {
            "type": "str",
            "required": False,
            "default": "postgres",
            "doc": "Engine attribute value (e.g. 'postgres', 'mysql', 'aurora-postgresql').",
        },
        "connections_baseline": {
            "type": "list",
            "required": False,
            "default": [10, 80],
            "doc": "[low, high] DatabaseConnections on the healthy baseline (scaled by load). "
                   "A low range (e.g. [0, 2]) makes a connection-exhaustion fault stand out "
                   "against the instance's own baseline.",
        },
        "total_storage_bytes": {
            "type": "int",
            "required": False,
            "default": 536870912000,
            "doc": "Total allocated storage in bytes.  Used to compute FreeStorageSpace.",
        },
        "account_id": {
            "type": "str",
            "required": False,
            "default": "123456789012",
            "doc": "AWS account ID for cloud.account.id.",
        },
        "cloud": {
            "type": "dict",
            "required": False,
            "default": {
                "provider": "aws",
                "platform": "aws_rds",
                "region": "us-east-1",
                "availability_zone": "us-east-1a",
            },
            "doc": "Cloud provider/platform/region/az for resource attributes.",
        },
    },
        impact_keys={
        "burst_balance_low": {
            "type": "float",
            "default": 200.0,
            "range": [0.0, 200.0],
            "doc": "Drive alert \"[AWS RDS OTel] Burst balance low\": set below 20.0 (min_burst_balance) to breach; default 200.0=healthy.",
        },
        "checkpoint_lag_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 600.0],
            "doc": "Drive alert \"[AWS RDS OTel] Checkpoint lag high\": set above 60.0 (max_checkpoint_lag) to breach; 0=healthy.",
        },
        "cpu_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 800.0],
            "doc": "Drive alert \"[AWS RDS OTel] CPU utilization high\": set above 80.0 (avg_cpu_utilization) to breach; 0=healthy.",
        },
        "database_connections_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1000.0],
            "doc": "Drive alert \"[AWS RDS OTel] Database connections high\": set above 100.0 (avg_connections) to breach; 0=healthy.",
        },
        "disk_queue_depth_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS RDS OTel] Disk queue depth high\": set above 5.0 (avg_disk_queue_depth) to breach; 0=healthy.",
        },
        "free_storage_space_low": {
            "type": "float",
            "default": 107374182400.0,
            "range": [0.0, 107374182400.0],
            "doc": "Drive alert \"[AWS RDS OTel] Free storage space low\": set below 10737418240.0 (min_free_storage) to breach; default 107374182400.0=healthy.",
        },
        "freeable_memory_low": {
            "type": "float",
            "default": 2684354560.0,
            "range": [0.0, 2684354560.0],
            "doc": "Drive alert \"[AWS RDS OTel] Freeable memory low\": set below 268435456.0 (min_freeable_memory) to breach; default 2684354560.0=healthy.",
        },
        "read_latency_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS RDS OTel] Read latency high\": set above 0.05 (max_read_latency) to breach; 0=healthy.",
        },
        "replica_lag_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 600.0],
            "doc": "Drive alert \"[AWS RDS OTel] Replica lag high\": set above 60.0 (max_replica_lag) to breach; 0=healthy.",
        },
        "swap_usage_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1048576000.0],
            "doc": "Drive alert \"[AWS RDS OTel] Swap usage high\": set above 104857600.0 (max_swap_usage) to breach; 0=healthy.",
        },
        "write_latency_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS RDS OTel] Write latency high\": set above 0.05 (max_write_latency) to breach; 0=healthy.",
        },
    },
    chaos_mode="explicit",
    family="cloud_compute",
    schema_version="1.0",
)
