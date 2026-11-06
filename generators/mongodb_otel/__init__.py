"""GeneratorSpec for the ``mongodb`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="mongodb_otel",
    thread_name="gen-mongodb-otel",
    module="generators.mongodb_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("mongodbreceiver.otel",),
    integrations=("mongodb_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "mongodb-primary",
            "doc": "service.name for the MongoDB instance.",
        },
        "host_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "host.name override.  Defaults to '<ns>-mongodb-host'.",
        },
        "server_port": {
            "type": "int",
            "required": False,
            "default": 27017,
            "doc": "server.port resource attribute and default mongodb.instance port.",
        },
        "mongodb_instance": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "attributes.mongodb.instance for the Server control.  Defaults to '<host_name>:<server_port>'.",
        },
        "databases": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "Database names.  Defaults to '<prefix>_primary/_analytics/_sessions'.",
        },
        "cloud": {
            "type": "dict",
            "required": False,
            "default": {
                "provider": "aws",
                "platform": "aws_ec2",
                "region": "us-east-1",
                "availability_zone": "us-east-1a",
            },
            "doc": "Cloud provider/platform/region/az for resource attributes.",
        },
    },
        impact_keys={
        "connection_exhaustion": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[MongoDB OTel] Connection exhaustion\": set below 10.0 (min_avail) to breach; default 100.0=healthy.",
        },
        "cursor_timeouts": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[MongoDB OTel] Cursor timeouts\": set above 0.0 (new_timeouts) to breach; 0=healthy.",
        },
        "global_lock_contention": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 5000.0],
            "doc": "Drive alert \"[MongoDB OTel] Global lock contention\": target lock-held RATE in ms/s (alert >500). Generator scales by emit cadence; 0=healthy.",
        },
        "high_cache_miss_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[MongoDB OTel] High cache miss rate\": set above 5.0 (miss_rate) to breach; 0=healthy.",
        },
        "high_cursor_count": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10000.0],
            "doc": "Drive alert \"[MongoDB OTel] High cursor count\": set above 1000.0 (cursors) to breach; 0=healthy.",
        },
        "high_memory_usage": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 42949672960.0],
            "doc": "Drive alert \"[MongoDB OTel] High memory usage\": set above 4294967296.0 (resident_bytes) to breach; 0=healthy.",
        },
        "high_operation_latency": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[MongoDB OTel] High operation latency\": set above 0.0 (op_count_rate) to breach; 0=healthy.",
        },
        "high_session_count": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 5000.0],
            "doc": "Drive alert \"[MongoDB OTel] High session count\": set above 500.0 (sessions) to breach; 0=healthy.",
        },
    },
    chaos_mode="explicit",
    family="database",
    schema_version="1.0",
)
