"""GeneratorSpec for the ``memcached`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="memcached_otel",
    thread_name="gen-memcached-otel",
    module="generators.memcached_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("memcachedreceiver.otel",),
    integrations=("memcached_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="cache",
        impact_keys={
        "high_connection_count": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 8000.0],
            "doc": "Drive alert \"[Memcached OTel] High connection count\": set above 800 to breach: current connections (memcached.connections.current); 0=healthy.",
        },
        "high_cpu_usage": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Memcached OTel] High CPU usage\": set above 80 to breach: CPU used in percent of one core (the alert fires above 0.8 cores); 0=healthy.",
        },
        "high_eviction_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Memcached OTel] High eviction rate\": set above 10 to breach: evictions per second; 0=healthy.",
        },
        "low_command_throughput": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Memcached OTel] Low command throughput\": any value below the default 100 (including 0) stops command traffic, so the command rate falls under the alert line of 1/s; default 100.0=healthy.",
        },
        "low_hit_ratio_for_get_operations": {
            "type": "float",
            "default": 800.0,
            "range": [0.0, 800.0],
            "doc": "Drive alert \"[Memcached OTel] Low hit ratio for get operations\": set below 80 to breach: target get hit ratio in percent (the alert fires when the miss ratio is above 20%); 100 and up, and the default 800.0, are healthy.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "memcached",
            "doc": "service.name for the Memcached instance.",
        },
        "host_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "host.name override.",
        },
        "cloud": {
            "type": "dict",
            "required": False,
            "default": None,
            "doc": "Cloud configuration dict.",
        },
    },
    schema_version="1.0"
)
