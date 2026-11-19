"""GeneratorSpec for the ``redisenterprise`` telemetry generator.

The redisenterprise_otel package covers cluster, node, database (bdb_*),
shard (redis_*), proxy, and listener metrics.
"""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="redisenterprise_otel",
    thread_name="gen-redisenterprise-otel",
    module="generators.redisenterprise_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("redisenterprise.otel",),
    integrations=("redisenterprise_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="cache",
        impact_keys={
        "tls_certificate_expiring_soon": {
            "type": "float",
            "default": 300.0,
            "range": [0.0, 300.0],
            "doc": "Drive alert \"[Redis Enterprise OTel] TLS certificate expiring soon\": set below 30.0 (days_until_expiry) to breach; default 300.0=healthy.",
        },
        "database_down": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Redis Enterprise OTel] Database down\": set below 1.0 (bdb_up) to breach; default 100.0=healthy.",
        },
        "database_high_latency": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Redis Enterprise OTel] Database high latency\": set above 5 to breach: average database latency in milliseconds (the alert fires above 5 ms); 0=healthy.",
        },
        "database_memory_utilization_critical": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 900.0],
            "doc": "Drive alert \"[Redis Enterprise OTel] Database memory utilization critical\": set above 90.0 (bdb_memory_limit) to breach; 0=healthy.",
        },
        "database_unexpected_key_evictions": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Redis Enterprise OTel] Database unexpected key evictions\": set above 0.0 (bdb_evicted_objects) to breach; 0=healthy.",
        },
        "authentication_failures_detected": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Redis Enterprise OTel] Authentication failures detected\": set above 0.0 (listener_auth_errors) to breach; 0=healthy.",
        },
        "node_down": {
            "type": "bool",
            "default": False,
            "range": [False, True],
            "doc": "When true, node_up=0 (node-down alert). Alert: [Redis Enterprise OTel] Node down.",
        },
        "node_cpu_saturation": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Redis Enterprise OTel] Node CPU saturation\": set above 85 to breach: node CPU utilisation in percent (the alert fires above 85%); 0=healthy.",
        },
        "node_free_memory_low": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Redis Enterprise OTel] Node free memory low\": set below 1.0 (free_gb) to breach; default 100.0=healthy.",
        },
        "node_storage_running_low": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Redis Enterprise OTel] Node storage running low\": set below 5.0 (persistent_gb) to breach; default 100.0=healthy.",
        },
        "proxy_file_descriptor_saturation": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 800.0],
            "doc": "Drive alert \"[Redis Enterprise OTel] Proxy file descriptor saturation\": set above 80.0 (dmcproxy_process_max_fds) to breach; 0=healthy.",
        },
        "shard_connection_saturation": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 800.0],
            "doc": "Drive alert \"[Redis Enterprise OTel] Shard connection saturation\": set above 80.0 (redis_maxclients) to breach; 0=healthy.",
        },
        "shard_down": {
            "type": "bool",
            "default": False,
            "range": [False, True],
            "doc": "When true, redis_up=0 (shard-down alert). Alert: [Redis Enterprise OTel] Shard down.",
        },
        "shard_high_memory_fragmentation": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Redis Enterprise OTel] Shard high memory fragmentation\": set above 1.5 (redis_mem_fragmentation_ratio) to breach; 0=healthy.",
        },
        "shard_persistence_failures": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Redis Enterprise OTel] Shard persistence failures\": set above 0.0 (failures) to breach; 0=healthy.",
        },
    },
    config_fields={
        "service_name": {"type": "str", "required": False, "default": "redis-enterprise",
                         "doc": "service.name for the Redis Enterprise metrics endpoint."},
        "cluster_name": {"type": "str", "required": False, "default": None,
                         "doc": "Redis Enterprise cluster FQDN.  Defaults to <namespace>-cluster."},
        "database_count": {"type": "int", "required": False, "default": 2,
                           "doc": "Number of synthetic Redis Enterprise databases."},
        "node_count": {"type": "int", "required": False, "default": 3,
                       "doc": "Number of synthetic Redis Enterprise cluster nodes."},
    },
    schema_version="1.0",
)
