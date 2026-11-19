"""GeneratorSpec for the ``redis`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id='redis_otel',
    thread_name='gen-redis-otel',
    module="generators.redis_otel.generator",
    client='otlp',
    signals=('metrics',),
    datasets=('redisreceiver.otel',),
    integrations=('redis_otel',),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={'service_name': {'type': 'str', 'required': False, 'default': 'redis-cache', 'doc': 'service.name for the Redis instance.'}, 'host_name': {'type': 'str', 'required': False, 'default': None, 'doc': "host.name override.  Defaults to '<ns>-redis-host'."}, 'keyspaces': {'type': 'list', 'required': False, 'default': [0], 'doc': 'List of keyspace integers (db indices) to report per-keyspace metrics for.'}, 'maxmemory_bytes': {'type': 'int', 'required': False, 'default': 2147483648, 'doc': 'Configured maxmemory in bytes.  Drives memory.used simulation.'}, 'cloud': {'type': 'dict', 'required': False, 'default': {'provider': 'aws', 'platform': 'aws_ec2', 'region': 'us-east-1', 'availability_zone': 'us-east-1a'}, 'doc': 'Cloud provider/platform/region/az for resource attributes.'}},
        impact_keys={
        "high_eviction_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1000.0],
            "doc": "Drive alert \"[Redis OTel] High eviction rate\": set above 100.0 (evicted) to breach; 0=healthy.",
        },
        "memory_fragmentation_indicates_swapping": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Redis OTel] Memory fragmentation indicates swapping\": set below 1.0 (redis.memory.fragmentation_ratio) to breach; default 100.0=healthy.",
        },
        "memory_approaching_maxmemory": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 900.0],
            "doc": "Drive alert \"[Redis OTel] Memory approaching maxmemory\": set above 90.0 (redis.maxmemory) to breach; 0=healthy.",
        },
        "rejected_connections": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Redis OTel] Rejected connections\": set above 0.0 (rejected) to breach; 0=healthy.",
        },
        "primary_has_no_connected_replicas": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[Redis OTel] Primary has no connected replicas\": >0 activates breach emit for this rule; 0=healthy.",
        },
    },
    chaos_mode='explicit',
    family='cache',
    schema_version="1.0",
)
