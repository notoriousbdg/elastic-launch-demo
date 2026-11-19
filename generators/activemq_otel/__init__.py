"""GeneratorSpec for the ``activemq_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="activemq_otel",
    thread_name="gen-activemq-otel",
    module="generators.activemq_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("activemq.otel",),
    integrations=("activemq_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="queue",
        impact_keys={
        "blocked_sends_detected": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[ActiveMQ OTel] Blocked sends detected\": set above 0.0 (rate) to breach; 0=healthy.",
        },
        "broker_memory_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[ActiveMQ OTel] Broker memory utilization high\": set above 0.85 (activemq.memory.utilization) to breach; 0=healthy.",
        },
        "dead_letter_queue_depth_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1000.0],
            "doc": "Drive alert \"[ActiveMQ OTel] Dead letter queue depth high\": set above 100.0 (dlq_depth) to breach; 0=healthy.",
        },
        "high_jvm_cpu_utilization": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[ActiveMQ OTel] High JVM CPU utilization\": set above 0.85 (activemq.jvm.cpu.process.usage) to breach; 0=healthy.",
        },
        "high_jvm_heap_utilization": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[ActiveMQ OTel] High JVM heap utilization\": set above 0.85 (heap.max) to breach; 0=healthy.",
        },
        "queue_depth_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10000.0],
            "doc": "Drive alert \"[ActiveMQ OTel] Queue depth high\": set above 1000.0 (queue_depth) to breach; 0=healthy.",
        },
        "persistent_store_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[ActiveMQ OTel] Persistent store utilization high\": set above 0.9 (activemq.store.utilization) to breach; 0=healthy.",
        },
    },
    config_fields={
        "service_name": {"type": "str", "required": False, "default": "activemq", "doc": "service.name"},
        "broker_name": {"type": "str", "required": False, "default": None, "doc": "broker name"},
        "destination_count": {"type": "int", "required": False, "default": 4, "doc": "destinations"},
        "host_name": {"type": "str", "required": False, "default": None, "doc": "host.name"},
        "cloud": {"type": "dict", "required": False, "default": None, "doc": "cloud"},
    },
    schema_version="1.0",
)
