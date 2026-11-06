"""GeneratorSpec for the ``rabbitmq`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="rabbitmq_otel",
    thread_name="gen-rabbitmq-otel",
    module="generators.rabbitmq_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("rabbitmqreceiver.otel",),
    integrations=("rabbitmq_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="queue",
        impact_keys={
        "disk_alarm_triggered": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[RabbitMQ OTel] Disk alarm triggered\": set above 0.0 (rabbitmq.node.disk_free_alarm) to breach; 0=healthy.",
        },
        "high_ready_queue_depth": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10000.0],
            "doc": "Drive alert \"[RabbitMQ OTel] High ready queue depth\": set above 1000.0 (ready_count) to breach; 0=healthy.",
        },
        "high_unacknowledged_message_count": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 5000.0],
            "doc": "Drive alert \"[RabbitMQ OTel] High unacknowledged message count\": set above 500.0 (unack_count) to breach; 0=healthy.",
        },
        "memory_alarm_triggered": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[RabbitMQ OTel] Memory alarm triggered\": set above 0.0 (rabbitmq.node.mem_alarm) to breach; 0=healthy.",
        },
        "queue_with_zero_consumers_and_ready_messages": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[RabbitMQ OTel] Queue with zero consumers and ready messages\": set above 0.0 (ready) to breach; 0=healthy.",
        },
    },
    config_fields={
        "service_name": {"type": "str", "required": False, "default": "rabbitmq", "doc": "service.name"},
        "host_name": {"type": "str", "required": False, "default": None, "doc": "host.name"},
        "cloud": {"type": "dict", "required": False, "default": None, "doc": "Cloud config"},
        "vhost": {"type": "str", "required": False, "default": "/", "doc": "vhost"},
        "node_name": {"type": "str", "required": False, "default": None, "doc": "node name"},
        "queue_names": {"type": "list", "required": False, "default": None, "doc": "queue names"},
    },
    schema_version="1.0",
)
