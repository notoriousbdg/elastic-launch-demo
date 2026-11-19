"""GeneratorSpec for the ``kafka`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id='kafka_otel',
    thread_name='gen-kafka-otel',
    module="generators.kafka_otel.generator",
    client='otlp',
    signals=('metrics',),
    datasets=('kafkametricsreceiver.otel',),
    integrations=('kafka_otel',),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={'service_name': {'type': 'str', 'required': False, 'default': 'kafka-broker', 'doc': 'service.name for the Kafka broker.'}, 'host_name': {'type': 'str', 'required': False, 'default': None, 'doc': "host.name override.  Defaults to '<ns>-kafka-broker'."}, 'cluster': {'type': 'str', 'required': False, 'default': None, 'doc': "Kafka cluster alias.  Defaults to '<ns>-events'."}, 'broker_count': {'type': 'int', 'required': False, 'default': 3, 'doc': 'Number of brokers to report in kafka.brokers.'}, 'partitions_per_topic': {'type': 'int', 'required': False, 'default': 6, 'doc': 'Partition count per topic.'}, 'topics': {'type': 'list', 'required': False, 'default': None, 'doc': "List of topic names.  Defaults to '<ns>.events.created/processed/failed'."}, 'consumer_groups': {'type': 'list', 'required': False, 'default': None, 'doc': 'List of consumer group names.  Defaults to app-consumer and analytics-consumer.'}, 'cloud': {'type': 'dict', 'required': False, 'default': {'provider': 'aws', 'platform': 'aws_msk', 'region': 'us-east-1', 'availability_zone': 'us-east-1a'}, 'doc': 'Cloud provider/platform/region/az for resource attributes.'}},
        impact_keys={
        "broker_count_below_threshold": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Kafka OTel] Broker count below threshold\": set below 2.0 (latest_brokers) to breach; default 100.0=healthy.",
        },
        "high_consumer_lag_by_topic": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 500000.0],
            "doc": "Drive alert \"[Kafka OTel] High consumer lag by topic\": set above 50000.0 (kafka.consumer_group.lag_sum) to breach; 0=healthy.",
        },
        "high_consumer_lag_per_partition": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100000.0],
            "doc": "Drive alert \"[Kafka OTel] High consumer lag per partition\": set above 10000.0 (kafka.consumer_group.lag) to breach; 0=healthy.",
        },
        "empty_consumer_groups": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[Kafka OTel] Empty consumer groups\": >0 activates breach emit for this rule; 0=healthy.",
        },
        "under_replicated_partitions": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[Kafka OTel] Under-replicated partitions\": >0 activates breach emit for this rule; 0=healthy.",
        },
    },
    chaos_mode='explicit',
    family='queue',
    schema_version="1.0",
)
