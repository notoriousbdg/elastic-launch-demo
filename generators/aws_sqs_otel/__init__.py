"""GeneratorSpec for the ``aws_sqs`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="aws_sqs_otel",
    thread_name="gen-aws-sqs-otel",
    module="generators.aws_sqs_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("aws.sqs.otel",),
    integrations=("aws_sqs_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="queue",
        impact_keys={
        "dlq_has_messages": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS SQS OTel] DLQ has messages\": set above 0.0 (dlq_depth) to breach; 0=healthy.",
        },
        "high_backlog": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10000.0],
            "doc": "Drive alert \"[AWS SQS OTel] High backlog\": set above 1000.0 (backlog) to breach; 0=healthy.",
        },
        "in_flight_saturation": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1000000.0],
            "doc": "Drive alert \"[AWS SQS OTel] In-flight saturation\": set above 100000.0 (in_flight) to breach; 0=healthy.",
        },
        "oldest_message_age_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 3000.0],
            "doc": "Drive alert \"[AWS SQS OTel] Oldest message age high\": set above 300.0 (max_age_seconds) to breach; 0=healthy.",
        },
        "producer_send_rate_drop": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS SQS OTel] Producer send rate drop\": set below 1.0 (total_sent) to breach; default 100.0=healthy.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "sqs-metrics",
            "doc": "Service name reported to the OTLP endpoint.",
        },
        "host_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "Host name override.",
        },
        "cloud": {
            "type": "dict",
            "required": False,
            "default": None,
            "doc": "Cloud configuration dict.",
        },
        "account_id": {
            "type": "str",
            "required": False,
            "default": "123456789012",
            "doc": "AWS account ID.",
        },
        "queue_names": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "List of SQS queue names to simulate.  Defaults to 3 synthetic names.",
        },
    },
    schema_version="1.0"
)
