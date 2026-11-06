"""GeneratorSpec for the ``aws_lambda`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="aws_lambda_otel",
    thread_name="gen-aws-lambda-otel",
    module="generators.aws_lambda_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("aws.lambda.otel",),
    integrations=("aws_lambda_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="cloud_compute",
        impact_keys={
        "dead_letter_errors": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS Lambda OTel] Dead letter errors\": set above 0.0 (total_dlq_errors) to breach; 0=healthy.",
        },
        "destination_delivery_failures": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS Lambda OTel] Destination delivery failures\": set above 0.0 (total_delivery_failures) to breach; 0=healthy.",
        },
        "high_async_event_age": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 600000.0],
            "doc": "Drive alert \"[AWS Lambda OTel] High async event age\": set above 60000.0 (max_async_age_ms) to breach; 0=healthy.",
        },
        "high_concurrent_executions": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 8000.0],
            "doc": "Drive alert \"[AWS Lambda OTel] High concurrent executions\": set above 800.0 (peak_concurrency) to breach; 0=healthy.",
        },
        "high_average_duration": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 30000.0],
            "doc": "Drive alert \"[AWS Lambda OTel] High average duration\": set above 3000.0 (avg_duration_ms) to breach; 0=healthy.",
        },
        "high_tail_duration": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100000.0],
            "doc": "Drive alert \"[AWS Lambda OTel] High tail duration\": set above 10000.0 (max_duration_ms) to breach; 0=healthy.",
        },
        "high_error_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS Lambda OTel] High error rate\": set above 10.0 (total_invocations) to breach; 0=healthy.",
        },
        "high_iterator_age": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 600000.0],
            "doc": "Drive alert \"[AWS Lambda OTel] High iterator age\": set above 60000.0 (max_iterator_age_ms) to breach; 0=healthy.",
        },
        "high_throttle_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS Lambda OTel] High throttle rate\": set above 10.0 (denominator) to breach; 0=healthy.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "lambda-metrics",
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
        "function_names": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "List of Lambda function names to simulate.  Defaults to 3 synthetic names.",
        },
    },
    schema_version="1.0"
)
