"""GeneratorSpec for the ``aws_cloudtrail_otel`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="aws_cloudtrail_otel",
    thread_name="gen-aws-cloudtrail-otel",
    module="generators.aws_cloudtrail_otel.generator",
    client="otlp",
    signals=("logs",),
    datasets=("aws.cloudtrail.otel",),
    integrations=("aws_cloudtrail_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="cloud_audit",
    impact_keys={
        "error_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Fraction of API calls that fail with an access-denied or throttle error.",
        },
        "records_per_cycle": {
            "type": "int",
            "default": 10,
            "range": [1, 500],
            "doc": "Number of CloudTrail events to emit per cycle during fault.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "cloudtrail",
            "doc": "service.name for the CloudTrail log source.",
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
        "account_id": {
            "type": "str",
            "required": False,
            "default": "123456789012",
            "doc": "AWS account ID.",
        },
        "principals": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "IAM principal ARNs.  Defaults to 5 synthetic user ARNs.",
        },
        "records_per_cycle": {
            "type": "int",
            "required": False,
            "default": 10,
            "doc": "Normal number of CloudTrail events to emit per cycle.",
        },
    },
    schema_version="1.0"
)
