"""GeneratorSpec for the ``aws_waf_otel`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="aws_waf_otel",
    thread_name="gen-aws-waf-otel",
    module="generators.aws_waf_otel.generator",
    client="otlp",
    signals=("logs",),
    datasets=("aws.waf.otel",),
    integrations=("aws_waf_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="cloud_audit",
    impact_keys={
        "block_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Fraction of requests that WAF blocks during fault.",
        },
        "records_per_cycle": {
            "type": "int",
            "default": 15,
            "range": [1, 1000],
            "doc": "Number of WAF log records to emit per cycle during fault.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "waf",
            "doc": "service.name for the WAF log source.",
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
        "web_acl_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "WAF Web ACL name.  Defaults to <namespace>-web-acl.",
        },
        "records_per_cycle": {
            "type": "int",
            "required": False,
            "default": 15,
            "doc": "Normal number of WAF log records to emit per cycle.",
        },
    },
    schema_version="1.0"
)
