"""GeneratorSpec for the ``aws_vpcflow_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="aws_vpcflow_otel",
    thread_name="gen-aws-vpcflow-otel",
    module="generators.aws_vpcflow_otel.generator",
    client="otlp",
    signals=("logs",),
    datasets=("aws.vpcflow",),
    integrations=("aws_vpcflow_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=True,
    core=True,
    config_fields={},
    impact_keys={
        "reject_rate": {
            "type": "float",
            "default": 0.2,
            "range": [0.0, 1.0],
            "doc": "Fraction of flow records with aws.vpc.flow.action=REJECT during fault.",
        },
        "bytes_multiplier": {
            "type": "float",
            "default": 1.0,
            "range": [0.1, 100.0],
            "doc": "Multiplier applied to aws.vpc.flow.bytes and packets during fault.",
        },
    },
    chaos_mode="explicit",
    family="core",
    schema_version="1.0",
)
