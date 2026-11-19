"""GeneratorSpec for the ``gcp_vpcflow_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="gcp_vpcflow_otel",
    thread_name="gen-gcp-vpcflow-otel",
    module="generators.gcp_vpcflow_otel.generator",
    client="otlp",
    signals=("logs",),
    datasets=("gcp.vpcflow",),
    integrations=("gcp_vpcflow_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=True,
    core=True,
    config_fields={},
    impact_keys={
        "bytes_multiplier": {
            "type": "float",
            "default": 1.0,
            "range": [0.1, 100.0],
            "doc": "Multiplier applied to gcp.vpc.flow.bytes_sent and packets_sent during fault.",
        },
        "records_per_cycle": {
            "type": "int",
            "default": 10,
            "range": [1, 500],
            "doc": "Number of VPC flow records to emit per cycle during fault.",
        },
    },
    chaos_mode="any",
    family="core",
    schema_version="1.0",
)
