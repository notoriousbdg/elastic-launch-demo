"""GeneratorSpec for the ``aws_cloudwatch_logs`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="aws_cloudwatch_logs",
    thread_name="gen-aws-cloudwatch-logs",
    module="generators.aws_cloudwatch_logs.generator",
    client="es_bulk",
    signals=("logs",),
    datasets=("aws_logs.rds", "aws_logs.ecs"),
    integrations=(),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "gateway_service": {
            "type": "str",
            "required": False,
            "default": "gateway",
            "doc": "ECS service whose access log is shipped; the log group is "
                   "/ecs/<ns>-<gateway_service>.",
        },
        "cloud": {
            "type": "dict",
            "required": False,
            "default": {"provider": "aws", "region": "us-east-1"},
            "doc": "Cloud provider/region for the log documents.",
        },
    },
    impact_keys={
        "connection_refusals": {
            "type": "int",
            "default": 0,
            "range": [0, 500],
            "doc": "Postgres FATAL 'remaining connection slots are reserved' lines per batch in the "
                   "RDS log (connection exhaustion); 0=healthy.",
        },
        "gateway_error_fraction": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Share of gateway access-log requests answered with 5xx; 0=healthy.",
        },
    },
    chaos_mode="explicit",
    family="cloud_compute",
    schema_version="1.0",
    identity_fields=("aws.cloudwatch.log_group", "cloud.account.id"),
)
