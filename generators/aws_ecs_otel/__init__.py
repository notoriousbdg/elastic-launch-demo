"""GeneratorSpec for the ``aws_ecs`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="aws_ecs_otel",
    thread_name="gen-aws-ecs-otel",
    module="generators.aws_ecs_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("aws.ecs.otel",),
    integrations=("aws_ecs_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="cloud_compute",
        impact_keys={
        "cpu_reservation_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 800.0],
            "doc": "Drive alert \"[AWS ECS OTel] CPU reservation high\": set above 80.0 (avg_cpu_res) to breach; 0=healthy.",
        },
        "cpu_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 850.0],
            "doc": "Drive alert \"[AWS ECS OTel] CPU utilization high\": set above 85.0 (avg_cpu_util) to breach; 0=healthy.",
        },
        "memory_reservation_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 800.0],
            "doc": "Drive alert \"[AWS ECS OTel] Memory reservation high\": set above 80.0 (avg_mem_res) to breach; 0=healthy.",
        },
        "memory_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 900.0],
            "doc": "Drive alert \"[AWS ECS OTel] Memory utilization high\": set above 90.0 (avg_mem_util) to breach; 0=healthy.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "ecs-metrics",
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
        "cluster_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "ECS cluster name.  Defaults to <namespace>-cluster.",
        },
        "service_names": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "List of ECS service names to simulate.  Defaults to 3 synthetic names.",
        },
    },
    schema_version="1.0"
)
