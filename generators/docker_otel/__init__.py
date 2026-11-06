"""GeneratorSpec for the ``docker`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="docker_otel",
    thread_name="gen-docker-otel",
    module="generators.docker_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("dockerstatsreceiver.otel",),
    integrations=("docker_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="platform",
    impact_keys={
        "cpu_utilization_multiplier": {
            "type": "float",
            "default": 1.0,
            "range": [1.0, 10.0],
            "doc": "Multiply container CPU utilization during fault.",
        },
        "memory_utilization_multiplier": {
            "type": "float",
            "default": 1.0,
            "range": [1.0, 10.0],
            "doc": "Multiply container memory utilization during fault.",
        },
        "cpu_throttle_rate": {
            "type": "float",
            "default": None,
            "range": [0.0, 1.0],
            "doc": "Target ratio of throttled_periods to total_periods. "
                   "Drives container.cpu.throttling_data.throttled_periods and "
                   "total_periods together so the throttle ratio is meaningful. "
                   "Backs CPU-limit / cgroup throttle stories.",
        },
        "memory_limit_bytes": {
            "type": "int",
            "default": None,
            "range": [67108864, 17179869184],  # 64MiB - 16GiB
            "doc": "Override container.memory.usage.limit in bytes. "
                   "Set lower than the app's actual heap to model a "
                   "'RAM cap too low for new version' scenario.",
        },
        "pids_multiplier": {
            "type": "float",
            "default": None,
            "range": [1.0, 20.0],
            "doc": "Multiplier for container.pids.count. "
                   "Models a runaway thread or fork-bomb scenario.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "docker-stats",
            "doc": "service.name for the Docker host.",
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
        "container_names": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "List of container names to simulate.  Defaults to 4 synthetic names.",
        },
    },
    schema_version="1.0"
)
