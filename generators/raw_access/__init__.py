"""GeneratorSpec for the ``raw_access`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id='raw_access',
    thread_name='gen-raw-access',
    module="generators.raw_access.generator",
    client='es_bulk',
    signals=('logs',),
    datasets=('generic',),
    integrations=(),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=True,
    core=True,
    config_fields={},
    impact_keys={
        "error_fraction": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Fraction of access logs that return 5xx during fault.",
        },
        "volume_multiplier": {
            "type": "float",
            "default": 1.0,
            "range": [0.1, 100.0],
            "doc": "Multiplier applied to batch size during fault.",
        },
    },
    chaos_mode='any',
    family='core',
    schema_version="1.0",
)
