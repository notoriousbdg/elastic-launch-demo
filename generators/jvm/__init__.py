"""GeneratorSpec for the ``jvm`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id='jvm',
    thread_name='gen-jvm-metrics',
    module="generators.jvm.generator",
    client='otlp',
    signals=('metrics',),
    datasets=('generic.otel',),
    integrations=(),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=True,
    core=False,
    config_fields={},
    impact_keys={
        'heap_pressure': {'type': 'float', 'default': 0.0, 'range': [0.0, 1.0], 'doc': 'Additional heap pressure fraction (0 = none, 1 = near full).  Scales heap used toward limit.'},
        'gc_pause_multiplier': {'type': 'float', 'default': 1.0, 'range': [1.0, 20.0], 'doc': 'Multiply Old GC pause duration during fault.'},
    },
    chaos_mode='any',
    family='platform',
    schema_version="1.0",
)
