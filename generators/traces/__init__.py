"""GeneratorSpec for the ``traces`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id='traces',
    thread_name='gen-traces',
    module="generators.traces.generator",
    client='otlp',
    signals=('traces', 'metrics', 'logs'),
    datasets=('generic',),
    integrations=(),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=True,
    core=True,
    config_fields={'db_system': {'type': 'str', 'required': False, 'default': 'postgresql', 'doc': 'Database system for DB spans and APM topology.  Also read by apm_rollup.py for the APM dependency node label.  Should match whichever database generator is enabled.'}},
    impact_keys={
        'latency_multiplier': {'type': 'float', 'default': 1.0, 'range': [1.0, 50.0], 'doc': 'Multiply all service latencies by this factor during the fault.'},
        'error_rate': {'type': 'float', 'default': 0.7, 'range': [0.0, 1.0], 'doc': 'Fraction of traces that error on chaos-affected services during fault.'},
    },
    chaos_mode='any',
    family='core',
    schema_version="1.0",
)
