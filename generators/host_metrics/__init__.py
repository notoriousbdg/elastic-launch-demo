"""GeneratorSpec for the ``host_metrics`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id='host_metrics',
    thread_name='gen-host-metrics',
    module="generators.host_metrics.generator",
    client='otlp',
    signals=('metrics',),
    datasets=('hostmetricsreceiver',),
    integrations=(),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=True,
    core=True,
    config_fields={},
    impact_keys={
        'cpu_pct': {
            'type': 'float', 'default': None, 'range': [0.0, 100.0],
            'doc': 'Override CPU utilisation percentage during the fault.',
        },
        'memory_pct': {
            'type': 'float', 'default': None, 'range': [0.0, 100.0],
            'doc': 'Override memory utilisation percentage during the fault.',
        },
        'fd_pressure': {
            'type': 'float', 'default': None, 'range': [0.0, 1.0],
            'doc': 'Scales process.open_file_descriptors toward the per-process fd_limit. '
                   'A value of 1.0 drives all processes to their fd_limit, creating a '
                   '"Too many open files" / ulimit scenario.',
        },
        'filesystem_pct': {
            'type': 'float', 'default': None, 'range': [0.0, 100.0],
            'doc': 'Target filesystem fill percentage (0-100) for all mountpoints. '
                   'The fill level ramps toward this value at ~2%/tick. '
                   'Backs NODE-DISK-PRESSURE and disk-full scenarios.',
        },
        'disk_io_multiplier': {
            'type': 'float', 'default': None, 'range': [1.0, 20.0],
            'doc': 'Multiplier for system.disk.operations per tick. '
                   'system.disk.io_time follows proportionally (A0 coupling). '
                   'Drives IO-saturation stories.',
        },
        'virtual_memory_multiplier': {
            'type': 'float', 'default': None, 'range': [1.0, 20.0],
            'doc': 'Multiplier for process.memory.virtual. '
                   'Models mmap growth from a misconfigured vm.max_map_count.',
        },
        'network_connections_multiplier': {
            'type': 'float', 'default': None, 'range': [1.0, 50.0],
            'doc': 'Multiplier for system.network.connections. '
                   'Models conntrack table exhaustion or ephemeral-port depletion.',
        },
    },
    chaos_mode='any',
    family='core',
    schema_version="1.0",
)
