"""GeneratorSpec for the ``nginx_metrics`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id='nginx_metrics',
    thread_name='gen-nginx-metrics',
    module="generators.nginx_metrics.generator",
    client='otlp',
    signals=('metrics',),
    datasets=('nginxreceiver',),
    integrations=(),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=True,
    core=False,
    config_fields={'server_names': {'type': 'list', 'required': False, 'default': None, 'doc': 'Host names for NGINX metric resources.  Must match nginx.server_names so the dashboard filter correlates logs with metrics.'}},
    impact_keys={
        "active_connections": {
            "type": "int",
            "default": 0,
            "range": [0, 10_000],
            "doc": "Override nginx.connections_current active gauge when >0 during fault.",
        },
        "request_rate_multiplier": {
            "type": "float",
            "default": 1.0,
            "range": [0.0, 100.0],
            "doc": "Multiplier applied to per-tick nginx.requests increment during fault.",
        },
    },
    chaos_mode='any',
    family='web',
    schema_version="1.0",
)
