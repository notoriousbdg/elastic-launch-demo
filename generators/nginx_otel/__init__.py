"""GeneratorSpec for the ``nginx`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id='nginx_otel',
    thread_name='gen-nginx-otel',
    module="generators.nginx_otel.generator",
    client='otlp',
    signals=('logs', 'traces', 'metrics'),
    datasets=('nginx.access.otel', 'nginx.error.otel', 'nginxreceiver.otel'),
    integrations=('nginx_otel',),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=True,
    core=True,
    config_fields={
        'service_name': {
            'type': 'str', 'required': False, 'default': 'nginx-proxy',
            'doc': 'service.name for the NGINX proxy.',
        },
        'server_names': {
            'type': 'list', 'required': False, 'default': None,
            'doc': "List of server hostnames.  Defaults to ['<ns>-nginx-01', '<ns>-nginx-02'].",
        },
        'endpoints': {
            'type': 'list', 'required': False, 'default': None,
            'doc': 'URL paths to cycle through.  Defaults to the built-in 21-path list.',
        },
        'cloud': {
            'type': 'list', 'required': False, 'default': None,
            'doc': 'List of cloud config dicts, one per instance.',
        },
    },
        impact_keys={
        "high_error_rate_by_service": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"High error rate by service\": set above 5.0 (error_rate_pct) to breach; 0=healthy.",
        },
        "dropped_connections_detected": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Nginx OTel] Dropped connections detected\": set above 0.0 (connections_handled) to breach; 0=healthy.",
        },
        "error_log_spike": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 500.0],
            "doc": "Drive alert \"[Nginx OTel] Error log spike\": set above 50.0 (error_count) to breach; 0=healthy.",
        },
        "high_4xx_error_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 4000.0],
            "doc": "Drive alert \"[Nginx OTel] High 4xx error rate\": set above 400.0 (http.response.status_code) to breach; 0=healthy.",
        },
        "high_5xx_error_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 5000.0],
            "doc": "Drive alert \"[Nginx OTel] High 5xx error rate\": set above 500.0 (http.response.status_code) to breach; 0=healthy.",
        },
        "high_active_connections": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1000.0],
            "doc": "Drive alert \"[Nginx OTel] High active connections\": any positive probe emits avg_active>=300 (OOTB >256); 0=healthy.",
        },
    },
    chaos_mode='any',
    family='core',
    schema_version="1.0",
)
