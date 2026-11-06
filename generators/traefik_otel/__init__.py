"""GeneratorSpec for the ``traefik`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="traefik_otel",
    thread_name="gen-traefik-otel",
    module="generators.traefik_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("traefik.otel",),
    integrations=("traefik_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="proxy",
        impact_keys={
        "file_descriptor_pressure": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 800.0],
            "doc": "Drive alert \"[Traefik OTel] File descriptor pressure\": set above 80.0 (process_max_fds) to breach; 0=healthy.",
        },
        "high_5xx_error_rate_by_entrypoint": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1000.0],
            "doc": "Drive alert \"[Traefik OTel] High 5xx error rate by entrypoint\": set above 100.0 (total) to breach; 0=healthy.",
        },
        "high_5xx_error_rate_by_router": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 500.0],
            "doc": "Drive alert \"[Traefik OTel] High 5xx error rate by router\": set above 50.0 (total) to breach; 0=healthy.",
        },
        "high_5xx_error_rate_by_service": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1000.0],
            "doc": "Drive alert \"[Traefik OTel] High 5xx error rate by service\": set above 100.0 (total) to breach; 0=healthy.",
        },
        "high_goroutine_count": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 20000.0],
            "doc": "Drive alert \"[Traefik OTel] High goroutine count\": set above 2000.0 (go_goroutines) to breach; 0=healthy.",
        },
        "high_memory_usage": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Traefik OTel] High memory usage\": set above 2.0 (process_resident_memory_bytes) to breach; 0=healthy.",
        },
        "high_open_connections": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10000.0],
            "doc": "Drive alert \"[Traefik OTel] High open connections\": set above 1000.0 (traefik_open_connections) to breach; 0=healthy.",
        },
        "stale_config_reload_success": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 240.0],
            "doc": "Drive alert \"[Traefik OTel] Stale config reload success\": set above 24.0 (was) to breach; 0=healthy.",
        },
        "tls_certificate_expiry_imminent": {
            "type": "float",
            "default": 300.0,
            "range": [0.0, 300.0],
            "doc": "Drive alert \"[Traefik OTel] TLS certificate expiry imminent\": set below 30.0 (days_until_expiry) to breach; default 300.0=healthy.",
        },
    },
    config_fields={
        "service_name": {"type": "str", "required": False, "default": "traefik",
                         "doc": "service.name for the Traefik Prometheus endpoint."},
        "entrypoint_count": {"type": "int", "required": False, "default": 2,
                             "doc": "Number of synthetic Traefik entrypoints (web, websecure)."},
    },
    schema_version="1.0",
)
