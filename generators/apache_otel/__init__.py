"""GeneratorSpec for the ``apache`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="apache_otel",
    thread_name="gen-apache-otel",
    module="generators.apache_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("apachereceiver.otel",),
    integrations=("apache_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="web",
        impact_keys={
        "server_restart_detected": {
            "type": "float",
            "default": 3000.0,
            "range": [0.0, 3000.0],
            "doc": "Drive alert \"[Apache OTel] Server restart detected\": set below 300.0 (min_uptime) to breach; default 3000.0=healthy.",
        },
        "low_idle_workers": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Apache OTel] Low idle workers\": set below 10.0 (e.g.) to breach; default 100.0=healthy.",
        },
        "high_server_load": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Apache OTel] High server load\": set above 5.0 (apache.load.1) to breach; 0=healthy.",
        },
        "no_requests_received": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[Apache OTel] No requests received\": >0 activates breach emit for this rule; 0=healthy.",
        },
        "high_server_load_2": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Apache OTel] High server load\": set above 0.8 (server_load) to breach; 0=healthy.",
        },
        "high_busy_workers_ratio": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Apache OTel] High busy workers ratio\": set above 0.0 (total_workers) to breach; 0=healthy.",
        },
        "high_request_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Apache OTel] High request rate\": >0 tips request delta for >1000/min (requests emitted as gauge for MAX/MIN); 0=healthy.",
        },
        "worker_pool_exhaustion": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Apache OTel] Worker pool exhaustion\": set above 0.0 (total) to breach; 0=healthy.",
        },
    },
    config_fields={
        "service_name": {"type": "str", "required": False, "default": "apache-httpd",
                         "doc": "service.name for the Apache HTTP server."},
        "host_name": {"type": "str", "required": False, "default": None,
                      "doc": "host.name override.  Defaults to <namespace>-apache."},
        "cloud": {"type": "dict", "required": False, "default": None,
                  "doc": "Cloud configuration dict."},
    },
    schema_version="1.0",
)
