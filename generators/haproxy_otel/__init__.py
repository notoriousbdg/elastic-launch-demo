"""GeneratorSpec for the ``haproxy_otel`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="haproxy_otel",
    thread_name="gen-haproxy-otel",
    module="generators.haproxy_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("haproxyreceiver.otel",),
    integrations=("haproxy_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="proxy",
        impact_keys={
        "backend_has_no_active_servers": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[HAProxy OTel] Backend has no active servers\": set below 1 to breach: haproxy.active on the first BACKEND row; default 100.0=healthy. Touches nothing else (no queue, error or check counters).",
        },
        "connection_errors_spike": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[HAProxy OTel] Connection errors spike\": connection errors added per emit cycle on backend and server rows; the alert needs more than 10 in its window, so any value from 1 up breaches; 0=healthy.",
        },
        "server_health_check_failures": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[HAProxy OTel] Server health check failures\": failed health checks added per emit cycle on server rows; set above 0 to breach; 0=healthy.",
        },
        "high_backend_connection_time": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1000.0],
            "doc": "Drive alert \"[HAProxy OTel] High backend connection time\": set above 100 to breach: backend/server connection time in milliseconds; 0=healthy.",
        },
        "high_backend_queue_time": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[HAProxy OTel] High backend queue time\": set above 10 to breach: backend/server queue time in milliseconds; 0=healthy. Does not mark the backend down or queue requests.",
        },
        "high_backend_response_time": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 5000.0],
            "doc": "Drive alert \"[HAProxy OTel] High backend response time\": set above 500 to breach: backend/server response time in milliseconds; 0=healthy.",
        },
        "request_errors_spike": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[HAProxy OTel] Request errors spike\": request errors added per emit cycle on frontend rows; the alert needs more than 10 in its window, so any value from 1 up breaches; 0=healthy.",
        },
        "requests_queued_at_backend": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[HAProxy OTel] Requests queued at backend\": set above 0 to breach: requests queued at the backends (haproxy.requests.queued); 0=healthy. Does not mark the backend down.",
        },
        "response_errors_spike": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[HAProxy OTel] Response errors spike\": response errors added per emit cycle on backend and server rows; the alert needs more than 10 in its window, so any value from 1 up breaches; 0=healthy.",
        },
        "connection_retries_or_redispatches_spike": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[HAProxy OTel] Connection retries or redispatches spike\": set above 0 to breach: connection retries (and half as many redispatches) added per emit cycle; the alert needs more than 5 in its window; 0=healthy.",
        },
        "session_saturation_approaching_limit": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[HAProxy OTel] Session saturation approaching limit\": set above 80 to breach: sessions as a percent of the limit (a fraction up to 1.0 also works); 0=healthy.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "haproxy",
            "doc": "service.name for the HAProxy instance.",
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
        "frontend_names": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "HAProxy frontend names.  Defaults to 2 synthetic names.",
        },
        "backend_names": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "HAProxy backend names.  Defaults to 2 synthetic names.",
        },
    },
    schema_version="1.0"
)
