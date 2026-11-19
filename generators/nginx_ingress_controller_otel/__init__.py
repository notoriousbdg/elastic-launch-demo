"""GeneratorSpec for the ``nginx_ingress_controller_otel`` telemetry generator.

NOTE: the package (0.4.1) ships two dashboards and no alert rule templates, so both
impact keys below only move dashboard panels; there is no alert to map them to.

NOTE: nginx_ingress_controller_otel declares no dataset in manifest.yml, but
the dashboards filter on nginx_ingress_controller.access.otel and
nginx_ingress_controller.error.otel (zookeeper-style).

Stock dashboards query body.structured.*; OTel defaults that field to flattened
(Lens cannot aggregate it).  The generator remaps body.structured to object via
logs-otel@custom and dual-writes the same keys to attributes.* (passthrough).
"""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="nginx_ingress_controller_otel",
    thread_name="gen-nginx-ingress-controller-otel",
    module="generators.nginx_ingress_controller_otel.generator",
    client="otlp",
    signals=("logs",),
    datasets=(
        "nginx_ingress_controller.access.otel",
        "nginx_ingress_controller.error.otel",
    ),
    integrations=("nginx_ingress_controller_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="proxy",
    impact_keys={
        "error_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Dashboard-only (the package has no alert rules): fraction of requests that result in 4xx/5xx responses during a fault.",
        },
        "records_per_cycle": {
            "type": "int",
            "default": 20,
            "range": [1, 500],
            "doc": "Dashboard-only (the package has no alert rules): number of ingress log records to emit per cycle during a fault.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "nginx-ingress-controller",
            "doc": "service.name for the nginx ingress controller.",
        },
        "upstream_services": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "Backend service names to appear in upstream.name.  Defaults to 4 synthetic services.",
        },
        "records_per_cycle": {
            "type": "int",
            "required": False,
            "default": 20,
            "doc": "Normal number of ingress access log records to emit per cycle.",
        },
    },
    schema_version="1.0"
)
