"""GeneratorSpec for the ``iis`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="iis_otel",
    thread_name="gen-iis-otel",
    module="generators.iis_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("iisreceiver.otel",),
    integrations=("iis_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="web",
        impact_keys={
        "application_pool_not_running": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[IIS OTel] Application pool not running\": >0 activates breach emit for this rule; 0=healthy.",
        },
        "bandwidth_throttling_detected": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[IIS OTel] Bandwidth throttling detected\": set above 0.0 (blocked_increase) to breach; 0=healthy.",
        },
        "request_queue_age_elevated": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 50000.0],
            "doc": "Drive alert \"[IIS OTel] Request queue age elevated\": set above 5000.0 (iis.request.queue.age.max) to breach; 0=healthy.",
        },
        "request_queue_depth_elevated": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[IIS OTel] Request queue depth elevated\": set above 10.0 (iis.request.queue.count) to breach; 0=healthy.",
        },
        "request_rejections_detected": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[IIS OTel] Request rejections detected\": set above 0.0 (rejected_increase) to breach; 0=healthy.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "iis",
            "doc": "service.name for the IIS instance.",
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
            "doc": "Cloud configuration dict.  IIS runs on Windows; use aws_ec2 or azure_vm.",
        },
        "site_names": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "IIS site names (iis.site resource attribute). Defaults to 2 synthetic site names.",
        },
        "pool_names": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "IIS application pool names (iis.application_pool). Defaults to 2 synthetic pool names.",
        },
    },
    schema_version="1.0"
)
