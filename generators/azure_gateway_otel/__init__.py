"""GeneratorSpec for the ``azure_gateway`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="azure_gateway_otel",
    thread_name="gen-azure-gateway-otel",
    module="generators.azure_gateway_otel.generator",
    client="otlp",
    signals=("logs",),
    datasets=("azure.application_gateway.otel",),
    integrations=("azure_gateway_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="cloud_audit",
    impact_keys={
        "error_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Fraction of requests that result in a 4xx/5xx response or firewall block.",
        },
        "records_per_cycle": {
            "type": "int",
            "default": 15,
            "range": [1, 500],
            "doc": "Number of Application Gateway log records to emit per cycle during fault.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "azure-application-gateway",
            "doc": "service.name for the Azure Application Gateway log source.",
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
        "subscription_id": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "Azure subscription ID (UUID).  Auto-generated if absent.",
        },
        "gateway_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "Azure Application Gateway resource name.  Defaults to <namespace>-agw.",
        },
        "backends": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "Backend pool host:port strings.  Defaults to 3 synthetic backends.",
        },
        "records_per_cycle": {
            "type": "int",
            "required": False,
            "default": 15,
            "doc": "Normal number of Application Gateway log records to emit per cycle.",
        },
    },
    schema_version="1.0"
)
