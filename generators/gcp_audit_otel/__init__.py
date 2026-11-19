"""GeneratorSpec for the ``gcp_audit`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="gcp_audit_otel",
    thread_name="gen-gcp-audit-otel",
    module="generators.gcp_audit_otel.generator",
    client="otlp",
    signals=("logs",),
    datasets=("gcp.auditlog.otel",),
    integrations=("gcp_audit_otel",),
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
            "doc": "Fraction of API calls that fail with a permission error during fault.",
        },
        "records_per_cycle": {
            "type": "int",
            "default": 10,
            "range": [1, 500],
            "doc": "Number of GCP audit log records to emit per cycle during fault.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "gcp-audit",
            "doc": "service.name for the GCP audit log source.",
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
        "project_id": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "GCP project ID.  Defaults to <namespace>-project.",
        },
        "principals": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "User email addresses or service accounts.  Defaults to 5 synthetic accounts.",
        },
        "records_per_cycle": {
            "type": "int",
            "required": False,
            "default": 10,
            "doc": "Normal number of GCP audit log records to emit per cycle.",
        },
    },
    schema_version="1.0"
)
