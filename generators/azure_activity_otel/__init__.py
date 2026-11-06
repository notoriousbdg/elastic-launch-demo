"""GeneratorSpec for the ``azure_activity`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="azure_activity_otel",
    thread_name="gen-azure-activity-otel",
    module="generators.azure_activity_otel.generator",
    client="otlp",
    signals=("logs",),
    datasets=("azure.activity.otel",),
    integrations=("azure_activity_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="cloud_audit",
        impact_keys={
        "change_storm_high_mutation_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 500.0],
            "doc": "Drive alert \"[Azure Activity OTel] Change storm (high mutation rate)\": set above 50.0 (mutation_count) to breach; 0=healthy.",
        },
        "failed_administrative_operations": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Azure Activity OTel] Failed administrative operations\": set above 3.0 (failed_count) to breach; 0=healthy.",
        },
        "resource_health_degradation": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[Azure Activity OTel] Resource health degradation\": >0 activates breach emit for this rule; 0=healthy.",
        },
        "role_assignment_write_privilege_escalation": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[Azure Activity OTel] Role assignment write (privilege escalation)\": >0 activates breach emit for this rule; 0=healthy.",
        },
        "security_events": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[Azure Activity OTel] Security events\": >0 activates breach emit for this rule; 0=healthy.",
        },
        "sensitive_key_access": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[Azure Activity OTel] Sensitive key access\": >0 activates breach emit for this rule; 0=healthy.",
        },
        "service_health_incident": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[Azure Activity OTel] Service health incident\": >0 activates breach emit for this rule; 0=healthy.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "azure-activity",
            "doc": "service.name for the Azure Activity log source.",
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
        "principals": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "Azure AD principal email addresses.  Defaults to 5 synthetic addresses.",
        },
        "records_per_cycle": {
            "type": "int",
            "required": False,
            "default": 10,
            "doc": "Normal number of Azure Activity log records to emit per cycle.",
        },
    },
    schema_version="1.0"
)
