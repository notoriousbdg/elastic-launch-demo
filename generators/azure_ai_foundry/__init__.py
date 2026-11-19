"""GeneratorSpec for the ``azure_ai_foundry`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="azure_ai_foundry",
    thread_name="gen-azure-ai-foundry",
    module="generators.azure_ai_foundry.generator",
    client="es_bulk",
    signals=("metrics", "logs"),
    datasets=("azure.ai_foundry", "azure_ai_foundry.logs"),
    integrations=("azure_ai_foundry",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "azure_region": {
            "type": "str",
            "required": False,
            "default": "eastus2",
            "doc": "Azure region for the Foundry resource.",
        },
        "model_deployment_name": {
            "type": "str",
            "required": False,
            "default": "gpt-4o-demo",
            "doc": "Model deployment name (azure.dimensions.model_deployment_name).",
        },
        "resource_name": {
            "type": "str",
            "required": False,
            "default": "ai-foundry-demo",
            "doc": "Cognitive Services account name.",
        },
    },
        impact_keys={
        "latency_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 50000.0],
            "doc": "Drive alert \"[Microsoft Foundry] Latency high\": response time in milliseconds; set above 5000.0 (time_to_response) to breach; 0=healthy.",
        },
        "model_availability_above_threshold": {
            "type": "float",
            "default": 990.0,
            "range": [0.0, 990.0],
            "doc": "Drive alert \"[Microsoft Foundry] Model Availability above threshold\": availability in tenths of a percent (990.0 = 99.0%); set below 990.0 (model_availability < 99%) to breach, 0 is a total outage; default 990.0=healthy.",
        },
        "provisioned_utilization_above_threshold": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 850.0],
            "doc": "Drive alert \"[Microsoft Foundry] Provisioned Utilization above threshold\": utilization in percent; set above 85.0 (provisioned_utilization) to breach, values above 100 clamp to 99.9%; 0=healthy.",
        },
    },
    chaos_mode="explicit",
    family="ai",
    schema_version="1.0",
)
