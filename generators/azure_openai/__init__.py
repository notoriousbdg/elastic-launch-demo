"""GeneratorSpec for the ``azure_openai`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id='azure_openai',
    thread_name='gen-azure-openai',
    module="generators.azure_openai.generator",
    client='es_bulk',
    signals=('logs', 'metrics'),
    datasets=('azure_openai.logs', 'azure.open_ai'),
    integrations=('azure_openai',),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={'azure_region': {'type': 'str', 'required': False, 'default': 'eastus', 'doc': 'Azure region for the OpenAI resource.'}, 'azure_openai_models': {'type': 'list', 'required': False, 'default': ['gpt-4o', 'gpt-4.1-mini'], 'doc': 'List of Azure OpenAI model deployment names.'}},
        impact_keys={
        "latency_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 50000.0],
            "doc": "Drive alert \"[Azure OpenAI] Latency high\": set above 5000.0 (time_to_response) to breach; 0=healthy.",
        },
        "provisioned_utilization_above_threshold": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 850.0],
            "doc": "Drive alert \"[Azure OpenAI] Provisioned Utilization above threshold\": set above 85.0 (provisioned_utilization) to breach; 0=healthy.",
        },
        "quota_error_rates_above_threshold": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[Azure OpenAI] Quota Error Rates above threshold\": >0 activates breach emit for this rule; 0=healthy.",
        },
    },
    chaos_mode='explicit',
    family='ai',
    schema_version="1.0",
)
