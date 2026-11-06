"""GeneratorSpec for the ``azure_billing`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="azure_billing",
    thread_name="gen-azure-billing",
    module="generators.azure_billing.generator",
    client="es_bulk",
    signals=("metrics",),
    datasets=("azure.billing",),
    integrations=("azure_billing",),
    always_install=True,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "azure_region": {
            "type": "str",
            "required": False,
            "default": "eastus",
            "doc": "Azure region for the billing resource.",
        },
    },
    impact_keys={
        "cost_multiplier": {
            "type": "float",
            "default": 1.0,
            "range": [0.1, 100.0],
            "doc": "Multiplier applied to pretax_cost and actual_cost during fault.",
        },
        "usage_multiplier": {
            "type": "float",
            "default": 1.0,
            "range": [0.1, 100.0],
            "doc": "Multiplier applied to usage_quantity during fault.",
        },
    },
    chaos_mode="explicit",
    family="ai",
    schema_version="1.0",
)
