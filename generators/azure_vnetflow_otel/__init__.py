"""GeneratorSpec for the ``azure_vnetflow_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="azure_vnetflow_otel",
    thread_name="gen-azure-vnetflow-otel",
    module="generators.azure_vnetflow_otel.generator",
    client="otlp",
    signals=("logs",),
    datasets=("azure.vnetflow.otel",),
    # Elastic ships no OTel content package for Azure flow logs (only the ECS
    # azure_network_watcher_vnet integration), so there is nothing to install.
    integrations=(),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=True,
    core=True,
    config_fields={},
    impact_keys={
        "deny_rate": {
            "type": "float",
            "default": 0.2,
            "range": [0.0, 1.0],
            "doc": "Fraction of flow records with azure.vnet.flow.action=Deny during fault.",
        },
        "bytes_multiplier": {
            "type": "float",
            "default": 1.0,
            "range": [0.1, 100.0],
            "doc": "Multiplier applied to azure.vnet.flow bytes and packets during fault.",
        },
    },
    chaos_mode="explicit",
    family="core",
    schema_version="1.0",
)
