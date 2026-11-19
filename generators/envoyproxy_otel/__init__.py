"""GeneratorSpec for the ``envoyproxy_otel`` telemetry generator.

Stock package dashboards query ``metrics-statsdreceiver-default`` /
``data_stream.dataset == "statsdreceiver"`` (not the ``.otel`` stream OTLP
writes). The generator bulk-indexes into that stock stream so dashboards
remain unmodified.
"""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="envoyproxy_otel",
    thread_name="gen-envoyproxy-otel",
    module="generators.envoyproxy_otel.generator",
    client="es_bulk",
    signals=("metrics",),
    datasets=("statsdreceiver",),
    integrations=("envoyproxy_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="proxy",
        impact_keys={
        "certificate_expiring_soon": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Envoy OTel] Certificate expiring soon\": set below 7.0 (min_days) to breach; default 100.0=healthy.",
        },
        "high_downstream_error_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Envoy OTel] High downstream error rate\": set above 5.0 (downstream_rq_completed) to breach; 0=healthy.",
        },
        "high_memory_pressure": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Envoy OTel] High memory pressure\": set above 0.9 (memory_heap_size) to breach; 0=healthy.",
        },
        "listeners_warming": {
            "type": "int",
            "default": 0,
            "range": [0, 20],
            "doc": "Warming listener count during fault. Alert: [Envoy OTel] Listeners warming.",
        },
        "server_not_live": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Envoy OTel] Server not live\": set below 1.0 (max_live) to breach; default 100.0=healthy.",
        },
        "server_state_not_live": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[Envoy OTel] Server state not LIVE\": >0 activates breach emit for this rule; 0=healthy.",
        },
        "warming_clusters_stuck": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Envoy OTel] Warming clusters stuck\": set above 0.0 (envoy.cluster_manager.warming_clusters) to breach; 0=healthy.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "envoy",
            "doc": "service.name for the Envoy StatsD exporter.",
        },
        "cluster_names": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "Envoy cluster names to model.  Defaults to 3 synthetic clusters.",
        },
    },
    schema_version="1.0"
)
