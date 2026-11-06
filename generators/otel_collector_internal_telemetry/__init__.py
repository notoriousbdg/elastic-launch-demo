"""GeneratorSpec for the ``otel_collector_internal_telemetry`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="otel_collector_internal_telemetry",
    thread_name="gen-otel-collector-internal-telemetry",
    module="generators.otel_collector_internal_telemetry.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("collectortelemetry.otel",),
    integrations=("otel_collector_internal_telemetry",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "agent_count": {
            "type": "int",
            "required": False,
            "default": 2,
            "doc": "Number of agent-mode collectors (each forwards to the gateway).",
        },
        "collector_version": {
            "type": "str",
            "required": False,
            "default": "0.121.0",
            "doc": "service.version reported by every collector instance.",
        },
        "cloud": {
            "type": "dict",
            "required": False,
            "default": {
                "provider": "aws",
                "platform": "aws_eks",
                "region": "us-east-1",
                "availability_zone": "us-east-1a",
            },
            "doc": "Cloud provider/platform/region/az for resource attributes.",
        },
    },
    impact_keys={
        "exporter_send_failed_log_records": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10000.0],
            "doc": "Drive alert \"[OpenTelemetry Collector Internal Telemetry Otel] Exporter send failed log records\": set above 0.0 (otelcol_exporter_send_failed_log_records) to breach; 0=healthy.",
        },
        "exporter_send_failed_metric_points": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10000.0],
            "doc": "Drive alert \"[OpenTelemetry Collector Internal Telemetry Otel] Exporter send failed metric points\": set above 0.0 (otelcol_exporter_send_failed_metric_points) to breach; 0=healthy.",
        },
        "exporter_send_failed_spans": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10000.0],
            "doc": "Drive alert \"[OpenTelemetry Collector Internal Telemetry Otel] Exporter send failed spans\": set above 0.0 (otelcol_exporter_send_failed_spans) to breach; 0=healthy.",
        },
        "queue_saturation": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Exporter queue utilization panels (otelcol_exporter_queue_size / "
                   "otelcol_exporter_queue_capacity): fill the gateway sending queues to this "
                   "percent (values <= 1 are fractions) and add receiver refusals; 0=healthy (<10%).",
        },
    },
    chaos_mode="explicit",
    family="platform",
    schema_version="1.0",
)
