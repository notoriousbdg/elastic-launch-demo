"""GeneratorSpec for the ``gcp_cloudrun_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="gcp_cloudrun_otel",
    thread_name="gen-gcp-cloudrun-otel",
    module="generators.gcp_cloudrun_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("gcp.cloudrun.otel",),
    integrations=("gcp_cloudrun_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "gcp-cloudrun",
            "doc": "service.name for the Cloud Monitoring collector resource.",
        },
        "cloud": {
            "type": "dict",
            "required": False,
            "default": {
                "provider": "gcp",
                "platform": "gcp_cloud_run",
                "region": "us-central1",
                "availability_zone": "us-central1-a",
            },
            "doc": "Cloud provider/platform/region; region is the Cloud Run location.",
        },
        "project_id": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "GCP project id.  Defaults to ScenarioIdentity(ns).gcp_project_id.",
        },
        "run_services": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "Cloud Run service roles (namespace-qualified).  Defaults to "
                   "checkout-api (two revisions, canary split), image-resizer and "
                   "notification-worker.  The first service is the fault target.",
        },
    },
    impact_keys={
        "error_5xx_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Fraction of requests on the first service that return 500/503 "
                   "(response_code_class=5xx). Set 0.25 to breach; 0=healthy "
                   "(well under 0.1% 5xx).",
        },
        "latency_multiplier": {
            "type": "float",
            "default": 1.0,
            "range": [1.0, 50.0],
            "doc": "Multiplier on request_latencies and request concurrency for the "
                   "first service. Set 8.0 to breach; 1.0=healthy (p95 ~100-300 ms).",
        },
        "instance_count_spike": {
            "type": "float",
            "default": 1.0,
            "range": [1.0, 50.0],
            "doc": "Multiplier on active container/instance_count, cold starts, "
                   "billable instance time and CPU/memory allocation time for the "
                   "first service (runaway autoscaling). Set 10.0 to breach; 1.0=healthy.",
        },
    },
    chaos_mode="explicit",
    family="cloud_compute",
    schema_version="1.0",
)
