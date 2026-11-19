"""GeneratorSpec for the ``gcp_cloudstorage_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="gcp_cloudstorage_otel",
    thread_name="gen-gcp-cloudstorage-otel",
    module="generators.gcp_cloudstorage_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("gcp.cloudstorage.otel",),
    integrations=("gcp_cloudstorage_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "gcp-cloudstorage",
            "doc": "service.name for the Cloud Monitoring collector resource.",
        },
        "cloud": {
            "type": "dict",
            "required": False,
            "default": {
                "provider": "gcp",
                "platform": "gcp_cloud_storage",
                "region": "us-central1",
                "availability_zone": "us-central1-a",
            },
            "doc": "Cloud provider/platform/region; region is the regional bucket location.",
        },
        "project_id": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "GCP project id.  Defaults to ScenarioIdentity(ns).gcp_project_id.",
        },
        "buckets": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "Bucket roles (namespace-qualified).  Defaults to data-lake, "
                   "media-assets and backups.  The first bucket is the fault target.",
        },
    },
    impact_keys={
        "error_4xx_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Fraction of requests on the first bucket rejected with "
                   "PERMISSION_DENIED (IAM binding removed, client-fault class). "
                   "Set 0.4 to breach; 0=healthy (only a trickle of NOT_FOUND).",
        },
        "error_5xx_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Fraction of requests on the first bucket failing with UNAVAILABLE / "
                   "INTERNAL / DEADLINE_EXCEEDED (service-fault class). Set 0.2 to "
                   "breach; 0=healthy (availability above 99.9%).",
        },
        "request_spike": {
            "type": "float",
            "default": 1.0,
            "range": [1.0, 50.0],
            "doc": "Multiplier on api/request_count and network sent/received bytes "
                   "for the first bucket (runaway reader or hot-linking). Set 10.0 to "
                   "breach; 1.0=healthy.",
        },
    },
    chaos_mode="explicit",
    family="cloud_compute",
    schema_version="1.0",
)
