"""GeneratorSpec for the ``otel_rum_dashboards`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="otel_rum_dashboards",
    thread_name="gen-otel-rum",
    module="generators.otel_rum_dashboards.generator",
    client="otlp",
    signals=("logs", "traces"),
    datasets=("generic.otel",),
    integrations=("otel_rum_dashboards",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "apps": {
            "type": "list", "required": False,
            "default": ["storefront-web", "checkout-web", "account-portal"],
            "doc": "Web app names (service.name, namespace-qualified).",
        },
        "document_load_span_apps": {
            "type": "list", "required": False,
            "default": ["account-portal"],
            "doc": "Apps still on @opentelemetry/instrumentation-document-load: page loads arrive "
                   "as documentLoad spans instead of browser.navigation_timing events (the "
                   "dashboard reads both).",
        },
        "app_version": {
            "type": "str", "required": False, "default": "2026.10.3",
            "doc": "service.version for every web app.",
        },
        "page_views_per_tick": {
            "type": "int", "required": False, "default": 6,
            "doc": "Average page views per app per tick.",
        },
    },
    impact_keys={
        "web_vital_degradation": {
            "type": "float",
            "default": 1.0,
            "range": [1.0, 8.0],
            "doc": "Multiplier on LCP, INP, FCP and TTFB (and CLS shifts) reported as "
                   "browser.web_vital events. Healthy 1.0 keeps LCP ~1.2-2.4 s and INP ~40-180 ms "
                   "(good); 3.0+ pushes the LCP / INP gauges past their 4 s / 500 ms poor marks.",
        },
        "js_error_rate": {
            "type": "float",
            "default": 0.02,
            "range": [0.0, 1.0],
            "doc": "Fraction of page views that throw an uncaught JS exception. Healthy 0.02 "
                   "(ResizeObserver / ChunkLoadError noise); during a fault most errors are a "
                   "TypeError in the checkout bundle, which tops the Top Errors table.",
        },
        "resource_load_latency": {
            "type": "float",
            "default": 1.0,
            "range": [1.0, 15.0],
            "doc": "Multiplier on static asset / API fetch time (resourceFetch spans, "
                   "browser.resource_timing events) and therefore load_event_end. Healthy 1.0 "
                   "gives a ~1-2.5 s page load median; 5.0 moves Load Median / 95th / 99th "
                   "percentile to 5-10 s.",
        },
    },
    chaos_mode="explicit",
    family="web",
    schema_version="1.0",
    identity_fields=("service.name",),
)
