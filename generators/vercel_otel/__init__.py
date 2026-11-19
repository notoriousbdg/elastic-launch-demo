"""GeneratorSpec for the ``vercel`` telemetry generator.

The vercel_otel package has 4 datasets:
  - vercel.logs.otel            (build and function logs)
  - vercel.analytics.v2.otel   (web analytics events)
  - vercel.auditlog.v1.otel    (audit log entries)
  - vercel.speedinsights.v1.otel (Web Vitals: CLS, INP, LCP, TTFB metrics)

This generator emits all four.  The SPEC.datasets tuple holds all four so
the deployer installs the vercel_otel integration and validates all streams.
"""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="vercel_otel",
    thread_name="gen-vercel-otel",
    module="generators.vercel_otel.generator",
    client="otlp",
    signals=("logs", "metrics"),
    datasets=(
        "vercel.logs.otel",
        "vercel.analytics.v2.otel",
        "vercel.auditlog.v1.otel",
        "vercel.speedinsights.v1.otel",
    ),
    integrations=("vercel_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="platform",
    impact_keys={
        "elevated_error_warning_log_rate_by_source": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": 'Drive alert "[Vercel OTel] Elevated ERROR/WARNING log rate by source": fraction of static/build log records emitted as WARNING 404s; set above 0.10 (bad_rate_pct > 10%) to breach; 0=healthy.',
        },
        "high_http_5xx_error_rate_by_log_source": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 0.2],
            "doc": 'Drive alert "[Vercel OTel] High HTTP 5xx error rate by log source": fraction of lambda/edge requests that return 5xx; set above 0.02 (error_rate_pct > 2%) to breach. Stay under 0.05, or the regional skew alert also fires; 0=healthy.',
        },
        "lambda_edge_function_crash_rate_elevated": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 0.2],
            "doc": 'Drive alert "[Vercel OTel] Lambda/edge function crash rate elevated": fraction of lambda/edge invocations that crash (status_code -1); set above 0.01 (crash_rate_pct > 1%) to breach; 0=healthy.',
        },
        "regional_error_rate_skew": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 0.3],
            "doc": 'Drive alert "[Vercel OTel] Regional error rate skew": 5xx rate of lambda/edge requests in the sin1 region; set above 0.05 (region_error_rate_pct > 5%) to breach. Above ~0.11 the global 5xx alert also fires; 0=healthy.',
        },
        "request_rate_drop": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": 'Drive alert "[Vercel OTel] Request rate drop (traffic collapse)": fraction of function log traffic removed; set above 0.5 (drop_pct > 50%) for 15 minutes to breach; 0=healthy.',
        },
        "page_view_rate_drop": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": 'Drive alert "[Vercel OTel] Page view rate drop": fraction of analytics page views removed; set above 0.5 (drop_pct > 50%) for 15 minutes to breach; 0=healthy.',
        },
        "firewall_deny_spike": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": 'Drive alert "[Vercel OTel] Firewall deny spike": extra denied firewall requests per cycle; set above 2 (denies > 100 in 10 minutes at the 5-10 s cycle) to breach; 0=healthy.',
        },
        "sensitive_audit_activity_spike_by_actor": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 50.0],
            "doc": 'Drive alert "[Vercel OTel] Sensitive audit activity spike by actor": extra sensitive audit events per cycle from one actor; set above 0.02 (sensitive_events > 5 in 60 minutes) to breach; 0=healthy.',
        },
        "elevated_ttfb_p75": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10.0],
            "doc": 'Drive alert "[Vercel OTel] Elevated TTFB p75 (edge/server latency)": above 1.0 most page loads take a needs-improvement / poor TTFB scaled by this value; set above 1.0 (ttfb_p75 > 1800 ms; healthy p75 is ~600 ms) and hold for about 20 minutes of the 60 minute window to breach; 0=healthy.',
        },
        "poor_core_web_vitals_p75_by_route": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10.0],
            "doc": 'Drive alert "[Vercel OTel] Poor Core Web Vitals (p75) by route": above 1.0 most page loads take a needs-improvement / poor LCP and INP scaled by this value; set above 1.0 (LCP p75 > 4000 ms; healthy p75 is ~2100 ms) to breach; 0=healthy.',
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "vercel",
            "doc": "service.name for the Vercel log drain and metrics source.",
        },
        "project_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "Vercel project name.  Defaults to <namespace>-app.",
        },
        "project_id": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "Vercel project id (prj_…).  Defaults to a stable id derived from the namespace.",
        },
        "records_per_cycle": {
            "type": "int",
            "required": False,
            "default": 80,
            "doc": "Number of log records to emit per cycle.",
        },
    },
    schema_version="1.0"
)
