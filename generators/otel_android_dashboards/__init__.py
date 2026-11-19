"""GeneratorSpec for the ``otel_android_dashboards`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="otel_android_dashboards",
    thread_name="gen-otel-android",
    module="generators.otel_android_dashboards.generator",
    client="otlp",
    signals=("logs", "traces"),
    datasets=("generic.otel",),
    integrations=("otel_android_dashboards",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "apps": {
            "type": "list", "required": False,
            "default": ["shopper-android", "courier-android"],
            "doc": "Android app names (service.name, namespace-qualified).",
        },
        "app_versions": {
            "type": "list", "required": False,
            "default": [["5.12.0", 15], ["5.13.1", 35], ["5.14.0", 50]],
            "doc": "[version, weight] pairs for service.version. The last entry is the newest "
                   "release, the one crash_rate faults land on.",
        },
        "devices": {
            "type": "list", "required": False, "default": None,
            "doc": "Device mix: dicts with manufacturer, model_identifier, model_name, weight. "
                   "Defaults to a Samsung / Google / Xiaomi / OnePlus / Motorola mix.",
        },
        "os_versions": {
            "type": "list", "required": False,
            "default": [["14", 45], ["13", 28], ["15", 15], ["12", 12]],
            "doc": "[os.version, weight] pairs (Android major versions).",
        },
        "installations_per_app": {
            "type": "int", "required": False, "default": 60,
            "doc": "Installed devices per app (distinct app.installation.id).",
        },
        "active_sessions_per_app": {
            "type": "int", "required": False, "default": 16,
            "doc": "Sessions open per app at any time.",
        },
    },
    impact_keys={
        "crash_rate": {
            "type": "float",
            "default": 0.003,
            "range": [0.0, 0.5],
            "doc": "Per-session, per-tick probability of a device.crash event on the newest app "
                   "version (a bad release: IndexOutOfBoundsException in checkout). Healthy 0.003 "
                   "keeps the Crash rate panel near 1-2%; 0.2+ drives it toward 30-60% and fills "
                   "Total crashes / Crashes table / Exception Details.",
        },
        "anr_rate": {
            "type": "float",
            "default": 0.004,
            "range": [0.0, 0.5],
            "doc": "Per-session, per-tick probability of an Application Not Responding event "
                   "(device.anr with the blocked main-thread stack) plus a 5-9 s Resumed span. "
                   "Healthy 0.004; raising it grows Exception Details occurrences and the p95 span "
                   "duration line.",
        },
        "slow_start": {
            "type": "float",
            "default": 1.0,
            "range": [1.0, 10.0],
            "doc": "Multiplier on AppStart span duration (cold 0.7-1.4 s, warm 0.25-0.6 s at the "
                   "healthy 1.0). Visible in All spans average duration and p95 duration over time.",
        },
    },
    chaos_mode="explicit",
    family="web",
    schema_version="1.0",
    identity_fields=("service.name",),
)
