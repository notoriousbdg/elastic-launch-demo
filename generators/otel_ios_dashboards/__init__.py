"""GeneratorSpec for the ``otel_ios_dashboards`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="otel_ios_dashboards",
    thread_name="gen-otel-ios",
    module="generators.otel_ios_dashboards.generator",
    client="otlp",
    signals=("logs", "traces"),
    datasets=("generic.otel",),
    integrations=("otel_ios_dashboards",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "apps": {
            "type": "list", "required": False,
            "default": ["shopper-ios", "courier-ios"],
            "doc": "iOS app names (service.name, namespace-qualified).",
        },
        "app_versions": {
            "type": "list", "required": False,
            "default": [["5.12.0", 12], ["5.13.1", 38], ["5.14.0", 50]],
            "doc": "[version, weight] pairs for service.version. The last entry is the newest "
                   "release, the one crash_rate faults land on.",
        },
        "devices": {
            "type": "list", "required": False, "default": None,
            "doc": "Device mix: dicts with model_identifier, model_name, weight. Defaults to an "
                   "iPhone 12-16 / iPad mix.",
        },
        "os_versions": {
            "type": "list", "required": False,
            "default": [["18.1", 38], ["18.0.1", 22], ["17.6.1", 25], ["17.5", 9], ["16.7.10", 6]],
            "doc": "[os.version, weight] pairs.",
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
            "doc": "Per-session, per-tick probability of an app.crash event on the newest app "
                   "version (a bad release: Swift 'Index out of range' trap in checkout). Healthy "
                   "0.003 keeps the Crash rate panel near 1-2%; 0.2+ drives it toward 30-60% and "
                   "fills Total crashes / Crashes table / Exception Details.",
        },
        "hang_rate": {
            "type": "float",
            "default": 0.004,
            "range": [0.0, 0.5],
            "doc": "Per-session, per-tick probability of a main-thread hang: an app.hang log with "
                   "the main-thread backtrace (counted by Exception Details) and a 2.5-8 s "
                   "'View appearing' span (p95 duration). Healthy 0.004.",
        },
        "slow_start": {
            "type": "float",
            "default": 1.0,
            "range": [1.0, 10.0],
            "doc": "Multiplier on AppLaunch span duration (cold 0.6-1.3 s, warm 0.2-0.5 s at the "
                   "healthy 1.0). Visible in All spans average duration and p95 duration over time.",
        },
    },
    chaos_mode="explicit",
    family="web",
    schema_version="1.0",
    identity_fields=("service.name",),
)
