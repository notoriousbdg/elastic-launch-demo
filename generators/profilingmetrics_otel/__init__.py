"""GeneratorSpec for the ``profilingmetrics_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="profilingmetrics_otel",
    thread_name="gen-profilingmetrics-otel",
    module="generators.profilingmetrics_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("profilingmetricsconnector.otel",),
    integrations=("profilingmetrics_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "sample_frequency_hz": {
            "type": "int", "required": False, "default": 20,
            "doc": "Profiler sampling frequency per busy CPU (EDOT profiler default 20 Hz).",
        },
        "hotspot_service": {
            "type": "str", "required": False, "default": None,
            "doc": "Scenario service whose process takes the cpu_hotspot_multiplier fault. "
                   "Defaults to the first JVM service, else the first scenario service.",
        },
        "extra_processes": {
            "type": "list", "required": False, "default": None,
            "doc": "Per-host infrastructure processes: dicts with executable, service, runtime "
                   "(jvm / cpython / go / v8js / dotnet / rust / native) and cpu (cores busy). "
                   "Defaults to containerd, envoy and a Node.js web frontend.",
        },
    },
    impact_keys={
        "cpu_hotspot_multiplier": {
            "type": "float",
            "default": 1.0,
            "range": [1.0, 10.0],
            "doc": "Multiplier on user-space samples of the hotspot service's process (runtime "
                   "and native frames). Healthy 1.0; 4.0+ makes that process dominate User / "
                   "Kernel CPU and its frame type (e.g. JVM) dominate User CPU by frame type.",
        },
        "kernel_time_spike": {
            "type": "float",
            "default": 1.0,
            "range": [1.0, 20.0],
            "doc": "Multiplier on kernel samples, weighted toward futex lock contention and "
                   "fsync disk writes. Healthy 1.0 keeps kernel ~10-20% of samples; 6.0+ makes "
                   "Kernel CPU by category / by system call show synchronization and disk spikes.",
        },
        "native_spike": {
            "type": "float",
            "default": 1.0,
            "range": [1.0, 20.0],
            "doc": "Multiplier on native samples in libcrypto.so.3 / libssl.so.3 (TLS handshake "
                   "storm). Healthy 1.0; raising it grows those slices in User CPU by shared "
                   "library and the Native series in User CPU by frame type.",
        },
    },
    chaos_mode="explicit",
    family="platform",
    schema_version="1.0",
)
