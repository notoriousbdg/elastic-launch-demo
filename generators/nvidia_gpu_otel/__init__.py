"""GeneratorSpec for the ``nvidia_gpu`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="nvidia_gpu_otel",
    thread_name="gen-nvidia-gpu-otel",
    module="generators.nvidia_gpu_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("nvidia_gpu.otel",),
    integrations=("nvidia_gpu_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="platform",
        impact_keys={
        "clock_frequency_throttled_under_load": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 500.0],
            "doc": "Drive alert \"[NVIDIA GPU OTel] Clock frequency throttled under load\": set above 50.0 (avg_util) to breach; 0=healthy.",
        },
        "framebuffer_memory_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[NVIDIA GPU OTel] Framebuffer memory utilization high\": set above 0.9 (total) to breach; 0=healthy.",
        },
        "gpu_idle_during_expected_workload": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[NVIDIA GPU OTel] GPU idle during expected workload\": set below 1.0 (max_util) to breach; default 100.0=healthy.",
        },
        "gpu_temperature_critical": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 830.0],
            "doc": "Drive alert \"[NVIDIA GPU OTel] GPU temperature critical\": set above 83.0 (DCGM_FI_DEV_GPU_TEMP) to breach; 0=healthy.",
        },
        "gpu_utilization_saturated": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 950.0],
            "doc": "Drive alert \"[NVIDIA GPU OTel] GPU utilization saturated\": set above 95.0 (avg_util) to breach; 0=healthy.",
        },
        "xid_errors_detected": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[NVIDIA GPU OTel] XID errors detected\": set above 0.0 (DCGM_FI_DEV_XID_ERRORS) to breach; 0=healthy.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "nvidia-gpu",
            "doc": "service.name for the DCGM exporter.",
        },
        "gpu_count": {
            "type": "int",
            "required": False,
            "default": 2,
            "doc": "Number of synthetic GPU devices to model.",
        },
        "model_name": {
            "type": "str",
            "required": False,
            "default": "A100-SXM4-40GB",
            "doc": "GPU model name reported in the modelName attribute.",
        },
        "driver_version": {
            "type": "str",
            "required": False,
            "default": "550.54.15",
            "doc": "DCGM_FI_DRIVER_VERSION label used by the inventory table.",
        },
        "baseline_xid_rate": {
            "type": "float",
            "required": False,
            "default": 0.0,
            "doc": "Per-tick probability a healthy GPU starts a transient XID (fills Error Details). 0=healthy; set >0 explicitly to demo ambient XIDs without breaching the alert (any panel fill still counts as a XID occurrence, so leave at 0 unless the alert is muted).",
        },
    },
    schema_version="1.0"
)
