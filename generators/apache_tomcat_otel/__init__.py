"""GeneratorSpec for the ``apache_tomcat_otel`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="apache_tomcat_otel",
    thread_name="gen-apache-tomcat-otel",
    module="generators.apache_tomcat_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("tomcat.otel",),
    integrations=("apache_tomcat_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="web",
        impact_keys={
        "connection_pool_saturation": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Connection/maxConnections ratio; values >0.8 fire connection-pool alert. Alert: [Tomcat OTel] Connection pool saturation.",
        },
        "file_descriptor_exhaustion": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Tomcat OTel] File descriptor exhaustion\": set above 0.0 (max_fds) to breach; 0=healthy.",
        },
        "high_jvm_process_cpu_usage": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Tomcat OTel] High JVM process CPU usage\": set above 0.9 (java_lang_OperatingSystem_ProcessCpuLoad) to breach; 0=healthy.",
        },
        "high_request_error_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Tomcat OTel] High request error rate\": set above 0.05 (requestCount) to breach; 0=healthy.",
        },
        "jvm_heap_memory_pressure": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Tomcat OTel] JVM heap memory pressure\": set above 0.85 (HeapMemoryUsage_max) to breach; 0=healthy.",
        },
        "old_generation_gc_activity": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Tomcat OTel] Old generation GC activity\": set above 0.0 (full_gc_count) to breach; 0=healthy.",
        },
        "request_latency_spike": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 5000.0],
            "doc": "Drive alert \"[Tomcat OTel] Request latency spike\": set above 500.0 (requestCount) to breach; 0=healthy.",
        },
        "session_rejection": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Tomcat OTel] Session rejection\": set above 0.0 (new_rejections) to breach; 0=healthy.",
        },
        "thread_pool_saturation": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Tomcat OTel] Thread pool saturation\": set above 0.8 (maxThreads) to breach; 0=healthy.",
        },
    },
    config_fields={
        "service_name": {"type": "str", "required": False, "default": "apache-tomcat",
                         "doc": "service.name for the Tomcat JMX exporter."},
        "connector_port": {"type": "int", "required": False, "default": 8080,
                           "doc": "HTTP connector port reported in the connector attribute."},
    },
    schema_version="1.0",
)
