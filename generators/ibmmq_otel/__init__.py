"""GeneratorSpec for the ``ibmmq`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="ibmmq_otel",
    thread_name="gen-ibmmq-otel",
    module="generators.ibmmq_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("ibmmq.otel",),
    integrations=("ibmmq_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="queue",
        impact_keys={
        "cpu_load_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 850.0],
            "doc": "Drive alert \"[IBM MQ OTel] CPU load high\": set above 85.0 (ibmmq_qmgr_cpu_load_five_minute_average_percentage) to breach; 0=healthy.",
        },
        "messages_expiring_before_consumption": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[IBM MQ OTel] Messages expiring before consumption\": set above 0 to breach: messages expired per emit cycle (SUM(RATE(ibmmq_qmgr_expired_message_total)) > 0); 0=healthy.",
        },
        "failed_connection_attempts": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[IBM MQ OTel] Failed connection attempts\": set above 0 to breach: failed MQCONN and MQOPEN calls per emit cycle (rate > 0); 0=healthy. Does not touch the message-operation, expiry or FDC counters.",
        },
        "failed_message_operations": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[IBM MQ OTel] Failed message operations\": set above 0 to breach: failed MQPUT, MQPUT1 and MQGET calls per emit cycle (rate > 0); 0=healthy. Does not touch the connection, expiry or FDC counters.",
        },
        "fdc_files_increasing": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[IBM MQ OTel] FDC files increasing\": set above 0 to breach: new FDC files per emit cycle (DELTA > 0); 0=healthy.",
        },
        "high_log_write_latency": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[IBM MQ OTel] High log write latency\": set above 5 to breach: log write latency in milliseconds (the alert fires above 0.005 s); 0=healthy.",
        },
        "high_transaction_rollback_ratio": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[IBM MQ OTel] High transaction rollback ratio\": set above 5 to breach: percent of transactions rolled back (the alert fires above 5%); 0=healthy.",
        },
        "log_filesystem_critically_low": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[IBM MQ OTel] Log filesystem critically low\": set below 10 to breach: target log filesystem free space in percent (ibmmq_qmgr_log_file_system_free_space_percentage); only applies below the default 100.0=healthy.",
        },
        "queue_manager_filesystem_low": {
            "type": "float",
            "default": 200.0,
            "range": [0.0, 200.0],
            "doc": "Drive alert \"[IBM MQ OTel] Queue manager filesystem low\": set below 20 to breach: target queue manager filesystem free space in percent; only applies below the default 200.0=healthy.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "ibmmq",
            "doc": "service.name for the IBM MQ exporter.",
        },
        "qmgr_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "Queue manager name.  Defaults to <namespace>_QM1.",
        },
        "host_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "host.name for the queue manager host. Defaults to <namespace>-ibmmq.",
        },
    },
    schema_version="1.0"
)
