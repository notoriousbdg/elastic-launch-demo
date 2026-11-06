"""GeneratorSpec for the ``oracle`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id='oracle_otel',
    thread_name='gen-oracle-otel',
    module="generators.oracle_otel.generator",
    client='otlp',
    signals=('metrics', 'logs'),
    datasets=('oracledbreceiver.otel',),
    integrations=('oracle_otel',),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        'service_name': {'type': 'str', 'required': False, 'default': 'oracle-db', 'doc': 'service.name for the Oracle instance.'},
        'host_name': {'type': 'str', 'required': False, 'default': None, 'doc': "host.name override."},
        'instance_name': {'type': 'str', 'required': False, 'default': None, 'doc': 'Oracle instance name.'},
        'db_service': {'type': 'str', 'required': False, 'default': None, 'doc': 'Oracle service name.'},
        'tablespaces': {'type': 'list', 'required': False, 'default': None, 'doc': 'Tablespace names.'},
        'queries': {'type': 'list', 'required': False, 'default': None, 'doc': 'Query templates.'},
        'cloud': {'type': 'dict', 'required': False, 'default': {'provider': 'aws', 'platform': 'aws_rds', 'region': 'us-east-1', 'availability_zone': 'us-east-1a'}, 'doc': 'Cloud attrs.'},
    },
        impact_keys={
        "buffer_cache_hit_ratio_low": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Oracle OTel] Buffer cache hit ratio low\": set below 0.9 (physical_reads/logical_reads) to breach; default 100.0=healthy.",
        },
        "dml_lock_utilisation_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Oracle OTel] DML lock utilisation high\": set above 0.0 (limit_val) to breach; 0=healthy.",
        },
        "enqueue_deadlocks_detected": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Oracle OTel] Enqueue deadlocks detected\": set above 0.0 (delta) to breach; 0=healthy.",
        },
        "enqueue_lock_utilisation_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Oracle OTel] Enqueue lock utilisation high\": set above 0.0 (limit_val) to breach; 0=healthy.",
        },
        "hard_parse_ratio_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Oracle OTel] Hard parse ratio high\": set above 10.0 (parse_calls) to breach; 0=healthy.",
        },
        "process_utilisation_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 850.0],
            "doc": "Drive alert \"[Oracle OTel] Process utilisation high\": set above 85.0 (processes.limit) to breach; 0=healthy.",
        },
        "rollback_to_commit_ratio_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Oracle OTel] Rollback-to-commit ratio high\": set above 0.0 (total_tx) to breach; 0=healthy.",
        },
        "session_utilisation_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Oracle OTel] Session utilisation high\": set above 0.0 (limit_val) to breach; 0=healthy.",
        },
        "tablespace_utilisation_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Oracle OTel] Tablespace utilisation high\": set above 0.0 (limit_val) to breach; 0=healthy.",
        },
        "transaction_limit_utilisation_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 850.0],
            "doc": "Drive alert \"[Oracle OTel] Transaction limit utilisation high\": set above 85.0 (transactions.limit) to breach; 0=healthy.",
        },
    },
    chaos_mode='explicit',
    family='database',
    schema_version="1.0",
)
