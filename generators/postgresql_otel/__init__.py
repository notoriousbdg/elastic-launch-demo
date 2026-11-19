"""GeneratorSpec for the ``postgresql`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id='postgresql_otel',
    thread_name='gen-postgresql-otel',
    module="generators.postgresql_otel.generator",
    client='otlp',
    signals=('metrics', 'logs'),
    datasets=('postgresqlreceiver.otel',),
    integrations=('postgresql_otel',),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        'service_name': {'type': 'str', 'required': False, 'default': 'postgresql-primary', 'doc': 'service.name for the PostgreSQL instance.'},
        'host_name': {'type': 'str', 'required': False, 'default': None, 'doc': "host.name override.  Defaults to '<ns>-postgresql-host'."},
        'db_system': {'type': 'str', 'required': False, 'default': 'postgresql', 'doc': 'db.system attribute value.  Also read by the trace generator.'},
        'databases': {'type': 'list', 'required': False, 'default': None, 'doc': "Database names.  Defaults to '<prefix>_app/_analytics/_archive/_audit'."},
        'tables': {'type': 'dict', 'required': False, 'default': None, 'doc': 'Dict of database name to list of table names.'},
        'users': {'type': 'list', 'required': False, 'default': ['app_user', 'reporting'], 'doc': 'PostgreSQL role names used in query samples.'},
        'queries': {'type': 'list', 'required': False, 'default': None, 'doc': 'List of {operation, sql} dicts to override the built-in query templates.'},
        'cloud': {'type': 'dict', 'required': False, 'default': {'provider': 'aws', 'platform': 'aws_rds', 'region': 'us-east-1', 'availability_zone': 'us-east-1a'}, 'doc': 'Cloud provider/platform/region/az for resource attributes.'},
    },
        impact_keys={
        "backend_buffer_writes_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1000.0],
            "doc": "Drive alert \"[PostgreSQL OTel] Backend buffer writes high\": set above 100.0 (summed) to breach; 0=healthy.",
        },
        "bgwriter_maxwritten_stops": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[PostgreSQL OTel] Bgwriter maxwritten stops\": set above 0.0 (postgresql.bgwriter.maxwritten) to breach; 0=healthy.",
        },
        "connection_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 800.0],
            "doc": "Drive alert \"[PostgreSQL OTel] Connection utilization high\": set above 80.0 (connection.max) to breach; 0=healthy.",
        },
        "deadlocks_detected": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[PostgreSQL OTel] Deadlocks detected\": set above 0.0 (postgresql.deadlocks) to breach; 0=healthy.",
        },
        "exclusive_lock_contention": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[PostgreSQL OTel] Exclusive lock contention\": set above 5.0 (lock_count) to breach; 0=healthy.",
        },
        "high_rollback_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[PostgreSQL OTel] High rollback rate\": set above 1.0 (total) to breach; 0=healthy.",
        },
        "high_sequential_scans": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[PostgreSQL OTel] High sequential scans\": set above 0.0 (seq_scans_val) to breach; 0=healthy.",
        },
        "high_temp_i_o_volume": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1000.0],
            "doc": "Drive alert \"[PostgreSQL OTel] High temp I/O volume\": set above 100.0 (postgresql.temp.io) to breach; 0=healthy.",
        },
        "long_checkpoint_duration": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 300000.0],
            "doc": "Drive alert \"[PostgreSQL OTel] Long checkpoint duration\": set above 30000.0 (total_duration_ms) to breach; 0=healthy.",
        },
        "long_running_queries": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 3000000.0],
            "doc": "Drive alert \"[PostgreSQL OTel] Long-running queries\": set above 300000.0 (total_exec_time) to breach; 0=healthy.",
        },
        "low_buffer_hit_ratio": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 990.0],
            "doc": "Drive alert \"[PostgreSQL OTel] Low buffer hit ratio\": set above 99.0 (at) to breach; 0=healthy.",
        },
        "queries_waiting_on_locks": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[PostgreSQL OTel] Queries waiting on locks\": set above 0.0 (samples) to breach; 0=healthy.",
        },
        "slow_top_queries": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100000.0],
            "doc": "Drive alert \"[PostgreSQL OTel] Slow top queries\": set above 10000.0 (attributes.postgresql.total_exec_time) to breach; 0=healthy.",
        },
        "temp_files_created": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[PostgreSQL OTel] Temp files created\": set above 0.0 (postgresql.temp_files) to breach; 0=healthy.",
        },
    },
    chaos_mode='explicit',
    family='database',
    schema_version="1.0",
)
