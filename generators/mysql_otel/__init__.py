"""GeneratorSpec for the ``mysql`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id='mysql_otel',
    thread_name='gen-mysql-otel',
    module="generators.mysql_otel.generator",
    client='otlp',
    signals=('metrics', 'logs'),
    datasets=('mysqlreceiver.otel',),
    integrations=('mysql_otel',),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        'service_name': {'type': 'str', 'required': False, 'default': 'mysql-primary', 'doc': 'service.name'},
        'host_name': {'type': 'str', 'required': False, 'default': None, 'doc': 'host.name'},
        'db_system': {'type': 'str', 'required': False, 'default': 'mysql', 'doc': 'db.system'},
        'databases': {'type': 'list', 'required': False, 'default': None, 'doc': 'databases'},
        'tables': {'type': 'dict', 'required': False, 'default': None, 'doc': 'tables'},
        'queries': {'type': 'list', 'required': False, 'default': None, 'doc': 'queries'},
        'cloud': {'type': 'dict', 'required': False, 'default': {'provider': 'gcp', 'platform': 'gcp_compute_engine', 'region': 'us-central1', 'availability_zone': 'us-central1-b'}, 'doc': 'cloud'},
    },
        impact_keys={
        "high_buffer_pool_dirty_page_ratio": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[MySQL OTel] High buffer pool dirty page ratio\": fraction 0–1 or percent (GT 75.0→0.75). Alert is >0.75; 0=healthy. Emit usage as double.",
        },
        "high_connection_error_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[MySQL OTel] High connection error rate\": set above 5.0 (increase) to breach; 0=healthy.",
        },
        "high_row_lock_contention": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1000.0],
            "doc": "Drive alert \"[MySQL OTel] High row lock contention\": set above 100.0 (increase) to breach; 0=healthy.",
        },
        "high_slow_query_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[MySQL OTel] High slow query rate\": set above 10.0 (increase) to breach; 0=healthy.",
        },
        "replication_lag": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 600.0],
            "doc": "Drive alert \"[MySQL OTel] Replication lag\": set above 60.0 (time_behind_source) to breach; 0=healthy.",
        },
        "thread_saturation": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "running/connected ratio; >0.9 fires. Alert: [MySQL OTel] Thread saturation.",
        },
    },
    chaos_mode='any',
    family='database',
    schema_version="1.0",
)
