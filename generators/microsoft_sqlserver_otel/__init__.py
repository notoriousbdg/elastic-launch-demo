"""GeneratorSpec for the ``microsoft_sqlserver_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id='microsoft_sqlserver_otel',
    thread_name='gen-microsoft-sqlserver-otel',
    module="generators.microsoft_sqlserver_otel.generator",
    client='otlp',
    signals=('metrics', 'logs'),
    datasets=('sqlserverreceiver.otel',),
    integrations=('microsoft_sqlserver_otel',),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        'service_name': {'type': 'str', 'required': False, 'default': 'sqlserver-primary', 'doc': 'service.name for the SQL Server instance.'},
        'host_name': {'type': 'str', 'required': False, 'default': None, 'doc': "host.name override.  Defaults to '<ns>-sqlserver-host'."},
        'instance_id': {'type': 'str', 'required': False, 'default': None, 'doc': "service.instance.id.  Defaults to '<host>\\\\MSSQLSERVER'."},
        'databases': {'type': 'list', 'required': False, 'default': None, 'doc': "Database names."},
        'queries': {'type': 'list', 'required': False, 'default': None, 'doc': 'Query templates.'},
        'cloud': {'type': 'dict', 'required': False, 'default': {'provider': 'azure', 'platform': 'azure_sql_db', 'region': 'eastus', 'availability_zone': ''}, 'doc': 'Cloud attrs.'},
    },
        impact_keys={
        "blocked_processes": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[SQL Server OTel] Blocked processes\": set above 0.0 (sqlserver.processes.blocked) to breach; 0=healthy.",
        },
        "buffer_cache_hit_ratio_low": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 950.0],
            "doc": "Drive alert \"[SQL Server OTel] Buffer cache hit ratio low\": set above 0 to breach: a value from 1 to 94 is the buffer cache hit ratio in percent, anything higher emits about 70% (the alert fires below 95%); 0=healthy.",
        },
        "database_i_o_latency_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[SQL Server OTel] Database I/O latency high\": set above 0 to breach: any positive value forces more than 100 ms of latency per I/O operation (the alert fires above 0.1 s per operation); 0=healthy.",
        },
        "deadlocks_detected": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[SQL Server OTel] Deadlocks detected\": set above 0.0 (sqlserver.deadlock.rate) to breach; 0=healthy.",
        },
        "execution_errors": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[SQL Server OTel] Execution errors\": set above 0.0 (sqlserver.database.execution.errors) to breach; 0=healthy.",
        },
        "free_list_stalls": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[SQL Server OTel] Free list stalls\": set above 0.0 (sqlserver.page.buffer_cache.free_list.stalls.rate) to breach; 0=healthy.",
        },
        "lock_timeouts": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[SQL Server OTel] Lock timeouts\": set above 0.0 (sqlserver.lock.timeout.rate) to breach; 0=healthy.",
        },
        "lock_wait_rate_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[SQL Server OTel] Lock wait rate high\": set above 10.0 (avg_wait_rate) to breach; 0=healthy.",
        },
        "login_rate_spike": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 500.0],
            "doc": "Drive alert \"[SQL Server OTel] Login rate spike\": set above 50.0 (sqlserver.login.rate) to breach; 0=healthy.",
        },
        "memory_grants_pending": {
            "type": "int",
            "default": 0,
            "range": [0, 1000],
            "doc": "Drive alert \"[SQL Server OTel] Memory grants pending\": set above 0 to breach: pending memory grants (sqlserver.memory.grants.pending.count); 0=healthy.",
        },
        "os_wait_duration_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 600.0],
            "doc": "Drive alert \"[SQL Server OTel] OS wait duration high\": set above 60 to breach: OS wait seconds added per emit cycle for each wait type (the alert needs more than 60 s of increase in its window); 0=healthy.",
        },
        "page_life_expectancy_low": {
            "type": "float",
            "default": 3000.0,
            "range": [0.0, 3000.0],
            "doc": "Drive alert \"[SQL Server OTel] Page life expectancy low\": set below 300 to breach: page life expectancy in seconds (0 is valid); a value from 300 to 2999 still emits 120 s (under the line); default 3000.0=healthy.",
        },
        "tempdb_space_low": {
            "type": "float",
            "default": 1024000.0,
            "range": [0.0, 1024000.0],
            "doc": "Drive alert \"[SQL Server OTel] TempDB space low\": set below 102400 to breach: free tempdb space in kB (0 is valid); a value from 102400 to 1023999 still emits 50000; default 1024000.0=healthy.",
        },
        "tempdb_version_store_large": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10485760.0],
            "doc": "Drive alert \"[SQL Server OTel] TempDB version store large\": set above 1048576.0 (max_version_store_kb) to breach; 0=healthy.",
        },
    },
    chaos_mode='explicit',
    family='database',
    schema_version="1.0",
)
