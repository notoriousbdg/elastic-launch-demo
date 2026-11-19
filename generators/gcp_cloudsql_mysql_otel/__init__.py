"""GeneratorSpec for the ``gcp_cloudsql_mysql_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="gcp_cloudsql_mysql_otel",
    thread_name="gen-gcp-cloudsql-mysql-otel",
    module="generators.gcp_cloudsql_mysql_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("gcp.cloudsql_mysql.otel",),
    integrations=("gcp_cloudsql_mysql_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "cloudsql-mysql",
            "doc": "service.name role for the Cloud SQL for MySQL instances.",
        },
        "primary_instance": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "Primary instance name. Defaults to '<ns>-mysql-primary'.",
        },
        "replica_instance": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "Read replica instance name. Defaults to '<ns>-mysql-replica'.",
        },
        "project_id": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "GCP project id. Defaults to ScenarioIdentity(ns).gcp_project_id.",
        },
        "tier": {
            "type": "str",
            "required": False,
            "default": "db-custom-4-16384",
            "doc": "Machine tier; sets reserved cores and memory quota.",
        },
        "disk_size_gb": {
            "type": "int",
            "required": False,
            "default": 500,
            "doc": "Provisioned disk size (disk/quota) in GiB.",
        },
        "max_connections": {
            "type": "int",
            "required": False,
            "default": 4000,
            "doc": "max_connections flag value (mysql/max_connections).",
        },
        "cloud": {
            "type": "dict",
            "required": False,
            "default": {
                "provider": "gcp",
                "platform": "gcp_cloud_sql",
                "region": "us-central1",
                "availability_zone": "us-central1-a",
            },
            "doc": "Cloud provider/platform/region/zone. region becomes the Cloud SQL region label.",
        },
    },
    impact_keys={
        "cpu_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Primary database/cpu/utilization in percent (e.g. 95 -> 0.95). Drives Max CPU %, Top 10 by CPU, CPU vs reserved cores. 0=healthy (~15-50%).",
        },
        "memory_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Primary database/memory/utilization in percent (e.g. 96). Drives Max memory %, memory vs quota. 0=healthy (~55-68%).",
        },
        "disk_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Primary disk/utilization and bytes_used in percent of quota (e.g. 93). Drives Max disk %, free GiB, disk vs quota. 0=healthy (~40-48%).",
        },
        "connections_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 4000.0],
            "doc": "Primary network/connections count (e.g. 3800 of max_connections 4000). Drives Connection usage %, connections vs max, threads connected, aborted connects. 0=healthy (~60-130).",
        },
        "replica_lag_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 3600.0],
            "doc": "Read replica replication/replica_lag and seconds_behind_master in seconds (e.g. 300). Drives Replica lag, network lag and Replication status. 0=healthy (<1 s).",
        },
        "instance_unavailable": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "1 = primary stops serving: up=0, instance_state FAILED, partial uptime, aborted connects. Drives Instances up, Not RUNNING, Partial downtime minutes. 0=healthy.",
        },
        "slow_queries_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Percent of primary queries counted in mysql/slow_queries_count (e.g. 15). Drives Slow query %, slow query ratio, Top 10 by slow queries, temp tables on disk. 0=healthy (<0.2%).",
        },
        "innodb_lock_waits_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 500.0],
            "doc": "Primary InnoDB row lock waits per second (e.g. 40); lock timeouts and deadlocks rise with it. Drives Row lock waits and lock timeouts, Deadlocks over time. 0=healthy (<0.5/s, no deadlocks).",
        },
    },
    chaos_mode="explicit",
    family="database",
    schema_version="1.0",
)
