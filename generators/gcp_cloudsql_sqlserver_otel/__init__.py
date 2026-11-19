"""GeneratorSpec for the ``gcp_cloudsql_sqlserver_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="gcp_cloudsql_sqlserver_otel",
    thread_name="gen-gcp-cloudsql-sqlserver-otel",
    module="generators.gcp_cloudsql_sqlserver_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("gcp.cloudsql_sqlserver.otel",),
    integrations=("gcp_cloudsql_sqlserver_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "cloudsql-sqlserver",
            "doc": "service.name role for the Cloud SQL for SQL Server instances.",
        },
        "primary_instance": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "Primary instance name. Defaults to '<ns>-sqlserver-primary'.",
        },
        "replica_instance": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "Read replica instance name. Defaults to '<ns>-sqlserver-replica'.",
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
            "default": "db-custom-8-32768",
            "doc": "Machine tier; sets reserved cores and memory quota.",
        },
        "disk_size_gb": {
            "type": "int",
            "required": False,
            "default": 750,
            "doc": "Provisioned disk size (disk/quota) in GiB.",
        },
        "databases": {
            "type": "list",
            "required": False,
            "default": ["orders", "inventory", "billing"],
            "doc": "User database names (attributes.database on transaction_count). Namespaced via ScenarioIdentity.database().",
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
            "range": [0.0, 5000.0],
            "doc": "Primary network/connections count (network/connections, e.g. 4000). Drives Conns, Open connections, Top 10 by connections, login attempts. 0=healthy (~60-130).",
        },
        "replica_lag_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 3600.0],
            "doc": "Read replica replication/replica_lag in seconds (e.g. 300). Drives Auto-failover requests and replica lag. 0=healthy (<1 s).",
        },
        "instance_unavailable": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "1 = primary stops serving: up=0, instance_state FAILED, partial uptime, failed logins, available_for_failover 0 and an auto-failover request. Drives Instances up, Not RUNNING, Partial downtime minutes. 0=healthy.",
        },
        "blocked_processes_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 500.0],
            "doc": "Primary sqlserver/connections/processes_blocked (e.g. 40), with lock waits, lock wait time, runnable tasks and memory grants pending rising. Drives Peak blocked processes, Blocked processes over time, Top 10 by blocked processes and average lock wait. 0=healthy (0-1).",
        },
        "deadlocks_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 500.0],
            "doc": "Primary sqlserver/transactions/deadlock_count per minute (e.g. 20), split across locked_resource values. Drives Deadlocks by locked resource, Top 10 by deadlocks, Deadlocks column. 0=healthy (no deadlocks).",
        },
    },
    chaos_mode="explicit",
    family="database",
    schema_version="1.0",
)
