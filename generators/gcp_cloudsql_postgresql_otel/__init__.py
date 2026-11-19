"""GeneratorSpec for the ``gcp_cloudsql_postgresql_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="gcp_cloudsql_postgresql_otel",
    thread_name="gen-gcp-cloudsql-postgresql-otel",
    module="generators.gcp_cloudsql_postgresql_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("gcp.cloudsql_postgresql.otel",),
    integrations=("gcp_cloudsql_postgresql_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "cloudsql-postgresql",
            "doc": "service.name role for the Cloud SQL for PostgreSQL instances.",
        },
        "primary_instance": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "Primary instance name. Defaults to '<ns>-postgresql-primary'.",
        },
        "replica_instance": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "Read replica instance name. Defaults to '<ns>-postgresql-replica'.",
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
            "default": 1000,
            "doc": "max_connections flag value; summed num_backends stays below it.",
        },
        "databases": {
            "type": "list",
            "required": False,
            "default": ["orders", "inventory", "billing"],
            "doc": "Database names inside the instance (attributes.database / Query Insights database). Namespaced via ScenarioIdentity.database().",
        },
        "insights_users": {
            "type": "list",
            "required": False,
            "default": ["app_rw", "reporting_ro", "etl_batch"],
            "doc": "Database users reported by Query Insights (attributes.user).",
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
            "range": [0.0, 1000.0],
            "doc": "Primary network/connections count (postgresql/num_backends summed across databases and network/connections, e.g. 950 of max 1000). Drives Conns, Top 10 by connections, Backends by state, New connections/s. 0=healthy (~60-130).",
        },
        "replica_lag_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 3600.0],
            "doc": "Read replica replication/replica_lag in seconds (e.g. 300); the primary's postgresql/replication/replica_byte_lag grows with it. Drives Replica lag, Replay byte lag by replica, Replicas of this instance. 0=healthy (<1 s).",
        },
        "instance_unavailable": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "1 = primary stops serving: up=0, instance_state FAILED, partial uptime, failed connection attempts. Drives Instances up, Not RUNNING, Partial downtime minutes. 0=healthy.",
        },
        "deadlocks_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 500.0],
            "doc": "Primary postgresql/deadlock_count per minute (e.g. 25), with Lock waits in backends_in_wait, lock time share and rollbacks rising. Drives Top 10 by deadlocks, Deadlocks over time, Rollback %, Backends in wait, IO and lock share. 0=healthy (no deadlocks).",
        },
        "transaction_id_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Primary postgresql/transaction_id_utilization in percent (e.g. 85 -> 0.85), with vacuum/oldest_transaction_age and few frozen XIDs. Drives Max XID %, XID utilization, Top 10 by XID, Oldest transaction age. 0=healthy (~5-12%).",
        },
    },
    chaos_mode="explicit",
    family="database",
    schema_version="1.0",
)
