#!/usr/bin/env python3
"""Cloud SQL for PostgreSQL generator: Cloud Monitoring metrics via OTLP.

Emulates the OpenTelemetry ``googlecloudmonitoringreceiver`` pulling
``cloudsql.googleapis.com/database/...`` time series for one primary and one
read replica, routed to ``metrics-gcp.cloudsql_postgresql.otel-default`` (read
by the gcp_cloudsql_postgresql_otel Overview and Instance Detail dashboards).
See ``generator_engine/gcp_cloudsql_common.py`` for the receiver's
GAUGE / DELTA / CUMULATIVE mapping.

Two monitored resources:

* ``cloudsql_database`` -- ``gcp.resource_type``, ``database_id``
  (``<project>:<instance>``), ``project_id``, ``region``. Instance and
  per-database series. Panels key PostgreSQL instances on
  ``postgresql/num_backends`` (the marker every Overview panel joins on).
* ``cloudsql_instance_database`` -- Query Insights
  (``postgresql/insights/aggregate/*``, CUMULATIVE, read with ``RATE``):
  ``resource_id``, ``resource_container``, ``location``, ``database``. The
  Overview drilldown filters ``database_id`` OR ``resource_id``.

Datapoint attributes read by panels: attributes.state, attributes.database,
attributes.transaction_type, attributes.source, attributes.operation_type,
attributes.login_status, attributes.wait_event, attributes.wait_event_type,
attributes.backend_type, attributes.action, attributes.oldest_transaction_type,
attributes.replica_name, attributes.replica_lag_type, attributes.user,
attributes.client_addr, attributes.lock_type, attributes.access_type.

Usage (standalone):
    python3 -m generators.gcp_cloudsql_postgresql_otel.generator
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/gcp_cloudsql_postgresql_otel@0.1.0"

import logging
import os
import random
import signal
import threading

from app.config import NAMESPACE
from app.telemetry import OTLPClient
from generator_engine.db_common import (
    CounterState,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)
from generator_engine.gcp_cloudsql_common import (
    M,
    SCRAPE_INTERVAL_S,
    CloudSQLInstance,
    CommonFaults,
    InstanceSample,
    build_instances,
    common_instance_metrics,
    connection_attempts,
    cumulative,
    default_cloud,
    delta,
    fval,
    gauge,
    insights_resource,
    instance_resource,
    receiver_scope,
    sample_instance,
)
from generator_engine.identity import ScenarioIdentity
from generator_engine.tick import run_tick_loop
from generators.gcp_cloudsql_postgresql_otel import SPEC

logger = logging.getLogger("gcp-cloudsql-postgresql-generator")

GENERATOR_ID = "gcp_cloudsql_postgresql_otel"
DATASET = "gcp.cloudsql_postgresql"  # -> metrics-gcp.cloudsql_postgresql.otel-default

METRIC_NAMES = (
    "cloudsql.googleapis.com/database/up",
    "cloudsql.googleapis.com/database/uptime",
    "cloudsql.googleapis.com/database/instance_state",
    "cloudsql.googleapis.com/database/cpu/utilization",
    "cloudsql.googleapis.com/database/cpu/reserved_cores",
    "cloudsql.googleapis.com/database/memory/utilization",
    "cloudsql.googleapis.com/database/memory/usage",
    "cloudsql.googleapis.com/database/memory/total_usage",
    "cloudsql.googleapis.com/database/memory/quota",
    "cloudsql.googleapis.com/database/disk/bytes_used",
    "cloudsql.googleapis.com/database/disk/quota",
    "cloudsql.googleapis.com/database/disk/utilization",
    "cloudsql.googleapis.com/database/disk/read_ops_count",
    "cloudsql.googleapis.com/database/disk/write_ops_count",
    "cloudsql.googleapis.com/database/disk/read_bytes_count",
    "cloudsql.googleapis.com/database/disk/write_bytes_count",
    "cloudsql.googleapis.com/database/network/connections",
    "cloudsql.googleapis.com/database/network/received_bytes_count",
    "cloudsql.googleapis.com/database/network/sent_bytes_count",
    "cloudsql.googleapis.com/database/network/connection_attempt_count",
    "cloudsql.googleapis.com/database/replication/replica_lag",
    "cloudsql.googleapis.com/database/available_for_failover",
    "cloudsql.googleapis.com/database/postgresql/num_backends",
    "cloudsql.googleapis.com/database/postgresql/num_backends_by_state",
    "cloudsql.googleapis.com/database/postgresql/transaction_count",
    "cloudsql.googleapis.com/database/postgresql/transaction_id_utilization",
    "cloudsql.googleapis.com/database/postgresql/transaction_id_count",
    "cloudsql.googleapis.com/database/postgresql/vacuum/oldest_transaction_age",
    "cloudsql.googleapis.com/database/postgresql/deadlock_count",
    "cloudsql.googleapis.com/database/postgresql/new_connection_count",
    "cloudsql.googleapis.com/database/postgresql/temp_bytes_written_count",
    "cloudsql.googleapis.com/database/postgresql/temp_files_written_count",
    "cloudsql.googleapis.com/database/postgresql/blocks_read_count",
    "cloudsql.googleapis.com/database/postgresql/statements_executed_count",
    "cloudsql.googleapis.com/database/postgresql/backends_in_wait",
    "cloudsql.googleapis.com/database/postgresql/replication/replica_byte_lag",
    "cloudsql.googleapis.com/database/postgresql/insights/aggregate/execution_time",
    "cloudsql.googleapis.com/database/postgresql/insights/aggregate/io_time",
    "cloudsql.googleapis.com/database/postgresql/insights/aggregate/lock_time",
    "cloudsql.googleapis.com/database/postgresql/insights/aggregate/row_count",
    "cloudsql.googleapis.com/database/postgresql/insights/aggregate/shared_blk_access_count",
)

PG = M + "postgresql/"
INSIGHTS = PG + "insights/aggregate/"
OPERATIONS = (("SELECT", 0.72), ("INSERT", 0.14), ("UPDATE", 0.11), ("DELETE", 0.03))
LAG_TYPES = ("sent_location", "write_location", "flush_location", "replay_location")
OLDEST_TYPES = ("running", "prepared", "replication_slot", "replica")
LOCK_TYPES = ("lw", "hw", "buffer_pin")
# (backend_type, wait_event, wait_event_type)
WAIT_EVENTS_HEALTHY = (
    ("client backend", "ClientRead", "Client"),
    ("client backend", "DataFileRead", "IO"),
    ("client backend", "WALWrite", "LWLock"),
    ("autovacuum worker", "VacuumDelay", "Timeout"),
    ("walsender", "WalSenderMain", "Activity"),
)
WAIT_EVENTS_LOCK = (
    ("client backend", "transactionid", "Lock"),
    ("client backend", "tuple", "Lock"),
    ("client backend", "relation", "Lock"),
)


class InsightsCounters(CounterState):
    """Cumulative Query Insights counters keyed by series; start near zero."""

    def __init__(self, rng: random.Random):
        super().__init__(rng, initial=0)
        self.values: dict[tuple, int] = {}

    def add(self, key: tuple, amount: float) -> int:
        self.values[key] = self.values.get(key, 0) + max(1, int(amount))
        return self.values[key]


def faults_from_chaos(chaos: dict | None) -> tuple[CommonFaults, dict[str, float]]:
    """Map resolved impact keys to shared and PostgreSQL-specific fault values."""
    common = CommonFaults(
        cpu_pct=fval(chaos, "cpu_utilization_high"),
        memory_pct=fval(chaos, "memory_utilization_high"),
        disk_pct=fval(chaos, "disk_utilization_high"),
        connections=fval(chaos, "connections_high"),
        replica_lag_s=fval(chaos, "replica_lag_high"),
        unavailable=fval(chaos, "instance_unavailable") >= 0.5,
    )
    engine = {
        "deadlocks_per_min": fval(chaos, "deadlocks_high"),
        "xid_pct": fval(chaos, "transaction_id_utilization_high"),
    }
    return common, engine


def _split(total: int, weights: list[float]) -> list[int]:
    """Partition one instance budget across databases (sum stays == total)."""
    wsum = sum(weights) or 1.0
    parts = [int(total * w / wsum) for w in weights]
    parts[0] += total - sum(parts)
    return parts


def postgres_metrics(
    inst: CloudSQLInstance,
    s: InstanceSample,
    engine: dict[str, float],
    databases: list[str],
    weights: list[float],
    replica_names: list[str],
    replica_lag_s: float,
    max_connections: int,
    rng: random.Random,
) -> list[dict]:
    """PostgreSQL engine series on the ``cloudsql_database`` resource."""
    primary = not inst.is_replica
    serving = s.up == 1
    deadlock_rate = engine["deadlocks_per_min"] if primary else 0.0
    xid_pct = engine["xid_pct"] if primary else 0.0
    out: list[dict] = []

    backends = min(max(len(databases), s.connections), int(max_connections * 0.99))
    per_db_backends = _split(backends, weights)
    tps = (rng.uniform(320, 620) if primary else rng.uniform(90, 180)) * (0.4 + s.load)
    if not serving:
        tps = rng.uniform(0.2, 1.0)
    tx_per_min = _split(max(len(databases), int(tps * 60)), weights)

    for db, nb, tx in zip(databases, per_db_backends, tx_per_min):
        out.append(gauge(PG + "num_backends", "1", int(nb), {"database": db}))
        active = max(1 if nb else 0, int(nb * rng.uniform(0.08, 0.16)))
        idle_tx = int(nb * rng.uniform(0.01, 0.03))
        if deadlock_rate > 0:
            active = int(nb * rng.uniform(0.35, 0.5))
            idle_tx = int(nb * rng.uniform(0.08, 0.15))
        idle = max(0, nb - active - idle_tx)
        for state, count in (("active", active), ("idle", idle), ("idle_in_transaction", idle_tx)):
            out.append(gauge(PG + "num_backends_by_state", "1", int(count), {"database": db, "state": state}))

        rollback_ratio = rng.uniform(0.002, 0.008)
        if deadlock_rate > 0:
            rollback_ratio = rng.uniform(0.06, 0.12)
        rollbacks = int(tx * rollback_ratio)
        out.append(delta(PG + "transaction_count", "1", max(0, tx - rollbacks),
                         {"database": db, "transaction_type": "commit"}))
        out.append(delta(PG + "transaction_count", "1", rollbacks,
                         {"database": db, "transaction_type": "rollback"}))

        share = tx / max(1, sum(tx_per_min))
        deadlocks = int(round(deadlock_rate * share)) if deadlock_rate > 0 else 0
        if deadlock_rate > 0 and db == databases[0]:
            deadlocks = max(1, deadlocks)
        out.append(delta(PG + "deadlock_count", "1", deadlocks, {"database": db}))

        out.append(delta(PG + "new_connection_count", "1",
                         max(0, int(nb * rng.uniform(0.3, 0.8))), {"database": db}))
        temp_files = rng.randint(0, 3) if primary else 0
        out.append(delta(PG + "temp_files_written_count", "1", temp_files, {"database": db}))
        out.append(delta(PG + "temp_bytes_written_count", "By",
                         temp_files * rng.randint(4, 64) * 1024 * 1024, {"database": db}))

        blocks = int(tx * rng.uniform(25, 60))
        hit_ratio = rng.uniform(0.985, 0.998) if s.memory < 0.9 else rng.uniform(0.90, 0.95)
        out.append(delta(PG + "blocks_read_count", "1", int(blocks * hit_ratio),
                         {"database": db, "source": "buffer_cache"}))
        out.append(delta(PG + "blocks_read_count", "1", int(blocks * (1 - hit_ratio)),
                         {"database": db, "source": "disk"}))

        statements = tx * rng.uniform(3.0, 5.0)
        for op, w in OPERATIONS:
            if not primary and op != "SELECT":
                count = 0
            else:
                count = int(statements * w * rng.uniform(0.9, 1.1))
            out.append(delta(PG + "statements_executed_count", "1", count,
                             {"database": db, "operation_type": op}))

    xid_util = rng.uniform(0.05, 0.12)
    if xid_pct > 0:
        xid_util = max(0.81, min(0.995, xid_pct / 100.0 if xid_pct <= 100 else 0.95))
    out.append(gauge(PG + "transaction_id_utilization", "10^2.%", round(xid_util, 4)))
    assigned = sum(tx_per_min) if primary else 0
    frozen = int(assigned * rng.uniform(0.6, 1.1)) if primary and xid_pct <= 0 else rng.randint(0, 50)
    out.append(delta(PG + "transaction_id_count", "1", int(assigned), {"action": "assigned"}))
    out.append(delta(PG + "transaction_id_count", "1", int(frozen), {"action": "frozen"}))

    ages = {"running": rng.randint(50, 4000), "prepared": 0,
            "replication_slot": rng.randint(100, 3000) if primary else 0, "replica": rng.randint(20, 800)}
    if xid_pct > 0:
        ages["running"] = int(xid_util * 2_000_000_000)
    for kind in OLDEST_TYPES:
        out.append(gauge(PG + "vacuum/oldest_transaction_age", "1", int(ages[kind]),
                         {"oldest_transaction_type": kind}))

    waits = [(*key, rng.randint(1, max(2, backends // 20))) for key in WAIT_EVENTS_HEALTHY]
    lock_waits = rng.randint(0, 2)
    if deadlock_rate > 0:
        lock_waits = int(max(10, deadlock_rate) * rng.uniform(0.8, 1.6))
    for i, key in enumerate(WAIT_EVENTS_LOCK):
        waits.append((*key, max(0, lock_waits // (i + 1)) if i == 0 or deadlock_rate > 0 else 0))
    for backend_type, event, event_type, count in waits:
        out.append(gauge(PG + "backends_in_wait", "1", int(count), {
            "backend_type": backend_type, "wait_event": event, "wait_event_type": event_type,
        }))

    out += connection_attempts(s, rng, base_per_min=sum(per_db_backends) * 0.6 + 30)

    if primary:
        base_lag = max(0.0, replica_lag_s)
        for name in replica_names:
            for i, lag_type in enumerate(LAG_TYPES):
                if base_lag >= 1:
                    lag_bytes = int(base_lag * rng.uniform(4, 6) * 1024 * 1024 * (0.4 + 0.2 * i))
                else:
                    lag_bytes = rng.randint(0, 64) * 1024 * (i + 1)
                out.append(gauge(PG + "replication/replica_byte_lag", "By", lag_bytes,
                                 {"replica_name": name, "replica_lag_type": lag_type}))
    return out


def insights_metrics(
    inst: CloudSQLInstance,
    s: InstanceSample,
    engine: dict[str, float],
    database: str,
    db_share: float,
    users: list[str],
    clients: list[str],
    counters: InsightsCounters,
    rng: random.Random,
) -> list[dict]:
    """Query Insights CUMULATIVE counters for one database (``cloudsql_instance_database``)."""
    primary = not inst.is_replica
    deadlock_rate = engine["deadlocks_per_min"] if primary else 0.0
    # Average active sessions -> execution microseconds per minute.
    aas = (s.cpu * inst.cores * rng.uniform(0.8, 1.1) + 0.2) * db_share
    if deadlock_rate > 0:
        aas *= 2.5
    out: list[dict] = []
    user_weights = [0.6, 0.25, 0.15][: len(users)] or [1.0]
    for user, uw in zip(users, user_weights):
        for client in clients:
            cw = uw / len(clients)
            attrs = {"user": user, "client_addr": client, "database": database}
            k = (inst.name, database, user, client)
            exec_us = aas * cw * 60_000_000
            out.append(cumulative(INSIGHTS + "execution_time", "us",
                                  counters.add(k + ("exec",), exec_us), counters.start_time_ns, attrs))
            io_share = rng.uniform(0.12, 0.22) if s.memory < 0.9 else rng.uniform(0.35, 0.5)
            for io_type, w in (("read", 0.7), ("write", 0.3)):
                out.append(cumulative(INSIGHTS + "io_time", "us",
                                      counters.add(k + ("io", io_type), exec_us * io_share * w),
                                      counters.start_time_ns, {**attrs, "io_type": io_type}))
            lock_share = rng.uniform(0.01, 0.03) if deadlock_rate <= 0 else rng.uniform(0.35, 0.55)
            for lock_type, w in zip(LOCK_TYPES, (0.5, 0.45, 0.05)):
                out.append(cumulative(INSIGHTS + "lock_time", "us",
                                      counters.add(k + ("lock", lock_type), exec_us * lock_share * w),
                                      counters.start_time_ns, {**attrs, "lock_type": lock_type}))
            rows = exec_us / 1000 * rng.uniform(1.5, 4.0)
            out.append(cumulative(INSIGHTS + "row_count", "1",
                                  counters.add(k + ("rows",), rows), counters.start_time_ns, attrs))
            blk = rows * rng.uniform(2, 5)
            hit = rng.uniform(0.97, 0.995) if s.memory < 0.9 else rng.uniform(0.85, 0.92)
            for access, frac in (("hit", hit), ("miss", 1 - hit)):
                out.append(cumulative(INSIGHTS + "shared_blk_access_count", "1",
                                      counters.add(k + ("blk", access), blk * frac),
                                      counters.start_time_ns, {**attrs, "access_type": access}))
    return out


def generate_metrics(
    instances: list[CloudSQLInstance],
    databases: list[str],
    users: list[str],
    counters: InsightsCounters,
    rng: random.Random,
    chaos: dict | None,
    max_connections: int,
    load: float | None = None,
) -> list[tuple[CloudSQLInstance, str | None, list[dict]]]:
    """One minute of metrics: (instance, insights database or None, metrics)."""
    common, engine = faults_from_chaos(chaos)
    load = business_load() if load is None else load
    weights = [0.55, 0.3, 0.15, 0.1, 0.05][: len(databases)] or [1.0]
    replicas = [i.name for i in instances if i.is_replica]
    samples = {i.name: sample_instance(i, common, load, rng) for i in instances}
    replica_lag = max((samples[n].replica_lag_s for n in replicas), default=0.0)
    batches: list[tuple[CloudSQLInstance, str | None, list[dict]]] = []
    for inst in instances:
        s = samples[inst.name]
        metrics = common_instance_metrics(inst, s, rng)
        metrics += postgres_metrics(inst, s, engine, databases, weights, replicas,
                                    replica_lag if common.replica_lag_s > 0 else 0.0,
                                    max_connections, rng)
        batches.append((inst, None, metrics))
        clients = ["10.20.0.14", "10.20.0.27"] if not inst.is_replica else ["10.20.0.41"]
        wsum = sum(weights)
        for db, w in zip(databases, weights):
            batches.append((inst, db, insights_metrics(
                inst, s, engine, db, w / wsum, users, clients, counters, rng)))
    return batches


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Emit Cloud SQL for PostgreSQL metrics every 60 s until stop_event is set."""
    rng = random.Random()
    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {}) or {}
    else:
        ns = NAMESPACE
        cfg = {}
    channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    identity = ScenarioIdentity.from_scenario(scenario_data, ns)
    cloud = {**default_cloud(), **(cfg.get("cloud") or {})}
    project_id = ScenarioIdentity(ns).gcp_project(cfg.get("project_id"))
    max_connections = int(cfg.get("max_connections") or 1000)
    databases = identity.databases(["orders", "inventory", "billing"])
    users = [str(u) for u in (cfg.get("insights_users") or ["app_rw", "reporting_ro", "etl_batch"])]
    instances = build_instances(
        identity, cfg, engine_role="postgresql", default_tier="db-custom-4-16384",
        disk_gb=500, project_id=project_id, cloud=cloud, rng=rng,
    )
    service_role = cfg.get("service_name") or "cloudsql-postgresql"
    dataset = otel_dataset(DATASET)
    resources: dict[tuple[str, str | None], dict] = {}
    for inst in instances:
        resources[(inst.name, None)] = instance_resource(
            identity, inst, dataset=dataset, service_role=service_role, cloud=cloud)
        for db in databases:
            resources[(inst.name, db)] = insights_resource(
                identity, inst, db, dataset=dataset, service_role=service_role, cloud=cloud)
    counters = InsightsCounters(rng)
    scope = receiver_scope()
    logger.info("Cloud SQL PostgreSQL generator started (namespace=%s, instances=%s)",
                ns, [i.database_id for i in instances])

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=channel_registry,
            generator_id=GENERATOR_ID,
            chaos_mode=chaos_mode,
            impact_keys=SPEC.impact_keys,
        )
        for inst, db, metrics in generate_metrics(
            instances, databases, users, counters, rng, active_chaos, max_connections,
        ):
            client.send_metrics_scoped(resources[(inst.name, db)], scope, metrics)

    run_tick_loop(stop_event, _emit_tick, interval=SCRAPE_INTERVAL_S)
    logger.info("Cloud SQL PostgreSQL generator stopped.")


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "60"))
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    client = OTLPClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
