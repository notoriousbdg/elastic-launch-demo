#!/usr/bin/env python3
"""Cloud SQL for MySQL generator: Cloud Monitoring metrics via OTLP.

Emulates the OpenTelemetry ``googlecloudmonitoringreceiver`` pulling
``cloudsql.googleapis.com/database/...`` time series for one primary and one
read replica, routed to ``metrics-gcp.cloudsql_mysql.otel-default`` (the
stream the gcp_cloudsql_mysql_otel Overview and Instance Detail dashboards
read). See ``generator_engine/gcp_cloudsql_common.py`` for the receiver's
GAUGE / DELTA / CUMULATIVE mapping.

Resource attributes: ``gcp.resource_type`` (``cloudsql_database``),
``database_id`` (``<project>:<instance>``), ``project_id``, ``region``.
Datapoint attributes read by panels: ``state`` (instance_state and the
replication thread states) and ``thread_kind``. Every panel filters on
``mysql/queries`` as the "this is a MySQL instance" marker, so both
instances emit it every minute (the Overview hexagon map additionally
needs >= 50 queries in range). One panel spells the marker
``cloudsql.googleapis.`com/database/mysql/queries``` - the same field.

Usage (standalone):
    python3 -m generators.gcp_cloudsql_mysql_otel.generator
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/gcp_cloudsql_mysql_otel@0.1.0"

import logging
import os
import random
import signal
import threading

from app.config import NAMESPACE
from app.telemetry import OTLPClient
from generator_engine.db_common import business_load, otel_dataset, resolve_active_chaos
from generator_engine.gcp_cloudsql_common import (
    M,
    SCRAPE_INTERVAL_S,
    CloudSQLInstance,
    CommonFaults,
    InstanceSample,
    build_instances,
    common_instance_metrics,
    default_cloud,
    delta,
    fval,
    gauge,
    instance_resource,
    receiver_scope,
    sample_instance,
)
from generator_engine.identity import ScenarioIdentity
from generator_engine.tick import run_tick_loop
from generators.gcp_cloudsql_mysql_otel import SPEC

logger = logging.getLogger("gcp-cloudsql-mysql-generator")

GENERATOR_ID = "gcp_cloudsql_mysql_otel"
DATASET = "gcp.cloudsql_mysql"  # -> metrics-gcp.cloudsql_mysql.otel-default

# Grounding: every metric the two package dashboards read.
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
    "cloudsql.googleapis.com/database/replication/replica_lag",
    "cloudsql.googleapis.com/database/available_for_failover",
    "cloudsql.googleapis.com/database/replication/network_lag",
    "cloudsql.googleapis.com/database/mysql/queries",
    "cloudsql.googleapis.com/database/mysql/slow_queries_count",
    "cloudsql.googleapis.com/database/mysql/max_connections",
    "cloudsql.googleapis.com/database/mysql/threads",
    "cloudsql.googleapis.com/database/mysql/connections_count",
    "cloudsql.googleapis.com/database/mysql/aborted_connects_count",
    "cloudsql.googleapis.com/database/mysql/aborted_clients_count",
    "cloudsql.googleapis.com/database/mysql/tmp_disk_tables_created_count",
    "cloudsql.googleapis.com/database/mysql/innodb_pages_read",
    "cloudsql.googleapis.com/database/mysql/innodb_pages_written",
    "cloudsql.googleapis.com/database/mysql/innodb/buffer_pool_read_requests_count",
    "cloudsql.googleapis.com/database/mysql/innodb/buffer_pool_reads_count",
    "cloudsql.googleapis.com/database/mysql/innodb/row_lock_waits_count",
    "cloudsql.googleapis.com/database/mysql/innodb/lock_timeout_count",
    "cloudsql.googleapis.com/database/mysql/innodb/deadlocks_count",
    "cloudsql.googleapis.com/database/mysql/replication/slave_io_running_state",
    "cloudsql.googleapis.com/database/mysql/replication/slave_sql_running_state",
    "cloudsql.googleapis.com/database/mysql/replication/seconds_behind_master",
    "cloudsql.googleapis.com/database/mysql/replication/last_io_errno",
    "cloudsql.googleapis.com/database/mysql/replication/last_sql_errno",
)

MY = M + "mysql/"
THREAD_KINDS = ("THREADS_CONNECTED", "THREADS_RUNNING", "THREADS_CACHED")


def faults_from_chaos(chaos: dict | None) -> tuple[CommonFaults, dict[str, float]]:
    """Map resolved impact keys to shared and MySQL-specific fault values."""
    common = CommonFaults(
        cpu_pct=fval(chaos, "cpu_utilization_high"),
        memory_pct=fval(chaos, "memory_utilization_high"),
        disk_pct=fval(chaos, "disk_utilization_high"),
        connections=fval(chaos, "connections_high"),
        replica_lag_s=fval(chaos, "replica_lag_high"),
        unavailable=fval(chaos, "instance_unavailable") >= 0.5,
    )
    engine = {
        "slow_pct": fval(chaos, "slow_queries_high"),
        "lock_waits_per_s": fval(chaos, "innodb_lock_waits_high"),
    }
    return common, engine


def mysql_metrics(
    inst: CloudSQLInstance,
    s: InstanceSample,
    engine: dict[str, float],
    max_connections: int,
    rng: random.Random,
) -> list[dict]:
    """MySQL engine series for one instance (per-minute deltas)."""
    primary = not inst.is_replica
    serving = s.up == 1
    qps = (rng.uniform(700, 1300) if primary else rng.uniform(280, 520)) * (0.4 + s.load)
    if not serving:
        qps = rng.uniform(1.0, 3.0)
    queries = max(60, int(qps * 60))

    slow_pct = engine["slow_pct"] if primary else 0.0
    if slow_pct > 0:
        slow = int(queries * min(slow_pct, 100.0) / 100.0)
        tmp_disk = rng.randint(300, 900)
    else:
        slow = rng.randint(0, max(1, queries // 2500))
        tmp_disk = rng.randint(2, 20)

    lock_rate = engine["lock_waits_per_s"] if primary else 0.0
    if lock_rate > 0:
        row_lock_waits = int(lock_rate * 60 * rng.uniform(0.95, 1.05))
        lock_timeouts = max(1, int(row_lock_waits * rng.uniform(0.05, 0.12)))
        deadlocks = max(1, int(lock_rate * rng.uniform(0.2, 0.4)))
    else:
        row_lock_waits = rng.randint(0, 25) if primary else 0
        lock_timeouts = 0
        deadlocks = 0

    read_requests = int(queries * rng.uniform(40, 70))
    hit = rng.uniform(0.996, 0.9995)
    if slow_pct > 0 or s.memory > 0.9:
        hit = rng.uniform(0.93, 0.96)
    pool_reads = int(read_requests * (1 - hit))

    attempts = max(1, int((rng.uniform(150, 400) if primary else rng.uniform(60, 140)) * (0.4 + s.load)))
    aborted_connects = rng.randint(0, 2)
    aborted_clients = rng.randint(0, 3)
    if s.connections >= int(max_connections * 0.9):
        aborted_connects = int(attempts * rng.uniform(0.15, 0.3))
        aborted_clients = rng.randint(20, 60)
    if not serving:
        aborted_connects = max(aborted_connects, int(attempts * rng.uniform(0.6, 0.9)))

    connected = max(1, s.connections)
    running = max(1, int(connected * rng.uniform(0.04, 0.12)))
    if lock_rate > 0 or slow_pct > 0:
        running = max(running, int(connected * rng.uniform(0.3, 0.5)))
    cached = rng.randint(8, 32)

    out = [
        delta(MY + "queries", "1", queries),
        delta(MY + "slow_queries_count", "1", slow),
        gauge(MY + "max_connections", "1", int(max_connections)),
        delta(MY + "connections_count", "1", attempts),
        delta(MY + "aborted_connects_count", "1", aborted_connects),
        delta(MY + "aborted_clients_count", "1", aborted_clients),
        delta(MY + "tmp_disk_tables_created_count", "1", tmp_disk),
        delta(MY + "innodb_pages_read", "1", pool_reads + rng.randint(50, 400)),
        delta(MY + "innodb_pages_written", "1", int(queries * rng.uniform(0.05, 0.12)) if primary else rng.randint(200, 900)),
        delta(MY + "innodb/buffer_pool_read_requests_count", "1", read_requests),
        delta(MY + "innodb/buffer_pool_reads_count", "1", pool_reads),
        delta(MY + "innodb/row_lock_waits_count", "1", row_lock_waits),
        delta(MY + "innodb/lock_timeout_count", "1", lock_timeouts),
        delta(MY + "innodb/deadlocks_count", "1", deadlocks),
    ]
    for kind, value in zip(THREAD_KINDS, (connected, running, cached)):
        out.append(gauge(MY + "threads", "1", int(value), {"thread_kind": kind}))

    if inst.is_replica:
        lagging = s.replica_lag_s >= 30
        io_state = "Connecting" if s.replica_lag_s >= 900 else "Yes"
        for state in ("Yes", "No", "Connecting"):
            out.append(gauge(MY + "replication/slave_io_running_state", "1", state == io_state, {"state": state}))
        for state in ("Yes", "No"):
            out.append(gauge(MY + "replication/slave_sql_running_state", "1", state == "Yes", {"state": state}))
        out.append(gauge(MY + "replication/seconds_behind_master", "s", int(round(s.replica_lag_s))))
        out.append(gauge(MY + "replication/last_io_errno", "1", 2003 if io_state == "Connecting" else 0))
        out.append(gauge(MY + "replication/last_sql_errno", "1", 1205 if lagging else 0))
    return out


def generate_metrics(
    instances: list[CloudSQLInstance],
    rng: random.Random,
    chaos: dict | None,
    max_connections: int,
    load: float | None = None,
) -> list[tuple[CloudSQLInstance, list[dict]]]:
    """One minute of metrics per instance."""
    common, engine = faults_from_chaos(chaos)
    load = business_load() if load is None else load
    batches = []
    for inst in instances:
        s = sample_instance(inst, common, load, rng)
        metrics = common_instance_metrics(inst, s, rng, include_network_lag=True)
        metrics += mysql_metrics(inst, s, engine, max_connections, rng)
        batches.append((inst, metrics))
    return batches


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Emit Cloud SQL for MySQL metrics every 60 s until stop_event is set."""
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
    max_connections = int(cfg.get("max_connections") or 4000)
    instances = build_instances(
        identity, cfg, engine_role="mysql", default_tier="db-custom-4-16384",
        disk_gb=500, project_id=project_id, cloud=cloud, rng=rng,
    )
    service_role = cfg.get("service_name") or "cloudsql-mysql"
    resources = {
        inst.name: instance_resource(
            identity, inst, dataset=otel_dataset(DATASET),
            service_role=service_role, cloud=cloud,
        )
        for inst in instances
    }
    scope = receiver_scope()
    logger.info("Cloud SQL MySQL generator started (namespace=%s, instances=%s)",
                ns, [i.database_id for i in instances])

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=channel_registry,
            generator_id=GENERATOR_ID,
            chaos_mode=chaos_mode,
            impact_keys=SPEC.impact_keys,
        )
        for inst, metrics in generate_metrics(instances, rng, active_chaos, max_connections):
            client.send_metrics_scoped(resources[inst.name], scope, metrics)

    run_tick_loop(stop_event, _emit_tick, interval=SCRAPE_INTERVAL_S)
    logger.info("Cloud SQL MySQL generator stopped.")


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
