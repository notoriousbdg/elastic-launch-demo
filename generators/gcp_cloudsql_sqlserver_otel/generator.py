#!/usr/bin/env python3
"""Cloud SQL for SQL Server generator: Cloud Monitoring metrics via OTLP.

Emulates the OpenTelemetry ``googlecloudmonitoringreceiver`` pulling
``cloudsql.googleapis.com/database/...`` time series for one HA primary and
one read replica, routed to ``metrics-gcp.cloudsql_sqlserver.otel-default``
(read by the gcp_cloudsql_sqlserver_otel Overview and Instance Detail
dashboards). See ``generator_engine/gcp_cloudsql_common.py`` for the
receiver's GAUGE / DELTA / CUMULATIVE mapping.

Resource attributes: ``gcp.resource_type`` (``cloudsql_database``),
``database_id`` (``<project>:<instance>``), ``project_id``, ``region``.
Every Overview panel keys SQL Server instances on
``sqlserver/memory/page_life_expectancy`` (the join marker), so both
instances emit it every minute.

Datapoint attributes read by panels: attributes.state,
attributes.login_status, attributes.database, attributes.locked_resource.

Usage (standalone):
    python3 -m generators.gcp_cloudsql_sqlserver_otel.generator
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/gcp_cloudsql_sqlserver_otel@0.1.0"

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
    connection_attempts,
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
from generators.gcp_cloudsql_sqlserver_otel import SPEC

logger = logging.getLogger("gcp-cloudsql-sqlserver-generator")

GENERATOR_ID = "gcp_cloudsql_sqlserver_otel"
DATASET = "gcp.cloudsql_sqlserver"  # -> metrics-gcp.cloudsql_sqlserver.otel-default

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
    "cloudsql.googleapis.com/database/auto_failover_request_count",
    "cloudsql.googleapis.com/database/sqlserver/transactions/batch_request_count",
    "cloudsql.googleapis.com/database/sqlserver/transactions/transaction_count",
    "cloudsql.googleapis.com/database/sqlserver/transactions/deadlock_count",
    "cloudsql.googleapis.com/database/sqlserver/transactions/lock_wait_count",
    "cloudsql.googleapis.com/database/sqlserver/transactions/lock_wait_time",
    "cloudsql.googleapis.com/database/sqlserver/connections/processes_blocked",
    "cloudsql.googleapis.com/database/sqlserver/connections/login_attempt_count",
    "cloudsql.googleapis.com/database/sqlserver/memory/buffer_cache_hit_ratio",
    "cloudsql.googleapis.com/database/sqlserver/memory/page_life_expectancy",
    "cloudsql.googleapis.com/database/sqlserver/memory/memory_grants_pending",
    "cloudsql.googleapis.com/database/sqlserver/schedulers/runnable_tasks",
)

SS = M + "sqlserver/"
# locked_resource values reported by sys.dm_os_performance_counters "Locks" object.
LOCKED_RESOURCES = (("Key", 0.45), ("Page", 0.25), ("Object", 0.15), ("RID", 0.1), ("Database", 0.05))


def faults_from_chaos(chaos: dict | None) -> tuple[CommonFaults, dict[str, float]]:
    """Map resolved impact keys to shared and SQL Server-specific fault values."""
    common = CommonFaults(
        cpu_pct=fval(chaos, "cpu_utilization_high"),
        memory_pct=fval(chaos, "memory_utilization_high"),
        disk_pct=fval(chaos, "disk_utilization_high"),
        connections=fval(chaos, "connections_high"),
        replica_lag_s=fval(chaos, "replica_lag_high"),
        unavailable=fval(chaos, "instance_unavailable") >= 0.5,
    )
    engine = {
        "blocked": fval(chaos, "blocked_processes_high"),
        "deadlocks_per_min": fval(chaos, "deadlocks_high"),
    }
    return common, engine


def sqlserver_metrics(
    inst: CloudSQLInstance,
    s: InstanceSample,
    engine: dict[str, float],
    databases: list[str],
    rng: random.Random,
) -> list[dict]:
    """SQL Server engine series for one instance (per-minute deltas)."""
    primary = not inst.is_replica
    serving = s.up == 1
    blocked_target = engine["blocked"] if primary else 0.0
    deadlock_rate = engine["deadlocks_per_min"] if primary else 0.0
    out: list[dict] = []

    batch_rps = (rng.uniform(900, 1800) if primary else rng.uniform(250, 500)) * (0.4 + s.load)
    if not serving:
        batch_rps = rng.uniform(0.5, 2.0)
    out.append(delta(SS + "transactions/batch_request_count", "1", max(1, int(batch_rps * 60))))

    tps = batch_rps * rng.uniform(0.25, 0.4)
    weights = [0.55, 0.3, 0.15, 0.1, 0.05][: len(databases)] or [1.0]
    wsum = sum(weights)
    for db, w in zip(databases, weights):
        out.append(delta(SS + "transactions/transaction_count", "1",
                         max(0, int(tps * 60 * w / wsum)), {"database": db}))

    blocked = rng.choice((0, 0, 0, 1)) if primary else 0
    if blocked_target > 0:
        blocked = int(max(1, blocked_target) * rng.uniform(0.9, 1.1))
    out.append(gauge(SS + "connections/processes_blocked", "1", int(blocked)))

    lock_waits_total = rng.randint(5, 40) if primary else rng.randint(0, 5)
    avg_wait_ms = rng.uniform(4, 25)
    if blocked_target > 0:
        lock_waits_total = int(blocked_target * rng.uniform(25, 40))
        avg_wait_ms = rng.uniform(2_500, 8_000)
    elif deadlock_rate > 0:
        lock_waits_total = int(deadlock_rate * rng.uniform(10, 20))
        avg_wait_ms = rng.uniform(400, 1_500)
    for resource, w in LOCKED_RESOURCES:
        count = int(lock_waits_total * w)
        out.append(delta(SS + "transactions/lock_wait_count", "1", count, {"locked_resource": resource}))
        out.append(delta(SS + "transactions/lock_wait_time", "ms",
                         int(count * avg_wait_ms * rng.uniform(0.8, 1.2)), {"locked_resource": resource}))
        deadlocks = 0
        if deadlock_rate > 0:
            deadlocks = int(round(deadlock_rate * w))
            if resource == "Key":
                deadlocks = max(1, deadlocks)
        out.append(delta(SS + "transactions/deadlock_count", "1", deadlocks, {"locked_resource": resource}))

    logins = max(1, int((rng.uniform(80, 200) if primary else rng.uniform(20, 60)) * (0.4 + s.load)))
    out.append(delta(SS + "connections/login_attempt_count", "1", logins))

    pressured = s.memory >= 0.9
    hit = rng.uniform(99.2, 99.9) if not pressured else rng.uniform(88.0, 94.0)
    ple = rng.uniform(2_400, 6_000) if not pressured else rng.uniform(40, 180)
    grants = 0 if not pressured else rng.randint(4, 20)
    if blocked_target > 0:
        grants = max(grants, rng.randint(2, 8))
    out.append(gauge(SS + "memory/buffer_cache_hit_ratio", "%", round(hit, 2)))
    out.append(gauge(SS + "memory/page_life_expectancy", "s", int(ple)))
    out.append(gauge(SS + "memory/memory_grants_pending", "1", int(grants)))

    schedulers = int(inst.cores)
    for sid in range(schedulers):
        runnable = rng.choice((0, 0, 0, 1))
        if s.cpu >= 0.85 or blocked_target > 0:
            runnable = rng.randint(4, 12)
        out.append(gauge(SS + "schedulers/runnable_tasks", "1", runnable, {"scheduler_id": str(sid)}))

    if primary:
        out.append(delta(M + "auto_failover_request_count", "1", 0 if serving else 1))

    out += connection_attempts(s, rng, base_per_min=logins * 0.9)
    return out


def generate_metrics(
    instances: list[CloudSQLInstance],
    databases: list[str],
    rng: random.Random,
    chaos: dict | None,
    load: float | None = None,
) -> list[tuple[CloudSQLInstance, list[dict]]]:
    """One minute of metrics per instance."""
    common, engine = faults_from_chaos(chaos)
    load = business_load() if load is None else load
    batches = []
    for inst in instances:
        s = sample_instance(inst, common, load, rng)
        metrics = common_instance_metrics(inst, s, rng)
        metrics += sqlserver_metrics(inst, s, engine, databases, rng)
        batches.append((inst, metrics))
    return batches


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Emit Cloud SQL for SQL Server metrics every 60 s until stop_event is set."""
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
    databases = identity.databases(["orders", "inventory", "billing"])
    instances = build_instances(
        identity, cfg, engine_role="sqlserver", default_tier="db-custom-8-32768",
        disk_gb=750, project_id=project_id, cloud=cloud, rng=rng,
    )
    service_role = cfg.get("service_name") or "cloudsql-sqlserver"
    resources = {
        inst.name: instance_resource(
            identity, inst, dataset=otel_dataset(DATASET),
            service_role=service_role, cloud=cloud,
        )
        for inst in instances
    }
    scope = receiver_scope()
    logger.info("Cloud SQL SQL Server generator started (namespace=%s, instances=%s)",
                ns, [i.database_id for i in instances])

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=channel_registry,
            generator_id=GENERATOR_ID,
            chaos_mode=chaos_mode,
            impact_keys=SPEC.impact_keys,
        )
        for inst, metrics in generate_metrics(instances, databases, rng, active_chaos):
            client.send_metrics_scoped(resources[inst.name], scope, metrics)

    run_tick_loop(stop_event, _emit_tick, interval=SCRAPE_INTERVAL_S)
    logger.info("Cloud SQL SQL Server generator stopped.")


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
