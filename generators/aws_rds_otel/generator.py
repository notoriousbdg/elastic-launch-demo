#!/usr/bin/env python3
"""RDS Generator — sends AWS RDS CloudWatch metrics via OTLP.

Emits data matching the aws_rds_otel content package
(discovery.datasets: aws.rds.otel), which populates the bundled
RDS Overview and Drilldown dashboards.

Signal types:
  - Metrics: CloudWatch RDS metrics in the AWS/RDS namespace

Metric names use the CloudWatch path format:
  amazonaws.com/AWS/RDS/<MetricName>
Elasticsearch stores each as metrics.amazonaws.com/AWS/RDS/<MetricName>
via the passthrough field mapping.

Attribute keys verified against:
  - elastic/integrations/packages/aws_rds_otel/kibana/dashboard/*.json

Key attributes: DBInstanceIdentifier, Engine, stat, cloud.region, cloud.account.id.
DBInstanceIdentifier and Engine are CloudWatch dimensions, so they are data-point
attributes (``attributes.DBInstanceIdentifier``, which the Template Library RDS
investigation workflow filters on) as well as resource attributes.

Usage (standalone):
    python3 -m generators.aws_rds_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/aws_rds_otel@0.12.0"

import logging
import os
import random
import signal
import threading

from app.telemetry import OTLPClient
from app.config import NAMESPACE
from generator_engine.db_common import (
    impact_value,
    CounterState,
    build_cloudwatch_metric,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

# ── Grounding constants — do not change names ─────────────────────────────────
# Metric names use the bare path; ES stores them under metrics.<name>.

METRIC_NAMES = (
    "amazonaws.com/AWS/RDS/CPUUtilization",
    "amazonaws.com/AWS/RDS/DatabaseConnections",
    "amazonaws.com/AWS/RDS/FreeStorageSpace",
    "amazonaws.com/AWS/RDS/FreeableMemory",
    "amazonaws.com/AWS/RDS/ReadIOPS",
    "amazonaws.com/AWS/RDS/WriteIOPS",
    "amazonaws.com/AWS/RDS/ReadLatency",
    "amazonaws.com/AWS/RDS/WriteLatency",
    "amazonaws.com/AWS/RDS/NetworkReceiveThroughput",
    "amazonaws.com/AWS/RDS/NetworkTransmitThroughput",
    "amazonaws.com/AWS/RDS/ReplicaLag",
    "amazonaws.com/AWS/RDS/SwapUsage",
    "amazonaws.com/AWS/RDS/BinLogDiskUsage",
    "amazonaws.com/AWS/RDS/TransactionLogsGeneration",
    "amazonaws.com/AWS/RDS/MaximumUsedTransactionIDs",
    "amazonaws.com/AWS/RDS/ReplicationSlotDiskUsage",
    "amazonaws.com/AWS/RDS/BurstBalance",
    "amazonaws.com/AWS/RDS/CheckpointLag",
    "amazonaws.com/AWS/RDS/DiskQueueDepth",
    "amazonaws.com/AWS/RDS/FailedSQLServerAgentJobsCount",
    "amazonaws.com/AWS/RDS/TempDbAvailableDataSpace",
    "amazonaws.com/AWS/RDS/TempDbAvailableLogSpace",
    "amazonaws.com/AWS/RDS/TempDbDataFileUsage",
    "amazonaws.com/AWS/RDS/TempDbLogFileUsage",
)

RDS_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscloudwatchreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("rds-generator")

BATCH_INTERVAL_MIN = 30
BATCH_INTERVAL_MAX = 60


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_rds",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


class RDSMetricState(CounterState):
    """Cumulative counter state for RDS metrics."""

    def __init__(self, rng: random.Random, total_storage: int):
        super().__init__(rng)
        self.transaction_logs_generated = rng.randint(0, 1_000_000_000)
        self.binlog_disk = rng.randint(0, 500_000_000)
        # Track free storage — starts near 40% free
        self.free_storage = int(total_storage * rng.uniform(0.3, 0.5))
        self.total_storage = total_storage

    def tick(self, rng: random.Random, storage_drain_rate: int = 0):
        load = business_load()
        self.transaction_logs_generated += int(rng.randint(1_000, 100_000) * load)
        self.binlog_disk += rng.randint(0, 10_000)
        if storage_drain_rate > 0:
            # Drain and pin below the 10 GiB free-storage alert floor.
            self.free_storage = max(0, self.free_storage - int(storage_drain_rate))
            self.free_storage = min(self.free_storage, 5_000_000_000)
        else:
            self.free_storage = max(
                int(self.total_storage * 0.05),
                self.free_storage - rng.randint(0, 1_000_000),
            )


def _emit_stats(
    metrics: list[dict],
    metric: str,
    unit: str,
    stats: dict[str, int | float],
    dims: dict,
) -> None:
    """Emit one AWS/RDS CloudWatch gauge per ``{stat: value}`` entry.

    ``dims`` carries the CloudWatch dimensions (DBInstanceIdentifier, Engine) plus
    cloud.region / cloud.account.id as data-point attributes.
    """
    for stat, value in stats.items():
        metrics.append(build_cloudwatch_metric("RDS", metric, value, dims, stat=stat, unit=unit))


def _generate_metrics(
    state: RDSMetricState,
    region: str,
    account_id: str,
    rng: random.Random,
    active_chaos: dict | None = None,
    *,
    instance_id: str | None = None,
    engine: str | None = None,
    connections_baseline: tuple[int, int] = (10, 80),
) -> list[dict]:
    # Impact keys must match SPEC / GT channel names (not legacy aliases).
    c = active_chaos or {}
    cpu_high = float(c.get("cpu_utilization_high", 0.0) or 0.0)
    replica_lag_high = float(c.get("replica_lag_high", 0.0) or 0.0)
    free_storage_low = c.get("free_storage_space_low")
    burst_low = c.get("burst_balance_low")
    checkpoint_high = float(c.get("checkpoint_lag_high", 0.0) or 0.0)
    connections_high = float(c.get("database_connections_high", 0.0) or 0.0)
    freeable_low = c.get("freeable_memory_low")
    swap_high = float(c.get("swap_usage_high", 0.0) or 0.0)
    read_lat_high = float(c.get("read_latency_high", 0.0) or 0.0)
    write_lat_high = float(c.get("write_latency_high", 0.0) or 0.0)
    disk_queue_high = float(c.get("disk_queue_depth_high", 0.0) or 0.0)

    # Free-storage chaos sets an absolute FreeStorageSpace level (bytes).
    storage_drain = 0
    if free_storage_low is not None:
        # Force state down on next tick via direct override after tick.
        storage_drain = 0
    state.tick(rng, storage_drain_rate=storage_drain)
    if free_storage_low is not None:
        # Alert: MIN < 10737418240 — GT often sets the threshold exactly.
        target = float(free_storage_low)
        state.free_storage = int(min(target, 10737418240 - 1))

    metrics: list[dict] = []
    load = business_load()
    dims = {"cloud.region": region, "cloud.account.id": account_id}
    if instance_id:
        dims["DBInstanceIdentifier"] = instance_id
    if engine:
        dims["Engine"] = engine

    # ── CPU / connections (Average for alerts, Maximum for peak Top-10 panels) ──
    if cpu_high > 0:
        # Alert: AVG > 80. GT may pass scaled values (e.g. 600) — clamp to %.
        cpu = min(100.0, max(81.0, float(cpu_high) if cpu_high <= 100 else 95.0))
    else:
        cpu = rng.uniform(5, 40) * load
    cpu_max = min(100.0, cpu * rng.uniform(1.05, 1.25))
    _emit_stats(metrics, "CPUUtilization", "%",
                {"Average": round(cpu, 2), "Maximum": round(cpu_max, 2)}, dims)

    if connections_high > 0:
        # Exhaustion plateaus at the ceiling: Average and Maximum hold the value the
        # fault sets (alert: AVG > 100).
        conns = conns_max = int(connections_high)
    else:
        low, high = connections_baseline
        conns = int(rng.randint(int(low), int(high)) * load)
        conns_max = int(conns * rng.uniform(1.05, 1.3))
    _emit_stats(metrics, "DatabaseConnections", "{connection}",
                {"Average": conns, "Maximum": conns_max}, dims)

    # ── Storage / memory ──
    _emit_stats(metrics, "FreeStorageSpace", "By", {"Average": state.free_storage}, dims)
    if freeable_low is not None:
        # Alert: MIN < 268435456 — clamp strictly under when key == threshold.
        freeable_mem = int(min(float(freeable_low), 268435456 - 1))
    else:
        freeable_mem = int(rng.randint(512_000_000, 2_000_000_000))
    _emit_stats(metrics, "FreeableMemory", "By", {"Average": freeable_mem}, dims)
    swap = int(swap_high) if swap_high > 0 else 0
    _emit_stats(metrics, "SwapUsage", "By", {"Average": swap}, dims)

    # ── IOPS and latency ──
    read_iops = int(rng.randint(10, 500) * load)
    write_iops = int(rng.randint(10, 200) * load)
    _emit_stats(metrics, "ReadIOPS", "{ops}/s",
                {"Average": read_iops, "Maximum": int(read_iops * rng.uniform(1.05, 1.4))}, dims)
    _emit_stats(metrics, "WriteIOPS", "{ops}/s",
                {"Average": write_iops, "Maximum": int(write_iops * rng.uniform(1.05, 1.4))}, dims)

    read_lat = rng.uniform(0.001, 0.010)
    write_lat = rng.uniform(0.001, 0.005)
    # A connection flood keeps every backend busy: write latency and queue depth rise
    # about 3x, still under their own alerts, while CPU and storage stay flat.
    io_pressure = 3.0 if connections_high > 0 else 1.0
    if write_lat_high <= 0:
        write_lat = min(0.049 / 1.2, write_lat * io_pressure)
    # Alert thresholds are seconds (> 0.05). GT may pass large numbers (e.g. 75).
    if read_lat_high > 0:
        read_lat = float(read_lat_high) if read_lat_high >= 0.05 else 0.051
    if write_lat_high > 0:
        write_lat = float(write_lat_high) if write_lat_high >= 0.05 else 0.051
    # Alerts filter Maximum for Read/Write latency.
    _emit_stats(metrics, "ReadLatency", "s",
                {"Average": round(read_lat, 4),
                 "Maximum": round(read_lat * rng.uniform(1.0, 1.2), 4)}, dims)
    _emit_stats(metrics, "WriteLatency", "s",
                {"Average": round(write_lat, 4),
                 "Maximum": round(write_lat * rng.uniform(1.0, 1.2), 4)}, dims)

    # ── Network throughput ──
    net_recv = int(rng.randint(100_000, 10_000_000) * load)
    net_xmit = int(rng.randint(100_000, 5_000_000) * load)
    _emit_stats(metrics, "NetworkReceiveThroughput", "By/s", {"Average": net_recv}, dims)
    _emit_stats(metrics, "NetworkTransmitThroughput", "By/s", {"Average": net_xmit}, dims)

    # ── Replication (alert uses Maximum) ──
    replica_lag = round(max(0.0, replica_lag_high), 2)
    _emit_stats(metrics, "ReplicaLag", "s",
                {"Average": replica_lag, "Maximum": replica_lag}, dims)
    replication_slot_disk = rng.randint(0, 100_000_000)
    _emit_stats(metrics, "ReplicationSlotDiskUsage", "By",
                {"Average": replication_slot_disk}, dims)

    # ── Log / transaction metrics ──
    _emit_stats(metrics, "TransactionLogsGeneration", "By",
                {"Average": state.transaction_logs_generated}, dims)
    _emit_stats(metrics, "BinLogDiskUsage", "By", {"Average": state.binlog_disk}, dims)
    max_txn_ids = rng.randint(0, 500_000_000)
    _emit_stats(metrics, "MaximumUsedTransactionIDs", "1", {"Average": max_txn_ids}, dims)

    # ── Burst / queue / SQL Server / TempDB (dashboard + alert gauges) ──
    if burst_low is not None:
        # Alert: MIN < 20 — GT often sets 20.0 exactly; clamp strictly under.
        burst = round(min(float(burst_low), 19.0), 2)
    else:
        burst = round(rng.uniform(40.0, 100.0), 2)
    _emit_stats(metrics, "BurstBalance", "%", {"Average": burst}, dims)
    checkpoint_lag = round(max(0.0, checkpoint_high), 2)
    _emit_stats(metrics, "CheckpointLag", "s", {"Maximum": checkpoint_lag}, dims)
    if disk_queue_high > 0:
        disk_queue = max(5.1, float(disk_queue_high))
    else:
        disk_queue = min(4.9, rng.uniform(0.0, 4.0) * load * io_pressure)
    # Alert reads Average; the RDS investigation workflow compares Maximum.
    _emit_stats(metrics, "DiskQueueDepth", "1",
                {"Average": round(disk_queue, 2),
                 "Maximum": round(disk_queue * rng.uniform(1.0, 1.3), 2)}, dims)
    _emit_stats(metrics, "FailedSQLServerAgentJobsCount", "{count}", {"Average": 0}, dims)
    tempdb_data_avail = int(rng.randint(50_000_000, 500_000_000))
    tempdb_log_avail = int(rng.randint(20_000_000, 200_000_000))
    tempdb_data_usage = round(rng.uniform(5.0, 60.0), 2)
    tempdb_log_usage = round(rng.uniform(5.0, 40.0), 2)
    _emit_stats(metrics, "TempDbAvailableDataSpace", "By", {"Average": tempdb_data_avail}, dims)
    _emit_stats(metrics, "TempDbAvailableLogSpace", "By", {"Average": tempdb_log_avail}, dims)
    _emit_stats(metrics, "TempDbDataFileUsage", "%", {"Average": tempdb_data_usage}, dims)
    _emit_stats(metrics, "TempDbLogFileUsage", "%", {"Average": tempdb_log_usage}, dims)

    return metrics



from generators.aws_rds_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def _instance_id(ns: str, cfg: dict) -> str:
    """The RDS ``DBInstanceIdentifier`` (a configured name is scoped to the scenario)."""
    if cfg.get("instance_id"):
        return ScenarioIdentity(ns, cfg).qualify(str(cfg["instance_id"]))
    return ScenarioIdentity(ns).label("rds-01")


def workflow_input_values(scenario_data: dict | None) -> dict[str, str]:
    """Values the library investigation workflows take as inputs.

    The deployer fills matching manual-trigger inputs with them (and the literal
    fallbacks alert-triggered runs use); see ``elastic_config/workflow_templates.py``.
    """
    ns = (scenario_data or {}).get("namespace") or NAMESPACE
    cfg = (scenario_data or {}).get("generator_config") or {}
    return {"db_instance_identifier": _instance_id(ns, cfg)}


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run RDS metrics generator loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {})
    else:
        ns = NAMESPACE
        cfg = {}

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("rds-instance")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("rds")
    cloud = cfg.get("cloud") or _default_cloud()
    region = cloud.get("region", "us-east-1")
    account_id = ScenarioIdentity.from_scenario(scenario_data, ns).aws_account_id
    engine: str = cfg.get("engine", "postgres")
    instance_id: str = _instance_id(ns, cfg)
    total_storage: int = cfg.get("total_storage_bytes", 500 * 1024 * 1024 * 1024)  # 500 GiB
    low, high = (cfg.get("connections_baseline") or (10, 80))[:2]
    connections_baseline = (int(low), int(high))

    resource = build_signal_resource(
        ns=ns,
        signal_type="metrics",
        dataset=otel_dataset("aws.rds"),
        service_name=service_name,
        service_version="1.0.0",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name,
        cloud=cloud,
        extra_attrs={
            "cloud.account.id": account_id,
            "aws.rds.instance.id": instance_id,
            "DBInstanceIdentifier": instance_id,
            "Engine": engine,
        },
    )

    metric_state = RDSMetricState(rng, total_storage)
    scope = {"name": RDS_SCOPE, "version": SCOPE_VERSION}

    logger.info("RDS generator started (namespace=%s, instance=%s, engine=%s)",
                ns, instance_id, engine)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="aws_rds_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        metrics = _generate_metrics(
            metric_state, region, account_id, rng, active_chaos,
            instance_id=instance_id, engine=engine, connections_baseline=connections_baseline,
        )
        client.send_metrics_scoped(resource, scope, metrics)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("RDS generator stopped.")


def main() -> None:  # pragma: no cover
    """Standalone entry point for local testing."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "60"))

    def _stop(sig, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    client = OTLPClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
