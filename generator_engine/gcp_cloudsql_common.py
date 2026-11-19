"""Shared plumbing for the Cloud SQL generators (MySQL, PostgreSQL, SQL Server).

The three ``gcp_cloudsql_*_otel`` generators emulate what the OpenTelemetry
``googlecloudmonitoringreceiver`` produces when it pulls Cloud Monitoring
``cloudsql.googleapis.com/database/...`` time series:

* Resource attributes are ``gcp.resource_type`` plus the monitored-resource
  labels. For ``cloudsql_database`` those are ``project_id``, ``database_id``
  (``<project>:<instance>``) and ``region``. Query Insights metrics use the
  ``cloudsql_instance_database`` resource (``resource_id``,
  ``resource_container``, ``location``, ``database``).
* Metric labels become datapoint attributes (``attributes.state``, ...).
* Metric kinds map as the receiver maps them
  (``internal/metrics_conversion.go``):

  - ``GAUGE``       -> OTLP gauge; ``BOOL`` values become int 0/1
  - ``DELTA``       -> OTLP sum, delta temporality, not monotonic. Elastic maps
    it as a gauge, which is why the stock panels use ``SUM_OVER_TIME``.
  - ``CUMULATIVE``  -> OTLP sum, cumulative temporality, monotonic (counter,
    read with ``RATE``).

Cloud Monitoring samples Cloud SQL once a minute and DELTA points carry a
60 s window, so the generators tick every 60 s and every delta value is a
per-minute count. Panels compute ``rate = SUM(value) / (minutes * 60)``.

Generators keep every metric name in their own ``generator.py`` (the package
coverage audit reads that file); this module only builds points and the
instance-level metrics every Cloud SQL engine shares.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any

from app.telemetry import _format_attributes
from generator_engine.db_common import build_gauge, build_signal_resource
from generator_engine.identity import ScenarioIdentity
from generator_engine.scrape_clock import scrape_now_ns, scrape_start_ns

RECEIVER_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/"
    "googlecloudmonitoringreceiver"
)
RECEIVER_VERSION = "0.130.0"
SCRAPE_INTERVAL_S = 60.0

INSTANCE_STATES = (
    "RUNNING", "SUSPENDED", "RUNNABLE", "PENDING_CREATE",
    "MAINTENANCE", "FAILED", "UNKNOWN_STATE",
)

# Common metric names (also listed in each generator's METRIC_NAMES).
M = "cloudsql.googleapis.com/database/"


def receiver_scope() -> dict[str, str]:
    return {"name": RECEIVER_SCOPE, "version": RECEIVER_VERSION}


def default_cloud() -> dict[str, str]:
    return {
        "provider": "gcp",
        "platform": "gcp_cloud_sql",
        "region": "us-central1",
        "availability_zone": "us-central1-a",
    }


# ── point builders ────────────────────────────────────────────────────────────

def gauge(name: str, unit: str, value: int | float | bool,
          attrs: dict[str, Any] | None = None) -> dict[str, Any]:
    """Cloud Monitoring GAUGE. ``bool`` becomes int 0/1 like the receiver does."""
    if isinstance(value, bool):
        value = 1 if value else 0
    return build_gauge(name, unit, value, attrs or None)


def delta(name: str, unit: str, value: int | float,
          attrs: dict[str, Any] | None = None) -> dict[str, Any]:
    """Cloud Monitoring DELTA over the last 60 s: delta temporality, non-monotonic."""
    dp: dict[str, Any] = {
        "startTimeUnixNano": scrape_start_ns(int(SCRAPE_INTERVAL_S)),
        "timeUnixNano": scrape_now_ns(),
    }
    if isinstance(value, float):
        dp["asDouble"] = float(value)
    else:
        dp["asInt"] = str(int(value))
    if attrs:
        dp["attributes"] = _format_attributes(attrs)
    return {
        "name": name,
        "unit": unit,
        "sum": {"dataPoints": [dp], "aggregationTemporality": 1, "isMonotonic": False},
    }


def cumulative(name: str, unit: str, value: int, start_time_ns: str,
               attrs: dict[str, Any] | None = None) -> dict[str, Any]:
    """Cloud Monitoring CUMULATIVE: monotonic cumulative sum with a stable start."""
    dp: dict[str, Any] = {
        "startTimeUnixNano": start_time_ns,
        "timeUnixNano": scrape_now_ns(),
        "asInt": str(int(value)),
    }
    if attrs:
        dp["attributes"] = _format_attributes(attrs)
    return {
        "name": name,
        "unit": unit,
        "sum": {"dataPoints": [dp], "aggregationTemporality": 2, "isMonotonic": True},
    }


# ── instance model ────────────────────────────────────────────────────────────

@dataclass
class CloudSQLInstance:
    """One Cloud SQL instance (primary or read replica)."""

    name: str
    role: str  # "primary" | "replica"
    project_id: str
    region: str
    zone: str
    tier: str
    cores: float
    memory_quota: int
    disk_quota: int
    disk_base_util: float
    memory_base_util: float
    cpu_base_util: float
    conn_base: int
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def database_id(self) -> str:
        return f"{self.project_id}:{self.name}"

    @property
    def is_replica(self) -> bool:
        return self.role == "replica"


_TIER_SHAPES = {
    # tier: (vCPUs, memory bytes)
    "db-custom-2-8192": (2.0, 8 * 1024**3),
    "db-custom-4-16384": (4.0, 16 * 1024**3),
    "db-custom-8-32768": (8.0, 32 * 1024**3),
    "db-n1-standard-4": (4.0, 15 * 1024**3),
    "db-n1-standard-8": (8.0, 30 * 1024**3),
}


def tier_shape(tier: str) -> tuple[float, int]:
    if tier in _TIER_SHAPES:
        return _TIER_SHAPES[tier]
    parts = tier.split("-")
    try:
        if parts[:2] == ["db", "custom"] and len(parts) >= 4:
            return float(parts[2]), int(parts[3]) * 1024**2
    except ValueError:
        pass
    return 4.0, 16 * 1024**3


def build_instances(
    identity: ScenarioIdentity,
    cfg: dict,
    *,
    engine_role: str,
    default_tier: str,
    disk_gb: int,
    project_id: str,
    cloud: dict,
    rng: random.Random,
) -> list[CloudSQLInstance]:
    """Return ``[primary, replica]`` with names from ScenarioIdentity."""
    tier = str(cfg.get("tier") or default_tier)
    cores, mem = tier_shape(tier)
    disk_quota = int(cfg.get("disk_size_gb") or disk_gb) * 1024**3
    region = cloud.get("region", "us-central1")
    zone = cloud.get("availability_zone", f"{region}-a")
    primary_name = cfg.get("primary_instance") or identity.label(f"{engine_role}-primary")
    replica_name = cfg.get("replica_instance") or identity.label(f"{engine_role}-replica")
    primary = CloudSQLInstance(
        name=identity.qualify(primary_name), role="primary", project_id=project_id,
        region=region, zone=zone, tier=tier, cores=cores, memory_quota=mem,
        disk_quota=disk_quota, disk_base_util=rng.uniform(0.38, 0.48),
        memory_base_util=rng.uniform(0.55, 0.68), cpu_base_util=rng.uniform(0.28, 0.40),
        conn_base=rng.randint(70, 110),
    )
    replica_zone = f"{region}-b" if zone.endswith("-a") else f"{region}-a"
    replica = CloudSQLInstance(
        name=identity.qualify(replica_name), role="replica", project_id=project_id,
        region=region, zone=replica_zone, tier=tier, cores=cores, memory_quota=mem,
        disk_quota=disk_quota, disk_base_util=primary.disk_base_util * rng.uniform(0.97, 1.0),
        memory_base_util=rng.uniform(0.45, 0.58), cpu_base_util=rng.uniform(0.15, 0.25),
        conn_base=rng.randint(25, 45),
    )
    return [primary, replica]


def instance_resource(
    identity: ScenarioIdentity,
    inst: CloudSQLInstance,
    *,
    dataset: str,
    service_role: str,
    cloud: dict,
) -> dict[str, Any]:
    """``cloudsql_database`` resource, routed to ``metrics-<dataset>.otel-default``."""
    cloud = {**cloud, "availability_zone": inst.zone}
    return build_signal_resource(
        identity=identity,
        signal_type="metrics",
        dataset=dataset,
        service_name=service_role,
        host_name=inst.name,
        service_instance_id=inst.name,
        cloud=cloud,
        extra_attrs={
            "gcp.resource_type": "cloudsql_database",
            "project_id": inst.project_id,
            "database_id": inst.database_id,
            "region": inst.region,
            "cloud.account.id": inst.project_id,
            "cloud.resource_id": inst.database_id,
        },
    )


def insights_resource(
    identity: ScenarioIdentity,
    inst: CloudSQLInstance,
    database: str,
    *,
    dataset: str,
    service_role: str,
    cloud: dict,
) -> dict[str, Any]:
    """``cloudsql_instance_database`` resource used by Query Insights metrics."""
    cloud = {**cloud, "availability_zone": inst.zone}
    return build_signal_resource(
        identity=identity,
        signal_type="metrics",
        dataset=dataset,
        service_name=service_role,
        host_name=inst.name,
        service_instance_id=inst.name,
        cloud=cloud,
        extra_attrs={
            "gcp.resource_type": "cloudsql_instance_database",
            "resource_id": inst.database_id,
            "resource_container": inst.project_id,
            "location": inst.region,
            "database": database,
            "cloud.account.id": inst.project_id,
        },
    )


# ── fault knobs shared by every engine ────────────────────────────────────────

@dataclass
class CommonFaults:
    """Resolved values of the shared impact keys (0 / False = healthy)."""

    cpu_pct: float = 0.0
    memory_pct: float = 0.0
    disk_pct: float = 0.0
    connections: float = 0.0
    replica_lag_s: float = 0.0
    unavailable: bool = False


def fval(chaos: dict | None, key: str) -> float:
    """Float value of an impact key from a resolved chaos dict (0.0 when absent)."""
    if not chaos:
        return 0.0
    raw = chaos.get(key)
    try:
        return float(raw) if raw is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _pct_to_fraction(pct: float, floor: float) -> float:
    if pct <= 0:
        return 0.0
    if pct > 100:
        pct = 97.0
    return max(floor, min(0.995, pct / 100.0))


@dataclass
class InstanceSample:
    """Per-tick instance values shared between common and engine metrics."""

    load: float
    up: int
    uptime_s: int
    state: str
    cpu: float
    memory: float
    disk: float
    connections: int
    replica_lag_s: float


def sample_instance(
    inst: CloudSQLInstance,
    faults: CommonFaults,
    load: float,
    rng: random.Random,
) -> InstanceSample:
    """Healthy baseline with faults applied to the primary (lag to the replica)."""
    primary = not inst.is_replica
    cpu = inst.cpu_base_util * (0.6 + 0.6 * load) * rng.uniform(0.92, 1.08)
    mem = inst.memory_base_util + rng.uniform(-0.02, 0.02)
    inst.disk_base_util = min(0.70, inst.disk_base_util + rng.uniform(0.0, 0.00005))
    disk = inst.disk_base_util
    conns = int(inst.conn_base * (0.6 + 0.6 * load) * rng.uniform(0.9, 1.1))
    lag = rng.uniform(0.05, 0.6) if inst.is_replica else 0.0
    up, uptime, state = 1, 60, "RUNNING"
    if primary:
        if faults.cpu_pct > 0:
            cpu = _pct_to_fraction(faults.cpu_pct, 0.81) * rng.uniform(0.98, 1.0)
        if faults.memory_pct > 0:
            mem = _pct_to_fraction(faults.memory_pct, 0.86) * rng.uniform(0.99, 1.0)
        if faults.disk_pct > 0:
            disk = _pct_to_fraction(faults.disk_pct, 0.86)
        if faults.connections > 0:
            conns = int(max(conns + 1, faults.connections))
        if faults.unavailable:
            up, uptime, state = 0, rng.randint(8, 40), "FAILED"
            cpu = rng.uniform(0.01, 0.04)
            conns = rng.randint(0, 2)
    elif faults.replica_lag_s > 0:
        lag = max(1.0, faults.replica_lag_s) * rng.uniform(0.95, 1.05)
    return InstanceSample(
        load=load, up=up, uptime_s=uptime, state=state,
        cpu=round(max(0.005, min(cpu, 0.999)), 4),
        memory=round(max(0.05, min(mem, 0.999)), 4),
        disk=round(max(0.01, min(disk, 0.999)), 4),
        connections=max(0, conns), replica_lag_s=round(lag, 3),
    )


def common_instance_metrics(
    inst: CloudSQLInstance,
    s: InstanceSample,
    rng: random.Random,
    *,
    include_network_lag: bool = False,
) -> list[dict[str, Any]]:
    """Availability, CPU, memory, disk, network and replica lag series."""
    out: list[dict[str, Any]] = [
        gauge(M + "up", "1", s.up),
        delta(M + "uptime", "s", s.uptime_s),
    ]
    for state in INSTANCE_STATES:
        out.append(gauge(M + "instance_state", "1", state == s.state, {"state": state}))

    out.append(gauge(M + "cpu/utilization", "10^2.%", s.cpu))
    out.append(gauge(M + "cpu/reserved_cores", "1", float(inst.cores)))

    total_usage = int(inst.memory_quota * s.memory)
    usage = int(total_usage * rng.uniform(0.80, 0.88))  # minus buffer/cache
    out.append(gauge(M + "memory/utilization", "10^2.%", s.memory))
    out.append(gauge(M + "memory/usage", "By", usage))
    out.append(gauge(M + "memory/total_usage", "By", total_usage))
    out.append(gauge(M + "memory/quota", "By", int(inst.memory_quota)))

    out.append(gauge(M + "disk/bytes_used", "By", int(inst.disk_quota * s.disk)))
    out.append(gauge(M + "disk/quota", "By", int(inst.disk_quota)))
    out.append(gauge(M + "disk/utilization", "10^2.%", s.disk))
    io = 0.15 if s.up == 0 else s.load
    read_ops = int(rng.uniform(900, 2400) * (0.5 + io) * (0.6 if inst.is_replica else 1.0))
    write_ops = int(rng.uniform(600, 1500) * (0.5 + io))
    out.append(delta(M + "disk/read_ops_count", "1", read_ops))
    out.append(delta(M + "disk/write_ops_count", "1", write_ops))
    out.append(delta(M + "disk/read_bytes_count", "By", read_ops * rng.randint(12_000, 24_000)))
    out.append(delta(M + "disk/write_bytes_count", "By", write_ops * rng.randint(9_000, 18_000)))

    out.append(gauge(M + "network/connections", "1", int(s.connections)))
    rx = int(rng.uniform(4e6, 14e6) * (0.4 + io) * 60 / 10)
    tx = int(rng.uniform(8e6, 30e6) * (0.4 + io) * 60 / 10)
    out.append(delta(M + "network/received_bytes_count", "By", rx))
    out.append(delta(M + "network/sent_bytes_count", "By", tx))

    if not inst.is_replica:
        # Reported for the HA (regional) primary only; 0 while it cannot fail over.
        out.append(gauge(M + "available_for_failover", "1", 1 if s.up == 1 else 0))
    if inst.is_replica:
        out.append(gauge(M + "replication/replica_lag", "s", float(s.replica_lag_s)))
        if include_network_lag:
            net_lag = max(0, int(min(s.replica_lag_s, 5.0))) if s.replica_lag_s > 30 else 0
            out.append(gauge(M + "replication/network_lag", "s", net_lag))
    return out


def connection_attempts(
    s: InstanceSample, rng: random.Random, *, base_per_min: float,
) -> list[dict[str, Any]]:
    """``network/connection_attempt_count`` by ``login_status`` (PG, SQL Server)."""
    ok = max(1, int(base_per_min * (0.4 + s.load) * rng.uniform(0.85, 1.15)))
    if s.up == 0:
        ok, fail = rng.randint(0, 3), rng.randint(20, 60)
    else:
        fail = rng.randint(0, 2)
    return [
        delta(M + "network/connection_attempt_count", "1", ok, {"login_status": "succeeded"}),
        delta(M + "network/connection_attempt_count", "1", fail, {"login_status": "failed"}),
    ]
