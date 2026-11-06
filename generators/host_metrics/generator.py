#!/usr/bin/env python3
"""Host Metrics Generator — sends system.* metrics for Elastic Infrastructure UI via OTLP.

Generates realistic host metrics matching the OTel hostmetricsreceiver scraper
scope names, so Elastic's Infrastructure UI recognizes them as host metrics.

Generates for 3 hosts (one per cloud: AWS, GCP, Azure) matching the active
scenario's multi-cloud architecture.

Usage (standalone):
    python3 -m generators.host_metrics.generator
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/system_otel@0.3.0"

import logging
import os
import random
import signal
import threading
import time

from app.telemetry import OTLPClient, ESBulkClient, _format_attributes, SCHEMA_URL
from app.config import ACTIVE_SCENARIO
from generator_engine.db_common import resolve_active_chaos as _resolve_active_chaos
from generator_engine.identity import ScenarioIdentity
from generator_engine.scrape_clock import scrape_now_ns, scrape_start_ns
from typing import Any

logger = logging.getLogger("host-metrics-generator")

# ── Configuration ─────────────────────────────────────────────────────────────
METRICS_INTERVAL = int(os.getenv("METRICS_INTERVAL", "10"))  # seconds between scrapes

# ── OTel hostmetricsreceiver scope names ──────────────────────────────────────
SCRAPER_BASE = "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper"
SCRAPERS = {
    "load": f"{SCRAPER_BASE}/loadscraper",
    "cpu": f"{SCRAPER_BASE}/cpuscraper",
    "memory": f"{SCRAPER_BASE}/memoryscraper",
    "disk": f"{SCRAPER_BASE}/diskscraper",
    "filesystem": f"{SCRAPER_BASE}/filesystemscraper",
    "network": f"{SCRAPER_BASE}/networkscraper",
    "processes": f"{SCRAPER_BASE}/processesscraper",
    "process": f"{SCRAPER_BASE}/processscraper",
}

# ── Per-process templates ─────────────────────────────────────────────────────
# Realistic processes running on a K8s node.  Each host gets its own set with
# unique PIDs derived from pid_base + host index offset.
PROCESS_TEMPLATES = [
    {
        "executable.name": "systemd",
        "executable.path": "/usr/lib/systemd/systemd",
        "command": "systemd",
        "command_line": "/usr/lib/systemd/systemd --switched-root --system --deserialize 22",
        "owner": "root",
        "pid_base": 1,
        "cpu_weight": 0.02,
        "mem_bytes_range": (10_000_000, 30_000_000),
        "virtual_bytes_range": (100_000_000, 200_000_000),
        "threads_range": (1, 3),
        "fd_range": (50, 150),
    },
    {
        "executable.name": "containerd",
        "executable.path": "/usr/bin/containerd",
        "command": "containerd",
        "command_line": "/usr/bin/containerd",
        "owner": "root",
        "pid_base": 512,
        "cpu_weight": 0.08,
        "mem_bytes_range": (80_000_000, 200_000_000),
        "virtual_bytes_range": (1_000_000_000, 2_000_000_000),
        "threads_range": (20, 50),
        "fd_range": (200, 500),
    },
    {
        "executable.name": "kubelet",
        "executable.path": "/usr/bin/kubelet",
        "command": "kubelet",
        "command_line": "/usr/bin/kubelet --config=/var/lib/kubelet/config.yaml --kubeconfig=/etc/kubernetes/kubelet.conf",
        "owner": "root",
        "pid_base": 1024,
        "cpu_weight": 0.12,
        "mem_bytes_range": (100_000_000, 300_000_000),
        "virtual_bytes_range": (1_500_000_000, 3_000_000_000),
        "threads_range": (15, 40),
        "fd_range": (300, 800),
    },
    {
        "executable.name": "kube-proxy",
        "executable.path": "/usr/bin/kube-proxy",
        "command": "kube-proxy",
        "command_line": "/usr/bin/kube-proxy --config=/var/lib/kube-proxy/config.conf",
        "owner": "root",
        "pid_base": 1200,
        "cpu_weight": 0.03,
        "mem_bytes_range": (20_000_000, 60_000_000),
        "virtual_bytes_range": (500_000_000, 1_000_000_000),
        "threads_range": (5, 12),
        "fd_range": (30, 100),
    },
    {
        "executable.name": "python3",
        "executable.path": "/usr/bin/python3",
        "command": "python3",
        "command_line": "python3 -m uvicorn app.main:app --host 0.0.0.0 --port 80",
        "owner": "app",
        "pid_base": 2048,
        "cpu_weight": 0.15,
        "mem_bytes_range": (120_000_000, 350_000_000),
        "virtual_bytes_range": (800_000_000, 1_500_000_000),
        "threads_range": (4, 12),
        "fd_range": (60, 200),
    },
    {
        "executable.name": "java",
        "executable.path": "/usr/bin/java",
        "command": "java",
        "command_line": "java -Xmx512m -jar /opt/service/service.jar",
        "owner": "app",
        "pid_base": 3000,
        "cpu_weight": 0.20,
        "mem_bytes_range": (200_000_000, 550_000_000),
        "virtual_bytes_range": (2_000_000_000, 4_000_000_000),
        "threads_range": (30, 80),
        "fd_range": (150, 400),
    },
    {
        "executable.name": "nginx",
        "executable.path": "/usr/sbin/nginx",
        "command": "nginx",
        "command_line": "nginx: worker process",
        "owner": "www-data",
        "pid_base": 4000,
        "cpu_weight": 0.06,
        "mem_bytes_range": (15_000_000, 80_000_000),
        "virtual_bytes_range": (300_000_000, 600_000_000),
        "threads_range": (1, 4),
        "fd_range": (100, 500),
    },
    {
        "executable.name": "otelcol",
        "executable.path": "/usr/bin/otelcol-contrib",
        "command": "otelcol-contrib",
        "command_line": "/usr/bin/otelcol-contrib --config=/etc/otelcol/config.yaml",
        "owner": "otel",
        "pid_base": 5000,
        "cpu_weight": 0.05,
        "mem_bytes_range": (60_000_000, 150_000_000),
        "virtual_bytes_range": (700_000_000, 1_200_000_000),
        "threads_range": (8, 20),
        "fd_range": (50, 200),
    },
    {
        "executable.name": "sshd",
        "executable.path": "/usr/sbin/sshd",
        "command": "sshd",
        "command_line": "sshd: /usr/sbin/sshd -D",
        "owner": "root",
        "pid_base": 800,
        "cpu_weight": 0.01,
        "mem_bytes_range": (5_000_000, 15_000_000),
        "virtual_bytes_range": (80_000_000, 150_000_000),
        "threads_range": (1, 2),
        "fd_range": (10, 30),
    },
    {
        "executable.name": "node",
        "executable.path": "/usr/bin/node",
        "command": "node",
        "command_line": "node /opt/monitoring/index.js",
        "owner": "monitor",
        "pid_base": 6000,
        "cpu_weight": 0.04,
        "mem_bytes_range": (40_000_000, 120_000_000),
        "virtual_bytes_range": (500_000_000, 1_000_000_000),
        "threads_range": (6, 14),
        "fd_range": (30, 100),
    },
]


# ── Host definitions from active scenario ─────────────────────────────────────
def _load_hosts_and_clusters():
    from scenario_engine import get_scenario

    scn = get_scenario(ACTIVE_SCENARIO)
    cluster_by_provider = {c["provider"]: c["name"] for c in scn.k8s_clusters}
    return scn.hosts, cluster_by_provider


HOSTS, CLUSTER_BY_PROVIDER = _load_hosts_and_clusters()

# Service hosts that appear on Hosts Overview via other receivers (kafka/nginx/redis).
# Without hostmetrics for these names, the overview table shows null CPU/Mem/Load when
# the dataset filter is cleared (those hosts rank first by doc count).
_INFRA_HOST_SUFFIXES = ("kafka-broker", "redis-host", "nginx-01", "nginx-02")


def _with_infra_hosts(hosts: list[dict], namespace: str) -> list[dict]:
    """Clone scenario hosts as companion infra hostnames used by other generators."""
    if not hosts or not namespace:
        return hosts
    existing = {h.get("host.name") for h in hosts}
    out = list(hosts)
    for i, suffix in enumerate(_INFRA_HOST_SUFFIXES):
        name = ScenarioIdentity(namespace).label(suffix)
        if name in existing:
            continue
        base = dict(hosts[i % len(hosts)])
        base["host.name"] = name
        base["host.id"] = name
        if "cloud.instance.id" in base:
            base["cloud.instance.id"] = name
        out.append(base)
    return out


def _build_host_resource(host_cfg: dict, cluster_by_provider: dict = CLUSTER_BY_PROVIDER) -> dict:
    """Build OTLP resource for a host with all required Infrastructure UI attributes.

    cluster_by_provider: provider → cluster-name map for the active scenario.
    Defaults to the module-level CLUSTER_BY_PROVIDER (standalone / no-scenario-data path).
    """
    attrs = {}
    for key in [
        "host.name",
        "host.id",
        "host.arch",
        "host.type",
        "host.image.id",
        "host.cpu.model.name",
        "host.cpu.vendor.id",
        "host.cpu.family",
        "host.cpu.model.id",
        "host.cpu.stepping",
        "host.cpu.cache.l2.size",
        "os.type",
        "os.description",
        "cloud.provider",
        "cloud.platform",
        "cloud.region",
        "cloud.availability_zone",
        "cloud.account.id",
        "cloud.instance.id",
    ]:
        if key in host_cfg:
            attrs[key] = host_cfg[key]

    cluster_name = cluster_by_provider.get(host_cfg.get("cloud.provider"))
    if cluster_name:
        attrs["k8s.cluster.name"] = cluster_name

    attrs["telemetry.sdk.name"] = "opentelemetry"
    attrs["telemetry.sdk.version"] = "1.24.0"
    attrs["telemetry.sdk.language"] = "python"
    attrs["data_stream.type"] = "metrics"
    attrs["data_stream.dataset"] = "hostmetricsreceiver"
    attrs["data_stream.namespace"] = "default"

    # Format array attributes specially
    formatted = _format_attributes(attrs)

    # Add array-type attributes (host.ip, host.mac)
    for arr_key in ["host.ip", "host.mac"]:
        if arr_key in host_cfg:
            formatted.append(
                {
                    "key": arr_key,
                    "value": {
                        "arrayValue": {
                            "values": [{"stringValue": v} for v in host_cfg[arr_key]]
                        }
                    },
                }
            )

    return {
        "attributes": formatted,
        "schemaUrl": SCHEMA_URL,
    }


# ── Per-host metric state (for cumulative counters) ──────────────────────────

# Filesystem mount points: (device, mountpoint, fs_type, disk_total_fraction)
# Each mountpoint gets a distinct fraction of disk_total_bytes so they do not
# emit byte-identical usage values.
_FS_MOUNTS = [
    ("/dev/sda1", "/",     "ext4", 0.40),   # root: smaller
    ("/dev/sdb1", "/data", "xfs",  0.60),   # data: larger
]


class HostMetricState:
    """Tracks cumulative counter values and slow-walk fill levels for a single host.

    Filesystem fill levels are walked in tick() with a small upward bias so that
    disk-filling charts show a realistic ramp rather than per-scrape noise.
    Setting fs_target[mountpoint] causes tick() to ramp toward that value at ~2%/tick.
    """

    def __init__(
        self, cpu_count: int, mem_total: int, disk_total: int, rng: random.Random
    ):
        self.cpu_count = cpu_count
        self.mem_total = mem_total
        self.disk_total = disk_total
        self._rng = rng
        # Cumulative counters
        self.cpu_time = {
            f"cpu{i}": {
                "user": rng.uniform(1000, 5000),
                "system": rng.uniform(500, 2000),
                "idle": rng.uniform(10000, 50000),
                "wait": rng.uniform(10, 200),
            }
            for i in range(cpu_count)
        }
        self.disk_io_read = rng.uniform(1e9, 5e9)
        self.disk_io_write = rng.uniform(2e9, 10e9)
        self.disk_ops_read = rng.randint(100000, 500000)
        self.disk_ops_write = rng.randint(200000, 800000)
        self.net_io_recv = rng.uniform(5e9, 20e9)
        self.net_io_send = rng.uniform(2e9, 10e9)
        # io_time tracks cumulative seconds spent in IO; advanced proportionally to ops.
        # Average service time constants (seconds per op) — these stay fixed per host.
        self._io_time_per_read_op = rng.uniform(0.0001, 0.0005)   # ~0.1–0.5ms
        self._io_time_per_write_op = rng.uniform(0.0002, 0.001)   # ~0.2–1ms
        self.disk_io_time_read = rng.uniform(1000, 10000)
        self.disk_io_time_write = rng.uniform(2000, 15000)
        self.net_packets_recv = rng.randint(5000000, 50000000)
        self.net_packets_send = rng.randint(3000000, 30000000)
        self.net_dropped_recv = rng.randint(0, 500)
        self.net_dropped_send = rng.randint(0, 300)
        self.net_errors_recv = rng.randint(0, 200)
        self.net_errors_send = rng.randint(0, 100)
        self.processes_created = rng.randint(10000, 100000)

        # Filesystem fill levels (0.0–1.0) — distinct baseline per mountpoint.
        # Walked slowly in tick() so disk-filling is a visible ramp, not noise.
        self.fs_fill: dict[str, float] = {
            mp: rng.uniform(lo, hi)
            for _, mp, _, _ in _FS_MOUNTS
            for lo, hi in [(0.20, 0.50) if mp == "/" else (0.10, 0.40)]
        }
        # Fault targets — None = free walk, float = ramp toward this value (0-1)
        self.fs_target: dict[str, float | None] = {}

    def tick(self):
        """Advance cumulative counters and filesystem fill levels."""
        rng = self._rng
        for cpu_id in self.cpu_time:
            self.cpu_time[cpu_id]["user"] += rng.uniform(0.5, 3.0)
            self.cpu_time[cpu_id]["system"] += rng.uniform(0.2, 1.5)
            self.cpu_time[cpu_id]["idle"] += rng.uniform(5.0, 9.0)
            self.cpu_time[cpu_id]["wait"] += rng.uniform(0.0, 0.5)
        # Disk IO: advance ops, then derive io_time from ops × per-op service time
        ops_r = rng.randint(5, 200)
        ops_w = rng.randint(10, 500)
        self.disk_io_read += rng.randint(50000, 5000000)
        self.disk_io_write += rng.randint(100000, 10000000)
        self.disk_ops_read += ops_r
        self.disk_ops_write += ops_w
        self.disk_io_time_read += ops_r * self._io_time_per_read_op
        self.disk_io_time_write += ops_w * self._io_time_per_write_op
        self.net_io_recv += rng.randint(100000, 50000000)
        self.net_io_send += rng.randint(50000, 20000000)
        self.net_packets_recv += rng.randint(100, 50000)
        self.net_packets_send += rng.randint(50, 30000)
        self.net_dropped_recv += rng.randint(0, 2)
        self.net_dropped_send += rng.randint(0, 1)
        self.net_errors_recv += rng.randint(0, 1)
        self.net_errors_send += rng.randint(0, 1)
        self.processes_created += rng.randint(1, 10)

        # Advance filesystem fill levels.
        # Baseline: gentle upward drift (+0.007/tick ≈ 0.7%) with small noise.
        # Fault target: ramp at up to 2%/tick until target is reached.
        for mp in list(self.fs_fill.keys()):
            target = self.fs_target.get(mp)
            if target is not None:
                diff = target - self.fs_fill[mp]
                step = min(abs(diff), 0.02) * (1.0 if diff > 0 else -1.0)
                self.fs_fill[mp] = max(0.01, min(0.99, self.fs_fill[mp] + step))
            else:
                self.fs_fill[mp] = max(0.01, min(0.95,
                    self.fs_fill[mp] + rng.uniform(-0.005, 0.007)))


class ProcessState:
    """Tracks per-process cumulative counters."""

    def __init__(self, template: dict, host_index: int, rng: random.Random):
        self.template = template
        self.pid = template["pid_base"] + host_index * 100
        self._rng = rng
        w = template["cpu_weight"]
        # Cumulative CPU time (seconds)
        self.cpu_time_user = rng.uniform(100, 5000) * w
        self.cpu_time_system = rng.uniform(50, 2000) * w
        # Cumulative disk I/O (bytes)
        self.disk_read = rng.uniform(1e6, 1e9) * w
        self.disk_write = rng.uniform(1e6, 1e9) * w
        # Cumulative context switches
        self.ctx_voluntary = rng.randint(10000, 500000)
        self.ctx_involuntary = rng.randint(1000, 50000)

    def tick(self):
        rng = self._rng
        w = self.template["cpu_weight"]
        self.cpu_time_user += rng.uniform(0.05, 2.0) * w
        self.cpu_time_system += rng.uniform(0.02, 0.8) * w
        self.disk_read += rng.randint(0, 500000) * w
        self.disk_write += rng.randint(0, 1000000) * w
        self.ctx_voluntary += rng.randint(1, 100)
        self.ctx_involuntary += rng.randint(0, 20)


def _build_sum_metric(
    name: str, value, unit: str, attributes: dict | None = None, is_int: bool = False
) -> dict:
    """Build a cumulative sum metric."""
    dp: dict = {
        "startTimeUnixNano": scrape_start_ns(),
        "timeUnixNano": scrape_now_ns(),
    }
    if is_int:
        dp["asInt"] = str(int(value))
    else:
        dp["asDouble"] = float(value)

    if attributes:
        dp["attributes"] = _format_attributes(attributes)

    return {
        "name": name,
        "unit": unit,
        "sum": {
            "dataPoints": [dp],
            "aggregationTemporality": 2,  # cumulative
            "isMonotonic": True,
        },
    }


def _resource_attr_map(resource: dict) -> dict[str, Any]:
    """Flatten OTLP resource attributes; drop data_stream.* (set on the doc root)."""
    out: dict[str, Any] = {}
    for attr in resource.get("attributes", []):
        key = attr["key"]
        if key.startswith("data_stream."):
            continue
        val = attr["value"]
        if "stringValue" in val:
            out[key] = val["stringValue"]
        elif "intValue" in val:
            out[key] = int(val["intValue"])
        elif "doubleValue" in val:
            out[key] = float(val["doubleValue"])
        elif "boolValue" in val:
            out[key] = bool(val["boolValue"])
    return out


def _otlp_attr_map(attributes: list[dict] | None) -> dict[str, Any]:
    """Flatten OTLP data-point attributes to a plain dict."""
    out: dict[str, Any] = {}
    for attr in attributes or []:
        key = attr["key"]
        val = attr["value"]
        if "stringValue" in val:
            out[key] = val["stringValue"]
        elif "intValue" in val:
            out[key] = int(val["intValue"])
        elif "doubleValue" in val:
            out[key] = float(val["doubleValue"])
        elif "boolValue" in val:
            out[key] = bool(val["boolValue"])
    return out


def _otlp_dp_value(dp: dict) -> int | float:
    if "asInt" in dp:
        return int(dp["asInt"])
    return float(dp["asDouble"])


def build_use_metrics_doc(
    resource: dict,
    use: dict[str, Any],
    *,
    timestamp_ms: int | None = None,
    attributes: dict[str, Any] | None = None,
    unit: str = "1",
    temporality: str | None = None,
) -> dict[str, Any]:
    """ES bulk companion doc for hostmetrics when OTLP never indexes (ALERTS #96/#103)."""
    ts = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
    doc: dict[str, Any] = {
        "@timestamp": ts,
        "unit": unit,
        "data_stream": {
            "type": "metrics",
            "dataset": "hostmetricsreceiver.otel",
            "namespace": "default",
        },
        "resource": {"attributes": _resource_attr_map(resource)},
        "attributes": attributes or {},
        "metrics": use,
    }
    if temporality:
        doc["temporality"] = temporality
        doc["start_timestamp"] = ts - 60_000
    return doc


def build_hostmetrics_bulk_docs(
    resource: dict,
    metrics_by_scope: dict[str, list],
    *,
    timestamp_ms: int | None = None,
) -> list[dict[str, Any]]:
    """Convert OTLP hostmetrics into ES bulk companions for Host Details dashboards.

    Metrics that share the same data-point attributes are co-located on one doc
    (avoids OTel fingerprint collisions). Attribute-bearing series (cpu/state,
    device/direction, mountpoint) stay separate so Lens filters resolve.
    """
    ts = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
    # key: (frozenset attrs, is_cumulative) -> {metric_name: value, ...} + unit
    buckets: dict[tuple[frozenset, bool], dict[str, Any]] = {}
    units: dict[tuple[frozenset, bool], str] = {}

    for metrics in metrics_by_scope.values():
        for metric in metrics:
            name = metric["name"]
            unit = metric.get("unit") or "1"
            if "gauge" in metric:
                dps = metric["gauge"]["dataPoints"]
                cumulative = False
            elif "sum" in metric:
                dps = metric["sum"]["dataPoints"]
                cumulative = True
            else:
                continue
            for dp in dps:
                attrs = _otlp_attr_map(dp.get("attributes"))
                key = (frozenset(attrs.items()), cumulative)
                buckets.setdefault(key, {})[name] = _otlp_dp_value(dp)
                units.setdefault(key, unit)

    docs: list[dict[str, Any]] = []
    for (attrs_fs, cumulative), metrics_obj in buckets.items():
        docs.append(
            build_use_metrics_doc(
                resource,
                metrics_obj,
                timestamp_ms=ts,
                attributes=dict(attrs_fs),
                unit=units.get((attrs_fs, cumulative), "1"),
                temporality="cumulative" if cumulative else None,
            )
        )
    return docs


def _build_gauge_metric(
    name: str, value, unit: str, attributes: dict | None = None, is_int: bool = False
) -> dict:
    """Build a gauge metric."""
    dp: dict = {"timeUnixNano": scrape_now_ns()}
    if is_int:
        dp["asInt"] = str(int(value))
    else:
        dp["asDouble"] = float(value)

    if attributes:
        dp["attributes"] = _format_attributes(attributes)

    return {
        "name": name,
        "unit": unit,
        "gauge": {"dataPoints": [dp]},
    }


def _generate_host_metrics(
    state: HostMetricState,
    rng: random.Random,
    cpu_spike_pct: float = 0,
    memory_spike_pct: float = 0,
    disk_io_multiplier: float = 1.0,
    network_connections_multiplier: float = 1.0,
) -> tuple[dict[str, list], dict[str, Any]]:
    """Generate all host metrics grouped by scraper scope name.

    Returns ``(metrics_by_scope, use_overview)``. ``use_overview`` feeds the
    ES bulk USE companion when OTLP metrics never land on the demo ES.
    """
    state.tick()
    metrics_by_scope: dict[str, list] = {}

    # ── Load metrics ──
    if cpu_spike_pct > 0:
        load_1m = rng.uniform(cpu_spike_pct / 20, cpu_spike_pct / 12)
    else:
        load_1m = rng.uniform(0.5, 4.0)
    load_5m = load_1m * rng.uniform(0.7, 1.1)
    load_15m = load_5m * rng.uniform(0.8, 1.05)
    metrics_by_scope[SCRAPERS["load"]] = [
        _build_gauge_metric("system.cpu.load_average.1m", load_1m, "{thread}"),
        _build_gauge_metric("system.cpu.load_average.5m", load_5m, "{thread}"),
        _build_gauge_metric("system.cpu.load_average.15m", load_15m, "{thread}"),
    ]

    # ── CPU metrics ──
    cpu_metrics = [
        _build_gauge_metric(
            "system.cpu.logical.count", state.cpu_count, "{cpu}", is_int=True
        ),
    ]
    for cpu_id, times in state.cpu_time.items():
        total = sum(times.values())
        for state_name, val in times.items():
            cpu_metrics.append(
                _build_sum_metric(
                    "system.cpu.time",
                    val,
                    "s",
                    attributes={"cpu": cpu_id, "state": state_name},
                )
            )
            # Bias utilization toward spike target when active
            if cpu_spike_pct > 0 and total > 0:
                target = cpu_spike_pct / 100.0
                if state_name == "user":
                    util = target * rng.uniform(0.55, 0.65)
                elif state_name == "system":
                    util = target * rng.uniform(0.25, 0.35)
                elif state_name == "idle":
                    util = max(0.01, 1.0 - target) * rng.uniform(0.8, 1.2)
                else:  # wait
                    util = target * rng.uniform(0.02, 0.08)
            else:
                util = val / total if total > 0 else 0
            cpu_metrics.append(
                _build_gauge_metric(
                    "system.cpu.utilization",
                    util,
                    "1",
                    attributes={"cpu": cpu_id, "state": state_name},
                )
            )
    metrics_by_scope[SCRAPERS["cpu"]] = cpu_metrics

    # ── Memory metrics ──
    if memory_spike_pct > 0:
        mem_used_pct = (memory_spike_pct / 100.0) * rng.uniform(0.95, 1.05)
        mem_used_pct = min(mem_used_pct, 0.97)
        mem_cached_pct = rng.uniform(0.01, 0.03)
        mem_buffered_pct = rng.uniform(0.005, 0.01)
    else:
        mem_used_pct = rng.uniform(0.35, 0.85)
        mem_cached_pct = rng.uniform(0.05, 0.20)
        mem_buffered_pct = rng.uniform(0.01, 0.05)
    mem_free_pct = 1.0 - mem_used_pct - mem_cached_pct - mem_buffered_pct
    if mem_free_pct < 0:
        mem_free_pct = 0.05
        mem_used_pct = 1.0 - mem_free_pct - mem_cached_pct - mem_buffered_pct

    mem_total = state.mem_total
    mem_states = {
        "used": mem_used_pct,
        "free": mem_free_pct,
        "cached": mem_cached_pct,
        "buffered": mem_buffered_pct,
    }
    mem_metrics = []
    for mem_state, pct in mem_states.items():
        mem_metrics.append(
            _build_gauge_metric(
                "system.memory.usage",
                int(mem_total * pct),
                "By",
                attributes={"state": mem_state},
                is_int=True,
            )
        )
        mem_metrics.append(
            _build_gauge_metric(
                "system.memory.utilization",
                pct,
                "1",
                attributes={"state": mem_state},
            )
        )
    # Add slab states for utilization
    for slab_state in ["slab_reclaimable", "slab_unreclaimable"]:
        slab_pct = rng.uniform(0.01, 0.03)
        mem_metrics.append(
            _build_gauge_metric(
                "system.memory.utilization",
                slab_pct,
                "1",
                attributes={"state": slab_state},
            )
        )
    metrics_by_scope[SCRAPERS["memory"]] = mem_metrics

    # ── Disk metrics ──
    # disk_io_multiplier scales reported throughput and operations so an IO-saturation
    # fault produces a visible spike without altering the underlying state counters
    # (which are cumulative and must stay monotonically increasing).
    _disk_io_mul = max(1.0, disk_io_multiplier)
    disk_metrics = []
    for device in ["sda", "sdb"]:
        disk_metrics.append(
            _build_sum_metric(
                "system.disk.io",
                state.disk_io_read * _disk_io_mul,
                "By",
                attributes={"device": device, "direction": "read"},
            )
        )
        disk_metrics.append(
            _build_sum_metric(
                "system.disk.io",
                state.disk_io_write * _disk_io_mul,
                "By",
                attributes={"device": device, "direction": "write"},
            )
        )
        disk_metrics.append(
            _build_sum_metric(
                "system.disk.operations",
                int(state.disk_ops_read * _disk_io_mul),
                "{operation}",
                attributes={"device": device, "direction": "read"},
                is_int=True,
            )
        )
        disk_metrics.append(
            _build_sum_metric(
                "system.disk.operations",
                int(state.disk_ops_write * _disk_io_mul),
                "{operation}",
                attributes={"device": device, "direction": "write"},
                is_int=True,
            )
        )
        # disk.io_time — cumulative seconds spent on IO (OTel dashboard panel)
        disk_metrics.append(
            _build_sum_metric(
                "system.disk.io_time",
                state.disk_io_time_read * _disk_io_mul,
                "s",
                attributes={"device": device, "direction": "read"},
            )
        )
        disk_metrics.append(
            _build_sum_metric(
                "system.disk.io_time",
                state.disk_io_time_write * _disk_io_mul,
                "s",
                attributes={"device": device, "direction": "write"},
            )
        )
    metrics_by_scope[SCRAPERS["disk"]] = disk_metrics

    # ── Filesystem metrics ──
    # Each mountpoint has its own disk_total (fraction of host disk_total) and a
    # slow-walk fill level from HostMetricState.fs_fill, so the two devices diverge
    # over time and disk-fill charts show a realistic ramp rather than per-scrape noise.
    fs_metrics = []
    reserved_pct = 0.05  # 5% reserved for root, matching real Linux ext4/xfs behavior
    for device, mountpoint, fs_type, total_frac in _FS_MOUNTS:
        mp_total = int(state.disk_total * total_frac)
        fill = state.fs_fill.get(mountpoint, 0.35)
        reserved = int(mp_total * reserved_pct)
        used = int(mp_total * fill)
        free = max(0, mp_total - used - reserved)
        fs_metrics.append(
            _build_gauge_metric(
                "system.filesystem.usage",
                used,
                "By",
                attributes={
                    "device": device,
                    "mountpoint": mountpoint,
                    "type": fs_type,
                    "state": "used",
                },
                is_int=True,
            )
        )
        fs_metrics.append(
            _build_gauge_metric(
                "system.filesystem.usage",
                free,
                "By",
                attributes={
                    "device": device,
                    "mountpoint": mountpoint,
                    "type": fs_type,
                    "state": "free",
                },
                is_int=True,
            )
        )
        fs_metrics.append(
            _build_gauge_metric(
                "system.filesystem.usage",
                reserved,
                "By",
                attributes={
                    "device": device,
                    "mountpoint": mountpoint,
                    "type": fs_type,
                    "state": "reserved",
                },
                is_int=True,
            )
        )
        fs_metrics.append(
            _build_gauge_metric(
                "system.filesystem.utilization",
                fill,
                "1",
                attributes={
                    "device": device,
                    "mountpoint": mountpoint,
                    "type": fs_type,
                },
            )
        )
    metrics_by_scope[SCRAPERS["filesystem"]] = fs_metrics
    fs_util_values = [
        m["gauge"]["dataPoints"][0]["asDouble"]
        for m in fs_metrics
        if m["name"] == "system.filesystem.utilization"
    ]

    # ── Network metrics ──
    net_metrics = []
    for device in ["eth0", "eth1"]:
        net_metrics.append(
            _build_sum_metric(
                "system.network.io",
                state.net_io_recv,
                "By",
                attributes={"device": device, "direction": "receive"},
            )
        )
        net_metrics.append(
            _build_sum_metric(
                "system.network.io",
                state.net_io_send,
                "By",
                attributes={"device": device, "direction": "transmit"},
            )
        )
        # network.packets — cumulative packet counts (OTel dashboard panel)
        net_metrics.append(
            _build_sum_metric(
                "system.network.packets",
                state.net_packets_recv,
                "{packet}",
                attributes={"device": device, "direction": "receive"},
                is_int=True,
            )
        )
        net_metrics.append(
            _build_sum_metric(
                "system.network.packets",
                state.net_packets_send,
                "{packet}",
                attributes={"device": device, "direction": "transmit"},
                is_int=True,
            )
        )
        # network.dropped — cumulative dropped packet counts (OTel dashboard panel)
        net_metrics.append(
            _build_sum_metric(
                "system.network.dropped",
                state.net_dropped_recv,
                "{packet}",
                attributes={"device": device, "direction": "receive"},
                is_int=True,
            )
        )
        net_metrics.append(
            _build_sum_metric(
                "system.network.dropped",
                state.net_dropped_send,
                "{packet}",
                attributes={"device": device, "direction": "transmit"},
                is_int=True,
            )
        )
        # network.errors — cumulative error counts (OTel dashboard panel)
        net_metrics.append(
            _build_sum_metric(
                "system.network.errors",
                state.net_errors_recv,
                "{error}",
                attributes={"device": device, "direction": "receive"},
                is_int=True,
            )
        )
        net_metrics.append(
            _build_sum_metric(
                "system.network.errors",
                state.net_errors_send,
                "{error}",
                attributes={"device": device, "direction": "transmit"},
                is_int=True,
            )
        )
    # network.connections — TCP connection count by state (OTel dashboard panel)
    # network_connections_multiplier scales ESTABLISHED and TIME_WAIT counts to model
    # conntrack-table-full or ephemeral-port-exhaustion faults.
    _net_mul = max(1.0, network_connections_multiplier)
    tcp_states = {
        "ESTABLISHED": int(rng.randint(40, 200) * _net_mul),
        "TIME_WAIT": int(rng.randint(5, 50) * _net_mul),
        "CLOSE_WAIT": rng.randint(0, 10),
        "LISTEN": rng.randint(10, 30),
        "SYN_SENT": rng.randint(0, 3),
        "SYN_RECV": rng.randint(0, 3),
        "FIN_WAIT1": rng.randint(0, 5),
        "FIN_WAIT2": rng.randint(0, 5),
    }
    for tcp_state, count in tcp_states.items():
        net_metrics.append(
            _build_gauge_metric(
                "system.network.connections",
                count,
                "{connection}",
                attributes={"protocol": "tcp", "state": tcp_state},
                is_int=True,
            )
        )
    metrics_by_scope[SCRAPERS["network"]] = net_metrics

    # ── Process metrics ──
    running_count = rng.randint(1, 8)
    sleeping = rng.randint(50, 200)
    metrics_by_scope[SCRAPERS["processes"]] = [
        _build_gauge_metric(
            "system.processes.count",
            running_count,
            "{process}",
            attributes={"status": "running"},
            is_int=True,
        ),
        _build_gauge_metric(
            "system.processes.count",
            sleeping,
            "{process}",
            attributes={"status": "sleeping"},
            is_int=True,
        ),
        # processes.created — cumulative process creation count (OTel dashboard panel)
        _build_sum_metric(
            "system.processes.created",
            state.processes_created,
            "{process}",
            is_int=True,
        ),
    ]

    use_overview = {
        "system.cpu.load_average.1m": float(load_1m),
        "system.filesystem.utilization": float(
            sum(fs_util_values) / len(fs_util_values)
        ) if fs_util_values else 0.0,
        "system.network.errors": int(state.net_errors_recv + state.net_errors_send),
    }
    return metrics_by_scope, use_overview


def _send_metrics_with_scopes(
    client: OTLPClient, resource: dict, metrics_by_scope: dict[str, list]
) -> int:
    """Send metrics grouped by scope name. Returns total metric count sent."""
    total = 0
    for scope_name, metrics in metrics_by_scope.items():
        if not metrics:
            continue
        # Build the payload with the specific scope name
        payload = {
            "resourceMetrics": [
                {
                    "resource": resource,
                    "scopeMetrics": [
                        {
                            "scope": {"name": scope_name, "version": "0.115.0"},
                            "metrics": metrics,
                        }
                    ],
                }
            ]
        }
        client._send(f"{client.endpoint}/v1/metrics", payload, "metrics")
        total += len(metrics)
    return total


# ── Per-process metrics ──────────────────────────────────────────────────────


def _build_process_resource(host_cfg: dict, proc_state: ProcessState) -> dict:
    """Build OTLP resource for an individual process (host attrs + process attrs)."""
    t = proc_state.template
    attrs = {}
    # Carry over host identification attributes
    for key in [
        "host.name",
        "host.id",
        "host.arch",
        "os.type",
        "os.description",
        "cloud.provider",
        "cloud.region",
    ]:
        if key in host_cfg:
            attrs[key] = host_cfg[key]

    # Process-specific resource attributes
    attrs["process.pid"] = proc_state.pid
    attrs["process.executable.name"] = t["executable.name"]
    attrs["process.executable.path"] = t["executable.path"]
    attrs["process.command"] = t["command"]
    attrs["process.command_line"] = t["command_line"]
    attrs["process.owner"] = t["owner"]
    attrs["process.parent_pid"] = 1 if proc_state.pid != 1 else 0

    attrs["telemetry.sdk.name"] = "opentelemetry"
    attrs["telemetry.sdk.version"] = "1.24.0"
    attrs["telemetry.sdk.language"] = "python"
    attrs["data_stream.type"] = "metrics"
    attrs["data_stream.dataset"] = "hostmetricsreceiver"
    attrs["data_stream.namespace"] = "default"

    return {
        "attributes": _format_attributes(attrs),
        "schemaUrl": SCHEMA_URL,
    }


def _generate_process_metrics(
    proc_state: ProcessState,
    rng: random.Random,
    fd_pressure: float = 0.0,
    virtual_memory_multiplier: float = 1.0,
) -> list[dict]:
    """Generate OTel process.* metrics for a single process.

    fd_pressure: 0.0-1.0 — scales open FD count from normal range toward
        the per-template fd_limit (default: fd_range[1] * 100), modeling
        a container nofile ulimit that is too low.
    virtual_memory_multiplier: >1 scales process.memory.virtual to model
        mmap growth (vm.max_map_count / SSTable mmap scenarios).
    """
    proc_state.tick()
    t = proc_state.template
    metrics = []

    # process.cpu.time — cumulative seconds by state (user, system)
    metrics.append(
        _build_sum_metric(
            "process.cpu.time",
            proc_state.cpu_time_user,
            "s",
            attributes={"state": "user"},
        )
    )
    metrics.append(
        _build_sum_metric(
            "process.cpu.time",
            proc_state.cpu_time_system,
            "s",
            attributes={"state": "system"},
        )
    )

    # process.cpu.utilization — instantaneous ratio (0..1)
    cpu_util = t["cpu_weight"] * rng.uniform(0.3, 1.5)
    cpu_util = min(cpu_util, 1.0)
    metrics.append(_build_gauge_metric("process.cpu.utilization", cpu_util, "1"))

    # process.memory.usage — resident memory (bytes)
    lo, hi = t["mem_bytes_range"]
    metrics.append(
        _build_gauge_metric(
            "process.memory.usage",
            rng.randint(lo, hi),
            "By",
            is_int=True,
        )
    )

    # process.memory.virtual — virtual memory size (bytes)
    # virtual_memory_multiplier scales this to model mmap growth (SSTable / vm.max_map_count fault).
    lo, hi = t["virtual_bytes_range"]
    _vmul = max(1.0, virtual_memory_multiplier)
    _virt_raw = rng.randint(lo, hi)
    metrics.append(
        _build_gauge_metric(
            "process.memory.virtual",
            int(_virt_raw * _vmul),
            "By",
            is_int=True,
        )
    )

    # process.threads — thread count
    lo, hi = t["threads_range"]
    metrics.append(
        _build_gauge_metric(
            "process.threads",
            rng.randint(lo, hi),
            "{thread}",
            is_int=True,
        )
    )

    # process.open_file_descriptors — open FD count
    # fd_pressure: 0 → normal range; >0 → ramps toward fd_limit ceiling,
    # modeling a container nofile ulimit that is too low for the workload.
    lo, hi = t["fd_range"]
    if fd_pressure > 0.0:
        fd_limit = t.get("fd_limit", hi * 100)
        fd_target = hi + (fd_limit - hi) * fd_pressure
        fd_val = int(fd_target * rng.uniform(0.95, 1.02))
        fd_val = min(fd_val, fd_limit)
    else:
        fd_val = rng.randint(lo, hi)
    metrics.append(
        _build_gauge_metric(
            "process.open_file_descriptors",
            fd_val,
            "{count}",
            is_int=True,
        )
    )

    # process.disk.io — cumulative bytes by direction
    metrics.append(
        _build_sum_metric(
            "process.disk.io",
            proc_state.disk_read,
            "By",
            attributes={"direction": "read"},
        )
    )
    metrics.append(
        _build_sum_metric(
            "process.disk.io",
            proc_state.disk_write,
            "By",
            attributes={"direction": "write"},
        )
    )

    # process.context_switches — cumulative by type
    metrics.append(
        _build_sum_metric(
            "process.context_switches",
            proc_state.ctx_voluntary,
            "{count}",
            attributes={"type": "voluntary"},
            is_int=True,
        )
    )
    metrics.append(
        _build_sum_metric(
            "process.context_switches",
            proc_state.ctx_involuntary,
            "{count}",
            attributes={"type": "involuntary"},
            is_int=True,
        )
    )

    return metrics


def _send_process_metrics(
    client: OTLPClient,
    host_cfg: dict,
    proc_states: list[ProcessState],
    rng: random.Random,
    fd_pressure: float = 0.0,
    virtual_memory_multiplier: float = 1.0,
) -> int:
    """Generate and send per-process metrics for one host. Returns metric count."""
    scope_name = SCRAPERS["process"]
    resource_metrics = []
    total = 0

    for ps in proc_states:
        metrics = _generate_process_metrics(
            ps, rng,
            fd_pressure=fd_pressure,
            virtual_memory_multiplier=virtual_memory_multiplier,
        )
        resource_metrics.append(
            {
                "resource": _build_process_resource(host_cfg, ps),
                "scopeMetrics": [
                    {
                        "scope": {"name": scope_name, "version": "0.115.0"},
                        "metrics": metrics,
                    }
                ],
            }
        )
        total += len(metrics)

    if resource_metrics:
        payload = {"resourceMetrics": resource_metrics}
        client._send(f"{client.endpoint}/v1/metrics", payload, "metrics")

    return total


# ── Run loop (used by ServiceManager and standalone) ──────────────────────────

from generators.host_metrics import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop

def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run host metrics generator loop until stop_event is set."""
    rng = random.Random()

    hosts = scenario_data["hosts"] if scenario_data else HOSTS
    if scenario_data:
        hosts = _with_infra_hosts(hosts, scenario_data.get("namespace") or "")
    # Per-deployment provider → cluster-name map; fall back to module global for standalone mode.
    _cluster_by_provider = (
        {c["provider"]: c["name"] for c in scenario_data.get("k8s_clusters", [])}
        if scenario_data else CLUSTER_BY_PROVIDER
    )
    # Build cloud_provider -> host_name mapping for targeted spikes
    _host_cloud_map: dict[str, str] = {}
    for h in hosts:
        _host_cloud_map[h["host.name"]] = h.get("cloud.provider", "")
    # Build service -> cloud_provider mapping from scenario_data
    _service_cloud: dict[str, str] = {}
    _channel_registry = {}
    if scenario_data:
        for svc_name, svc_cfg in scenario_data.get("services", {}).items():
            _service_cloud[svc_name] = svc_cfg.get("cloud_provider", "")
        _channel_registry = scenario_data.get("channel_registry", {})

    # Build resources and metric state for each host
    host_resources = []
    host_states = []
    host_proc_states: list[list[ProcessState]] = []
    for host_idx, host_cfg in enumerate(hosts):
        resource = _build_host_resource(host_cfg, _cluster_by_provider)
        state = HostMetricState(
            cpu_count=host_cfg["cpu_count"],
            mem_total=host_cfg["memory_total_bytes"],
            disk_total=host_cfg["disk_total_bytes"],
            rng=rng,
        )
        host_resources.append(resource)
        host_states.append(state)
        # Per-process states for this host
        proc_states = [ProcessState(t, host_idx, rng) for t in PROCESS_TEMPLATES]
        host_proc_states.append(proc_states)

    total_metrics = 0
    scrape_count = 0
    proc_count = len(PROCESS_TEMPLATES) * len(hosts)

    logger.info(
        "Host metrics generator started (interval=%ds, hosts=%d, processes=%d)",
        METRICS_INTERVAL,
        len(hosts),
        proc_count,
    )

    es_bulk = ESBulkClient()

    def _emit_tick() -> None:
        nonlocal host_cfg, proc_states, resource, scrape_count, state, total_metrics
        spikes = chaos_controller.get_infra_spikes() if chaos_controller else {}
        cpu_pct = spikes.get("cpu_pct", 0)
        mem_pct = spikes.get("memory_pct", 0)
        disk_pct = spikes.get("node_disk_fill_pct", 0)

        # Resolve generator_impact keys declared in channel YAML.
        _host_chaos: dict | None = None
        if chaos_controller and _channel_registry:
            from generators.host_metrics import SPEC as _HOST_SPEC
            _host_chaos = _resolve_active_chaos(
                chaos_controller, _channel_registry, "host_metrics",
                chaos_mode="any", impact_keys=_HOST_SPEC.impact_keys,
            )

        _chaos_fd_pressure: float = (_host_chaos or {}).get("fd_pressure", 0.0) or 0.0
        _chaos_fs_pct: float | None = (_host_chaos or {}).get("filesystem_pct")
        _chaos_disk_mul: float = (_host_chaos or {}).get("disk_io_multiplier", 1.0) or 1.0
        _chaos_vmem_mul: float = (_host_chaos or {}).get("virtual_memory_multiplier", 1.0) or 1.0
        _chaos_net_mul: float = (_host_chaos or {}).get("network_connections_multiplier", 1.0) or 1.0

        # Build set of cloud providers with active faults (for targeting)
        spiked_clouds: set[str] = set()
        has_active_faults = False
        if chaos_controller and (cpu_pct > 0 or mem_pct > 0 or disk_pct > 0):
            active_channels = chaos_controller.get_active_channels()
            if active_channels:
                has_active_faults = True
                for ch_id in active_channels:
                    ch = _channel_registry.get(ch_id, {})
                    for svc in ch.get("affected_services", []):
                        cloud = _service_cloud.get(svc)
                        if cloud:
                            spiked_clouds.add(cloud)

        batch_metrics = 0
        for host_cfg, resource, state, proc_states in zip(
            hosts,
            host_resources,
            host_states,
            host_proc_states,
        ):
            host_name = host_cfg["host.name"]
            host_cloud = _host_cloud_map.get(host_name, "")
            # Spike this host if: sliders active AND (no faults → spike all, or this host's cloud is affected)
            if (cpu_pct > 0 or mem_pct > 0) and (
                not has_active_faults or host_cloud in spiked_clouds
            ):
                host_cpu = cpu_pct
                host_mem = mem_pct
            else:
                host_cpu = 0
                host_mem = 0

            # Propagate filesystem_pct target onto state so the slow walk ramps correctly.
            # Converting from 0-100 percentage to 0.0-1.0 fraction.
            # Setting to None clears the fault target and lets the walk drift freely.
            # The Chaos UI node disk fill slider fills the hosts of the active channels' clouds
            # (the same machines as the Kubernetes nodes), unless the channel sets its own.
            _fs_pct = _chaos_fs_pct
            if _fs_pct is None and disk_pct > 0 and has_active_faults and host_cloud in spiked_clouds:
                _fs_pct = disk_pct
            for _, mp, _, _ in _FS_MOUNTS:
                state.fs_target[mp] = (_fs_pct / 100.0) if _fs_pct is not None else None

            metrics_by_scope, _use_overview = _generate_host_metrics(
                state, rng,
                cpu_spike_pct=host_cpu,
                memory_spike_pct=host_mem,
                disk_io_multiplier=_chaos_disk_mul,
                network_connections_multiplier=_chaos_net_mul,
            )
            sent = _send_metrics_with_scopes(client, resource, metrics_by_scope)
            batch_metrics += sent
            if es_bulk.configured:
                # Full Host Details companion when OTLP never indexes (ALERTS #103).
                # Covers Systems Ops USE tiles too (load / filesystem / network).
                bulk_docs = build_hostmetrics_bulk_docs(resource, metrics_by_scope)
                if bulk_docs:
                    es_bulk.send_bulk(
                        "metrics-hostmetricsreceiver.otel-default",
                        bulk_docs,
                        params={"pipeline": "_none"},
                    )
            # Per-process metrics
            sent = _send_process_metrics(
                client, host_cfg, proc_states, rng,
                fd_pressure=_chaos_fd_pressure,
                virtual_memory_multiplier=_chaos_vmem_mul,
            )
            batch_metrics += sent

        scrape_count += 1
        total_metrics += batch_metrics
        logger.info(
            "Scrape %d: sent %d metrics across %d hosts + %d processes (total=%d)",
            scrape_count,
            batch_metrics,
            len(hosts),
            proc_count,
            total_metrics,
        )

    run_tick_loop(stop_event, _emit_tick, interval=lambda: METRICS_INTERVAL)

    logger.info(
        "Host metrics generator stopped. Total: %d metrics in %d scrapes",
        total_metrics,
        scrape_count,
    )


# ── Standalone entry point ────────────────────────────────────────────────────
def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    client = OTLPClient()
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    duration = int(os.environ.get("RUN_DURATION", "60"))
    timer = threading.Timer(duration, stop_event.set)
    timer.daemon = True
    timer.start()
    logger.info("Running for %ds (standalone mode)", duration)

    run(client, stop_event)
    timer.cancel()
    client.close()


if __name__ == "__main__":
    main()
