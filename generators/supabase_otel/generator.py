#!/usr/bin/env python3
"""Supabase Generator — sends Supabase Prometheus metrics via OTLP.

Emits data matching the supabase_otel content package
(discovery.datasets: supabase.metrics.otel).

Metric names are grounded from the three Elastic package dashboards:
  - [Supabase OTel] Node & Infrastructure
  - [Supabase OTel] Databases & PostgreSQL
  - [Supabase OTel] Services & API

Prometheus label conventions (from Supabase Metrics API samples):
  - ``supabase_project_ref`` and ``service_type`` on every series
  - ``node_vmstat_*`` is Prometheus ``untyped`` → OTLP gauge (AVG_OVER_TIME)
  - ``node_cpu_seconds_total`` is a counter (RATE); idle must stay < 1.0/s

Usage (standalone):
    python3 -m generators.supabase_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/supabase_otel@0.4.0"
# Realtime connection panels read attributes.supabase.realtime.connected,
# attributes.supabase.realtime.connected_cluster and attributes.supabase.realtime.`limit`
# from realtime_logs "Billing metrics" records (see _generate_service_logs).

import logging
import os
import random
import signal
import threading
from typing import Any

from app.telemetry import OTLPClient
from app.config import NAMESPACE
from generator_engine.db_common import (
    CounterState,
    build_cumulative_sum,
    build_gauge,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

SUPABASE_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
)
SUPABASE_LOG_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("supabase-generator")
EMIT_INTERVAL_S = 15.0
# Cold start / post-mapping-reset: emit a few ticks quickly so RATE() and
# TBUCKET charts are non-empty before the first full scrape interval.
_WARMUP_TICKS = 3
_WARMUP_INTERVAL_S = 1.0
_METRICS_DATA_STREAM = "metrics-supabase.metrics.otel-default"
_VMSTAT_GAUGE_FIELDS = (
    "metrics.node_vmstat_pgpgin",
    "metrics.node_vmstat_pgpgout",
    "metrics.node_vmstat_pgfault",
    "metrics.node_vmstat_pgmajfault",
    "metrics.node_vmstat_oom_kill",
)

# Real node_exporter mode set; util panels filter idle / user / system / iowait.
CPU_MODES = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")
CPU_IDS = ("0", "1")
FS_MOUNTS = (
    {"device": "nvme0n1p1", "mountpoint": "/"},
    {"device": "nvme0n1p2", "mountpoint": "/var/lib/postgresql"},
)
DISK_DEVICE = "nvme0n1"
NET_DEVICE = "eth0"
_GIB = 1024 ** 3
_FS_SIZE_BYTES = {"/": 100 * _GIB, "/var/lib/postgresql": 500 * _GIB}
_PG_MOUNT = "/var/lib/postgresql"
_HTTP_CODES = ("200", "201", "204", "400", "401", "404", "429", "500", "502")
_MAX_CONNECTIONS = 100
_SUPAVISOR_POOL_SIZE = 20

# One impact key per package alert (supabase_otel@0.4.0); 0 means the fault is off.
# Counter-style faults (deadlocks, restarts, timeouts, OOM kills) add that many per emit, and
# a healthy run never moves those counters, because INCREASE(...) > 0 is the alert.
_FAULT_KEYS = (
    "auth_api_server_errors",
    "auth_service_is_down",
    "disk_io_saturation",
    "filesystem_nearly_full",
    "filesystem_is_read_only",
    "host_cpu_saturation",
    "host_memory_pressure",
    "host_oom_kill_detected",
    "long_running_transaction",
    "metrics_collection_is_failing",
    "pgbouncer_clients_queueing_for_connections",
    "pgbouncer_is_down",
    "postgresql_connection_budget_nearly_exhausted",
    "postgresql_deadlocks_detected",
    "postgresql_is_down",
    "postgresql_restarted",
    "postgrest_database_pool_timeouts",
    "replication_lag_is_high",
    "replication_slot_retaining_wal",
    "supavisor_pool_exhausted",
)


def _faults(chaos: dict | None) -> dict[str, float]:
    return {key: float((chaos or {}).get(key) or 0.0) for key in _FAULT_KEYS}


# Metric names queried by the supabase_otel dashboards and alert rules.
METRIC_NAMES = (
    # Node — CPU
    "node_cpu_seconds_total",
    "node_cpu_guest_seconds_total",
    # Node — memory
    "node_memory_MemTotal_bytes",
    "node_memory_Active_bytes",
    "node_memory_Inactive_bytes",
    "node_memory_Cached_bytes",
    "node_memory_Dirty_bytes",
    "node_memory_Slab_bytes",
    "node_memory_Mapped_bytes",
    "node_memory_AnonPages_bytes",
    # Node — vmstat
    "node_vmstat_pgpgin",
    "node_vmstat_pgpgout",
    "node_vmstat_pgfault",
    "node_vmstat_pgmajfault",
    "node_vmstat_oom_kill",
    # Node — network
    "node_network_receive_packets_total",
    "node_network_transmit_packets_total",
    "node_network_receive_errs_total",
    "node_network_transmit_errs_total",
    "node_network_receive_drop_total",
    "node_network_transmit_drop_total",
    # Node — disk
    "node_disk_read_bytes_total",
    "node_disk_written_bytes_total",
    "node_disk_reads_completed_total",
    "node_disk_writes_completed_total",
    "node_disk_io_time_seconds_total",
    "node_disk_discard_time_seconds_total",
    "node_disk_reads_merged_total",
    "node_disk_discards_merged_total",
    # Node — filesystem
    "node_filesystem_free_bytes",
    "node_filesystem_files",
    "node_filesystem_files_free",
    "node_filesystem_readonly",
    # PostgreSQL
    "pg_stat_database_num_backends",
    "pg_stat_database_xact_rollback_total",
    "pg_stat_database_tup_fetched_total",
    "pg_stat_database_tup_inserted_total",
    "pg_stat_database_tup_updated_total",
    "pg_stat_database_temp_bytes_total",
    "pg_stat_database_blks_hit_total",
    "pg_stat_database_conflicts_confl_lock_total",
    "pg_stat_database_conflicts_confl_deadlock_total",
    "pg_stat_database_conflicts_confl_snapshot_total",
    "pg_database_size_bytes",
    "pg_stat_statements_total_queries",
    "pg_stat_statements_total_time_seconds",
    "pg_stat_bgwriter_checkpoints_timed_total",
    "pg_stat_bgwriter_maxwritten_clean_total",
    "pg_stat_bgwriter_checkpoint_sync_time_total",
    "pg_wal_size_mb",
    "pg_ls_archive_statusdir_wal_pending_count",
    "pg_status_in_recovery",
    "physical_replication_lag_physical_replication_lag_seconds",
    "physical_replication_lag_is_wal_replay_paused",
    # PgBouncer
    "pgbouncer_pools_server_active_connections",
    "pgbouncer_pools_client_waiting_connections",
    "pgbouncer_pools_server_testing_connections",
    "pgbouncer_pools_client_maxwait_seconds",
    "pgbouncer_stats_sql_transactions_pooled_total",
    "pgbouncer_stats_received_bytes_total",
    "pgbouncer_stats_client_wait_seconds_total",
    "pgbouncer_stats_server_in_transaction_seconds_total",
    "pgbouncer_databases_max_connections",
    "pgbouncer_databases_pool_size",
    "pgbouncer_databases_disabled",
    "pgbouncer_config_max_client_connections",
    "pgbouncer_used_clients",
    "pgbouncer_free_clients",
    # Go sql connection pool (auth / realtime services)
    "db_sql_connection_max_open",
    "db_sql_connection_wait_duration_milliseconds_total",
    "db_sql_connection_closed_max_lifetime_total",
    "db_sql_connection_closed_max_idle_time_total",
    "db_sql_connection_closed_max_idle_total",
    # PostgREST / services
    "pgrst_db_pool_available",
    "pgrst_db_pool_timeouts_total",
    "pgrst_jwt_cache_hits_total",
    "pgrst_jwt_cache_requests_total",
    "pgrst_schema_cache_loads_total",
    "pgrst_schema_cache_query_time_seconds",
    "go_memstats_alloc_bytes",
    "go_memstats_sys_bytes",
    "go_memstats_heap_objects",
    "go_memstats_lookups_total",
    "process_resident_memory_bytes",
    "process_virtual_memory_bytes",
    "process_runtime_go_goroutines",
    "process_max_fds",
    "process_runtime_go_gc_pause_total_ns_total",
    "process_runtime_go_gc_count_total",
    "realtime_postgres_changes_total_subscriptions",
    "direct_connection_stats_connection_count",
    "auth_users_user_count",
    "global_auth_version_major",
    "global_auth_version_patch",
    "postgresql_restarts_total",
    # Alert rules (supabase_otel@0.4.0)
    "http_status_codes_total",
    "gotrue_running",
    "node_disk_io_time_weighted_seconds_total",
    "node_filesystem_avail_bytes",
    "node_filesystem_size_bytes",
    "node_filesystem_device_error",
    "node_memory_MemAvailable_bytes",
    "node_memory_SwapTotal_bytes",
    "node_memory_SwapFree_bytes",
    "pg_stat_activity_xact_runtime",
    "up",
    "pg_scrape_collector_success",
    "node_scrape_collector_success",
    "pg_exporter_last_scrape_error",
    "pgbouncer_up",
    "max_connections_connection_count",
    "connection_stats_connection_count",
    "pg_stat_database_deadlocks_total",
    "pg_stat_database_xact_commit_total",
    "pg_up",
    "pgrst_db_pool_waiting",
    "pg_stat_replication_replay_lag",
    "physical_replication_lag_is_connected_to_primary",
    "replication_slots_max_lag_bytes",
    "supavisor_pool_connections_checked_out",
    "supavisor_pool_connections_idle",
    "supavisor_connections_active",
)


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_ec2",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


class _SupabaseState(CounterState):
    """Cumulative counters for Supabase Prometheus-style metrics."""

    def __init__(self, rng: random.Random):
        super().__init__(rng)
        # CPU seconds per (cpu, mode).  Keep values consistent with
        # start_time_ns (~60s ago): huge initials make RATE(idle) >> 1 and the
        # util panel clamps to 0% via ``Idle Rate >= 1 → 0.0``.
        prior_s = 60.0
        self.cpu: dict[tuple[str, str], float] = {}
        for cpu in CPU_IDS:
            idle0 = prior_s * rng.uniform(0.55, 0.75)
            user0 = prior_s * rng.uniform(0.10, 0.20)
            system0 = prior_s * rng.uniform(0.05, 0.12)
            iowait0 = max(0.0, prior_s - idle0 - user0 - system0) * 0.7
            other0 = max(0.0, prior_s - idle0 - user0 - system0 - iowait0)
            self.cpu[(cpu, "idle")] = idle0
            self.cpu[(cpu, "user")] = user0
            self.cpu[(cpu, "system")] = system0
            self.cpu[(cpu, "iowait")] = iowait0
            self.cpu[(cpu, "nice")] = other0 * 0.4
            self.cpu[(cpu, "irq")] = other0 * 0.2
            self.cpu[(cpu, "softirq")] = other0 * 0.3
            self.cpu[(cpu, "steal")] = other0 * 0.1
        self.cpu_guest: dict[tuple[str, str], float] = {
            (cpu, mode): rng.uniform(0.0, 1.0)
            for cpu in CPU_IDS
            for mode in ("user", "nice")
        }
        # vmstat is Prometheus untyped → gauge; values still climb like /proc/vmstat.
        self.pgpgin = rng.randint(1_000_000, 50_000_000)
        self.pgpgout = rng.randint(1_000_000, 50_000_000)
        self.pgfault = rng.randint(10_000_000, 500_000_000)
        self.pgmajfault = rng.randint(1_000, 100_000)
        self.oom_kill = 0
        self.net_rx_pkts = rng.randint(10_000_000, 500_000_000)
        self.net_tx_pkts = rng.randint(10_000_000, 500_000_000)
        self.net_rx_errs = rng.randint(0, 100)
        self.net_tx_errs = rng.randint(0, 100)
        self.net_rx_drop = rng.randint(0, 50)
        self.net_tx_drop = rng.randint(0, 50)
        self.disk_read_bytes = rng.randint(10_000_000_000, 500_000_000_000)
        self.disk_written_bytes = rng.randint(5_000_000_000, 200_000_000_000)
        self.disk_reads = rng.randint(1_000_000, 50_000_000)
        self.disk_writes = rng.randint(500_000, 20_000_000)
        self.disk_io_time = float(rng.randint(1_000, 50_000))
        self.disk_weighted_time = self.disk_io_time * 1.5
        self.disk_discard_time = float(rng.randint(10, 1_000))
        self.disk_reads_merged = rng.randint(100_000, 5_000_000)
        self.disk_discards_merged = rng.randint(1_000, 100_000)
        # PostgreSQL
        self.xact_rollback = rng.randint(100, 50_000)
        self.tup_fetched = rng.randint(1_000_000, 100_000_000)
        self.tup_inserted = rng.randint(100_000, 10_000_000)
        self.tup_updated = rng.randint(50_000, 5_000_000)
        self.temp_bytes = rng.randint(1_000_000, 500_000_000)
        self.blks_hit = rng.randint(10_000_000, 1_000_000_000)
        self.confl_lock = rng.randint(0, 100)
        self.confl_deadlock = rng.randint(0, 20)
        self.deadlocks = rng.randint(0, 5)
        self.xact_commit = rng.randint(1_000, 1_000_000)
        self.http_codes = {code: rng.randint(100, 5_000) for code in _HTTP_CODES}
        self.confl_snapshot = rng.randint(0, 50)
        self.statements_queries = rng.randint(1_000_000, 50_000_000)
        self.statements_time = float(rng.randint(10_000, 500_000))
        self.checkpoints_timed = rng.randint(1_000, 50_000)
        self.maxwritten_clean = rng.randint(100, 5_000)
        self.checkpoint_sync_time = float(rng.randint(100, 10_000))
        self.wal_pending = rng.randint(0, 500)
        # PgBouncer
        self.txns_pooled = rng.randint(100_000, 10_000_000)
        self.received_bytes = rng.randint(1_000_000_000, 100_000_000_000)
        self.client_wait_seconds = float(rng.randint(10, 5_000))
        self.server_in_txn_seconds = float(rng.randint(1_000, 100_000))
        # sql / postgrest / go
        self.conn_wait_ms = float(rng.randint(1_000, 100_000))
        self.closed_max_lifetime = rng.randint(100, 10_000)
        self.closed_max_idle_time = rng.randint(100, 10_000)
        self.closed_max_idle = rng.randint(100, 10_000)
        self.pool_timeouts = rng.randint(0, 500)
        self.jwt_hits = rng.randint(100_000, 10_000_000)
        self.jwt_requests = rng.randint(100_000, 10_000_000)
        self.schema_cache_loads = rng.randint(1, 50)
        self.go_lookups = rng.randint(1_000_000, 50_000_000)
        self.gc_pause_ns = rng.randint(1_000_000_000, 50_000_000_000)
        self.gc_count = rng.randint(1_000, 100_000)
        # INCREASE(postgresql_restarts_total) > 0 is the alert: a healthy run starts at 0.
        self.pg_restarts = 0

    def tick(
        self,
        rng: random.Random,
        load: float,
        faults: dict,
        wall: float | None = None,
    ) -> None:
        # CPU: wall-clock seconds split across modes.  Idle must stay < 1.0/s
        # so ``1 - RATE(idle)`` util panels stay non-zero.  ``wall`` must match
        # the real sleep between emits (warmup uses a shorter interval).
        wall = EMIT_INTERVAL_S if wall is None else wall
        cpu_fault = faults.get("host_cpu_saturation", 0.0)
        idle_frac = max(0.0, 1.0 - cpu_fault) if cpu_fault > 0 else rng.uniform(0.45, 0.70)
        busy = 1.0 - idle_frac
        user_frac = busy * 0.50
        system_frac = busy * 0.28
        iowait_frac = busy * 0.15
        nice_frac = busy * 0.03
        irq_frac = busy * 0.02
        softirq_frac = busy * 0.015
        steal_frac = max(0.0, busy - user_frac - system_frac - iowait_frac
                         - nice_frac - irq_frac - softirq_frac)
        for cpu in CPU_IDS:
            self.cpu[(cpu, "idle")] += wall * idle_frac
            self.cpu[(cpu, "user")] += wall * user_frac
            self.cpu[(cpu, "system")] += wall * system_frac
            self.cpu[(cpu, "iowait")] += wall * iowait_frac
            self.cpu[(cpu, "nice")] += wall * nice_frac
            self.cpu[(cpu, "irq")] += wall * irq_frac
            self.cpu[(cpu, "softirq")] += wall * softirq_frac
            self.cpu[(cpu, "steal")] += wall * steal_frac
        for cpu in CPU_IDS:
            self.cpu_guest[(cpu, "user")] += wall * rng.uniform(0.0, 0.01)
            self.cpu_guest[(cpu, "nice")] += wall * rng.uniform(0.0, 0.005)

        scale = wall / EMIT_INTERVAL_S
        self.pgpgin += int(rng.randint(100, 5_000) * load * scale)
        self.pgpgout += int(rng.randint(100, 5_000) * load * scale)
        self.pgfault += int(rng.randint(1_000, 50_000) * load * scale)
        self.pgmajfault += int(rng.randint(0, 20) * scale)
        # DELTA(node_vmstat_oom_kill) > 0 is the alert; a healthy run never kills.
        self.oom_kill += int(faults.get("host_oom_kill_detected", 0.0))

        self.net_rx_pkts += int(rng.randint(1_000, 50_000) * load * scale)
        self.net_tx_pkts += int(rng.randint(1_000, 40_000) * load * scale)
        self.net_rx_errs += rng.randint(0, 1) if scale >= 1.0 else 0
        self.net_tx_errs += rng.randint(0, 1) if scale >= 1.0 else 0

        self.disk_read_bytes += int(rng.randint(100_000, 5_000_000) * load * scale)
        self.disk_written_bytes += int(rng.randint(50_000, 2_000_000) * load * scale)
        self.disk_reads += int(rng.randint(10, 500) * load * scale)
        self.disk_writes += int(rng.randint(5, 200) * load * scale)
        io_busy = faults.get("disk_io_saturation", 0.0)
        io_delta = wall * io_busy if io_busy > 0 else rng.uniform(0.01, 0.5) * load * scale
        self.disk_io_time += io_delta
        self.disk_weighted_time += io_delta * (6.0 if io_busy > 0 else rng.uniform(1.0, 3.0))
        self.disk_discard_time += rng.uniform(0.0, 0.05) * scale
        self.disk_reads_merged += int(rng.randint(1, 50) * load * scale)
        self.disk_discards_merged += rng.randint(0, 5) if scale >= 1.0 else 0

        self.xact_rollback += int(rng.randint(0, 5) * scale)
        self.xact_commit += int(rng.randint(100, 5_000) * load * scale)
        self.deadlocks += int(faults.get("postgresql_deadlocks_detected", 0.0))
        self.pg_restarts += int(faults.get("postgresql_restarted", 0.0))
        self.tup_fetched += int(rng.randint(1_000, 50_000) * load * scale)
        self.tup_inserted += int(rng.randint(50, 2_000) * load * scale)
        self.tup_updated += int(rng.randint(20, 1_000) * load * scale)
        self.temp_bytes += int(rng.randint(0, 100_000) * scale)
        self.blks_hit += int(rng.randint(5_000, 100_000) * load * scale)
        self.confl_lock += int(rng.randint(0, 2) * scale)
        self.confl_deadlock += int(rng.randint(0, 1) * scale)
        self.confl_snapshot += int(rng.randint(0, 1) * scale)
        self.statements_queries += int(rng.randint(100, 5_000) * load * scale)
        self.statements_time += rng.uniform(0.5, 20.0) * load * scale
        self.checkpoints_timed += rng.randint(0, 2) if scale >= 1.0 else 0
        self.maxwritten_clean += rng.randint(0, 1) if scale >= 1.0 else 0
        self.checkpoint_sync_time += rng.uniform(0.0, 5.0) * scale
        self.wal_pending += rng.randint(0, 3) if scale >= 1.0 else 0

        self.txns_pooled += int(rng.randint(50, 2_000) * load * scale)
        self.received_bytes += int(rng.randint(100_000, 5_000_000) * load * scale)
        self.client_wait_seconds += rng.uniform(0.0, 0.05) * scale
        self.server_in_txn_seconds += rng.uniform(1.0, 30.0) * load * scale

        self.conn_wait_ms += rng.uniform(1.0, 50.0) * load * scale
        self.closed_max_lifetime += rng.randint(0, 2) if scale >= 1.0 else 0
        self.closed_max_idle_time += rng.randint(0, 2) if scale >= 1.0 else 0
        self.closed_max_idle += rng.randint(0, 2) if scale >= 1.0 else 0
        # INCREASE(pgrst_db_pool_timeouts_total) > 0 is the alert: only the fault adds timeouts.
        self.pool_timeouts += int(faults.get("postgrest_database_pool_timeouts", 0.0))

        # Auth API requests by status code (the "Auth API server errors" alert).
        requests = int(rng.randint(50, 400) * load * scale)
        p_5xx = faults.get("auth_api_server_errors", 0.0) or 0.003
        n_5xx = int(round(requests * p_5xx))
        n_4xx = int(requests * 0.04)
        self.http_codes["500"] += n_5xx - n_5xx // 2
        self.http_codes["502"] += n_5xx // 2
        for code, share in (("400", 0.4), ("401", 0.3), ("404", 0.25), ("429", 0.05)):
            self.http_codes[code] += int(n_4xx * share)
        ok = max(0, requests - n_5xx - n_4xx)
        self.http_codes["200"] += int(ok * 0.9)
        self.http_codes["201"] += int(ok * 0.07)
        self.http_codes["204"] += ok - int(ok * 0.9) - int(ok * 0.07)
        jwt_delta = int(rng.randint(50, 2_000) * load * scale)
        self.jwt_requests += jwt_delta
        self.jwt_hits += int(jwt_delta * rng.uniform(0.85, 0.98))
        if rng.random() < 0.05 * scale:
            self.schema_cache_loads += 1
        self.go_lookups += int(rng.randint(100, 5_000) * load * scale)
        self.gc_pause_ns += int(rng.randint(100_000, 5_000_000) * load * scale)
        self.gc_count += rng.randint(0, 3) if scale >= 1.0 else 0


def _common_attrs(
    project_ref: str,
    service_type: str = "db",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Prometheus labels present on every Supabase Metrics API series."""
    attrs: dict[str, Any] = {
        "supabase_project_ref": project_ref,
        "service_type": service_type,
    }
    if extra:
        attrs.update(extra)
    return attrs


def _sum(
    name: str,
    unit: str,
    value: int | float,
    start: str,
    attrs: dict | None = None,
) -> dict:
    return build_cumulative_sum(name, unit, value, start, attrs)


def _gauge(
    name: str,
    unit: str,
    value: int | float,
    attrs: dict | None = None,
) -> dict:
    return build_gauge(name, unit, value, attrs)


def _vmstat_mapped_as_counter(elastic_url: str, api_key: str) -> bool:
    """Return True if any node_vmstat_* field is stuck as counter_long."""
    import httpx

    url = (
        f"{elastic_url.rstrip('/')}/{_METRICS_DATA_STREAM}/_field_caps"
        f"?fields={','.join(_VMSTAT_GAUGE_FIELDS)}"
    )
    try:
        resp = httpx.get(
            url,
            headers={
                "Authorization": f"ApiKey {api_key}",
                "Content-Type": "application/json",
            },
            timeout=15,
        )
    except Exception as exc:
        logger.warning("Could not inspect %s field caps: %s", _METRICS_DATA_STREAM, exc)
        return False
    if resp.status_code == 404:
        return False
    if resp.status_code >= 300:
        logger.warning(
            "field_caps for %s failed (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )
        return False

    fields = resp.json().get("fields", {})
    for name in _VMSTAT_GAUGE_FIELDS:
        for meta in fields.get(name, {}).values():
            if meta.get("time_series_metric") == "counter":
                return True
    return False


def _reset_vmstat_counter_mapping() -> bool:
    """Delete the metrics stream if vmstat fields are stuck as counters.

    Dashboard AVG_OVER_TIME panels reject counter_long.  Prometheus untyped
    vmstat metrics must map as gauges; the mapping is sticky on the backing
    index after an earlier cumulative-sum emit.

    Returns True when the stream was deleted (caller should warm up emits).
    """
    try:
        from app.config import ELASTIC_API_KEY, ELASTIC_URL
        from app.telemetry import ESBulkClient
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for vmstat mapping check: %s", exc)
        return False

    if not (ELASTIC_URL and ELASTIC_API_KEY):
        return False

    try:
        client = ESBulkClient()
    except Exception as exc:
        logger.warning("Could not construct ESBulkClient for vmstat check: %s", exc)
        return False

    if not client.configured:
        return False

    if not _vmstat_mapped_as_counter(client.elastic_url, client.api_key):
        logger.info(
            "supabase vmstat fields are not mapped as counters; leaving %s alone",
            _METRICS_DATA_STREAM,
        )
        return False

    import httpx

    try:
        resp = httpx.delete(
            f"{client.elastic_url.rstrip('/')}/_data_stream/{_METRICS_DATA_STREAM}",
            headers={
                "Authorization": f"ApiKey {client.api_key}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )
    except Exception as exc:
        logger.warning("Could not delete %s: %s", _METRICS_DATA_STREAM, exc)
        return False

    if resp.status_code < 300:
        logger.info(
            "Deleted %s so node_vmstat_* remap as gauges (was counter)",
            _METRICS_DATA_STREAM,
        )
        return True
    if resp.status_code == 404:
        logger.info("Data stream %s did not exist", _METRICS_DATA_STREAM)
        return False
    logger.warning(
        "Could not delete %s: %s %s",
        _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
    )
    return False



# Ambient log errors so the error panels (Gateway 4xx, Top SQLSTATE codes) are not empty on a
# healthy project. No package alert reads the log streams, so these cannot trip one; 5xx and
# pool errors stay fault-only.
_BASELINE_EDGE_4XX_RATE = 0.04
_BASELINE_PG_ERROR_RATE = 0.03


def _generate_service_logs(
    rng: random.Random,
    project_ref: str,
    client: "OTLPClient",
    pool_exhaustion: bool,
    slow_rate: float,
    load: float,
) -> list[dict]:
    """Generate a batch of Supabase service log records covering all 9 services."""
    records: list[dict] = []

    def _r(body: str, sev: str = "INFO", attrs: dict | None = None) -> dict:
        return client.build_log_record(sev, body, attrs)

    http_methods = ("GET", "POST", "PUT", "DELETE", "PATCH")
    api_paths = (
        "/rest/v1/users", "/rest/v1/items", "/rest/v1/orders",
        "/storage/v1/object/bucket/file.png", "/storage/v1/bucket",
        "/realtime/v1/websocket", "/auth/v1/token", "/auth/v1/user",
        "/functions/v1/process-payment", "/functions/v1/send-email",
    )
    user_agents = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "axios/1.6.2",
        "python-requests/2.31.0",
        "Dart/3.0 (dart:io)",
    )

    # edge_logs — 3–8 requests per tick
    n_edge = rng.randint(5, 12) if pool_exhaustion else rng.randint(3, 8)
    for _ in range(n_edge):
        method = rng.choice(http_methods)
        path = rng.choice(api_paths)
        if pool_exhaustion and rng.random() < 0.4:
            status = rng.choice((500, 502, 503))
        elif slow_rate > 0.3 and rng.random() < 0.3:
            status = rng.choice((400, 401, 403, 404))
        elif rng.random() < _BASELINE_EDGE_4XX_RATE:
            status = rng.choice((400, 401, 403, 404))
        else:
            status = rng.choice((200, 200, 200, 200, 201, 204, 301))
        duration_ns = int(rng.uniform(5_000_000, 500_000_000) * (1 + slow_rate * 5))
        records.append(_r(
            f"{method} {path} → {status}",
            "WARN" if status >= 400 else "INFO",
            {
                "supabase.service": "edge_logs",
                "supabase_project_ref": project_ref,
                "http.request.method": method,
                "http.response.status_code": status,
                "url.path": path,
                "user_agent.original": rng.choice(user_agents),
                "supabase.event.duration": duration_ns,
            },
        ))

    # postgres_logs — 2–5 records per tick
    pg_severities = ("LOG", "INFO", "NOTICE", "WARNING")
    pg_error_severities = ("ERROR", "FATAL")
    pg_operations = ("SELECT", "INSERT", "UPDATE", "DELETE")
    pg_backend_types = ("client backend", "background worker", "autovacuum worker")
    sqlstates_ok = ("00000",)
    sqlstates_err = ("23505", "42P01", "40001", "55P03", "08006")
    pg_bodies = (
        "statement: SELECT * FROM users WHERE id = $1",
        "statement: INSERT INTO events (type, data) VALUES ($1, $2)",
        "autovacuum: found dead rows",
        "checkpoint starting: time",
        "database system is ready to accept connections",
    )
    n_pg = rng.randint(2, 5)
    for _ in range(n_pg):
        is_err = (pool_exhaustion and rng.random() < 0.2) or rng.random() < _BASELINE_PG_ERROR_RATE
        sev = rng.choice(pg_error_severities) if is_err else rng.choice(pg_severities)
        sqlstate = rng.choice(sqlstates_err) if is_err else rng.choice(sqlstates_ok)
        records.append(_r(
            rng.choice(pg_bodies),
            "ERROR" if is_err else "INFO",
            {
                "supabase.service": "postgres_logs",
                "supabase_project_ref": project_ref,
                "supabase.db.error_severity": sev,
                "db.response.status_code": sqlstate,
                "db.operation.name": rng.choice(pg_operations),
                "supabase.db.backend_type": rng.choice(pg_backend_types),
            },
        ))

    # edge_function_invocation_logs — 2–4 per tick
    function_names = ("process-payment", "send-email", "resize-image", "webhook-handler")
    function_ids = {
        "process-payment": ("fn-abc123", "deploy-v1-001"),
        "send-email": ("fn-def456", "deploy-v2-003"),
        "resize-image": ("fn-ghi789", "deploy-v1-007"),
        "webhook-handler": ("fn-jkl012", "deploy-v3-002"),
    }
    n_fn = rng.randint(2, 4)
    for _ in range(n_fn):
        fn_name = rng.choice(function_names)
        fn_id, fn_deploy = function_ids[fn_name]
        fn_status = 500 if pool_exhaustion and rng.random() < 0.3 else 200
        fn_dur = int(rng.uniform(10_000_000, 2_000_000_000) * (1 + slow_rate * 3))
        records.append(_r(
            f"Function {fn_name} returned {fn_status}",
            "ERROR" if fn_status >= 500 else "INFO",
            {
                "supabase.service": "edge_function_invocation_logs",
                "supabase_project_ref": project_ref,
                "http.response.status_code": fn_status,
                "supabase.event.duration": fn_dur,
                "supabase.function.id": fn_id,
                "supabase.function.deployment_id": fn_deploy,
            },
        ))

    # edge_function_logs — 1–3 console log lines per tick
    fn_log_bodies = (
        "Processing request",
        "Database query completed",
        "Sending response",
        "Webhook payload validated",
    )
    for _ in range(rng.randint(1, 3)):
        records.append(_r(
            rng.choice(fn_log_bodies),
            rng.choice(("info", "info", "info", "warn")),
            {
                "supabase.service": "edge_function_logs",
                "supabase_project_ref": project_ref,
            },
        ))

    # supavisor_logs — 1–3 per tick; cover all event types panel 17 classifies
    sv_fn_names_normal = (
        "Supavisor.ClientHandler.handle_auth_success/3",
        "Supavisor.ClientHandler.handle_event/2",
        "Supavisor.DbHandler.query/3",
        "Supavisor.Manager.update_conn/2",
        "Supavisor.Tenants.start_local_pool/1",
        "Supavisor.Tenants.start_tenant/1",
    )
    sv_fn_error = "Supavisor.ClientHandler.Error.terminate_with_error/2"
    n_sv = rng.randint(2, 3) if pool_exhaustion else rng.randint(1, 2)
    for _ in range(n_sv):
        fn = sv_fn_error if pool_exhaustion and rng.random() < 0.4 else rng.choice(sv_fn_names_normal)
        records.append(_r(
            f"supavisor event: {fn}",
            "ERROR" if "Error" in fn else "INFO",
            {
                "supabase.service": "supavisor_logs",
                "supabase_project_ref": project_ref,
                "code.function.name": fn,
            },
        ))

    # pgbouncer_logs — 1 per tick
    if rng.random() < 0.7:
        if pool_exhaustion:
            body = f"C-0x{rng.randint(0,0xffff):04x}: closing because connection pool is full"
        else:
            body = f"C-0x{rng.randint(0,0xffff):04x}: login attempt for user app_user"
        records.append(_r(
            body,
            "WARN" if pool_exhaustion else "INFO",
            {"supabase.service": "pgbouncer_logs", "supabase_project_ref": project_ref},
        ))

    # postgrest_logs — 1 per tick
    pgrst_bodies = (
        "Listening on port 3000",
        'GET /rest/v1/users?select=* HTTP/1.1" 200 - 0.002s',
        'POST /rest/v1/rpc/get_count HTTP/1.1" 200 - 0.001s',
    )
    records.append(_r(
        rng.choice(pgrst_bodies),
        "INFO",
        {"supabase.service": "postgrest_logs", "supabase_project_ref": project_ref},
    ))

    # storage_logs — 1–3 per tick
    storage_methods = ("GET", "POST", "DELETE", "PUT")
    storage_paths = (
        "/storage/v1/object/avatars/user123.png",
        "/storage/v1/object/documents/report.pdf",
        "/storage/v1/bucket/public",
    )
    n_st = rng.randint(1, 3)
    for _ in range(n_st):
        method = rng.choice(storage_methods)
        path = rng.choice(storage_paths)
        status = 200 if method == "GET" else 201 if method in ("POST", "PUT") else 204
        dur_ns = int(rng.uniform(5_000_000, 200_000_000))
        attrs: dict[str, Any] = {
            "supabase.service": "storage_logs",
            "supabase_project_ref": project_ref,
            "http.request.method": method,
            "http.response.status_code": status,
            "supabase.event.duration": dur_ns,
        }
        if rng.random() < 0.2:
            event_type = rng.choice(("ObjectCreated", "ObjectRemoved"))
            records.append(_r(f"storage lifecycle: {event_type} {path}", "INFO", {
                "supabase.service": "storage_logs",
                "supabase_project_ref": project_ref,
            }))
        else:
            records.append(_r(f"{method} {path} → {status}", "INFO", attrs))

    # realtime_logs — Realtime's periodic "Billing metrics" lines. Panels classify them as
    # Connections (supabase.realtime.connected IS NOT NULL), Presence events
    # (body "*presence_events]*") and Channel events (body "*:events]*"), and plot
    # connected (this node), connected_cluster (all nodes) and limit (tenant
    # max_concurrent_users). Peak Utilisation = connected_cluster / limit, so the cluster
    # total is a share of the limit and each node holds part of it.
    conn_limit = 500
    connected_cluster = max(1, min(int(conn_limit * rng.uniform(0.25, 0.55) * load),
                                   int(conn_limit * 0.7)))
    connected = max(1, int(connected_cluster * rng.uniform(0.3, 0.6)))
    # external_id is the tenant reference used as "Tenant" column in panel 39
    rt_external_id = project_ref
    rt_base = {
        "supabase.service": "realtime_logs",
        "supabase_project_ref": project_ref,
        "supabase.realtime.external_id": rt_external_id,
    }
    records.append(_r(
        "Billing metrics: [:realtime, :connections]",
        "INFO",
        {
            **rt_base,
            "supabase.realtime.connected": connected,
            "supabase.realtime.connected_cluster": connected_cluster,
            "supabase.realtime.limit": conn_limit,
        },
    ))
    if rng.random() < 0.6:
        records.append(_r(
            "Billing metrics: [:realtime, :rate_counter, :channel, :events]", "INFO", rt_base))
    if rng.random() < 0.4:
        records.append(_r(
            "Billing metrics: [:realtime, :rate_counter, :channel, :presence_events]",
            "INFO", rt_base))
    # WARN when the cluster nears the tenant limit, plus occasional join throttling.
    if connected_cluster > conn_limit * 0.8:
        records.append(_r(
            f"Connection limit approaching: {connected_cluster}/{conn_limit} connected",
            "WARN", rt_base,
        ))
    elif rng.random() < 0.15:
        records.append(_r(
            "ClientJoinRateLimitReached: Too many joins per second", "WARN", rt_base))
    # ERROR occasionally — node disconnects, subscription failures
    if rng.random() < 0.1:
        records.append(_r(
            rng.choice([
                "Node disconnected unexpectedly",
                "Subscription channel error: timeout",
                "Failed to broadcast message to channel",
            ]),
            "ERROR",
            rt_base,
        ))

    return records


from generators.supabase_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop

def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    rng = random.Random()
    ns = scenario_data["namespace"] if scenario_data else NAMESPACE
    cfg = scenario_data.get("generator_config", {}) if scenario_data else {}
    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("supabase")
    project_ref = cfg.get("project_ref") or ScenarioIdentity(ns).label("project")
    cloud = cfg.get("cloud") or _default_cloud()
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("supabase")
    datname = f"{project_ref}_db"


    # supabase_project_ref is both a datapoint attribute (the Prometheus label the dashboards
    # read as a bare field) and a resource attribute: every package alert groups BY
    # resource.attributes.supabase_project_ref, so without it they all group by null.
    resource = build_signal_resource(
        ns=ns,
        signal_type="metrics",
        dataset=otel_dataset("supabase.metrics"),
        service_name=service_name,
        service_version="1.0.0",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name,
        cloud=cloud,
        extra_attrs={"supabase_project_ref": project_ref},
    )
    log_resource = build_signal_resource(
        ns=ns,
        signal_type="logs",
        dataset=otel_dataset("supabase"),
        service_name=service_name,
        service_version="1.0.0",
        service_instance_id=f"{service_name}-logs-001",
        host_name=host_name,
        cloud=cloud,
        # Options List control on the dashboard filters by this resource attribute
        extra_attrs={"supabase.project.ref": project_ref},
    )
    scope = {"name": SUPABASE_SCOPE, "version": SCOPE_VERSION}
    log_scope = {"name": SUPABASE_LOG_SCOPE, "version": SCOPE_VERSION}
    state = _SupabaseState(rng)
    stream_reset = _reset_vmstat_counter_mapping()
    # Always warm a few ticks so RATE()/TBUCKET panels aren't empty on start;
    # use more ticks after a stream wipe (sticky counter → gauge remap).
    warmup_left = _WARMUP_TICKS + (2 if stream_reset else 0)

    # Stable gauge baselines (jittered each tick).
    mem_total = 16 * 1024 * 1024 * 1024
    db_size = rng.randint(2 * 1024 * 1024 * 1024, 20 * 1024 * 1024 * 1024)
    auth_users = rng.randint(500, 50_000)
    realtime_subs = rng.randint(10, 500)

    def A(extra: dict[str, Any] | None = None, service_type: str = "db") -> dict[str, Any]:
        return _common_attrs(project_ref, service_type=service_type, extra=extra)

    logger.info("Supabase generator started (namespace=%s, project=%s)", ns, project_ref)

    _tick_delay = 0.0
    def _emit_tick() -> None:
        nonlocal _tick_delay, auth_users, db_size, realtime_subs, warmup_left
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="supabase_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        faults = _faults(active_chaos)
        # Log flavor only: connection-pressure faults read as pool exhaustion, slow-path
        # faults as slow queries. The metrics below follow the faults directly.
        pool_exhaustion = any(faults[k] > 0 for k in (
            "pgbouncer_clients_queueing_for_connections", "postgrest_database_pool_timeouts",
            "postgresql_connection_budget_nearly_exhausted", "supavisor_pool_exhausted",
        ))
        slow_rate = 1.0 if any(faults[k] > 0 for k in (
            "long_running_transaction", "replication_lag_is_high", "disk_io_saturation",
        )) else 0.0
        load = business_load()
        tick_wall = _WARMUP_INTERVAL_S if warmup_left > 0 else EMIT_INTERVAL_S
        state.tick(rng, load=load, faults=faults, wall=tick_wall)
        start = state.start_time_ns
        metrics: list[dict] = []

        # ── Node CPU ──────────────────────────────────────────────────────
        for cpu in CPU_IDS:
            for mode in CPU_MODES:
                metrics.append(_sum(
                    "node_cpu_seconds_total", "s",
                    round(state.cpu[(cpu, mode)], 3), start,
                    A({"cpu": cpu, "mode": mode}),
                ))
        for cpu in CPU_IDS:
            for mode in ("user", "nice"):
                metrics.append(_sum(
                    "node_cpu_guest_seconds_total", "s",
                    round(state.cpu_guest[(cpu, mode)], 3), start,
                    A({"cpu": cpu, "mode": mode}),
                ))

        # ── Node memory (gauges) ──────────────────────────────────────────
        active = int(mem_total * rng.uniform(0.25, 0.45))
        inactive = int(mem_total * rng.uniform(0.10, 0.20))
        cached = int(mem_total * rng.uniform(0.15, 0.30))
        slab = int(mem_total * rng.uniform(0.02, 0.06))
        mapped = int(mem_total * rng.uniform(0.01, 0.05))
        anon = int(mem_total * rng.uniform(0.10, 0.25))
        dirty = int(rng.randint(100_000, 50_000_000))
        used_frac = faults["host_memory_pressure"] or rng.uniform(0.40, 0.70)
        swap_total = 2 * _GIB
        metrics.append(_gauge("node_memory_MemAvailable_bytes", "By", int(mem_total * (1 - used_frac)), A()))
        metrics.append(_gauge("node_memory_SwapTotal_bytes", "By", swap_total, A()))
        metrics.append(_gauge(
            "node_memory_SwapFree_bytes", "By", int(swap_total * rng.uniform(0.97, 1.0)), A(),
        ))
        metrics.append(_gauge("node_memory_MemTotal_bytes", "By", mem_total, A()))
        metrics.append(_gauge("node_memory_Active_bytes", "By", active, A()))
        metrics.append(_gauge("node_memory_Inactive_bytes", "By", inactive, A()))
        metrics.append(_gauge("node_memory_Cached_bytes", "By", cached, A()))
        metrics.append(_gauge("node_memory_Dirty_bytes", "By", dirty, A()))
        metrics.append(_gauge("node_memory_Slab_bytes", "By", slab, A()))
        metrics.append(_gauge("node_memory_Mapped_bytes", "By", mapped, A()))
        metrics.append(_gauge("node_memory_AnonPages_bytes", "By", anon, A()))

        # ── Node vmstat (Prometheus untyped → gauge) ──────────────────────
        metrics.append(_gauge("node_vmstat_pgpgin", "1", state.pgpgin, A()))
        metrics.append(_gauge("node_vmstat_pgpgout", "1", state.pgpgout, A()))
        metrics.append(_gauge("node_vmstat_pgfault", "1", state.pgfault, A()))
        metrics.append(_gauge("node_vmstat_pgmajfault", "1", state.pgmajfault, A()))
        metrics.append(_gauge("node_vmstat_oom_kill", "1", state.oom_kill, A()))

        # ── Node network ──────────────────────────────────────────────────
        net = A({"device": NET_DEVICE})
        metrics.append(_sum("node_network_receive_packets_total", "1", state.net_rx_pkts, start, net))
        metrics.append(_sum("node_network_transmit_packets_total", "1", state.net_tx_pkts, start, net))
        metrics.append(_sum("node_network_receive_errs_total", "1", state.net_rx_errs, start, net))
        metrics.append(_sum("node_network_transmit_errs_total", "1", state.net_tx_errs, start, net))
        metrics.append(_sum("node_network_receive_drop_total", "1", state.net_rx_drop, start, net))
        metrics.append(_sum("node_network_transmit_drop_total", "1", state.net_tx_drop, start, net))

        # ── Node disk ─────────────────────────────────────────────────────
        disk = A({"device": DISK_DEVICE})
        metrics.append(_sum("node_disk_read_bytes_total", "By", state.disk_read_bytes, start, disk))
        metrics.append(_sum("node_disk_written_bytes_total", "By", state.disk_written_bytes, start, disk))
        metrics.append(_sum("node_disk_reads_completed_total", "1", state.disk_reads, start, disk))
        metrics.append(_sum("node_disk_writes_completed_total", "1", state.disk_writes, start, disk))
        metrics.append(_sum(
            "node_disk_io_time_seconds_total", "s",
            round(state.disk_io_time, 3), start, disk,
        ))
        metrics.append(_sum(
            "node_disk_io_time_weighted_seconds_total", "s",
            round(state.disk_weighted_time, 3), start, disk,
        ))
        metrics.append(_sum(
            "node_disk_discard_time_seconds_total", "s",
            round(state.disk_discard_time, 3), start, disk,
        ))
        metrics.append(_sum("node_disk_reads_merged_total", "1", state.disk_reads_merged, start, disk))
        metrics.append(_sum(
            "node_disk_discards_merged_total", "1", state.disk_discards_merged, start, disk,
        ))

        # ── Node filesystem ───────────────────────────────────────────────
        for fs in FS_MOUNTS:
            size = _FS_SIZE_BYTES[fs["mountpoint"]]
            is_pg_mount = fs["mountpoint"] == _PG_MOUNT
            used = (faults["filesystem_nearly_full"] if is_pg_mount and faults["filesystem_nearly_full"]
                    else rng.uniform(0.35, 0.65))
            free = int(size * (1 - used))
            files = rng.randint(1_000_000, 10_000_000)
            files_free = int(files * rng.uniform(0.6, 0.95))
            fs_attrs = A(fs)
            metrics.append(_gauge("node_filesystem_free_bytes", "By", free, fs_attrs))
            metrics.append(_gauge("node_filesystem_avail_bytes", "By", free, fs_attrs))
            metrics.append(_gauge("node_filesystem_size_bytes", "By", size, fs_attrs))
            metrics.append(_gauge("node_filesystem_files", "1", files, fs_attrs))
            metrics.append(_gauge("node_filesystem_files_free", "1", files_free, fs_attrs))
            metrics.append(_gauge(
                "node_filesystem_readonly", "1",
                1 if is_pg_mount and faults["filesystem_is_read_only"] > 0 else 0, fs_attrs,
            ))
            metrics.append(_gauge("node_filesystem_device_error", "1", 0, fs_attrs))

        # ── PostgreSQL ────────────────────────────────────────────────────
        db = A({"datname": datname})
        conn_fault = faults["postgresql_connection_budget_nearly_exhausted"]
        backends = int(_MAX_CONNECTIONS * conn_fault) if conn_fault > 0 else rng.randint(10, 40)
        metrics.append(_gauge("pg_stat_database_num_backends", "{conns}", backends, db))
        metrics.append(_gauge("max_connections_connection_count", "{conns}", _MAX_CONNECTIONS, A()))
        metrics.append(_gauge("connection_stats_connection_count", "{conns}", rng.randint(5, 30), A()))
        metrics.append(_sum("pg_stat_database_deadlocks_total", "1", state.deadlocks, start, db))
        metrics.append(_sum("pg_stat_database_xact_commit_total", "1", state.xact_commit, start, db))
        metrics.append(_gauge("pg_up", "1", 0 if faults["postgresql_is_down"] > 0 else 1, A()))
        metrics.append(_gauge(
            "pg_stat_activity_xact_runtime", "s",
            faults["long_running_transaction"] or round(rng.uniform(0.0, 30.0), 1), A(),
        ))
        metrics.append(_sum("pg_stat_database_xact_rollback_total", "1", state.xact_rollback, start, db))
        metrics.append(_sum("pg_stat_database_tup_fetched_total", "1", state.tup_fetched, start, db))
        metrics.append(_sum("pg_stat_database_tup_inserted_total", "1", state.tup_inserted, start, db))
        metrics.append(_sum("pg_stat_database_tup_updated_total", "1", state.tup_updated, start, db))
        metrics.append(_sum("pg_stat_database_temp_bytes_total", "By", state.temp_bytes, start, db))
        metrics.append(_sum("pg_stat_database_blks_hit_total", "1", state.blks_hit, start, db))
        metrics.append(_sum(
            "pg_stat_database_conflicts_confl_lock_total", "1", state.confl_lock, start, db,
        ))
        metrics.append(_sum(
            "pg_stat_database_conflicts_confl_deadlock_total", "1", state.confl_deadlock, start, db,
        ))
        metrics.append(_sum(
            "pg_stat_database_conflicts_confl_snapshot_total", "1", state.confl_snapshot, start, db,
        ))
        db_size += int(rng.randint(0, 100_000) * load)
        metrics.append(_gauge("pg_database_size_bytes", "By", db_size, db))
        metrics.append(_sum("pg_stat_statements_total_queries", "1", state.statements_queries, start, A()))
        metrics.append(_sum(
            "pg_stat_statements_total_time_seconds", "s",
            round(state.statements_time, 3), start, A(),
        ))
        metrics.append(_sum(
            "pg_stat_bgwriter_checkpoints_timed_total", "1", state.checkpoints_timed, start, A(),
        ))
        metrics.append(_sum(
            "pg_stat_bgwriter_maxwritten_clean_total", "1", state.maxwritten_clean, start, A(),
        ))
        metrics.append(_sum(
            "pg_stat_bgwriter_checkpoint_sync_time_total", "ms",
            round(state.checkpoint_sync_time, 3), start, A(),
        ))
        metrics.append(_gauge("pg_wal_size_mb", "MiBy", round(rng.uniform(64.0, 512.0), 1), A()))
        # Dashboard uses RATE() — emit as cumulative so the panel is non-empty.
        metrics.append(_sum(
            "pg_ls_archive_statusdir_wal_pending_count", "1", state.wal_pending, start, A(),
        ))
        metrics.append(_gauge("pg_status_in_recovery", "1", 0, A()))
        lag = faults["replication_lag_is_high"] or rng.uniform(0.0, 0.5)
        metrics.append(_gauge(
            "physical_replication_lag_physical_replication_lag_seconds", "s", round(lag, 3), A(),
        ))
        metrics.append(_gauge("physical_replication_lag_is_wal_replay_paused", "1", 0, A()))
        metrics.append(_gauge("physical_replication_lag_is_connected_to_primary", "1", 1, A()))
        metrics.append(_gauge("pg_stat_replication_replay_lag", "s", round(rng.uniform(0.0, 0.3), 3), A()))
        slot_gib = faults["replication_slot_retaining_wal"]
        metrics.append(_gauge(
            "replication_slots_max_lag_bytes", "By",
            int(slot_gib * _GIB) if slot_gib > 0 else rng.randint(1_000_000, 500_000_000), A(),
        ))

        # ── PgBouncer ─────────────────────────────────────────────────────
        queueing = faults["pgbouncer_clients_queueing_for_connections"]
        sv_active = rng.randint(30, 80) if queueing > 0 else rng.randint(5, 25)
        cl_waiting = rng.randint(20, 120) if queueing > 0 else rng.randint(0, 3)
        sv_testing = rng.randint(0, 5)
        maxwait = queueing if queueing > 0 else rng.uniform(0.0, 0.3)
        # The queueing alert groups BY database and user.
        pool = A({"database": datname, "user": "postgres"})
        metrics.append(_gauge("pgbouncer_up", "1", 0 if faults["pgbouncer_is_down"] > 0 else 1, A()))
        max_client = 200
        used_clients = min(max_client, sv_active + cl_waiting + rng.randint(0, 10))
        free_clients = max(0, max_client - used_clients)
        metrics.append(_gauge(
            "pgbouncer_pools_server_active_connections", "{conns}", sv_active, pool,
        ))
        metrics.append(_gauge(
            "pgbouncer_pools_client_waiting_connections", "{conns}", cl_waiting, pool,
        ))
        metrics.append(_gauge(
            "pgbouncer_pools_server_testing_connections", "{conns}", sv_testing, pool,
        ))
        metrics.append(_gauge(
            "pgbouncer_pools_client_maxwait_seconds", "s", round(maxwait, 3), pool,
        ))
        metrics.append(_sum(
            "pgbouncer_stats_sql_transactions_pooled_total", "1", state.txns_pooled, start, A(),
        ))
        metrics.append(_sum(
            "pgbouncer_stats_received_bytes_total", "By", state.received_bytes, start, A(),
        ))
        metrics.append(_sum(
            "pgbouncer_stats_client_wait_seconds_total", "s",
            round(state.client_wait_seconds, 3), start, A(),
        ))
        metrics.append(_sum(
            "pgbouncer_stats_server_in_transaction_seconds_total", "s",
            round(state.server_in_txn_seconds, 3), start, A(),
        ))
        metrics.append(_gauge("pgbouncer_databases_max_connections", "{conns}", 100, A()))
        metrics.append(_gauge("pgbouncer_databases_pool_size", "{conns}", 20, A()))
        metrics.append(_gauge("pgbouncer_databases_disabled", "1", 0, A()))
        metrics.append(_gauge(
            "pgbouncer_config_max_client_connections", "{conns}", max_client, A(),
        ))
        metrics.append(_gauge("pgbouncer_used_clients", "{conns}", used_clients, A()))
        metrics.append(_gauge("pgbouncer_free_clients", "{conns}", free_clients, A()))

        # ── Go sql connection pool ────────────────────────────────────────
        metrics.append(_gauge("db_sql_connection_max_open", "{conns}", 50, A(service_type="gotrue")))
        metrics.append(_sum(
            "db_sql_connection_wait_duration_milliseconds_total", "ms",
            round(state.conn_wait_ms, 3), start, A(service_type="gotrue"),
        ))
        metrics.append(_sum(
            "db_sql_connection_closed_max_lifetime_total", "1", state.closed_max_lifetime, start,
            A(service_type="gotrue"),
        ))
        metrics.append(_sum(
            "db_sql_connection_closed_max_idle_time_total", "1",
            state.closed_max_idle_time, start, A(service_type="gotrue"),
        ))
        metrics.append(_sum(
            "db_sql_connection_closed_max_idle_total", "1", state.closed_max_idle, start,
            A(service_type="gotrue"),
        ))

        # ── PostgREST / Go runtime / Auth / Realtime ──────────────────────
        timeouts = faults["postgrest_database_pool_timeouts"] > 0
        pool_avail = 0 if timeouts else rng.randint(5, 20)
        metrics.append(_gauge("pgrst_db_pool_available", "{conns}", pool_avail, A()))
        metrics.append(_gauge("pgrst_db_pool_waiting", "{conns}", rng.randint(5, 20) if timeouts else 0, A()))
        metrics.append(_sum("pgrst_db_pool_timeouts_total", "1", state.pool_timeouts, start, A()))
        metrics.append(_sum("pgrst_jwt_cache_hits_total", "1", state.jwt_hits, start, A()))
        metrics.append(_sum("pgrst_jwt_cache_requests_total", "1", state.jwt_requests, start, A()))
        metrics.append(_sum("pgrst_schema_cache_loads_total", "1", state.schema_cache_loads, start, A()))
        metrics.append(_gauge(
            "pgrst_schema_cache_query_time_seconds", "s",
            round(rng.uniform(0.001, 0.05), 4), A(),
        ))

        alloc = int(rng.randint(50_000_000, 400_000_000))
        sys_bytes = alloc + rng.randint(20_000_000, 100_000_000)
        gt = "gotrue"
        metrics.append(_gauge("go_memstats_alloc_bytes", "By", alloc, A(service_type=gt)))
        metrics.append(_gauge("go_memstats_sys_bytes", "By", sys_bytes, A(service_type=gt)))
        metrics.append(_gauge(
            "go_memstats_heap_objects", "1", rng.randint(100_000, 2_000_000), A(service_type=gt),
        ))
        metrics.append(_sum(
            "go_memstats_lookups_total", "1", state.go_lookups, start, A(service_type=gt),
        ))

        rss = int(rng.randint(100_000_000, 800_000_000))
        metrics.append(_gauge("process_resident_memory_bytes", "By", rss, A(service_type=gt)))
        metrics.append(_gauge(
            "process_virtual_memory_bytes", "By",
            rss + rng.randint(200_000_000, 1_000_000_000), A(service_type=gt),
        ))
        metrics.append(_gauge(
            "process_runtime_go_goroutines", "1", rng.randint(50, 500), A(service_type=gt),
        ))
        metrics.append(_gauge("process_max_fds", "1", 1024, A(service_type=gt)))
        metrics.append(_sum(
            "process_runtime_go_gc_pause_total_ns_total", "ns", state.gc_pause_ns, start,
            A(service_type=gt),
        ))
        metrics.append(_sum(
            "process_runtime_go_gc_count_total", "1", state.gc_count, start, A(service_type=gt),
        ))

        realtime_subs = max(0, realtime_subs + rng.randint(-5, 10))
        metrics.append(_gauge(
            "realtime_postgres_changes_total_subscriptions", "1", realtime_subs, A(),
        ))
        direct = rng.randint(20, 80) if conn_fault > 0 else rng.randint(2, 20)
        metrics.append(_gauge("direct_connection_stats_connection_count", "1", direct, A()))
        auth_users = max(0, auth_users + rng.randint(0, 5))
        metrics.append(_gauge("auth_users_user_count", "1", auth_users, A(service_type=gt)))
        metrics.append(_gauge("global_auth_version_major", "1", 2, A(service_type=gt)))
        metrics.append(_gauge("global_auth_version_patch", "1", 177, A(service_type=gt)))
        metrics.append(_sum("postgresql_restarts_total", "1", state.pg_restarts, start, A()))

        # ── Service health, auth requests, scrape health, Supavisor ───────
        metrics.append(_gauge(
            "gotrue_running", "1", 0 if faults["auth_service_is_down"] > 0 else 1, A(service_type=gt),
        ))
        for code in _HTTP_CODES:
            metrics.append(_sum(
                "http_status_codes_total", "1", state.http_codes[code], start,
                A({"code": code}, service_type=gt),
            ))
        metrics.append(_gauge("up", "1", 0 if faults["metrics_collection_is_failing"] > 0 else 1, A()))
        metrics.append(_gauge("pg_scrape_collector_success", "1", 1, A()))
        metrics.append(_gauge("node_scrape_collector_success", "1", 1, A()))
        metrics.append(_gauge("pg_exporter_last_scrape_error", "1", 0, A()))
        sv_frac = faults["supavisor_pool_exhausted"]
        checked_out = (
            int(round(_SUPAVISOR_POOL_SIZE * min(1.0, sv_frac))) if sv_frac > 0
            else int(_SUPAVISOR_POOL_SIZE * rng.uniform(0.30, 0.60))
        )
        sv = A({"tenant": project_ref, "mode": "transaction"})
        metrics.append(_gauge("supavisor_pool_connections_checked_out", "{conns}", checked_out, sv))
        metrics.append(_gauge(
            "supavisor_pool_connections_idle", "{conns}", _SUPAVISOR_POOL_SIZE - checked_out, sv,
        ))
        metrics.append(_gauge(
            "supavisor_connections_active", "{conns}", checked_out + rng.randint(0, 5), sv,
        ))

        client.send_metrics_scoped(resource, scope, metrics)
        log_records = _generate_service_logs(rng, project_ref, client, pool_exhaustion, slow_rate, load)
        client.send_logs_scoped(log_resource, log_scope, log_records)
        if warmup_left > 0:
            warmup_left -= 1
            # After the last warmup tick, sleep a full interval so the next
            # RATE() window matches EMIT_INTERVAL_S wall increments.
            wait_s = _WARMUP_INTERVAL_S if warmup_left > 0 else EMIT_INTERVAL_S
            _tick_delay = wait_s
        else:
            _tick_delay = EMIT_INTERVAL_S
    run_tick_loop(stop_event, _emit_tick, interval=lambda: _tick_delay)

    logger.info("Supabase generator stopped.")


def main() -> None:  # pragma: no cover
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
