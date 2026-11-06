#!/usr/bin/env python3
"""IBM MQ Generator — sends IBM MQ queue manager metrics via OTLP.

Emits data matching the ibmmq_otel content package
(discovery.datasets: ibmmq.otel).

Metric names (58) are the intersection of the IBM MQ Prometheus exporter
``ibmmq_qmgr_*`` series and the fields queried by the four package dashboards
(overview / message-traffic / error-analysis / resources).

Authority:
  - elastic/integrations/packages/ibmmq_otel/kibana/dashboard/*.json
  - elastic/integrations/packages/ibmmq_otel/manifest.yml
  - ibm-messaging/mq-metric-samples Prometheus exporter (ibmmq_qmgr_* prefix)
  - open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver

Usage (standalone):
    python3 -m generators.ibmmq_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/ibmmq_otel@0.3.1"

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
    build_cumulative_sum,
    build_gauge,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

IBMMQ_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("ibmmq-generator")
EMIT_INTERVAL_S = 30.0

# 58 metric names queried by ibmmq_otel dashboards. Do not invent alternate
# spellings — e.g. dashboards use ibmmq_qmgr_errors_file_system_free_space_percentage,
# not ibmmq_qmgr_mq_errors_file_system_free_space_percentage.
METRIC_NAMES = (
    # Message traffic counters
    "ibmmq_qmgr_mqput_mqput1_total",
    "ibmmq_qmgr_mqput_mqput1_bytes_total",
    "ibmmq_qmgr_destructive_get_total",
    "ibmmq_qmgr_destructive_get_bytes_total",
    "ibmmq_qmgr_persistent_message_mqput_total",
    "ibmmq_qmgr_persistent_message_mqput1_total",
    "ibmmq_qmgr_persistent_message_destructive_get_total",
    "ibmmq_qmgr_non_persistent_message_mqput_total",
    "ibmmq_qmgr_non_persistent_message_mqput1_total",
    "ibmmq_qmgr_non_persistent_message_destructive_get_total",
    "ibmmq_qmgr_topic_mqput_mqput1_total",
    "ibmmq_qmgr_published_to_subscribers_message_total",
    "ibmmq_qmgr_durable_subscription_create_total",
    "ibmmq_qmgr_durable_subscription_delete_total",
    "ibmmq_qmgr_non_durable_subscription_create_total",
    "ibmmq_qmgr_non_durable_subscription_delete_total",
    "ibmmq_qmgr_mqconn_mqconnx_total",
    "ibmmq_qmgr_mqdisc_total",
    # Transaction counters
    "ibmmq_qmgr_commit_total",
    "ibmmq_qmgr_rollback_total",
    "ibmmq_qmgr_expired_message_total",
    # Failed API counters
    "ibmmq_qmgr_failed_mqput_total",
    "ibmmq_qmgr_failed_mqput1_total",
    "ibmmq_qmgr_failed_mqget_total",
    "ibmmq_qmgr_failed_browse_total",
    "ibmmq_qmgr_failed_mqconn_mqconnx_total",
    "ibmmq_qmgr_failed_mqopen_total",
    "ibmmq_qmgr_failed_mqclose_total",
    "ibmmq_qmgr_failed_mqinq_total",
    "ibmmq_qmgr_failed_mqset_total",
    "ibmmq_qmgr_failed_mqcb_total",
    "ibmmq_qmgr_failed_mqsubrq_total",
    "ibmmq_qmgr_failed_subscription_create_alter_resume_total",
    "ibmmq_qmgr_failed_subscription_delete_total",
    "ibmmq_qmgr_failed_topic_mqput_mqput1_total",
    # Log / resource counters
    "ibmmq_qmgr_log_logical_written_bytes_total",
    "ibmmq_qmgr_log_physical_written_bytes_total",
    # Resource / health gauges
    "ibmmq_qmgr_fdc_files",
    "ibmmq_qmgr_log_write_latency_seconds",
    "ibmmq_qmgr_log_slowest_write_since_restart_seconds",
    "ibmmq_qmgr_log_write_size_bytes",
    "ibmmq_qmgr_log_in_use_bytes",
    "ibmmq_qmgr_log_file_system_in_use_bytes",
    "ibmmq_qmgr_log_file_system_free_space_percentage",
    "ibmmq_qmgr_log_primary_space_in_use_percentage",
    "ibmmq_qmgr_log_workload_primary_space_utilization_percentage",
    "ibmmq_qmgr_queue_manager_file_system_free_space_percentage",
    "ibmmq_qmgr_errors_file_system_free_space_percentage",
    "ibmmq_qmgr_trace_file_system_free_space_percentage",
    "ibmmq_qmgr_cpu_load_one_minute_average_percentage",
    "ibmmq_qmgr_cpu_load_five_minute_average_percentage",
    "ibmmq_qmgr_cpu_load_fifteen_minute_average_percentage",
    "ibmmq_qmgr_system_cpu_time_percentage",
    "ibmmq_qmgr_user_cpu_time_percentage",
    "ibmmq_qmgr_system_cpu_time_estimate_for_queue_manager_percentage",
    "ibmmq_qmgr_user_cpu_time_estimate_for_queue_manager_percentage",
    "ibmmq_qmgr_ram_free_percentage",
    "ibmmq_qmgr_ram_usage_estimate_for_queue_manager_bytes",
)


def _default_cloud() -> dict:
    return {"provider": "aws", "platform": "aws_ec2",
            "region": "us-east-1", "availability_zone": "us-east-1a"}


class _IBMMQCounters(CounterState):
    """Cumulative counters for IBM MQ qmgr Prometheus series."""

    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.put = rng.randint(500_000, 5_000_000)
        self.put_bytes = rng.randint(50_000_000, 500_000_000)
        self.get = rng.randint(500_000, 5_000_000)
        self.get_bytes = rng.randint(50_000_000, 500_000_000)
        self.persist_put = rng.randint(100_000, 1_000_000)
        self.persist_put1 = rng.randint(10_000, 100_000)
        self.persist_get = rng.randint(100_000, 1_000_000)
        self.nonpersist_put = rng.randint(200_000, 2_000_000)
        self.nonpersist_put1 = rng.randint(20_000, 200_000)
        self.nonpersist_get = rng.randint(200_000, 2_000_000)
        self.topic_put = rng.randint(50_000, 500_000)
        self.pub_sub = rng.randint(50_000, 500_000)
        self.durable_create = rng.randint(100, 5_000)
        self.durable_delete = rng.randint(50, 2_000)
        self.nondurable_create = rng.randint(1_000, 50_000)
        self.nondurable_delete = rng.randint(500, 25_000)
        self.conn = rng.randint(10_000, 500_000)
        self.disc = rng.randint(10_000, 500_000)
        self.commit = rng.randint(100_000, 2_000_000)
        self.rollback = rng.randint(10, 5_000)
        self.expired = 0
        self.failed_put = 0
        self.failed_put1 = 0
        self.failed_get = 0
        self.failed_browse = 0
        self.failed_conn = 0
        self.failed_open = 0
        self.failed_close = 0
        self.failed_inq = 0
        self.failed_set = 0
        self.failed_cb = 0
        self.failed_subrq = 0
        self.failed_sub_create = 0
        self.failed_sub_delete = 0
        self.failed_topic_put = 0
        self.log_logical = rng.randint(10_000_000, 200_000_000)
        self.log_physical = rng.randint(10_000_000, 200_000_000)

    def tick(
        self,
        rng: random.Random,
        load: float,
        conn_failures: int = 0,
        msg_failures: int = 0,
        rollback_pct: float = 0.0,
    ) -> None:
        puts = max(1, int(rng.randint(40, 200) * load))
        gets = max(1, int(puts * rng.uniform(0.85, 1.05)))
        avg_bytes = rng.randint(256, 4096)

        self.put += puts
        self.put_bytes += puts * avg_bytes
        self.get += gets
        self.get_bytes += gets * avg_bytes

        persist_share = rng.uniform(0.3, 0.5)
        persist_puts = int(puts * persist_share)
        persist_gets = int(gets * persist_share)
        self.persist_put += persist_puts
        self.persist_put1 += max(0, int(persist_puts * 0.1))
        self.persist_get += persist_gets
        self.nonpersist_put += puts - persist_puts
        self.nonpersist_put1 += max(0, int((puts - persist_puts) * 0.1))
        self.nonpersist_get += gets - persist_gets

        topic = max(0, int(puts * rng.uniform(0.05, 0.2)))
        self.topic_put += topic
        self.pub_sub += max(0, int(topic * rng.uniform(0.8, 1.2)))

        self.durable_create += rng.randint(0, 2)
        self.durable_delete += rng.randint(0, 1)
        self.nondurable_create += max(0, int(rng.randint(0, 8) * load))
        self.nondurable_delete += max(0, int(rng.randint(0, 6) * load))

        conns = max(1, int(rng.randint(2, 20) * load))
        self.conn += conns
        self.disc += max(0, conns - rng.randint(0, 2))

        commits = max(1, int(puts * rng.uniform(0.4, 0.8)))
        # Healthy baseline: 0 rollbacks/expiries/failed-ops — these alerts
        # breach on any occurrence (rate > 0.0), so ambient noise must be 0. Each fault
        # below moves only its own alert's counters.
        self.commit += commits
        if rollback_pct > 0:
            # rollback / (commit + rollback) == rollback_pct / 100
            share = min(rollback_pct, 99.0) / 100.0
            self.rollback += max(1, int(round(commits * share / (1.0 - share))))

        if conn_failures > 0:  # "Failed connection attempts": mqconn + mqopen
            self.failed_conn += conn_failures
            self.failed_open += max(1, conn_failures // 3)
            self.failed_close += 1
            self.failed_inq += 1
        if msg_failures > 0:  # "Failed message operations": put + put1 + get
            self.failed_put += msg_failures
            self.failed_put1 += max(1, msg_failures // 3)
            self.failed_get += max(1, msg_failures // 2)
            self.failed_browse += msg_failures // 4
            self.failed_topic_put += msg_failures // 5

        log_delta = int(puts * avg_bytes * rng.uniform(0.5, 1.5))
        self.log_logical += log_delta
        self.log_physical += int(log_delta * rng.uniform(0.9, 1.1))



from generators.ibmmq_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def emit_cycle(
    counters: _IBMMQCounters,
    fdc_files: int,
    rng: random.Random,
    chaos: dict | None,
    load: float,
    qmgr_name: str,
) -> tuple[list[dict], int]:
    """Advance the counters one emit cycle; return (metrics, new fdc_files)."""
    c = chaos or {}

    def _key(name: str, default: float = 0.0) -> float:
        value = impact_value(c, name, default=default)
        return default if value is None else float(value)

    conn_failures = int(max(0.0, _key("failed_connection_attempts")))
    msg_failures = int(max(0.0, _key("failed_message_operations")))
    rollback_pct = max(0.0, _key("high_transaction_rollback_ratio"))
    latency_ms = max(0.0, _key("high_log_write_latency"))
    cpu_load_pct = max(0.0, _key("cpu_load_high"))
    expiring = max(0.0, _key("messages_expiring_before_consumption"))
    fdc_key = max(0.0, _key("fdc_files_increasing"))
    # Filesystem keys are a target free-space %; they apply only below the SPEC default
    # (log 100, queue manager 200), so a channel that drives another key leaves them healthy.
    log_fs_target = _key("log_filesystem_critically_low", 100.0)
    qmgr_fs_target = _key("queue_manager_filesystem_low", 200.0)

    counters.tick(
        rng, load=load, conn_failures=conn_failures, msg_failures=msg_failures,
        rollback_pct=rollback_pct,
    )
    if fdc_key > 0:
        fdc_files += max(1, int(fdc_key))
    if expiring > 0:
        counters.expired += max(1, int(expiring))

    qmgr_attrs = {"qmgr": qmgr_name}
    start_ns = counters.start_time_ns
    metrics: list[dict] = []

    def _sum(name: str, unit: str, value: int) -> None:
        metrics.append(build_cumulative_sum(name, unit, value, start_ns, qmgr_attrs))

    def _gauge(name: str, unit: str, value: int | float) -> None:
        metrics.append(build_gauge(name, unit, value, qmgr_attrs))

    # Message traffic
    _sum("ibmmq_qmgr_mqput_mqput1_total", "{msgs}", counters.put)
    _sum("ibmmq_qmgr_mqput_mqput1_bytes_total", "By", counters.put_bytes)
    _sum("ibmmq_qmgr_destructive_get_total", "{msgs}", counters.get)
    _sum("ibmmq_qmgr_destructive_get_bytes_total", "By", counters.get_bytes)
    _sum("ibmmq_qmgr_persistent_message_mqput_total", "{msgs}", counters.persist_put)
    _sum("ibmmq_qmgr_persistent_message_mqput1_total", "{msgs}", counters.persist_put1)
    _sum("ibmmq_qmgr_persistent_message_destructive_get_total", "{msgs}",
         counters.persist_get)
    _sum("ibmmq_qmgr_non_persistent_message_mqput_total", "{msgs}",
         counters.nonpersist_put)
    _sum("ibmmq_qmgr_non_persistent_message_mqput1_total", "{msgs}",
         counters.nonpersist_put1)
    _sum("ibmmq_qmgr_non_persistent_message_destructive_get_total", "{msgs}",
         counters.nonpersist_get)
    _sum("ibmmq_qmgr_topic_mqput_mqput1_total", "{msgs}", counters.topic_put)
    _sum("ibmmq_qmgr_published_to_subscribers_message_total", "{msgs}", counters.pub_sub)
    _sum("ibmmq_qmgr_durable_subscription_create_total", "{subs}",
         counters.durable_create)
    _sum("ibmmq_qmgr_durable_subscription_delete_total", "{subs}",
         counters.durable_delete)
    _sum("ibmmq_qmgr_non_durable_subscription_create_total", "{subs}",
         counters.nondurable_create)
    _sum("ibmmq_qmgr_non_durable_subscription_delete_total", "{subs}",
         counters.nondurable_delete)
    _sum("ibmmq_qmgr_mqconn_mqconnx_total", "{conns}", counters.conn)
    _sum("ibmmq_qmgr_mqdisc_total", "{conns}", counters.disc)

    # Transactions / expiry
    _sum("ibmmq_qmgr_commit_total", "{txns}", counters.commit)
    _sum("ibmmq_qmgr_rollback_total", "{txns}", counters.rollback)
    _sum("ibmmq_qmgr_expired_message_total", "{msgs}", counters.expired)

    # Failed APIs
    _sum("ibmmq_qmgr_failed_mqput_total", "{ops}", counters.failed_put)
    _sum("ibmmq_qmgr_failed_mqput1_total", "{ops}", counters.failed_put1)
    _sum("ibmmq_qmgr_failed_mqget_total", "{ops}", counters.failed_get)
    _sum("ibmmq_qmgr_failed_browse_total", "{ops}", counters.failed_browse)
    _sum("ibmmq_qmgr_failed_mqconn_mqconnx_total", "{ops}", counters.failed_conn)
    _sum("ibmmq_qmgr_failed_mqopen_total", "{ops}", counters.failed_open)
    _sum("ibmmq_qmgr_failed_mqclose_total", "{ops}", counters.failed_close)
    _sum("ibmmq_qmgr_failed_mqinq_total", "{ops}", counters.failed_inq)
    _sum("ibmmq_qmgr_failed_mqset_total", "{ops}", counters.failed_set)
    _sum("ibmmq_qmgr_failed_mqcb_total", "{ops}", counters.failed_cb)
    _sum("ibmmq_qmgr_failed_mqsubrq_total", "{ops}", counters.failed_subrq)
    _sum("ibmmq_qmgr_failed_subscription_create_alter_resume_total", "{ops}",
         counters.failed_sub_create)
    _sum("ibmmq_qmgr_failed_subscription_delete_total", "{ops}",
         counters.failed_sub_delete)
    _sum("ibmmq_qmgr_failed_topic_mqput_mqput1_total", "{ops}",
         counters.failed_topic_put)

    # Log I/O counters
    _sum("ibmmq_qmgr_log_logical_written_bytes_total", "By", counters.log_logical)
    _sum("ibmmq_qmgr_log_physical_written_bytes_total", "By", counters.log_physical)

    # Resource / health gauges
    # Healthy free space stays well above alert floors (log <10%, qmgr <20%).
    free_base = rng.uniform(40.0, 85.0)
    log_free = free_base * rng.uniform(0.9, 1.0)
    if log_fs_target < 100.0:
        log_free = log_fs_target * rng.uniform(0.9, 1.0)
    qmgr_free = free_base * rng.uniform(0.95, 1.05)
    if qmgr_fs_target < 200.0:
        qmgr_free = min(qmgr_fs_target, 100.0) * rng.uniform(0.9, 1.0)
    if cpu_load_pct > 0:
        # GT may send oversized values (e.g. 637). Clamp into (85, 100] so
        # TS AVG_OVER_TIME can clear the OOTB >85 threshold; rematerialize
        # the stream if a long healthy lookback dilutes the tip.
        target = cpu_load_pct if cpu_load_pct <= 100.0 else 95.0
        if target <= 85.0:
            target = 90.0
        cpu_1m = round(min(100.0, target * rng.uniform(0.98, 1.02)), 1)
    else:
        # Healthy CPU stays under the 85% alert threshold.
        cpu_1m = round(min(70.0, rng.uniform(5.0, 55.0) * load), 1)
    # Healthy log latency must stay under the 0.005s alert threshold.
    if latency_ms > 0:
        # The key is the log write latency in ms; the alert fires above 5 ms.
        latency = round(latency_ms / 1000.0, 4)
    else:
        latency = round(rng.uniform(0.0002, 0.003), 4)

    _gauge("ibmmq_qmgr_fdc_files", "{files}", fdc_files)
    _gauge("ibmmq_qmgr_log_write_latency_seconds", "s", latency)
    _gauge(
        "ibmmq_qmgr_log_slowest_write_since_restart_seconds", "s",
        round(latency * rng.uniform(2.0, 8.0), 4),
    )
    _gauge("ibmmq_qmgr_log_write_size_bytes", "By", rng.randint(4096, 65536))
    _gauge("ibmmq_qmgr_log_in_use_bytes", "By",
           rng.randint(50_000_000, 2_000_000_000))
    _gauge("ibmmq_qmgr_log_file_system_in_use_bytes", "By",
           rng.randint(100_000_000, 5_000_000_000))
    _gauge("ibmmq_qmgr_log_file_system_free_space_percentage", "%",
           round(log_free, 1))
    _gauge("ibmmq_qmgr_log_primary_space_in_use_percentage", "%",
           round(min(98.0, (100.0 - log_free) * rng.uniform(0.8, 1.1)), 1))
    _gauge("ibmmq_qmgr_log_workload_primary_space_utilization_percentage", "%",
           round(min(98.0, (100.0 - log_free) * rng.uniform(0.7, 1.0)), 1))
    _gauge("ibmmq_qmgr_queue_manager_file_system_free_space_percentage", "%",
           round(qmgr_free, 1))
    _gauge("ibmmq_qmgr_errors_file_system_free_space_percentage", "%",
           round(min(95.0, free_base + rng.uniform(5.0, 15.0)), 1))
    _gauge("ibmmq_qmgr_trace_file_system_free_space_percentage", "%",
           round(min(95.0, free_base + rng.uniform(5.0, 20.0)), 1))
    _gauge("ibmmq_qmgr_cpu_load_one_minute_average_percentage", "%", cpu_1m)
    _gauge("ibmmq_qmgr_cpu_load_five_minute_average_percentage", "%",
           round(cpu_1m * rng.uniform(0.9, 1.1), 1))
    _gauge("ibmmq_qmgr_cpu_load_fifteen_minute_average_percentage", "%",
           round(cpu_1m * rng.uniform(0.8, 1.0), 1))
    _gauge("ibmmq_qmgr_system_cpu_time_percentage", "%",
           round(cpu_1m * rng.uniform(0.3, 0.6), 1))
    _gauge("ibmmq_qmgr_user_cpu_time_percentage", "%",
           round(cpu_1m * rng.uniform(0.4, 0.7), 1))
    _gauge("ibmmq_qmgr_system_cpu_time_estimate_for_queue_manager_percentage", "%",
           round(cpu_1m * rng.uniform(0.2, 0.5), 1))
    _gauge("ibmmq_qmgr_user_cpu_time_estimate_for_queue_manager_percentage", "%",
           round(cpu_1m * rng.uniform(0.3, 0.6), 1))
    _gauge("ibmmq_qmgr_ram_free_percentage", "%",
           round(rng.uniform(25.0, 70.0), 1))
    _gauge("ibmmq_qmgr_ram_usage_estimate_for_queue_manager_bytes", "By",
           rng.randint(512 * 1024 * 1024, 4 * 1024 * 1024 * 1024))
    return metrics, fdc_files


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

    ident = ScenarioIdentity.from_scenario(scenario_data, ns)
    service_name = ident.service("ibmmq")
    qmgr_name = ident.label(cfg.get("qmgr_name") or "QM1")
    cloud = cfg.get("cloud") or _default_cloud()
    host_name = ident.host("ibmmq")


    resource = build_signal_resource(
        ns=ns, signal_type="metrics", dataset=otel_dataset("ibmmq"),
        service_name=service_name, service_version="9.3.5.0",
        service_instance_id=ident.instance(service_name),
        host_name=host_name, cloud=cloud,
        extra_attrs={"ibmmq.qmgr": qmgr_name},
    )
    scope = {"name": IBMMQ_SCOPE, "version": SCOPE_VERSION}

    counters = _IBMMQCounters(rng)
    fdc_files = 0  # DELTA(fdc_files) > 0 alert — keep flat unless chaos
    logger.info("IBM MQ generator started (namespace=%s, qmgr=%s)", ns, qmgr_name)

    def _emit_tick() -> None:
        nonlocal fdc_files
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="ibmmq_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        metrics, fdc_files = emit_cycle(
            counters, fdc_files, rng, active_chaos, business_load(), qmgr_name,
        )
        client.send_metrics_scoped(resource, scope, metrics)
    run_tick_loop(stop_event, _emit_tick, interval=lambda: EMIT_INTERVAL_S)

    logger.info("IBM MQ generator stopped.")


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "60"))
    def _stop(sig, frame): stop_event.set()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    client = OTLPClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
