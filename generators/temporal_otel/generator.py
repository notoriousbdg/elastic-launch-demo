#!/usr/bin/env python3
"""Temporal Cloud generator — sends ``temporal_cloud_v1_*`` metrics via OTLP.

Models the Temporal Cloud OpenMetrics (v1) endpoint scraped by an OTel
Collector ``prometheus`` receiver.  The endpoint publishes pre-computed
per-second rates and point-in-time values, so every series is a **gauge**
(the package dashboards, alerts, and SLOs all use ``AVG_OVER_TIME`` /
``MAX_OVER_TIME`` / ``avg`` / ``max`` — no ``RATE`` / ``INCREASE``).

Emits data matching the temporal_otel content package
(discovery.datasets: temporal.cloud_metrics.otel), which populates the
``[Temporal OTel] Cloud Metrics`` dashboard, its 7 alert rule templates, and
the 3 SLO templates (service latency p99, task queue backlog, workflow
success rate).

Entities: two Temporal namespaces (payments, orders) in one account, each
with three task queues and one or two workflow types per queue.

Datapoint attributes: temporal_namespace, temporal_account, region,
temporal_task_queue, temporal_workflow_type, operation, task_type,
timeout_type, action_type, is_background.

Usage (standalone):
    python3 -m generators.temporal_otel.generator
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/temporal_otel@1.0.0"

import hashlib
import logging
import os
import random
import signal
import threading
from typing import Any

from app.config import NAMESPACE
from app.telemetry import OTLPClient
from generator_engine.db_common import (
    CounterState,
    build_gauge,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)
from generator_engine.identity import ScenarioIdentity

# ── Grounding constants — do not change names ─────────────────────────────────

METRIC_PREFIX = "temporal_cloud_v1_"

METRIC_NAMES = (
    "temporal_cloud_v1_workflow_success_count",
    "temporal_cloud_v1_workflow_failed_count",
    "temporal_cloud_v1_workflow_timeout_count",
    "temporal_cloud_v1_workflow_terminate_count",
    "temporal_cloud_v1_workflow_schedule_to_close_latency_p99",
    "temporal_cloud_v1_activity_success_count",
    "temporal_cloud_v1_activity_task_fail_count",
    "temporal_cloud_v1_activity_task_timeout_count",
    "temporal_cloud_v1_no_poller_tasks_count",
    "temporal_cloud_v1_approximate_backlog_count",
    "temporal_cloud_v1_poll_success_count",
    "temporal_cloud_v1_poll_success_sync_count",
    "temporal_cloud_v1_poll_timeout_count",
    "temporal_cloud_v1_namespace_open_workflows",
    "temporal_cloud_v1_total_action_count",
    "temporal_cloud_v1_billable_action_count",
    "temporal_cloud_v1_action_limit",
    "temporal_cloud_v1_operations_count",
    "temporal_cloud_v1_operations_limit",
    "temporal_cloud_v1_service_request_count",
    "temporal_cloud_v1_service_error_count",
    "temporal_cloud_v1_service_request_throttled_count",
    "temporal_cloud_v1_resource_exhausted_error_count",
    "temporal_cloud_v1_service_pending_requests",
    "temporal_cloud_v1_service_latency_p99",
)

PROMETHEUS_SCOPE = {
    "name": "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver",
    "version": "0.121.0",
}

# Attribute keys (prometheus labels on the Temporal Cloud v1 endpoint).
ATTR_NAMESPACE = "temporal_namespace"
ATTR_ACCOUNT = "temporal_account"
ATTR_REGION = "region"
ATTR_TASK_QUEUE = "temporal_task_queue"
ATTR_WORKFLOW_TYPE = "temporal_workflow_type"
ATTR_OPERATION = "operation"
ATTR_TASK_TYPE = "task_type"
ATTR_TIMEOUT_TYPE = "timeout_type"
ATTR_ACTION_TYPE = "action_type"
ATTR_IS_BACKGROUND = "is_background"

TASK_TYPES = ("Workflow", "Activity")
TIMEOUT_TYPES = ("StartToClose", "ScheduleToStart", "Heartbeat")
ACTION_TYPES = (
    ("workflow_started", 0.20),
    ("activity_scheduled", 0.45),
    ("timer_started", 0.12),
    ("signal_received", 0.10),
    ("activity_heartbeat", 0.08),
    ("query", 0.05),
)
# Frontend gRPC operations: (name, base requests/sec per namespace).
OPERATIONS = (
    ("StartWorkflowExecution", 6.0),
    ("SignalWorkflowExecution", 3.0),
    ("SignalWithStartWorkflowExecution", 1.5),
    ("RespondWorkflowTaskCompleted", 18.0),
    ("RespondActivityTaskCompleted", 30.0),
    ("RecordActivityTaskHeartbeat", 4.0),
    ("QueryWorkflow", 1.0),
    ("DescribeWorkflowExecution", 0.8),
)

DEFAULT_NAMESPACES: list[dict[str, Any]] = [
    {
        "name": "payments",
        "region": "aws-us-east-1",
        "action_limit": 500.0,
        "task_queues": {
            "payment-processing": ["PaymentWorkflow", "ChargeCardWorkflow"],
            "refunds": ["RefundWorkflow"],
            "fraud-checks": ["FraudScreeningWorkflow"],
        },
    },
    {
        "name": "orders",
        "region": "aws-us-west-2",
        "action_limit": 400.0,
        "task_queues": {
            "order-fulfillment": ["OrderFulfillmentWorkflow", "ShipmentWorkflow"],
            "inventory-sync": ["InventorySyncWorkflow"],
            "notifications": ["CustomerNotificationWorkflow"],
        },
    },
]

# Healthy ceilings (alert / SLO thresholds in comments).
HEALTHY_ACTION_UTIL = (0.18, 0.38)        # alert: utilization_pct > 80
HEALTHY_ACTIVITY_TIMEOUT_MAX = 0.008      # alert: timeout_rate > 0.05 per (ns, tq, type)
HEALTHY_ACTIVITY_FAIL_RATIO = (0.002, 0.012)  # alert: fail ratio > 10%
HEALTHY_OPEN_WORKFLOWS = (1_500.0, 4_500.0)   # alert: open_workflows > 10000
HEALTHY_BACKLOG_MAX = 40.0                # alert: > 500; SLO: < 1000
HEALTHY_LATENCY_P99 = (0.012, 0.065)      # SLO: service p99 < 0.1 s
HEALTHY_WF_FAIL_RATIO = (0.001, 0.006)    # SLO: success / (s+f+t) > 0.95

logger = logging.getLogger("temporal-otel-generator")

BATCH_INTERVAL_MIN = 10
BATCH_INTERVAL_MAX = 20


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_ec2",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


def temporal_account_id(ident: ScenarioIdentity, override: str | None = None) -> str:
    """Return the Temporal Cloud account id: stable per scenario namespace."""
    if override:
        return str(override)
    return hashlib.sha256(f"{ident.namespace}:temporal-account".encode()).hexdigest()[:5]


def build_topology(
    ident: ScenarioIdentity,
    account: str,
    namespaces: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Qualify Temporal namespace names (``<scenario>-<name>.<account>``)."""
    topo: list[dict[str, Any]] = []
    for entry in namespaces or DEFAULT_NAMESPACES:
        queues = entry.get("task_queues") or {"default": ["DefaultWorkflow"]}
        topo.append({
            "name": f"{ident.qualify(entry['name'])}.{account}",
            "region": entry.get("region", "aws-us-east-1"),
            "action_limit": float(entry.get("action_limit", 500.0)),
            "task_queues": {str(q): [str(w) for w in wts] for q, wts in queues.items()},
        })
    return topo


def _positive(chaos: dict, key: str) -> float:
    try:
        value = float(chaos.get(key) or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return value if value > 0 else 0.0


def fault_levels(active_chaos: dict | None) -> dict[str, float]:
    """Translate impact keys into breach values (0.0 = healthy).

    Any positive key value activates the fault.  Values at/below the alert
    threshold are lifted to a value that clearly breaches it (GT probes are
    activation tokens, not literal metric values — ALERTS.md #14/#41).
    """
    chaos = active_chaos or {}
    action_pct = _positive(chaos, "namespace_approaching_action_rate_limit")
    if action_pct:
        action_pct = min(150.0, action_pct if action_pct > 80.0 else 95.0)
    act_timeout = _positive(chaos, "activity_task_timeout_rate_elevated")
    if act_timeout:
        act_timeout = max(act_timeout, 0.5)
    act_fail_pct = _positive(chaos, "high_activity_task_failure_rate")
    if act_fail_pct:
        act_fail_pct = min(95.0, act_fail_pct if act_fail_pct > 10.0 else 50.0)
    no_poller = _positive(chaos, "no_pollers_on_task_queue")
    if no_poller:
        no_poller = max(no_poller, 1.0)
    open_wf = _positive(chaos, "namespace_open_workflow_count_anomaly")
    if open_wf:
        open_wf = open_wf if open_wf > 10_000.0 else 40_000.0
    backlog = _positive(chaos, "task_queue_backlog_growing")
    if backlog:
        # Above 1000 so the backlog SLO burns along with the 500-task alert.
        backlog = backlog if backlog > 1_000.0 else 2_500.0
    wf_timeout = _positive(chaos, "workflow_timeout_rate_elevated")
    if wf_timeout:
        wf_timeout = max(wf_timeout, 0.5)
    return {
        "action_pct": action_pct,
        "activity_timeout": act_timeout,
        "activity_fail_pct": act_fail_pct,
        "no_poller": no_poller,
        "open_workflows": open_wf,
        "backlog": backlog,
        "workflow_timeout": wf_timeout,
    }


class TemporalMetricState(CounterState):
    """Slow-moving per-series state so dashboard lines stay smooth.

    Every Temporal Cloud v1 series is a gauge; nothing here is cumulative.
    Values are clamped to the healthy band each tick so ambient drift can
    never cross an alert threshold (ALERTS.md trap #2).
    """

    def __init__(self, rng: random.Random, topology: list[dict[str, Any]]):
        super().__init__(rng, initial=0)
        self.wf_base: dict[tuple[str, str, str], float] = {}
        self.backlog: dict[tuple[str, str, str], float] = {}
        self.open_workflows: dict[str, float] = {}
        self.wf_latency: dict[tuple[str, str], float] = {}
        for ns in topology:
            name = ns["name"]
            lo, hi = HEALTHY_OPEN_WORKFLOWS
            self.open_workflows[name] = rng.uniform(lo + 500, hi - 500)
            for queue, wtypes in ns["task_queues"].items():
                for wt in wtypes:
                    self.wf_base[(name, queue, wt)] = rng.uniform(1.0, 8.0)
                    self.wf_latency[(name, wt)] = rng.uniform(0.8, 30.0)
                for tt in TASK_TYPES:
                    self.backlog[(name, queue, tt)] = rng.uniform(0.0, 15.0)

    def tick(self, rng: random.Random, drift: int = 0) -> None:  # noqa: ARG002
        lo, hi = HEALTHY_OPEN_WORKFLOWS
        for key, value in self.open_workflows.items():
            self.open_workflows[key] = min(hi, max(lo, value + rng.uniform(-120.0, 120.0)))
        for key, value in self.backlog.items():
            self.backlog[key] = min(HEALTHY_BACKLOG_MAX, max(0.0, value + rng.uniform(-6.0, 6.0)))


def _g(name: str, value: float, attrs: dict[str, Any], unit: str = "1") -> dict[str, Any]:
    return build_gauge(name, unit, float(round(value, 6)), attrs)


def _generate_metrics(
    state: TemporalMetricState,
    topology: list[dict[str, Any]],
    account: str,
    rng: random.Random,
    active_chaos: dict | None = None,
    *,
    load: float | None = None,
) -> list[dict[str, Any]]:
    """Return one scrape worth of ``temporal_cloud_v1_*`` gauges."""
    faults = fault_levels(active_chaos)
    state.tick(rng)
    traffic = 0.5 + 0.5 * (business_load() if load is None else load)
    metrics: list[dict[str, Any]] = []

    # Faults land on the first namespace / first task queue (one hot tenant).
    fault_ns = topology[0]["name"] if topology else ""
    fault_queue = next(iter(topology[0]["task_queues"]), "") if topology else ""

    for ns in topology:
        name = ns["name"]
        is_fault_ns = name == fault_ns
        base = {ATTR_NAMESPACE: name, ATTR_ACCOUNT: account, ATTR_REGION: ns["region"]}
        ns_success = 0.0

        for queue, wtypes in ns["task_queues"].items():
            is_fault_q = is_fault_ns and queue == fault_queue
            q_attrs = {**base, ATTR_TASK_QUEUE: queue}
            q_success = 0.0

            # ── Workflow outcomes (per workflow type) ──
            for wt in wtypes:
                attrs = {**q_attrs, ATTR_WORKFLOW_TYPE: wt}
                success = state.wf_base[(name, queue, wt)] * traffic * rng.uniform(0.9, 1.1)
                failed = success * rng.uniform(*HEALTHY_WF_FAIL_RATIO)
                terminated = success * rng.uniform(0.0, 0.002)
                # Healthy: no workflow timeouts at all (alert: > 0.01).
                timeout = 0.0
                latency = state.wf_latency[(name, wt)] * rng.uniform(0.85, 1.15)
                if is_fault_q and faults["workflow_timeout"]:
                    # Timeouts at least equal to completions → burns the success SLO.
                    timeout = max(faults["workflow_timeout"], success)
                    latency *= 10.0
                q_success += success
                metrics.append(_g("temporal_cloud_v1_workflow_success_count", success, attrs))
                metrics.append(_g("temporal_cloud_v1_workflow_failed_count", failed, attrs))
                metrics.append(_g("temporal_cloud_v1_workflow_timeout_count", timeout, attrs))
                metrics.append(_g("temporal_cloud_v1_workflow_terminate_count", terminated, attrs))
                metrics.append(_g(
                    "temporal_cloud_v1_workflow_schedule_to_close_latency_p99", latency, attrs, "s",
                ))
            ns_success += q_success

            # ── Activities (per task queue) ──
            act_success = q_success * rng.uniform(3.0, 6.0)
            if is_fault_q and faults["activity_fail_pct"]:
                pct = faults["activity_fail_pct"]
                act_fail = act_success * pct / (100.0 - pct)
            else:
                act_fail = act_success * rng.uniform(*HEALTHY_ACTIVITY_FAIL_RATIO)
            metrics.append(_g("temporal_cloud_v1_activity_success_count", act_success, q_attrs))
            metrics.append(_g("temporal_cloud_v1_activity_task_fail_count", act_fail, q_attrs))
            for tt in TIMEOUT_TYPES:
                t_rate = rng.uniform(0.0, HEALTHY_ACTIVITY_TIMEOUT_MAX) if tt == "StartToClose" else 0.0
                if is_fault_q and faults["activity_timeout"] and tt == "StartToClose":
                    t_rate = faults["activity_timeout"]
                metrics.append(_g(
                    "temporal_cloud_v1_activity_task_timeout_count", t_rate,
                    {**q_attrs, ATTR_TIMEOUT_TYPE: tt},
                ))

            # ── Task queue polling / backlog (per task type) ──
            for tt in TASK_TYPES:
                attrs = {**q_attrs, ATTR_TASK_TYPE: tt}
                poll_success = (q_success * rng.uniform(2.5, 4.0)) if tt == "Workflow" else act_success
                no_poller = 0.0
                backlog = state.backlog[(name, queue, tt)]
                if is_fault_q and tt == "Activity":
                    if faults["no_poller"]:
                        no_poller = faults["no_poller"]
                        poll_success *= 0.3
                    if faults["backlog"]:
                        backlog = faults["backlog"] * rng.uniform(0.95, 1.1)
                sync = poll_success * rng.uniform(0.93, 0.99)
                poll_timeout = poll_success * rng.uniform(0.01, 0.04)
                metrics.append(_g("temporal_cloud_v1_poll_success_count", poll_success, attrs))
                metrics.append(_g("temporal_cloud_v1_poll_success_sync_count", sync, attrs))
                metrics.append(_g("temporal_cloud_v1_poll_timeout_count", poll_timeout, attrs))
                metrics.append(_g("temporal_cloud_v1_no_poller_tasks_count", no_poller, attrs))
                metrics.append(_g("temporal_cloud_v1_approximate_backlog_count", backlog, attrs))

        # ── Namespace capacity ──
        open_wf = state.open_workflows[name]
        if is_fault_ns and faults["open_workflows"]:
            open_wf = faults["open_workflows"] * rng.uniform(1.0, 1.05)
        metrics.append(_g("temporal_cloud_v1_namespace_open_workflows", open_wf, base))

        limit = ns["action_limit"]
        util = rng.uniform(*HEALTHY_ACTION_UTIL)
        if is_fault_ns and faults["action_pct"]:
            util = faults["action_pct"] / 100.0 * rng.uniform(1.0, 1.04)
        foreground = limit * util
        background = limit * rng.uniform(0.02, 0.06)
        metrics.append(_g("temporal_cloud_v1_total_action_count", foreground,
                          {**base, ATTR_IS_BACKGROUND: "false"}))
        metrics.append(_g("temporal_cloud_v1_total_action_count", background,
                          {**base, ATTR_IS_BACKGROUND: "true"}))
        metrics.append(_g("temporal_cloud_v1_action_limit", limit, base))
        for action_type, share in ACTION_TYPES:
            metrics.append(_g(
                "temporal_cloud_v1_billable_action_count", (foreground + background) * share,
                {**base, ATTR_ACTION_TYPE: action_type},
            ))
        metrics.append(_g("temporal_cloud_v1_operations_count", (foreground + background) * 1.6, base))
        metrics.append(_g("temporal_cloud_v1_operations_limit", limit * 2.0, base))

        # ── Frontend service (per gRPC operation) ──
        throttle_on = is_fault_ns and bool(faults["action_pct"])
        for op, op_base in OPERATIONS:
            attrs = {**base, ATTR_OPERATION: op}
            requests = op_base * traffic * rng.uniform(0.85, 1.15)
            errors = requests * rng.uniform(0.0002, 0.001)
            throttled = requests * rng.uniform(0.04, 0.08) if throttle_on else 0.0
            exhausted = requests * rng.uniform(0.02, 0.05) if throttle_on else 0.0
            pending = rng.uniform(20.0, 60.0) if throttle_on else rng.uniform(0.0, 4.0)
            latency = rng.uniform(0.15, 0.35) if throttle_on else rng.uniform(*HEALTHY_LATENCY_P99)
            metrics.append(_g("temporal_cloud_v1_service_request_count", requests, attrs))
            metrics.append(_g("temporal_cloud_v1_service_error_count", errors + exhausted, attrs))
            metrics.append(_g("temporal_cloud_v1_service_request_throttled_count", throttled, attrs))
            metrics.append(_g("temporal_cloud_v1_resource_exhausted_error_count", exhausted, attrs))
            metrics.append(_g("temporal_cloud_v1_service_pending_requests", pending, attrs))
            metrics.append(_g("temporal_cloud_v1_service_latency_p99", latency, attrs, "s"))

    return metrics


from generators.temporal_otel import SPEC as _ALERT_SPEC  # noqa: E402
from generator_engine.tick import run_tick_loop  # noqa: E402


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run the Temporal Cloud metrics loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {}) or {}
    else:
        ns = NAMESPACE
        cfg = {}

    channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    ident = ScenarioIdentity.from_scenario(scenario_data, ns)
    account = temporal_account_id(ident, cfg.get("account_id"))
    topology = build_topology(ident, account, cfg.get("namespaces"))
    service_name = ident.service("temporal-cloud")
    cloud = cfg.get("cloud") or _default_cloud()

    resource = build_signal_resource(
        identity=ident,
        signal_type="metrics",
        dataset=otel_dataset("temporal.cloud_metrics"),
        service_name=service_name,
        service_version="v1",
        host_name=ident.host("otel-collector"),
        cloud=cloud,
        extra_attrs={
            "server.address": f"{account}.tmprl.cloud",
            "server.port": 443,
            "url.scheme": "https",
            "temporal.account": account,
        },
    )

    state = TemporalMetricState(rng, topology)
    logger.info(
        "Temporal OTel generator started (namespace=%s, temporal namespaces=%s)",
        ns, [t["name"] for t in topology],
    )

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=channel_registry,
            generator_id="temporal_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        metrics = _generate_metrics(state, topology, account, rng, active_chaos)
        client.send_metrics_scoped(resource, PROMETHEUS_SCOPE, metrics)

    run_tick_loop(
        stop_event, _emit_tick,
        interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX),
    )
    logger.info("Temporal OTel generator stopped.")


def main() -> None:  # pragma: no cover
    """Standalone entry point for local testing."""
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(name)s %(levelname)s %(message)s")
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
