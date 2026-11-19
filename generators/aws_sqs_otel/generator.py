#!/usr/bin/env python3
"""AWS SQS Metrics Generator — sends SQS CloudWatch metrics via OTLP.

Emits data matching the aws_sqs_otel content package
(discovery.datasets: aws.sqs.otel), which populates the bundled SQS
Queue Overview and DLQ Monitoring dashboards.

Signal types:
  - Metrics: CloudWatch SQS metrics in the AWS/SQS namespace

Metric names use the CloudWatch path format:
  amazonaws.com/AWS/SQS/<MetricName>
Elasticsearch stores each as metrics.amazonaws.com/AWS/SQS/<MetricName>
via the passthrough field mapping.

Attribute keys verified against:
  - elastic/integrations/packages/aws_sqs_otel/kibana/dashboard/*.json

Key attributes: QueueName, cloud.account.id, cloud.region, stat

Alerting rule grounding:
  - aws_sqs_otel-dlq-messages-present.json:  ApproximateNumberOfMessagesVisible
    on the DLQ queue (QueueName ends with "_dlq" or "-dlq").

Usage (standalone):
    python3 -m generators.aws_sqs_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/aws_sqs_otel@0.13.0"

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
# Names verified against aws_sqs_otel dashboard ES|QL queries.

METRIC_NAMES = (
    "amazonaws.com/AWS/SQS/NumberOfMessagesSent",
    "amazonaws.com/AWS/SQS/NumberOfMessagesReceived",
    "amazonaws.com/AWS/SQS/NumberOfMessagesDeleted",
    "amazonaws.com/AWS/SQS/ApproximateNumberOfMessagesVisible",
    "amazonaws.com/AWS/SQS/ApproximateNumberOfMessagesNotVisible",
    "amazonaws.com/AWS/SQS/ApproximateAgeOfOldestMessage",
    "amazonaws.com/AWS/SQS/NumberOfEmptyReceives",
)

SQS_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscloudwatchreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("aws-sqs-generator")

BATCH_INTERVAL_MIN = 30
BATCH_INTERVAL_MAX = 60


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_sqs",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


class SQSQueueState(CounterState):
    """Per-interval CloudWatch Sum deltas for a single SQS queue.

    Dashboard panels use ``sum_over_time`` on NumberOfMessages* with
    ``stat == "Sum"``. Those queries reject OTLP cumulative counters
    (``counter_long``); CloudWatch Sum stats are period deltas, so emit
    gauges with the per-scrape increment.
    """

    def __init__(self, rng: random.Random):
        super().__init__(rng)

    def tick(self, rng: random.Random, active_chaos: dict | None = None) -> dict[str, int]:
        load = business_load()
        c = active_chaos or {}
        sent = int(rng.randint(10, 5_000) * load)
        # OOTB "[AWS SQS OTel] Producer send rate drop" uses
        # SUM(SUM_OVER_TIME(NumberOfMessagesSent)) WHERE total_sent < 1
        # (30m window). SPEC default 100.0 = healthy; GT sets e.g. 10.0 on
        # the probe channel. Any value below the healthy default means
        # "drop active" — emit zero sends so the window drains to < 1.
        # (resolve_active_chaos fills the default 100.0 on sibling channels,
        # so backlog/in-flight/age chaos does not accidentally zero sends.)
        if float(c.get("producer_send_rate_drop", 100.0)) < 100.0:
            sent = 0
        consumed = int(sent * rng.uniform(0.8, 1.0)) if sent else 0
        backlog = float(c.get("high_backlog", 0.0))
        if backlog > 0 and sent > 0:
            consumed = int(sent * rng.uniform(0.1, 0.5))
        received = max(0, consumed) if sent == 0 else max(1, consumed)
        deleted = max(0, consumed - rng.randint(0, 5)) if consumed else 0
        empty = int(rng.randint(1, 50) * load)
        return {
            "sent": sent,
            "received": received,
            "deleted": deleted,
            "empty": empty,
        }


def _emit_stats(
    metrics: list[dict],
    metric: str,
    stats: dict[str, int | float],
    dims: dict,
    unit: str,
) -> None:
    """Emit one CloudWatch period gauge per ``{stat: value}`` entry."""
    for stat, value in stats.items():
        metrics.append(build_cloudwatch_metric(
            "SQS", metric, value, dims, stat=stat, unit=unit,
        ))


def _generate_queue_metrics(
    queue_name: str,
    state: SQSQueueState,
    region: str,
    account_id: str,
    rng: random.Random,
    active_chaos: dict | None = None,
    dlq_message_count: int = 0,
) -> list[dict]:
    c = active_chaos or {}
    depth_mult = 1.0
    age_mult = 1.0
    backlog = float(c.get("high_backlog", 0.0))
    in_flight = float(c.get("in_flight_saturation", 0.0))
    oldest_age = float(c.get("oldest_message_age_high", 0.0))
    if backlog > 0:
        depth_mult = max(depth_mult, backlog / 1000.0)
    if oldest_age > 0:
        age_mult = max(age_mult, oldest_age / 60.0)

    deltas = state.tick(rng, active_chaos)
    load = business_load()
    metrics: list[dict] = []

    dims = {
        "QueueName": queue_name,
        "cloud.region": region,
        "cloud.account.id": account_id,
    }

    # Period deltas as gauges with CloudWatch Sum stat (not cumulative counters).
    metrics.append(build_cloudwatch_metric("SQS", "NumberOfMessagesSent",
                                           deltas["sent"], dims, stat="Sum", unit="1"))
    metrics.append(build_cloudwatch_metric("SQS", "NumberOfMessagesReceived",
                                           deltas["received"], dims, stat="Sum", unit="1"))
    metrics.append(build_cloudwatch_metric("SQS", "NumberOfMessagesDeleted",
                                           deltas["deleted"], dims, stat="Sum", unit="1"))
    metrics.append(build_cloudwatch_metric("SQS", "NumberOfEmptyReceives",
                                           deltas["empty"], dims, stat="Sum", unit="1"))

    # Queue depth — gauge.
    # OOTB High backlog filters stat=="Average" (non-DLQ); DLQ has messages
    # filters stat=="Maximum" (QueueName LIKE *dlq*). Emit both stats so each
    # rule sees the series it expects (CloudWatch scrapes often carry both).
    is_dlq = queue_name.endswith("_dlq") or queue_name.endswith("-dlq")
    if is_dlq:
        # DLQ visible count stays at 0 (healthy) unless dlq_has_messages chaos
        # is active — any occurrence breaches the "DLQ has messages" alert.
        base_visible = dlq_message_count
    else:
        base_visible = int(rng.randint(0, 50) * load * depth_mult)
        if backlog > 0:
            # Alert WHERE backlog > 1000 — clamp strictly above if GT sets boundary.
            target = int(backlog)
            if target <= 1000:
                target = 1001
            base_visible = max(base_visible, target)
    _emit_stats(
        metrics, "ApproximateNumberOfMessagesVisible",
        {"Average": base_visible, "Maximum": base_visible}, dims, unit="1",
    )
    not_visible = int(rng.randint(0, 10) * depth_mult)
    if in_flight > 0:
        # Alert WHERE in_flight > 100000 — clamp strictly above boundary.
        target = int(in_flight)
        if target <= 100000:
            target = 100001
        not_visible = max(not_visible, target)
    # Alert (in-flight saturation) filters stat=="Average"; dashboard top-10
    # panel filters stat=="Maximum" — emit both so each consumer sees its series.
    _emit_stats(
        metrics, "ApproximateNumberOfMessagesNotVisible",
        {"Average": not_visible, "Maximum": not_visible}, dims, unit="1",
    )

    base_age = rng.uniform(0.0, 5.0)
    if oldest_age > 0:
        # Alert WHERE max_age_seconds > 300 — clamp strictly above boundary.
        target = float(oldest_age)
        if target <= 300.0:
            target = 301.0
        base_age = max(base_age, target)
    # OOTB Oldest message age high filters stat=="Maximum"; the package ML job
    # (queue backlog anomaly) reads Average.
    age = round(base_age * age_mult, 1)
    metrics.append(build_cloudwatch_metric("SQS", "ApproximateAgeOfOldestMessage",
                                           age, dims, stat="Maximum", unit="s"))
    metrics.append(build_cloudwatch_metric("SQS", "ApproximateAgeOfOldestMessage",
                                           round(age * 0.8, 1), dims, stat="Average", unit="s"))

    return metrics



from generators.aws_sqs_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def _queue_names(ns: str, cfg: dict) -> list[str]:
    """SQS ``QueueName`` dimensions, from config or scenario identity."""
    return cfg.get("queue_names") or [
        ScenarioIdentity(ns).label("orders"),
        ScenarioIdentity(ns).label("events"),
        ScenarioIdentity(ns).label("orders-dlq"),  # Dead-letter queue — triggers DLQ alerting rule
    ]


def workflow_input_values(scenario_data: dict | None) -> dict[str, str]:
    """Values the library investigation workflows take as inputs.

    The deployer fills matching manual-trigger inputs with them (and the literal
    fallbacks alert-triggered runs use); see ``elastic_config/workflow_templates.py``.
    """
    ns = (scenario_data or {}).get("namespace") or NAMESPACE
    cfg = (scenario_data or {}).get("generator_config") or {}
    queues = [q for q in _queue_names(ns, cfg) if "dlq" not in q.lower()] or _queue_names(ns, cfg)
    return {"queue_name": queues[0]}


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run SQS metrics generator loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {})
    else:
        ns = NAMESPACE
        cfg = {}

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("sqs-metrics")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("sqs")
    cloud = cfg.get("cloud") or _default_cloud()
    region = cloud.get("region", "us-east-1")
    account_id = ScenarioIdentity.from_scenario(scenario_data, ns).aws_account_id

    queue_names = _queue_names(ns, cfg)

    queue_states = {q: SQSQueueState(rng) for q in queue_names}


    resource = build_signal_resource(
        ns=ns,
        signal_type="metrics",
        dataset=otel_dataset("aws.sqs"),
        service_name=service_name,
        service_version="1.0.0",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name,
        cloud=cloud,
        extra_attrs={"cloud.account.id": account_id},
    )

    scope = {"name": SQS_SCOPE, "version": SCOPE_VERSION}

    logger.info("SQS metrics generator started (namespace=%s, queues=%d)", ns, len(queue_names))

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="aws_sqs_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        dlq_count = int(impact_value(active_chaos, "dlq_has_messages", default=0))
        if float((active_chaos or {}).get("dlq_has_messages", 0.0)) > 0:
            dlq_count = max(dlq_count, 1)

        metrics: list[dict] = []
        for q, state in queue_states.items():
            metrics.extend(_generate_queue_metrics(
                q, state, region, account_id, rng, active_chaos, dlq_count,
            ))
        client.send_metrics_scoped(resource, scope, metrics)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("SQS metrics generator stopped.")


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
