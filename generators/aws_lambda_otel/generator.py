#!/usr/bin/env python3
"""AWS Lambda Metrics Generator — sends Lambda CloudWatch metrics via OTLP.

Emits data matching the aws_lambda_otel content package
(discovery.datasets: aws.lambda.otel), which populates the bundled Lambda
Overview dashboard.

Signal types:
  - Metrics: CloudWatch Lambda metrics in the AWS/Lambda namespace

Metric names use the CloudWatch path format:
  amazonaws.com/AWS/Lambda/<MetricName>
Elasticsearch stores each as metrics.amazonaws.com/AWS/Lambda/<MetricName>
via the passthrough field mapping.

Attribute keys verified against:
  - elastic/integrations/packages/aws_lambda_otel/kibana/dashboard/*.json

Key attributes: FunctionName, cloud.account.id, cloud.region, stat

Dashboard panels filter ``WHERE Resource IS NULL``. Do NOT emit a Resource
dimension — that is the alias/version dimension set, and it blanks every panel.

CloudWatch Sum stats must be period-delta gauges (not OTLP cumulative counters).
Dashboard TS queries use ``sum_over_time`` which rejects ``counter_long``.

Usage (standalone):
    python3 -m generators.aws_lambda_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/aws_lambda_otel@0.12.0"

import logging
import os
import random
import signal
import threading

from app.telemetry import OTLPClient
from app.config import NAMESPACE
from generator_engine.db_common import (
    CounterState,
    build_cloudwatch_metric,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

# ── Grounding constants — do not change names ─────────────────────────────────
# Names verified against aws_lambda_otel dashboard ES|QL queries.

METRIC_NAMES = (
    "amazonaws.com/AWS/Lambda/Invocations",
    "amazonaws.com/AWS/Lambda/Errors",
    "amazonaws.com/AWS/Lambda/Throttles",
    "amazonaws.com/AWS/Lambda/Duration",
    "amazonaws.com/AWS/Lambda/ConcurrentExecutions",
    "amazonaws.com/AWS/Lambda/AsyncEventsDropped",
    "amazonaws.com/AWS/Lambda/AsyncEventsReceived",
    "amazonaws.com/AWS/Lambda/AsyncEventAge",
    "amazonaws.com/AWS/Lambda/IteratorAge",
    "amazonaws.com/AWS/Lambda/DeadLetterErrors",
    "amazonaws.com/AWS/Lambda/DestinationDeliveryFailures",
)

# Package alert thresholds (aws_lambda_otel@0.12.0): average > 3000 ms, tail > 10000 ms.
_AVG_FAULT_CAP_MS = 9_000.0
_TAIL_QUIET_CAP_MS = 9_900.0

LAMBDA_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscloudwatchreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("aws-lambda-generator")

BATCH_INTERVAL_MIN = 30
BATCH_INTERVAL_MAX = 60


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_lambda",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


class LambdaFunctionState(CounterState):
    """Per-interval CloudWatch Sum deltas for a single Lambda function.

    Dashboard panels use ``sum_over_time`` on Invocations/Errors/Throttles/
    AsyncEventsDropped with ``stat == "Sum"``. Those queries reject OTLP
    cumulative counters (``counter_long``); CloudWatch Sum stats are period
    deltas, so emit gauges with the per-scrape increment.
    """

    def __init__(self, rng: random.Random):
        super().__init__(rng)

    def tick(
        self,
        rng: random.Random,
        active_chaos: dict | None = None,
        error_rate: float = 0.0,
        throttle_rate: float = 0.0,
    ) -> dict[str, int]:
        load = business_load()
        invocations = int(rng.randint(10, 500) * load)
        if error_rate > 0:
            # The package alert only evaluates windows with more than 10 invocations;
            # at low load the random draw can fall short, so an error fault guarantees it.
            invocations = max(invocations, 11)
        errors = int(invocations * error_rate) if error_rate > 0 else 0
        # OOTB "[AWS Lambda OTel] High throttle rate" computes
        #   ROUND(total_throttles / denominator * 100.0, 2)
        # with long SUM gauges. ES|QL integer-divides long/long → 0 whenever
        # invocations > 0, so the WHERE throttle_rate_pct > 0.5 never matches.
        # Work around without editing the template: during throttle chaos emit
        # Invocations=0 and a non-zero Throttles count so denominator ==
        # throttles and rate becomes 100.
        if throttle_rate > 0:
            if throttle_rate <= 1.0:
                throttles = max(20, int(invocations * throttle_rate))
            else:
                # GT channels pass absolute-ish counts (e.g. 50.0).
                throttles = max(20, int(throttle_rate))
            invocations = 0
        else:
            throttles = 0
        async_dropped = 0
        if active_chaos and rng.random() < 0.2:
            async_dropped = rng.randint(1, 20)
        elif rng.random() < 0.05:
            async_dropped = rng.randint(0, 3)
        return {
            "invocations": invocations,
            "errors": errors,
            "throttles": throttles,
            "async_dropped": async_dropped,
        }


def _generate_function_metrics(
    function_name: str,
    state: LambdaFunctionState,
    region: str,
    account_id: str,
    rng: random.Random,
    active_chaos: dict | None = None,
) -> list[dict]:
    c = active_chaos or {}
    error_rate = float(c.get("high_error_rate", 0.0))
    throttle_rate = float(c.get("high_throttle_rate", 0.0))
    avg_duration = float(c.get("high_average_duration", 1.0))
    tail_duration = float(c.get("high_tail_duration", 1.0))
    async_age = float(c.get("high_async_event_age", 0.0))
    iterator_age = float(c.get("high_iterator_age", 0.0))
    concurrent_key = float(c.get("high_concurrent_executions", 0.0))
    dlq_errors = float(c.get("dead_letter_errors", 0.0))
    dest_failures = float(c.get("destination_delivery_failures", 0.0))

    deltas = state.tick(rng, active_chaos, error_rate, throttle_rate)
    # DeadLetterErrors is its own CloudWatch metric; it must not also raise Errors, or
    # the DLQ fault trips the error-rate alert whenever invocations are low.
    if dest_failures > 0:
        deltas["async_dropped"] = max(deltas["async_dropped"], int(dest_failures))
    load = business_load()
    metrics: list[dict] = []

    # Function-level dimensions only. Do not set Resource — dashboards filter
    # ``Resource IS NULL`` to select the FunctionName-only CloudWatch series.
    dims = {
        "FunctionName": function_name,
        "cloud.region": region,
        "cloud.account.id": account_id,
    }

    # Period deltas as gauges with CloudWatch Sum stat (not cumulative counters).
    metrics.append(build_cloudwatch_metric("Lambda", "Invocations", deltas["invocations"],
                                           dims, stat="Sum", unit="{count}"))
    metrics.append(build_cloudwatch_metric("Lambda", "Errors", deltas["errors"],
                                           dims, stat="Sum", unit="{count}"))
    metrics.append(build_cloudwatch_metric("Lambda", "Throttles", deltas["throttles"],
                                           dims, stat="Sum", unit="{count}"))
    metrics.append(build_cloudwatch_metric("Lambda", "AsyncEventsDropped",
                                           deltas["async_dropped"],
                                           dims, stat="Sum", unit="{count}"))
    async_received = deltas["invocations"] + deltas["async_dropped"]
    metrics.append(build_cloudwatch_metric("Lambda", "AsyncEventsReceived",
                                           async_received,
                                           dims, stat="Sum", unit="{count}"))

    # Duration — Average + Maximum + Sum (dashboard panels filter each)
    # The average and tail alerts must be drivable independently, but Maximum can never
    # be below Average. So the average fault is capped under the 10000 ms tail threshold,
    # and the tail fault raises only Maximum.
    base_duration = rng.uniform(50.0, 800.0) * load
    avg_ms = base_duration
    if avg_duration > 1.0:
        avg_ms = max(base_duration, min(avg_duration, _AVG_FAULT_CAP_MS))
    max_ms = avg_ms * rng.uniform(1.1, 2.5)
    if tail_duration > 1.0:
        max_ms = max(max_ms, tail_duration)
    elif avg_duration > 1.0:
        max_ms = max(avg_ms, min(max_ms, _TAIL_QUIET_CAP_MS))
    dur_avg = round(avg_ms, 2)
    dur_max = round(max_ms, 2)
    dur_sum = round(avg_ms * max(1, deltas["invocations"]), 2)
    metrics.append(build_cloudwatch_metric("Lambda", "Duration",
                                           dur_avg, dims, stat="Average", unit="ms"))
    metrics.append(build_cloudwatch_metric("Lambda", "Duration",
                                           dur_max, dims, stat="Maximum", unit="ms"))
    metrics.append(build_cloudwatch_metric("Lambda", "Duration",
                                           dur_sum, dims, stat="Sum", unit="ms"))

    # Concurrent executions — gauge
    concurrent = int(rng.randint(1, 50) * load)
    if concurrent_key > 0:
        concurrent = max(concurrent, int(concurrent_key))
    # Always emit age gauges so OOTB alert ES|QL columns exist (chaos raises them).
    async_age_ms = round(async_age, 1) if async_age > 0 else round(rng.uniform(10.0, 500.0), 1)
    # Average feeds the detail dashboard; Maximum feeds the "High async event age" alert.
    async_age_max_ms = round(async_age_ms * (1.0 if async_age > 0 else rng.uniform(1.0, 1.5)), 1)
    iterator_age_ms = round(iterator_age, 1) if iterator_age > 0 else round(rng.uniform(10.0, 1_000.0), 1)
    metrics.append(build_cloudwatch_metric(
        "Lambda", "AsyncEventAge", async_age_ms, dims, stat="Average", unit="ms",
    ))
    metrics.append(build_cloudwatch_metric(
        "Lambda", "AsyncEventAge", async_age_max_ms, dims, stat="Maximum", unit="ms",
    ))
    metrics.append(build_cloudwatch_metric(
        "Lambda", "IteratorAge", iterator_age_ms, dims, stat="Maximum", unit="ms",
    ))
    # Alert reads Maximum; the package ML job (performance anomaly) reads Average.
    metrics.append(build_cloudwatch_metric("Lambda", "ConcurrentExecutions",
                                           concurrent, dims, stat="Maximum", unit="1"))
    metrics.append(build_cloudwatch_metric("Lambda", "ConcurrentExecutions",
                                           round(concurrent * 0.8, 1), dims, stat="Average", unit="1"))

    # Dedicated DLQ / destination metrics (OOTB alerts filter these MetricNames).
    # Healthy baseline stays at 0 — any occurrence breaches the alert.
    dlq_count = int(dlq_errors) if dlq_errors > 0 else 0
    dest_count = int(dest_failures) if dest_failures > 0 else 0
    metrics.append(build_cloudwatch_metric(
        "Lambda", "DeadLetterErrors", dlq_count, dims, stat="Sum", unit="{count}",
    ))
    metrics.append(build_cloudwatch_metric(
        "Lambda", "DestinationDeliveryFailures", dest_count, dims, stat="Sum", unit="{count}",
    ))

    return metrics



from generators.aws_lambda_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop

def _function_names(ns: str, cfg: dict) -> list[str]:
    """Lambda ``FunctionName`` dimensions, from config or scenario identity."""
    return cfg.get("function_names") or [
        ScenarioIdentity(ns).label("api-handler"),
        ScenarioIdentity(ns).label("event-processor"),
        ScenarioIdentity(ns).label("data-transformer"),
    ]


def workflow_input_values(scenario_data: dict | None) -> dict[str, str]:
    """Values the library investigation workflows take as inputs.

    The deployer fills matching manual-trigger inputs with them (and the literal
    fallbacks alert-triggered runs use); see ``elastic_config/workflow_templates.py``.
    """
    ns = (scenario_data or {}).get("namespace") or NAMESPACE
    cfg = (scenario_data or {}).get("generator_config") or {}
    return {"function_name": _function_names(ns, cfg)[0]}


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run Lambda metrics generator loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {})
    else:
        ns = NAMESPACE
        cfg = {}

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("lambda-metrics")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("lambda")
    cloud = cfg.get("cloud") or _default_cloud()
    region = cloud.get("region", "us-east-1")
    account_id = ScenarioIdentity.from_scenario(scenario_data, ns).aws_account_id

    function_names = _function_names(ns, cfg)

    # One state per function
    function_states = {fn: LambdaFunctionState(rng) for fn in function_names}


    resource = build_signal_resource(
        ns=ns,
        signal_type="metrics",
        dataset=otel_dataset("aws.lambda"),
        service_name=service_name,
        service_version="1.0.0",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name,
        cloud=cloud,
        extra_attrs={
            "cloud.account.id": account_id,
            "FunctionName": function_names[0] if function_names else ScenarioIdentity(ns).label("function"),
        },
    )

    scope = {"name": LAMBDA_SCOPE, "version": SCOPE_VERSION}

    logger.info("Lambda metrics generator started (namespace=%s, functions=%d)",
                ns, len(function_names))

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="aws_lambda_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        metrics: list[dict] = []
        for fn, state in function_states.items():
            metrics.extend(_generate_function_metrics(
                fn, state, region, account_id, rng, active_chaos,
            ))
        client.send_metrics_scoped(resource, scope, metrics)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("Lambda metrics generator stopped.")


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
