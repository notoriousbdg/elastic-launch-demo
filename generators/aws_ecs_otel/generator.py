#!/usr/bin/env python3
"""AWS ECS Metrics Generator — sends ECS CloudWatch metrics via OTLP.

Emits data matching the aws_ecs_otel content package
(discovery.datasets: aws.ecs.otel), which populates the bundled ECS
metrics dashboard and alerting rule templates.

Signal types:
  - Metrics: CloudWatch ECS metrics in the AWS/ECS namespace

Metric names use the CloudWatch path format:
  amazonaws.com/AWS/ECS/<MetricName>
Elasticsearch stores each as metrics.amazonaws.com/AWS/ECS/<MetricName>
via the passthrough field mapping.

Attribute keys verified against:
  - elastic/integrations/packages/aws_ecs_otel/kibana/dashboard/*.json
  - elastic/integrations/packages/aws_ecs_otel/kibana/alerting_rule_template/*.json

Dashboard metrics (AWS/ECS, not Container Insights):
  CPUUtilization, MemoryUtilization, ActiveConnectionCount,
  NewConnectionCount, ProcessedBytes, RequestCount

Alert-only metrics: CPUReservation, MemoryReservation

Key attributes: ClusterName, ServiceName, cloud.account.id, cloud.region,
Namespace, MetricName, stat

Usage (standalone):
    python3 -m generators.aws_ecs_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/aws_ecs_otel@0.12.0"

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
# Names verified against aws_ecs_otel dashboard ES|QL queries and alert templates.
# Classic AWS/ECS service metrics (NOT Container Insights).

METRIC_NAMES = (
    "amazonaws.com/AWS/ECS/CPUUtilization",
    "amazonaws.com/AWS/ECS/MemoryUtilization",
    "amazonaws.com/AWS/ECS/ActiveConnectionCount",
    "amazonaws.com/AWS/ECS/NewConnectionCount",
    "amazonaws.com/AWS/ECS/ProcessedBytes",
    "amazonaws.com/AWS/ECS/RequestCount",
    "amazonaws.com/AWS/ECS/LiveTaskCount",
    "amazonaws.com/AWS/ECS/RunningTaskCount",
    "amazonaws.com/AWS/ECS/CPUReservation",
    "amazonaws.com/AWS/ECS/MemoryReservation",
)

ECS_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscloudwatchreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("aws-ecs-generator")

BATCH_INTERVAL_MIN = 30
BATCH_INTERVAL_MAX = 60


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_ecs",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


class ECSServiceState(CounterState):
    """Per-service task health state for ECS metrics."""

    def __init__(self, rng: random.Random, desired_tasks: int = 3):
        super().__init__(rng)
        self.desired_tasks = desired_tasks
        self.running_tasks = desired_tasks

    def tick(
        self,
        desired_tasks: int,
        active_chaos: dict | None = None,
        task_failure_rate: float = 0.0,
    ):
        if active_chaos and task_failure_rate > 0:
            failed = int(desired_tasks * task_failure_rate)
            self.running_tasks = max(0, desired_tasks - failed)
        else:
            self.running_tasks = desired_tasks


def _service_dims(
    cluster_name: str,
    service_name: str,
    region: str,
    account_id: str,
    metric_name: str,
) -> dict:
    return {
        "ClusterName": cluster_name,
        "ServiceName": service_name,
        "Namespace": "AWS/ECS",
        "MetricName": metric_name,
        "cloud.region": region,
        "cloud.account.id": account_id,
    }


def _cluster_dims(
    cluster_name: str,
    region: str,
    account_id: str,
    metric_name: str,
) -> dict:
    """Cluster-level dimensions (no ServiceName) for reservation metrics."""
    return {
        "ClusterName": cluster_name,
        "Namespace": "AWS/ECS",
        "MetricName": metric_name,
        "cloud.region": region,
        "cloud.account.id": account_id,
    }


def _generate_service_metrics(
    cluster_name: str,
    service_name: str,
    state: ECSServiceState,
    desired_tasks: int,
    region: str,
    account_id: str,
    rng: random.Random,
    chaos: dict | None = None,
) -> list[dict]:
    c = chaos or {}
    cpu_util_key = float(c.get("cpu_utilization_high", 0.0))
    mem_util_key = float(c.get("memory_utilization_high", 0.0))

    state.tick(desired_tasks, chaos, 0.0)
    load = business_load()
    metrics: list[dict] = []
    ns = "ECS"

    cpu_avg = min(100.0, round(rng.uniform(8.0, 45.0) * load, 2))
    if cpu_util_key > 0:
        cpu_avg = min(100.0, max(cpu_avg, cpu_util_key))
    cpu_max = min(100.0, round(cpu_avg * rng.uniform(1.05, 1.35), 2))
    cpu_dims = _service_dims(cluster_name, service_name, region, account_id, "CPUUtilization")
    metrics.append(build_cloudwatch_metric(ns, "CPUUtilization", cpu_avg,
                                           cpu_dims, stat="Average", unit="%"))
    metrics.append(build_cloudwatch_metric(ns, "CPUUtilization", cpu_max,
                                           cpu_dims, stat="Maximum", unit="%"))

    mem_avg = min(100.0, round(rng.uniform(15.0, 55.0), 2))
    if mem_util_key > 0:
        mem_avg = min(100.0, max(mem_avg, mem_util_key))
    mem_max = min(100.0, round(mem_avg * rng.uniform(1.05, 1.30), 2))
    mem_dims = _service_dims(cluster_name, service_name, region, account_id, "MemoryUtilization")
    metrics.append(build_cloudwatch_metric(ns, "MemoryUtilization", mem_avg,
                                           mem_dims, stat="Average", unit="%"))
    metrics.append(build_cloudwatch_metric(ns, "MemoryUtilization", mem_max,
                                           mem_dims, stat="Maximum", unit="%"))

    # Active connections — CloudWatch Sum stat (Service Detail dashboard filters stat == "Sum")
    healthy_frac = state.running_tasks / max(1, desired_tasks)
    active_conns = int(rng.randint(20, 400) * load * healthy_frac)
    conn_dims = _service_dims(
        cluster_name, service_name, region, account_id, "ActiveConnectionCount",
    )
    metrics.append(build_cloudwatch_metric(ns, "ActiveConnectionCount", active_conns,
                                           conn_dims, stat="Sum", unit="{connection}"))

    # Task counts — Average stat (Service Detail dashboard Q9/Q10)
    task_dims = _service_dims(cluster_name, service_name, region, account_id, "LiveTaskCount")
    metrics.append(build_cloudwatch_metric(ns, "LiveTaskCount", float(state.running_tasks),
                                           task_dims, stat="Average", unit="{task}"))
    rtask_dims = _service_dims(cluster_name, service_name, region, account_id, "RunningTaskCount")
    metrics.append(build_cloudwatch_metric(ns, "RunningTaskCount", float(state.running_tasks),
                                           rtask_dims, stat="Average", unit="{task}"))

    # New connections / processed bytes — period Sum gauges (CloudWatch Sum stats)
    new_conn_delta = int(rng.randint(5, 80) * load * healthy_frac)
    new_dims = _service_dims(
        cluster_name, service_name, region, account_id, "NewConnectionCount",
    )
    metrics.append(build_cloudwatch_metric(ns, "NewConnectionCount", new_conn_delta,
                                           new_dims, stat="Sum", unit="{connection}"))

    bytes_delta = int(rng.randint(50_000, 2_000_000) * load * healthy_frac)
    bytes_dims = _service_dims(
        cluster_name, service_name, region, account_id, "ProcessedBytes",
    )
    metrics.append(build_cloudwatch_metric(ns, "ProcessedBytes", bytes_delta,
                                           bytes_dims, stat="Sum", unit="By"))

    # Request count — period Sum; the Service Detail "Request count" panel (package 0.12.0)
    # filters stat == "Sum" on this field and renders N/A without it.
    request_delta = int(rng.randint(50, 1_500) * load * healthy_frac)
    request_dims = _service_dims(
        cluster_name, service_name, region, account_id, "RequestCount",
    )
    metrics.append(build_cloudwatch_metric(ns, "RequestCount", request_delta,
                                           request_dims, stat="Sum", unit="{request}"))

    return metrics


def _generate_cluster_metrics(
    cluster_name: str,
    service_states: dict[str, ECSServiceState],
    desired_tasks: int,
    region: str,
    account_id: str,
    rng: random.Random,
    chaos: dict | None = None,
) -> list[dict]:
    """Emit cluster-level CPU/Memory reservation for alert templates."""
    c = chaos or {}
    cpu_res_key = float(c.get("cpu_reservation_high", 0.0))
    mem_res_key = float(c.get("memory_reservation_high", 0.0))
    metrics: list[dict] = []
    ns = "ECS"
    total_desired = desired_tasks * max(1, len(service_states))
    total_running = sum(s.running_tasks for s in service_states.values())
    base_res = min(95.0, 25.0 + (total_running / max(1, total_desired)) * 50.0)
    cpu_res = round(min(100.0, base_res + rng.uniform(-5.0, 8.0)), 2)
    mem_res = round(min(100.0, base_res + rng.uniform(-5.0, 10.0)), 2)
    if cpu_res_key > 0:
        cpu_res = min(100.0, max(cpu_res, cpu_res_key))
    if mem_res_key > 0:
        mem_res = min(100.0, max(mem_res, mem_res_key))

    cpu_dims = _cluster_dims(cluster_name, region, account_id, "CPUReservation")
    metrics.append(build_cloudwatch_metric(ns, "CPUReservation", cpu_res,
                                           cpu_dims, stat="Average", unit="%"))
    mem_dims = _cluster_dims(cluster_name, region, account_id, "MemoryReservation")
    metrics.append(build_cloudwatch_metric(ns, "MemoryReservation", mem_res,
                                           mem_dims, stat="Average", unit="%"))
    return metrics



from generators.aws_ecs_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run ECS metrics generator loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {})
    else:
        ns = NAMESPACE
        cfg = {}

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    service_name_param = cfg.get("service_name", "ecs-metrics")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("ecs")
    cloud = cfg.get("cloud") or _default_cloud()
    region = cloud.get("region", "us-east-1")
    account_id = ScenarioIdentity.from_scenario(scenario_data, ns).aws_account_id
    cluster_name = cfg.get("cluster_name") or ScenarioIdentity(ns).label("cluster")

    default_service_names = [
        ScenarioIdentity(ns).label("api"),
        ScenarioIdentity(ns).label("worker"),
        ScenarioIdentity(ns).label("frontend"),
    ]
    ecs_service_names: list[str] = cfg.get("service_names") or default_service_names

    desired_tasks = 3
    service_states = {svc: ECSServiceState(rng, desired_tasks) for svc in ecs_service_names}


    resource = build_signal_resource(
        ns=ns,
        signal_type="metrics",
        dataset=otel_dataset("aws.ecs"),
        service_name=service_name_param,
        service_version="1.0.0",
        service_instance_id=f"{service_name_param}-001",
        host_name=host_name,
        cloud=cloud,
        extra_attrs={
            "cloud.account.id": account_id,
            "ClusterName": cluster_name,
        },
    )

    scope = {"name": ECS_SCOPE, "version": SCOPE_VERSION}

    logger.info("ECS metrics generator started (namespace=%s, cluster=%s, services=%d)",
                ns, cluster_name, len(ecs_service_names))

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="aws_ecs_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        metrics: list[dict] = []
        for svc, state in service_states.items():
            metrics.extend(_generate_service_metrics(
                cluster_name, svc, state, desired_tasks, region, account_id, rng, active_chaos,
            ))
        metrics.extend(_generate_cluster_metrics(
            cluster_name, service_states, desired_tasks, region, account_id, rng, active_chaos,
        ))
        client.send_metrics_scoped(resource, scope, metrics)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("ECS metrics generator stopped.")


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
