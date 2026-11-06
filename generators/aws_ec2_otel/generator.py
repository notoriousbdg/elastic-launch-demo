#!/usr/bin/env python3
"""AWS EC2 Metrics Generator — sends EC2 CloudWatch metrics via OTLP.

Emits data matching the aws_ec2_otel content package
(discovery.datasets: aws.ec2.otel), which populates the bundled EC2
Overview **and** Instance Detail dashboards.

Signal types:
  - Metrics: CloudWatch EC2 metrics in the AWS/EC2 namespace

Metric names use the CloudWatch path format:
  amazonaws.com/AWS/EC2/<MetricName>
Elasticsearch stores each as metrics.amazonaws.com/AWS/EC2/<MetricName>
via the passthrough field mapping.

Attribute keys verified against:
  - elastic/integrations/packages/aws_ec2_otel/kibana/dashboard/*.json

Dashboard panels query these metrics with CloudWatch ``stat`` attributes:
  - CPUUtilization: Average + Maximum
  - StatusCheckFailed / _System / _Instance / _AttachedEBS: Maximum
  - NetworkIn / NetworkOut: Average + Sum
  - EBSReadBytes / EBSWriteBytes: Average + Sum
  - EBSReadOps: Sum
  - EBSWriteOps: Average + Sum
  - EBSIOBalance%: Average (Instance Detail burst-balance panels)
  - DiskReadBytes / DiskWriteBytes: Sum (Instance Detail disk panels)
  - CPUCreditBalance: Average
  - CPUSurplusCreditsCharged: Sum
  - InstanceEBSIOPSExceededCheck / InstanceEBSThroughputExceededCheck: Maximum

CloudWatch period stats are gauges (not OTLP cumulative counters) so
TS avg_over_time / sum_over_time panels work correctly.

Key attributes: InstanceId, Namespace, MetricName, cloud.account.id,
cloud.region, stat

Usage (standalone):
    python3 -m generators.aws_ec2_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/aws_ec2_otel@0.14.0"

import logging
import os
import random
import signal
import threading

from app.telemetry import OTLPClient
from app.config import NAMESPACE
from generator_engine.db_common import (
    impact_value,
    build_cloudwatch_metric,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

# ── Grounding constants — do not change names ─────────────────────────────────
# Names verified against aws_ec2_otel Overview + Instance Detail dashboards.

METRIC_NAMES = (
    "amazonaws.com/AWS/EC2/CPUUtilization",
    "amazonaws.com/AWS/EC2/NetworkIn",
    "amazonaws.com/AWS/EC2/NetworkOut",
    "amazonaws.com/AWS/EC2/EBSReadBytes",
    "amazonaws.com/AWS/EC2/EBSWriteBytes",
    "amazonaws.com/AWS/EC2/EBSReadOps",
    "amazonaws.com/AWS/EC2/EBSWriteOps",
    "amazonaws.com/AWS/EC2/EBSIOBalance%",
    "amazonaws.com/AWS/EC2/DiskReadBytes",
    "amazonaws.com/AWS/EC2/DiskWriteBytes",
    "amazonaws.com/AWS/EC2/StatusCheckFailed",
    "amazonaws.com/AWS/EC2/StatusCheckFailed_System",
    "amazonaws.com/AWS/EC2/StatusCheckFailed_Instance",
    "amazonaws.com/AWS/EC2/StatusCheckFailed_AttachedEBS",
    "amazonaws.com/AWS/EC2/CPUCreditBalance",
    "amazonaws.com/AWS/EC2/CPUSurplusCreditsCharged",
    "amazonaws.com/AWS/EC2/InstanceEBSIOPSExceededCheck",
    "amazonaws.com/AWS/EC2/InstanceEBSThroughputExceededCheck",
)

EC2_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscloudwatchreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("aws-ec2-generator")

BATCH_INTERVAL_MIN = 30
BATCH_INTERVAL_MAX = 60


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_ec2",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


def _dims(
    instance_id: str,
    region: str,
    account_id: str,
    metric_name: str,
) -> dict[str, str]:
    return {
        "InstanceId": instance_id,
        "Namespace": "AWS/EC2",
        "MetricName": metric_name,
        "cloud.region": region,
        "cloud.account.id": account_id,
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
            "EC2", metric, value, dims, stat=stat, unit=unit,
        ))


def _status_failed(active_chaos: dict | None, key: str) -> int:
    """Map status-check impact keys to Maximum gauge 0/1 (breach above 1.0)."""
    val = float(impact_value(active_chaos, key, default=0.0) or 0.0)
    return 1 if val > 0 else 0


def _generate_instance_metrics(
    instance_id: str,
    region: str,
    account_id: str,
    rng: random.Random,
    active_chaos: dict | None = None,
) -> list[dict]:
    metrics: list[dict] = []
    load = business_load()

    cpu_chaos = float(impact_value(active_chaos, "high_cpu_utilization", default=0.0) or 0.0)
    if active_chaos is None or "low_cpu_credit_balance" not in active_chaos:
        credit_balance = 100.0
    else:
        credit_balance = float(active_chaos["low_cpu_credit_balance"])
    surplus = float(impact_value(active_chaos, "cpu_surplus_credits_charged", default=0.0) or 0.0)
    ebs_read_chaos = float(impact_value(active_chaos, "high_ebs_read_throughput", default=0.0) or 0.0)
    ebs_write_chaos = float(
        impact_value(active_chaos, "high_ebs_write_throughput", default=0.0) or 0.0
    )

    # CPU Utilization — Average + Maximum (peak CPU panels filter Maximum)
    if cpu_chaos > 0:
        cpu_avg = min(100.0, float(cpu_chaos))
    else:
        cpu_avg = min(100.0, rng.uniform(5.0, 60.0) * load)
    cpu_max = min(100.0, cpu_avg * rng.uniform(1.05, 1.35))
    _emit_stats(
        metrics, "CPUUtilization",
        {"Average": round(cpu_avg, 4), "Maximum": round(cpu_max, 4)},
        _dims(instance_id, region, account_id, "CPUUtilization"),
        unit="1",
    )

    # Network I/O — period gauges for Average + Sum (not cumulative counters)
    net_in_avg = int(rng.randint(100_000, 5_000_000) * load)
    net_out_avg = int(rng.randint(50_000, 2_000_000) * load)
    _emit_stats(
        metrics, "NetworkIn",
        {"Average": net_in_avg, "Sum": int(net_in_avg * rng.uniform(1.0, 1.4))},
        _dims(instance_id, region, account_id, "NetworkIn"),
        unit="By",
    )
    _emit_stats(
        metrics, "NetworkOut",
        {"Average": net_out_avg, "Sum": int(net_out_avg * rng.uniform(1.0, 1.4))},
        _dims(instance_id, region, account_id, "NetworkOut"),
        unit="By",
    )

    # EBS volume I/O — Overview + Detail panels (keep read/write non-zero)
    if ebs_read_chaos > 0:
        ebs_read_avg = int(ebs_read_chaos)
    else:
        ebs_read_avg = int(rng.randint(100_000, 5_000_000) * load)
    if ebs_write_chaos > 0:
        ebs_write_avg = int(ebs_write_chaos)
    else:
        ebs_write_avg = int(rng.randint(100_000, 10_000_000) * load)
    _emit_stats(
        metrics, "EBSReadBytes",
        {"Average": ebs_read_avg, "Sum": int(ebs_read_avg * rng.uniform(1.0, 1.4))},
        _dims(instance_id, region, account_id, "EBSReadBytes"),
        unit="By",
    )
    _emit_stats(
        metrics, "EBSWriteBytes",
        {"Average": ebs_write_avg, "Sum": int(ebs_write_avg * rng.uniform(1.0, 1.4))},
        _dims(instance_id, region, account_id, "EBSWriteBytes"),
        unit="By",
    )

    ebs_read_ops = int(rng.randint(10, 500) * load)
    ebs_write_ops_avg = int(rng.randint(10, 2_000) * load)
    metrics.append(build_cloudwatch_metric(
        "EC2", "EBSReadOps", ebs_read_ops,
        _dims(instance_id, region, account_id, "EBSReadOps"),
        stat="Sum", unit="1",
    ))
    _emit_stats(
        metrics, "EBSWriteOps",
        {"Average": ebs_write_ops_avg, "Sum": int(ebs_write_ops_avg * rng.uniform(1.0, 1.4))},
        _dims(instance_id, region, account_id, "EBSWriteOps"),
        unit="1",
    )

    # Instance Detail — EBS IO burst balance (CloudWatch name includes '%')
    ebs_io_balance = round(rng.uniform(70.0, 100.0), 2)
    metrics.append(build_cloudwatch_metric(
        "EC2", "EBSIOBalance%", ebs_io_balance,
        _dims(instance_id, region, account_id, "EBSIOBalance%"),
        stat="Average", unit="1",
    ))

    # Instance Detail — classic instance-store Disk* metrics (Sum)
    disk_read = int(rng.randint(50_000, 2_000_000) * load)
    disk_write = int(rng.randint(50_000, 3_000_000) * load)
    metrics.append(build_cloudwatch_metric(
        "EC2", "DiskReadBytes", disk_read,
        _dims(instance_id, region, account_id, "DiskReadBytes"),
        stat="Sum", unit="By",
    ))
    metrics.append(build_cloudwatch_metric(
        "EC2", "DiskWriteBytes", disk_write,
        _dims(instance_id, region, account_id, "DiskWriteBytes"),
        stat="Sum", unit="By",
    ))

    # Status checks — overall + per-type Maximum gauges
    sys_failed = _status_failed(active_chaos, "system_status_check_failed")
    inst_failed = _status_failed(active_chaos, "instance_status_check_failed")
    ebs_failed = _status_failed(active_chaos, "ebs_attached_volume_status_check_failed")
    overall_failed = 1 if (sys_failed or inst_failed or ebs_failed) else 0
    for name, value in (
        ("StatusCheckFailed", overall_failed),
        ("StatusCheckFailed_System", sys_failed),
        ("StatusCheckFailed_Instance", inst_failed),
        ("StatusCheckFailed_AttachedEBS", ebs_failed),
    ):
        metrics.append(build_cloudwatch_metric(
            "EC2", name, value,
            _dims(instance_id, region, account_id, name),
            stat="Maximum", unit="1",
        ))

    # Burstable CPU credits
    metrics.append(build_cloudwatch_metric(
        "EC2", "CPUCreditBalance", round(credit_balance, 2),
        _dims(instance_id, region, account_id, "CPUCreditBalance"),
        stat="Average", unit="1",
    ))
    metrics.append(build_cloudwatch_metric(
        "EC2", "CPUSurplusCreditsCharged", round(surplus, 2),
        _dims(instance_id, region, account_id, "CPUSurplusCreditsCharged"),
        stat="Sum", unit="1",
    ))

    # EBS performance-cap exceeded checks (0 = healthy)
    metrics.append(build_cloudwatch_metric(
        "EC2", "InstanceEBSIOPSExceededCheck", 0,
        _dims(instance_id, region, account_id, "InstanceEBSIOPSExceededCheck"),
        stat="Maximum", unit="1",
    ))
    metrics.append(build_cloudwatch_metric(
        "EC2", "InstanceEBSThroughputExceededCheck", 0,
        _dims(instance_id, region, account_id, "InstanceEBSThroughputExceededCheck"),
        stat="Maximum", unit="1",
    ))

    return metrics



from generators.aws_ec2_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run EC2 metrics generator loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {})
    else:
        ns = NAMESPACE
        cfg = {}

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("ec2-metrics")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("ec2")
    cloud = cfg.get("cloud") or _default_cloud()
    region = cloud.get("region", "us-east-1")
    account_id = ScenarioIdentity.from_scenario(scenario_data, ns).aws_account_id

    # Synthetic instance IDs
    default_instance_ids = [
        f"i-{''.join(rng.choices('abcdef0123456789', k=17))}" for _ in range(3)
    ]
    instance_ids: list[str] = cfg.get("instance_ids") or default_instance_ids


    resource = build_signal_resource(
        ns=ns,
        signal_type="metrics",
        dataset=otel_dataset("aws.ec2"),
        service_name=service_name,
        service_version="1.0.0",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name,
        cloud=cloud,
        extra_attrs={"cloud.account.id": account_id},
    )

    scope = {"name": EC2_SCOPE, "version": SCOPE_VERSION}

    logger.info("EC2 metrics generator started (namespace=%s, instances=%d)", ns, len(instance_ids))

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="aws_ec2_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        metrics: list[dict] = []
        for iid in instance_ids:
            metrics.extend(_generate_instance_metrics(
                iid, region, account_id, rng, active_chaos,
            ))
        client.send_metrics_scoped(resource, scope, metrics)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("EC2 metrics generator stopped.")


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
