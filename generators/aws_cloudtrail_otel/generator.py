#!/usr/bin/env python3
"""AWS CloudTrail Log Generator — sends CloudTrail API audit logs via OTLP.

Emits data matching the aws_cloudtrail_otel content package
(discovery.datasets: aws.cloudtrail.otel), which populates the bundled
CloudTrail dashboards.

Signal types:
  - Logs: CloudTrail management and data event records

Field names verified from aws_cloudtrail_otel Lens dashboard JSON:
  rpc.system, rpc.service, rpc.method,
  aws.access_key.id, aws.error.code, source.address, outcome,
  user_agent.original

Usage (standalone):
    python3 -m generators.aws_cloudtrail_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/aws_cloudtrail_otel@0.3.0"

import logging
import os
import random
import signal
import threading
import time

from app.telemetry import OTLPClient
from app.config import NAMESPACE
from generator_engine.db_common import (
    build_audit_log_record,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

# ── Grounding constants — do not change names ─────────────────────────────────

CLOUDTRAIL_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("cloudtrail-generator")
BATCH_INTERVAL_MIN = 5
BATCH_INTERVAL_MAX = 15

# Common AWS API calls by service
_API_CALLS = [
    ("aws-api", "S3", "GetObject"),
    ("aws-api", "S3", "PutObject"),
    ("aws-api", "S3", "ListBuckets"),
    ("aws-api", "EC2", "DescribeInstances"),
    ("aws-api", "EC2", "RunInstances"),
    ("aws-api", "EC2", "StopInstances"),
    ("aws-api", "IAM", "GetUser"),
    ("aws-api", "IAM", "CreatePolicy"),
    ("aws-api", "IAM", "AttachUserPolicy"),
    ("aws-api", "RDS", "DescribeDBInstances"),
    ("aws-api", "Lambda", "InvokeFunction"),
    ("aws-api", "KMS", "Decrypt"),
    ("aws-api", "STS", "AssumeRole"),
    ("aws-api", "CloudWatch", "PutMetricData"),
]

_ERROR_CODES = [
    "AccessDenied", "UnauthorizedOperation", "Throttling",
    "InvalidParameterValue", "NoSuchBucket", "NoSuchEntity",
]

# Real CloudTrail userAgent values. The "Logs by user agent" panel parses
# user_agent.original with REPLACE(... ^\[|\/.*$|\ -\ .*$ ...) so the first
# token before '/' (or the whole string) becomes the donut label.
_USER_AGENTS = [
    "console.amazonaws.com",
    "[aws-cli/2.15.0 Python/3.11.6 Linux/5.15.0 exe-command/2.2.0]",
    "aws-sdk-go/1.44.200 (go1.19; linux; amd64)",
    "Boto3/1.34.0 Python/3.11.6 Linux/5.15.0 Botocore/1.34.0",
    "aws-sdk-java/2.20.0 Linux/5.15.0 OpenJDK_64-Bit_Server_VM/17.0.9+9 java/17.0.9",
    "aws-sdk-nodejs/2.1490.0 linux/v18.18.0 callback",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Terraform/1.6.0 (+https://www.terraform.io) aws-sdk-go/1.44.0",
]


def _default_cloud() -> dict:
    return {"provider": "aws", "platform": "aws_cloudtrail",
            "region": "us-east-1", "availability_zone": "us-east-1a"}


def _generate_records(
    principals: list[str],
    account_id: str,
    region: str,
    rng: random.Random,
    count: int,
    error_rate: float = 0.0,
    batch_interval_max: float = BATCH_INTERVAL_MAX,
) -> list[dict]:
    now_ns = int(time.time() * 1e9)
    records = []
    for _ in range(count):
        principal = rng.choice(principals)
        rpc_system, rpc_service, rpc_method = rng.choice(_API_CALLS)

        is_error = rng.random() < max(error_rate, 0.005)
        outcome = "failure" if is_error else "success"
        severity = "WARN" if is_error else "INFO"
        error_code = rng.choice(_ERROR_CODES) if is_error else None

        jitter = rng.randint(0, int(batch_interval_max * 1e9))
        extra: dict = {
            "rpc.system": rpc_system,
            "rpc.service": rpc_service,
            "rpc.method": rpc_method,
            "aws.access_key.id": f"AKIA{''.join(rng.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=16))}",
            "cloud.account.id": account_id,
            "cloud.region": region,
            "user_agent.original": rng.choice(_USER_AGENTS),
        }
        if error_code:
            extra["aws.error.code"] = error_code

        records.append(build_audit_log_record(
            actor=principal,
            action=f"{rpc_service}:{rpc_method}",
            outcome=outcome,
            source_address=f"203.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}",
            event_name="aws.cloudtrail.event",
            severity=severity,
            extra_attrs=extra,
            jitter_ns=jitter,
        ))
    return records



from generators.aws_cloudtrail_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("cloudtrail")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("cloudtrail")
    cloud = cfg.get("cloud") or _default_cloud()
    region = cloud.get("region", "us-east-1")
    account_id = ScenarioIdentity.from_scenario(scenario_data, ns).aws_account_id
    records_per_cycle: int = cfg.get("records_per_cycle", 10)

    default_principals = [
        f"arn:aws:iam::{account_id}:user/{ScenarioIdentity(ns).label(role)}"
        for role in ("admin", "developer", "readonly", "ci-bot", "infra-svc")
    ]
    principals: list[str] = cfg.get("principals") or default_principals


    resource = build_signal_resource(
        ns=ns, signal_type="logs", dataset=otel_dataset("aws.cloudtrail"),
        service_name=service_name, service_version="1.0.0",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name, cloud=cloud,
        extra_attrs={"cloud.account.id": account_id},
    )
    scope = {"name": CLOUDTRAIL_SCOPE, "version": SCOPE_VERSION}
    logger.info("CloudTrail log generator started (namespace=%s)", ns)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="aws_cloudtrail_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        error_rate = float((active_chaos or {}).get("error_rate", 0.0))
        n = int((active_chaos or {}).get("records_per_cycle", records_per_cycle))
        load = business_load()
        n = max(1, int(n * load))

        records = _generate_records(principals, account_id, region, rng, n, error_rate)
        client.send_logs_scoped(resource, scope, records)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("CloudTrail log generator stopped.")


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
