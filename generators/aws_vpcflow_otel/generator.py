#!/usr/bin/env python3
"""AWS VPC Flow Log Generator — sends AWS VPC flow logs via OTLP.

Generates logs that populate:
  - [AWS VPC OTEL] VPC Flow Logs Overview dashboard
  - [AWS VPC OTEL] Traffic Analysis dashboard
  - [AWS VPC OTEL] Interface Analysis dashboard

Usage (standalone):
    python3 -m generators.aws_vpcflow_otel.generator
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/aws_vpcflow_otel@0.4.0"

import logging
import os
import random
import signal
import threading

from app.telemetry import OTLPClient, _format_attributes, SCHEMA_URL, _now_ns
from app.config import NAMESPACE
from generator_engine.db_common import build_signal_resource, resolve_active_chaos
from generator_engine.identity import ScenarioIdentity

logger = logging.getLogger("aws-vpc-flow-generator")

FLOW_INTERVAL = int(os.getenv("VPC_FLOW_INTERVAL", "5"))
BATCH_SIZE_MIN = 10
BATCH_SIZE_MAX = 20

SCOPE_VERSION = "1.0.0"

_INTERNAL_IPS = [
    "10.0.1.42", "10.0.1.100", "10.0.2.15", "10.0.2.88",
    "10.0.3.22", "10.0.3.55", "10.0.4.10", "10.0.4.77",
    "10.0.5.33", "10.0.5.99",
]
_EXTERNAL_IPS = [
    "203.0.113.10", "203.0.113.25", "198.51.100.44", "198.51.100.77",
    "192.0.2.100", "192.0.2.200", "54.239.28.85", "52.94.76.10",
]
_COMMON_PORTS = [22, 53, 80, 443, 3306, 5432, 6379, 8080, 8443, 9200, 9300]
_PORT_TO_PROTOCOL = {
    22: "ssh", 53: "dns", 80: "http", 443: "https",
    3306: "mysql", 5432: "postgresql", 6379: "redis",
    8080: "http", 8443: "https", 9200: "http", 9300: "http",
}


def _build_resource(ident: ScenarioIdentity) -> dict:
    return build_signal_resource(
        identity=ident,
        role="aws-vpc-flow",
        signal_type="logs",
        dataset="aws.vpcflow",
        service_version="1.0.0",
        cloud={
            "provider": "aws",
            "platform": "aws_ec2",
            "region": "us-east-1",
            "availability_zone": "us-east-1a",
        },
        extra_attrs={"cloud.account.id": ident.aws_account_id},
    )


def _generate_flow_record(
    rng: random.Random,
    *,
    eni_ids: list[str],
    reject_rate: float = 0.2,
    bytes_multiplier: float = 1.0,
) -> dict:
    action = "REJECT" if rng.random() < reject_rate else "ACCEPT"
    src_ip = rng.choice(_INTERNAL_IPS + _EXTERNAL_IPS)
    dst_ip = rng.choice(_INTERNAL_IPS + _EXTERNAL_IPS)
    src_port = rng.choice(_COMMON_PORTS) if rng.random() < 0.6 else rng.randint(1024, 65535)
    dst_port = rng.choice(_COMMON_PORTS) if rng.random() < 0.7 else rng.randint(1024, 65535)
    bytes_transferred = int(rng.randint(64, 1_500_000) * max(bytes_multiplier, 0.1))
    transport = rng.choice(["tcp"] * 6 + ["udp"] * 3 + ["icmp"])
    packets = max(1, int((bytes_transferred // rng.randint(500, 1500)) * max(bytes_multiplier, 0.1)))
    protocol = _PORT_TO_PROTOCOL.get(dst_port, _PORT_TO_PROTOCOL.get(src_port, transport))

    attrs = {
        "aws.vpc.flow.action": action,
        "aws.vpc.flow.bytes": bytes_transferred,
        "aws.vpc.flow.packets": packets,
        "source.address": src_ip,
        "source.port": src_port,
        "destination.address": dst_ip,
        "destination.port": dst_port,
        "network.transport": transport,
        "network.interface.name": rng.choice(eni_ids),
        "network.protocol.name": protocol,
    }
    body = f"{action} {src_ip}:{src_port} -> {dst_ip}:{dst_port} {bytes_transferred} bytes"
    return {
        "timeUnixNano": _now_ns(),
        "severityText": "INFO",
        "severityNumber": 9,
        "body": {"stringValue": body},
        "attributes": _format_attributes(attrs),
    }


from generators.aws_vpcflow_otel import SPEC as _ALERT_SPEC
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
    ident = ScenarioIdentity.from_scenario(scenario_data, ns)
    scope_name = ident.label("aws-vpc-flow-generator")
    resource = _build_resource(ident)
    eni_ids = [ident.aws_id("eni", "flow", i) for i in range(1, 9)]
    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    cfg = scenario_data.get("generator_config", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    logger.info("AWS VPC flow generator started (interval=%ds, scope=%s)", FLOW_INTERVAL, scope_name)
    batch_count = 0
    def _emit_tick() -> None:
        nonlocal batch_count
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="aws_vpcflow_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        reject_rate = float((active_chaos or {}).get("reject_rate", 0.2))
        bytes_multiplier = float((active_chaos or {}).get("bytes_multiplier", 1.0))

        batch_size = rng.randint(BATCH_SIZE_MIN, BATCH_SIZE_MAX)
        records = [
            _generate_flow_record(
                rng,
                eni_ids=eni_ids,
                reject_rate=reject_rate if active_chaos else 0.2,
                bytes_multiplier=bytes_multiplier if active_chaos else 1.0,
            )
            for _ in range(batch_size)
        ]
        payload = {
            "resourceLogs": [{
                "resource": resource,
                "scopeLogs": [{
                    "scope": {"name": scope_name, "version": SCOPE_VERSION},
                    "logRecords": records,
                }],
            }]
        }
        client._send(f"{client.endpoint}/v1/logs", payload, "aws-vpc-flow")
        batch_count += 1
        if batch_count % 12 == 0:
            logger.info("AWS VPC flow batch %d: sent %d records", batch_count, batch_size)
    run_tick_loop(stop_event, _emit_tick, interval=lambda: FLOW_INTERVAL)

    logger.info("AWS VPC flow generator stopped after %d batches", batch_count)


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = OTLPClient()
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    duration = int(os.environ.get("RUN_DURATION", "60"))
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
