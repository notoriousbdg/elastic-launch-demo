#!/usr/bin/env python3
"""GCP VPC Flow Log Generator — sends GCP VPC flow logs via OTLP.

Generates logs that populate:
  - [GCP VPC OTEL] VPC Flow Logs Overview dashboard
  - [GCP VPC OTEL] Traffic Analysis dashboard
  - [GCP VPC OTEL] Interface Analysis dashboard

Usage (standalone):
    python3 -m generators.gcp_vpcflow_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/gcp_vpcflow_otel@0.3.0"

import logging
import os
import random
import signal
import threading

from app.telemetry import OTLPClient, _format_attributes, SCHEMA_URL, _now_ns
from app.config import NAMESPACE
from generator_engine.db_common import business_load, resolve_active_chaos

logger = logging.getLogger("gcp-vpc-flow-generator")

FLOW_INTERVAL = int(os.getenv("VPC_FLOW_INTERVAL", "5"))
BATCH_SIZE_MIN = 10
BATCH_SIZE_MAX = 20

SCOPE_NAME = f"{NAMESPACE}-gcp-vpc-flow-generator"
SCOPE_VERSION = "1.0.0"

_INTERNAL_IPS = [
    "10.128.0.15", "10.128.0.20", "10.128.1.5", "10.128.1.30",
    "10.128.2.10", "10.128.2.50", "10.128.3.8", "10.128.3.42",
]
_EXTERNAL_IPS = [
    "35.201.97.10", "35.201.97.55", "34.120.50.22", "34.120.50.88",
    "104.199.30.5", "104.199.30.77", "142.250.80.10", "142.250.80.25",
]
_COUNTRY_CODES = ["USA", "DEU", "GBR", "JPN", "AUS", "CAN", "FRA", "BRA", "IND", "SGP"]
_COMMON_PORTS = [22, 53, 80, 443, 3306, 5432, 6379, 8080, 8443, 9200, 9300]
_PORT_TO_PROTOCOL = {
    22: "ssh", 53: "dns", 80: "http", 443: "https",
    3306: "mysql", 5432: "postgresql", 6379: "redis",
    8080: "http", 8443: "https", 9200: "http", 9300: "http",
}


def _build_resource(ns: str) -> dict:
    attrs = {
        "cloud.provider": "gcp",
        "cloud.platform": "gcp_compute_engine",
        "cloud.region": "us-central1",
        "cloud.account.id": ScenarioIdentity(ns).gcp_project_id,
        "data_stream.type": "logs",
        "data_stream.dataset": "gcp.vpcflow",
        "data_stream.namespace": "default",
        "service.name": ScenarioIdentity(ns).service("gcp-vpc-flow"),
        "telemetry.sdk.name": "opentelemetry",
        "telemetry.sdk.version": "1.24.0",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _generate_flow_record(
    rng: random.Random,
    vpc_names: list[str],
    *,
    bytes_multiplier: float = 1.0,
) -> dict:
    src_ip = rng.choice(_INTERNAL_IPS + _EXTERNAL_IPS)
    dst_ip = rng.choice(_INTERNAL_IPS + _EXTERNAL_IPS)
    bytes_sent = int(rng.randint(64, 2_000_000) * max(bytes_multiplier, 0.1))
    packets_sent = max(1, int((bytes_sent // rng.randint(500, 1500)) * max(bytes_multiplier, 0.1)))
    reporter = rng.choice(["SRC", "DEST"])
    src_vpc = rng.choice(vpc_names)
    dst_vpc = rng.choice(vpc_names)
    transport = rng.choice(["tcp"] * 6 + ["udp"] * 3 + ["icmp"])
    src_port = rng.choice(_COMMON_PORTS) if rng.random() < 0.6 else rng.randint(1024, 65535)
    dst_port = rng.choice(_COMMON_PORTS) if rng.random() < 0.7 else rng.randint(1024, 65535)
    protocol = _PORT_TO_PROTOCOL.get(dst_port, _PORT_TO_PROTOCOL.get(src_port, transport))

    attrs = {
        "gcp.vpc.flow.bytes_sent": bytes_sent,
        "gcp.vpc.flow.packets_sent": packets_sent,
        "gcp.vpc.flow.reporter": reporter,
        "gcp.vpc.flow.source.vpc.name": src_vpc,
        "gcp.vpc.flow.destination.vpc.name": dst_vpc,
        "gcp.vpc.flow.source.geo.country.iso_code.alpha3": rng.choice(_COUNTRY_CODES),
        "source.address": src_ip,
        "source.port": src_port,
        "destination.address": dst_ip,
        "destination.port": dst_port,
        "network.transport": transport,
        "network.interface.name": rng.choice(["nic0", "nic1"]),
        "network.protocol.name": protocol,
    }
    body = f"{reporter} {src_ip} -> {dst_ip} via {src_vpc} {bytes_sent}B {packets_sent}pkts {transport}"
    return {
        "timeUnixNano": _now_ns(),
        "severityText": "INFO",
        "severityNumber": 9,
        "body": {"stringValue": body},
        "attributes": _format_attributes(attrs),
    }


from generators.gcp_vpcflow_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop




def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    rng = random.Random()
    if scenario_data:
        ns = scenario_data["namespace"]
        scope_name = ScenarioIdentity(ns).label("gcp-vpc-flow-generator")
        vpc_names = [ScenarioIdentity(ns).label("vpc-prod"), ScenarioIdentity(ns).label("vpc-staging"), ScenarioIdentity(ns).label("vpc-data")]
    else:
        ns = NAMESPACE
        scope_name = SCOPE_NAME
        vpc_names = [ScenarioIdentity(ns).label("vpc-prod"), ScenarioIdentity(ns).label("vpc-staging"), ScenarioIdentity(ns).label("vpc-data")]

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    cfg = scenario_data.get("generator_config", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "any")
    records_per_cycle = int(cfg.get("records_per_cycle", BATCH_SIZE_MAX))

    resource = _build_resource(ns)

    logger.info("GCP VPC flow generator started (interval=%ds, scope=%s)", FLOW_INTERVAL, scope_name)
    batch_count = 0
    def _emit_tick() -> None:
        nonlocal batch_count
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="gcp_vpcflow_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        bytes_multiplier = float((active_chaos or {}).get("bytes_multiplier", 1.0))

        if active_chaos:
            n = int((active_chaos or {}).get("records_per_cycle", records_per_cycle))
            load = business_load()
            batch_size = max(1, int(n * load))
        else:
            batch_size = rng.randint(BATCH_SIZE_MIN, BATCH_SIZE_MAX)

        records = [
            _generate_flow_record(
                rng,
                vpc_names,
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
        client._send(f"{client.endpoint}/v1/logs", payload, "gcp-vpc-flow")
        batch_count += 1
        if batch_count % 12 == 0:
            logger.info("GCP VPC flow batch %d: sent %d records", batch_count, batch_size)
    run_tick_loop(stop_event, _emit_tick, interval=lambda: FLOW_INTERVAL)

    logger.info("GCP VPC flow generator stopped after %d batches", batch_count)


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
