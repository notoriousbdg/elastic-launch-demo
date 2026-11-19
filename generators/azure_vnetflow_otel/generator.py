#!/usr/bin/env python3
"""Azure VNet Flow Log Generator — sends Azure VNet flow logs via OTLP.

Models Network Watcher VNet flow logs (flow log version 4), the successor to
NSG flow logs: one log record per flow tuple, with the deciding rule, flow
state, encryption status and per-direction packet/byte counters.

No Elastic OTel content package exists for this data, so the records feed the
executive dashboard's VPC flow panels (dataset ``azure.vnetflow.otel``) rather
than a package dashboard.

Usage (standalone):
    python3 -m generators.azure_vnetflow_otel.generator
"""

from __future__ import annotations

import logging
import os
import random
import signal
import threading

from app.telemetry import OTLPClient, _format_attributes, _now_ns
from app.config import NAMESPACE
from generator_engine.db_common import build_signal_resource, otel_dataset, resolve_active_chaos
from generator_engine.identity import ScenarioIdentity

logger = logging.getLogger("azure-vnet-flow-generator")

FLOW_INTERVAL = int(os.getenv("VNET_FLOW_INTERVAL", "5"))
BATCH_SIZE_MIN = 10
BATCH_SIZE_MAX = 20

SCOPE_VERSION = "1.0.0"
FLOW_LOG_VERSION = 4

_INTERNAL_IPS = [
    "10.1.0.4", "10.1.0.5", "10.1.1.10", "10.1.1.24",
    "10.1.2.7", "10.1.2.31", "10.1.3.12", "10.1.3.40",
    "10.1.4.9", "10.1.4.66",
]
_EXTERNAL_IPS = [
    "203.0.113.10", "203.0.113.25", "198.51.100.44", "198.51.100.77",
    "192.0.2.100", "192.0.2.200",
]
_COMMON_PORTS = [22, 53, 80, 443, 1433, 3389, 5432, 6379, 8080, 8443, 10255]
_PORT_TO_PROTOCOL = {
    22: "ssh", 53: "dns", 80: "http", 443: "https",
    1433: "mssql", 3389: "rdp", 5432: "postgresql", 6379: "redis",
    8080: "http", 8443: "https", 10255: "https",
}
_ALLOW_RULES = [
    "DefaultRule_AllowVnetInBound",
    "DefaultRule_AllowVnetOutBound",
    "DefaultRule_AllowInternetOutBound",
    "UserRule_allow-https-inbound",
    "UserRule_allow-appgw-probes",
]
_DENY_RULES = [
    "DefaultRule_DenyAllInBound",
    "UserRule_deny-untrusted-inbound",
    "UserRule_deny-db-from-web-subnet",
]
_ENCRYPTION = ["NX"] * 6 + ["X"] * 3 + ["NX_HW_NOT_SUPPORTED"]
_REGION = "eastus"


def _build_resource(ident: ScenarioIdentity) -> dict:
    return build_signal_resource(
        identity=ident,
        role="azure-vnet-flow",
        signal_type="logs",
        dataset=otel_dataset("azure.vnetflow"),
        service_version="1.0.0",
        cloud={
            "provider": "azure",
            "platform": "azure_vm",
            "region": _REGION,
            "availability_zone": f"{_REGION}-1",
        },
        extra_attrs={"cloud.account.id": ident.azure_subscription_id},
    )


def _network_ids(ident: ScenarioIdentity) -> dict:
    """Azure resource ids for the VNet, NSG and NICs the flows are logged against."""
    sub = ident.azure_subscription_id
    rg = ident.label("rg")
    base = f"/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Network"
    return {
        "vnets": [f"{base}/virtualNetworks/{ident.label('vnet', i)}" for i in range(1, 4)],
        "nsg": f"{base}/networkSecurityGroups/{ident.label('nsg', 1)}",
        "nics": [ident.label("nic", i) for i in range(1, 9)],
    }


def _generate_flow_record(
    rng: random.Random,
    *,
    net: dict,
    deny_rate: float = 0.2,
    bytes_multiplier: float = 1.0,
) -> dict:
    denied = rng.random() < deny_rate
    action = "Deny" if denied else "Allow"
    direction = rng.choice(["I", "I", "O"])
    src_ip = rng.choice(_INTERNAL_IPS + _EXTERNAL_IPS)
    dst_ip = rng.choice(_INTERNAL_IPS + _EXTERNAL_IPS)
    src_port = rng.choice(_COMMON_PORTS) if rng.random() < 0.4 else rng.randint(1024, 65535)
    dst_port = rng.choice(_COMMON_PORTS) if rng.random() < 0.7 else rng.randint(1024, 65535)
    transport = rng.choice(["tcp"] * 7 + ["udp"] * 3)
    protocol = _PORT_TO_PROTOCOL.get(dst_port, _PORT_TO_PROTOCOL.get(src_port, transport))
    mult = max(bytes_multiplier, 0.1)

    if denied:
        # Denied flows are logged with state D and carry no counters.
        state = "D"
        rule = rng.choice(_DENY_RULES)
        bytes_sent = bytes_received = packets_sent = packets_received = 0
    else:
        state = rng.choice(["B", "C", "C", "E"])
        rule = rng.choice(_ALLOW_RULES)
        bytes_sent = int(rng.randint(64, 1_200_000) * mult)
        bytes_received = int(rng.randint(64, 900_000) * mult)
        packets_sent = max(1, bytes_sent // rng.randint(500, 1500))
        packets_received = max(1, bytes_received // rng.randint(500, 1500))

    attrs = {
        "azure.vnet.flow.version": FLOW_LOG_VERSION,
        "azure.vnet.flow.action": action,
        "azure.vnet.flow.direction": direction,
        "azure.vnet.flow.state": state,
        "azure.vnet.flow.encryption": rng.choice(_ENCRYPTION),
        "azure.vnet.flow.rule": rule,
        "azure.vnet.flow.acl_id": net["nsg"],
        "azure.vnet.flow.target_resource_id": rng.choice(net["vnets"]),
        "azure.vnet.flow.bytes_sent": bytes_sent,
        "azure.vnet.flow.bytes_received": bytes_received,
        "azure.vnet.flow.packets_sent": packets_sent,
        "azure.vnet.flow.packets_received": packets_received,
        "source.address": src_ip,
        "source.port": src_port,
        "destination.address": dst_ip,
        "destination.port": dst_port,
        "network.transport": transport,
        "network.direction": "ingress" if direction == "I" else "egress",
        "network.interface.name": rng.choice(net["nics"]),
        "network.protocol.name": protocol,
    }
    body = (
        f"{action} {src_ip}:{src_port} -> {dst_ip}:{dst_port} "
        f"{transport} state={state} rule={rule} {bytes_sent + bytes_received} bytes"
    )
    return {
        "timeUnixNano": _now_ns(),
        "severityText": "INFO",
        "severityNumber": 9,
        "body": {"stringValue": body},
        "attributes": _format_attributes(attrs),
    }


from generators.azure_vnetflow_otel import SPEC as _ALERT_SPEC
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
    scope_name = ident.label("azure-vnet-flow-generator")
    resource = _build_resource(ident)
    net = _network_ids(ident)
    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    cfg = scenario_data.get("generator_config", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    logger.info("Azure VNet flow generator started (interval=%ds, scope=%s)", FLOW_INTERVAL, scope_name)
    batch_count = 0

    def _emit_tick() -> None:
        nonlocal batch_count
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="azure_vnetflow_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        deny_rate = float((active_chaos or {}).get("deny_rate", 0.2))
        bytes_multiplier = float((active_chaos or {}).get("bytes_multiplier", 1.0))

        batch_size = rng.randint(BATCH_SIZE_MIN, BATCH_SIZE_MAX)
        records = [
            _generate_flow_record(
                rng,
                net=net,
                deny_rate=deny_rate if active_chaos else 0.2,
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
        client._send(f"{client.endpoint}/v1/logs", payload, "azure-vnet-flow")
        batch_count += 1
        if batch_count % 12 == 0:
            logger.info("Azure VNet flow batch %d: sent %d records", batch_count, batch_size)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: FLOW_INTERVAL)

    logger.info("Azure VNet flow generator stopped after %d batches", batch_count)


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
