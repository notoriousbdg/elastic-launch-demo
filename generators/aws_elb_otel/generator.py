#!/usr/bin/env python3
"""ELB Access Generator — sends ALB access log records via OTLP.

Emits data matching the aws_elb_otel content package
(discovery.datasets: aws.elbaccess.otel), which populates the bundled
ALB Access Logs dashboard.

Signal types:
  - Logs: HTTP access log records with ECS-aligned fields

Field names verified against:
  - elastic/integrations/packages/aws_elb_otel/kibana/dashboard/*.json

Key log fields emitted:
  aws.elb.status.code (int), client.address, cloud.resource_id,
  http.request.size, http.response.size, network.protocol.name, url.full

Dashboard panels compare status codes numerically
(``aws.elb.status.code >= 500``), so the status must be an int — not a
string.  Records use camelCase OTLP JSON with ``_format_attributes``.

Note: Elasticsearch stores OTLP attributes via passthrough mapping so
``attributes.aws.elb.status.code`` is also accessible as
``aws.elb.status.code`` in ES|QL queries, which is how the dashboard
panels query these fields.

Usage (standalone):
    python3 -m generators.aws_elb_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/aws_elb_otel@0.3.0"

import logging
import os
import random
import signal
import threading

from app.telemetry import OTLPClient, _format_attributes, _now_ns
from app.config import NAMESPACE
from generator_engine.db_common import (
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

# ── Grounding constants — do not change names ─────────────────────────────────

ELB_ACCESS_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver"
)
SCOPE_VERSION = "0.115.0"

PROTOCOLS = ("https", "http", "h2", "grpc")

# Status codes and their approximate weights in normal operation.
# 5xx kept near-zero so healthy baseline doesn't breach downstream 5xx alerts.
NORMAL_STATUS_WEIGHTS = {
    200: 0.715,
    201: 0.08,
    204: 0.05,
    301: 0.04,
    302: 0.04,
    400: 0.02,
    401: 0.02,
    403: 0.02,
    404: 0.01,
    500: 0.002,
    502: 0.001,
    503: 0.001,
}

# During fault, 5xx rates increase
FAULT_STATUS_WEIGHTS = {
    200: 0.40,
    201: 0.05,
    204: 0.03,
    301: 0.02,
    302: 0.02,
    400: 0.05,
    401: 0.03,
    403: 0.02,
    404: 0.03,
    500: 0.15,
    502: 0.10,
    503: 0.10,
}

logger = logging.getLogger("elb-access-generator")

BATCH_INTERVAL_MIN = 5
BATCH_INTERVAL_MAX = 15


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_elb",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


def _pick_status(rng: random.Random, active_chaos: dict | None, error_fraction: float) -> int:
    weights = FAULT_STATUS_WEIGHTS if active_chaos else NORMAL_STATUS_WEIGHTS
    # Apply the impact key: error_fraction shifts weight toward 5xx
    if active_chaos and error_fraction > 0:
        adjusted = {}
        for code, w in weights.items():
            if code >= 500:
                adjusted[code] = w * (1 + error_fraction * 10)
            else:
                adjusted[code] = w * max(0.01, 1 - error_fraction)
        weights = adjusted
    codes = list(weights.keys())
    w_list = [weights[c] for c in codes]
    total = sum(w_list)
    w_norm = [w / total for w in w_list]
    return rng.choices(codes, weights=w_norm, k=1)[0]


def _generate_logs(
    lb_resource_id: str,
    endpoints: list[dict],
    client_ips: list[str],
    rng: random.Random,
    records_per_cycle: int,
    active_chaos: dict | None = None,
) -> list[dict]:
    now_ns = int(_now_ns())
    logs: list[dict] = []

    error_fraction = float((active_chaos or {}).get("error_fraction", 0.0))

    for _ in range(records_per_cycle):
        ep = rng.choice(endpoints)
        status = _pick_status(rng, active_chaos, error_fraction)
        protocol = rng.choice(PROTOCOLS)
        req_size = rng.randint(100, 10_000)
        resp_size = rng.randint(200, 500_000) if status < 500 else rng.randint(50, 500)
        client_ip = rng.choice(client_ips)
        jitter = rng.randint(0, int(BATCH_INTERVAL_MAX * 1e9))
        method = ep.get("method", "GET")
        path = ep["path"]
        url = ep["url"]

        attrs = {
            "aws.elb.status.code": status,
            "client.address": client_ip,
            "cloud.resource_id": lb_resource_id,
            "http.request.size": req_size,
            "http.response.size": resp_size,
            "network.protocol.name": protocol,
            "url.full": url,
            "http.request.method": method,
            "url.path": path,
        }

        logs.append({
            "timeUnixNano": str(now_ns - jitter),
            "observedTimeUnixNano": str(now_ns),
            "severityNumber": 9,
            "severityText": "INFO",
            "eventName": "aws.elb.access_log",
            "body": {"stringValue": f"{method} {path} {status}"},
            "attributes": _format_attributes(attrs),
        })

    return logs



from generators.aws_elb_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop

def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run ELB access log generator loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {})
    else:
        ns = NAMESPACE
        cfg = {}

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("alb-frontend")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("alb")
    cloud = cfg.get("cloud") or _default_cloud()
    records_per_cycle: int = cfg.get("records_per_cycle", 20)

    _ident = ScenarioIdentity.from_scenario(scenario_data, ns)
    lb_resource_id = cfg.get("lb_resource_id") or (
        f"app/{_ident.label('alb')}/{_ident.aws_id('instance', 'alb', 1)[2:]}"
    )

    # Default endpoints
    ns_path = ns.replace("-", "/")
    default_endpoints = [
        {"url": f"https://{host_name}/api/v1/status", "path": "/api/v1/status", "method": "GET"},
        {"url": f"https://{host_name}/api/v1/health", "path": "/api/v1/health", "method": "GET"},
        {"url": f"https://{host_name}/api/v1/{ns_path}", "path": f"/api/v1/{ns_path}", "method": "GET"},
        {"url": f"https://{host_name}/api/v1/{ns_path}", "path": f"/api/v1/{ns_path}", "method": "POST"},
        {"url": f"https://{host_name}/api/v1/{ns_path}/batch", "path": f"/api/v1/{ns_path}/batch", "method": "POST"},
    ]
    endpoints: list[dict] = cfg.get("endpoints") or default_endpoints

    # Synthetic client IPs
    default_ips = [f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}" for _ in range(50)]
    client_ips: list[str] = cfg.get("client_ips") or default_ips


    resource = build_signal_resource(
        ns=ns,
        signal_type="logs",
        dataset=otel_dataset("aws.elbaccess"),
        service_name=service_name,
        service_version="1.0.0",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name,
        cloud=cloud,
        extra_attrs={
            "aws.elb.load_balancer.arn": (
                f"arn:aws:elasticloadbalancing:{cloud.get('region', 'us-east-1')}"
                f":{ScenarioIdentity.from_scenario(scenario_data, ns).aws_account_id}:loadbalancer/{lb_resource_id}"
            ),
            "cloud.resource_id": lb_resource_id,
        },
    )

    scope = {"name": ELB_ACCESS_SCOPE, "version": SCOPE_VERSION}

    logger.info("ELB access generator started (namespace=%s, lb=%s)", ns, lb_resource_id)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="aws_elb_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        n = int((active_chaos or {}).get("records_per_cycle", records_per_cycle))
        n = max(1, int(n * business_load()))
        logs = _generate_logs(lb_resource_id, endpoints, client_ips, rng, n, active_chaos)
        client.send_logs_scoped(resource, scope, logs)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("ELB access generator stopped.")


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
