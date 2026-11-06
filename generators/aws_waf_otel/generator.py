#!/usr/bin/env python3
"""AWS WAF Log Generator — sends WAF web request logs via OTLP.

Emits data matching the aws_waf_otel content package
(discovery.datasets: aws.waf.otel), which populates the bundled
WAF dashboards.

Field names verified from aws_waf_otel Lens dashboard JSON:
  aws.waf.action, aws.waf.source.name, aws.waf.terminating_rule.id,
  client.address, server.address, geo.country.iso_code, url.path

Usage (standalone):
    python3 -m generators.aws_waf_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/aws_waf_otel@0.3.0"

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

WAF_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("waf-generator")
BATCH_INTERVAL_MIN = 5
BATCH_INTERVAL_MAX = 15

_COUNTRIES = ["US", "DE", "GB", "FR", "JP", "CA", "AU", "BR", "IN", "CN",
              "RU", "KR", "NL", "SG", "SE"]
_URLS = ["/api/v1/products", "/api/v1/checkout", "/api/v1/users", "/api/v1/search",
         "/api/v1/cart", "/login", "/admin", "/.env", "/wp-admin", "/etc/passwd"]
_RULE_IDS = ["AWSManagedRulesCommonRuleSet", "AWSManagedRulesSQLiRuleSet",
             "AWSManagedRulesXSSRuleSet", "AWSManagedRulesKnownBadInputsRuleSet",
             "rate-limit-rule", "geo-block-rule"]


def _default_cloud() -> dict:
    return {"provider": "aws", "platform": "aws_waf",
            "region": "us-east-1", "availability_zone": "us-east-1a"}


def _generate_records(
    web_acl_name: str,
    account_id: str,
    region: str,
    rng: random.Random,
    count: int,
    block_rate: float = 0.0,
    *,
    domain: str = "example.internal",
) -> list[dict]:
    now_ns = int(time.time() * 1e9)
    records = []
    domains = [f"{sub}.{domain}" for sub in ("api", "www", "app")]
    for _ in range(count):
        is_blocked = rng.random() < max(block_rate, 0.02)
        action = "BLOCK" if is_blocked else "ALLOW"
        outcome = "failure" if is_blocked else "success"
        severity = "WARN" if is_blocked else "INFO"
        rule_id = rng.choice(_RULE_IDS) if is_blocked else None

        client_ip = f"{rng.randint(1,223)}.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}"
        extra: dict = {
            "aws.waf.action": action,
            "aws.waf.source.name": web_acl_name,
            "client.address": client_ip,
            "server.address": rng.choice(domains),
            "geo.country.iso_code": rng.choice(_COUNTRIES),
            "url.path": rng.choice(_URLS),
            "cloud.account.id": account_id,
            "cloud.region": region,
        }
        if rule_id:
            extra["aws.waf.terminating_rule.id"] = rule_id

        jitter = rng.randint(0, int(BATCH_INTERVAL_MAX * 1e9))
        records.append(build_audit_log_record(
            actor=client_ip,
            action="http.request",
            outcome=outcome,
            source_address=client_ip,
            event_name="aws.waf.log",
            severity=severity,
            extra_attrs=extra,
            jitter_ns=jitter,
        ))
    return records



from generators.aws_waf_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("waf")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("waf")
    cloud = cfg.get("cloud") or _default_cloud()
    region = cloud.get("region", "us-east-1")
    account_id = ScenarioIdentity.from_scenario(scenario_data, ns).aws_account_id
    web_acl_name = cfg.get("web_acl_name") or ScenarioIdentity(ns).label("web-acl")
    site_domain = ScenarioIdentity(ns).domain()
    records_per_cycle: int = cfg.get("records_per_cycle", 15)


    resource = build_signal_resource(
        ns=ns, signal_type="logs", dataset=otel_dataset("aws.waf"),
        service_name=service_name, service_version="1.0.0",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name, cloud=cloud,
        extra_attrs={
            "cloud.account.id": account_id,
            "aws.waf.source.name": web_acl_name,
        },
    )
    scope = {"name": WAF_SCOPE, "version": SCOPE_VERSION}
    logger.info("WAF log generator started (namespace=%s, acl=%s)", ns, web_acl_name)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="aws_waf_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        block_rate = float((active_chaos or {}).get("block_rate", 0.0))
        n = int((active_chaos or {}).get("records_per_cycle", records_per_cycle))
        load = business_load()
        n = max(1, int(n * load))

        records = _generate_records(
            web_acl_name, account_id, region, rng, n, block_rate, domain=site_domain,
        )
        client.send_logs_scoped(resource, scope, records)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("WAF log generator stopped.")


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
