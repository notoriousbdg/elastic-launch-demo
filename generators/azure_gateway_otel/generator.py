#!/usr/bin/env python3
"""Azure Application Gateway Log Generator — sends AGW access and firewall logs via OTLP.

Emits data matching the azure_gateway_otel content package
(discovery.datasets: azure.application_gateway.otel).

Two log categories share one dataset stream:
  - ApplicationGatewayAccessLog  — per-request access logs
  - ApplicationGatewayFirewallLog — WAF rule matches

Field names verified against:
  - elastic/integrations/packages/azure_gateway_otel/kibana/dashboard/*.json
  - opentelemetry-collector-contrib azureencodingextension AGW mapping
    (category_agw.go + testdata/agw/*_expected.yaml)

Dashboard-queried attributes:
  Access: azure.category, azure.request.duration, azure.agw.backend.*,
          azure.agw.listener.name, azure.agw.rule.name,
          http.response.status_code, http.response.size, client.address,
          host.name, url.path, error.type,
          resource.attributes.service.instance.id,
          resource.attributes.cloud.resource_id
  Firewall: azure.category, security_rule.*, azure.firewall.policy.*,
            client.address, host.name, url.original, body.text

ES|QL column paths (OTLP log attributes are indexed under ``attributes.*``):
  Access:   attributes.azure.request.duration (s), attributes.azure.agw.backend.latency (s),
            attributes.azure.agw.backend.pool.name, attributes.http.response.size (bytes),
            attributes.client.address, attributes.host.name
  Firewall: attributes.security_rule.uuid, attributes.security_rule.ruleset.name,
            attributes.azure.firewall.policy.id, attributes.client.address,
            attributes.host.name

Usage (standalone):
    python3 -m generators.azure_gateway_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/azure_gateway_otel@0.2.0"

import logging
import os
import random
import signal
import threading
import uuid
from collections import defaultdict

from app.telemetry import OTLPClient, _format_attributes
from app.config import NAMESPACE
from generator_engine.db_common import (
    build_audit_log_record,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

AZURE_GATEWAY_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/"
    "extension/encoding/azureencodingextension"
)
SCOPE_VERSION = "0.115.0"

# Dashboard-queried log attributes (azure_gateway_otel Lens / Options List).
LOG_ATTRIBUTES = (
    "azure.agw.backend.latency",
    "azure.agw.backend.pool.name",
    "azure.agw.backend.setting.name",
    "azure.agw.backend.status_code",
    "azure.agw.latency",
    "azure.agw.listener.name",
    "azure.agw.rule.name",
    "azure.category",
    "azure.firewall.policy.id",
    "azure.firewall.policy.object.name",
    "azure.firewall.policy.scope.type",
    "azure.operation.name",
    "azure.request.duration",
    "client.address",
    "error.type",
    "host.name",
    "http.response.size",
    "http.response.status_code",
    "security_rule.action",
    "security_rule.category",
    "security_rule.ruleset.name",
    "security_rule.uuid",
    "security_rule.version",
    "url.original",
    "url.path",
)

# Dashboard Options List / ES|QL resource attributes.
RESOURCE_ATTRIBUTES = (
    "cloud.resource_id",
    "service.instance.id",
)

# Fraction of access logs that are gateway-terminated (no backend status).
# Backend Health "Gateway-Terminated Requests Over Time" filters
# attributes.azure.agw.backend.status_code IS NULL — keep this high enough
# that the panel always has points in a 24h window.
_GATEWAY_TERMINATED_RATE = 0.10

logger = logging.getLogger("azure-gateway-generator")
BATCH_INTERVAL_MIN = 5
BATCH_INTERVAL_MAX = 15

_HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"]
_URL_PATHS = [
    "/api/products", "/api/users", "/api/orders", "/api/payments",
    "/api/search", "/health", "/metrics", "/api/auth/token",
    "/api/v2/catalog", "/api/v2/cart",
]
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "python-requests/2.31.0",
    "curl/8.1.2",
    "okhttp/4.11.0",
    "Dalvik/2.1.0 (Android 14)",
]
_SSL_CIPHERS = [
    "ECDHE-RSA-AES256-GCM-SHA384",
    "ECDHE-RSA-AES128-GCM-SHA256",
    "TLS_AES_256_GCM_SHA384",
]
_LISTENERS = ["https-listener", "http-listener", "api-listener"]
_RULES = ["api-route", "default-route", "static-route", "health-route"]
_BACKEND_POOLS = [
    ("backendPool01", "httpSetting01"),
    ("apiPool", "httpsSetting01"),
    ("staticPool", "httpSetting02"),
]
_ERROR_TYPES = [
    "ERRORINFO_UPSTREAM_NO_LIVE",
    "ERRORINFO_REQUEST_TIME_OUT",
    "ERRORINFO_SSL_ERROR",
    "ERRORINFO_4XX",
    "ERRORINFO_5XX",
]
# (ruleSetType/category, ruleGroup/ruleset.name, ruleId/uuid, message)
_WAF_RULES = [
    ("Microsoft_DefaultRuleSet", "REQUEST-942-APPLICATION-ATTACK-SQLI",
     "942100", "SQL Injection Attack Detected"),
    ("Microsoft_DefaultRuleSet", "REQUEST-941-APPLICATION-ATTACK-XSS",
     "941100", "XSS Attack Detected via libinjection"),
    ("Microsoft_DefaultRuleSet", "REQUEST-930-APPLICATION-ATTACK-LFI",
     "930100", "Path Traversal Attack"),
    ("Microsoft_DefaultRuleSet", "REQUEST-932-APPLICATION-ATTACK-RCE",
     "932160", "Remote Command Execution: Unix Shell Code Found"),
    ("OWASP", "920-PROTOCOL-ENFORCEMENT",
     "920350", "Host header is a numeric IP address"),
    ("OWASP", "913-SCANNER-DETECTION",
     "913100", "Found User-Agent associated with security scanner"),
]
_POLICY_SCOPES = [
    ("Global", "Global"),
    ("Listener", "https-listener"),
    ("Location", "api-path-map"),
]


# Client populations. Real gateways see a skewed mix of returning clients (Top Clients
# panel), and WAF hits come from a handful of scanners rather than one IP per request.
_POOL_RNG = random.Random(20250517)
_ACCESS_CLIENTS = [
    f"203.{_POOL_RNG.randint(0, 255)}.{_POOL_RNG.randint(0, 255)}.{_POOL_RNG.randint(1, 254)}"
    for _ in range(60)
]
_ACCESS_CLIENT_WEIGHTS = [1.0 / (i + 1) ** 0.8 for i in range(len(_ACCESS_CLIENTS))]
_WAF_CLIENTS = [
    f"185.{_POOL_RNG.randint(0, 255)}.{_POOL_RNG.randint(0, 255)}.{_POOL_RNG.randint(1, 254)}"
    for _ in range(12)
]
_WAF_CLIENT_WEIGHTS = [1.0 / (i + 1) for i in range(len(_WAF_CLIENTS))]
# Multi-site listeners: extra host headers served next to the primary gateway host.
_SITE_PREFIXES = ("www", "portal")


def _default_cloud() -> dict:
    return {"provider": "azure", "platform": "azure_vm",
            "region": "eastus", "availability_zone": "1"}


def _synthetic_uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128)))


def _cloud_resource_id(subscription_id: str, resource_group: str, gateway_name: str) -> str:
    return (
        f"/SUBSCRIPTIONS/{subscription_id.upper()}"
        f"/RESOURCEGROUPS/{resource_group.upper()}"
        f"/PROVIDERS/MICROSOFT.NETWORK/APPLICATIONGATEWAYS/{gateway_name}"
    )


def _parse_host_port(backend: str) -> tuple[str, int]:
    if ":" in backend:
        host, port_s = backend.rsplit(":", 1)
        try:
            return host, int(port_s)
        except ValueError:
            return backend, 80
    return backend, 80


def _build_access_record(
    gateway_host: str,
    backends: list[str],
    instance_id: str,
    rng: random.Random,
    error_rate: float,
    jitter_ns: int,
) -> dict:
    method = rng.choice(_HTTP_METHODS)
    path = rng.choice(_URL_PATHS)
    query = f"page={rng.randint(1, 20)}" if rng.random() < 0.3 else ""
    client_ip = rng.choices(_ACCESS_CLIENTS, weights=_ACCESS_CLIENT_WEIGHTS)[0]
    listener = rng.choice(_LISTENERS)
    rule = rng.choice(_RULES)

    # Gateway-terminated is independent of the general error_rate so the
    # Backend Health panel always receives docs with backend.status_code IS NULL.
    gateway_terminated = rng.random() < _GATEWAY_TERMINATED_RATE
    # Floor at 5% so Overview "Errors by Status Code" / status-class panels
    # stay populated even when no chaos channel is active.
    is_error = gateway_terminated or rng.random() < max(error_rate, 0.05)

    if is_error:
        http_status = rng.choice([400, 403, 404, 500, 502, 503])
        # Durations are seconds (azureencodingextension maps timeTaken → azure.request.duration).
        duration_s = round(rng.uniform(0.08, 2.5), 3)
        outcome = "failure"
        severity = "ERROR" if http_status >= 500 else "WARN"
    else:
        http_status = rng.choice([200, 200, 200, 201, 204, 301])
        duration_s = round(rng.uniform(0.003, 0.180), 3)
        outcome = "success"
        severity = "INFO"

    received = rng.randint(64, 8192)
    sent = rng.randint(128, 65536)

    extra: dict = {
        "azure.category": "ApplicationGatewayAccessLog",
        "azure.operation.name": "ApplicationGatewayAccess",
        "azure.agw.listener.name": listener,
        "azure.agw.rule.name": rule,
        "client.address": client_ip,
        "client.port": rng.randint(1024, 65535),
        "http.request.method": method,
        "url.path": path,
        "url.original": f"{path}?{query}" if query else path,
        "url.query": query,
        "user_agent.original": rng.choice(_USER_AGENTS),
        "http.response.status_code": http_status,
        "network.protocol.name": "http",
        "network.protocol.version": "1.1",
        "http.request.size": received,
        "http.response.size": sent,
        "azure.request.duration": duration_s,
        "tls.enabled": "on",
        "tls.cipher": rng.choice(_SSL_CIPHERS),
        "tls.protocol.original": "TLSv1.2",
        "host.name": gateway_host,
        "http.request.header.host": gateway_host,
        "azure.service.request.id": _synthetic_uuid(rng).replace("-", ""),
        "security_rule.ruleset.mode": "Prevention",
    }

    if gateway_terminated:
        # Match azureencodingextension gateway-rejected samples: error.type set,
        # backend pool/setting/status/latency omitted.
        extra["error.type"] = rng.choice(_ERROR_TYPES)
        extra["azure.agw.latency"] = duration_s
    else:
        pool_name, setting_name = rng.choice(_BACKEND_POOLS)
        backend = rng.choice(backends)
        server_addr, server_port = _parse_host_port(backend)
        backend_status = http_status if not is_error else rng.choice(
            [http_status, 500, 502, 503]
        )
        backend_latency = round(max(0.001, duration_s * rng.uniform(0.55, 0.95)), 3)
        extra.update({
            "azure.agw.backend.pool.name": pool_name,
            "azure.agw.backend.setting.name": setting_name,
            "server.address": server_addr,
            "server.port": server_port,
            "azure.agw.backend.status_code": backend_status,
            "azure.agw.backend.latency": backend_latency,
            "azure.agw.latency": round(max(0.0, duration_s - backend_latency), 3),
            "network.local.port": rng.randint(1024, 65535),
            "azure.firewall.evaluation.duration": round(rng.uniform(0.0, 0.005), 3),
            "error.type": "ERRORINFO_NO_ERROR" if not is_error else rng.choice(_ERROR_TYPES),
        })

    return build_audit_log_record(
        actor="",
        action=f"HTTP {method} {path}",
        outcome=outcome,
        source_address=client_ip,
        event_name="azure.resource.log",
        severity=severity,
        extra_attrs=extra,
        jitter_ns=jitter_ns,
    )


def _build_firewall_record(
    gateway_host: str,
    subscription_id: str,
    resource_group: str,
    gateway_name: str,
    instance_id: str,
    rng: random.Random,
    jitter_ns: int,
) -> dict:
    rule_set_type, rule_group, rule_id, message = rng.choice(_WAF_RULES)
    # Dashboard panels count Blocked / Detected exactly (not Block / Detection).
    action = rng.choice(["Blocked", "Blocked", "Detected", "Matched"])
    path = rng.choice(_URL_PATHS)
    client_ip = rng.choices(_WAF_CLIENTS, weights=_WAF_CLIENT_WEIGHTS)[0]
    scope_type, scope_name = rng.choice(_POLICY_SCOPES)
    policy_id = (
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Network/ApplicationGatewayWebApplicationFirewallPolicies"
        f"/{gateway_name}-waf"
    )

    extra: dict = {
        "azure.category": "ApplicationGatewayFirewallLog",
        "azure.operation.name": "ApplicationGatewayFirewall",
        "client.address": client_ip,
        "client.port": rng.randint(1024, 65535),
        "url.original": path,
        "security_rule.category": rule_set_type,
        "security_rule.version": "2.1" if rule_set_type.startswith("Microsoft") else "3.0",
        "security_rule.uuid": rule_id,
        "security_rule.ruleset.name": rule_group,
        "security_rule.action": action,
        "azure.firewall.site": "Global",
        "host.name": gateway_host,
        "azure.service.request.id": _synthetic_uuid(rng).replace("-", ""),
        "azure.firewall.policy.id": policy_id,
        "azure.firewall.policy.scope.type": scope_type,
        "azure.firewall.policy.object.name": scope_name,
    }

    record = build_audit_log_record(
        actor="",
        action=f"WAF {action} {rule_group}",
        outcome="failure",
        source_address=client_ip,
        event_name="azure.resource.log",
        severity="WARN" if action in ("Detected", "Matched") else "ERROR",
        extra_attrs=extra,
        jitter_ns=jitter_ns,
    )
    # WAF message lands in body.text (VALUES(body.text) on Top Rule IDs panel).
    record["body"] = {"stringValue": message}
    return record


def _make_resource(
    *,
    ns: str,
    service_name: str,
    instance_id: str,
    host_name: str,
    cloud: dict,
    subscription_id: str,
    resource_group: str,
    cloud_resource_id: str,
) -> dict:
    return build_signal_resource(
        ns=ns, signal_type="logs", dataset=otel_dataset("azure.application_gateway"),
        service_name=service_name, service_version="1.0.0",
        service_instance_id=instance_id,
        host_name=host_name, cloud=cloud,
        extra_attrs={
            "cloud.resource_id": cloud_resource_id,
            "cloudevents.event_source": "azure.resource.log",
            "azure.subscription_id": subscription_id,
            "azure.resource_group": resource_group,
        },
    )



from generators.azure_gateway_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("azure-application-gateway")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("azure-agw")
    cloud = cfg.get("cloud") or _default_cloud()
    subscription_id = ScenarioIdentity.from_scenario(scenario_data, ns).azure_subscription_id
    gateway_name = cfg.get("gateway_name") or ScenarioIdentity(ns).label("agw")
    resource_group = ScenarioIdentity(ns).label("rg")
    gateway_host = cfg.get("gateway_host") or ScenarioIdentity(ns).domain("api")
    records_per_cycle: int = cfg.get("records_per_cycle", 15)
    # Primary site takes most traffic; Top Hosts / WAF Targets show a multi-site gateway.
    site_hosts = [gateway_host] + [ScenarioIdentity(ns).domain(p) for p in _SITE_PREFIXES]
    site_weights = [0.7] + [0.3 / len(_SITE_PREFIXES)] * len(_SITE_PREFIXES)
    cloud_resource_id = _cloud_resource_id(subscription_id, resource_group, gateway_name)

    default_backends = [
        f"10.{rng.randint(0, 9)}.0.{i}:8080" for i in range(1, 4)
    ]
    backends: list[str] = cfg.get("backends") or default_backends
    # Per-instance resources — dashboards COUNT_DISTINCT(service.instance.id).
    instances = [
        ScenarioIdentity.from_scenario(scenario_data, ns).instance("agw", i)
        for i in range(1, 4)
    ]


    scope = {
        "name": AZURE_GATEWAY_SCOPE,
        "version": SCOPE_VERSION,
        "attributes": _format_attributes({
            "encoding.format": "azure.application_gateway",
        }),
    }
    logger.info("Azure Application Gateway generator started (namespace=%s, gw=%s)", ns, gateway_name)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="azure_gateway_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        error_rate = float((active_chaos or {}).get("error_rate", 0.0))
        n = int((active_chaos or {}).get("records_per_cycle", records_per_cycle))
        load = business_load()
        n = max(1, int(n * load))

        by_instance: dict[str, list[dict]] = defaultdict(list)
        for _ in range(n):
            jitter = rng.randint(0, int(BATCH_INTERVAL_MAX * 1e9))
            instance_id = rng.choice(instances)
            host = rng.choices(site_hosts, weights=site_weights)[0]
            if rng.random() < 0.20 or (active_chaos and error_rate > 0.1):
                by_instance[instance_id].append(_build_firewall_record(
                    host, subscription_id, resource_group, gateway_name,
                    instance_id, rng, jitter,
                ))
            else:
                by_instance[instance_id].append(_build_access_record(
                    host, backends, instance_id, rng, error_rate, jitter,
                ))

        for instance_id, records in by_instance.items():
            resource = _make_resource(
                ns=ns, service_name=service_name, instance_id=instance_id,
                host_name=host_name, cloud=cloud,
                subscription_id=subscription_id, resource_group=resource_group,
                cloud_resource_id=cloud_resource_id,
            )
            client.send_logs_scoped(resource, scope, records)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("Azure Application Gateway generator stopped.")


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
