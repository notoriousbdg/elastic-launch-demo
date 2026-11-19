"""Microsoft Foundry (azure_ai_foundry) telemetry generator.

Source: elastic/integrations packages/azure_ai_foundry@0.13.0

Writes:
- ``metrics-azure.ai_foundry-<ns>`` — co-located Azure Monitor metrics
- ``logs-azure_ai_foundry.logs-<ns>`` — Audit / RequestResponse / GatewayLogs

Package dashboards and OOTB alerts target ``*-default``. When the scenario
namespace differs, each batch is dual-written there.

Documents use ``pipeline=_none``. Emit the **post-pipeline** shape from
``sample_event.json`` / ``*-expected.json`` (backend_* bodies, source.geo,
event.outcome). Do not emit pre-ingest raw Event Hub lines.

Out of scope: Cognitive Services Billing panels query ``azure.billing``.
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/azure_ai_foundry@0.13.0"

import logging
import math
import os
import random
import threading
import time
import uuid
from typing import Any

from app.telemetry import ESBulkClient
from generator_engine.identity import ScenarioIdentity
from generator_engine.azure_common import (
    ensure_dashboard_mappings,
    ensure_dashboard_mappings_from_env,
    subscription_name as _subscription_name,
)
from generator_engine.es_doc_common import (
    build_base_doc,
    business_load,
    emit_namespaces,
    resolve_active_chaos,
    send_dual_bulk,
)

logger = logging.getLogger(__name__)

DATA_STREAMS = (
    "metrics-azure.ai_foundry",
    "logs-azure_ai_foundry.logs",
)

DOC_FIELDS = (
    "azure.ai_foundry.model_requests.total",
    "azure.ai_foundry.model_availability_rate.avg",
    "azure.ai_foundry.time_to_response.avg",
    "azure.ai_foundry.provisioned_utilization.avg",
    "azure.dimensions.model_deployment_name",
    "azure.dimensions.status_code",
    "azure.ai_foundry.category",
    "azure.ai_foundry.is_request_success",
    "azure.ai_foundry.sku",
    "azure.ai_foundry.properties.backend_request_body.model",
    "azure.ai_foundry.properties.backend_response_body.usage.total_tokens",
    "azure.ai_foundry.properties.backend_response_body.choices.finish_reason",
    "event.outcome",
    "source.geo.location",
)

_BULK_PARAMS = {"pipeline": "_none"}


# Guardrails dashboards need continuous content-filter / jailbreak / protected
# traffic. A 4% chance with one GatewayLogs doc per 30s leaves panels empty.
_BASELINE_CONTENT_FILTER_RATE = 0.25
_BASELINE_ERROR_STATUS_RATE = 0.05
_GATEWAY_LOGS_PER_TICK = 8

# Documentation IPs with stable geo (pipeline=_none → emit geo yourself).
_GEO_CALLERS = (
    {"ip": "89.160.20.156", "lat": 58.4167, "lon": 15.6167, "country": "SE", "city": "Linkoping"},
    {"ip": "89.160.20.112", "lat": 59.3293, "lon": 18.0686, "country": "SE", "city": "Stockholm"},
    {"ip": "198.51.100.10", "lat": 37.7749, "lon": -122.4194, "country": "US", "city": "San Francisco"},
    {"ip": "203.0.113.25", "lat": 51.5074, "lon": -0.1278, "country": "GB", "city": "London"},
)


def _profile_cloud(profile: dict, cfg: dict) -> dict[str, str]:
    return {
        "provider": "azure",
        "region": cfg.get("azure_region") or profile.get("azure_region", "eastus2"),
        "account_id": str(profile.get("azure_subscription_id") or ""),
        "account_name": profile.get("azure_subscription_name", "Demo Subscription"),
    }


def _resource_name(ns: str, cfg: dict) -> str:
    """Cognitive Services account and APIM host label, scoped to the scenario."""
    return ScenarioIdentity(ns).qualify(cfg.get("resource_name") or "ai-foundry-demo")


def _safe_filter_results() -> dict[str, Any]:
    return {
        "hate": {"filtered": False, "severity": "safe"},
        "self_harm": {"filtered": False, "severity": "safe"},
        "sexual": {"filtered": False, "severity": "safe"},
        "violence": {"filtered": False, "severity": "safe"},
        "jailbreak": {"filtered": False, "detected": False},
        "profanity": {"filtered": False, "detected": False},
        "protected_material_text": {"filtered": False, "detected": False},
        "protected_material_code": {"filtered": False, "detected": False},
    }


def _blocked_filter_results(severity: str, *, jailbreak: bool = False, protected: bool = False) -> dict[str, Any]:
    results = _safe_filter_results()
    results["hate"] = {"filtered": True, "severity": severity}
    results["violence"] = {"filtered": True, "severity": severity}
    if jailbreak:
        results["jailbreak"] = {"filtered": True, "detected": True}
    if protected:
        results["protected_material_text"] = {"filtered": True, "detected": True}
        results["protected_material_code"] = {"filtered": True, "detected": True}
    return results


def _filtered_categories(severity: str, *, jailbreak: bool = False, protected: bool = False) -> list[dict[str, str]]:
    cats = [
        {"category_name": "hate", "severity": severity},
        {"category_name": "violence", "severity": severity},
    ]
    if jailbreak:
        cats.append({"category_name": "jailbreak", "severity": severity})
    if protected:
        cats.append({"category_name": "protected_material_text", "severity": severity})
        cats.append({"category_name": "protected_material_code", "severity": severity})
    return cats


def build_metric_docs(
    ts: float,
    ns: str,
    profile: dict,
    cfg: dict,
    rng: random.Random,
    chaos: dict | None,
) -> list[dict[str, Any]]:
    """Emit success + error status_code series (Overview error-rate panels)."""
    cloud = _profile_cloud(profile, cfg)
    subscription_id = cloud["account_id"]
    region = cloud["region"]
    resource = _resource_name(ns, cfg)
    deployment = cfg.get("model_deployment_name", "gpt-4o-demo")
    model_name = profile.get("azure_model_name", deployment)
    load = business_load()

    lat_mult = float((chaos or {}).get("latency_high", 0.0) or 0.0)
    # Key unit: availability in tenths of a percent (990 = 99.0%, healthy). Explicit None
    # check: 0 is a valid value (total outage) and must not fall back to healthy.
    raw_availability = (chaos or {}).get("model_availability_above_threshold")
    availability = 990.0 if raw_availability is None else float(raw_availability)
    ptu = float((chaos or {}).get("provisioned_utilization_above_threshold", 0.0) or 0.0)

    if availability >= 990.0:
        availability = min(1.0, max(0.99, rng.gauss(0.997, 0.002)))
    else:
        availability = min(1.0, max(0.0, availability / 1000.0))
    if ptu > 0:
        ptu_val = min(0.999, max(ptu / 100.0, 0.86))
    else:
        ptu_val = max(0.0, min(0.70, rng.gauss(0.30, 0.08)))

    calls = max(1, int(rng.randint(10, 60) * load))
    input_tokens = calls * rng.randint(40, 200)
    output_tokens = calls * rng.randint(20, 120)
    total_tokens = input_tokens + output_tokens
    response_ms = float(max(200.0, min(4000.0, math.exp(rng.gauss(6.8, 0.5)))))
    if lat_mult > 0:
        response_ms = max(response_ms, lat_mult, 5001.0)
    ttft = response_ms * rng.uniform(0.05, 0.2)
    ttlb = response_ms * rng.uniform(0.9, 1.05)
    tps = max(1.0, total_tokens / max(response_ms / 1000.0, 0.1))

    sub_name = _subscription_name(profile, ns)

    def _one(status: str, reqs: int) -> dict[str, Any]:
        doc = build_base_doc(
            timestamp=ts,
            dataset="azure.ai_foundry",
            namespace=ns,
            doc_type="metrics",
            agent_type="metricbeat",
            cloud=cloud,
        )
        in_tok = max(1, reqs * rng.randint(40, 200))
        out_tok = max(0, reqs * rng.randint(10, 120))
        # Overview panels compare status_code as string ("200") and as number (>=400).
        dimensions = {
            "model_deployment_name": deployment,
            "model_name": model_name,
            "model_version": str(profile.get("azure_model_version", "1")),
            "api_name": "AIServices",
            "region": region,
            "status_code": status,
            "fingerprint": f"{deployment}-{status}-{region}",
        }
        doc["azure"] = {
            "subscription_id": subscription_id,
            "subscription_name": sub_name,
            "timegrain": "PT1M",
            "namespace": "Microsoft.CognitiveServices/accounts",
            "resource": {
                "name": resource,
                "id": (
                    f"/subscriptions/{subscription_id}/resourceGroups/"
                    f"{profile.get('azure_resource_group', ns)}/"
                    f"providers/Microsoft.CognitiveServices/accounts/{resource}"
                ),
                "type": "Microsoft.CognitiveServices/accounts",
                "group": profile.get("azure_resource_group", ns),
            },
            "ai_foundry": {
                "model_requests": {"total": reqs},
                "model_availability_rate": {"avg": float(availability)},
                "input_tokens": {"total": in_tok},
                "output_tokens": {"total": out_tok},
                "total_tokens": {"total": in_tok + out_tok},
                "provisioned_utilization": {"avg": float(ptu_val)},
                "tokens_per_second": {"avg": float(tps)},
                "time_to_response": {"avg": float(response_ms)},
                "normalized_time_to_first_token": {"avg": float(ttft)},
                "normalized_time_between_tokens": {"avg": float(rng.uniform(10.0, 40.0))},
                "time_to_last_byte": {"avg": float(ttlb)},
            },
            "dimensions": dimensions,
        }
        doc["metricset"] = {"name": "monitor", "period": 300000}
        doc["service"] = {"type": "azure"}
        doc["agent"]["id"] = f"azure-foundry-{subscription_id}-{status}"
        return doc

    docs = [_one("200", calls)]
    # Overview error panels filter status_code > 200 / >= 400.
    err_fraction = _BASELINE_ERROR_STATUS_RATE
    if availability < 0.99:
        err_fraction = max(err_fraction, 0.2)
    err_calls = max(1, int(calls * err_fraction))
    docs.append(_one("500", err_calls))
    docs.append(_one("429", max(1, err_calls // 2)))
    return docs


def _azure_log_envelope(
    ts: float,
    ns: str,
    profile: dict,
    cfg: dict,
    cloud: dict[str, str],
) -> dict[str, Any]:
    """Common azure.subscription_id + azure.resource for Guardrails controls."""
    resource = _resource_name(ns, cfg)
    subscription_id = cloud["account_id"]
    resource_group = profile.get("azure_resource_group", ns)
    return {
        "subscription_id": subscription_id,
        "resource": {
            "id": (
                f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/"
                f"providers/Microsoft.ApiManagement/service/{resource}-apim"
            ),
            "name": f"{resource}-apim",
            "group": resource_group,
            "type": "Microsoft.ApiManagement/service",
        },
    }


def _gateway_log(
    ts: float,
    ns: str,
    profile: dict,
    cfg: dict,
    rng: random.Random,
    chaos: dict | None,
    *,
    mode: str | None = None,
) -> dict[str, Any]:
    """Build one GatewayLogs doc.

    ``mode`` forces a Guardrails panel shape:
    - ``success`` — finish_reason stop
    - ``filter`` — content_filter (hate/violence)
    - ``jailbreak`` — jailbreak detected+filtered
    - ``protected`` — protected_material_* detected+filtered
    - ``None`` — probabilistic using baseline/chaos rates
    """
    cloud = _profile_cloud(profile, cfg)
    deployment = cfg.get("model_deployment_name", "gpt-4o-demo")
    prompt = rng.choice(profile.get("prompt_templates", ["Suggest a book to read."]))
    response = rng.choice(profile.get("response_templates", ["Try Project Hail Mary by Andy Weir."]))
    geo = rng.choice(_GEO_CALLERS)

    # Content filtering is baseline-only: no package alert and no declared impact key.
    filter_rate = _BASELINE_CONTENT_FILTER_RATE
    severity = "medium"

    if mode is None:
        if rng.random() < filter_rate:
            pick = rng.random()
            if pick < 0.35:
                mode = "jailbreak"
            elif pick < 0.60:
                mode = "protected"
            else:
                mode = "filter"
        else:
            mode = "success"

    is_filtered = mode != "success"
    jailbreak = mode == "jailbreak"
    protected = mode == "protected"

    # Same declared key as the metrics (time_to_response > 5000 ms), so logs agree.
    latency_high = float((chaos or {}).get("latency_high", 0.0) or 0.0)
    duration_ms = rng.randint(80, 800)
    if latency_high > 0:
        duration_ms = max(duration_ms, int(latency_high), 5001)

    prompt_tokens = rng.randint(20, 120)
    completion_tokens = 0 if is_filtered else rng.randint(10, 180)

    if is_filtered:
        filter_results = _blocked_filter_results(severity, jailbreak=jailbreak, protected=protected)
        filtered_cats = _filtered_categories(severity, jailbreak=jailbreak, protected=protected)
        backend_response_body: dict[str, Any] = {
            "id": f"chatcmpl-{uuid.uuid4().hex[:24]}",
            "model": deployment,
            "object": "chat.completion",
            "created": int(ts),
            "choices": [
                {
                    "index": 0,
                    "finish_reason": "content_filter",
                    "message": {"role": "assistant", "content": ""},
                    "content_filter_results": filter_results,
                }
            ],
            "error": {
                "code": "content_filter",
                "message": (
                    "The response was filtered due to the prompt triggering "
                    "Azure OpenAI's content management policy."
                ),
                "param": "prompt",
                "status": 400,
                "innererror": {
                    "code": "ResponsibleAIPolicyViolation",
                    "content_filtered_categories": filtered_cats,
                    "content_filter_result": filter_results,
                },
            },
            "content_filtered_categories": filtered_cats,
            "usage": {
                "input_tokens": prompt_tokens,
                "output_tokens": 0,
                "total_tokens": prompt_tokens,
            },
        }
        is_success = False
        outcome = "failure"
        backend_code = 400
        result_type = "Failed"
    else:
        backend_response_body = {
            "id": f"chatcmpl-{uuid.uuid4().hex[:24]}",
            "model": deployment,
            "object": "chat.completion",
            "created": int(ts),
            "choices": [
                {
                    "index": 0,
                    "finish_reason": "stop",
                    "message": {"role": "assistant", "content": response},
                    "content_filter_results": _safe_filter_results(),
                }
            ],
            "usage": {
                "input_tokens": prompt_tokens,
                "output_tokens": completion_tokens,
                "total_tokens": prompt_tokens + completion_tokens,
            },
        }
        is_success = True
        outcome = "success"
        backend_code = 200
        result_type = "Succeeded"

    resource = _resource_name(ns, cfg)
    subscription_id = cloud["account_id"]
    apim_host = f"{resource}-apim.azure-api.net"
    path = f"/deployments/{deployment}/chat/completions"
    query = "api-version=2024-02-15-preview"
    backend_url = (
        f"https://{resource}.openai.azure.com/openai/deployments/"
        f"{deployment}/chat/completions?{query}"
    )

    doc = build_base_doc(
        timestamp=ts,
        dataset="azure_ai_foundry.logs",
        namespace=ns,
        doc_type="logs",
        agent_type="filebeat",
        cloud=cloud,
    )
    doc["event"]["outcome"] = outcome
    doc["event"]["duration"] = int(duration_ms * 1_000_000)
    doc["http"] = {
        "request": {"method": "POST"},
        "response": {"status_code": backend_code},
    }
    doc["url"] = {
        "domain": apim_host,
        "original": f"https://{apim_host}{path}?{query}",
        "path": path,
        "query": query,
        "scheme": "https",
    }
    doc["log"] = {"level": "4"}
    doc["source"] = {
        "ip": geo["ip"],
        "geo": {
            "city_name": geo["city"],
            "country_iso_code": geo["country"],
            "location": {"lat": geo["lat"], "lon": geo["lon"]},
        },
    }
    azure = _azure_log_envelope(ts, ns, profile, cfg, cloud)
    azure["ai_foundry"] = {
        "category": "GatewayLogs",
        "correlation_id": str(uuid.uuid4()),
        "deployment_version": "0.44.18652.0",
        "duration_ms": duration_ms,
        "is_request_success": is_success,
        "location": cloud["region"],
        "operation_name": "Microsoft.ApiManagement/GatewayLogs",
        "result_type": result_type,
        "sku": rng.choice(["Basic", "Standard", "Premium"]),
        "caller_ip_address": geo["ip"],
        "timestamp": int(ts * 1000),
        "properties": {
            "api_id": "azure-ai-foundry-apim-api",
            "api_revision": "1",
            "backend_method": "POST",
            "backend_protocol": "HTTP/1.1",
            "backend_request_body": {
                "messages": [{"role": "user", "content": prompt}],
                "model": deployment,
            },
            "backend_response_body": backend_response_body,
            "backend_response_code": backend_code,
            "backend_time": duration_ms,
            "backend_url": backend_url,
            "cache": "none",
            "client_protocol": "HTTP/1.1",
            "client_tls_version": "1.3",
            "operation_id": "ChatCompletions_Create",
        },
    }
    doc["azure"] = azure
    return doc


def build_log_docs(
    ts: float,
    ns: str,
    profile: dict,
    cfg: dict,
    rng: random.Random,
    chaos: dict | None,
) -> list[dict[str, Any]]:
    cloud = _profile_cloud(profile, cfg)
    deployment = cfg.get("model_deployment_name", "gpt-4o-demo")
    docs: list[dict[str, Any]] = []
    envelope = _azure_log_envelope(ts, ns, profile, cfg, cloud)

    audit = build_base_doc(
        timestamp=ts,
        dataset="azure_ai_foundry.logs",
        namespace=ns,
        doc_type="logs",
        agent_type="filebeat",
        cloud=cloud,
    )
    audit["event"]["outcome"] = "success"
    audit["azure"] = {
        **envelope,
        "ai_foundry": {
            "category": "Audit",
            "operation_name": "ChatCompletions_Audit",
            "tenant": ScenarioIdentity(ns).azure_tenant_id,
            "asset_identity": deployment,
            "sku": "Standard",
            "properties": {"action": "ChatCompletion", "result": "Success"},
        },
    }
    docs.append(audit)

    # RequestResponse stays lightweight; APIM/Guardrails panels use GatewayLogs.
    rr = build_base_doc(
        timestamp=ts,
        dataset="azure_ai_foundry.logs",
        namespace=ns,
        doc_type="logs",
        agent_type="filebeat",
        cloud=cloud,
    )
    rr["event"]["outcome"] = "success"
    rr["azure"] = {
        **envelope,
        "ai_foundry": {
            "category": "RequestResponse",
            "correlation_id": str(uuid.uuid4()),
            "duration_ms": rng.randint(50, 400),
            "location": cloud["region"],
            "operation_name": "ChatCompletions",
            "result_signature": "Succeeded",
            "caller_ip_address": "198.51.100.10",
            "tenant": ScenarioIdentity(ns).azure_tenant_id,
            "sku": "Standard",
            "properties": {
                "model_deployment_name": deployment,
                "stream_type": "Synchronous",
                "object": "chat.completion",
            },
        },
    }
    docs.append(rr)

    # Guaranteed Guardrails shapes every tick, then fill remaining with mixed traffic.
    forced_modes = ("success", "filter", "jailbreak", "protected")
    for mode in forced_modes:
        docs.append(_gateway_log(ts, ns, profile, cfg, rng, chaos, mode=mode))
    extra = max(0, _GATEWAY_LOGS_PER_TICK - len(forced_modes))
    for _ in range(extra):
        docs.append(_gateway_log(ts, ns, profile, cfg, rng, chaos, mode=None))
    return docs



from generators.azure_ai_foundry import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop

def run(
    client: ESBulkClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    if scenario_data and "scenario" in scenario_data:
        scenario = scenario_data["scenario"]
        ns = scenario_data["namespace"]
    else:
        from app.config import ACTIVE_SCENARIO
        from scenario_engine import get_scenario

        scenario = get_scenario(ACTIVE_SCENARIO)
        ns = scenario.namespace

    profile = getattr(scenario, "llm_profile", None) or {
        "azure_subscription_id": ScenarioIdentity(ns).azure_subscription_id,
        "azure_region": "eastus2",
        "prompt_templates": ["Suggest a book to read."],
        "response_templates": ["Try Project Hail Mary by Andy Weir."],
    }
    cfg = {}
    if hasattr(scenario, "get_generator_config"):
        cfg = scenario.get_generator_config("azure_ai_foundry") or {}
    _channel_registry = scenario.channel_registry if hasattr(scenario, "channel_registry") else {}

    if not client.configured:
        logger.warning("ESBulkClient not configured; azure_ai_foundry will not emit.")
        return

    # Keep azure.dimensions as object+keyword so Overview status_code filters work
    # alongside azure_openai / azure_billing on shared metrics-* data views.
    for emit_ns in emit_namespaces(ns):
        ensure_dashboard_mappings_from_env(emit_ns)

    rng = random.Random()
    logger.info(
        "azure_ai_foundry generator started (namespace=%s, emit=%s) [%s]",
        ns,
        ",".join(emit_namespaces(ns)),
        SOURCE_INTEGRATION,
    )

    def _emit_tick() -> None:
        chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="azure_ai_foundry",
            chaos_mode="explicit",
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        ts = time.time()
        send_dual_bulk(
            client,
            "metrics-azure.ai_foundry",
            ns,
            build_metric_docs(ts, ns, profile, cfg, rng, chaos),
        )
        send_dual_bulk(
            client,
            "logs-azure_ai_foundry.logs",
            ns,
            build_log_docs(ts, ns, profile, cfg, rng, chaos),
        )
    run_tick_loop(stop_event, _emit_tick, interval=30)


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "60"))
    client = ESBulkClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
