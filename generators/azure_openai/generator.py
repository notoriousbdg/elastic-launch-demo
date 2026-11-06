"""Azure OpenAI telemetry generator.

Writes two data streams that the ``azure_openai`` Elastic integration package
expects:

- ``logs-azure_openai.logs-<ns>`` — Azure diagnostic logs (RequestResponse,
  Audit, and gateway categories including Content Filtering).
- ``metrics-azure.open_ai-<ns>`` — Azure Monitor metrics for the OpenAI resource.

OOTB package alert rules hardcode the ``default`` namespace
(``metrics-azure.open_ai-default``, ``logs-azure_openai.logs-default``). When
the scenario namespace is not ``default``, each batch is dual-written there so
GT / banking / inttest namespaces keep scenario-scoped data while OOTB alerts
still see tip.

Documents are sent with ``pipeline=_none`` to bypass the package
``default_pipeline``.  Fleet's ``final_pipeline`` still runs and adds
``event.ingested``.

All field names are fixed by the ``azure_openai`` package ``fields/*.yml`` files
and the fields queried by the package dashboards (Overview, PTU, Content
Filtering).  Scenario-specific values (resource names, subscription, resource
group, model deployments, tenant, prompt text) come from
``scenario.llm_profile``.

Billing metrics are emitted by the separate ``azure_billing`` generator.

The content-filter fault raises gateway log records with
``finish_reason=content_filter`` / ``error.code=content_filter`` and hate /
violence categories at severity ``high``.  The PTU fault raises
``provisioned_managed_utilization_v2.avg`` above 95 percent.
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/azure_openai@1.15.0"

import logging
import math
import os
import random
import signal
import threading
import time
import uuid
from typing import Any

from app.telemetry import ESBulkClient
from generator_engine.azure_common import (
    ensure_dashboard_mappings_from_env,
    subscription_name as _subscription_name,
)
from generator_engine.es_doc_common import (
    BULK_PARAMS,
    build_base_doc,
    business_load,
    emit_namespaces,
    resolve_active_chaos,
)

logger = logging.getLogger(__name__)

# ── Module-level declarations ────────────────────────────────────────────────

DATA_STREAMS = (
    "logs-azure_openai.logs",
    "metrics-azure.open_ai",
)

DOC_FIELDS = (
    "azure.open_ai.category",
    "azure.open_ai.operation_name",
    "azure.open_ai.result_signature",
    "azure.open_ai.model_deployment_name",
    "azure.open_ai.stream_type",
    "azure.open_ai.properties.model_deployment_name",
    "azure.open_ai.properties.request_time",
    "azure.open_ai.properties.response_time",
    "azure.open_ai.properties.backend_response_body.choices.finish_reason",
    "azure.open_ai.properties.backend_response_body.error.innererror.content_filter_result.custom_blocklists.id",
    "azure.open_ai.generated_tokens.total",
    "azure.open_ai.processed_prompt_tokens.total",
    "azure.open_ai.token_transaction.total",
    "azure.open_ai.requests.total",
    "azure.open_ai.time_to_response.avg",
    "azure.open_ai.active_tokens.total",
    "azure.open_ai.provisioned_managed_utilization_v2.avg",
    "azure.open_ai.context_tokens_cache_match_rate.avg",
    "azure.dimensions.model_deployment_name",
    "azure.dimensions.model_name",
    "azure.subscription_id",
    "azure.subscription_name",
)

# Azure diagnostic log categories.
_LOG_CATEGORIES = ("RequestResponse", "Audit", "ApiManagementGatewayLogs")

# Batch timing for the live loop.
_BATCH_INTERVAL_MIN = 10.0
_BATCH_INTERVAL_MAX = 20.0

# Pipeline parameter.
_BULK_PARAMS = {"pipeline": "_none"}

# OOTB azure_openai alert templates query *-default only.
_OOTB_NAMESPACE = "default"


# Always emit a small fraction of content-filtered gateway logs so the
# Content Filtering dashboard is populated even when no fault is active.
_BASELINE_CONTENT_FILTER_RATE = 0.03

# Healthy baseline: thin error tail so status pies are not 100% success.
# Keep 429s rare — quota alert fires when COUNT(429) > 5 per model.
_BASELINE_QUOTA_RATE = 0.002
_BASELINE_SERVER_ERROR_RATE = 0.012

_MODEL_TRAFFIC_WEIGHTS = {
    "gpt-4o": 45,
    "gpt-4.1": 20,
    "gpt-4.1-mini": 25,
    "text-embedding-3-large": 10,
}


def _resource_groups(profile: dict, ns: str) -> list[str]:
    groups = list(profile.get("azure_resource_groups") or [])
    primary = profile.get("azure_resource_group") or ns
    if primary and primary not in groups:
        groups.insert(0, primary)
    for extra in (ScenarioIdentity(ns).label("ai-rg"), ScenarioIdentity(ns).label("platform-rg")):
        if extra not in groups:
            groups.append(extra)
        if len(groups) >= 3:
            break
    return groups


def _openai_resources(profile: dict, ns: str) -> list[str]:
    resources = list(profile.get("azure_openai_resources") or [])
    if not resources:
        resources = [ScenarioIdentity(ns).label("openai-eastus")]
    for extra in (ScenarioIdentity(ns).label("openai-eastus2"), ScenarioIdentity(ns).label("openai-swedencentral")):
        if extra not in resources:
            resources.append(extra)
        if len(resources) >= 3:
            break
    return resources


def _openai_models(profile: dict) -> list[str]:
    models = list(profile.get("azure_openai_models") or [])
    for extra in ("gpt-4o", "gpt-4.1-mini", "gpt-4.1", "text-embedding-3-large"):
        if extra not in models:
            models.append(extra)
        if len(models) >= 4:
            break
    return models


def _pick_model(profile: dict, rng: random.Random) -> str:
    models = _openai_models(profile)
    weights = [_MODEL_TRAFFIC_WEIGHTS.get(m, 12) for m in models]
    return rng.choices(models, weights=weights, k=1)[0]


def _pick_resource(profile: dict, ns: str, rng: random.Random) -> str:
    resources = _openai_resources(profile, ns)
    weights = [55, 30, 15][: len(resources)]
    return rng.choices(resources, weights=weights, k=1)[0]


def _pick_resource_group(profile: dict, ns: str, rng: random.Random) -> str:
    groups = _resource_groups(profile, ns)
    weights = [60, 25, 15][: len(groups)]
    return rng.choices(groups, weights=weights, k=1)[0]


def _model_version(model: str) -> str:
    if "gpt-4.1" in model:
        return "2025-04-14"
    if "gpt-4o" in model:
        return "2024-08-06"
    return "2024-05-01"


def _safe_filter_results() -> dict:
    return {
        "hate": {"filtered": False, "severity": "safe"},
        "violence": {"filtered": False, "severity": "safe"},
        "sexual": {"filtered": False, "severity": "safe"},
        "self_harm": {"filtered": False, "severity": "safe"},
        "jailbreak": {"filtered": False, "detected": False},
        "profanity": {"filtered": False, "detected": False},
        # Content Filter saved search columns include custom_blocklists.id.
        "custom_blocklists": {"filtered": False, "id": "demo-allowlist"},
    }


def _blocked_filter_results(
    hate_severity: str,
    violence_severity: str,
    *,
    blocklist_id: str = "demo-blocklist",
    blocklist_filtered: bool = False,
) -> dict:
    return {
        "hate": {
            "filtered": hate_severity in ("medium", "high"),
            "severity": hate_severity,
        },
        "violence": {
            "filtered": violence_severity in ("medium", "high"),
            "severity": violence_severity,
        },
        "sexual": {"filtered": False, "severity": "safe"},
        "self_harm": {"filtered": False, "severity": "safe"},
        "jailbreak": {"filtered": False, "detected": False},
        "profanity": {"filtered": False, "detected": False},
        "custom_blocklists": {
            "filtered": blocklist_filtered,
            "id": blocklist_id,
        },
    }


def _filtered_categories(hate_severity: str, violence_severity: str) -> list[dict]:
    cats: list[dict] = []
    if hate_severity in ("medium", "high"):
        cats.append({"category_name": "hate", "severity": hate_severity})
    if violence_severity in ("medium", "high"):
        cats.append({"category_name": "violence", "severity": violence_severity})
    if not cats:
        cats.append({"category_name": "hate", "severity": "high"})
    return cats


# ── Document builders ────────────────────────────────────────────────────────

def _build_log_doc(
    ts: float,
    ns: str,
    profile: dict,
    rng: random.Random,
    chaos: dict | None,
    *,
    category: str = "RequestResponse",
) -> dict:
    """Build one ``logs-azure_openai.logs`` document."""
    subscription_id = profile.get("azure_subscription_id", "")
    subscription_name = _subscription_name(profile, ns)
    resource = _pick_resource(profile, ns, rng)
    resource_group = _pick_resource_group(profile, ns, rng)
    model = _pick_model(profile, rng)
    model_version = _model_version(model)

    prompt_tokens = int(math.exp(rng.gauss(4.8, 1.0)))
    prompt_tokens = max(1, min(prompt_tokens, 8000))
    completion_tokens = int(math.exp(rng.gauss(2.7, 1.2)))
    completion_tokens = max(1, min(completion_tokens, 4096))
    response_ms = int(math.exp(rng.gauss(7.5, 0.7)))
    response_ms = max(100, min(response_ms, 4000))
    latency_high = float((chaos or {}).get("latency_high", 0.0) or 0.0)
    response_ms = max(100, min(response_ms, 4000))
    if latency_high > 0:
        response_ms = max(response_ms, int(latency_high), 5001)
    request_ms = max(5, int(response_ms * rng.uniform(0.02, 0.08)))

    prompt_text = rng.choice(profile.get("prompt_templates", ["Hello."]))
    response_text = rng.choice(profile.get("response_templates", ["Hello."]))
    stream_type = rng.choice(["Synchronous", "Streaming"])

    # Content-filter fault only applies to gateway diagnostic logs.
    is_filtered = False
    hate_severity = "safe"
    violence_severity = "safe"
    if category == "ApiManagementGatewayLogs":
        # Baseline-only: no package alert and no declared impact key drives this.
        if rng.random() < _BASELINE_CONTENT_FILTER_RATE:
            is_filtered = True
            severity = "medium"
            hate_severity = severity
            violence_severity = severity

    quota_rate = float((chaos or {}).get("quota_error_rates_above_threshold", 0.0) or 0.0)
    if is_filtered:
        result_signature = "400"
        outcome = "failure"
        is_success = False
    elif quota_rate > 0 and rng.random() < max(quota_rate, 0.5):
        result_signature = "429"
        outcome = "failure"
        is_success = False
    elif rng.random() < _BASELINE_QUOTA_RATE:
        result_signature = "429"
        outcome = "failure"
        is_success = False
    elif rng.random() < _BASELINE_SERVER_ERROR_RATE:
        result_signature = "500"
        outcome = "failure"
        is_success = False
    else:
        result_signature = "200"
        outcome = "success"
        is_success = True

    doc = build_base_doc(
        timestamp=ts,
        dataset="azure_openai.logs",
        namespace=ns,
        doc_type="logs",
        agent_type="azure-eventhub",
        cloud={
            "provider": "azure",
            "region": profile.get("azure_region", "eastus"),
            "account_id": subscription_id,
            "account_name": subscription_name,
        },
    )

    properties: dict[str, Any] = {
        "model_deployment_name": model,
        "model_name": model,
        "model_version": model_version,
        "stream_type": stream_type,
        "api_name": "ChatCompletions",
        "request_time": request_ms,
        "response_time": response_ms,
        "request_length": float(len(prompt_text) + rng.randint(50, 400)),
        "response_length": float(0 if is_filtered else len(response_text) + rng.randint(50, 800)),
        "object_id": str(uuid.uuid4()),
        "operation_id": "ChatCompletions_Create",
    }

    if category in ("RequestResponse", "ApiManagementGatewayLogs"):
        backend_request_body = {
            "messages": [
                {"role": "user", "content": prompt_text},
            ],
            "model": model,
            "stream": stream_type == "Streaming",
        }

        if is_filtered:
            filtered_cats = _filtered_categories(hate_severity, violence_severity)
            # Occasionally attribute the block to a custom blocklist so the
            # Content Filter search column custom_blocklists.id is populated.
            blocklist_hit = rng.random() < 0.35
            blocklist_id = ScenarioIdentity(ns).label("blocked-prompts")
            filter_results = _blocked_filter_results(
                hate_severity,
                violence_severity,
                blocklist_id=blocklist_id,
                blocklist_filtered=blocklist_hit,
            )
            if blocklist_hit:
                filtered_cats = list(filtered_cats) + [
                    {"category_name": "custom_blocklists", "severity": hate_severity}
                ]
            backend_response_body: dict[str, Any] = {
                "id": f"chatcmpl-{uuid.uuid4().hex[:24]}",
                "model": model,
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
            completion_tokens = 0
        else:
            backend_response_body = {
                "id": f"chatcmpl-{uuid.uuid4().hex[:24]}",
                "model": model,
                "object": "chat.completion",
                "created": int(ts),
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": response_text},
                        "content_filter_results": _safe_filter_results(),
                    }
                ],
                "usage": {
                    "input_tokens": prompt_tokens,
                    "output_tokens": completion_tokens,
                    "total_tokens": prompt_tokens + completion_tokens,
                },
            }

        properties["backend_request_body"] = backend_request_body
        properties["backend_response_body"] = backend_response_body
        properties["backend_response_code"] = int(result_signature)
        properties["backend_method"] = "POST"
        properties["backend_protocol"] = "https"
        properties["backend_time"] = response_ms
        properties["client_protocol"] = "https"

    azure_open_ai: dict[str, Any] = {
        "category": category,
        "operation_name": "ChatCompletions_Create",
        "result_signature": result_signature,
        "is_request_success": is_success,
        "location": profile.get("azure_region", "eastus"),
        "correlation_id": str(uuid.uuid4()),
        "caller_ip_address": (
            f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"
        ),
        "model_deployment_name": model,
        "stream_type": stream_type,
        "properties": properties,
    }

    doc["azure"] = {
        "open_ai": azure_open_ai,
        "subscription_id": subscription_id,
        "subscription_name": subscription_name,
        "resource": {
            "id": (
                f"/subscriptions/{subscription_id}/"
                f"resourceGroups/{resource_group}/"
                f"providers/Microsoft.CognitiveServices/accounts/{resource}"
            ),
            "name": resource,
            "group": resource_group,
            "type": "Microsoft.CognitiveServices/accounts",
        },
    }

    doc["event"]["category"] = category
    doc["event"]["action"] = "ChatCompletions_Create"
    doc["event"]["outcome"] = outcome
    # ECS event.duration is nanoseconds.
    doc["event"]["duration"] = int(response_ms * 1_000_000)

    # Quota alert filters http.response.status_code == 429 and groups by
    # azure.dimensions.model_deployment_name.
    doc["http"] = {"response": {"status_code": int(result_signature)}}
    doc["azure"]["dimensions"] = {"model_deployment_name": model}

    callers = profile.get("callers", [])
    if callers:
        caller = rng.choice(callers)
        doc["client"] = {"user": {"email": caller["email"]}}

    return doc


def _build_metrics_doc(
    ts: float,
    ns: str,
    profile: dict,
    rng: random.Random,
    calls: int,
    chaos: dict | None,
) -> dict:
    """Build one ``metrics-azure.open_ai`` document."""
    subscription_id = profile.get("azure_subscription_id", "")
    subscription_name = _subscription_name(profile, ns)
    resource = _pick_resource(profile, ns, rng)
    resource_group = _pick_resource_group(profile, ns, rng)
    model = _pick_model(profile, rng)
    model_version = _model_version(model)
    stream_type = rng.choice(["Synchronous", "Streaming"])

    doc = build_base_doc(
        timestamp=ts,
        dataset="azure.open_ai",
        namespace=ns,
        doc_type="metrics",
        agent_type="azure-monitor",
        cloud={
            "provider": "azure",
            "region": profile.get("azure_region", "eastus"),
            "account_id": subscription_id,
            "account_name": subscription_name,
        },
    )

    # Generated (completion) tokens and processed prompt tokens.
    generated_tokens = calls * rng.randint(20, 80)
    processed_prompt_tokens = calls * rng.randint(40, 200)
    if "embedding" in model:
        generated_tokens = max(1, int(generated_tokens * 0.15))
        processed_prompt_tokens = int(processed_prompt_tokens * rng.uniform(1.2, 2.0))
    elif model == "gpt-4o":
        generated_tokens = int(generated_tokens * rng.uniform(1.1, 1.6))
    token_transaction = generated_tokens + processed_prompt_tokens

    response_ms = int(math.exp(rng.gauss(7.5, 0.6)))
    response_ms = max(200, min(response_ms, 4000))
    latency_high = float((chaos or {}).get("latency_high", 0.0) or 0.0)
    if latency_high > 0:
        response_ms = max(response_ms, int(latency_high), 5001)

    ptu_target = float((chaos or {}).get("provisioned_utilization_above_threshold", 0.0) or 0.0)
    if ptu_target > 0:
        ptu_util = min(0.999, max(ptu_target / 100.0, 0.86) + rng.gauss(0, 0.01))
    else:
        # Spread PTU across resources: primary higher, others quieter.
        base = 0.22 if resource.endswith("eastus") else 0.12
        ptu_util = max(0.02, min(0.78, rng.gauss(base, 0.12)))

    active_tokens = calls * rng.randint(100, 600)
    # Cache match rate is higher when PTU is healthy; dips under load.
    if ptu_target > 0:
        cache_rate = max(5.0, min(40.0, rng.gauss(25.0, 8.0)))
    else:
        cache_rate = max(25.0, min(95.0, rng.gauss(68.0, 14.0)))

    # Metrics status_code dimension: mostly 200 with a thin error tail.
    if rng.random() < _BASELINE_QUOTA_RATE:
        status_code = "429"
    elif rng.random() < _BASELINE_SERVER_ERROR_RATE:
        status_code = "500"
    else:
        status_code = "200"

    doc["azure"] = {
        "open_ai": {
            "model_deployment_name": model,
            "generated_tokens": {"total": generated_tokens},
            "processed_prompt_tokens": {"total": processed_prompt_tokens},
            "token_transaction": {"total": token_transaction},
            "requests": {"total": calls},
            "time_to_response": {"avg": float(response_ms)},
            "active_tokens": {"total": active_tokens},
            "provisioned_managed_utilization_v2": {"avg": float(ptu_util)},
            "context_tokens_cache_match_rate": {"avg": round(cache_rate, 1)},
        },
        "resource": {
            "id": (
                f"/subscriptions/{subscription_id}/"
                f"resourceGroups/{resource_group}/"
                f"providers/Microsoft.CognitiveServices/accounts/{resource}"
            ),
            "name": resource,
            "group": resource_group,
            "type": "Microsoft.CognitiveServices/accounts",
        },
        "subscription_id": subscription_id,
        "subscription_name": subscription_name,
        "dimensions": {
            "model_deployment_name": model,
            "model_name": model,
            "model_version": model_version,
            "api_name": "ChatCompletions" if "embedding" not in model else "Embeddings",
            "stream_type": stream_type,
            "status_code": status_code,
        },
        "timegrain": "PT1M",
        "namespace": "Microsoft.CognitiveServices/accounts",
    }

    doc["metricset"] = {"period": 60000}

    return doc


# ── Live run loop ────────────────────────────────────────────────────────────


from generators.azure_openai import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop

def run(
    client: ESBulkClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run the Azure OpenAI generator loop until ``stop_event`` is set."""
    if scenario_data and "scenario" in scenario_data:
        scenario = scenario_data["scenario"]
        ns = scenario_data["namespace"]
    else:
        from app.config import ACTIVE_SCENARIO
        from scenario_engine import get_scenario
        scenario = get_scenario(ACTIVE_SCENARIO)
        ns = scenario.namespace

    profile = scenario.llm_profile
    _channel_registry = scenario.channel_registry if hasattr(scenario, "channel_registry") else {}

    if not client.configured:
        logger.warning("ESBulkClient not configured; Azure OpenAI generator will not emit.")
        return

    target_ns = emit_namespaces(ns)
    for emit_ns in target_ns:
        ensure_dashboard_mappings_from_env(emit_ns)

    rng = random.Random()
    logger.info(
        "Azure OpenAI generator started (namespace=%s, emit=%s)",
        ns,
        ",".join(target_ns),
    )

    interval = None
    def _emit_tick() -> None:
        nonlocal emit_ns, interval
        chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="azure_openai",
            chaos_mode="explicit",
            impact_keys=_ALERT_SPEC.impact_keys,
        )

        load = business_load()
        now = time.time()
        calls = max(1, int(load * rng.randint(20, 60)))

        for emit_ns in target_ns:
            # Diagnostic logs: RequestResponse + Audit + gateway (content filtering).
            log_docs = []
            for _ in range(calls):
                cat = rng.choices(
                    _LOG_CATEGORIES,
                    weights=[0.7, 0.1, 0.2],
                )[0]
                log_docs.append(_build_log_doc(
                    now + rng.uniform(-5, 0), emit_ns, profile, rng, chaos, category=cat
                ))
            client.send_bulk(
                f"logs-azure_openai.logs-{emit_ns}", log_docs, params=BULK_PARAMS
            )

            # Metrics.
            metrics_doc = _build_metrics_doc(now, emit_ns, profile, rng, calls, chaos)
            client.send_bulk(
                f"metrics-azure.open_ai-{emit_ns}", [metrics_doc], params=BULK_PARAMS
            )

        logger.debug(
            "Azure OpenAI: sent %d log docs x %d ns (load=%.2f, chaos=%s)",
            calls,
            len(target_ns),
            load,
            bool(chaos),
        )
        interval = rng.uniform(_BATCH_INTERVAL_MIN, _BATCH_INTERVAL_MAX)
    run_tick_loop(stop_event, _emit_tick, interval=lambda: interval)

    logger.info("Azure OpenAI generator stopped.")


# ── Standalone entry point ────────────────────────────────────────────────────

def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    client = ESBulkClient()
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    duration = int(os.environ.get("RUN_DURATION", "60"))
    timer = threading.Timer(duration, stop_event.set)
    timer.daemon = True
    timer.start()
    logger.info("Running for %ds (standalone mode)", duration)

    run(client, stop_event)
    timer.cancel()
    client.close()


if __name__ == "__main__":
    main()
