"""AWS Bedrock telemetry generator.

Writes three data streams that the ``aws_bedrock`` Elastic integration package
expects:

- ``logs-aws_bedrock.invocation-<ns>`` — one log doc per LLM call.
- ``metrics-aws_bedrock.runtime-<ns>`` — 5-min aggregated runtime metrics.
- ``metrics-aws_bedrock.guardrails-<ns>`` — 5-min guardrail metrics.

Package dashboards and data views are installed against the ``default``
namespace. When the scenario namespace is not ``default``, each batch is
dual-written there so Overview / Guardrails panels keep working while
scenario-scoped streams remain available for GT.

Documents are sent with ``pipeline=_none`` to bypass the package
``default_pipeline``.  The Fleet ``final_pipeline`` still runs and adds
``event.ingested``.

All field names are fixed by the ``aws_bedrock`` package ``fields/*.yml`` files.
Scenario-specific values (prompt text, model names, account identifiers,
guardrail names) come from ``scenario.llm_profile``.

The three data streams share one simulation tick so invocation counts agree
with runtime ``invocations`` and guardrail ``invocations_intervened``.
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/aws_bedrock@2.2.0"

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
from generator_engine.es_doc_common import (
    build_base_doc,
    business_load,
    resolve_active_chaos,
    send_dual_bulk,
)

logger = logging.getLogger(__name__)

# ── Module-level declarations ────────────────────────────────────────────────

DATA_STREAMS = (
    "logs-aws_bedrock.invocation",
    "metrics-aws_bedrock.runtime",
    "metrics-aws_bedrock.guardrails",
)

DOC_FIELDS = (
    "cloud.account.name",
    "gen_ai.system",
    "gen_ai.request.model.id",
    "gen_ai.request.id",
    "gen_ai.request.max_tokens",
    "gen_ai.request.temperature",
    "gen_ai.request.top_p",
    "gen_ai.input.messages",
    "gen_ai.output.messages",
    "gen_ai.response.finish_reasons",
    "gen_ai.usage.prompt_tokens",
    "gen_ai.usage.completion_tokens",
    "gen_ai.performance.response_time",
    "gen_ai.compliance.violation_detected",
    "gen_ai.guardrail_id",
    "gen_ai.policy.match_detail.score",
    "gen_ai.policy.match_detail.action",
    "aws_bedrock.invocation.model_id",
    "aws_bedrock.invocation.request_id",
    "aws_bedrock.invocation.error_code",
    "aws_bedrock.runtime.invocations",
    "aws_bedrock.runtime.invocation_latency",
    "aws_bedrock.runtime.invocation_client_errors",
    "aws_bedrock.runtime.invocation_server_errors",
    "aws_bedrock.runtime.invocation_throttles",
    "aws_bedrock.runtime.input_token_count",
    "aws_bedrock.runtime.output_token_count",
    "aws_bedrock.runtime.output_image_count",
    "aws_bedrock.guardrails.invocations",
    "aws_bedrock.guardrails.invocations_intervened",
    "aws_bedrock.guardrails.invocation_latency",
    "aws_bedrock.guardrails.guardrail_policy_type",
)

# AWS error codes that appear in the invocation logs.
_ERROR_CODES = (
    "ValidationException",
    "AccessDeniedException",
    "ThrottlingException",
    "ServiceUnavailableException",
    "ModelNotReadyException",
    "ModelStreamErrorException",
)

# Provider finish reasons.
_FINISH_REASONS = ("end_turn", "max_tokens", "tool_use", "stop_sequence")
_FINISH_WEIGHTS = (0.45, 0.44, 0.10, 0.01)

# Guardrail policy types that appear in guardrail metrics.
_GUARDRAIL_POLICY_TYPES = (
    "CONTENT_POLICY",
    "SENSITIVE_INFORMATION_POLICY",
    "WORD_POLICY",
    "GROUNDING_POLICY",
    "CONTEXTUAL_GROUNDING_POLICY",
)

_IMAGE_MODEL_MARKERS = ("image-generator", "stable-diffusion", "nova-canvas")

# Image models take image prompts, not the scenario's text-chat turns. A profile can set
# ``image_prompt_templates``; these generic prompts are the fallback.
_DEFAULT_IMAGE_PROMPTS = (
    "Generate a clean product banner with a blue gradient background.",
    "Create an illustration of a support agent helping a customer.",
    "Design a minimalist dashboard icon set in flat style.",
    "Render a photorealistic office scene with natural light.",
)


def _is_image_model(model_id: str) -> bool:
    lower = model_id.lower()
    return any(marker in lower for marker in _IMAGE_MODEL_MARKERS)


def _account_cloud(profile: dict) -> dict:
    return {
        "provider": "aws",
        "region": profile["aws_region"],
        "account_id": profile["aws_account_id"],
        "account_name": profile.get(
            "aws_account_name",
            f"{profile.get('aws_account_id', 'aws')}-account",
        ),
    }


def _model_version(model_id: str) -> str:
    if ":" in model_id:
        return model_id.rsplit(":", 1)[-1]
    return "1"

# Batch timing for the live loop.
_BATCH_INTERVAL_MIN = 8.0
_BATCH_INTERVAL_MAX = 15.0

# Guardrail content sources — package dashboard KQL uses title case.
_CONTENT_SOURCES = ("Input", "Output")

def _emit_namespaces(ns: str) -> list[str]:
    """Scenario namespace plus ``default`` for package dashboards."""
    if ns == _OOTB_NAMESPACE:
        return [ns]
    return [ns, _OOTB_NAMESPACE]


def _docs_for_namespace(docs: list[dict], emit_ns: str) -> list[dict]:
    """Clone docs with ``data_stream.namespace`` rewritten to ``emit_ns``."""
    out: list[dict] = []
    for doc in docs:
        copy = dict(doc)
        ds = dict(doc.get("data_stream") or {})
        ds["namespace"] = emit_ns
        copy["data_stream"] = ds
        out.append(copy)
    return out


def _send_stream(client: ESBulkClient, dataset: str, ns: str, docs: list[dict]) -> None:
    """Bulk-index ``docs`` to every emit namespace (scenario + default)."""
    for emit_ns in _emit_namespaces(ns):
        client.send_bulk(
            f"{dataset}-{emit_ns}",
            _docs_for_namespace(docs, emit_ns),
            params=_BULK_PARAMS,
        )


# ── Simulation helpers ────────────────────────────────────────────────────────

def _weighted_choice(rng: random.Random, items: tuple, weights: tuple) -> Any:
    """Pick one item using the given weights."""
    r = rng.random()
    cumulative = 0.0
    for item, w in zip(items, weights):
        cumulative += w
        if r <= cumulative:
            return item
    return items[-1]


def _invocation_count(rng: random.Random, load: float, chaos: dict | None) -> int:
    """Number of LLM invocations to generate for one tick."""
    base = int(load * rng.randint(40, 120))
    if chaos:
        # Throttle fault: fewer successful calls but more throttle errors.
        mult = chaos.get("throttle_multiplier", 1.0)
        if mult > 1.0:
            base = max(1, int(base / mult))
    return max(1, base)


def _error_rate(chaos: dict | None) -> float:
    """Fraction of calls that produce an error document.

    Baseline is high enough that Discover's ``error_code`` column is visibly
    populated (success rows omit the field — ES|QL shows those as null).
    """
    base = 0.12
    if chaos:
        mult = chaos.get("throttle_multiplier", 1.0)
        if mult > 1.0:
            # Throttle fault raises the ThrottlingException share.
            return min(0.4, base * mult)
    return base


def _error_quota(invocations: int, chaos: dict | None) -> int:
    """Minimum error docs per batch (~12%, at least one)."""
    return max(1, int(invocations * _error_rate(chaos)))


def _guardrail_intervention_rate(chaos: dict | None) -> float:
    """Fraction of calls where the guardrail intervenes.

    A small baseline keeps the Guardrails dashboard populated outside chaos.
    """
    baseline = 0.04
    if chaos:
        rate = chaos.get("guardrail_intervention_rate", 0.0)
        if rate > 0:
            return float(rate)
    return baseline


# ── Document builders ────────────────────────────────────────────────────────

def _build_invocation_doc(
    ts: float,
    ns: str,
    profile: dict,
    rng: random.Random,
    chaos: dict | None,
    *,
    force_error: bool = False,
) -> dict:
    """Build one ``logs-aws_bedrock.invocation`` document."""
    doc = build_base_doc(
        timestamp=ts,
        dataset="aws_bedrock.invocation",
        namespace=ns,
        doc_type="logs",
        agent_type="aws-s3",
        cloud=_account_cloud(profile),
    )

    model = rng.choice(profile["bedrock_models"])
    request_id = str(uuid.uuid4())

    # Token counts: skewed lognormal to match real distribution.
    prompt_tokens = int(math.exp(rng.gauss(4.8, 1.2)))  # median ~121
    prompt_tokens = max(1, min(prompt_tokens, 8000))
    completion_tokens = int(math.exp(rng.gauss(2.7, 1.4)))  # median ~15
    completion_tokens = max(1, min(completion_tokens, 2000))
    if _is_image_model(model):
        completion_tokens = 0

    # Response time in ms: lognormal, heavier tail.
    response_ms = int(math.exp(rng.gauss(8.6, 0.8)))  # median ~5400
    response_ms = max(100, min(response_ms, 60000))
    if chaos:
        response_ms = int(response_ms * float(chaos.get("latency_multiplier", 1.0)))

    err_rate = _error_rate(chaos)
    is_error = force_error or rng.random() < err_rate

    finish_reason = _weighted_choice(rng, _FINISH_REASONS, _FINISH_WEIGHTS)
    if is_error:
        # Pick a weighted error code.
        error_code = rng.choices(
            _ERROR_CODES,
            weights=[0.5, 0.25, 0.12, 0.07, 0.04, 0.02],
        )[0]
        if chaos and chaos.get("throttle_multiplier", 1.0) > 1.0:
            # Make ThrottlingException more likely during the throttle fault.
            error_code = rng.choices(
                _ERROR_CODES,
                weights=[0.2, 0.1, 0.55, 0.08, 0.05, 0.02],
            )[0]
    else:
        error_code = None

    # Prompt / response text from the scenario.
    is_image = _is_image_model(model)
    if is_image:
        prompt_text = rng.choice(profile.get("image_prompt_templates") or _DEFAULT_IMAGE_PROMPTS)
        response_text = ""  # image models return images, never text
    else:
        prompt_text = rng.choice(profile.get("prompt_templates", ["Hello."]))
        response_text = rng.choice(profile.get("response_templates", ["Hello."]))
    request_size = max(64, len(prompt_text) + prompt_tokens * 4)
    response_size = max(32, len(response_text) + completion_tokens * 4)
    start_response_ms = max(20, int(response_ms * rng.uniform(0.05, 0.25)))

    doc["gen_ai"] = {
        "system": "aws.bedrock",
        "request": {
            "id": request_id,
            "model": {
                "id": model,
                "role": "assistant",
                "version": _model_version(model),
                "type": "image" if _is_image_model(model) else "chat",
            },
        },
        "response": {
            "id": str(uuid.uuid4()),
            "finish_reasons": [finish_reason],
            "model": model,
        },
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
        },
        "performance": {
            "response_time": response_ms,
            "start_response_time": start_response_ms,
            "request_size": request_size,
            "response_size": response_size,
        },
        "prompt": prompt_text,
    }
    if not is_image:
        doc["gen_ai"]["completion"] = response_text
    if not is_image:
        # aws_bedrock@2.1.0 maps Converse inference parameters and the request/response
        # messages to gen_ai.*; this generator bypasses pipelines, so write that shape.
        # Failed invocations have no model output.
        doc["gen_ai"]["request"].update({
            "max_tokens": rng.choice((512, 1024, 2048, 4096)),  # long
            "temperature": round(rng.uniform(0.0, 1.0), 2),     # double
            "top_p": round(rng.uniform(0.5, 1.0), 2),           # double
        })
        doc["gen_ai"]["input"] = {
            "messages": [{"role": "user", "content": [{"text": prompt_text}]}],
        }
        if not is_error:
            doc["gen_ai"]["output"] = {
                "messages": [{"role": "assistant", "content": [{"text": response_text}]}],
            }

    inv: dict[str, Any] = {
        "model_id": model,
        "request_id": request_id,
        "schema_type": "bedrock.invoke_model",
        "schema_version": "1.0",
        "task_type": "IMAGE_GENERATION" if _is_image_model(model) else "TEXT_GENERATION",
        "input": {
            "input_token_count": prompt_tokens,
            "input_content_type": "application/json",
        },
        "output": {
            "output_token_count": completion_tokens,
            "output_content_type": "application/json",
        },
        "result": "error" if is_error else "success",
    }
    if not is_image:
        inv["output"]["completion_text"] = response_text
    if _is_image_model(model) and not is_error:
        inv["image_generation_config"] = {
            "number_of_images": rng.randint(1, 4),
            "quality": rng.choice(["standard", "premium"]),
            "height": 1024,
            "width": 1024,
            "cfg_scale": round(rng.uniform(5.0, 10.0), 1),
            "seed": rng.randint(0, 2_000_000_000),
        }

    if is_error and error_code:
        doc["error"] = {"type": error_code, "message": f"AWS Bedrock: {error_code}"}
        doc["event"]["outcome"] = "failure"
        inv["error"] = error_code
        inv["error_code"] = error_code
        doc["gen_ai"]["response"]["error_code"] = error_code
    else:
        doc["event"]["outcome"] = "success"

    # Guardrail overlay: text models only (keeps prompt/completion useful).
    # The package Discover panel filters on ``gen_ai.guardrail_id`` only (not
    # ``violation_detected``), so every guardrail-checked doc must carry a
    # non-null ``violation_code`` or the column reads as blank. Bias toward
    # interventions so the "where guardrails intervened" table is populated.
    intervention_rate = _guardrail_intervention_rate(chaos)
    has_guardrail = (
        len(profile.get("bedrock_guardrail_id", "")) > 0
        and not _is_image_model(model)
        and rng.random() < max(0.30, intervention_rate * 3)
    )
    intervened = False
    if has_guardrail:
        # Among guardrail-checked calls, intervene most of the time.
        intervened = rng.random() < max(0.65, intervention_rate)
        policy_type = rng.choice(_GUARDRAIL_POLICY_TYPES)
        score = round(rng.uniform(0.6, 0.99), 3) if intervened else round(rng.uniform(0.1, 0.55), 3)
        threshold = round(rng.uniform(0.55, 0.75), 3)
        action = "BLOCKED" if intervened else "NONE"
        doc["gen_ai"]["compliance"] = {
            "violation_detected": intervened,
            # Always set — panel shows all guardrail_id docs, not only intervened.
            "violation_code": policy_type,
        }
        if intervened:
            inv["output"]["completion_text"] = response_text
            inv["result"] = "success"
        doc["gen_ai"]["guardrail_id"] = profile["bedrock_guardrail_id"]
        doc["gen_ai"]["policy"] = {
            "name": policy_type,
            "violation": intervened,
            "action": action,
            "match_detail": {
                "score": score,
                "threshold": threshold,
                "action": action,
            },
        }

    doc["aws_bedrock"] = {"invocation": inv}

    # Caller identity from the scenario personas.
    callers = profile.get("callers", [])
    if callers:
        caller = rng.choice(callers)
        doc["client"] = {"user": {"email": caller["email"]}}
        doc["source"] = {
            "geo": {
                "country_iso_code": caller.get("country", "US"),
                "city_name": caller.get("city", "Unknown"),
                "region_name": caller.get("city", "Unknown"),
                "location": {
                    "lat": caller.get("lat", 0.0),
                    "lon": caller.get("lon", 0.0),
                },
            }
        }

    return doc


def _build_runtime_doc(
    ts: float,
    ns: str,
    profile: dict,
    model_id: str | None,
    rng: random.Random,
    invocations: int,
    chaos: dict | None,
) -> dict:
    """Build one ``metrics-aws_bedrock.runtime`` document.

    ``model_id=None`` produces an account-level total (matching the real data
    where ~25% of documents have no model_id).
    """
    doc = build_base_doc(
        timestamp=ts,
        dataset="aws_bedrock.runtime",
        namespace=ns,
        doc_type="metrics",
        agent_type="aws-cloudwatch",
        cloud=_account_cloud(profile),
    )

    latency_ms = int(math.exp(rng.gauss(7.2, 0.5)))  # median ~1300
    latency_ms = max(50, min(latency_ms, 30000))
    if chaos:
        latency_ms = int(latency_ms * float(chaos.get("latency_multiplier", 1.0)))

    client_errors = rng.randint(0, 5)
    server_errors = rng.randint(0, 2)
    # Lens emptyAsNull:true maps sum(0) → null; keep a healthy baseline.
    _BASELINE_THROTTLES = 1
    if chaos is None:
        throttles = rng.randint(_BASELINE_THROTTLES, 3)
    else:
        mult = float(chaos.get("throttle_multiplier", 1.0))
        throttles = int(rng.randint(5, 30) * mult) if mult > 1.0 else 0
        if mult > 1.0:
            client_errors += int(rng.randint(2, 10) * mult)
            server_errors += int(rng.randint(1, 5) * mult)

    input_tokens = invocations * rng.randint(40, 350)
    output_tokens = 0 if (model_id and _is_image_model(model_id)) else invocations * rng.randint(8, 180)
    output_images = 0
    if model_id and _is_image_model(model_id):
        output_images = invocations * rng.randint(1, 4)
    elif model_id is None:
        # Account-level rollup: a small share of calls may be image models.
        output_images = int(invocations * rng.uniform(0.0, 0.08))

    doc["aws"] = {
        "cloudwatch": {
            "namespace": "AWS/Bedrock",
        },
    }

    runtime: dict[str, Any] = {
        "invocations": invocations,
        "invocation_latency": latency_ms,
        "invocation_client_errors": client_errors,
        "invocation_server_errors": server_errors,
        "invocation_throttles": throttles,
        "input_token_count": input_tokens,
        "output_token_count": output_tokens,
        "output_image_count": output_images,
    }
    if model_id:
        runtime["model_id"] = model_id
        # Overview "Generated images" panels filter:
        #   model_id:* AND NOT quality:* AND NOT image_size:*
        # Real CloudWatch emits ImageSize/Quality as separate dimension series.
        # Do NOT attach them on the model_id series or those panels go empty.

    doc["aws_bedrock"] = {"runtime": runtime}

    # TSDB required dimensions — preserve agent.type/version from scaffold.
    doc["agent"]["id"] = f"bedrock-runtime-{profile['aws_account_id']}"

    # Period: 300,000 ms (5 minutes), matching the real CloudWatch metric period.
    doc["metricset"] = {"period": 300000}

    return doc


def _build_guardrail_doc(
    ts: float,
    ns: str,
    profile: dict,
    rng: random.Random,
    intervened_count: int,
    total_count: int,
    chaos: dict | None,
    *,
    content_source: str,
    policy_type: str | None = None,
) -> dict:
    """Build one ``metrics-aws_bedrock.guardrails`` document.

    Package dashboards split metrics two ways:
    - overall Input/Output: ``not guardrail_policy_type : *``
    - by policy type: ``guardrail_policy_type : *``

    Omit ``guardrail_policy_type`` for the overall series (do not send null).
    Content source must be title-case ``Input`` / ``Output`` to match KQL.
    """
    doc = build_base_doc(
        timestamp=ts,
        dataset="aws_bedrock.guardrails",
        namespace=ns,
        doc_type="metrics",
        agent_type="aws-cloudwatch",
        cloud=_account_cloud(profile),
    )

    guardrail_arn = profile.get(
        "bedrock_guardrail_arn",
        f"arn:aws:bedrock:us-east-1:{profile['aws_account_id']}:guardrail/default",
    )
    guardrail_version = profile.get("bedrock_guardrail_version", "1")

    # Text unit counts: each invocation may check multiple text units.
    text_units = max(1, total_count * rng.randint(1, 3))
    latency_ms = int(rng.uniform(40, 250))
    if chaos:
        latency_ms = int(latency_ms * float(chaos.get("latency_multiplier", 1.0)))

    # emptyAsNull panels need non-zero throttle sums on the healthy path.
    _BASELINE_THROTTLES = 1
    if chaos is None:
        gr_throttles = rng.randint(_BASELINE_THROTTLES, 2)
    else:
        mult = float(chaos.get("throttle_multiplier", 1.0))
        gr_throttles = int(rng.randint(3, 12) * mult) if mult > 1.0 else 0

    guardrails: dict[str, Any] = {
        "invocations": max(0, total_count),
        "invocations_intervened": max(0, intervened_count),
        "invocation_latency": latency_ms,
        "invocation_client_errors": rng.randint(0, 2),
        "invocation_server_errors": rng.randint(0, 1),
        "invocation_throttles": gr_throttles,
        "text_unit_count": text_units,
        "guardrail_content_source": content_source,
        "operation": "ApplyGuardrail",
        "guardrail_arn": guardrail_arn,
        "guardrail_version": guardrail_version,
    }
    if policy_type is not None:
        guardrails["guardrail_policy_type"] = policy_type

    doc["aws_bedrock"] = {"guardrails": guardrails}
    doc["agent"]["id"] = f"bedrock-guardrail-{profile['aws_account_id']}"
    doc["metricset"] = {"period": 300000}

    return doc


def _build_guardrail_docs(
    ts: float,
    ns: str,
    profile: dict,
    rng: random.Random,
    intervened_count: int,
    total_count: int,
    chaos: dict | None,
) -> list[dict]:
    """Emit overall + by-policy guardrail series for both content sources."""
    docs: list[dict] = []
    n_policies = len(_GUARDRAIL_POLICY_TYPES)

    for content_source in _CONTENT_SOURCES:
        # Overall Input/Output KPIs (no policy_type dimension).
        docs.append(
            _build_guardrail_doc(
                ts,
                ns,
                profile,
                rng,
                intervened_count,
                total_count,
                chaos,
                content_source=content_source,
                policy_type=None,
            )
        )
        # By-policy panels require policy_type present.
        for policy_type in _GUARDRAIL_POLICY_TYPES:
            share_total = max(1, total_count // n_policies + rng.randint(0, 2))
            share_intervened = max(
                0, intervened_count // n_policies + (1 if rng.random() < 0.4 else 0)
            )
            docs.append(
                _build_guardrail_doc(
                    ts,
                    ns,
                    profile,
                    rng,
                    share_intervened,
                    share_total,
                    chaos,
                    content_source=content_source,
                    policy_type=policy_type,
                )
            )
    return docs


# ── Live run loop ────────────────────────────────────────────────────────────


from generators.aws_bedrock import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop

def run(
    client: ESBulkClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run the AWS Bedrock generator loop until ``stop_event`` is set."""
    if scenario_data and "scenario" in scenario_data:
        scenario = scenario_data["scenario"]
        ns = scenario_data["namespace"]
    else:
        from app.config import ACTIVE_SCENARIO
        from scenario_engine import get_scenario
        scenario = get_scenario(ACTIVE_SCENARIO)
        ns = scenario.namespace

    profile = scenario.llm_profile
    cfg = scenario.get_generator_config("bedrock") if hasattr(scenario, "get_generator_config") else {}
    _channel_registry = scenario.channel_registry if hasattr(scenario, "channel_registry") else {}

    if not client.configured:
        logger.warning(
            "ESBulkClient not configured; Bedrock generator will not emit."
        )
        return

    rng = random.Random()
    logger.info("AWS Bedrock generator started (namespace=%s)", ns)

    interval = None
    def _emit_tick() -> None:
        nonlocal interval
        chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="aws_bedrock",
            chaos_mode="explicit",
            impact_keys=_ALERT_SPEC.impact_keys,
        )

        load = business_load()
        now = time.time()

        invocations = _invocation_count(rng, load, chaos)
        invocations_per_model = max(1, invocations // len(profile["bedrock_models"]))

        # Invocation logs — guarantee ~12% errors (min 1) so error_code is visible.
        err_quota = _error_quota(invocations, chaos)
        err_slots = set(rng.sample(range(invocations), min(err_quota, invocations)))
        inv_docs = [
            _build_invocation_doc(
                now + rng.uniform(-10, 0),
                ns,
                profile,
                rng,
                chaos,
                force_error=(i in err_slots),
            )
            for i in range(invocations)
        ]
        send_dual_bulk(client, "logs-aws_bedrock.invocation", ns, inv_docs)

        # Runtime metrics: one doc per model, plus one account-level total.
        rt_docs = []
        for model in profile["bedrock_models"]:
            rt_docs.append(
                _build_runtime_doc(now, ns, profile, model, rng, invocations_per_model, chaos)
            )
        rt_docs.append(
            _build_runtime_doc(now, ns, profile, None, rng, invocations, chaos)
        )
        send_dual_bulk(client, "metrics-aws_bedrock.runtime", ns, rt_docs)

        # Guardrail metrics: only when guardrails are configured.
        if profile.get("bedrock_guardrail_id"):
            intervened = _guardrail_intervention_rate(chaos)
            intervened_count = int(invocations * max(0.05, intervened))
            gd_docs = _build_guardrail_docs(
                now, ns, profile, rng, intervened_count, invocations, chaos
            )
            send_dual_bulk(client, "metrics-aws_bedrock.guardrails", ns, gd_docs)

        logger.debug("Bedrock: sent %d invocations (load=%.2f, chaos=%s)", invocations, load, bool(chaos))
        interval = rng.uniform(_BATCH_INTERVAL_MIN, _BATCH_INTERVAL_MAX)
    run_tick_loop(stop_event, _emit_tick, interval=lambda: interval)

    logger.info("AWS Bedrock generator stopped.")


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
