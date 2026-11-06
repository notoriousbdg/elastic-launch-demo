"""Google Vertex AI telemetry generator.

Writes three data streams that the ``gcp_vertexai`` Elastic integration
package expects:

- ``metrics-gcp_vertexai.metrics-<ns>`` — Vertex AI monitoring metrics,
  including histogram-typed latency fields.
- ``logs-gcp_vertexai.auditlogs-<ns>`` — Cloud Audit Logs from Vertex AI.
- ``logs-gcp_vertexai.prompt_response_logs-<ns>`` — prompt and response text
  from Vertex AI Model Monitoring (BigQuery export).

Package dashboards target the ``default`` namespace. When the scenario
namespace is not ``default``, each batch is dual-written there so Overview
panels keep working while scenario-scoped streams remain available for GT.

Documents are sent with ``pipeline=_none`` to bypass the package
``default_pipeline``.  Fleet's ``final_pipeline`` still runs and adds
``event.ingested``.

All field names are fixed by the ``gcp_vertexai`` package ``fields/*.yml``
files.  Scenario-specific values (project, region, model names, caller emails,
prompt text) come from ``scenario.llm_profile``.

The latency fault raises the histogram-typed latency fields.  The audit log
dashboard map panel requires ``source.geo.location`` (geo_point) and
``source.geo.region_name``.  These are set from the scenario callers list
because the GeoIP processor is bypassed.  ``user_agent.*`` is set explicitly
for the same reason (the package ``user_agent`` processor never runs).
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/gcp_vertexai@1.5.1"

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
    build_histogram,
    business_load,
    emit_namespaces,
    exp_histogram_bounds,
    make_histogram_counts,
    resolve_active_chaos,
    send_dual_bulk,
)

logger = logging.getLogger(__name__)

# ── Module-level declarations ────────────────────────────────────────────────

DATA_STREAMS = (
    "metrics-gcp_vertexai.metrics",
    "logs-gcp_vertexai.auditlogs",
    "logs-gcp_vertexai.prompt_response_logs",
)

DOC_FIELDS = (
    "gcp.vertexai.publisher.online_serving.model_invocation_count",
    "gcp.vertexai.publisher.online_serving.token_count",
    "gcp.vertexai.publisher.online_serving.consumed_throughput",
    "gcp.vertexai.publisher.online_serving.model_invocation_latencies",
    "gcp.vertexai.publisher.online_serving.first_token_latencies",
    "gcp.vertexai.prediction.online.prediction_count",
    "gcp.vertexai.prediction.online.prediction_latencies",
    "gcp.vertexai.prediction.online.response_count",
    "gcp.vertexai.prediction.online.error_count",
    "gcp.vertexai.prediction.online.cpu.utilization",
    "gcp.vertexai.prediction.online.memory.bytes_used",
    "gcp.vertexai.prediction.online.network.received_bytes_count",
    "gcp.vertexai.prediction.online.network.sent_bytes_count",
    "gcp.labels.resource.model_user_id",
    "gcp.labels.resource.endpoint_id",
    "gcp.labels.metrics.response_code",
    "gcp.vertexai.audit.service_name",
    "gcp.vertexai.audit.resource_name",
    "gcp.vertexai.audit.type",
    "user_agent.name",
    "source.geo.region_name",
    "client.user.email",
)

# Exponential histogram bounds matching the real Vertex AI distribution.
_HIST_BOUNDS = exp_histogram_bounds(start=1.0, ratio=1.2, buckets=48)

# Batch timing for the live loop.
_BATCH_INTERVAL_MIN = 10.0
_BATCH_INTERVAL_MAX = 20.0


# Global Vertex AI locations. Used whenever the scenario only sets a single
# ``gcp_region`` (or a short list) so Metrics Overview region breakdowns and
# maps look like a multi-region global deployment — not one pin.
_DEFAULT_GCP_REGIONS = (
    "us-central1",           # Iowa
    "us-east1",              # South Carolina
    "us-west1",              # Oregon
    "northamerica-northeast1",  # Montréal
    "europe-west1",          # Belgium
    "europe-west2",          # London
    "europe-west3",          # Frankfurt
    "europe-west4",          # Netherlands
    "asia-southeast1",       # Singapore
    "asia-northeast1",       # Tokyo
    "asia-south1",           # Mumbai
    "australia-southeast1",  # Sydney
    "southamerica-east1",    # São Paulo
    "me-central1",           # Doha
)

# Traffic skew: HQ / primary regions get more volume than edge regions.
_REGION_WEIGHTS = {
    "us-central1": 18,
    "us-east1": 12,
    "us-west1": 8,
    "northamerica-northeast1": 6,
    "europe-west1": 12,
    "europe-west2": 8,
    "europe-west3": 8,
    "europe-west4": 7,
    "asia-southeast1": 9,
    "asia-northeast1": 8,
    "asia-south1": 7,
    "australia-southeast1": 5,
    "southamerica-east1": 5,
    "me-central1": 4,
}

# Global caller personas for audit / prompt map panels when the scenario
# llm_profile only ships a couple of US cities.
_DEFAULT_GLOBAL_CALLERS = (
    {"email": "ops.nyc@{ns}.internal", "country": "US", "city": "New York",
     "region": "New York", "lat": 40.7128, "lon": -74.0060},
    {"email": "eng.chi@{ns}.internal", "country": "US", "city": "Chicago",
     "region": "Illinois", "lat": 41.8781, "lon": -87.6298},
    {"email": "sre.sfo@{ns}.internal", "country": "US", "city": "San Francisco",
     "region": "California", "lat": 37.7749, "lon": -122.4194},
    {"email": "platform.yul@{ns}.internal", "country": "CA", "city": "Montreal",
     "region": "Quebec", "lat": 45.5017, "lon": -73.5673},
    {"email": "risk.lon@{ns}.internal", "country": "GB", "city": "London",
     "region": "England", "lat": 51.5074, "lon": -0.1278},
    {"email": "ml.fra@{ns}.internal", "country": "DE", "city": "Frankfurt",
     "region": "Hesse", "lat": 50.1109, "lon": 8.6821},
    {"email": "data.dub@{ns}.internal", "country": "IE", "city": "Dublin",
     "region": "Leinster", "lat": 53.3498, "lon": -6.2603},
    {"email": "app.sin@{ns}.internal", "country": "SG", "city": "Singapore",
     "region": "Central Singapore", "lat": 1.3521, "lon": 103.8198},
    {"email": "mobile.tyo@{ns}.internal", "country": "JP", "city": "Tokyo",
     "region": "Tokyo", "lat": 35.6762, "lon": 139.6503},
    {"email": "payments.bom@{ns}.internal", "country": "IN", "city": "Mumbai",
     "region": "Maharashtra", "lat": 19.0760, "lon": 72.8777},
    {"email": "support.syd@{ns}.internal", "country": "AU", "city": "Sydney",
     "region": "New South Wales", "lat": -33.8688, "lon": 151.2093},
    {"email": "latam.sao@{ns}.internal", "country": "BR", "city": "Sao Paulo",
     "region": "Sao Paulo", "lat": -23.5505, "lon": -46.6333},
    {"email": "mea.dxb@{ns}.internal", "country": "AE", "city": "Dubai",
     "region": "Dubai", "lat": 25.2048, "lon": 55.2708},
)


def _regions_for_profile(profile: dict) -> list[str]:
    """Return a multi-region list even when llm_profile only sets gcp_region."""
    raw = profile.get("gcp_regions")
    if isinstance(raw, list) and len(raw) >= 4:
        return list(raw)

    primary: str | None = None
    if isinstance(raw, list) and raw:
        primary = str(raw[0])
    elif isinstance(profile.get("gcp_region"), str) and profile["gcp_region"]:
        primary = str(profile["gcp_region"])

    regions = list(_DEFAULT_GCP_REGIONS)
    if primary:
        regions = [primary] + [r for r in regions if r != primary]
    return regions


def _pick_region(profile: dict, rng: random.Random) -> str:
    regions = _regions_for_profile(profile)
    weights = [_REGION_WEIGHTS.get(r, 6) for r in regions]
    return rng.choices(regions, weights=weights, k=1)[0]


def _callers_for_profile(profile: dict, ns: str) -> list[dict]:
    """Expand thin caller lists so map panels show global presence."""
    callers = list(profile.get("callers") or [])
    if len(callers) >= 8:
        return callers

    seen_cities = {(c.get("city") or "").lower() for c in callers}
    merged = list(callers)
    for template in _DEFAULT_GLOBAL_CALLERS:
        city = template["city"]
        if city.lower() in seen_cities:
            continue
        entry = dict(template)
        entry["email"] = template["email"].format(ns=ns)
        merged.append(entry)
        seen_cities.add(city.lower())
    return merged


def _pick_caller(profile: dict, rng: random.Random, ns: str = "demo") -> dict:
    callers = _callers_for_profile(profile, ns)
    if callers:
        return rng.choice(callers)
    return {
        "email": f"user@{ScenarioIdentity(ns).domain()}",
        "country": "US",
        "city": "New York",
        "region": "New York",
        "lat": 40.71,
        "lon": -74.0,
    }

# Parsed user agents (pipeline=_none skips the user_agent processor).
_USER_AGENTS = (
    {
        "original": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36,gzip(gfe),gzip(gfe)"
        ),
        "name": "Chrome",
        "version": "135.0.0.0",
        "os": {"name": "Mac OS X", "full": "Mac OS X 10.15.7", "version": "10.15.7"},
        "device": {"name": "Mac"},
    },
    {
        "original": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36,gzip(gfe)"
        ),
        "name": "Chrome",
        "version": "134.0.0.0",
        "os": {"name": "Windows", "full": "Windows 10", "version": "10"},
        "device": {"name": "Other"},
    },
    {
        "original": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36,gzip(gfe)"
        ),
        "name": "Chrome",
        "version": "133.0.0.0",
        "os": {"name": "Linux", "full": "Linux", "version": ""},
        "device": {"name": "Other"},
    },
    {
        "original": (
            "google-cloud-sdk gcloud/515.0.0 command/ai.models.generate-content "
            "invocation-id/a1b2c3 environment/None environment-version/None "
            "interactive/False from-script/False python/3.12.3 term/xterm-256color"
        ),
        "name": "Other",
        "version": "515.0.0",
        "os": {"name": "Other", "full": "Other", "version": ""},
        "device": {"name": "Other"},
    },
    {
        "original": "python-requests/2.32.3",
        "name": "Other",
        "version": "2.32.3",
        "os": {"name": "Other", "full": "Other", "version": ""},
        "device": {"name": "Other"},
    },
    {
        "original": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
            "(KHTML, like Gecko) Version/18.3 Safari/605.1.15,gzip(gfe)"
        ),
        "name": "Safari",
        "version": "18.3",
        "os": {"name": "Mac OS X", "full": "Mac OS X 10.15.7", "version": "10.15.7"},
        "device": {"name": "Mac"},
    },
)

_AUDIT_ACTIONS = (
    "google.cloud.aiplatform.v1.PredictionService.StreamGenerateContent",
    "google.cloud.aiplatform.v1.PredictionService.GenerateContent",
    "google.cloud.aiplatform.v1.PredictionService.Predict",
    "google.cloud.aiplatform.internal.PredictionService.CountTokens",
)

_RESPONSE_CODES = (
    ("200", 0.92),
    ("429", 0.04),
    ("500", 0.03),
    ("503", 0.01),
)

def _endpoints_for_profile(profile: dict, ns: str) -> list[str]:
    """Vertex endpoint ids, scoped to the scenario whether defaulted or configured."""
    ident = ScenarioIdentity(ns)
    raw = profile.get("vertex_endpoints") or ["endpoint-chat", "endpoint-risk"]
    return [ident.qualify(str(name)) for name in raw]


def _pick_response_code(rng: random.Random, chaos: dict | None = None) -> str:
    error_rate = float((chaos or {}).get("error_rate", 0.0))
    throttle_rate = float((chaos or {}).get("throttle_rate", 0.0))
    if error_rate > 0 or throttle_rate > 0:
        ok_weight = max(0.01, 1.0 - error_rate - throttle_rate)
        weighted = [("200", ok_weight)]
        if throttle_rate > 0:
            weighted.append(("429", throttle_rate))
        if error_rate > 0:
            weighted.extend([
                ("500", error_rate * 0.6),
                ("503", error_rate * 0.3),
                ("500", error_rate * 0.1),
            ])
        roll = rng.random() * sum(w for _, w in weighted)
        cumulative = 0.0
        for code, weight in weighted:
            cumulative += weight
            if roll <= cumulative:
                return code
        return weighted[-1][0]

    roll = rng.random()
    cumulative = 0.0
    for code, weight in _RESPONSE_CODES:
        cumulative += weight
        if roll <= cumulative:
            return code
    return "200"


# ── Document builders ────────────────────────────────────────────────────────

def _build_metrics_doc(
    ts: float,
    ns: str,
    profile: dict,
    rng: random.Random,
    response_count: int,
    chaos: dict | None,
    *,
    model: str | None = None,
    endpoint_id: str | None = None,
    response_code: str | None = None,
) -> dict:
    """Build one ``metrics-gcp_vertexai.metrics`` document.

    Field paths match the ``gcp_vertexai`` package Metrics Overview dashboard:
    publisher online-serving metrics, prediction online metrics, and
    ``gcp.labels.resource.*`` / ``gcp.labels.metrics.*`` dimensions.
    """
    region = _pick_region(profile, rng)
    project = profile.get("gcp_project_id", ScenarioIdentity(ns).gcp_project_id)
    model = model or rng.choice(profile.get("vertex_models", ["gemini-2.5-flash"]))
    endpoint_id = endpoint_id or rng.choice(_endpoints_for_profile(profile, ns))
    response_code = response_code or _pick_response_code(rng, chaos)

    doc = build_base_doc(
        timestamp=ts,
        dataset="gcp_vertexai.metrics",
        namespace=ns,
        doc_type="metrics",
        agent_type="gcp",
        cloud={
            "provider": "gcp",
            "region": region,
            "project_id": project,
        },
    )

    # Token counts: lognormal, average ~1295.
    token_count = int(math.exp(rng.gauss(7.2, 1.0)))
    token_count = max(41, min(token_count, 115580))

    # Consumed throughput (characters throughput): average ~3000.
    throughput = int(math.exp(rng.gauss(8.0, 1.2)))
    throughput = max(2084, min(throughput, 678884))

    # Latency multiplier from the latency fault.
    latency_mult = float((chaos or {}).get("latency_multiplier", 1.0))
    error_rate = float((chaos or {}).get("error_rate", 0.0))

    # Histogram for model invocation latencies.  Peak near bucket 10 (~6.2 s).
    total_inv = max(1, response_count)
    peak = max(0, min(int(10 * latency_mult), len(_HIST_BOUNDS) - 1))
    inv_counts = make_histogram_counts(rng, _HIST_BOUNDS, total_inv, peak_bucket=peak)

    # Histogram for first-token latencies.  Peak near bucket 7 (~3.6 s).
    ftl_peak = max(0, min(int(7 * latency_mult), len(_HIST_BOUNDS) - 1))
    ftl_counts = make_histogram_counts(rng, _HIST_BOUNDS, total_inv, peak_bucket=ftl_peak)

    # Prediction latencies: similar to invocation latencies but slightly faster.
    pred_peak = max(0, min(int(9 * latency_mult), len(_HIST_BOUNDS) - 1))
    pred_counts = make_histogram_counts(rng, _HIST_BOUNDS, total_inv, peak_bucket=pred_peak)

    # Error count: non-200 response codes carry the errors; scale with error_rate.
    if response_code == "200":
        error_count = max(0, int(response_count * error_rate * rng.uniform(0.3, 0.8))) if error_rate > 0 else 0
    else:
        scale = 1.0 + error_rate
        error_count = max(1, int(response_count * rng.uniform(0.5, 1.0) * scale))

    cpu_util = round(max(5.0, min(185.0, rng.gauss(42.0, 18.0) * latency_mult)), 2)
    memory_bytes = int(max(256_000_000, rng.gauss(2_400_000_000, 600_000_000)))
    recv_bytes = int(max(1_000, response_count * rng.randint(2_000, 40_000)))
    sent_bytes = int(max(1_000, response_count * rng.randint(4_000, 80_000)))
    replicas = max(1, int(rng.gauss(3.0, 1.0)))

    doc["gcp"] = {
        "labels": {
            "resource": {
                "model_user_id": model,
                "model_version_id": "",
                "publisher": "google",
                "location": region,
                "endpoint_id": endpoint_id,
                "resource_container": project,
            },
            "metrics": {
                "request_type": rng.choice(["shared", "dedicated"]),
                "type": rng.choice(["input", "output"]),
                "method": rng.choice(
                    ["Predict", "RawPredict", "StreamRawPredict", "ChatCompletions"]
                ),
                "response_code": response_code,
                "latency_type": rng.choice(["model", "overhead"]),
            },
        },
        "vertexai": {
            "publisher": {
                "online_serving": {
                    "model_invocation_count": response_count,
                    "token_count": token_count,
                    "character_count": throughput,
                    "consumed_throughput": throughput,
                    "model_invocation_latencies": build_histogram(_HIST_BOUNDS, inv_counts),
                    "first_token_latencies": build_histogram(_HIST_BOUNDS, ftl_counts),
                }
            },
            "prediction": {
                "online": {
                    "prediction_count": response_count,
                    "response_count": response_count,
                    "error_count": error_count,
                    "replicas": replicas,
                    "target_replicas": replicas,
                    "prediction_latencies": build_histogram(_HIST_BOUNDS, pred_counts),
                    "cpu": {"utilization": cpu_util},
                    "memory": {"bytes_used": memory_bytes},
                    "network": {
                        "received_bytes_count": recv_bytes,
                        "sent_bytes_count": sent_bytes,
                    },
                }
            },
        },
    }

    doc["service"] = {"type": "gcp"}
    doc["metricset"] = {"period": 60000, "name": "metrics"}

    return doc


def _build_audit_doc(
    ts: float,
    ns: str,
    profile: dict,
    rng: random.Random,
    chaos: dict | None = None,
) -> dict:
    """Build one ``logs-gcp_vertexai.auditlogs`` document."""
    project = profile.get("gcp_project_id", ScenarioIdentity(ns).gcp_project_id)
    region = _pick_region(profile, rng)
    model = rng.choice(profile.get("vertex_models", ["gemini-2.5-flash"]))
    caller = _pick_caller(profile, rng, ns)
    action = rng.choice(_AUDIT_ACTIONS)
    resource_name = (
        f"projects/{project}/locations/{region}/publishers/google/models/{model}"
    )
    user_agent = dict(rng.choice(_USER_AGENTS))
    error_rate = float((chaos or {}).get("error_rate", 0.0))
    throttle_rate = float((chaos or {}).get("throttle_rate", 0.0))
    is_error = rng.random() < max(error_rate, throttle_rate * 0.5)

    doc = build_base_doc(
        timestamp=ts,
        dataset="gcp_vertexai.auditlogs",
        namespace=ns,
        doc_type="logs",
        agent_type="gcp-pubsub",
        cloud={
            "provider": "gcp",
            "region": region,
            "project_id": project,
        },
    )

    email = caller.get("email", f"user@{ScenarioIdentity(ns).domain()}")
    doc["gcp"] = {
        "vertexai": {
            "audit": {
                "type": "type.googleapis.com/google.cloud.audit.AuditLog",
                "service_name": "aiplatform.googleapis.com",
                "resource_name": resource_name,
                "resource_type": "audited_resource",
                "authentication_info": {
                    "principal_email": email,
                },
                "authorization_info": [
                    {
                        "granted": True,
                        "permission": "aiplatform.endpoints.predict",
                        "permission_type": "DATA_READ",
                        "resource": resource_name,
                    }
                ],
                "request": {
                    "@type": "type.googleapis.com/google.cloud.aiplatform.v1.GenerateContentRequest",
                    "endpoint": resource_name,
                },
                "response": {
                    "@type": "type.googleapis.com/google.cloud.aiplatform.v1.GenerateContentResponse",
                },
                "status": {
                    "code": 13 if is_error else 0,
                    "message": "Internal error" if is_error else "",
                },
            }
        }
    }

    # Prefer administrative region (state/province); fall back to city for
    # older scenario callers that only set city.
    region_name = caller.get("region") or caller.get("city", "Unknown")
    doc["source"] = {
        "ip": f"{rng.randint(1, 223)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}",
        "geo": {
            "country_iso_code": caller.get("country", "US"),
            "city_name": caller.get("city", "Unknown"),
            "region_name": region_name,
            "location": {
                "lat": caller.get("lat", 40.71),
                "lon": caller.get("lon", -74.0),
            },
        },
    }

    doc["client"] = {"user": {"email": email}}
    doc["user_agent"] = user_agent
    doc["log"] = {
        "level": "INFO",
        "logger": f"projects/{project}/logs/cloudaudit.googleapis.com%2Fdata_access",
    }
    doc["event"] = {
        "dataset": "gcp_vertexai.auditlogs",
        "action": action,
        "outcome": "failure" if is_error else "success",
        "kind": "event",
        "id": uuid.uuid4().hex[:12],
        "type": ["error" if is_error else "info"],
    }

    return doc


def _build_prompt_response_doc(
    ts: float,
    ns: str,
    profile: dict,
    rng: random.Random,
) -> dict:
    """Build one ``logs-gcp_vertexai.prompt_response_logs`` document."""
    region = _pick_region(profile, rng)
    doc = build_base_doc(
        timestamp=ts,
        dataset="gcp_vertexai.prompt_response_logs",
        namespace=ns,
        doc_type="logs",
        agent_type="gcp",
        cloud={
            "provider": "gcp",
            "region": region,
            "project_id": profile.get("gcp_project_id", ScenarioIdentity(ns).gcp_project_id),
        },
    )

    model = rng.choice(profile.get("vertex_models", ["gemini-2.5-flash"]))
    prompt_text = rng.choice(profile.get("prompt_templates", ["Hello."]))
    response_text = rng.choice(profile.get("response_templates", ["Hello."]))

    prompt_tokens = int(math.exp(rng.gauss(4.8, 1.0)))
    prompt_tokens = max(1, min(prompt_tokens, 8000))
    completion_tokens = int(math.exp(rng.gauss(2.7, 1.2)))
    completion_tokens = max(1, min(completion_tokens, 4096))

    doc["gcp"] = {
        "vertexai": {
            "prompt_response_logs": {
                "model_name": model,
                "request": {
                    "contents": [{"role": "user", "parts": [{"text": prompt_text}]}]
                },
                "response": {
                    "candidates": [
                        {
                            "content": {"role": "model", "parts": [{"text": response_text}]},
                            "finish_reason": "STOP",
                        }
                    ],
                    "usage_metadata": {
                        "prompt_token_count": prompt_tokens,
                        "candidates_token_count": completion_tokens,
                        "total_token_count": prompt_tokens + completion_tokens,
                    },
                },
            }
        }
    }

    return doc


def _metrics_docs_for_batch(
    ts: float,
    ns: str,
    profile: dict,
    rng: random.Random,
    response_count: int,
    chaos: dict | None,
) -> list[dict]:
    """Emit one metrics doc per model so 'by model' panels have series."""
    models = profile.get("vertex_models", ["gemini-2.5-flash"])
    endpoints = _endpoints_for_profile(profile, ns)
    docs: list[dict] = []
    remaining = response_count
    for i, model in enumerate(models):
        if i == len(models) - 1:
            share = remaining
        else:
            share = max(1, response_count // len(models))
            remaining -= share
        docs.append(
            _build_metrics_doc(
                ts,
                ns,
                profile,
                rng,
                max(1, share),
                chaos,
                model=model,
                endpoint_id=endpoints[i % len(endpoints)],
                response_code=_pick_response_code(rng, chaos),
            )
        )
    return docs


# ── Live run loop ────────────────────────────────────────────────────────────


from generators.gcp_vertexai import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop

def run(
    client: ESBulkClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run the Vertex AI generator loop until ``stop_event`` is set."""
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
        logger.warning("ESBulkClient not configured; Vertex AI generator will not emit.")
        return

    # If early-start wrote metrics before the package template existed, heal
    # the object/histogram conflict so Lens percentile panels work.
    for emit_ns in emit_namespaces(ns):
        _maybe_heal_metrics_stream(client, emit_ns)

    rng = random.Random()
    logger.info(
        "Vertex AI generator started (namespace=%s, emit=%s)",
        ns,
        ",".join(emit_namespaces(ns)),
    )

    # Re-check mapping conflict periodically — early-start pollution can land
    # after the first heal if the package template arrives mid-run.
    _heal_every = 50
    _cycle = 0

    interval = None
    def _emit_tick() -> None:
        nonlocal _cycle, emit_ns, interval
        _cycle += 1
        if _cycle % _heal_every == 0:
            for emit_ns in emit_namespaces(ns):
                _maybe_heal_metrics_stream(client, emit_ns)

        chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="gcp_vertexai",
            chaos_mode="explicit",
            impact_keys=_ALERT_SPEC.impact_keys,
        )

        load = business_load()
        now = time.time()
        response_count = max(1, int(load * rng.randint(10, 40)))

        # Metrics: one doc per model so dashboard series are populated.
        met_docs = _metrics_docs_for_batch(now, ns, profile, rng, response_count, chaos)
        send_dual_bulk(client, "metrics-gcp_vertexai.metrics", ns, met_docs)

        # Audit logs: one per call.
        audit_docs = [
            _build_audit_doc(now + rng.uniform(-5, 0), ns, profile, rng, chaos)
            for _ in range(response_count)
        ]
        send_dual_bulk(client, "logs-gcp_vertexai.auditlogs", ns, audit_docs)

        # Prompt/response logs.
        pr_docs = [
            _build_prompt_response_doc(now + rng.uniform(-5, 0), ns, profile, rng)
            for _ in range(response_count)
        ]
        send_dual_bulk(client, "logs-gcp_vertexai.prompt_response_logs", ns, pr_docs)

        logger.debug(
            "Vertex AI: sent %d responses / %d metric docs (load=%.2f, chaos=%s)",
            response_count,
            len(met_docs),
            load,
            bool(chaos),
        )
        interval = rng.uniform(_BATCH_INTERVAL_MIN, _BATCH_INTERVAL_MAX)
    run_tick_loop(stop_event, _emit_tick, interval=lambda: interval)

    logger.info("Vertex AI generator stopped.")


# ── Deployer backfill hook ────────────────────────────────────────────────────

def _package_histogram_ready(elastic_url: str, api_key: str) -> bool:
    """True when the gcp_vertexai package maps prediction_latencies as histogram."""
    import httpx
    from elastic_config.deployer_base import _es_headers

    try:
        with httpx.Client(timeout=15) as http:
            resp = http.get(
                f"{elastic_url}/_component_template/metrics-gcp_vertexai.metrics@package",
                headers=_es_headers(api_key),
            )
        if resp.status_code >= 300:
            return False

        def _walk(node: Any) -> bool:
            if isinstance(node, dict):
                for key, value in node.items():
                    if key == "prediction_latencies" and isinstance(value, dict):
                        if value.get("type") == "histogram":
                            return True
                    if _walk(value):
                        return True
            elif isinstance(node, list):
                return any(_walk(item) for item in node)
            return False

        return _walk(resp.json())
    except Exception:
        return False


def _metrics_stream_needs_heal(
    elastic_url: str, api_key: str, met_ds: str
) -> bool:
    """True when any latency field is mapped as object (should be histogram).

    Triggers healing whether the conflict is single-type (all docs landed before
    the package template arrived → only 'object') or mixed-type (some docs landed
    after the template → both 'object' and 'histogram').  Either state causes
    Lens to report "Field ... is of the wrong type".
    """
    import httpx
    from elastic_config.deployer_base import _es_headers

    latency_fields = ",".join([
        "gcp.vertexai.prediction.online.prediction_latencies",
        "gcp.vertexai.publisher.online_serving.model_invocation_latencies",
        "gcp.vertexai.publisher.online_serving.first_token_latencies",
    ])
    try:
        with httpx.Client(timeout=15) as http:
            resp = http.get(
                f"{elastic_url}/{met_ds}/_field_caps",
                headers=_es_headers(api_key),
                params={"fields": latency_fields},
            )
        if resp.status_code >= 300:
            return False
        fields = (resp.json().get("fields") or {})
        for _field, type_map in fields.items():
            if "object" in type_map:
                return True
        return False
    except Exception:
        return False


def _prepare_metrics_stream(elastic_url: str, api_key: str, met_ds: str) -> None:
    """Reset the metrics data stream so histogram fields map correctly.

    ``gcp.vertexai.prediction.online.prediction_latencies`` (and the publisher
    latency fields) are ``histogram`` in the package template.  If documents are
    indexed before that template is applied, Elasticsearch dynamically maps the
    ``{values, counts}`` payload as an object.  Lens then reports
    "Field ... is of the wrong type" when the data stream spans both mappings.

    Deleting the stream forces a fresh backing index that picks up the package
    histogram mapping.  Also widens TSDB ``look_back_time`` for the backfill
    window (same pattern as the Bedrock generator).
    """
    import httpx
    from elastic_config.deployer_base import _es_headers

    headers = _es_headers(api_key)
    with httpx.Client(timeout=30) as http:
        try:
            http.put(
                f"{elastic_url}/_component_template/metrics-gcp_vertexai.metrics@custom",
                headers=headers,
                json={"template": {"settings": {"index": {"look_back_time": "7d"}}}},
            )
            logger.info("Set look_back_time=7d on metrics-gcp_vertexai.metrics@custom")
        except Exception as exc:
            logger.warning(
                "Could not set look_back_time on metrics-gcp_vertexai.metrics@custom: %s",
                exc,
            )

        try:
            resp = http.delete(f"{elastic_url}/_data_stream/{met_ds}", headers=headers)
            if resp.status_code < 300:
                logger.info("Deleted data stream %s (histogram mapping reset)", met_ds)
            elif resp.status_code == 404:
                logger.info("Data stream %s did not exist yet", met_ds)
            else:
                logger.warning(
                    "Could not delete data stream %s: %s %s",
                    met_ds,
                    resp.status_code,
                    resp.text[:200],
                )
        except Exception as exc:
            logger.warning("Could not delete data stream %s: %s", met_ds, exc)


def _maybe_heal_metrics_stream(client: ESBulkClient, ns: str) -> None:
    """Drop a polluted metrics stream once the package histogram mapping exists.

    Live generators start during deploy (``on_otlp_ready``) *before* the
    ``gcp_vertexai`` package is installed, which dynamically maps the
    ``{values, counts}`` histogram payload as an object.  After the package
    lands, any latency field still carrying an object mapping triggers a stream
    reset so Lens percentile panels work.  Catches both the single-type case
    (all docs as object) and the mixed-type case (some docs after the template).
    """
    elastic_url = getattr(client, "elastic_url", None) or ""
    api_key = getattr(client, "api_key", None) or ""
    if not elastic_url or not api_key:
        elastic_url = os.environ.get("ELASTIC_URL") or os.environ.get("ELASTICSEARCH_URL") or ""
        api_key = os.environ.get("ELASTIC_API_KEY") or os.environ.get("ELASTICSEARCH_API_KEY") or ""
    if not elastic_url or not api_key:
        return

    met_ds = f"metrics-gcp_vertexai.metrics-{ns}"
    try:
        if not _package_histogram_ready(elastic_url, api_key):
            return
        if not _metrics_stream_needs_heal(elastic_url, api_key, met_ds):
            return
        logger.warning(
            "Healing %s: latency field(s) mapped as object, expected histogram",
            met_ds,
        )
        _prepare_metrics_stream(elastic_url, api_key, met_ds)
    except Exception as exc:
        logger.warning("Metrics stream heal skipped: %s", exc)


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
