#!/usr/bin/env python3
"""Vercel Generator — sends Vercel drain payloads via OTLP.

Emits data matching the vercel_otel content package (discovery.datasets from
manifest.yml). Four datasets populate the four package dashboards:

  - vercel.logs.otel              → [Vercel OTel] Logs
  - vercel.analytics.v2.otel      → [Vercel OTel] Web Analytics
  - vercel.auditlog.v1.otel       → [Vercel OTel] Audit Logs
  - vercel.speedinsights.v1.otel  → [Vercel OTel] Speed Insights

Field names are grounded from Elastic's hosted Vercel drain encoder
(``vercelencodingextension``) and the package dashboard ES|QL panels.
Elastic's managed ``/vercel`` endpoint maps drain JSON into these attribute
names; this generator emits the same attributes over OTLP so the dashboards
light up without a real drain.

Usage (standalone):
    python3 -m generators.vercel_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity
from generator_engine.scrape_clock import scrape_clock, scrape_now_ns

SOURCE_INTEGRATION = "elastic/integrations packages/vercel_otel@0.4.1"

import logging
import math
import os
import random
import signal
import threading
import time
import uuid

import httpx

from app.telemetry import OTLPClient, _format_attributes as _format_attributes_flat, SEVERITY_MAP
from app.config import NAMESPACE
from elastic_config.deployer_base import _es_headers
from generator_engine.db_common import (
    build_gauge,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)


def _format_attributes(attrs: dict) -> list[dict]:
    """Like app.telemetry._format_attributes, but nested dicts become kvlistValue.

    Vercel dashboards flatten ``vercel.proxy`` as a map attribute; stringifying
    the dict breaks Discover / ES|QL map access.
    """
    formatted: list[dict] = []
    for key, value in attrs.items():
        if value is None:
            continue
        if isinstance(value, dict):
            formatted.append({
                "key": key,
                "value": {"kvlistValue": {"values": _format_attributes(value)}},
            })
        elif isinstance(value, list):
            formatted.append({
                "key": key,
                "value": {
                    "arrayValue": {
                        "values": [
                            {"kvlistValue": {"values": _format_attributes(v)}}
                            if isinstance(v, dict)
                            else _format_attributes_flat({"_": v})[0]["value"]
                            for v in value
                        ]
                    }
                },
            })
        else:
            formatted.extend(_format_attributes_flat({key: value}))
    return formatted

VERCEL_SCOPE = "io.vercel.log-drain"
SCOPE_VERSION = "1.0.0"

logger = logging.getLogger("vercel-generator")
# Short cycles so Logs panels (esp. Top Paths total>=100) fill within minutes.
BATCH_INTERVAL_MIN = 5
BATCH_INTERVAL_MAX = 10
# Weekend business_load() is 0.15 — floor so demo dashboards stay populated.
_VOLUME_LOAD_FLOOR = 0.75
# Logs dashboard timeRestore is now-24h/h with BUCKET(..., 20, ?_tstart, ?_tend).
# After a stream reset, "current time only" docs collapse into one bucket and the
# Request Volume / Warning & Error trend charts show "No results found".
_DASHBOARD_LOOKBACK_S = 24 * 3600
_NS = 1_000_000_000

# OTel logs share logs-otel@template (includes logs@custom). The Logs dashboard
# runs FIELD_EXTRACT(vercel.proxy, "waf_action"), which requires the root to be
# mapped as flattened — dynamic mapping stores a JSON string as keyword.
_LOGS_CUSTOM_TEMPLATE = "logs@custom"
_LOGS_OTEL_CUSTOM_TEMPLATE = "logs-otel@custom"
_LOGS_DATA_STREAM = "logs-vercel.logs.otel-default"
_METRICS_DATA_STREAM = "metrics-vercel.speedinsights.v1.otel-default"

# CWV rating buckets (good / needs-improvement / poor). Dashboard "Poor *"
# panels use AVG(CASE(metric > threshold)); without a poor tail those stay 0%.
# The good ranges stop short of the "good" thresholds (LCP 2500 ms, INP 200 ms,
# CLS 0.1, TTFB 800 ms) that the package "<vital> p75 99.5%" SLOs compare with.
_LCP_BUCKETS_MS = ((900.0, 2300.0), (2500.0, 4000.0), (4000.0, 8000.0))
_INP_BUCKETS_MS = ((40.0, 180.0), (200.0, 500.0), (500.0, 1200.0))
_CLS_BUCKETS = ((0.0, 0.08), (0.1, 0.25), (0.25, 0.5))
_TTFB_BUCKETS_MS = ((100.0, 700.0), (800.0, 1800.0), (1800.0, 4000.0))
_VITAL_BUCKETS = {
    "LCP": _LCP_BUCKETS_MS, "INP": _INP_BUCKETS_MS, "CLS": _CLS_BUCKETS, "TTFB": _TTFB_BUCKETS_MS,
}
# A healthy site: ~88% of page loads rate good, 8% needs-improvement, 4% poor,
# so every p75 sits well inside "good" and the Poor % panels still show a tail.
_VITAL_WEIGHTS = (88, 8, 4)  # good, needs-improvement, poor
_HEALTHY_NON_GOOD_SHARE = 0.12
# Under a web-vitals fault most page loads degrade to needs-improvement / poor.
_FAULT_VITAL_WEIGHTS = (10, 30, 60)

# Metric names queried by vercel_otel-speed-insights.json
METRIC_NAMES = (
    "LCP",
    "INP",
    "CLS",
    "TTFB",
)

# Attributes queried across the four dashboards (passthrough → bare ES|QL names)
LOG_ATTRIBUTES = (
    "vercel.log.source",
    "vercel.log.type",
    "vercel.execution.region",
    "vercel.project.name",
    "vercel.project.id",
    "vercel.proxy",
    "http.response.status_code",
    "http.route",
    "log.level",
    "url.path",
)

ANALYTICS_ATTRIBUTES = (
    "vercel.analytics.event.type",
    "vercel.analytics.event.name",
    "vercel.device.type",
    "vercel.project.id",
    "vercel.referrer",
    "device.id",
    "url.path",
    "url.query",
    "http.route",
    "geo.country.iso_code",
    "user_agent.name",
)

AUDIT_ATTRIBUTES = (
    "vercel.actor.type",
    "vercel.project.id",
    "user.id",
    "user.name",
    "user.email",
    "client.address",
)

SPEED_ATTRIBUTES = (
    "vercel.project.id",
    "vercel.device.type",
    "vercel.connection.speed",
    "url.path",
    "http.route",
    "geo.country.iso_code",
    "user_agent.os.name",
)

_DEPLOYMENT_REGIONS = ("iad1", "sfo1", "fra1", "hnd1", "sin1")
_LOG_SOURCES = ("lambda", "edge", "static", "build", "firewall", "external", "redirect")
_LOG_TYPES = (
    "stdout", "stderr", "command", "middleware", "edge-function-invocation",
    "metric", "report", "fatal",
)
_PATHS = (
    "/", "/pricing", "/docs", "/docs/getting-started", "/blog",
    "/api/products", "/api/users", "/api/orders", "/api/payments", "/api/auth/login",
    "/dashboard", "/dashboard/[id]", "/settings",
)
# Fixed ids only — random ids shatter url.path cardinality so the package
# "Top Paths by Error Rate" panel (WHERE total >= 100) never fills.
_DASHBOARD_IDS = ("1", "2", "42")
# Bias traffic onto a handful of routes so totals cross 100 quickly.
_PATH_WEIGHTS = (8, 6, 6, 5, 5, 10, 10, 12, 8, 8, 6, 6, 5)
_COUNTRIES = ("US", "DE", "GB", "JP", "FR", "CA", "AU", "BR", "IN", "NL")
_DEVICE_TYPES = ("desktop", "mobile", "tablet")
_BROWSERS = ("Chrome", "Safari", "Firefox", "Edge", "Samsung Internet")
_OS_NAMES = ("macOS", "Windows", "iOS", "Android", "Linux")
_CONNECTION_SPEEDS = ("4g", "3g", "2g", "slow-2g", "wifi")
_REFERRERS = (
    None,
    "https://www.google.com/",
    "https://www.bing.com/",
    "https://news.ycombinator.com/",
    "https://twitter.com/",
    "https://www.linkedin.com/",
    "https://github.com/",
)
_UTM = (
    None,
    "utm_source=google&utm_medium=cpc&utm_campaign=spring_launch",
    "utm_source=newsletter&utm_medium=email&utm_campaign=weekly",
    "utm_source=twitter&utm_medium=social&utm_campaign=product_hunt",
)
# Vercel activity-log action names (kebab-case) — audit dashboard filters these.
_AUDIT_ACTIONS = (
    "deployment-delete",
    "drain-created",
    "drain-updated",
    "drain-deleted",
    "env-variable-add",
    "env-variable-edit",
    "env-variable-delete",
    "env-variable-read",
    "user-token-created",
    "user-token-deleted",
    "integration-installation-completed",
    "integration-installation-removed",
    "project-created",
    "project-deleted",
    "domain-buy",
    "domain-delete",
    "team-member-add",
    "team-member-delete",
)
_ACTOR_TYPES = ("user", "app", "system", "integration")
_CUSTOM_EVENTS = (
    "button_click", "signup_start", "signup_complete", "checkout_start",
    "checkout_complete", "search", "share",
)

# ── Faults and healthy-baseline budgets (vercel_otel@0.4.1 alerts) ───────────
# One impact key per package alert. Every alert except the audit one filters
# deployment.environment.name == "production", so that is the default environment.
_FAULT_KEYS = (
    "elevated_error_warning_log_rate_by_source",
    "high_http_5xx_error_rate_by_log_source",
    "lambda_edge_function_crash_rate_elevated",
    "regional_error_rate_skew",
    "request_rate_drop",
    "page_view_rate_drop",
    "firewall_deny_spike",
    "sensitive_audit_activity_spike_by_actor",
    "elevated_ttfb_p75",
    "poor_core_web_vitals_p75_by_route",
)

# Healthy requests fail 1% of the time, mostly with 4xx; only a quarter are 5xx and a
# small share of lambda/edge failures crash. That leaves the 2% 5xx and 1% crash alerts
# real headroom even on a 15-minute window over the smaller `edge` source.
_BASELINE_ERROR_RATE = 0.01
_BASELINE_CRASH_SHARE = 0.05
_BASELINE_ERROR_STATUSES = (400, 401, 403, 404, 404, 404, 500, 503)

# Regions rotate through a fixed 20-slot cycle so each keeps a stable share of traffic.
# sin1 is the smallest (10%): a regional fault there is visible without lifting the
# global lambda/edge 5xx rate over its own 2% alert.
_REGION_CYCLE = (
    ("iad1",) * 6 + ("sfo1",) * 5 + ("fra1",) * 4 + ("hnd1",) * 3 + ("sin1",) * 2
)
_SKEW_REGION = "sin1"

# Healthy firewall activity: a deny is rare, so the "more than 100 denies in 10
# minutes" alert needs a fault to fire. Healthy records are INFO.
_WAF_ACTIONS = ("deny", "challenge", "log", "rate_limit")
_WAF_WEIGHTS = (5, 25, 60, 10)

# Healthy audit activity must keep sensitive events per actor per hour far below the
# alert's "more than 5". At the real 5-10 s cycle (~480 cycles/hour) this budget is
# ~6 sensitive events/hour across ~20 actors.
_AUDIT_CYCLE_PROBABILITY = 0.25
_AUDIT_SENSITIVE_PROBABILITY = 0.05
_SENSITIVE_AUDIT_PATTERNS = (
    "env-variable", "drain", "webhook", "token", "team-", "domain", "project-transfer",
)
_FAULT_AUDIT_ACTOR = "riley.garcia"

# Steady-state events are live: only the startup seed is back-dated across 24 h.
_LIVE_JITTER_S = 5


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_serverless",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


def _project_id(ns: str) -> str:
    return f"prj_{ns.replace('-', '')[:16]}vercel01"


def _deployment_id(rng: random.Random) -> str:
    return f"dpl_{rng.randint(10**20, 10**21 - 1):022d}"


def _sample_bucket(
    rng: random.Random,
    buckets: tuple,
    mult: float = 1.0,
    *,
    rating: int | None = None,
    weights: tuple = _VITAL_WEIGHTS,
) -> float:
    """Sample a Web Vital from good / needs-improvement / poor ranges.

    ``rating`` (0 good, 1 needs-improvement, 2 poor) picks the range; None draws
    it from ``weights``."""
    if rating is None:
        lo, hi = rng.choices(buckets, weights=weights, k=1)[0]
    else:
        lo, hi = buckets[rating]
    return round(rng.uniform(lo, hi) * mult, 4 if hi <= 1.0 else 1)


def healthy_ratings(rng: random.Random, k: int) -> list[int]:
    """Ratings for one cycle's ``k`` healthy page loads of one vital.

    About 12% of loads are needs-improvement / poor (2:1), but never more than
    ``k - ceil(0.75 k)``, so the cycle's p75 (nearest rank) is always a good
    load: Speed Insights sampling noise must not read as an SLO breach."""
    expected = k * _HEALTHY_NON_GOOD_SHARE
    non_good = int(expected) + (1 if rng.random() < expected - int(expected) else 0)
    non_good = min(non_good, k - math.ceil(0.75 * k))
    ratings = [0] * (k - non_good) + [2 if rng.random() < 1 / 3 else 1 for _ in range(non_good)]
    rng.shuffle(ratings)
    return ratings


def _proxy_mapping_types(elastic_url: str, api_key: str) -> set[str]:
    """Return ES mapping types reported for vercel.proxy via field_caps."""
    try:
        resp = httpx.get(
            f"{elastic_url.rstrip('/')}/{_LOGS_DATA_STREAM}/_field_caps",
            params={"fields": "vercel.proxy,attributes.vercel.proxy"},
            headers=_es_headers(api_key),
            timeout=30,
        )
    except Exception as exc:
        logger.warning("field_caps for vercel.proxy failed: %s", exc)
        return set()
    if resp.status_code == 404:
        return set()
    if resp.status_code >= 300:
        logger.warning(
            "field_caps for vercel.proxy failed (%s): %s",
            resp.status_code, resp.text[:200],
        )
        return set()
    # field_caps also returns parent paths (e.g. attributes as object) — ignore
    # those so a healthy flattened mapping is not treated as a conflict.
    wanted = {"vercel.proxy", "attributes.vercel.proxy"}
    types: set[str] = set()
    for name, field_info in resp.json().get("fields", {}).items():
        if name in wanted:
            types.update(field_info.keys())
    return types


def _merge_logs_custom_proxy_mapping(elastic_url: str, api_key: str) -> None:
    """Ensure logs@custom / logs-otel@custom map attributes.vercel.proxy flattened."""
    headers = _es_headers(api_key)
    # logs-otel@template composes both; prefer the OTel-specific slot and keep
    # logs@custom in sync so either path applies the leaf mapping.
    for tmpl in (_LOGS_OTEL_CUSTOM_TEMPLATE, _LOGS_CUSTOM_TEMPLATE):
        url = f"{elastic_url.rstrip('/')}/_component_template/{tmpl}"
        existing_template: dict = {}
        try:
            existing = httpx.get(url, headers=headers, timeout=30)
            if existing.status_code == 200:
                existing_template = (
                    existing.json()
                    .get("component_templates", [{}])[0]
                    .get("component_template", {})
                    .get("template", {})
                )
        except Exception as exc:
            logger.warning("Could not read %s: %s", tmpl, exc)

        mappings = existing_template.get("mappings") or {}
        props = dict(mappings.get("properties") or {})
        attrs = dict(props.get("attributes") or {})
        attr_props = dict(attrs.get("properties") or {})
        attr_props["vercel.proxy"] = {"type": "flattened"}
        # Keep passthrough semantics from otel@mappings; only add the leaf property.
        attrs["properties"] = attr_props
        props["attributes"] = attrs
        body: dict = {
            "template": {
                "mappings": {**mappings, "properties": props},
            }
        }
        if existing_template.get("settings"):
            body["template"]["settings"] = existing_template["settings"]

        resp = httpx.put(url, headers=headers, json=body, timeout=30)
        if resp.status_code >= 300:
            raise RuntimeError(
                f"put {tmpl} failed ({resp.status_code}): {resp.text[:300]}"
            )
        logger.info(
            "Ensured attributes.vercel.proxy flattened mapping on %s",
            tmpl,
        )


def _delete_data_stream(elastic_url: str, api_key: str, name: str) -> None:
    resp = httpx.delete(
        f"{elastic_url.rstrip('/')}/_data_stream/{name}",
        headers=_es_headers(api_key),
        timeout=30,
    )
    if resp.status_code < 300:
        logger.info("Deleted data stream %s for remapping", name)
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist", name)
    else:
        logger.warning(
            "Could not delete %s (%s): %s",
            name, resp.status_code, resp.text[:200],
        )


def ensure_dashboard_mappings(
    elastic_url: str,
    api_key: str,
    *,
    reset_streams: bool = True,
) -> None:
    """Force vercel.proxy → flattened and drop stale keyword-mapped indices.

    The managed ``/vercel`` drain writes ``vercel.proxy`` as a flattened object.
    Generic OTLP intake + dynamic mapping stored an earlier JSON string as
    ``keyword``, which breaks ``FIELD_EXTRACT(vercel.proxy, "waf_action")``.
    Speed Insights ``BY deployment.id`` also needs a clean metrics stream after
    older emits that omitted ``resource.attributes.deployment.id``.
    """
    _merge_logs_custom_proxy_mapping(elastic_url, api_key)
    if not reset_streams:
        return

    types = _proxy_mapping_types(elastic_url, api_key)
    # Empty types → stream missing (nothing to migrate). Only reset when an
    # existing mapping is not purely flattened (keyword / conflict).
    if not types or types == {"flattened"}:
        return

    logger.info(
        "vercel.proxy mapped as %s; resetting vercel logs/metrics streams",
        sorted(types),
    )
    _delete_data_stream(elastic_url, api_key, _LOGS_DATA_STREAM)
    # Clear Speed Insights history that still has null resource deployment.id
    # or second-scale LCP values from earlier generator revisions.
    _delete_data_stream(elastic_url, api_key, _METRICS_DATA_STREAM)


def ensure_dashboard_mappings_from_env() -> None:
    """Best-effort mapping fix using ELASTIC_URL / ELASTIC_API_KEY."""
    try:
        from app.telemetry import ESBulkClient
        client = ESBulkClient()
        if not client.configured:
            return
        ensure_dashboard_mappings(client.elastic_url, client.api_key, reset_streams=True)
    except Exception as exc:
        logger.warning("Vercel dashboard mapping ensure skipped: %s", exc)


def _concrete_path(route: str, rng: random.Random) -> str:
    """Expand dynamic route segments so url.path differs from http.route."""
    if "[id]" in route:
        return route.replace("[id]", rng.choice(_DASHBOARD_IDS))
    return route


def _pick_route(rng: random.Random) -> str:
    return rng.choices(_PATHS, weights=_PATH_WEIGHTS, k=1)[0]


def _age_ns(rng: random.Random) -> int:
    """Nanoseconds to subtract so 24h BUCKET charts get many populated buckets.

    ~40% of events stay in the last 2h (live feel); the rest spread across the
    full dashboard lookback window.
    """
    if rng.random() < 0.4:
        return rng.randint(0, 2 * 3600 * _NS)
    return rng.randint(0, _DASHBOARD_LOOKBACK_S * _NS)


def _log_record(
    attrs: dict,
    event_name: str,
    severity: str = "INFO",
    body: str | None = None,
    age_ns: int = 0,
) -> dict:
    severity_upper = severity.upper()
    now = int(time.time() * _NS)
    event_ts = max(0, now - int(age_ns))
    rec: dict = {
        "timeUnixNano": str(event_ts),
        "observedTimeUnixNano": str(now),
        "severityNumber": SEVERITY_MAP.get(severity_upper, 9),
        "severityText": severity_upper,
        "eventName": event_name,
        "attributes": _format_attributes(attrs),
    }
    if body is not None:
        rec["body"] = {"stringValue": body}
    return rec


def _faults(chaos: dict | None) -> dict[str, float]:
    """Resolved impact values; 0 means the fault is off."""
    return {key: float((chaos or {}).get(key) or 0.0) for key in _FAULT_KEYS}


def _live_age_ns(rng: random.Random) -> int:
    return rng.randint(0, _LIVE_JITTER_S * _NS)


def _status_and_level(
    rng: random.Random, source: str, region: str, faults: dict,
) -> tuple[int, str]:
    """HTTP status and log level for one record; each fault drives only its own alert."""
    lambda_edge = source in ("lambda", "edge")
    # A firewall record describes a rule decision, not a failed request.
    if source != "firewall" and rng.random() < _BASELINE_ERROR_RATE:
        if lambda_edge and rng.random() < _BASELINE_CRASH_SHARE:
            return -1, "ERROR"
        status = rng.choice(_BASELINE_ERROR_STATUSES)
        return status, ("ERROR" if status >= 500 else "WARNING")
    if lambda_edge:
        if rng.random() < faults.get("lambda_edge_function_crash_rate_elevated", 0.0):
            return -1, "ERROR"
        p_5xx = faults.get("high_http_5xx_error_rate_by_log_source", 0.0)
        if region == _SKEW_REGION:
            p_5xx = max(p_5xx, faults.get("regional_error_rate_skew", 0.0))
        if rng.random() < p_5xx:
            return rng.choice((500, 502, 503)), "ERROR"
    elif source in ("static", "build"):
        if rng.random() < faults.get("elevated_error_warning_log_rate_by_source", 0.0):
            return 404, "WARNING"
    if source == "firewall":
        return 403, "INFO"
    status = rng.choices((200, 201, 204, 301, 302, 304), weights=(70, 5, 5, 5, 10, 5), k=1)[0]
    return status, "INFO"


def _build_function_log(
    project: str,
    project_id: str,
    host: str,
    route: str,
    region: str,
    rng: random.Random,
    faults: dict | None = None,
    *,
    spread: bool = True,
    source: str | None = None,
    waf_action: str | None = None,
) -> dict:
    if source is None:
        source = rng.choices(
            _LOG_SOURCES,
            weights=(45, 20, 15, 5, 5, 5, 5),
            k=1,
        )[0]
    status, level = _status_and_level(rng, source, region, faults or {})
    is_crash = status == -1
    is_error = level != "INFO"

    log_type = None if source in ("static", "firewall", "redirect") else rng.choice(_LOG_TYPES)
    duration_ms = round(rng.uniform(5.0, 200.0) * (5.0 if is_error else 1.0), 1)
    client_ip = f"203.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"
    url_path = _concrete_path(route, rng)
    browser = rng.choice(_BROWSERS)
    age_ns = _age_ns(rng) if spread else _live_age_ns(rng)
    event_ms = int((time.time() * _NS - age_ns) / 1_000_000)

    # Nested vercel.proxy map (snake_case keys). logs@custom maps this attribute
    # as flattened so FIELD_EXTRACT(vercel.proxy, "waf_action") works. Promoted
    # semconv fields (method, user agent, client IP, scheme) stay outside the map.
    proxy: dict = {
        "timestamp": event_ms,
        "host": host,
        "path": url_path,
        "region": region,
        "status_code": status if status != -1 else 200,
        "vercel_cache": rng.choice(("MISS", "HIT", "STALE", "BYPASS")),
        "path_type": "func" if source == "lambda" else source,
    }
    if source == "firewall":
        proxy["waf_action"] = waf_action or rng.choices(_WAF_ACTIONS, weights=_WAF_WEIGHTS, k=1)[0]
        proxy["waf_rule_id"] = f"rule_{rng.randint(1000, 9999)}"

    if is_crash:
        msg = f"Error in {route}: Function crashed"
    elif is_error:
        msg = f"GET {url_path} {status}"
    else:
        msg = f"GET {url_path} {status} in {duration_ms}ms"

    attrs = {
        "vercel.log.source": source,
        "vercel.execution.region": region,
        "vercel.project.name": project,
        "vercel.project.id": project_id,
        "deployment.id": _deployment_id(rng),
        "http.response.status_code": status,
        "http.request.method": "GET",
        "http.route": route,
        "log.level": level,
        "url.path": url_path,
        "url.scheme": "https",
        "server.address": host,
        "client.address": client_ip,
        "user_agent.original": browser,
        "vercel.proxy": proxy,
    }
    if log_type is not None:
        attrs["vercel.log.type"] = log_type

    return _log_record(
        attrs=attrs,
        event_name="vercel.function.log",
        severity=level,
        body=msg,
        age_ns=age_ns,
    )


def now_ms() -> int:
    return int(time.time() * 1000)


def _build_analytics_event(
    project_id: str,
    host: str,
    region: str,
    rng: random.Random,
    *,
    spread: bool = True,
    drop_pageviews: float = 0.0,
) -> dict | None:
    """One analytics record, or None when a page view is dropped by the fault."""
    event_type = rng.choices(("pageview", "event"), weights=(85, 15), k=1)[0]
    if event_type == "pageview" and rng.random() < drop_pageviews:
        return None
    route = _pick_route(rng)
    path = _concrete_path(route, rng)
    device = rng.choice(_DEVICE_TYPES)
    referrer = rng.choice(_REFERRERS)
    query = rng.choice(_UTM)
    attrs: dict = {
        "vercel.analytics.event.type": event_type,
        "vercel.project.id": project_id,
        "vercel.device.type": device,
        "device.id": rng.randint(10_000, 99_999_999),
        "url.path": path,
        "http.route": route,
        "geo.country.iso_code": rng.choice(_COUNTRIES),
        "user_agent.name": rng.choice(_BROWSERS),
        "user_agent.os.name": rng.choice(_OS_NAMES),
        "vercel.region": region,
        "server.address": host,
    }
    if event_type == "event":
        attrs["vercel.analytics.event.name"] = rng.choice(_CUSTOM_EVENTS)
    if referrer is not None:
        attrs["vercel.referrer"] = referrer
    if query is not None:
        attrs["url.query"] = query

    return _log_record(
        attrs=attrs,
        event_name="vercel.analytics.pageview" if event_type == "pageview" else "vercel.analytics.event",
        severity="INFO",
        age_ns=_age_ns(rng) if spread else _live_age_ns(rng),
    )


_SENSITIVE_AUDIT_ACTIONS = tuple(
    a for a in _AUDIT_ACTIONS if any(p in a for p in _SENSITIVE_AUDIT_PATTERNS)
)
_ROUTINE_AUDIT_ACTIONS = tuple(a for a in _AUDIT_ACTIONS if a not in _SENSITIVE_AUDIT_ACTIONS)


def _pick_audit_action(rng: random.Random) -> str:
    pool = _SENSITIVE_AUDIT_ACTIONS if rng.random() < _AUDIT_SENSITIVE_PROBABILITY else _ROUTINE_AUDIT_ACTIONS
    return rng.choice(pool)


def _build_audit_log(
    project_id: str,
    ns: str,
    rng: random.Random,
    *,
    spread: bool = True,
    action: str | None = None,
    actor: str | None = None,
) -> dict:
    action = action or _pick_audit_action(rng)
    actor_type = rng.choices(_ACTOR_TYPES, weights=(70, 10, 10, 10), k=1)[0]
    user_id = f"user_{rng.randint(1000, 9999)}"
    user_name = actor or f"{rng.choice(('alex', 'sam', 'jordan', 'casey', 'riley'))}.{rng.choice(('lee', 'nguyen', 'patel', 'garcia'))}"
    attrs = {
        "vercel.actor.type": actor_type,
        "vercel.project.id": project_id,
        "user.id": user_id,
        "user.name": user_name,
        "user.email": f"{user_name}@{ScenarioIdentity(ns).domain()}",
        "client.address": f"67.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}",
        "vercel.audit_log.payload": {
            "project_id": project_id,
            "action_detail": action,
        },
    }
    return _log_record(
        attrs=attrs,
        event_name=action,
        severity="INFO",
        body=f"{actor_type} {user_name} performed {action}",
        age_ns=_age_ns(rng) if spread else _live_age_ns(rng),
    )


def _build_vitals(
    project_id: str,
    deployment_id: str,
    rng: random.Random,
    lcp_mult: float,
    ttfb_mult: float,
    ratings: dict[str, int] | None = None,
) -> list[dict]:
    """Web Vitals gauges for one page load.

    ``lcp_mult`` degrades LCP and INP, ``ttfb_mult`` degrades TTFB: above 1 the
    load is drawn from the fault mix (mostly poor) and scaled by the value; at or
    below 1 the healthy distribution is untouched. ``ratings`` pins each healthy
    vital's rating (see :func:`healthy_ratings`); missing ones are drawn at random."""
    lcp_mult = max(1.0, lcp_mult)
    ttfb_mult = max(1.0, ttfb_mult)
    ratings = ratings or {}

    def _vital(name: str, mult: float) -> float:
        if mult > 1.0:
            return _sample_bucket(rng, _VITAL_BUCKETS[name], mult, weights=_FAULT_VITAL_WEIGHTS)
        return _sample_bucket(rng, _VITAL_BUCKETS[name], rating=ratings.get(name))

    route = _pick_route(rng)
    path = _concrete_path(route, rng)
    device = rng.choice(_DEVICE_TYPES)
    # Speed Insights panels group BY deployment.id (resource alias). Keep the
    # same id on the datapoint so attribute-level filters still resolve.
    attrs = {
        "vercel.project.id": project_id,
        "vercel.device.type": device,
        "vercel.connection.speed": rng.choice(_CONNECTION_SPEEDS),
        "url.path": path,
        "http.route": route,
        "geo.country.iso_code": rng.choice(_COUNTRIES),
        "user_agent.os.name": rng.choice(_OS_NAMES),
        "deployment.id": deployment_id,
    }
    # Web Vitals drain values are milliseconds for LCP/INP/TTFB (dashboard
    # poor thresholds: LCP>4000, INP>500, TTFB>1800). CLS stays unitless.
    # Mix good / needs-improvement / poor so "Poor *" percentage panels light up.
    return [
        build_gauge("LCP", "ms", _vital("LCP", lcp_mult), attrs),
        build_gauge("INP", "ms", _vital("INP", lcp_mult), attrs),
        build_gauge("CLS", "1", _vital("CLS", 1.0), attrs),
        build_gauge("TTFB", "ms", _vital("TTFB", ttfb_mult), attrs),
    ]


def emit_cycle(
    client: OTLPClient,
    resources: dict,
    scope: dict,
    rng: random.Random,
    *,
    project_name: str,
    project_id: str,
    host_name: str,
    deployment_id: str,
    ns: str,
    n: int,
    faults: dict,
    cycle_no: int,
    spread: bool,
) -> None:
    """Emit one cycle of logs, analytics, audit records and Web Vitals.

    ``spread`` back-dates records across the 24 h dashboard lookback (the startup seed
    only); steady-state cycles are live so recent-vs-previous volume alerts stay quiet.
    """
    region = rng.choice(_DEPLOYMENT_REGIONS) if spread else _REGION_CYCLE[cycle_no % len(_REGION_CYCLE)]

    n_logs = n - int(round(n * min(1.0, faults.get("request_rate_drop", 0.0))))
    fn_logs = [
        _build_function_log(
            project_name, project_id, host_name, _pick_route(rng), region, rng, faults,
            spread=spread,
        )
        for _ in range(n_logs)
    ]
    fn_logs += [
        _build_function_log(
            project_name, project_id, host_name, _pick_route(rng), region, rng, faults,
            spread=spread, source="firewall", waf_action="deny",
        )
        for _ in range(int(round(faults.get("firewall_deny_spike", 0.0))))
    ]
    if fn_logs:
        client.send_logs_scoped(resources["logs"], scope, fn_logs)

    analytics = [
        _build_analytics_event(
            project_id, host_name, region, rng, spread=spread,
            drop_pageviews=faults.get("page_view_rate_drop", 0.0),
        )
        for _ in range(max(1, n // 2))
    ]
    analytics = [a for a in analytics if a is not None]
    if analytics:
        client.send_logs_scoped(resources["analytics"], scope, analytics)

    if spread:
        audit_logs = [_build_audit_log(project_id, ns, rng, spread=True) for _ in range(max(1, n // 5))]
    else:
        audit_logs = [
            _build_audit_log(project_id, ns, rng, spread=False)
            for _ in range(1 if rng.random() < _AUDIT_CYCLE_PROBABILITY else 0)
        ]
    audit_logs += [
        _build_audit_log(
            project_id, ns, rng, spread=spread,
            action=rng.choice(_SENSITIVE_AUDIT_ACTIONS), actor=_FAULT_AUDIT_ACTOR,
        )
        # Any positive key is a breach request: round up so small values still emit.
        for _ in range(math.ceil(faults.get("sensitive_audit_activity_spike_by_actor", 0.0)))
    ]
    if audit_logs:
        client.send_logs_scoped(resources["audit"], scope, audit_logs)

    # Several page loads per cycle: the p75 alerts group by route over an hour and need
    # enough samples per route that a healthy tail does not read as a breach.
    # Each page load is its own sample, not part of a scrape: its four vitals share
    # one time (one document), and the next load is 1 ns later so two loads with
    # identical attributes never collide into one data point.
    vitals: list[dict] = []
    loads = max(1, n // 4)
    cycle_ratings = {name: healthy_ratings(rng, loads) for name in METRIC_NAMES}
    for load in range(loads):
        sample = _build_vitals(
            project_id, deployment_id, rng,
            lcp_mult=faults.get("poor_core_web_vitals_p75_by_route", 0.0),
            ttfb_mult=faults.get("elevated_ttfb_p75", 0.0),
            ratings={name: r[load] for name, r in cycle_ratings.items()},
        )
        load_ts = str(int(scrape_now_ns()) + load)
        for metric in sample:
            metric["gauge"]["dataPoints"][0]["timeUnixNano"] = load_ts
        vitals += sample
    client.send_metrics_scoped(resources["vitals"], scope, vitals)


from generators.vercel_otel import SPEC as _ALERT_SPEC
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

    ensure_dashboard_mappings_from_env()

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("vercel")
    project_name = cfg.get("project_name") or ScenarioIdentity(ns).label("app")
    project_id = cfg.get("project_id") or _project_id(ns)
    records_per_cycle: int = cfg.get("records_per_cycle", 80)
    cloud = cfg.get("cloud") or _default_cloud()
    host_name = cfg.get("host_name") or f"{project_name}.vercel.app"
    environment = cfg.get("environment") or "production"
    # Stable resource deployment.id — Speed Insights groups BY deployment.id
    # (resource alias). A missing resource value shows up as a null row.
    deployment_id = cfg.get("deployment_id") or f"dpl_{uuid.uuid4().hex[:20]}"


    def _make_resource(signal_type: str, dataset: str) -> dict:
        return build_signal_resource(
            ns=ns, signal_type=signal_type, dataset=otel_dataset(dataset),
            service_name=service_name, service_version="1.0.0",
            service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
            host_name=host_name, cloud=cloud,
            extra_attrs={
                "vercel.project.name": project_name,
                "vercel.project.id": project_id,
                # Package dashboard controls filter on the semconv v1.27+ name.
                "deployment.environment.name": environment,
                "deployment.id": deployment_id,
            },
        )

    res_logs = _make_resource("logs", "vercel.logs")
    res_analytics = _make_resource("logs", "vercel.analytics.v2")
    res_audit = _make_resource("logs", "vercel.auditlog.v1")
    res_vitals = _make_resource("metrics", "vercel.speedinsights.v1")
    scope = {"name": VERCEL_SCOPE, "version": SCOPE_VERSION}
    logger.info("Vercel generator started (namespace=%s, project=%s)", ns, project_name)

    resources = {"logs": res_logs, "analytics": res_analytics, "audit": res_audit, "vitals": res_vitals}

    def _emit(faults: dict, n: int, cycle_no: int, spread: bool) -> None:
        emit_cycle(
            client, resources, scope, rng,
            project_name=project_name, project_id=project_id, host_name=host_name,
            deployment_id=deployment_id, ns=ns, n=n, faults=faults,
            cycle_no=cycle_no, spread=spread,
        )

    # Seed across the 24h dashboard window so Request Volume / Warning trend
    # BUCKET charts are populated immediately after a stream reset.
    seed_n = max(records_per_cycle * 3, 240)
    with scrape_clock():
        _emit({}, seed_n, 0, spread=True)

    cycle_no = 0
    while not stop_event.is_set():
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="vercel_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        load = max(_VOLUME_LOAD_FLOOR, business_load())
        n = max(1, int(records_per_cycle * load))
        cycle_no += 1
        # Own loop rather than run_tick_loop: one scrape time per cycle, so the
        # four Web Vitals of a page load share one document (logs keep wall time).
        with scrape_clock():
            _emit(_faults(active_chaos), n, cycle_no, spread=False)
        stop_event.wait(rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("Vercel generator stopped.")


def main() -> None:  # pragma: no cover
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "60"))

    def _stop(sig, frame):  # noqa: ARG001
        stop_event.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    client = OTLPClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
