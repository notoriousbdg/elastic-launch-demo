#!/usr/bin/env python3
"""Browser RUM generator — OpenTelemetry JS (browser) SDK shaped telemetry.

Feeds the ``[RUM OTel] Page Load & WebVitals`` dashboard, which reads
``traces-*.otel-*`` and ``logs-*.otel-*`` filtered on
``resource.attributes.telemetry.sdk.language == "webjs"`` and a parseable
``resource.attributes.user_agent.original``:

* page loads: ``browser.navigation_timing`` events (scope
  ``@opentelemetry/browser-instrumentation/navigation-timing``) carrying
  ``browser.resource_timing.fetch_start`` and
  ``browser.navigation_timing.load_event_end``; apps listed in
  ``document_load_span_apps`` send ``documentLoad`` spans (scope
  ``@opentelemetry/instrumentation-document-load``) instead, so Total Visits does
  not double count a page view
* web vitals: ``browser.web_vital`` events with ``browser.web_vital.name``
  (lcp / inp / cls / fcp / ttfb) and ``browser.web_vital.value``
* errors: ``exception`` events with ``exception.message`` /
  ``exception.stacktrace`` (scope ``.../browser-instrumentation/errors``, or
  ``instrumentation-web-exception`` for the legacy apps)
* resource timing events, user-interaction ``click`` spans and fetch client spans
* resource attributes ``browser.name`` / ``browser.platform`` (Browser and OS
  breakdown panels) and ``user_agent.original`` for each browser profile

``os.name`` is deliberately not set: the Android / iOS dashboards filter on
``os.name`` over the same ``generic.otel`` streams, and browser data would leak
into their installation and session counts.

Usage (standalone):
    python3 -m generators.otel_rum_dashboards.generator
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/otel_rum_dashboards@0.0.6"

import logging
import os
import random
import signal
import threading
import time
from typing import Any

from app.config import NAMESPACE
from app.telemetry import OTLPClient
from generator_engine import mobile_common as mc
from generator_engine.db_common import impact_value, resolve_active_chaos
from generator_engine.identity import ScenarioIdentity
from generator_engine.tick import run_tick_loop
from generators.otel_rum_dashboards import SPEC

logger = logging.getLogger("otel-rum-generator")

INTERVAL = int(os.getenv("OTEL_RUM_INTERVAL", "10"))

SDK_VERSION = "2.2.0"
BROWSER_INSTR = "0.3.0"
SCOPE_NAV = ("@opentelemetry/browser-instrumentation/navigation-timing", BROWSER_INSTR)
SCOPE_VITALS = ("@opentelemetry/browser-instrumentation/web-vitals", BROWSER_INSTR)
SCOPE_ERRORS = ("@opentelemetry/browser-instrumentation/errors", BROWSER_INSTR)
SCOPE_RESOURCE = ("@opentelemetry/browser-instrumentation/resource-timing", BROWSER_INSTR)
SCOPE_DOCLOAD = ("@opentelemetry/instrumentation-document-load", "0.48.0")
SCOPE_WEB_EXCEPTION = ("@opentelemetry/instrumentation-web-exception", "0.12.0")
SCOPE_USER_INTERACTION = ("@opentelemetry/instrumentation-user-interaction", "0.48.0")
SCOPE_FETCH = ("@opentelemetry/instrumentation-fetch", "0.205.0")

# (browser.name, browser.platform, browser.mobile, user_agent.original, weight)
BROWSERS: list[tuple[str, str, bool, str, float]] = [
    ("Chrome", "Windows", False,
     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
     "Chrome/129.0.0.0 Safari/537.36", 30),
    ("Chrome", "macOS", False,
     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
     "Chrome/129.0.0.0 Safari/537.36", 14),
    ("Safari", "macOS", False,
     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) "
     "Version/18.0 Safari/605.1.15", 10),
    ("Mobile Safari", "iOS", True,
     "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
     "Version/18.0 Mobile/15E148 Safari/604.1", 16),
    ("Chrome Mobile", "Android", True,
     "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) "
     "Chrome/129.0.0.0 Mobile Safari/537.36", 14),
    ("Edge", "Windows", False,
     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
     "Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0", 8),
    ("Firefox", "Linux", False,
     "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0", 8),
]

PAGES = {
    "storefront": ["/", "/products", "/products/{id}", "/search?q=boots", "/cart"],
    "checkout": ["/checkout", "/checkout/shipping", "/checkout/payment", "/checkout/confirmation"],
    "account": ["/account", "/account/orders", "/account/orders/{id}", "/account/settings"],
}
DEFAULT_PAGES = ["/", "/about", "/help", "/contact"]

# (name, good, poor) thresholds from web.dev — browser.web_vital.rating
_RATING = {"lcp": (2500, 4000), "inp": (200, 500), "cls": (0.1, 0.25), "fcp": (1800, 3000), "ttfb": (800, 1800)}

BASELINE_ERROR_RATE = 0.02


def _healthy_errors(cdn: str) -> list[tuple[str, str, str, float]]:
    return [
        ("Error", "ResizeObserver loop completed with undelivered notifications.",
         "Error: ResizeObserver loop completed with undelivered notifications.\n"
         f"    at https://{cdn}/assets/vendor.7d21aa.js:1:23456", 5.0),
        ("ChunkLoadError", f"Loading chunk 812 failed. (error: https://{cdn}/assets/812.b41e0c.js)",
         f"ChunkLoadError: Loading chunk 812 failed. (error: https://{cdn}/assets/812.b41e0c.js)\n"
         f"    at __webpack_require__.f.j (https://{cdn}/assets/runtime.0c3a51.js:1:5121)\n"
         f"    at https://{cdn}/assets/runtime.0c3a51.js:1:1488\n"
         f"    at Array.reduce (<anonymous>)\n"
         f"    at __webpack_require__.e (https://{cdn}/assets/runtime.0c3a51.js:1:1452)", 2.0),
        ("TypeError", "Failed to fetch",
         "TypeError: Failed to fetch\n"
         f"    at fetchRecommendations (https://{cdn}/assets/storefront.9a1c3e.js:1:48213)\n"
         f"    at https://{cdn}/assets/storefront.9a1c3e.js:1:49102\n"
         f"    at commitHookEffectListMount (https://{cdn}/assets/vendor.7d21aa.js:1:88412)", 2.0),
    ]


def _fault_error(cdn: str) -> tuple[str, str, str]:
    return (
        "TypeError", "Cannot read properties of undefined (reading 'items')",
        "TypeError: Cannot read properties of undefined (reading 'items')\n"
        f"    at CartSummary (https://{cdn}/assets/cart-widget.4f9c2a.js:2:18342)\n"
        f"    at renderWithHooks (https://{cdn}/assets/vendor.7d21aa.js:1:66220)\n"
        f"    at updateFunctionComponent (https://{cdn}/assets/vendor.7d21aa.js:1:71388)\n"
        f"    at beginWork (https://{cdn}/assets/vendor.7d21aa.js:1:79102)\n"
        f"    at performUnitOfWork (https://{cdn}/assets/vendor.7d21aa.js:1:101734)",
    )


def _rating(name: str, value: float) -> str:
    good, poor = _RATING[name]
    return "good" if value <= good else ("needs-improvement" if value <= poor else "poor")


def _pages_for(raw_app: str) -> list[str]:
    for key, pages in PAGES.items():
        if key in raw_app:
            return pages
    return DEFAULT_PAGES


class RumState:
    def __init__(self, ident: ScenarioIdentity, cfg: dict, rng: random.Random):
        raw_apps = cfg.get("apps") or SPEC.config_fields["apps"]["default"]
        legacy_raw = cfg.get("document_load_span_apps")
        if legacy_raw is None:
            legacy_raw = SPEC.config_fields["document_load_span_apps"]["default"]
        version = str(cfg.get("app_version") or SPEC.config_fields["app_version"]["default"])
        self.page_views = max(1, int(cfg.get("page_views_per_tick") or 6))
        self.cdn = ident.domain("cdn")
        self.apps: list[dict[str, Any]] = []
        for raw in raw_apps:
            name = ident.qualify(str(raw))
            resources = []
            for b_name, platform, mobile, ua, weight in BROWSERS:
                resources.append((mc.resource({
                    "service.name": name,
                    "service.version": version,
                    "service.namespace": ident.namespace,
                    "deployment.environment.name": "production",
                    "telemetry.sdk.name": "opentelemetry",
                    "telemetry.sdk.language": "webjs",
                    "telemetry.sdk.version": SDK_VERSION,
                    "user_agent.original": ua,
                    "browser.name": b_name,
                    "browser.platform": platform,
                    "browser.mobile": mobile,
                    "browser.language": rng.choice(["en-US", "en-US", "en-GB", "de-DE", "fr-FR"]),
                }), weight))
            self.apps.append({
                "name": name,
                "legacy": str(raw) in {str(x) for x in legacy_raw},
                "host": ident.domain(str(raw)),
                "pages": _pages_for(str(raw)),
                "resources": resources,
                "sessions": [mc.uuid4(rng) for _ in range(30)],
            })


def _resource_list(cdn: str, app_host: str, page: str) -> list[tuple[str, str]]:
    return [
        ("script", f"https://{cdn}/assets/vendor.7d21aa.js"),
        ("script", f"https://{cdn}/assets/app.3b8e04.js"),
        ("link", f"https://{cdn}/assets/app.91f2d7.css"),
        ("img", f"https://{cdn}/img/hero-{sum(map(ord, page)) % 9}.webp"),
        ("fetch", f"https://{app_host}/api/v1/session"),
        ("fetch", f"https://{app_host}/api/v1/recommendations"),
    ]


def simulate_tick(
    state: RumState,
    rng: random.Random,
    now_ns: int,
    *,
    web_vital_degradation: float = 1.0,
    js_error_rate: float = BASELINE_ERROR_RATE,
    resource_load_latency: float = 1.0,
    interval_s: float = INTERVAL,
) -> tuple[mc.SignalBatch, mc.SignalBatch]:
    """Page views for every app in one tick. Returns (logs, spans)."""
    deg = max(1.0, float(web_vital_degradation))
    rlat = max(1.0, float(resource_load_latency))
    logs = mc.SignalBatch()
    spans = mc.SignalBatch()
    window_ns = int(interval_s * 1e9)
    healthy_errors = _healthy_errors(state.cdn)
    fault_error = _fault_error(state.cdn)

    for app in state.apps:
        n_views = max(1, state.page_views + rng.randint(-2, 2))
        for _ in range(n_views):
            res = mc.weighted_choice(rng, [r for r, _ in app["resources"]], [w for _, w in app["resources"]])
            session_id = rng.choice(app["sessions"])
            if rng.random() < 0.03:  # session rotation
                app["sessions"][app["sessions"].index(session_id)] = mc.uuid4(rng)
            page = rng.choice(app["pages"]).replace("{id}", str(rng.randint(1000, 9999)))
            url = f"https://{app['host']}{page}"
            t0 = now_ns - int(rng.uniform(0.05, 1.0) * window_ns)
            base = {"session.id": session_id, "url.full": url}

            # ── navigation timing (ms since navigation start) ────────────────
            fetch_start = rng.uniform(1.0, 6.0)
            ttfb_base = rng.uniform(80, 320)
            dns = rng.uniform(0, 25)
            connect = rng.uniform(5, 40)
            response_start = fetch_start + ttfb_base * deg
            response_end = response_start + rng.uniform(20, 120)
            resources_ms = rng.uniform(350, 1100) * rlat
            dom_interactive = response_end + rng.uniform(150, 450)
            dcl_start = dom_interactive + rng.uniform(5, 40)
            dcl_end = dcl_start + rng.uniform(5, 60)
            dom_complete = max(dcl_end, response_end + resources_ms) + rng.uniform(20, 120)
            load_start = dom_complete + rng.uniform(0, 5)
            load_end = load_start + rng.uniform(1, 25)
            r1 = lambda v: float(round(v, 1))  # noqa: E731

            if app["legacy"]:
                trace_id, root = mc.hex_id(rng, 16), mc.hex_id(rng, 8)
                spans.add(res, SCOPE_DOCLOAD, mc.span(
                    "documentLoad", trace_id=trace_id, span_id=root, start_ns=t0,
                    duration_ms=load_end - fetch_start,
                    attributes={**base, "http.url": url}))
                spans.add(res, SCOPE_DOCLOAD, mc.span(
                    "documentFetch", trace_id=trace_id, span_id=mc.hex_id(rng, 8), parent_span_id=root,
                    start_ns=t0 + int(fetch_start * mc.MS), duration_ms=response_end - fetch_start,
                    attributes={"session.id": session_id, "http.url": url,
                                "http.response_content_length": rng.randint(18_000, 90_000)}))
                for kind, rurl in _resource_list(state.cdn, app["host"], page):
                    start = response_end + rng.uniform(0, 0.3) * resources_ms
                    spans.add(res, SCOPE_DOCLOAD, mc.span(
                        "resourceFetch", trace_id=trace_id, span_id=mc.hex_id(rng, 8),
                        parent_span_id=root, start_ns=t0 + int(start * mc.MS),
                        duration_ms=rng.uniform(30, 250) * rlat,
                        attributes={"session.id": session_id, "http.url": rurl,
                                    "http.response_content_length": rng.randint(2_000, 400_000)}))
            else:
                logs.add(res, SCOPE_NAV, mc.log_record(
                    t0 + int(load_end * mc.MS), event_name="browser.navigation_timing",
                    attributes={
                        **base,
                        "browser.navigation_timing.type": rng.choice(["navigate"] * 9 + ["reload"]),
                        "browser.resource_timing.fetch_start": r1(fetch_start),
                        "browser.resource_timing.domain_lookup_start": r1(fetch_start),
                        "browser.resource_timing.domain_lookup_end": r1(fetch_start + dns),
                        "browser.resource_timing.connect_start": r1(fetch_start + dns),
                        "browser.resource_timing.connect_end": r1(fetch_start + dns + connect),
                        "browser.resource_timing.request_start": r1(fetch_start + dns + connect + 1),
                        "browser.resource_timing.response_start": r1(response_start),
                        "browser.resource_timing.response_end": r1(response_end),
                        "browser.navigation_timing.dom_interactive": r1(dom_interactive),
                        "browser.navigation_timing.dom_content_loaded_event_start": r1(dcl_start),
                        "browser.navigation_timing.dom_content_loaded_event_end": r1(dcl_end),
                        "browser.navigation_timing.dom_complete": r1(dom_complete),
                        "browser.navigation_timing.load_event_start": r1(load_start),
                        "browser.navigation_timing.load_event_end": r1(load_end),
                    }))
                for kind, rurl in _resource_list(state.cdn, app["host"], page)[: rng.randint(3, 6)]:
                    start = response_end + rng.uniform(0, 0.3) * resources_ms
                    dur = rng.uniform(30, 250) * rlat
                    logs.add(res, SCOPE_RESOURCE, mc.log_record(
                        t0 + int((start + dur) * mc.MS), event_name="browser.resource_timing",
                        attributes={
                            "session.id": session_id,
                            "url.full": rurl,
                            "browser.resource_timing.initiator_type": kind,
                            "browser.resource_timing.fetch_start": r1(start),
                            "browser.resource_timing.response_end": r1(start + dur),
                            "browser.resource_timing.transfer_size": rng.randint(800, 380_000),
                        }))

            # ── web vitals ───────────────────────────────────────────────────
            fcp = (fetch_start + ttfb_base + rng.uniform(250, 700)) * deg
            vitals = {
                "ttfb": ttfb_base * deg,
                "fcp": fcp,
                "lcp": fcp + (rng.uniform(200, 900) + 0.3 * resources_ms) * deg,
            }
            if rng.random() < 0.65:
                vitals["inp"] = rng.uniform(40, 180) * deg
            if rng.random() < 0.85:
                vitals["cls"] = min(1.0, rng.uniform(0.005, 0.08) * deg)
            for name, value in vitals.items():
                v = float(round(value, 4)) if name == "cls" else r1(value)
                logs.add(res, SCOPE_VITALS, mc.log_record(
                    t0 + int((load_end + rng.uniform(50, 4000)) * mc.MS), event_name="browser.web_vital",
                    attributes={**base,
                                "browser.web_vital.name": name,
                                "browser.web_vital.value": v,
                                "browser.web_vital.delta": v,
                                "browser.web_vital.rating": _rating(name, v),
                                "browser.web_vital.id": f"v4-{rng.randint(10**12, 10**13 - 1)}-{rng.randint(10**12, 10**13 - 1)}",
                                "browser.web_vital.navigation_type": "navigate"}))

            # ── user interaction + fetch ─────────────────────────────────────
            if rng.random() < 0.5:
                trace_id, root = mc.hex_id(rng, 16), mc.hex_id(rng, 8)
                t_click = t0 + int((load_end + rng.uniform(500, 6000)) * mc.MS)
                method = rng.choice(["POST", "GET", "GET"])
                fetch_ms = rng.uniform(80, 400) * rlat
                failed = rng.random() < 0.02
                spans.add(res, SCOPE_USER_INTERACTION, mc.span(
                    "click", trace_id=trace_id, span_id=root, start_ns=t_click,
                    duration_ms=fetch_ms + rng.uniform(5, 40),
                    attributes={"session.id": session_id, "event_type": "click",
                                "target_element": "BUTTON",
                                "target_xpath": rng.choice(['//*[@id="add-to-cart"]', '//*[@id="checkout"]',
                                                            '//*[@id="search-submit"]', '//*[@id="save"]']),
                                "http.url": url}))
                spans.add(res, SCOPE_FETCH, mc.span(
                    f"HTTP {method}", trace_id=trace_id, span_id=mc.hex_id(rng, 8), parent_span_id=root,
                    start_ns=t_click + int(2 * mc.MS), duration_ms=fetch_ms, kind=mc.SPAN_KIND_CLIENT,
                    attributes={"session.id": session_id, "http.request.method": method,
                                "url.full": f"https://{app['host']}/api/v1/cart",
                                "server.address": app["host"],
                                "http.response.status_code": rng.choice([502, 503]) if failed else 200},
                    status_code=mc.STATUS_ERROR if failed else mc.STATUS_UNSET))

            # ── uncaught JS errors ───────────────────────────────────────────
            if rng.random() < js_error_rate:
                if js_error_rate > BASELINE_ERROR_RATE and rng.random() < 0.85:
                    exc_type, msg, stack = fault_error
                else:
                    exc_type, msg, stack, _w = mc.weighted_choice(
                        rng, healthy_errors, [e[3] for e in healthy_errors])
                logs.add(res, SCOPE_WEB_EXCEPTION if app["legacy"] else SCOPE_ERRORS, mc.log_record(
                    t0 + int((dom_interactive + rng.uniform(10, 3000)) * mc.MS),
                    severity="ERROR", body=msg, event_name="exception",
                    attributes={**base, "exception.type": exc_type, "exception.message": msg,
                                "exception.stacktrace": stack}))
    return logs, spans


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
    cfg = scenario_data.get("generator_config", {}) if scenario_data else {}
    channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")
    state = RumState(ident, cfg, rng)
    logger.info("RUM OTel generator started (apps=%s, interval=%ds)", [a["name"] for a in state.apps], INTERVAL)
    ticks = 0

    def _tick() -> None:
        nonlocal ticks
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=channel_registry,
            generator_id=SPEC.id,
            chaos_mode=chaos_mode,
            impact_keys=SPEC.impact_keys,
        )
        degradation = float(impact_value(active_chaos, "web_vital_degradation", default=1.0))
        error_rate = float(impact_value(active_chaos, "js_error_rate", default=BASELINE_ERROR_RATE))
        latency = float(impact_value(active_chaos, "resource_load_latency", default=1.0))
        now_ns = int(time.time() * 1e9)
        logs, spans = simulate_tick(state, rng, now_ns, web_vital_degradation=degradation,
                                    js_error_rate=error_rate, resource_load_latency=latency)
        mc.send_traces(client, spans, "otel-rum-traces")
        mc.send_logs(client, logs, "otel-rum-logs")
        ticks += 1
        if ticks % 30 == 0:
            logger.info("RUM tick %d: %d spans, %d logs", ticks, len(spans), len(logs))

    run_tick_loop(stop_event, _tick, interval=lambda: INTERVAL)
    logger.info("RUM OTel generator stopped after %d ticks", ticks)


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = OTLPClient()
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    threading.Timer(int(os.environ.get("RUN_DURATION", "60")), stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
