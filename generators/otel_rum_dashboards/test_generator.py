"""RUM OTel generator: dashboard field coverage, healthy baseline, impact keys."""

from __future__ import annotations

import random
import statistics

from generator_engine import mobile_common as mc
from generator_engine.identity import ScenarioIdentity
from generators.otel_rum_dashboards import SPEC
from generators.otel_rum_dashboards.generator import RumState, simulate_tick

NOW = 1_700_000_000 * 10**9
PAGE_LOAD_SCOPES = {"@opentelemetry/browser-instrumentation/navigation-timing",
                    "@opentelemetry/instrumentation-document-load"}
ERROR_SCOPES = {"@opentelemetry/browser-instrumentation/errors", "@opentelemetry/instrumentation-web-exception"}


def _run(ticks: int = 30, seed: int = 5, **impact):
    rng = random.Random(seed)
    state = RumState(ScenarioIdentity("rum"), {}, rng)
    logs, spans = [], []
    for i in range(ticks):
        lb, sb = simulate_tick(state, rng, NOW + i * 10 * 10**9, **impact)
        for batch, sink in ((lb, logs), (sb, spans)):
            for resource, scope, recs in batch.grouped():
                ra = mc.attr_map(resource)
                for r in recs:
                    sink.append((ra, scope["name"], r, mc.attr_map(r)))
    return logs, spans


def _vitals(logs, name):
    return [a["browser.web_vital.value"] for _ra, _s, r, a in logs
            if r.get("eventName") == "browser.web_vital" and a["browser.web_vital.name"] == name]


def _load_ms(logs, spans):
    """The dashboard's load_duration: COALESCE(transaction.duration, load_event_end - fetch_start)."""
    out = [a["browser.navigation_timing.load_event_end"] - a["browser.resource_timing.fetch_start"]
           for _ra, scope, r, a in logs if r.get("eventName") == "browser.navigation_timing"]
    out += [(int(s["endTimeUnixNano"]) - int(s["startTimeUnixNano"])) / 1e6
            for _ra, scope, s, _a in spans if s["name"] == "documentLoad"]
    return out


def _errors(logs):
    return [(ra, a) for ra, scope, _r, a in logs if scope in ERROR_SCOPES]


def test_every_record_matches_the_dashboard_filters():
    logs, spans = _run(ticks=5)
    for ra, _s, _r, _a in logs + spans:
        assert ra["telemetry.sdk.language"] == "webjs"
        assert ra["user_agent.original"].startswith("Mozilla/5.0")
        assert ra["browser.name"] and ra["browser.platform"]
        assert "os.name" not in ra  # keeps browser data out of the Android / iOS dashboards
    assert {ra["browser.name"] for ra, *_ in logs} >= {"Chrome", "Safari", "Firefox", "Edge"}
    assert len({ra["service.name"] for ra, *_ in logs}) == 3


def test_page_loads_come_from_both_instrumentations_without_double_counting():
    logs, spans = _run(ticks=5)
    nav = [(ra, scope) for ra, scope, r, _a in logs if r.get("eventName") == "browser.navigation_timing"]
    doc = [(ra, scope) for ra, scope, s, _a in spans if s["name"] == "documentLoad"]
    assert nav and doc
    assert all(scope in PAGE_LOAD_SCOPES for _ra, scope in nav + doc)
    nav_apps = {ra["service.name"] for ra, _s in nav}
    doc_apps = {ra["service.name"] for ra, _s in doc}
    assert doc_apps == {"rum-account-portal"} and not nav_apps & doc_apps


def test_all_five_web_vitals_are_reported_as_doubles():
    logs, _spans = _run(ticks=5)
    for name in ("lcp", "inp", "cls", "fcp", "ttfb"):
        values = _vitals(logs, name)
        assert values, name
        assert all(isinstance(v, float) for v in values)


def test_healthy_baseline_is_good_vitals_and_rare_errors():
    logs, spans = _run()
    assert statistics.median(_vitals(logs, "lcp")) < 2500
    assert statistics.median(_vitals(logs, "inp")) < 200
    assert statistics.median(_vitals(logs, "cls")) < 0.1
    assert statistics.median(_load_ms(logs, spans)) < 2500
    page_views = len(_vitals(logs, "fcp"))
    errors = _errors(logs)
    assert 0 < len(errors) / page_views < 0.06
    for _ra, a in errors:
        assert a["exception.message"] and a["exception.stacktrace"]


def test_web_vital_degradation_pushes_lcp_and_inp_to_poor():
    logs, _spans = _run(web_vital_degradation=3.0)
    assert statistics.median(_vitals(logs, "lcp")) > 4000
    assert statistics.median(_vitals(logs, "inp")) > 200


def test_js_error_rate_raises_errors_led_by_the_fault_error():
    logs, _spans = _run(js_error_rate=0.6)
    page_views = len(_vitals(logs, "fcp"))
    errors = _errors(logs)
    assert len(errors) / page_views > 0.45
    top = max(set(a["exception.message"] for _ra, a in errors),
              key=lambda m: sum(a["exception.message"] == m for _ra, a in errors))
    assert top == "Cannot read properties of undefined (reading 'items')"


def test_resource_load_latency_raises_page_load_percentiles():
    logs, spans = _run()
    slow_logs, slow_spans = _run(resource_load_latency=5.0)
    healthy, slow = _load_ms(logs, spans), _load_ms(slow_logs, slow_spans)
    assert statistics.median(slow) > 2.5 * statistics.median(healthy)
    fetches = [(int(s["endTimeUnixNano"]) - int(s["startTimeUnixNano"])) / 1e6
               for _ra, _sc, s, _a in slow_spans if s["name"] == "resourceFetch"]
    assert statistics.median(fetches) > 300


def test_spec_shape():
    assert SPEC.wants_chaos and SPEC.chaos_mode == "explicit"
    assert set(SPEC.impact_keys) == {"web_vital_degradation", "js_error_rate", "resource_load_latency"}
    assert SPEC.integrations == ("otel_rum_dashboards",)
