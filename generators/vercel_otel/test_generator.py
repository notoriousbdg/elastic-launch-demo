"""Vercel-specific tests for the vercel_otel generator.

Standard emit/registry tests live in tests/test_generators_emit.py.
These tests lock the drain-schema field names the package dashboards query.
"""
from __future__ import annotations

import importlib
import math
import threading

import pytest

try:
    import dotenv  # noqa: F401
    _HAS_DOTENV = True
except ImportError:
    _HAS_DOTENV = False

pytestmark = pytest.mark.skipif(
    not _HAS_DOTENV,
    reason="Generator modules require dotenv via app.telemetry; run inside Docker",
)


class _StubClient:
    def __init__(self):
        self.logs: list[dict] = []
        self.metrics: list[dict] = []
        self.resources: list[dict] = []

    def send_logs_scoped(self, resource, scope, records):
        self.resources.append(resource)
        self.logs.extend(records)

    def send_metrics_scoped(self, resource, scope, metrics):
        self.resources.append(resource)
        self.metrics.extend(metrics)

    def send_metrics(self, resource, metrics):
        raise AssertionError("use send_metrics_scoped so the Vercel scope is preserved")

    def close(self):
        pass


def _attr_map(record: dict) -> dict:
    return {a["key"]: a["value"] for a in record.get("attributes", [])}


def _attr_value(value: dict):
    if "stringValue" in value:
        return value["stringValue"]
    if "intValue" in value:
        return int(value["intValue"])
    if "doubleValue" in value:
        return float(value["doubleValue"])
    if "boolValue" in value:
        return value["boolValue"]
    if "kvlistValue" in value:
        return {
            item["key"]: _attr_value(item["value"])
            for item in value["kvlistValue"].get("values", [])
        }
    if "arrayValue" in value:
        return [_attr_value(v) for v in value["arrayValue"].get("values", [])]
    return value


def _run(monkeypatch=None) -> _StubClient:
    mod = importlib.import_module("generators.vercel_otel.generator")
    if monkeypatch is not None:
        monkeypatch.setattr(mod, "ensure_dashboard_mappings_from_env", lambda: None)
    else:
        mod.ensure_dashboard_mappings_from_env = lambda: None  # type: ignore[method-assign]
    client = _StubClient()
    stop = threading.Event()
    threading.Timer(0.2, stop.set).start()
    mod.run(client, stop, scenario_data={
        "namespace": "test",
        "channel_registry": {},
        "generator_config": {
            "records_per_cycle": 8,
            "project_name": "demo-app",
            "environment": "production",
            "deployment_id": "dpl_testfixed00000001",
        },
    })
    return client


def test_emits_all_four_datasets(monkeypatch):
    client = _run(monkeypatch)
    datasets = set()
    for res in client.resources:
        for a in res["attributes"]:
            if a["key"] == "data_stream.dataset":
                datasets.add(a["value"]["stringValue"])
    assert datasets == {
        "vercel.logs",
        "vercel.analytics.v2",
        "vercel.auditlog.v1",
        "vercel.speedinsights.v1",
    }


def test_resource_has_deployment_id(monkeypatch):
    client = _run(monkeypatch)
    for res in client.resources:
        keys = {a["key"]: _attr_value(a["value"]) for a in res["attributes"]}
        assert keys.get("deployment.id") == "dpl_testfixed00000001"
        assert keys.get("deployment.environment.name") == "production"


def test_logs_dashboard_fields(monkeypatch):
    client = _run(monkeypatch)
    log_records = [r for r in client.logs if r.get("eventName") == "vercel.function.log"]
    assert log_records
    required = {
        "vercel.log.source",
        "vercel.execution.region",
        "vercel.project.name",
        "vercel.project.id",
        "http.response.status_code",
        "http.route",
        "log.level",
        "url.path",
        "vercel.proxy",
    }
    for r in log_records:
        keys = set(_attr_map(r))
        missing = required - keys
        assert not missing, missing
        attrs = {k: _attr_value(v) for k, v in _attr_map(r).items()}
        assert attrs["log.level"] in ("INFO", "WARNING", "ERROR")
        assert isinstance(attrs["http.response.status_code"], int)
        proxy = attrs["vercel.proxy"]
        assert isinstance(proxy, dict), "vercel.proxy must be a map (flattened), not a JSON string"
        assert "path" in proxy and "host" in proxy
        assert "method" not in proxy  # promoted to http.request.method
        if attrs["vercel.log.source"] == "firewall":
            assert "waf_action" in proxy


def test_analytics_dashboard_fields(monkeypatch):
    client = _run(monkeypatch)
    analytics = [
        r for r in client.logs
        if str(r.get("eventName", "")).startswith("vercel.analytics.")
    ]
    assert analytics
    for r in analytics:
        attrs = {k: _attr_value(v) for k, v in _attr_map(r).items()}
        assert attrs["vercel.analytics.event.type"] in ("pageview", "event")
        assert "device.id" in attrs
        assert "url.path" in attrs
        assert "http.route" in attrs
        assert "vercel.device.type" in attrs
        assert "vercel.project.id" in attrs


def test_audit_uses_vercel_action_event_names(monkeypatch):
    _run(monkeypatch)
    # May be empty on a single short cycle — force one more sample via builder.
    mod = importlib.import_module("generators.vercel_otel.generator")
    rng = __import__("random").Random(0)
    rec = mod._build_audit_log("prj_test", "test", rng)
    assert "-" in rec["eventName"]  # kebab-case Vercel action
    attrs = {k: _attr_value(v) for k, v in _attr_map(rec).items()}
    for key in ("vercel.actor.type", "user.id", "user.name", "user.email", "client.address"):
        assert key in attrs


def test_speed_insights_metric_names_and_attrs(monkeypatch):
    client = _run(monkeypatch)
    assert client.metrics
    names = {m["name"] for m in client.metrics}
    assert names == {"LCP", "INP", "CLS", "TTFB"}
    units = {m["name"]: m["unit"] for m in client.metrics}
    assert units["LCP"] == "ms"
    assert units["INP"] == "ms"
    assert units["TTFB"] == "ms"
    assert units["CLS"] == "1"
    for m in client.metrics:
        dps = m["gauge"]["dataPoints"]
        assert dps
        keys = {a["key"] for a in dps[0].get("attributes", [])}
        for required in (
            "vercel.project.id",
            "vercel.device.type",
            "vercel.connection.speed",
            "url.path",
            "http.route",
            "geo.country.iso_code",
            "user_agent.os.name",
            "deployment.id",
        ):
            assert required in keys, f"{m['name']} missing {required}"
        assert _attr_value(
            next(a["value"] for a in dps[0]["attributes"] if a["key"] == "deployment.id")
        ) == "dpl_testfixed00000001"


def test_speed_insights_includes_poor_tail():
    """Dashboard Poor % panels need some samples above CWV poor thresholds."""
    mod = importlib.import_module("generators.vercel_otel.generator")
    rng = __import__("random").Random(0)
    lcps, inps, clss, ttfbs = [], [], [], []
    for _ in range(200):
        for m in mod._build_vitals("prj_test", "dpl_test", rng, 1.0, 1.0):
            val = float(m["gauge"]["dataPoints"][0]["asDouble"])
            if m["name"] == "LCP":
                lcps.append(val)
            elif m["name"] == "INP":
                inps.append(val)
            elif m["name"] == "CLS":
                clss.append(val)
            elif m["name"] == "TTFB":
                ttfbs.append(val)
    assert any(v > 4000 for v in lcps), "expected some poor LCP (>4s)"
    assert any(v > 500 for v in inps), "expected some poor INP (>500ms)"
    assert any(v > 0.25 for v in clss), "expected some poor CLS (>0.25)"
    assert any(v > 1800 for v in ttfbs), "expected some poor TTFB (>1.8s)"
    assert any(v < 2500 for v in lcps), "expected some good LCP samples"


def test_path_cardinality_supports_error_rate_panel(monkeypatch):
    """Top Paths by Error Rate filters WHERE total >= 100 — paths must reuse."""
    client = _run(monkeypatch)
    paths = []
    for r in client.logs:
        if r.get("eventName") != "vercel.function.log":
            continue
        attrs = {k: _attr_value(v) for k, v in _attr_map(r).items()}
        paths.append(attrs["url.path"])
    assert paths
    # Startup seed alone is >= 80 records; with fixed dashboard ids, unique
    # paths must stay small enough that totals can cross 100 within minutes.
    assert len(set(paths)) <= 20
    assert all("/dashboard/" not in p or p.split("/")[-1] in {"1", "2", "42"} for p in paths)


def test_only_the_seed_cycle_spans_the_dashboard_lookback(monkeypatch):
    """Request Volume / Warning trend use 20 buckets over now-24h/h, so the startup seed
    spreads over 24h. Steady-state cycles must be live: back-dating them makes the most
    recent 15 minutes thinner than the previous 15 and trips the rate-drop alerts."""
    sim = _simulate(monkeypatch, minutes=10, fault=None)
    seed_n = 240  # max(records_per_cycle * 3, 240)
    logs = sim.logs["vercel.logs"]
    seed, steady = logs[:seed_n], logs[seed_n:]
    assert steady
    seed_ages_h = [(sim.now - r["ts"]) / 3600 for r in seed]
    steady_ages_min = [(sim.now - r["ts"]) / 60 for r in steady]
    assert max(seed_ages_h) > 3.0 and len({int(a) for a in seed_ages_h}) >= 4
    assert max(steady_ages_min) <= 10.5, "steady-state events must not be back-dated"


def test_ensure_dashboard_mappings_writes_flattened(monkeypatch):
    mod = importlib.import_module("generators.vercel_otel.generator")
    calls = []

    class _Resp:
        def __init__(self, status_code=200, payload=None):
            self.status_code = status_code
            self._payload = payload or {}
            self.text = "{}"

        def json(self):
            return self._payload

    def fake_get(url, **kwargs):
        calls.append(("GET", url))
        if "_field_caps" in url:
            return _Resp(200, {
                "fields": {
                    "attributes.vercel.proxy": {"keyword": {}},
                    "vercel.proxy": {"keyword": {}},
                }
            })
        if "_component_template" in url:
            return _Resp(200, {"component_templates": []})
        return _Resp(404)

    def fake_put(url, **kwargs):
        calls.append(("PUT", url, kwargs.get("json")))
        return _Resp(200)

    def fake_delete(url, **kwargs):
        calls.append(("DELETE", url))
        return _Resp(200)

    monkeypatch.setattr(mod.httpx, "get", fake_get)
    monkeypatch.setattr(mod.httpx, "put", fake_put)
    monkeypatch.setattr(mod.httpx, "delete", fake_delete)

    mod.ensure_dashboard_mappings("https://es.example", "key", reset_streams=True)
    puts = [c for c in calls if c[0] == "PUT"]
    assert len(puts) >= 2  # logs-otel@custom + logs@custom
    for put in puts:
        proxy = (
            put[2]["template"]["mappings"]["properties"]["attributes"]["properties"]["vercel.proxy"]
        )
        assert proxy == {"type": "flattened"}
    deletes = [c[1] for c in calls if c[0] == "DELETE"]
    assert any("logs-vercel.logs.otel-default" in u for u in deletes)
    assert any("metrics-vercel.speedinsights.v1.otel-default" in u for u in deletes)



# ═════════════════════════════════════════════════════════════════════════════
# Package alerts (vercel_otel@0.4.1), evaluated on a simulated run()
# ═════════════════════════════════════════════════════════════════════════════
# run() is driven on a fake clock: every wait() advances it by STEP_S. Each alert below
# mirrors its ES|QL template over the template's window, relative to the final "now".

import random
from collections import defaultdict
from dataclasses import dataclass, field

STEP_S = 30            # default simulated cycle length (the real cycle is 5-10 s)
TOTAL_MINUTES = 75
_REAL_RANDOM = random.Random
_SENSITIVE_PATTERNS = ("env-variable", "drain", "webhook", "token", "team-", "domain", "project-transfer")


@dataclass
class _Sim:
    now: float
    logs: dict = field(default_factory=lambda: defaultdict(list))  # dataset -> [{ts, level, event, attrs}]
    metrics: list = field(default_factory=list)                    # [{name, ts, value, attrs}]
    resources: list = field(default_factory=list)


class _SimClient:
    def __init__(self, sim: _Sim):
        self.sim = sim

    @staticmethod
    def _dataset(resource: dict) -> str:
        return next(a["value"]["stringValue"] for a in resource["attributes"]
                    if a["key"] == "data_stream.dataset")

    def send_logs_scoped(self, resource, scope, records):
        self.sim.resources.append(resource)
        for r in records:
            self.sim.logs[self._dataset(resource)].append({
                "ts": int(r["timeUnixNano"]) / 1e9,
                "level": r["severityText"],
                "event": r.get("eventName"),
                "attrs": {k: _attr_value(v) for k, v in _attr_map(r).items()},
            })

    def send_metrics_scoped(self, resource, scope, metrics):
        self.sim.resources.append(resource)
        for m in metrics:
            dp = m["gauge"]["dataPoints"][0]
            self.sim.metrics.append({
                "name": m["name"], "ts": int(dp["timeUnixNano"]) / 1e9,
                "value": float(dp["asDouble"] if "asDouble" in dp else dp["asInt"]),
                "attrs": {a["key"]: _attr_value(a["value"]) for a in dp.get("attributes", [])},
            })


def _simulate(monkeypatch, *, minutes=TOTAL_MINUTES, fault=None, fault_minutes=60,
              seed=7, records_per_cycle=60, config=None, step_s=STEP_S) -> _Sim:
    mod = importlib.import_module("generators.vercel_otel.generator")
    start = 1_700_000_000.0
    clock = {"t": start}
    end = start + minutes * 60
    fault_from = end - fault_minutes * 60

    monkeypatch.setattr(mod, "ensure_dashboard_mappings_from_env", lambda: None)
    monkeypatch.setattr(mod, "business_load", lambda: 1.0)
    monkeypatch.setattr(mod.time, "time", lambda: clock["t"])
    # Metric points take the scrape clock, which reads time.time_ns().
    monkeypatch.setattr(mod.time, "time_ns", lambda: int(clock["t"] * 1e9))
    monkeypatch.setattr(mod.random, "Random", lambda *a: _REAL_RANDOM(seed))

    class _Stop:
        def is_set(self):
            return clock["t"] >= end

        def wait(self, _timeout=None):
            clock["t"] += step_s

    class _Chaos:
        def get_active_channels(self):
            return ["ch1"] if fault and clock["t"] >= fault_from else []

    registry = {"ch1": {"generator_impact": {"vercel_otel": fault or {}}}}
    sim = _Sim(now=end)
    cfg = {"records_per_cycle": records_per_cycle, "project_name": "demo-app",
           "environment": "production", "deployment_id": "dpl_sim00000000001"}
    cfg.update(config or {})
    mod.run(_SimClient(sim), _Stop(), scenario_data={
        "namespace": "test", "channel_registry": registry, "generator_config": cfg,
    }, chaos_controller=_Chaos())
    return sim


# ── alert evaluators ─────────────────────────────────────────────────────────

def _window(rows, now, minutes):
    return [r for r in rows if now - minutes * 60 <= r["ts"] <= now]


def _group(rows, key):
    out = defaultdict(list)
    for r in rows:
        out[key(r)].append(r)
    return out


def _p75(values):
    v = sorted(values)
    pos = 0.75 * (len(v) - 1)
    lo = int(pos)
    return v[lo] + (v[min(lo + 1, len(v) - 1)] - v[lo]) * (pos - lo)


def _lambda_edge(rows):
    return [r for r in rows if r["attrs"].get("vercel.log.source") in ("lambda", "edge")]


def _rate_alert(rows, total_min, pct_over, num):
    """total > N AND num/total*100 > pct_over for any group in `rows` (already grouped)."""
    return len(rows) > total_min and num(rows) / len(rows) * 100.0 > pct_over


def _error_warning_log_rate(sim):
    for _src, rows in _group(_window(sim.logs["vercel.logs"], sim.now, 15),
                             lambda r: r["attrs"]["vercel.log.source"]).items():
        if _rate_alert(rows, 20, 10.0, lambda g: sum(1 for r in g if r["level"] in ("ERROR", "WARNING"))):
            return True
    return False


def _http_5xx_by_source(sim):
    rows = [r for r in _lambda_edge(_window(sim.logs["vercel.logs"], sim.now, 15))
            if r["attrs"]["http.response.status_code"] != -1]
    return any(
        _rate_alert(g, 20, 2.0, lambda x: sum(1 for r in x if r["attrs"]["http.response.status_code"] >= 500))
        for g in _group(rows, lambda r: r["attrs"]["vercel.log.source"]).values()
    )


def _crash_rate(sim):
    rows = _lambda_edge(_window(sim.logs["vercel.logs"], sim.now, 15))
    return any(
        _rate_alert(g, 20, 1.0, lambda x: sum(1 for r in x if r["attrs"]["http.response.status_code"] == -1))
        for g in _group(rows, lambda r: r["attrs"]["vercel.log.source"]).values()
    )


def _regional_skew(sim):
    rows = [r for r in _lambda_edge(_window(sim.logs["vercel.logs"], sim.now, 15))
            if r["attrs"]["http.response.status_code"] != -1]
    return any(
        _rate_alert(g, 20, 5.0, lambda x: sum(1 for r in x if r["attrs"]["http.response.status_code"] >= 500))
        for g in _group(rows, lambda r: r["attrs"]["vercel.execution.region"]).values()
    )


def _firewall_deny_spike(sim):
    rows = [r for r in _window(sim.logs["vercel.logs"], sim.now, 10)
            if r["attrs"].get("vercel.log.source") == "firewall"
            and (r["attrs"].get("vercel.proxy") or {}).get("waf_action") == "deny"]
    return len(rows) > 100


def _volume_drop(rows, now):
    win = _window(rows, now, 30)
    for _project, g in _group(win, lambda r: r["attrs"]["vercel.project.id"]).items():
        recent = sum(1 for r in g if r["ts"] >= now - 15 * 60)
        baseline = len(g) - recent
        if baseline > 5 and 100.0 - recent / baseline * 100.0 > 50.0:
            return True
    return False


def _request_rate_drop(sim):
    return _volume_drop(sim.logs["vercel.logs"], sim.now)


def _page_view_rate_drop(sim):
    return _volume_drop([r for r in sim.logs["vercel.analytics.v2"]
                         if r["attrs"].get("vercel.analytics.event.type") == "pageview"], sim.now)


def _ttfb_p75(sim):
    vals = [m["value"] for m in _window(sim.metrics, sim.now, 60) if m["name"] == "TTFB"]
    return len(vals) >= 20 and _p75(vals) > 1800.0


def _poor_core_web_vitals(sim):
    poor = {"LCP": 4000.0, "INP": 500.0, "CLS": 0.25}
    win = [m for m in _window(sim.metrics, sim.now, 60) if m["name"] in poor]
    for (name, _route), g in _group(win, lambda m: (m["name"], m["attrs"].get("http.route") or m["attrs"].get("url.path"))).items():
        if len(g) >= 5 and _p75([m["value"] for m in g]) > poor[name]:
            return True
    return False


def _sensitive_audit(sim):
    rows = [r for r in _window(sim.logs["vercel.auditlog.v1"], sim.now, 60)
            if any(p in (r["event"] or "") for p in _SENSITIVE_PATTERNS)]
    counts = defaultdict(int)
    for r in rows:
        counts[r["attrs"].get("user.email") or r["attrs"].get("user.id")] += 1
    return any(c > 5 for c in counts.values())


ALERTS = {
    "elevated_error_warning_log_rate_by_source": _error_warning_log_rate,
    "high_http_5xx_error_rate_by_log_source": _http_5xx_by_source,
    "lambda_edge_function_crash_rate_elevated": _crash_rate,
    "regional_error_rate_skew": _regional_skew,
    "request_rate_drop": _request_rate_drop,
    "page_view_rate_drop": _page_view_rate_drop,
    "firewall_deny_spike": _firewall_deny_spike,
    "sensitive_audit_activity_spike_by_actor": _sensitive_audit,
    "elevated_ttfb_p75": _ttfb_p75,
    "poor_core_web_vitals_p75_by_route": _poor_core_web_vitals,
}

# Magnitudes used by the gt-vercel scenario channels, and how long the fault must run
# for its alert (the drop alerts compare the last 15 minutes with the 15 before).
GT_VALUES = {
    "elevated_error_warning_log_rate_by_source": 0.4,
    "high_http_5xx_error_rate_by_log_source": 0.025,
    "lambda_edge_function_crash_rate_elevated": 0.04,
    "regional_error_rate_skew": 0.08,
    "request_rate_drop": 0.9,
    "page_view_rate_drop": 0.9,
    "firewall_deny_spike": 15.0,
    "sensitive_audit_activity_spike_by_actor": 3.0,
    "elevated_ttfb_p75": 3.0,
    "poor_core_web_vitals_p75_by_route": 2.0,
}
# The ratio alerts (thresholds 1-5%) evaluate 15-30 minute windows, so their reliability
# depends on how many events a window holds. Test them at the real 7.5 s cycle and 80
# records per cycle over a shorter timeline; the 60-minute alerts use the coarse default.
_REAL = {"minutes": 45, "step_s": 7.5, "records_per_cycle": 80}
PROFILES = {
    "elevated_error_warning_log_rate_by_source": {**_REAL, "fault_minutes": 30},
    "high_http_5xx_error_rate_by_log_source": {**_REAL, "fault_minutes": 30},
    "lambda_edge_function_crash_rate_elevated": {**_REAL, "fault_minutes": 30},
    "regional_error_rate_skew": {**_REAL, "fault_minutes": 30},
    "request_rate_drop": {**_REAL, "fault_minutes": 15},
    "page_view_rate_drop": {**_REAL, "fault_minutes": 15},
    "firewall_deny_spike": {**_REAL, "fault_minutes": 30},
}


def _breached(sim) -> set[str]:
    return {key for key, fires in ALERTS.items() if fires(sim)}


def test_alert_table_covers_every_impact_key():
    from generators.vercel_otel import SPEC

    assert set(ALERTS) == set(SPEC.impact_keys) == set(GT_VALUES)


@pytest.mark.parametrize("seed", [3, 7, 11])
def test_healthy_run_breaches_no_alert(monkeypatch, seed):
    sim = _simulate(monkeypatch, seed=seed)
    assert _breached(sim) == set()


@pytest.mark.parametrize("key", sorted(GT_VALUES))
def test_fault_alone_breaches_only_its_own_alert(monkeypatch, key):
    sim = _simulate(monkeypatch, fault={key: GT_VALUES[key]}, **PROFILES.get(key, {}))
    assert _breached(sim) == {key}


def test_small_sensitive_audit_value_still_breaches(monkeypatch):
    """The doc promises a breach above 0.02; rounding to whole events used to emit none."""
    sim = _simulate(monkeypatch, fault={"sensitive_audit_activity_spike_by_actor": 0.05})
    assert "sensitive_audit_activity_spike_by_actor" in _breached(sim)


@pytest.mark.parametrize("seed", [3, 7])
def test_healthy_run_at_the_real_cycle_rate_breaches_no_alert(monkeypatch, seed):
    """The 15-minute ratio alerts, over windows as large as production's."""
    assert _breached(_simulate(monkeypatch, seed=seed, **_REAL)) == set()


def test_environment_defaults_to_production(monkeypatch):
    sim = _simulate(monkeypatch, minutes=1, config={"environment": None})
    envs = {a["value"]["stringValue"] for res in sim.resources for a in res["attributes"]
            if a["key"] == "deployment.environment.name"}
    assert envs == {"production"}, "9 of the 10 package alerts filter on production"


def test_healthy_firewall_records_do_not_count_as_warnings(monkeypatch):
    sim = _simulate(monkeypatch, minutes=20)
    firewall = [r for r in sim.logs["vercel.logs"] if r["attrs"].get("vercel.log.source") == "firewall"]
    assert firewall and {r["level"] for r in firewall} == {"INFO"}


def test_healthy_sensitive_audit_budget_at_the_real_cycle_rate():
    """Real cycles are 5-10 s. Expected healthy sensitive events per hour must be far
    below the 'more than 5 per actor' limit: sum over ~20 actors, with margin."""
    mod = importlib.import_module("generators.vercel_otel.generator")
    cycles_per_hour = 3600 / ((mod.BATCH_INTERVAL_MIN + mod.BATCH_INTERVAL_MAX) / 2)
    expected_total = cycles_per_hour * mod._AUDIT_CYCLE_PROBABILITY * mod._AUDIT_SENSITIVE_PROBABILITY
    assert expected_total / 20 <= 0.5, expected_total


# ── package SLO baselines: "[Vercel OTel] <vital> p75 99.5%" ──────────────────

_SLO_GOOD = {"LCP": 2500.0, "INP": 200.0, "CLS": 0.1, "TTFB": 800.0}


def _nearest_rank_p75(values):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(0.75 * len(ordered)) - 1)]


def _cycle_vitals(mod, rng, loads, lcp_mult=0.0, ttfb_mult=0.0):
    ratings = {name: mod.healthy_ratings(rng, loads) for name in mod.METRIC_NAMES}
    out = defaultdict(list)
    for i in range(loads):
        for m in mod._build_vitals("prj_test", "dpl_test", rng, lcp_mult, ttfb_mult,
                                   ratings={n: r[i] for n, r in ratings.items()}):
            out[m["name"]].append(float(m["gauge"]["dataPoints"][0]["asDouble"]))
    return out


@pytest.mark.parametrize("loads", [1, 2, 4, 5, 6, 20, 60])
def test_healthy_cycle_p75_meets_every_vital_slo(loads):
    """Every cycle's p75 is a good load, so no SLO timeslice burns budget when healthy."""
    mod = importlib.import_module("generators.vercel_otel.generator")
    rng = random.Random(loads)
    for _ in range(200):
        for name, values in _cycle_vitals(mod, rng, loads).items():
            assert _nearest_rank_p75(values) < _SLO_GOOD[name], (name, values)


def test_healthy_ratings_keep_a_non_good_tail():
    mod = importlib.import_module("generators.vercel_otel.generator")
    rng = random.Random(1)
    ratings = [r for _ in range(500) for r in mod.healthy_ratings(rng, 20)]
    share = sum(1 for r in ratings if r) / len(ratings)
    assert 0.08 < share < 0.16 and 2 in ratings


def test_web_vitals_faults_push_p75_past_the_slo_and_alert_thresholds():
    mod = importlib.import_module("generators.vercel_otel.generator")
    rng = random.Random(5)
    vitals = _cycle_vitals(mod, rng, 20, lcp_mult=GT_VALUES["poor_core_web_vitals_p75_by_route"],
                           ttfb_mult=GT_VALUES["elevated_ttfb_p75"])
    assert _nearest_rank_p75(vitals["LCP"]) > 4000.0
    assert _nearest_rank_p75(vitals["INP"]) > 500.0
    assert _nearest_rank_p75(vitals["TTFB"]) > 1800.0
