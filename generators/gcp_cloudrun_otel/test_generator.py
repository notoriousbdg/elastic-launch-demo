"""Tests for the gcp_cloudrun_otel generator."""

from __future__ import annotations

import random

from generator_engine.identity import ScenarioIdentity
from generators.gcp_cloudrun_otel import SPEC
from generators.gcp_cloudrun_otel.generator import (
    HISTOGRAM_METRICS,
    METRIC_NAMES,
    RevisionState,
    build_exponential_histogram,
    build_revisions,
    generate_revision_metrics,
)

_P = "run.googleapis.com/"
NOW = 1_700_000_000.0


def _attrs(dp: dict) -> dict:
    return {a["key"]: next(iter(a["value"].values())) for a in dp.get("attributes", [])}


def _revisions():
    ident = ScenarioIdentity("gtgcpcloudrun")
    return build_revisions(ident, ["checkout-api", "image-resizer", "notification-worker"],
                           ident.gcp_project_id, "us-central1")


def _emit(chaos=None, seed=3, target_only=True):
    rng = random.Random(seed)
    out = []
    for rev in _revisions():
        if target_only and rev["service_index"] != 0:
            continue
        out.extend(generate_revision_metrics(rev, RevisionState(rng), rng, chaos, now=NOW))
    return out


def _gauges(metrics, suffix):
    out = []
    for m in metrics:
        if m["name"] == _P + suffix and "gauge" in m:
            for dp in m["gauge"]["dataPoints"]:
                v = float(dp["asDouble"]) if "asDouble" in dp else float(dp["asInt"])
                out.append((_attrs(dp), v))
    return out


def _hist(metrics, suffix):
    return [dp for m in metrics if m["name"] == _P + suffix
            for dp in m["exponentialHistogram"]["dataPoints"]]


def _percentile(dp, q):
    """Approximate percentile from exponential-histogram buckets."""
    import math
    total = int(dp["count"])
    rank = q * total
    seen = int(dp["zeroCount"])
    if seen >= rank:
        return 0.0
    base = 2 ** (2 ** -dp["scale"])
    for i, c in enumerate(dp["positive"]["bucketCounts"]):
        seen += int(c)
        if seen >= rank:
            return base ** (dp["positive"]["offset"] + i + 1)
    return math.inf


def _error_pct(metrics):
    pts = _gauges(metrics, "request_count")
    total = sum(v for _, v in pts)
    return 100.0 * sum(v for a, v in pts if a["response_code_class"] == "5xx") / total


def test_spec_shape():
    assert SPEC.client == "otlp"
    assert SPEC.datasets == ("gcp.cloudrun.otel",)
    assert SPEC.wants_chaos is True and SPEC.chaos_mode == "explicit"
    assert len(SPEC.impact_keys) >= 3
    assert all(not f["required"] for f in SPEC.config_fields.values())


def test_revisions_and_identity():
    revs = _revisions()
    services = {r["service"] for r in revs}
    assert services == {"gtgcpcloudrun-checkout-api", "gtgcpcloudrun-image-resizer",
                        "gtgcpcloudrun-notification-worker"}
    assert len(revs) == 4
    assert all(r["revision"].startswith(r["service"] + "-") for r in revs)
    assert all(r["project_id"] == "gtgcpcloudrun-project" for r in revs)


def test_every_dashboard_metric_and_shape():
    metrics = _emit(target_only=False)
    assert {m["name"] for m in metrics} == set(METRIC_NAMES)
    for m in metrics:
        if m["name"] in HISTOGRAM_METRICS:
            body = m["exponentialHistogram"]
            assert body["aggregationTemporality"] == 1
            for dp in body["dataPoints"]:
                buckets = sum(int(c) for c in dp["positive"]["bucketCounts"])
                assert buckets + int(dp["zeroCount"]) == int(dp["count"])
                assert dp["min"] <= dp["max"]
        else:
            assert "gauge" in m
    states = {a.get("state") for a, _ in _gauges(metrics, "container/instance_count")}
    assert states == {"active", "idle"}
    conc = _hist(metrics, "container/max_request_concurrencies")
    assert conc and all(_attrs(dp)["state"] == "active" for dp in conc)


def test_exponential_histogram_scaling():
    m = build_exponential_histogram(_P + "x", "ms", [1.0, 2.0, 4.0, 0.0], 1000, {}, NOW)
    dp = m["exponentialHistogram"]["dataPoints"][0]
    assert int(dp["count"]) == 1000
    assert sum(int(c) for c in dp["positive"]["bucketCounts"]) + int(dp["zeroCount"]) == 1000


def test_healthy_baseline():
    metrics = _emit(target_only=False)
    assert _error_pct(metrics) < 0.5
    lat = _hist(metrics, "request_latencies")
    ok = [dp for dp in lat if _attrs(dp)["response_code_class"] == "2xx"]
    assert all(_percentile(dp, 0.95) < 1000 for dp in ok)


def test_error_5xx_rate():
    metrics = _emit({"error_5xx_rate": 0.25})
    assert 20.0 < _error_pct(metrics) < 30.0
    codes = {a["response_code"] for a, _ in _gauges(metrics, "request_count")}
    assert {"500", "503"} <= codes


def test_latency_multiplier():
    healthy = [dp for dp in _hist(_emit(), "request_latencies")
               if _attrs(dp)["response_code_class"] == "2xx"]
    slow = [dp for dp in _hist(_emit({"latency_multiplier": 8.0}), "request_latencies")
            if _attrs(dp)["response_code_class"] == "2xx"]
    assert _percentile(slow[0], 0.95) > 4 * _percentile(healthy[0], 0.95)


def test_instance_count_spike():
    def active(ms):
        return sum(v for a, v in _gauges(ms, "container/instance_count") if a["state"] == "active")

    def billable(ms):
        return sum(v for _, v in _gauges(ms, "container/billable_instance_time"))

    healthy, spiked = _emit(), _emit({"instance_count_spike": 10.0})
    assert active(spiked) >= 8 * active(healthy)
    assert billable(spiked) > 5 * billable(healthy)


def test_non_target_service_unaffected():
    rng = random.Random(5)
    rev = [r for r in _revisions() if r["service_index"] == 1][0]
    metrics = generate_revision_metrics(rev, RevisionState(rng), rng,
                                        {"error_5xx_rate": 0.9}, now=NOW)
    assert _error_pct(metrics) < 1.0
