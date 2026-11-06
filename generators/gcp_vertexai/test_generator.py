"""Technology-specific tests for the ``gcp_vertexai`` generator."""
from __future__ import annotations

import random

from generators.gcp_vertexai import SPEC
from generators.gcp_vertexai.generator import (
    _build_audit_doc,
    _build_metrics_doc,
    _callers_for_profile,
    _pick_region,
    _pick_response_code,
    _regions_for_profile,
)


def test_healthy_baseline_response_codes():
    rng = random.Random(1)
    codes = [_pick_response_code(rng) for _ in range(200)]
    assert codes.count("200") > 150


def test_error_rate_biases_5xx():
    rng = random.Random(1)
    chaos = {"error_rate": 0.9, "throttle_rate": 0.0}
    codes = [_pick_response_code(rng, chaos) for _ in range(300)]
    errors = sum(1 for c in codes if c.startswith("5"))
    assert errors > 150


def test_throttle_rate_biases_429():
    rng = random.Random(1)
    chaos = {"error_rate": 0.0, "throttle_rate": 0.8}
    codes = [_pick_response_code(rng, chaos) for _ in range(300)]
    assert codes.count("429") > 150


def test_metrics_error_count_rises_with_error_rate():
    rng = random.Random(2)
    profile = {"vertex_models": ["gemini-2.5-flash"], "vertex_endpoints": ["ep-1"]}
    healthy = _build_metrics_doc(1.0, "ns", profile, rng, 20, None)
    faulted = _build_metrics_doc(1.0, "ns", profile, rng, 20, {"error_rate": 0.9})
    healthy_err = healthy["gcp"]["vertexai"]["prediction"]["online"]["error_count"]
    faulted_err = faulted["gcp"]["vertexai"]["prediction"]["online"]["error_count"]
    assert faulted_err >= healthy_err


def test_audit_logs_fail_under_error_rate():
    rng = random.Random(3)
    profile = {"vertex_models": ["gemini-2.5-flash"]}
    outcomes = [
        _build_audit_doc(1.0, "ns", profile, rng, {"error_rate": 0.95})["event"]["outcome"]
        for _ in range(100)
    ]
    assert outcomes.count("failure") > 70


def test_chaos_off_audit_logs_mostly_success():
    rng = random.Random(3)
    profile = {"vertex_models": ["gemini-2.5-flash"]}
    outcomes = [
        _build_audit_doc(1.0, "ns", profile, rng, None)["event"]["outcome"]
        for _ in range(100)
    ]
    assert outcomes.count("success") == 100


def test_single_gcp_region_expands_to_global_mix():
    """Thin llm_profile gcp_region must not pin every doc to one region."""
    profile = {"gcp_region": "us-central1"}
    regions = _regions_for_profile(profile)
    assert len(regions) >= 10
    assert "us-central1" in regions
    assert "europe-west1" in regions
    assert "asia-southeast1" in regions
    assert "me-central1" in regions

    rng = random.Random(11)
    seen = {_pick_region(profile, rng) for _ in range(80)}
    assert len(seen) >= 6


def test_thin_callers_expand_for_global_map():
    profile = {
        "callers": [
            {"email": "a@x", "country": "US", "city": "New York",
             "region": "New York", "lat": 40.7, "lon": -74.0},
            {"email": "b@x", "country": "US", "city": "Chicago",
             "region": "Illinois", "lat": 41.8, "lon": -87.6},
        ]
    }
    callers = _callers_for_profile(profile, "inttest")
    countries = {c["country"] for c in callers}
    assert len(callers) >= 10
    assert countries >= {"US", "GB", "SG", "JP", "BR"}


def test_audit_docs_span_many_geo_regions():
    rng = random.Random(5)
    profile = {
        "gcp_region": "us-central1",
        "vertex_models": ["gemini-2.5-flash"],
        "callers": [
            {"email": "a@x", "country": "US", "city": "New York",
             "region": "New York", "lat": 40.7, "lon": -74.0},
            {"email": "b@x", "country": "US", "city": "Chicago",
             "region": "Illinois", "lat": 41.8, "lon": -87.6},
        ],
    }
    docs = [_build_audit_doc(1.0, "inttest", profile, rng, None) for _ in range(60)]
    cloud_regions = {d["cloud"]["region"] for d in docs}
    geo_regions = {d["source"]["geo"]["region_name"] for d in docs}
    assert len(cloud_regions) >= 6
    assert len(geo_regions) >= 6


def test_impact_keys_declared():
    assert sorted(SPEC.impact_keys) == ["error_rate", "latency_multiplier", "throttle_rate"]


def test_emit_namespaces_includes_ootb_default():
    from generator_engine.es_doc_common import emit_namespaces

    assert emit_namespaces("default") == ["default"]
    assert emit_namespaces("xborder") == ["xborder", "default"]


def test_docs_for_namespace_rewrites_data_stream():
    from generator_engine.es_doc_common import docs_for_namespace

    docs = [{"data_stream": {"type": "metrics", "dataset": "gcp.vertexai", "namespace": "xborder"}, "v": 1}]
    out = docs_for_namespace(docs, "default")
    assert out[0]["data_stream"]["namespace"] == "default"
    assert docs[0]["data_stream"]["namespace"] == "xborder"
    assert out[0]["v"] == 1
