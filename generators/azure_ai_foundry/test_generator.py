"""azure_ai_foundry field-shape and chaos tests."""

from __future__ import annotations

import random

import pytest

from generators.azure_ai_foundry.generator import (
    DOC_FIELDS,
    SOURCE_INTEGRATION,
    _gateway_log,
    build_log_docs,
    build_metric_docs,
)

_PROFILE = {
    "azure_subscription_id": "12cabcb4-86e8-404f-a3d2-1dc9982f45ca",
    "azure_region": "eastus2",
    "azure_resource_group": "rg-demo",
    "prompt_templates": ["Suggest a book."],
    "response_templates": ["Try Project Hail Mary."],
}
_CFG = {
    "azure_region": "eastus2",
    "model_deployment_name": "gpt-4o-demo",
    "resource_name": "ai-foundry-demo",
}


def test_source_citation():
    assert "azure_ai_foundry@0.13.0" in SOURCE_INTEGRATION


def test_metric_docs_include_error_status_codes():
    docs = build_metric_docs(1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(1), None)
    statuses = {d["azure"]["dimensions"]["status_code"] for d in docs}
    assert "200" in statuses
    assert "500" in statuses
    assert "429" in statuses
    assert all(d["azure"].get("subscription_name") for d in docs)
    assert all(d["azure"]["dimensions"].get("fingerprint") for d in docs)
    assert DOC_FIELDS


def test_healthy_baseline_no_chaos():
    docs = build_metric_docs(1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(1), None)
    ok = next(d for d in docs if d["azure"]["dimensions"]["status_code"] == "200")
    assert ok["azure"]["ai_foundry"]["time_to_response"]["avg"] <= 5000
    assert ok["azure"]["ai_foundry"]["model_availability_rate"]["avg"] * 100 >= 99
    assert ok["azure"]["ai_foundry"]["provisioned_utilization"]["avg"] * 100 <= 85


def test_latency_multiplier_alone():
    docs = build_metric_docs(
        1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(2),
        {"latency_high": 10000.0},
    )
    ok = next(d for d in docs if d["azure"]["dimensions"]["status_code"] == "200")
    assert ok["azure"]["ai_foundry"]["time_to_response"]["avg"] > 5000
    assert ok["azure"]["ai_foundry"]["provisioned_utilization"]["avg"] * 100 <= 85


def test_availability_rate_alone():
    docs = build_metric_docs(
        1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(3),
        {"model_availability_above_threshold": 90.0},
    )
    ok = next(d for d in docs if d["azure"]["dimensions"]["status_code"] == "200")
    assert ok["azure"]["ai_foundry"]["model_availability_rate"]["avg"] * 100 < 99
    assert ok["azure"]["ai_foundry"]["time_to_response"]["avg"] <= 5000


def test_provisioned_utilization_alone():
    docs = build_metric_docs(
        1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(4),
        {"provisioned_utilization_above_threshold": 92.0},
    )
    ok = next(d for d in docs if d["azure"]["dimensions"]["status_code"] == "200")
    assert ok["azure"]["ai_foundry"]["provisioned_utilization"]["avg"] * 100 > 85
    assert ok["azure"]["ai_foundry"]["time_to_response"]["avg"] <= 5000


def test_gateway_logs_match_apim_and_guardrails_shape():
    docs = build_log_docs(
        1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(3), {"content_filter_rate": 1.0}
    )
    cats = {d["azure"]["ai_foundry"]["category"] for d in docs}
    assert cats == {"Audit", "RequestResponse", "GatewayLogs"}
    gateways = [d for d in docs if d["azure"]["ai_foundry"]["category"] == "GatewayLogs"]
    assert len(gateways) >= 4
    # Every log carries Guardrails control fields.
    for d in docs:
        assert d["azure"]["subscription_id"]
        assert d["azure"]["resource"]["group"]
        assert d["azure"]["resource"]["name"]

    filtered = [
        d
        for d in gateways
        if d["azure"]["ai_foundry"]["properties"]["backend_response_body"]
        .get("choices", [{}])[0]
        .get("finish_reason")
        == "content_filter"
    ]
    assert filtered
    gw = filtered[0]
    props = gw["azure"]["ai_foundry"]["properties"]
    assert props["backend_request_body"]["model"] == "gpt-4o-demo"
    assert props["backend_request_body"]["messages"][0]["content"]
    assert "usage" in props["backend_response_body"]
    assert props["backend_response_body"]["error"]["code"] == "content_filter"
    assert gw["azure"]["ai_foundry"]["is_request_success"] is False
    assert gw["azure"]["ai_foundry"]["sku"]
    assert gw["event"]["outcome"] == "failure"
    assert "lat" in gw["source"]["geo"]["location"]
    assert gw["http"]["response"]["status_code"] == 400
    assert gw["url"]["path"]
    assert gw["azure"]["resource"]["id"]

    jailbroken = [
        d
        for d in gateways
        if d["azure"]["ai_foundry"]["properties"]["backend_response_body"]
        .get("error", {})
        .get("innererror", {})
        .get("content_filter_result", {})
        .get("jailbreak", {})
        .get("detected")
        is True
    ]
    assert jailbroken
    protected = [
        d
        for d in gateways
        if d["azure"]["ai_foundry"]["properties"]["backend_response_body"]
        .get("error", {})
        .get("innererror", {})
        .get("content_filter_result", {})
        .get("protected_material_text", {})
        .get("detected")
        is True
    ]
    assert protected


def test_gateway_success_path_has_backend_bodies():
    # Seed that likely yields success given baseline rate; retry until success.
    found = None
    for seed in range(50):
        docs = build_log_docs(1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(seed), None)
        gw = next(d for d in docs if d["azure"]["ai_foundry"]["category"] == "GatewayLogs")
        if gw["azure"]["ai_foundry"]["is_request_success"]:
            found = gw
            break
    assert found is not None
    props = found["azure"]["ai_foundry"]["properties"]
    assert props["backend_response_body"]["choices"][0]["message"]["content"]
    assert props["backend_response_body"]["usage"]["total_tokens"] > 0
    assert found["event"]["outcome"] == "success"


# ── package alerts, evaluated on one emitted metric cycle ─────────────────────
# azure_ai_foundry@0.13.0: latency MAX(time_to_response.avg) > 5000 ms;
# availability MIN(model_availability_rate.avg) * 100 < 99; provisioned
# utilization MAX(provisioned_utilization.avg) * 100 > 85.

GT_VALUES = {  # scenarios_testing/gt-azureaifoundry/channels
    "latency_high": 37500.0,
    "model_availability_above_threshold": 100.0,
    "provisioned_utilization_above_threshold": 637.0,
}


def _breached(chaos: dict | None, seed: int) -> set[str]:
    docs = build_metric_docs(1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(seed), chaos)
    metrics = [d["azure"]["ai_foundry"] for d in docs]
    out = set()
    if max(m["time_to_response"]["avg"] for m in metrics) > 5000:
        out.add("latency_high")
    if min(m["model_availability_rate"]["avg"] for m in metrics) * 100 < 99:
        out.add("model_availability_above_threshold")
    if max(m["provisioned_utilization"]["avg"] for m in metrics) * 100 > 85:
        out.add("provisioned_utilization_above_threshold")
    return out


def _availability(chaos: dict | None, seed: int = 1) -> float:
    docs = build_metric_docs(1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(seed), chaos)
    return min(d["azure"]["ai_foundry"]["model_availability_rate"]["avg"] for d in docs)


def test_healthy_cycle_breaches_no_alert():
    for seed in range(50):
        assert _breached(None, seed) == set(), seed


@pytest.mark.parametrize("key", sorted(GT_VALUES))
def test_fault_alone_breaches_only_its_own_alert(key):
    for seed in range(20):
        assert _breached({key: GT_VALUES[key]}, seed) == {key}, (key, seed)


@pytest.mark.parametrize("value", [0.0, 1.0, 500.0, 989.0])
def test_availability_key_below_990_breaches_including_zero(value):
    """0 is the worst availability in the key's [0, 990] range; it must not read as healthy."""
    assert _breached({"model_availability_above_threshold": value}, 1) == {
        "model_availability_above_threshold"
    }


def test_availability_key_unit_is_tenths_of_a_percent():
    assert _availability({"model_availability_above_threshold": 0.0}) == 0.0
    assert _availability({"model_availability_above_threshold": 500.0}) == 0.5
    assert _availability({"model_availability_above_threshold": 990.0}) >= 0.99  # healthy


# ── gateway logs follow the declared keys ─────────────────────────────────────

def _gateway_duration_ms(chaos: dict | None, seed: int) -> float:
    doc = _gateway_log(
        1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(seed), chaos, mode="success",
    )
    return doc["event"]["duration"] / 1_000_000


def test_gateway_log_latency_follows_latency_high():
    for seed in range(20):
        assert _gateway_duration_ms({"latency_high": 10000.0}, seed) >= 5001
        assert _gateway_duration_ms(None, seed) < 5000


def test_gateway_log_ignores_the_undeclared_latency_multiplier():
    for seed in range(20):
        assert _gateway_duration_ms({"latency_multiplier": 5.0}, seed) < 5000


def _filtered_share(chaos: dict | None, n: int = 400) -> float:
    rng = random.Random(11)
    filtered = 0
    for _ in range(n):
        doc = _gateway_log(1_700_000_000.0, "demo", _PROFILE, _CFG, rng, chaos)
        body = doc["azure"]["ai_foundry"]["properties"].get("backend_response_body") or {}
        if (body.get("choices") or [{}])[0].get("finish_reason") == "content_filter":
            filtered += 1
    return filtered / n


def test_content_filter_share_is_the_baseline_and_ignores_the_undeclared_key():
    baseline = _filtered_share(None)
    assert 0.15 < baseline < 0.35
    assert _filtered_share({"content_filter_rate": 1.0}) == baseline

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.azure_ai_foundry import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['latency_high', 'model_availability_above_threshold', 'provisioned_utilization_above_threshold']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


def test_emit_namespaces_includes_ootb_default():
    from generator_engine.es_doc_common import emit_namespaces

    assert emit_namespaces("default") == ["default"]
    assert emit_namespaces("xborder") == ["xborder", "default"]
