"""Technology-specific tests for the ``azure_openai`` generator.

Add tests here only for behaviour that is special to azure_openai.
Shared shape / registry / chaos tests live in tests/test_llm_generators.py.
"""

from __future__ import annotations

import random
import time

import pytest

pytest.importorskip("dotenv")

import generators.azure_openai.generator as mod


def test_request_response_queryable_by_package_filters():
    """Overview dashboard filters on azure.open_ai.category / result_signature."""
    rng = random.Random(1)
    profile = {
        "azure_subscription_id": "sub-1",
        "azure_subscription_name": "Sub One",
        "azure_resource_group": "rg",
        "azure_openai_resources": ["openai"],
        "azure_openai_models": ["gpt-4o"],
        "azure_region": "eastus",
        "prompt_templates": ["hi"],
        "response_templates": ["hello"],
    }
    doc = mod._build_log_doc(time.time(), "banking", profile, rng, None, category="RequestResponse")
    assert doc["azure"]["open_ai"]["category"] == "RequestResponse"
    assert doc["data_stream"]["dataset"] == "azure_openai.logs"


def test_content_filter_emits_custom_blocklists_id():
    """Content Filter search column custom_blocklists.id must be non-null."""
    rng = random.Random(11)
    profile = {
        "azure_subscription_id": "sub-1",
        "azure_subscription_name": "Sub One",
        "azure_resource_group": "rg",
        "azure_openai_resources": ["openai"],
        "azure_openai_models": ["gpt-4o"],
        "azure_region": "eastus",
        "prompt_templates": ["hi"],
        "response_templates": ["hello"],
    }
    found = False
    for _ in range(1000):  # baseline filter rate is 3%
        doc = mod._build_log_doc(
            time.time(), "banking", profile, rng,
            None,
            category="ApiManagementGatewayLogs",
        )
        body = doc["azure"]["open_ai"]["properties"]["backend_response_body"]
        if body["choices"][0]["finish_reason"] != "content_filter":
            continue
        cf = body["error"]["innererror"]["content_filter_result"]["custom_blocklists"]
        assert cf["id"]
        assert "filtered" in cf
        assert body["choices"][0]["content_filter_results"]["custom_blocklists"]["id"]
        found = True
        break
    assert found


def test_metrics_have_dimension_breakdown_fields():
    rng = random.Random(2)
    profile = {
        "azure_subscription_id": "sub-1",
        "azure_subscription_name": "Sub One",
        "azure_resource_group": "rg",
        "azure_openai_resources": ["openai"],
        "azure_openai_models": ["gpt-4.1-mini"],
        "azure_region": "eastus",
    }
    doc = mod._build_metrics_doc(time.time(), "banking", profile, rng, 10, None)
    assert doc["azure"]["dimensions"]["model_deployment_name"]
    assert doc["azure"]["open_ai"]["token_transaction"]["total"] > 0


def test_thin_profile_enriches_model_and_resource_variety():
    rng = random.Random(9)
    profile = {
        "azure_subscription_id": "sub-1",
        "azure_subscription_name": "Sub One",
        "azure_resource_group": "rg",
        "azure_openai_resources": ["openai"],
        "azure_openai_models": ["gpt-4o"],
        "azure_region": "eastus",
    }
    docs = [
        mod._build_metrics_doc(time.time(), "banking", profile, rng, 10, None)
        for _ in range(60)
    ]
    models = {d["azure"]["dimensions"]["model_deployment_name"] for d in docs}
    resources = {d["azure"]["resource"]["name"] for d in docs}
    groups = {d["azure"]["resource"]["group"] for d in docs}
    assert len(models) >= 3
    assert len(resources) >= 2
    assert len(groups) >= 2


_PROFILE = {
    "azure_subscription_id": "sub-1",
    "azure_subscription_name": "Sub One",
    "azure_resource_group": "rg",
    "azure_openai_resources": ["openai"],
    "azure_openai_models": ["gpt-4o"],
    "azure_region": "eastus",
    "prompt_templates": ["hi"],
    "response_templates": ["hello"],
}


def test_healthy_metrics_baseline():
    doc = mod._build_metrics_doc(time.time(), "banking", _PROFILE, random.Random(1), 10, None)
    assert doc["azure"]["open_ai"]["time_to_response"]["avg"] <= 5000
    assert doc["azure"]["open_ai"]["provisioned_managed_utilization_v2"]["avg"] * 100 <= 85


def test_emit_namespaces_includes_ootb_default():
    from generator_engine.es_doc_common import emit_namespaces

    assert emit_namespaces("default") == ["default"]
    assert emit_namespaces("gtazureopenai") == ["gtazureopenai", "default"]


def test_run_dual_writes_scenario_and_default_streams(monkeypatch):
    """OOTB alerts FROM *-default; scenario ns alone is not enough."""
    monkeypatch.setattr(mod, "ensure_dashboard_mappings_from_env", lambda *_a, **_k: None)
    monkeypatch.setattr(
        mod,
        "resolve_active_chaos",
        lambda **_k: {},
    )
    monkeypatch.setattr(mod, "business_load", lambda: 1.0)

    sent: list[tuple[str, int]] = []

    class _Client:
        configured = True

        def send_bulk(self, index, docs, params=None):
            sent.append((index, len(docs)))

    class _Stop:
        def __init__(self):
            self._n = 0

        def is_set(self):
            return self._n > 0

        def wait(self, _interval):
            self._n += 1

    class _Scenario:
        llm_profile = _PROFILE
        channel_registry = {}
        namespace = "gtazureopenai"

    mod.run(
        _Client(),
        _Stop(),
        scenario_data={"scenario": _Scenario(), "namespace": "gtazureopenai"},
    )
    indexes = [i for i, _ in sent]
    assert "logs-azure_openai.logs-gtazureopenai" in indexes
    assert "metrics-azure.open_ai-gtazureopenai" in indexes
    assert "logs-azure_openai.logs-default" in indexes
    assert "metrics-azure.open_ai-default" in indexes


def test_ptu_utilization_target_alone():
    doc = mod._build_metrics_doc(
        time.time(), "banking", _PROFILE, random.Random(2), 10,
        {"provisioned_utilization_above_threshold": 95.0},
    )
    assert doc["azure"]["open_ai"]["provisioned_managed_utilization_v2"]["avg"] * 100 > 85
    assert doc["azure"]["open_ai"]["time_to_response"]["avg"] <= 5000


def test_latency_multiplier_alone():
    doc = mod._build_metrics_doc(
        time.time(), "banking", _PROFILE, random.Random(3), 10,
        {"latency_high": 5001.0},
    )
    assert doc["azure"]["open_ai"]["time_to_response"]["avg"] > 5000
    assert doc["azure"]["open_ai"]["provisioned_managed_utilization_v2"]["avg"] * 100 <= 85


def test_quota_error_rate_alone():
    found = False
    rng = random.Random(4)
    for _ in range(20):
        doc = mod._build_log_doc(
            time.time(), "banking", _PROFILE, rng,
            {"quota_error_rates_above_threshold": 1.0},
            category="RequestResponse",
        )
        if doc["http"]["response"]["status_code"] == 429:
            assert doc["azure"]["dimensions"]["model_deployment_name"]
            found = True
            break
    assert found


def _filtered_count(chaos, n: int = 2000) -> int:
    rng = random.Random(5)
    filtered = 0
    for _ in range(n):
        doc = mod._build_log_doc(
            time.time(), "banking", _PROFILE, rng, chaos, category="ApiManagementGatewayLogs",
        )
        body = doc["azure"]["open_ai"]["properties"]["backend_response_body"]
        if body["choices"][0]["finish_reason"] == "content_filter":
            filtered += 1
    return filtered


def test_content_filtering_is_baseline_only():
    """content_filter_rate is not a declared impact key, so a channel can never set it."""
    baseline = _filtered_count(None)
    assert 0.01 < baseline / 2000 < 0.06  # _BASELINE_CONTENT_FILTER_RATE = 0.03
    assert _filtered_count({"content_filter_rate": 1.0}) == baseline

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.azure_openai import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['latency_high', 'provisioned_utilization_above_threshold', 'quota_error_rates_above_threshold']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
