"""Technology-specific tests for the ``azure_billing`` generator."""

from __future__ import annotations

import datetime
import random
import time

import pytest

pytest.importorskip("dotenv")

import generators.azure_billing.generator as mod
from generators.azure_billing import SPEC

_PROFILE = {
    "azure_subscription_id": "sub-1",
    "azure_subscription_name": "Sub One",
    "azure_resource_group": "rg",
    "azure_openai_resources": ["openai"],
    "azure_openai_models": ["gpt-4o"],
    "azure_region": "eastus",
}


def _yesterday_utc(ts: float | None = None) -> datetime.date:
    ref = datetime.datetime.fromtimestamp(
        ts if ts is not None else time.time(), datetime.timezone.utc
    )
    return ref.date() - datetime.timedelta(days=1)


def _parse_ts(ts_str: str) -> float:
    return datetime.datetime.strptime(
        ts_str, "%Y-%m-%dT%H:%M:%S.%fZ"
    ).replace(tzinfo=datetime.timezone.utc).timestamp()


def test_usage_doc_matches_dashboard_fields():
    rng = random.Random(3)
    ts = time.time()
    day = _yesterday_utc(ts)
    doc = mod._build_usage_doc(ts, day, "banking", _PROFILE, rng, 25)
    billing = doc["azure"]["billing"]
    assert billing["currency"] == "USD"
    assert billing["product"]
    assert "pretax_cost" in billing
    assert "actual_cost" not in billing
    assert "forecast_cost" not in billing
    assert billing["usage_start"] == f"{day.isoformat()}T00:00:00.000Z"
    assert billing["usage_end"] == f"{day.isoformat()}T23:59:59.000Z"
    assert billing["usage_date"] == billing["usage_start"]
    # @timestamp stays at ingest time (data-view time filter).
    assert abs(_parse_ts(doc["@timestamp"]) - ts) < 1.0
    assert doc["azure"]["subscription_name"]
    assert doc["azure"]["resource"]["type"]
    assert doc["azure"]["resource"]["group"]


def test_usage_doc_timestamp_is_ingest_not_usage_day():
    rng = random.Random(2)
    ingest = time.time()
    day = datetime.date(2026, 8, 1)
    doc = mod._build_usage_doc(ingest, day, "banking", _PROFILE, rng, 25)
    assert abs(_parse_ts(doc["@timestamp"]) - ingest) < 1.0
    assert doc["azure"]["billing"]["usage_start"].startswith("2026-08-01T00:00:00")


def test_forecast_doc_actual_vs_forecast_split():
    ingest = time.time()
    past = datetime.date(2026, 8, 9)
    future = datetime.date(2026, 8, 12)
    actual = mod._build_forecast_doc(
        ingest, past, "banking", _PROFILE,
        subscription_id="sub-1", kind="actual", cost=12.5,
    )
    forecast = mod._build_forecast_doc(
        ingest, future, "banking", _PROFILE,
        subscription_id="sub-1", kind="forecast", cost=18.0,
    )
    ab = actual["azure"]["billing"]
    fb = forecast["azure"]["billing"]
    assert ab == {
        "currency": "USD",
        "usage_date": "2026-08-09T00:00:00.000Z",
        "actual_cost": 12.5,
    }
    assert fb == {
        "currency": "USD",
        "usage_date": "2026-08-12T00:00:00.000Z",
        "forecast_cost": 18.0,
    }
    assert "pretax_cost" not in ab
    assert "usage_start" not in ab
    assert actual["azure"]["subscription_id"] == "sub-1"
    assert "resource" not in actual["azure"]


def test_billing_batch_covers_history_and_forecast_window():
    rng = random.Random(7)
    # Fixed mid-day so "today" is unambiguous.
    ingest = datetime.datetime(2026, 8, 11, 16, 0, 0, tzinfo=datetime.timezone.utc).timestamp()
    docs = mod._build_billing_batch(ingest, "banking", _PROFILE, rng, load=1.0)

    usage = [d for d in docs if "pretax_cost" in d["azure"]["billing"]]
    forecastish = [d for d in docs if "pretax_cost" not in d["azure"]["billing"]]
    assert usage
    assert forecastish

    usage_starts = {d["azure"]["billing"]["usage_start"] for d in usage}
    # Last 14 complete UTC days: 2026-07-28 .. 2026-08-10
    expected_usage_days = {
        (datetime.date(2026, 8, 11) - datetime.timedelta(days=i)).isoformat()
        + "T00:00:00.000Z"
        for i in range(1, 15)
    }
    assert usage_starts == expected_usage_days
    for d in usage:
        b = d["azure"]["billing"]
        assert "actual_cost" not in b
        assert "forecast_cost" not in b
        assert b["usage_end"].endswith("T23:59:59.000Z")

    today = datetime.date(2026, 8, 11)
    for d in forecastish:
        b = d["azure"]["billing"]
        day = datetime.date.fromisoformat(b["usage_date"][:10])
        assert b["usage_date"].endswith("T00:00:00.000Z")
        if day < today:
            assert "actual_cost" in b
            assert "forecast_cost" not in b
        else:
            assert "forecast_cost" in b
            assert "actual_cost" not in b

    forecast_dates = {d["azure"]["billing"]["usage_date"][:10] for d in forecastish}
    # azure_billing@1.13.0 forecasts over a 720h (30 day) window by default.
    expected_forecast = {
        (today + datetime.timedelta(days=i)).isoformat()
        for i in range(-2, 31)
    }
    assert forecast_dates == expected_forecast


def test_thin_profile_enriches_breakdown_dimensions():
    """Pies must not collapse to a single 100% slice on thin llm_profiles."""
    rng = random.Random(11)
    day = _yesterday_utc()
    docs = [
        mod._build_usage_doc(time.time(), day, "banking", _PROFILE, rng, 25)
        for _ in range(80)
    ]
    groups = {d["azure"]["resource"]["group"] for d in docs}
    subs = {d["azure"]["subscription_name"] for d in docs}
    resources = {d["azure"]["resource"]["name"] for d in docs}
    products = {d["azure"]["billing"]["product"] for d in docs}
    types = {d["azure"]["resource"]["type"] for d in docs}
    assert len(groups) >= 3
    assert len(subs) >= 2
    assert len(resources) >= 3
    assert len(products) >= 3
    assert "Microsoft.CognitiveServices" in types
    assert len(types) >= 2


def test_openai_lines_dominate_for_openai_billing_dashboard():
    rng = random.Random(5)
    day = _yesterday_utc()
    docs = [
        mod._build_usage_doc(time.time(), day, "banking", _PROFILE, rng, 25)
        for _ in range(100)
    ]
    cog = sum(
        1 for d in docs
        if d["azure"]["resource"]["type"] == "Microsoft.CognitiveServices"
    )
    assert cog >= 40


def test_healthy_baseline_no_chaos():
    rng = random.Random(1)
    doc = mod._build_usage_doc(
        time.time(), _yesterday_utc(), "banking", _PROFILE, rng, 25
    )
    billing = doc["azure"]["billing"]
    assert billing["pretax_cost"] > 0
    assert billing["usage_quantity"] > 0
    assert "actual_cost" not in billing


def test_cost_and_usage_multipliers_applied():
    rng = random.Random(1)
    # Force identical line selection by passing an explicit line.
    line = {
        "resource_type": "Microsoft.CognitiveServices",
        "service_name": "Azure OpenAI",
        "product": "Azure OpenAI - gpt-4o",
        "meter_name": "gpt-4o tokens",
        "unit": "1K Tokens",
        "resource_name": "openai",
        "cost": 1.0,
        "usage_qty": 1000.0,
    }
    day = _yesterday_utc()
    baseline = mod._build_usage_doc(
        time.time(), day, "banking", _PROFILE, rng, 25, line=line,
    )
    rng = random.Random(1)
    chaos = mod._build_usage_doc(
        time.time(), day, "banking", _PROFILE, rng, 25,
        cost_multiplier=5.0, usage_multiplier=4.0, line=line,
    )
    assert chaos["azure"]["billing"]["pretax_cost"] == pytest.approx(5.0)
    assert chaos["azure"]["billing"]["usage_quantity"] == pytest.approx(4000.0)
    assert baseline["azure"]["billing"]["pretax_cost"] == pytest.approx(1.0)


def test_build_billing_doc_alias_defaults_to_yesterday():
    rng = random.Random(4)
    ts = datetime.datetime(2026, 8, 11, 16, 30, 0, tzinfo=datetime.timezone.utc).timestamp()
    doc = mod._build_billing_doc(ts, "banking", _PROFILE, rng, 25)
    assert doc["azure"]["billing"]["usage_start"] == "2026-08-10T00:00:00.000Z"
    assert "forecast_cost" not in doc["azure"]["billing"]


def test_spec_wants_chaos_and_impact_keys():
    assert SPEC.wants_chaos is True
    assert SPEC.chaos_mode == "explicit"
    assert sorted(SPEC.impact_keys) == ["cost_multiplier", "usage_multiplier"]


def test_emit_namespaces_includes_ootb_default():
    from generator_engine.es_doc_common import emit_namespaces

    assert emit_namespaces("default") == ["default"]
    assert emit_namespaces("xborder") == ["xborder", "default"]


def test_docs_for_namespace_rewrites_data_stream():
    from generator_engine.es_doc_common import docs_for_namespace

    docs = [{"data_stream": {"type": "metrics", "dataset": "azure.billing", "namespace": "xborder"}, "v": 1}]
    out = docs_for_namespace(docs, "default")
    assert out[0]["data_stream"]["namespace"] == "default"
    assert docs[0]["data_stream"]["namespace"] == "xborder"


def test_send_dual_bulk_writes_both_namespaces():
    from generator_engine.es_doc_common import send_dual_bulk

    sent = []

    class _Client:
        def send_bulk(self, index, docs, params=None):
            sent.append((index, docs[0]["data_stream"]["namespace"], params))

    send_dual_bulk(
        _Client(),
        "metrics-azure.billing",
        "xborder",
        [{"data_stream": {"namespace": "xborder"}, "v": 1}],
    )
    assert sent[0][0] == "metrics-azure.billing-xborder"
    assert sent[1][0] == "metrics-azure.billing-default"
    assert sent[1][1] == "default"
    assert sent[0][2] == {"pipeline": "_none"}
