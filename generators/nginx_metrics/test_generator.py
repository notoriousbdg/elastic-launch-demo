"""Technology-specific tests for the ``nginx_metrics`` generator."""

from __future__ import annotations

import random

from generators.nginx_metrics import SPEC
from generators.nginx_metrics.generator import NginxState, _generate_metrics


def _gauge(metrics, name, attr_key=None, attr_val=None) -> float:
    for m in metrics:
        if m["name"] != name:
            continue
        dp = m["gauge"]["dataPoints"][0]
        if attr_key is not None:
            attrs = {a["key"]: next(iter(a["value"].values())) for a in dp.get("attributes", [])}
            if attrs.get(attr_key) != attr_val:
                continue
        return float(dp.get("asDouble", dp.get("asInt")))
    raise AssertionError(f"missing {name} {attr_key}={attr_val}")


def _counter(metrics, name) -> int:
    for m in metrics:
        if m["name"] == name:
            return int(m["sum"]["dataPoints"][0]["asInt"])
    raise AssertionError(f"missing counter {name}")


def test_healthy_baseline_no_chaos():
    rng = random.Random(1)
    state = NginxState(rng)
    assert state.connections_handled == state.connections_accepted
    before = state.requests
    metrics, snap = _generate_metrics(state, rng, active_chaos=None)
    active = _gauge(metrics, "nginx.connections_current", "state", "active")
    assert 50 <= active <= 300
    assert _counter(metrics, "nginx.requests") > before
    assert state.connections_handled == state.connections_accepted
    assert _counter(metrics, "nginx.connections_accepted") == _counter(
        metrics, "nginx.connections_handled"
    )
    assert snap["requests"] == state.requests
    assert snap["active"] == active


def test_handled_equals_accepted_under_chaos():
    """No chaos key can open an accepted/handled gap; the drop alert is nginx_otel's."""
    rng = random.Random(1)
    state = NginxState(rng)
    for _ in range(20):
        _generate_metrics(
            state, rng, active_chaos={"request_rate_multiplier": 3.0, "active_connections": 900}
        )
        assert state.connections_handled == state.connections_accepted


def test_active_connections_override():
    rng = random.Random(1)
    state = NginxState(rng)
    metrics, _snap = _generate_metrics(state, rng, active_chaos={"active_connections": 900})
    active = _gauge(metrics, "nginx.connections_current", "state", "active")
    assert active == 900


def test_request_rate_multiplier_boosts_requests():
    rng = random.Random(1)
    state = NginxState(rng)
    before = state.requests
    _generate_metrics(state, rng, active_chaos={"request_rate_multiplier": 10.0})
    delta_chaos = state.requests - before

    rng = random.Random(1)
    state2 = NginxState(rng)
    before2 = state2.requests
    _generate_metrics(state2, rng, active_chaos=None)
    delta_base = state2.requests - before2

    assert delta_chaos > delta_base * 3


def test_bulk_docs_cover_overview_metrics():
    from generators.nginx_metrics.generator import build_nginxreceiver_bulk_docs

    host = {
        "host.name": "xborder-nginx-01",
        "service.name": "nginx-proxy",
        "service.instance.id": "nginx-proxy-001",
        "cloud.provider": "aws",
        "cloud.platform": "aws_ec2",
        "cloud.region": "us-east-1",
    }
    docs = build_nginxreceiver_bulk_docs(
        host,
        requests=100,
        accepted=50,
        handled=50,
        active=80,
        reading=10,
        writing=20,
        waiting=30,
        timestamp_ms=1_700_000_000_000,
    )
    counter_doc = next(d for d in docs if "nginx.requests" in d["metrics"])
    assert counter_doc["metrics"]["nginx.connections_accepted"] == 50
    assert counter_doc["metrics"]["nginx.connections_handled"] == 50
    states = {
        d["attributes"]["state"]
        for d in docs
        if "nginx.connections_current" in d["metrics"]
    }
    assert states == {"active", "reading", "writing", "waiting"}
    assert len(docs) == 5


def test_spec_wants_chaos_and_impact_keys():
    assert SPEC.wants_chaos is True
    assert sorted(SPEC.impact_keys) == ["active_connections", "request_rate_multiplier"]
