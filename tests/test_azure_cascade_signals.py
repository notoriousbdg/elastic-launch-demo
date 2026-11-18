"""Cascade services show their own measurable symptom: channels 13, 14 and 15.

A dependent that only logs a cascade line gives Nightshift nothing to confirm recovery with, so
retrieval-service (13), model-router (14, 15) report a metric that is baseline before the fault,
out of band once the service's lag has passed, and converges back after the fix.
"""
from __future__ import annotations

import pytest

from tests._azure_harness import REG, phase, records, values

CASES = [
    # channel, service, metric, baseline check, during check, recovery seconds
    (13, "retrieval-service", "retrieval.index_freshness_minutes", lambda x: x <= 18, lambda x: 90 <= x <= 480, 150),
    (14, "model-router", "model_router.premium_traffic_share_pct", lambda x: 25 <= x <= 40, lambda x: 70 <= x <= 95, 90),
    (15, "model-router", "model_router.calls_per_turn_p95", lambda x: 2 <= x <= 4, lambda x: 25 <= x <= 120, 45),
]


def _vals(ch, which, svc, metric, **kw):
    _, metrics = records(phase(ch, which, **kw), cycles=8)
    v = values(metrics, svc, metric)
    assert v, f"{svc} emitted no {metric}"
    return v


@pytest.mark.parametrize("ch,svc,metric,base,during,recovery", CASES)
def test_signal_phases(ch, svc, metric, base, during, recovery):
    assert all(base(x) for x in _vals(ch, "before", svc, metric))
    lag = REG[ch]["service_lag_seconds"][svc]
    assert all(base(x) for x in _vals(ch, "during", svc, metric, elapsed=max(1.0, lag / 3))), "moved before its lag"
    steady = _vals(ch, "during", svc, metric, elapsed=lag + 300)
    assert all(during(x) for x in steady), steady
    assert not any(base(x) for x in _vals(ch, "after", svc, metric, since=5)), "snapped back at the fix"
    assert all(base(x) for x in _vals(ch, "after", svc, metric, since=recovery + 15))


@pytest.mark.parametrize("ch,svc,metric,base,during,recovery", CASES)
def test_verification_names_the_signal_and_covers_recovery(ch, svc, metric, base, during, recovery):
    entries = [v for v in REG[ch]["remediation_effects"]["verification"] if metric in str(v.get("field"))]
    assert entries, f"ch{ch}: no verification entry for {metric}"
    assert all(e["within_seconds"] >= recovery for e in entries)


def test_router_status_follows_the_load_signals():
    logs, _ = records(phase(15, "during", elapsed=300), cycles=6)
    status = {r["attrs"].get("router.status") for r in logs
              if r["service"] == "model-router" and "router_load" in r["body"]}
    assert status == {"ANOMALY"}
    logs, _ = records(phase(15, "before"), cycles=6)
    status = {r["attrs"].get("router.status") for r in logs
              if r["service"] == "model-router" and "router_load" in r["body"]}
    assert status == {"NOMINAL"}
