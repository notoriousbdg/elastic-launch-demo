"""Azure GenAI Assistant: dependents degrade after the component at fault (service_lag_seconds).

For every channel that lags a service, the service shows no fault or cascade lines right after
the trigger and shows them once its lag has passed, while the service that owns the cause fails
immediately. Metrics of a lagged dependent follow the same lag.
"""
from __future__ import annotations

import pytest

from tests._azure_harness import REG, SCN, phase, records, values

LAGGED = [(ch, svc, secs) for ch, d in sorted(REG.items())
          for svc, secs in (d.get("service_lag_seconds") or {}).items()]


def _emits_fault_lines(ch: dict, svc: str) -> bool:
    if svc in (ch.get("cascade_messages") or {}):
        return True
    affected = set(ch.get("affected_services") or [])
    err = ch.get("error_services")
    return svc in affected and (not err or svc in err)


def _fault_lines(logs, svc):
    return [r for r in logs if r["service"] == svc and r["severity"] in ("WARN", "ERROR", "FATAL")]


def test_every_multi_service_channel_orders_its_services():
    missing = [n for n, d in REG.items() if "service_lag_seconds" not in d]
    assert not missing, f"channels without service_lag_seconds: {missing}"


@pytest.mark.parametrize("ch,svc,secs", [x for x in LAGGED if _emits_fault_lines(REG[x[0]], x[1])])
def test_dependent_stays_quiet_until_its_lag_passes(ch, svc, secs):
    early, _ = records(phase(ch, "during", elapsed=max(1.0, secs / 4)), cycles=6)
    late, _ = records(phase(ch, "during", elapsed=secs + 90), cycles=6)
    assert not _fault_lines(early, svc), f"ch{ch}: {svc} logs fault lines before its {secs}s lag"
    assert _fault_lines(late, svc), f"ch{ch}: {svc} never logs fault lines after its lag"


@pytest.mark.parametrize("ch", sorted({c for c, _, _ in LAGGED}))
def test_cause_service_fails_immediately(ch):
    d = REG[ch]
    lagged = set(d["service_lag_seconds"])
    roots = [s for s in d["affected_services"] if s not in lagged and _emits_fault_lines(d, s)]
    if not roots:
        pytest.skip("this channel's cause service only lags dependents' metrics")
    logs, _ = records(phase(ch, "during", elapsed=1.0), cycles=6)
    assert any(_fault_lines(logs, s) for s in roots), f"ch{ch}: no root fault lines at once"


def test_dependent_metric_follows_the_lag():
    # channel 1: model-router fails at once; assistant-web degrades 40 s later
    name = "assistant_web.degraded_answer_pct"
    _, early = records(phase(1, "during", elapsed=20.0), cycles=6)
    _, late = records(phase(1, "during", elapsed=40 + 90.0), cycles=6)
    before = values(early, "assistant-web", name)
    after = values(late, "assistant-web", name)
    assert before and after
    assert sum(after) / len(after) > 5 * (sum(before) / len(before) + 0.01)


# ── package streams recover with the service metrics (generator_hold_seconds) ──

from generator_engine.db_common import resolve_active_chaos  # noqa: E402

HELD = [(ch, gid, secs) for ch, d in sorted(REG.items())
        for gid, secs in (d.get("generator_hold_seconds") or {}).items()]


def _chaos(ch: int, gid: str, which: str, **kw):
    return resolve_active_chaos(phase(ch, which, **kw), REG, gid, chaos_mode="explicit")


@pytest.mark.parametrize("ch,gid,secs", HELD)
def test_package_stream_holds_then_releases(ch, gid, secs):
    assert _chaos(ch, gid, "during"), "impact applies while the fault is active"
    held = _chaos(ch, gid, "after", since=secs / 2)
    assert held and ch in held["channel_ids"], f"ch{ch}: {gid} snapped to baseline at the fix"
    assert _chaos(ch, gid, "after", since=secs + 5) is None, f"ch{ch}: {gid} never released"


@pytest.mark.parametrize("ch", [3, 9])
def test_event_style_generators_stop_at_the_fix(ch):
    assert not REG[ch].get("generator_hold_seconds")
    for gid in REG[ch]["generator_impact"]:
        assert _chaos(ch, gid, "after", since=1.0) is None


def test_hold_covers_the_service_recovery():
    """A held stream must not release before the channel's own metrics have recovered."""
    for ch, gid, secs in HELD:
        recovery = max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])
        assert secs <= recovery, f"ch{ch}: hold {secs}s outlasts the {recovery}s verification window"
