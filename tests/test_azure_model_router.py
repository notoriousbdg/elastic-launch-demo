"""Azure model-router channels (01 availability drop, 10 bad canary) and its ch16 signals.

Each signal metric sits in its baseline band before the fault, deviates during it while the
status log says ANOMALY (with the bad configuration visible), and converges back after the
opsctl fix with the corrected configuration. Channel 16's quota metrics on model-router only
move for channel 16.
"""
from __future__ import annotations

import pytest

from app import ops
from tests._azure_harness import (
    OPS_ACTOR, PROFILE, REG, SCN, attr_values, bodies, phase, records, values,
)

SVC = "model-router"


def _within(ch: int) -> float:
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _sig(ch, which, metric, **kw):
    logs, metrics = records(phase(ch, which, **kw))
    vals = values(metrics, SVC, metric)
    assert vals, f"{SVC} emitted no {metric}"
    return logs, vals


def _status(logs, op):
    return {r["attrs"].get("router.status") for r in logs
            if r["service"] == SVC and r["attrs"].get("operation") == op}


def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _assert_cmd_lines(logs, ch):
    for line in _cmd_lines(ch):
        cmd = ops.parse_command(line, PROFILE)
        hits = [r["body"] for r in logs
                if cmd.name in r["body"] and f"={OPS_ACTOR}" in r["body"]
                and r["body"].startswith(("azure-activity:", "config-service:"))]
        assert len(hits) == 1, (line, hits)


# -- ch01 ---------------------------------------------------------------------

def test_ch01_availability_phases():
    m = "model_router.deployment_availability_pct"
    logs, v = _sig(1, "before", m)
    assert all(99.5 <= x <= 100.0 for x in v), v
    assert _status(logs, "deployment_health") == {"NOMINAL"}
    assert attr_values(logs, SVC, "deployment.foundry_model_version") == {"gpt-4o-2024-08-06"}

    logs, v = _sig(1, "during", m)
    lo, hi = 93.0, 97.0  # channel 1 fault_params.availability_pct
    assert all(lo <= x <= hi for x in v), v
    assert _status(logs, "deployment_health") == {"ANOMALY"}
    assert "gpt-4o-2024-11-20" in attr_values(logs, SVC, "deployment.foundry_model_version")
    # the router picks a deployment per request, so sample enough cycles to see the failing one
    many, _ = records(phase(1, "during"), cycles=60)
    assert any("deployment=gpt-4o-chat-prod" in b for b in bodies(many, SVC, "route_selected"))

    logs, v = _sig(1, "after", m, since=_within(1))
    assert all(x >= 99.5 for x in v), v
    assert _status(logs, "deployment_health") == {"NOMINAL"}
    assert attr_values(logs, SVC, "deployment.foundry_model_version") == {"gpt-4o-2024-08-06"}
    _assert_cmd_lines(logs, 1)


def test_ch01_latency_up_then_back():
    m = "model_router.route_latency_ms"
    _, before = _sig(1, "before", m)
    _, during = _sig(1, "during", m)
    _, after = _sig(1, "after", m, since=_within(1))
    assert max(before) <= 1600.0 and min(during) > 4000.0 and max(after) <= 1600.0


def test_ch01_converges_rather_than_snaps():
    _, early = _sig(1, "after", "model_router.deployment_availability_pct", since=10)
    assert max(early) < 99.0, early


def test_ch01_cause_event_precedes_symptoms():
    ev = REG[1]["infrastructure_events"][0]
    assert ev["offset_seconds"] < 0 and ev["service"] == SVC
    assert ev["attributes"]["deployment.foundry_model_version"] == "gpt-4o-2024-11-20"


# -- ch10 ---------------------------------------------------------------------

def test_ch10_canary_phases():
    share, empty = "model_router.canary_traffic_share_pct", "model_router.empty_completion_pct"
    logs, v = _sig(10, "before", share)
    assert all(4.0 <= x <= 6.0 for x in v), v
    _, e = _sig(10, "before", empty)
    assert all(x <= 0.5 for x in e), e
    assert _status(logs, "canary_health") == {"NOMINAL"}
    assert attr_values(logs, SVC, "router.canary_weight") == {"5"}

    logs, v = _sig(10, "during", share)
    assert all(abs(x - 25.0) <= 1.0 for x in v), v
    _, e = _sig(10, "during", empty)
    assert all(30.0 <= x <= 76.0 for x in e), e
    assert _status(logs, "canary_health") == {"ANOMALY"}
    assert "25" in attr_values(logs, SVC, "router.canary_weight")

    logs, v = _sig(10, "after", share, since=_within(10))
    assert all(x <= 0.5 for x in v), v
    _, e = _sig(10, "after", empty, since=_within(10))
    assert all(x <= 0.5 for x in e), e
    assert _status(logs, "canary_health") == {"NOMINAL"}
    assert attr_values(logs, SVC, "router.canary_weight") == {"0"}
    _assert_cmd_lines(logs, 10)


def test_ch10_converges_rather_than_snaps():
    _, v = _sig(10, "after", "model_router.canary_traffic_share_pct", since=10)
    assert min(v) > 5.0, v
    _, e = _sig(10, "after", "model_router.empty_completion_pct", since=10)
    assert min(e) > 5.0, e


def test_ch10_cause_event_precedes_symptoms():
    ev = REG[10]["infrastructure_events"][0]
    assert ev["offset_seconds"] < 0 and ev["service"] == SVC


# -- ch16 signals on model-router ---------------------------------------------

def _need_ch16_fix():
    if "remediation_effects" not in REG[16]:
        pytest.skip("channel 16 has no remediation_effects yet (owned by another worker)")


def test_ch16_quota_signals_phases():
    tpm, thr = "model_router.tpm_quota_used_pct", "model_router.throttled_429_count"
    logs, v = _sig(16, "before", tpm)
    assert all(38.0 <= x <= 76.0 for x in v), v
    _, t = _sig(16, "before", thr)
    assert all(0 <= x <= 2 for x in t), t
    assert _status(logs, "quota_health") == {"NOMINAL"}

    logs, v = _sig(16, "during", tpm)
    assert all(96.0 <= x <= 100.0 for x in v), v
    _, t = _sig(16, "during", thr)
    assert all(25 <= x <= 92 for x in t), t
    assert _status(logs, "quota_health") == {"ANOMALY"}
    assert "450000" in attr_values(logs, SVC, "router.tpm_capacity")

    _need_ch16_fix()
    logs, v = _sig(16, "after", tpm, since=90)
    assert all(38.0 <= x <= 76.0 for x in v), v
    _, t = _sig(16, "after", thr, since=90)
    assert all(0 <= x <= 2 for x in t), t
    assert _status(logs, "quota_health") == {"NOMINAL"}
    assert attr_values(logs, SVC, "router.tpm_capacity") == {"600000"}


def test_ch16_quota_converges_rather_than_snaps():
    _need_ch16_fix()
    _, v = _sig(16, "after", "model_router.tpm_quota_used_pct", since=10)
    assert min(v) > 80.0, v


@pytest.mark.parametrize("ch", [1, 10])
def test_quota_metrics_stay_baseline_for_other_channels(ch):
    _, v = _sig(ch, "during", "model_router.tpm_quota_used_pct")
    assert all(38.0 <= x <= 76.0 for x in v), v
    _, t = _sig(ch, "during", "model_router.throttled_429_count")
    assert all(x <= 2 for x in t), t


def test_other_channels_do_not_move_canary_or_availability():
    _, v = _sig(16, "during", "model_router.canary_traffic_share_pct")
    assert all(4.0 <= x <= 6.0 for x in v), v
    _, a = _sig(10, "during", "model_router.deployment_availability_pct")
    assert all(x >= 99.5 for x in a), a


# -- commands -------------------------------------------------------------------

@pytest.mark.parametrize("ch", [1, 10])
def test_expected_commands_parse(ch):
    assert len(ops.expected_commands(REG[ch]["remediation_command"], PROFILE)) == len(_cmd_lines(ch))
    for line in _cmd_lines(ch):
        ops.parse_command(line, PROFILE)
