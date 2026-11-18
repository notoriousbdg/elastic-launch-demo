"""Azure cost and identity channels (02, 03, 14, 17): before / during / after a fault.

Each channel's signal metric sits in its baseline band before the fault, deviates during it
(consistent with the channel's fault_params) while the service's status log says ANOMALY and
shows the configuration that caused it, and converges back after the opsctl fix (not
instantly), with the corrected configuration on the status log and one audit line per
remediation command.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest
import yaml

from app import ops
from tests._azure_harness import (
    OPS_ACTOR, PROFILE, REG, ROOT, SCN, attr_values, bodies, controller, phase, records, values,
)

CG, IB = "cost-guard", "identity-broker"
CHANNELS = [2, 3, 14, 17]
ROLE_ID = "7c1e5d2a-93b4-4f0e-a6d8-2b9f41c0e6a3"


def _within(ch: int) -> float:
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _params(ch: int) -> dict:
    """The channel's fault_params as written in its YAML (the registry does not carry them)."""
    (path,) = (ROOT / "scenarios" / "azure" / "channels").glob(f"{ch:02d}-*.yaml")
    return yaml.safe_load(path.read_text())["fault_params"]


def _signal(ch, which, svc, metric, **kw):
    logs, metrics = records(phase(ch, which, **kw))
    vals = values(metrics, svc, metric)
    assert vals, f"{svc} emitted no {metric}"
    return logs, vals


def _status(logs, svc, contains, key):
    got = {r["attrs"].get(key) for r in logs if r["service"] == svc and contains in r["body"]}
    assert got, f"no {svc} line containing {contains!r}"
    return got


# ── ch02: support-agent v3.7.2 prompt loop -> burn rate and token rate ─────

def test_ch02_burn_rate_phases():
    m = "cost_guard.burn_rate_usd_per_hour"
    logs, v = _signal(2, "before", CG, m)
    assert all(50.0 <= x <= 62.0 for x in v), v
    assert _status(logs, CG, "spend_snapshot", "cost.status") == {"NOMINAL"}
    assert attr_values(logs, CG, "agent.version") == {"v3.7.1"}

    logs, v = _signal(2, "during", CG, m)
    assert all(640.0 <= x <= 680.0 for x in v), v
    assert _status(logs, CG, "spend_snapshot", "cost.status") == {"ANOMALY"}
    assert attr_values(logs, CG, "agent.version") == {"v3.7.2"}
    assert "support-agent-v3.7.2-loop" in attr_values(logs, CG, "agent.prompt_template_version")

    logs, v = _signal(2, "after", CG, m, since=_within(2))
    assert all(50.0 <= x <= 62.0 for x in v), v
    assert _status(logs, CG, "spend_snapshot", "cost.status") == {"NOMINAL"}
    assert attr_values(logs, CG, "agent.version") == {"v3.7.1"}
    _assert_cmd_lines(logs, 2)


def test_ch02_tokens_per_minute_phases():
    m = "cost_guard.tokens_per_minute_k"
    _, v = _signal(2, "before", CG, m)
    assert all(420 <= x <= 900 for x in v), v
    lo, hi = _params(2)["tokens_per_min"]["randint"]
    _, v = _signal(2, "during", CG, m)
    assert all(lo / 1000 <= x <= hi / 1000 for x in v), v
    _, v = _signal(2, "after", CG, m, since=_within(2))
    assert all(420 <= x <= 900 for x in v), v


def test_ch02_ramps_in_and_converges():
    _, early = _signal(2, "during", CG, "cost_guard.burn_rate_usd_per_hour", elapsed=5)
    assert max(early) < 400.0, early
    _, mid = _signal(2, "after", CG, "cost_guard.burn_rate_usd_per_hour", since=10)
    assert min(mid) > 400.0, mid


def test_ch02_burn_matches_fault_params():
    p = _params(2)
    lo, hi = p["spend_usd_per_hr"]["randint"]
    assert (lo, hi) == (640, 680)
    assert p["baseline_cost_usd_per_hr"] == 55


# ── ch03: Owner role assignment -> Key Vault secret reads ──────────────────

M3 = "identity_broker.keyvault_unapproved_secret_reads_per_min"


def test_ch03_secret_reads_phases():
    logs, v = _signal(3, "before", IB, M3)
    assert set(v) == {0.0}
    assert _status(logs, IB, "token_stats", "auth.status") == {"NOMINAL"}
    assert _status(logs, IB, "token_stats", "authz.role_assignment") == {"none"}

    logs, v = _signal(3, "during", IB, M3)
    assert all(10 <= x <= 24 for x in v), v
    assert _status(logs, IB, "token_stats", "auth.status") == {"ANOMALY"}
    assert _status(logs, IB, "token_stats", "authz.role_assignment") == {ROLE_ID}
    assert _status(logs, IB, "token_stats", "authz.role_name") == {_params(3)["role_name"]}
    assert _status(logs, IB, "token_stats", "keyvault.secret_version") == {"a41c07d9"}

    logs, v = _signal(3, "after", IB, M3, since=_within(3))
    assert set(v) == {0.0}
    assert _status(logs, IB, "token_stats", "auth.status") == {"NOMINAL"}
    assert _status(logs, IB, "token_stats", "authz.role_assignment") == {"revoked"}
    assert _status(logs, IB, "token_stats", "keyvault.secret_version") == {"f7b20c5e"}
    _assert_cmd_lines(logs, 3)


def test_ch03_converges_rather_than_snaps():
    _, early = _signal(3, "during", IB, M3, elapsed=5)
    assert max(early) < 10, early
    _, mid = _signal(3, "after", IB, M3, since=10)
    assert min(mid) > 0, mid


def test_ch03_needs_both_commands():
    """Deleting the role assignment alone does not resolve the channel or rotate the secret."""
    inst = _ops_inst()
    ctl = inst.chaos_controller
    ctl.trigger(3, user_email="sre@example.com")
    first, second = _cmd_lines(3)
    ops.execute(inst, first)
    assert ctl.get_channel_status(3)["state"] != "STANDBY"
    logs, _ = records(ctl)
    assert _status(logs, IB, "token_stats", "authz.role_assignment") == {"revoked"}
    assert _status(logs, IB, "token_stats", "keyvault.secret_version") == {"a41c07d9"}
    ops.execute(inst, second)
    assert ctl.get_channel_status(3)["state"] == "STANDBY"
    logs, _ = records(ctl)
    assert _status(logs, IB, "token_stats", "keyvault.secret_version") == {"f7b20c5e"}


# ── ch14: budget revision in EUR-cents -> utilization 1-3%, no throttling ──

M14U, M14T = "cost_guard.budget_utilization_pct", "cost_guard.throttle_decisions"


def test_ch14_utilization_phases():
    p = _params(14)
    logs, v = _signal(14, "before", CG, M14U)
    assert all(30.0 <= x <= 70.0 for x in v), v
    assert _status(logs, CG, "spend_snapshot", "cost.status") == {"NOMINAL"}

    logs, v = _signal(14, "during", CG, M14U)
    assert all(1.0 <= x <= 3.0 for x in v), v
    assert all(abs(x - p["threshold_pct_used"]) < 0.2 for x in v), (v, p["threshold_pct_used"])
    assert _status(logs, CG, "spend_snapshot", "cost.status") == {"ANOMALY"}
    assert attr_values(logs, CG, "budget.unit") == {p["configured_unit"]}
    assert attr_values(logs, CG, "budget.limit") == {str(p["configured_budget"])}
    assert attr_values(logs, CG, "budget.config_revision") == {"budget-rev-2026-10-17"}
    assert attr_values(logs, CG, "budget.name") == {p["budget_name"]}

    logs, v = _signal(14, "after", CG, M14U, since=_within(14))
    assert all(100.0 < x < 260.0 for x in v), v
    real = p["spend_to_date"] / p["intended_budget"] * 100
    assert all(abs(x - real) < 5.0 for x in v), (v, real)
    assert _status(logs, CG, "spend_snapshot", "cost.status") == {"NOMINAL"}
    assert attr_values(logs, CG, "budget.unit") == {p["intended_unit"]}
    assert attr_values(logs, CG, "budget.limit") == {str(p["intended_budget"])}
    assert attr_values(logs, CG, "budget.config_revision") == {"budget-rev-2026-09-30"}
    _assert_cmd_lines(logs, 14)


def test_ch14_throttle_decisions_phases():
    _, v = _signal(14, "before", CG, M14T)
    assert all(0 <= x <= 6 for x in v), v
    _, v = _signal(14, "during", CG, M14T)
    assert set(v) == {0.0}, v
    _, v = _signal(14, "after", CG, M14T, since=_within(14))
    assert all(x > 0 for x in v), v


def test_ch14_converges_rather_than_snaps():
    _, early = _signal(14, "after", CG, M14U, since=10)
    assert max(early) < 100.0, early
    _, early_t = _signal(14, "after", CG, M14T, since=10)
    assert max(early_t) < 40, early_t


# ── ch17: expired Entra client secret -> token failures ────────────────────

M17F, M17D = "identity_broker.token_failure_pct", "identity_broker.credential_days_to_expiry"
BROKER_SECRET = "identity-broker-client-secret"


def _days(metrics, secret):
    return [m["value"] for m in metrics if m["service"] == IB and m["name"] == M17D
            and m["attrs"].get("auth.credential") == secret]


def test_ch17_token_failure_phases():
    logs, v = _signal(17, "before", IB, M17F)
    assert all(0.1 <= x <= 1.2 for x in v), v
    assert _status(logs, IB, "token_stats", "auth.status") == {"NOMINAL"}
    assert _status(logs, IB, "token_stats", "entra.client_secret_id") == {"secret-2025-10-prod-a41c"}

    logs, v = _signal(17, "during", IB, M17F, elapsed=1)
    assert all(95.0 <= x <= 100.0 for x in v), v     # abrupt: no ramp
    assert _status(logs, IB, "token_stats", "auth.status") == {"ANOMALY"}
    assert _status(logs, IB, "token_stats", "entra.client_secret_id") == {"secret-2025-10-prod-a41c"}
    fails = values(records(phase(17, "during"))[1], IB, "identity_broker.tokens_issued_per_min")
    assert all(x <= 10 for x in fails), fails

    logs, v = _signal(17, "after", IB, M17F, since=_within(17))
    assert all(x <= 1.2 for x in v), v
    assert _status(logs, IB, "token_stats", "auth.status") == {"NOMINAL"}
    assert _status(logs, IB, "token_stats", "entra.client_secret_id") == {"secret-2026-11-prod-c93e"}
    _assert_cmd_lines(logs, 17)


def test_ch17_failures_match_fault_params():
    lo, hi = _params(17)["token_failures_per_min"]["randint"]
    logs, _ = records(phase(17, "during"))
    f = {int(r["attrs"]["auth.failures_per_min"]) for r in logs if "token_stats" in r["body"] and r["service"] == IB}
    assert all(lo <= x <= hi for x in f), f


def test_ch17_credential_days_phases():
    _, metrics = records(phase(17, "before"))
    assert all(21 <= x <= 180 for x in _days(metrics, BROKER_SECRET))
    _, metrics = records(phase(17, "during"))
    assert all(x < 0 for x in _days(metrics, BROKER_SECRET)), _days(metrics, BROKER_SECRET)
    assert all(21 <= x <= 180 for x in _days(metrics, "partner-portal-client-secret"))
    _, metrics = records(phase(17, "after", since=_within(17)))
    d = _days(metrics, BROKER_SECRET)
    assert d and all(178 <= x <= 180 for x in d), d


def test_ch17_converges_rather_than_snaps():
    _, early = _signal(17, "after", IB, M17F, since=10)
    assert min(early) > 50.0, early
    _, metrics = records(phase(17, "after", since=10))
    assert all(x < 100 for x in _days(metrics, BROKER_SECRET))


# ── channels only move their own metrics ───────────────────────────────────

def test_other_channel_does_not_move_signals():
    for other in (2, 14, 17, 3):
        logs, metrics = records(phase(other, "during"))
        if other not in (2,):
            assert all(x < 100 for x in values(metrics, CG, "cost_guard.burn_rate_usd_per_hour"))
        if other not in (3,):
            assert set(values(metrics, IB, M3)) == {0.0}
        if other not in (17,):
            assert all(x <= 1.2 for x in values(metrics, IB, M17F))


# ── shared: cause events, command parsing, audit lines ─────────────────────

def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _assert_cmd_lines(logs, ch):
    for line in _cmd_lines(ch):
        name = ops.parse_command(line.removeprefix("opsctl "), PROFILE).target.split("/")[-1]
        hits = [r for r in logs if name in r["body"] and OPS_ACTOR in r["body"]]
        assert len(hits) == 1, (line, [r["body"] for r in logs if name in r["body"]])
        assert "{actor}" not in hits[0]["body"]


@pytest.mark.parametrize("ch", CHANNELS)
def test_cause_event_precedes_symptoms(ch):
    ev = REG[ch]["infrastructure_events"]
    assert len(ev) == 1 and ev[0]["offset_seconds"] < 0
    assert ev[0]["service"] in SCN.services
    assert "{actor}" not in ev[0]["body"]


@pytest.mark.parametrize("ch", CHANNELS)
def test_remediation_commands_parse(ch):
    lines = _cmd_lines(ch)
    assert lines
    for line in lines:
        assert line.startswith("opsctl ")
        ops.parse_command(line.removeprefix("opsctl "), PROFILE)
    assert len(ops.expected_commands(REG[ch]["remediation_command"], PROFILE)) == len(lines)


def _ops_inst():
    ctl = controller()
    return SimpleNamespace(
        chaos_controller=ctl,
        ctx=SimpleNamespace(services={s: {} for s in SCN.services}, scenario=SCN),
        resolve_channel=lambda ch, **kw: ctl.resolve(ch, **kw),
    )


@pytest.mark.parametrize("ch", CHANNELS)
def test_opsctl_resolves_channel_with_one_audit_per_command(ch):
    inst = _ops_inst()
    ctl = inst.chaos_controller
    ctl.trigger(ch, user_email="sre@example.com")
    lines = _cmd_lines(ch)
    results = []
    for line in lines:
        assert ctl.get_channel_status(ch)["state"] != "STANDBY"
        results.append(ops.execute(inst, line))
    assert ctl.get_channel_status(ch)["state"] == "STANDBY"
    with ctl._lock:
        for e in ctl._pending_events:
            e["emit_at"] = 0
        evs, ctl._pending_events = list(ctl._pending_events), []
    for line, r in zip(lines, results):
        name = ops.parse_command(line.removeprefix("opsctl "), PROFILE).target.split("/")[-1]
        hit = [e for e in evs if name in e["body"] and OPS_ACTOR in e["body"]]
        assert len(hit) == 1, (line, [e["body"] for e in evs])
        assert f"change_id={r['change_id']}" in hit[0]["body"]
        assert "{actor}" not in hit[0]["body"]
