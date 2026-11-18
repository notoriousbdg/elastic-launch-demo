"""Gaming auth and payment channels (11, 12, 13, 14): before / during / after a fault.

Each channel's signal sits in its baseline band before the fault, leaves it during the fault
(consistent with fault_params) while the status line carries the bad configuration, and
converges back after the opsctl fix with the restored configuration on the status line.
Other channels leave these signals alone.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from tests._gaming_harness import (
    OPS_ACTOR, PROFILE, REG, SCN, attr_values, bodies, controller, phase, records, values,
)

AUTH, PAY = "auth-gateway", "payment-processor"


def _within(ch):
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _run(ch, which, **kw):
    return records(phase(ch, which, **kw))


def _m(metrics, svc, name):
    v = values(metrics, svc, name)
    assert v, (svc, name)
    return v


def _in(vals, lo, hi):
    return all(lo <= x <= hi for x in vals)


def _status(logs, svc, op, key):
    return {str(r["attrs"][key]) for r in logs
            if r["service"] == svc and r["attrs"].get("operation") == op and key in r["attrs"]}


def _assert_fix_lines(logs, ch):
    svc = {11: AUTH, 12: AUTH, 13: PAY, 14: PAY}[ch]
    fixes = bodies(logs, svc, 'msg="change applied"')
    for line in _cmd_lines(ch):
        assert any(f'cmd="{line}"' in b and f"actor={OPS_ACTOR}" in b for b in fixes), (line, fixes)


# ── ch11: auth-token-policy TTL 60 -> 15 min, jitter 20% -> 0% ─────────────

def test_ch11_token_refresh_phases():
    rate, lat = "auth.token_refresh_rate", "auth.request_latency_ms"
    logs, m = _run(11, "before")
    assert _in(_m(m, AUTH, rate), 300, 600)
    assert _in(_m(m, AUTH, lat), 30, 80)
    assert _status(logs, AUTH, "token_policy_status", "config.token_ttl_min") == {"60"}
    assert _status(logs, AUTH, "token_policy_status", "config.refresh_jitter_pct") == {"20"}
    assert _status(logs, AUTH, "token_policy_status", "auth.status") == {"HEALTHY"}

    logs, m = _run(11, "during")
    assert _in(_m(m, AUTH, rate), 6000, 12000), _m(m, AUTH, rate)
    assert _in(_m(m, AUTH, lat), 400, 1500), _m(m, AUTH, lat)
    assert _status(logs, AUTH, "token_policy_status", "config.token_ttl_min") == {"15"}
    assert _status(logs, AUTH, "token_policy_status", "config.refresh_jitter_pct") == {"0"}
    assert _status(logs, AUTH, "token_policy_status", "auth.status") == {"DEGRADED"}
    assert bodies(logs, AUTH, "AUTH-TOKEN-STORM")
    assert bodies(logs, AUTH, "CHG0052811")
    assert not bodies(logs, "game-server", "AUTH-TOKEN-STORM")  # cascade line, not the error
    assert bodies(logs, "game-server", "session revalidation failed")

    logs, m = _run(11, "after", since=_within(11))
    assert _in(_m(m, AUTH, rate), 300, 600), _m(m, AUTH, rate)
    assert _in(_m(m, AUTH, lat), 30, 80)
    assert _status(logs, AUTH, "token_policy_status", "config.token_ttl_min") == {"60"}
    assert attr_values(logs, AUTH, "config.refresh_jitter_pct") == {"20"}
    assert _status(logs, AUTH, "token_policy_status", "auth.status") == {"HEALTHY"}
    assert not bodies(logs, AUTH, "AUTH-TOKEN-STORM")
    _assert_fix_lines(logs, 11)


def test_ch11_ramps_and_converges_over_minutes():
    _, m = _run(11, "during", elapsed=17)
    assert max(_m(m, AUTH, "auth.token_refresh_rate")) < 1500
    _, m = _run(11, "after", since=60)
    assert min(_m(m, AUTH, "auth.token_refresh_rate")) > 5000
    _, m = _run(11, "after", since=300)
    assert _in(_m(m, AUTH, "auth.token_refresh_rate"), 2000, 6000)


# ── ch12: Cloud Armor auth-edge-armor rule 1000 (rate-based ban) disabled ──

def test_ch12_login_failure_phases():
    lf, rl = "auth.login.failures_per_s", "auth.edge.rate_limited_requests_per_s"
    logs, m = _run(12, "before")
    assert _in(_m(m, AUTH, lf), 2, 10)
    assert _in(_m(m, AUTH, rl), 20, 80)
    assert _status(logs, AUTH, "edge_policy_status", "security.armor_policy_revision") == {"18"}
    assert _status(logs, AUTH, "edge_policy_status", "auth.status") == {"HEALTHY"}

    logs, m = _run(12, "during")
    assert _in(_m(m, AUTH, lf), 400, 1500), _m(m, AUTH, lf)
    assert set(_m(m, AUTH, rl)) == {0.0}
    assert _status(logs, AUTH, "edge_policy_status", "security.armor_policy_revision") == {"19"}
    assert _status(logs, AUTH, "edge_policy_status", "security.ban_rule_state") == {"disabled"}
    assert _status(logs, AUTH, "edge_policy_status", "auth.status") == {"DEGRADED"}
    assert bodies(logs, AUTH, "AUTH-TAKEOVER-DETECT")
    assert bodies(logs, AUTH, "CHG0052812")
    assert bodies(logs, PAY, "saved_card purchase requires step-up")

    logs, m = _run(12, "after", since=_within(12))
    assert _in(_m(m, AUTH, lf), 2, 10), _m(m, AUTH, lf)
    assert _in(_m(m, AUTH, rl), 20, 80), _m(m, AUTH, rl)
    assert _status(logs, AUTH, "edge_policy_status", "security.armor_policy_revision") == {"18"}
    assert _status(logs, AUTH, "edge_policy_status", "security.ban_rule_state") == {"enabled"}
    assert not bodies(logs, AUTH, "AUTH-TAKEOVER-DETECT")
    _assert_fix_lines(logs, 12)


def test_ch12_rate_limit_drops_before_failures_and_spikes_after_fix():
    _, m = _run(12, "during", elapsed=5)  # policy already changed, traffic not yet arrived
    assert set(_m(m, AUTH, "auth.edge.rate_limited_requests_per_s")) == {0.0}
    assert max(_m(m, AUTH, "auth.login.failures_per_s")) <= 10
    _, m = _run(12, "after", since=30)
    assert min(_m(m, AUTH, "auth.login.failures_per_s")) > 300
    assert min(_m(m, AUTH, "auth.edge.rate_limited_requests_per_s")) > 200
    _, m = _run(12, "after", since=120)
    assert _in(_m(m, AUTH, "auth.login.failures_per_s"), 2, 10)
    assert min(_m(m, AUTH, "auth.edge.rate_limited_requests_per_s")) > 400


# ── ch13: stripe-sdk v12.1.0-rc2 caps the connection pool at 2 per host ────

def test_ch13_purchase_phases():
    lat, fr, wait = "payment.transaction_latency_ms", "payment.failure_rate", "payment.http_pool.waiting"
    logs, m = _run(13, "before")
    assert _in(_m(m, PAY, lat), 180, 400)
    assert _in(_m(m, PAY, fr), 0.2, 1.0)
    assert _in(_m(m, PAY, wait), 0, 2)
    assert set(_m(m, PAY, "payment.http_pool.max_connections_per_host")) == {50.0}
    assert _status(logs, PAY, "provider_status", "deployment.stripe_sdk") == {"v12.0.4"}

    logs, m = _run(13, "during")
    assert _in(_m(m, PAY, lat), 4000, 12000), _m(m, PAY, lat)
    assert _in(_m(m, PAY, fr), 15, 35), _m(m, PAY, fr)
    assert _in(_m(m, PAY, wait), 150, 400)
    assert set(_m(m, PAY, "payment.http_pool.max_connections_per_host")) == {2.0}
    assert _status(logs, PAY, "provider_status", "deployment.stripe_sdk") == {"v12.1.0-rc2"}
    stripe = [r for r in logs if r["service"] == PAY and r["attrs"].get("operation") == "provider_status"]
    assert {r["attrs"]["payment.status"] for r in stripe if r["attrs"]["payment.provider"] == "stripe"} == {"DEGRADED"}
    assert {r["attrs"]["payment.status"] for r in stripe if r["attrs"]["payment.provider"] == "paypal"} == {"HEALTHY"}
    assert bodies(logs, PAY, "IAP-PURCHASE-FAIL")
    assert bodies(logs, PAY, "CHG0052813")
    assert not bodies(logs, AUTH, "IAP-PURCHASE-FAIL")

    logs, m = _run(13, "after", since=_within(13))
    assert _in(_m(m, PAY, lat), 180, 400), _m(m, PAY, lat)
    assert _in(_m(m, PAY, fr), 0.2, 1.0)
    assert _in(_m(m, PAY, wait), 0, 2)
    assert _status(logs, PAY, "provider_status", "deployment.stripe_sdk") == {"v12.0.4"}
    assert not bodies(logs, PAY, "IAP-PURCHASE-FAIL")
    _assert_fix_lines(logs, 13)


def test_ch13_converges_with_the_rollout():
    _, m = _run(13, "after", since=10)
    assert min(_m(m, PAY, "payment.transaction_latency_ms")) > 3000
    _, m = _run(13, "after", since=45)
    assert max(_m(m, PAY, "payment.transaction_latency_ms")) < 3500


# ── ch14: wallet-ledger isolation serializable -> read_committed ──────────

def test_ch14_ledger_phases():
    disc, retr = "payment.ledger.discrepancies_per_min", "payment.ledger.serialization_retries_per_min"
    logs, m = _run(14, "before")
    assert set(_m(m, PAY, disc)) == {0.0}
    assert _in(_m(m, PAY, retr), 10, 40)
    assert _status(logs, PAY, "ledger_status", "config.isolation_level") == {"serializable"}
    assert _status(logs, PAY, "ledger_status", "ledger.status") == {"HEALTHY"}

    logs, m = _run(14, "during")
    assert _in(_m(m, PAY, disc), 20, 90), _m(m, PAY, disc)
    assert set(_m(m, PAY, retr)) == {0.0}
    assert _status(logs, PAY, "ledger_status", "config.isolation_level") == {"read_committed"}
    assert _status(logs, PAY, "ledger_status", "ledger.status") == {"DEGRADED"}
    assert bodies(logs, PAY, "IAP-LEDGER-INCONSIST")
    assert bodies(logs, PAY, "CHG0052814")

    logs, m = _run(14, "after", since=_within(14))
    assert set(_m(m, PAY, disc)) == {0.0}
    assert _in(_m(m, PAY, retr), 10, 40)
    assert _status(logs, PAY, "ledger_status", "config.isolation_level") == {"serializable"}
    assert _status(logs, PAY, "ledger_status", "ledger.status") == {"HEALTHY"}
    assert not bodies(logs, PAY, "IAP-LEDGER-INCONSIST")
    assert bodies(logs, PAY, "job wallet-reconcile finished")
    _assert_fix_lines(logs, 14)


def test_ch14_retries_drop_at_once_and_recover_gradually():
    _, m = _run(14, "during", elapsed=5)
    assert set(_m(m, PAY, "payment.ledger.serialization_retries_per_min")) == {0.0}
    assert set(_m(m, PAY, "payment.ledger.discrepancies_per_min")) == {0.0}
    _, m = _run(14, "after", since=5)
    assert min(_m(m, PAY, "payment.ledger.discrepancies_per_min")) >= 15
    assert max(_m(m, PAY, "payment.ledger.serialization_retries_per_min")) < 10


# ── isolation: a channel moves only its own signals ───────────────────────

SIGNALS = {
    11: (AUTH, "auth.token_refresh_rate", 300, 600),
    12: (AUTH, "auth.login.failures_per_s", 2, 10),
    13: (PAY, "payment.transaction_latency_ms", 180, 400),
    14: (PAY, "payment.ledger.discrepancies_per_min", 0, 0),
}


@pytest.mark.parametrize("ch", [3, 7, 10, 11, 12, 13, 14, 18, 20])
def test_other_channels_leave_signals_at_baseline(ch):
    _, m = _run(ch, "during")
    for own, (svc, name, lo, hi) in SIGNALS.items():
        if own != ch:
            assert _in(_m(m, svc, name), lo, hi), (ch, name, _m(m, svc, name))


# ── opsctl: commands parse and resolve the channel ───────────────────────

CHANNELS = [11, 12, 13, 14]


def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


@pytest.mark.parametrize("ch", CHANNELS)
def test_expected_commands_parse(ch):
    parsed = ops.expected_commands(REG[ch]["remediation_command"], PROFILE)
    assert parsed and len(parsed) == len(_cmd_lines(ch))


def _ops_inst():
    ctl = controller()
    return SimpleNamespace(
        chaos_controller=ctl,
        ctx=SimpleNamespace(services={s: {} for s in SCN.services}, scenario=SCN),
        resolve_channel=lambda ch, **kw: ctl.resolve(ch, **kw),
    )


@pytest.mark.parametrize("ch", CHANNELS)
def test_opsctl_resolves_channel_with_one_change_line_per_command(ch):
    inst = _ops_inst()
    ctl = inst.chaos_controller
    ctl.trigger(ch, user_email="liveops@example.com")
    lines = _cmd_lines(ch)
    for line in lines:
        assert ctl.get_channel_status(ch)["state"] != "STANDBY"
        ops.execute(inst, line)
    assert ctl.get_channel_status(ch)["state"] == "STANDBY"
    with ctl._lock:
        evs = list(ctl._pending_events)
    applied = [e["body"] for e in evs if 'msg="change applied"' in e["body"]]
    assert len(applied) == len(lines), [e["body"] for e in evs]
    for line in lines:
        assert any(f'cmd="{line}"' in b for b in applied), (line, applied)
