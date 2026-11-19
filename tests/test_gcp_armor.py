"""gcp Cloud Armor channels (04, 05, 06): before / during / after a fault.

The signal metric on cloud-armor-waf sits in its baseline before the fault, deviates during it
(consistent with fault_params) while the status log says ANOMALY, and converges after the opsctl
fix with the changed configuration on the status log and one gcp-audit line per command.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from tests._gcp_harness import (
    OPS_ACTOR, PROFILE, REG, SCN, attr_values, bodies, controller, phase, records, values,
)

SVC = "cloud-armor-waf"


def _within(ch: int) -> float:
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _sig(ch, which, metric, **kw):
    logs, metrics = records(phase(ch, which, **kw))
    vals = values(metrics, SVC, metric)
    assert vals, f"{SVC} emitted no {metric}"
    return logs, vals


def _status(logs, contains):
    return {r["attrs"].get("armor.status") for r in logs if r["service"] == SVC and contains in r["body"]}


# -- ch04: HTTP flood, no rate limit on gcpnet-edge-policy ------------------

def test_ch04_phases():
    m = "armor.ddos_mitigated_pct"
    logs, v = _sig(4, "before", m)
    assert all(99.0 <= x <= 100.0 for x in v), v
    assert _status(logs, "ddos_mitigated=") == {"NOMINAL"}
    assert attr_values(logs, SVC, "armor.rule.rate_limit_global") == {"off"}

    logs, v = _sig(4, "during", m)
    assert all(55.0 <= x <= 70.0 for x in v), v
    assert _status(logs, "ddos_mitigated=") == {"ANOMALY"}
    assert attr_values(logs, SVC, "armor.policy") >= {"gcpnet-edge-policy"}
    _, f5 = _sig(4, "during", "armor.backend_5xx_pct")
    assert all(8.0 <= x <= 25.0 for x in f5), f5
    _, vol = _sig(4, "during", "armor.attack_volume_mbps")
    assert all(20000 <= x <= 90000 for x in vol), vol

    logs, v = _sig(4, "after", m, since=_within(4))
    assert all(x >= 99.0 for x in v), v
    assert _status(logs, "ddos_mitigated=") == {"NOMINAL"}
    assert "on" in attr_values(logs, SVC, "armor.rule.rate_limit_global")
    _, f5 = _sig(4, "after", "armor.backend_5xx_pct", since=_within(4))
    assert all(x <= 0.5 for x in f5), f5
    # the attack keeps coming after the fix
    _, vol = _sig(4, "after", "armor.attack_volume_mbps", since=_within(4))
    assert all(x >= 20000 for x in vol), vol
    _assert_audit(logs, 4)


def test_ch04_ramps_in_and_converges():
    _, early = _sig(4, "during", "armor.ddos_mitigated_pct", elapsed=5)
    assert min(early) > 85.0, early
    _, mid = _sig(4, "after", "armor.ddos_mitigated_pct", since=10)
    assert max(mid) < 95.0, mid


# -- ch05: deploy-sa lost securityAdmin -> rollback denied, false positives --

def test_ch05_phases():
    m = "armor.false_positive_rate_pct"
    logs, v = _sig(5, "before", m)
    assert all(x < 1.0 for x in v), v
    assert _status(logs, "policy_revision=") == {"NOMINAL"}
    assert not bodies(logs, SVC, "PERMISSION_DENIED")

    logs, v = _sig(5, "during", m)
    assert all(15.0 <= x <= 60.0 for x in v), v
    assert _status(logs, "policy_revision=") == {"ANOMALY"}
    assert "sqli-v33-stable-bad" in attr_values(logs, SVC, "armor.policy_revision")
    denied = bodies(logs, SVC, "PERMISSION_DENIED")
    assert denied and all("methodName=v1.compute.securityPolicies.patch" in b for b in denied)
    _, dn = _sig(5, "during", "armor.rollback_denied_per_min")
    assert all(20 <= x <= 400 for x in dn), dn

    logs, v = _sig(5, "after", m, since=_within(5))
    assert all(x < 1.0 for x in v), v
    assert _status(logs, "policy_revision=") == {"NOMINAL"}
    assert attr_values(logs, SVC, "armor.policy_revision") == {"sqli-v33-stable"}
    assert not bodies(logs, SVC, "PERMISSION_DENIED")
    _assert_audit(logs, 5)


def test_ch05_converges_rather_than_snaps():
    _, early = _sig(5, "after", "armor.false_positive_rate_pct", since=10)
    assert min(early) > 5.0, early


# -- ch06: armor-sync-v2.1.0-beta -> backends out of sync ------------------

def test_ch06_phases():
    logs, v = _sig(6, "before", "armor.backends_synced")
    assert set(v) == {6}
    _, lag = _sig(6, "before", "armor.policy_sync_lag_s")
    assert all(10 <= x <= 30 for x in lag), lag
    assert _status(logs, "sync_agent=") == {"NOMINAL"}

    logs, v = _sig(6, "during", "armor.backends_synced")
    assert all(1 <= x <= 3 for x in v), v
    _, lag = _sig(6, "during", "armor.policy_sync_lag_s")
    assert all(120 <= x <= 600 for x in lag), lag
    assert _status(logs, "sync_agent=") == {"ANOMALY"}
    assert "armor-sync-v2.1.0-beta" in attr_values(logs, SVC, "armor.sync_agent_version")

    logs, v = _sig(6, "after", "armor.backends_synced", since=_within(6))
    assert set(v) == {6}, v
    _, lag = _sig(6, "after", "armor.policy_sync_lag_s", since=_within(6))
    assert all(10 <= x <= 30 for x in lag), lag
    assert _status(logs, "sync_agent=") == {"NOMINAL"}
    assert attr_values(logs, SVC, "armor.sync_agent_version") == {"armor-sync-v2.0.4"}
    _assert_audit(logs, 6)


def test_ch06_converges_rather_than_snaps():
    _, v = _sig(6, "after", "armor.backends_synced", since=5)
    assert max(v) < 6, v


# -- shared: command audit lines and opsctl end to end ----------------------

CHANNELS = [4, 5, 6]


def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _assert_audit(logs, ch):
    audits = bodies(logs, "vpc-network-manager", "gcp-audit:")
    n = len([a for a in audits if f"principalEmail={OPS_ACTOR}" in a])
    assert n == len(_cmd_lines(ch)), audits


def _ops_inst():
    ctl = controller()
    return SimpleNamespace(
        chaos_controller=ctl,
        ctx=SimpleNamespace(services={s: {} for s in SCN.services}, scenario=SCN),
        resolve_channel=lambda ch, **kw: ctl.resolve(ch, **kw),
    )


@pytest.mark.parametrize("ch", CHANNELS)
def test_expected_commands_parse(ch):
    assert len(ops.expected_commands(REG[ch]["remediation_command"], PROFILE)) == len(_cmd_lines(ch))


@pytest.mark.parametrize("ch", CHANNELS)
def test_opsctl_resolves_channel_with_one_audit_per_command(ch):
    inst = _ops_inst()
    ctl = inst.chaos_controller
    ctl.trigger(ch, user_email="netops@example.com")
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
    audits = [e["body"] for e in evs if e["body"].startswith("gcp-audit:")]
    assert len(audits) == len(lines), audits
    for r in results:
        assert sum(f"change_id={r['change_id']}" in a for a in audits) == 1, (r, audits)
    assert all("{actor}" not in a and f"principalEmail={OPS_ACTOR}" in a for a in audits)
