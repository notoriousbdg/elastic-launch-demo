"""Financial: every fix is an opsctl command that resolves its channel and leaves one audit record.

The fix's audit line comes from audit-logger on the change-mgmt stream, names the exact
command and the actor, and is followed by the changed component's own confirmation line.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from tests._financial_harness import (
    AUDIT_MARK, OPS_ACTOR, PROFILE, REG, SCN, bodies, cmd_lines, controller, phase, records,
)

CHANNELS = sorted(REG)


def _ops_inst():
    ctl = controller()
    return SimpleNamespace(
        chaos_controller=ctl,
        ctx=SimpleNamespace(services={s: {} for s in SCN.services}, scenario=SCN),
        resolve_channel=lambda ch, **kw: ctl.resolve(ch, **kw),
    )


def test_twenty_channels_at_schema_13():
    assert CHANNELS == list(range(1, 21))
    assert str(SCN.schema_version) == "1.3"


@pytest.mark.parametrize("ch", CHANNELS)
def test_expected_commands_parse(ch):
    assert len(ops.expected_commands(REG[ch]["remediation_command"], PROFILE)) == len(cmd_lines(ch))


@pytest.mark.parametrize("ch", CHANNELS)
def test_cause_is_logged_before_the_symptoms(ch):
    events = REG[ch].get("infrastructure_events") or []
    assert any(float(e.get("offset_seconds", 0)) < 0 for e in events), ch


@pytest.mark.parametrize("ch", CHANNELS)
def test_opsctl_resolves_channel_with_one_audit_per_command(ch):
    inst = _ops_inst()
    ctl = inst.chaos_controller
    ctl.trigger(ch, user_email="sre@example.com")
    lines = cmd_lines(ch)
    results = []
    for line in lines:
        assert ctl.get_channel_status(ch)["state"] != "STANDBY"
        results.append(ops.execute(inst, line))
    assert ctl.get_channel_status(ch)["state"] == "STANDBY"
    with ctl._lock:
        for e in ctl._pending_events:
            e["emit_at"] = 0
        evs, ctl._pending_events = list(ctl._pending_events), []
    audits = [e for e in evs if AUDIT_MARK in e["body"]]
    assert len(audits) == len(lines), [e["body"] for e in evs]
    for line, r in zip(lines, results):
        hit = [a for a in audits if f'cmd="{line}"' in a["body"]]
        assert len(hit) == 1, (line, [a["body"] for a in audits])
        assert hit[0]["service"] == "audit-logger"
        assert f"actor={OPS_ACTOR}" in hit[0]["body"]
        assert f"change_id={r['change_id']}" in hit[0]["body"]
        assert "{actor}" not in hit[0]["body"]
    own = [e for e in evs if AUDIT_MARK not in e["body"]]
    assert own, [e["body"] for e in evs]


@pytest.mark.parametrize("ch", CHANNELS)
def test_chaos_ui_resolve_logs_each_command_once(ch):
    logs, _ = records(phase(ch, "after", since=5))
    audit = bodies(logs, "audit-logger", AUDIT_MARK)
    for line in cmd_lines(ch):
        hits = [b for b in audit if f'cmd="{line}"' in b]
        assert len(hits) == 1, (line, audit)
        assert f"actor={OPS_ACTOR}" in hits[0]
