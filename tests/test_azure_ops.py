"""Azure GenAI Assistant: each channel's real opsctl fix resolves it and leaves its own audit record.

Runs every channel's `remediation_command` through app.ops.execute (the path Claude Code and the
Chaos UI use), not through ChaosController.resolve, so the commands, the ops profile and the
channels' `{actor}` fix lines are checked against each other.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from app.chaos.controller import ACTIVE, STANDBY, ChaosController
from tests._azure_harness import PROFILE, REG, SCN

CHANNELS = sorted(REG)


class _Inst:
    def __init__(self):
        self.chaos_controller = ChaosController(channel_registry=REG, actors=PROFILE.get("actors"))
        self.ctx = SimpleNamespace(services={s: {} for s in SCN._service_specs},
                                   scenario=SimpleNamespace(ops_profile=PROFILE))
        self.deployment_id = "dep-az"

    def resolve_channel(self, channel, **kw):
        return self.chaos_controller.resolve(channel, **kw)

    def trigger_channel(self, channel, **kw):
        return self.chaos_controller.trigger(channel, **kw)


def _drain_all(inst):
    ctl = inst.chaos_controller
    with ctl._lock:
        for e in ctl._pending_events:
            e["emit_at"] = 0
    out = []
    for svc in inst.ctx.services:
        out += [(svc, e) for e in ctl.drain_events(svc)]
    return out


def _lines(ch):
    return [c for c in ops.expected_commands(REG[ch]["remediation_command"], PROFILE)]


@pytest.mark.parametrize("ch", CHANNELS)
def test_fix_commands_resolve_channel_and_each_leaves_one_record(ch):
    inst = _Inst()
    inst.trigger_channel(ch, user_email="sre@example.com")
    cmds = _lines(ch)
    assert cmds, f"channel {ch} has no parsable remediation_command"
    for i, cmd in enumerate(cmds):
        assert inst.chaos_controller.get_channel_status(ch)["state"] == ACTIVE
        res = ops.execute(inst, cmd.raw, user="sre-oncall@contoso-demo.onmicrosoft.com")
        assert res["status"] == "applied", res
    assert inst.chaos_controller.get_channel_status(ch)["state"] == STANDBY
    assert ch in inst.chaos_controller.get_fixed_channels()
    records = [e["body"] for _, e in _drain_all(inst)]
    for cmd in cmds:
        hits = [b for b in records if "change_id=" in b and cmd.name in b]
        assert hits, f"no audit record for {cmd.raw!r}: {records}"
    assert not any("{actor}" in b for b in records)


@pytest.mark.parametrize("ch", CHANNELS)
def test_symptom_fix_does_not_resolve(ch):
    """A restart-style change on the affected service is recorded but fixes nothing."""
    inst = _Inst()
    inst.trigger_channel(ch, user_email="sre@example.com")
    ops.execute(inst, "patch job/unrelated-job --noop true", user="sre-oncall@contoso-demo.onmicrosoft.com")
    assert inst.chaos_controller.get_channel_status(ch)["state"] == ACTIVE
