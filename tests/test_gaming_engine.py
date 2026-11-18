"""Game engine channels (01, 02, 03, 19): before / during / after a fault.

Each channel's game-server signal sits in its baseline band before the fault, moves out of
band during it while the status line carries the bad configuration, and converges back
after the opsctl fix with the restored configuration. Other channels' signals stay put.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from tests._gaming_harness import PROFILE, REG, SCN, attr_values, bodies, controller, phase, records

GS = "game-server"
CHANNELS = [1, 2, 3, 19]


def _within(ch):
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _run(ch, which, **kw):
    return records(phase(ch, which, **kw))


def _v(metrics, name, **attrs):
    out = [m["value"] for m in metrics if m["service"] == GS and m["name"] == name
           and all(m["attrs"].get(k) == v for k, v in attrs.items())]
    assert out, (name, attrs)
    return out


def _norm(v):
    # OTLP JSON carries intValue as a string
    return int(v) if isinstance(v, str) and v.isdigit() else v


def _status(logs, op, key):
    return {_norm(r["attrs"][key]) for r in logs
            if r["service"] == GS and r["attrs"].get("operation") == op and key in r["attrs"]}


def _between(xs, lo, hi):
    return all(lo <= x <= hi for x in xs)


def _assert_baseline(metrics, skip=()):
    if 1 not in skip:
        assert _between(_v(metrics, "game_server.snapshot_bytes", **{"deployment.track": "canary"}), 600, 900)
        assert _between(_v(metrics, "game_server.state_reconciles_per_min", **{"deployment.track": "canary"}), 0, 3)
    if 2 not in skip:
        assert _between(_v(metrics, "game_server.physics.overflow_events_per_min"), 0, 1)
        assert _between(_v(metrics, "game_server.physics.max_body_velocity_mps"), 20, 60)
    if 3 not in skip:
        assert _between(_v(metrics, "game_server.fleet.ready_servers"), 560, 600)
        assert _between(_v(metrics, "game_server.fleet.pending_pods"), 0, 2)
    if 19 not in skip and 2 not in skip:
        assert _between(_v(metrics, "game_server.tick_rate"), 63.4, 64.0)
        assert _between(_v(metrics, "game_server.frame_time_ms"), 8, 14.5)
    if 19 not in skip:
        assert _between(_v(metrics, "game_server.entity_count"), 800, 1400)
    # the stable track is the comparison peer for channel 1 and never moves
    assert _between(_v(metrics, "game_server.snapshot_bytes", **{"deployment.track": "stable"}), 600, 900)


def test_before_everything_at_baseline():
    logs, m = _run(1, "before")
    _assert_baseline(m)
    assert _status(logs, "build_status", "deployment.server_build") == {"v7.2.0"}
    assert _status(logs, "physics_status", "config.physics_revision") == {"physics-tuning-r18"}
    assert _status(logs, "fleet_status", "infra.nodegroup_max_size") == {120}
    assert _status(logs, "entity_status", "config.despawn_ttl_s") == {120}
    assert not [r for r in logs if r["severity"] in ("WARN", "ERROR") and r["service"] == GS]


# ── ch01: canary build without delta compression ───────────────────────────

def test_ch01_phases():
    logs, m = _run(1, "during")
    assert _between(_v(m, "game_server.snapshot_bytes", **{"deployment.track": "canary"}), 2400, 3200)
    assert _between(_v(m, "game_server.state_reconciles_per_min", **{"deployment.track": "canary"}), 150, 400)
    _assert_baseline(m, skip=(1,))
    canary = [r for r in logs if r["attrs"].get("operation") == "build_status"
              and r["attrs"].get("deployment.track") == "canary"]
    assert {r["attrs"]["deployment.server_build"] for r in canary} == {"v7.2.1-canary"}
    assert {r["attrs"]["net.status"] for r in canary} == {"DEGRADED"}
    stable = [r for r in logs if r["attrs"].get("operation") == "build_status"
              and r["attrs"].get("deployment.track") == "stable"]
    assert {r["attrs"]["deployment.server_build"] for r in stable} == {"v7.2.0"}
    assert bodies(logs, GS, "NET-STATE-DESYNC")
    assert bodies(logs, "matchmaking-engine", "rematch")

    logs, m = _run(1, "after", since=60)
    _assert_baseline(m)
    assert _status(logs, "build_status", "deployment.server_build") == {"v7.2.0"}
    assert not bodies(logs, None, "NET-STATE-DESYNC")
    assert bodies(logs, GS, "cmd=\"opsctl rollback deployment/game-server-canary")


def test_ch01_ramps_and_converges():
    _, m = _run(1, "during", elapsed=20)
    assert max(_v(m, "game_server.snapshot_bytes", **{"deployment.track": "canary"})) < 1500
    _, m = _run(1, "after", since=20)
    assert min(_v(m, "game_server.snapshot_bytes", **{"deployment.track": "canary"})) > 1200


# ── ch02: physics tuning with unlimited depenetration velocity ─────────────

def test_ch02_phases():
    logs, m = _run(2, "during")
    assert _between(_v(m, "game_server.physics.overflow_events_per_min"), 80, 250)
    assert _between(_v(m, "game_server.physics.max_body_velocity_mps"), 1e4, 1e6)
    tick = _v(m, "game_server.tick_rate")
    assert _between(tick, 45, 64) and min(tick) < 63.4  # mild dip only
    _assert_baseline(m, skip=(2,))
    assert _status(logs, "physics_status", "config.physics_revision") == {"physics-tuning-r19"}
    assert _status(logs, "physics_status", "physics.max_depenetration_velocity_mps") == {0}
    assert _status(logs, "physics_status", "physics.status") == {"DEGRADED"}
    assert bodies(logs, GS, "PHYS-OVERFLOW")

    logs, m = _run(2, "after", since=30)
    _assert_baseline(m)
    assert _status(logs, "physics_status", "config.physics_revision") == {"physics-tuning-r18"}
    assert _status(logs, "physics_status", "physics.status") == {"HEALTHY"}
    assert not bodies(logs, None, "PHYS-OVERFLOW")


def test_ch02_converges_not_instant():
    _, m = _run(2, "after", since=10)
    assert min(_v(m, "game_server.physics.overflow_events_per_min")) > 20


# ── ch03: node group max size 120 -> 80 ────────────────────────────────────

def test_ch03_phases():
    logs, m = _run(3, "during")
    assert _between(_v(m, "game_server.fleet.ready_servers"), 400, 430)
    assert _between(_v(m, "game_server.fleet.pending_pods"), 140, 170)
    assert set(_v(m, "game_server.fleet.nodes")) == {80.0}
    _assert_baseline(m, skip=(3,))
    assert _status(logs, "fleet_status", "infra.nodegroup_max_size") == {80}
    assert _status(logs, "fleet_status", "fleet.status") == {"DEGRADED"}
    assert bodies(logs, "matchmaking-engine", "MM-QUEUE-OVERFLOW")
    assert bodies(logs, GS, "[Fleet] ")  # game-server logs its own symptom, not the MM error
    assert not bodies(logs, GS, "MM-QUEUE-OVERFLOW")
    # active matches dip mildly
    assert _between(_v(m, "game_server.active_matches"), 400, 470)

    logs, m = _run(3, "after", since=180)
    _assert_baseline(m)
    assert _between(_v(m, "game_server.active_matches"), 510, 560)
    assert _status(logs, "fleet_status", "infra.nodegroup_max_size") == {120}
    assert _status(logs, "fleet_status", "fleet.status") == {"HEALTHY"}


def test_ch03_nodes_join_gradually():
    _, m = _run(3, "after", since=60)
    assert _between(_v(m, "game_server.fleet.pending_pods"), 80, 130)
    logs, _ = _run(3, "after", since=_within(3))
    assert not bodies(logs, None, "MM-QUEUE-OVERFLOW")


# ── ch19: despawn timer disabled ───────────────────────────────────────────

def test_ch19_phases():
    logs, m = _run(19, "during")
    assert _between(_v(m, "game_server.entity_count"), 3000, 6000)
    assert _between(_v(m, "game_server.frame_time_ms"), 25, 55)
    assert _between(_v(m, "game_server.tick_rate"), 18, 40)
    _assert_baseline(m, skip=(19,))
    assert _status(logs, "entity_status", "config.despawn_ttl_s") == {0}
    assert _status(logs, "entity_status", "engine.status") == {"DEGRADED"}
    assert bodies(logs, GS, "ENGINE-TICKRATE-DEGRAD")

    logs, m = _run(19, "after", since=150)
    _assert_baseline(m)
    assert _status(logs, "entity_status", "config.despawn_ttl_s") == {120}
    assert _status(logs, "entity_status", "engine.status") == {"HEALTHY"}
    assert not bodies(logs, None, "ENGINE-TICKRATE-DEGRAD")


def test_ch19_ramps_and_converges():
    _, m = _run(19, "during", elapsed=30)
    assert max(_v(m, "game_server.entity_count")) < 3500
    _, m = _run(19, "after", since=60)
    ents = _v(m, "game_server.entity_count")
    assert _between(ents, 1400, 4000)
    assert max(_v(m, "game_server.tick_rate")) < 63.4


# ── isolation: unrelated channels leave the game server alone ──────────────

@pytest.mark.parametrize("ch", [4, 7, 9, 13, 17])
def test_unrelated_channels_keep_game_server_at_baseline(ch):
    _, m = _run(ch, "during")
    _assert_baseline(m)


# ── remediation commands ──────────────────────────────────────────────────

def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


@pytest.mark.parametrize("ch", CHANNELS)
def test_remediation_command_parses(ch):
    lines = _cmd_lines(ch)
    assert len(lines) == 1
    assert len(ops.expected_commands(REG[ch]["remediation_command"], PROFILE)) == len(lines)
    effects = REG[ch]["remediation_effects"]
    target = lines[0].split()[2]
    assert any("{actor}" in e["body"] and target in e["body"] for e in effects["log_events"])
    assert any(e["offset_seconds"] < 0 for e in REG[ch]["infrastructure_events"])


def _ops_inst():
    ctl = controller()
    return SimpleNamespace(
        chaos_controller=ctl,
        ctx=SimpleNamespace(services={s: {} for s in SCN.services}, scenario=SCN),
        resolve_channel=lambda ch, **kw: ctl.resolve(ch, **kw),
    )


@pytest.mark.parametrize("ch", CHANNELS)
def test_opsctl_resolves_channel_with_one_change_line(ch):
    inst = _ops_inst()
    ctl = inst.chaos_controller
    ctl.trigger(ch, user_email="liveops@example.com")
    for line in _cmd_lines(ch):
        assert ctl.get_channel_status(ch)["state"] != "STANDBY"
        ops.execute(inst, line)
    assert ctl.get_channel_status(ch)["state"] == "STANDBY"
    with ctl._lock:
        evs = list(ctl._pending_events)
    applied = [e for e in evs if 'msg="change applied"' in e["body"]]
    assert len(applied) == 1, [e["body"] for e in evs]
    assert applied[0]["service"] == GS


@pytest.mark.parametrize("ch", CHANNELS)
def test_status_attribute_steady_after_fix(ch):
    key, healthy = {1: ("deployment.server_build", "v7.2.0"),
                    2: ("config.physics_revision", "physics-tuning-r18"),
                    3: ("infra.nodegroup_max_size", 120),
                    19: ("config.despawn_ttl_s", 120)}[ch]
    logs, _ = _run(ch, "after", since=_within(ch))
    assert {_norm(v) for v in attr_values(logs, GS, key)} == {healthy}
