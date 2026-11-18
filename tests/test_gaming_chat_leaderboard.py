"""Gaming chat and leaderboard channels (07, 08, 09, 10): before / during / after a fault.

Each channel's signal sits in its baseline band before the fault, leaves it during the fault
(consistent with fault_params) while the status line carries the bad config value, and
converges back after the opsctl fix with the restored value. Other channels' signals stay put.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from generator_engine.registry import GENERATORS
from tests._gaming_harness import (
    OPS_ACTOR, PROFILE, REG, SCN, attr_values, bodies, controller, phase, records, values,
)

CHAT, LB = "chat-service", "leaderboard-api"
OTHER_CHAT_BANDS = {"guild-12345": (5, 30), "match-lobby": (60, 200),
                    "trade-market": (20, 80), "lfg-ranked": (15, 60)}


def _within(ch):
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _run(ch, which, **kw):
    return records(phase(ch, which, **kw))


def _in(vals, lo, hi):
    assert vals, "no readings"
    assert all(lo <= v <= hi for v in vals), (lo, hi, vals)


def _rate(m, channel):
    return [x["value"] for x in m if x["service"] == CHAT and x["name"] == "chat.message_rate"
            and x["attrs"].get("chat.channel") == channel]


def _op_attr(logs, svc, op, key, **match):
    return {r["attrs"][key] for r in logs if r["service"] == svc
            and r["attrs"].get("operation") == op and key in r["attrs"]
            and all(r["attrs"].get(k) == v for k, v in match.items())}


def _assert_fix_lines(logs, ch):
    for line in _cmd_lines(ch):
        target = line.split()[2]
        assert any(OPS_ACTOR in b and target in b for b in bodies(logs, contains="change applied")), (ch, target)


def _assert_baseline_chat_lb(m, skip=()):
    if 7 not in skip:
        _in(_rate(m, "global-en"), 40, 120)
    if 8 not in skip:
        _in(values(m, CHAT, "chat.voice.mix_latency_ms"), 4, 9)
    if 9 not in skip:
        assert set(values(m, LB, "leaderboard.redis.evicted_keys_per_s")) == {0.0}
        assert set(values(m, LB, "leaderboard.consistency.mismatched_entries")) == {0.0}
    if 10 not in skip:
        assert set(values(m, LB, "leaderboard.season_pass.xp_events_dropped_per_min")) == {0.0}
        assert set(values(m, LB, "leaderboard.consumer.rebalances_per_min")) == {0.0}


# ── ch07: chat-ratelimits r42 removed the per-channel aggregate limit ──────

def test_ch07_phases():
    logs, m = _run(7, "before")
    _in(_rate(m, "global-en"), 40, 120)
    _in(values(m, CHAT, "chat.ratelimit.rejections_per_s"), 5, 30)
    assert _op_attr(logs, CHAT, "ratelimit_status", "config.ratelimit_revision") == {"r41"}
    assert _op_attr(logs, CHAT, "ratelimit_status", "chat.status") == {"OK"}
    _assert_baseline_chat_lb(m)

    logs, m = _run(7, "during")
    _in(_rate(m, "global-en"), 3000, 8000)
    for channel, (lo, hi) in OTHER_CHAT_BANDS.items():
        _in(_rate(m, channel), lo, hi)
    assert set(values(m, CHAT, "chat.ratelimit.rejections_per_s")) == {0.0}
    assert attr_values(logs, CHAT, "config.ratelimit_revision") == {"r42"}
    assert _op_attr(logs, CHAT, "ratelimit_status", "chat.status") == {"DEGRADED"}
    assert bodies(logs, CHAT, "CHAT-FLOOD-DETECT")
    assert bodies(logs, "moderation-engine", "automod-scanner: scan backlog")
    _assert_baseline_chat_lb(m, skip={7})

    logs, m = _run(7, "after", since=_within(7))
    _in(_rate(m, "global-en"), 40, 120)
    _in(values(m, CHAT, "chat.ratelimit.rejections_per_s"), 5, 30)
    assert _op_attr(logs, CHAT, "ratelimit_status", "config.ratelimit_revision") == {"r41"}
    assert not bodies(logs, None, "CHAT-FLOOD-DETECT")
    _assert_fix_lines(logs, 7)


def test_ch07_ramps_and_converges():
    _, m = _run(7, "during", elapsed=5)
    assert max(_rate(m, "global-en")) <= 120
    # the restored limit caps global-en at 500 msg/s and rejects the bots' sends for a while
    _, m = _run(7, "after", since=10)
    _in(_rate(m, "global-en"), 300, 500)
    assert min(values(m, CHAT, "chat.ratelimit.rejections_per_s")) > 1000


# ── ch08: voice-mixer CPU limit 2000m -> 1000m ─────────────────────────────

def test_ch08_phases():
    logs, m = _run(8, "before")
    _in(values(m, CHAT, "chat.voice.mix_latency_ms"), 4, 9)
    _in(values(m, CHAT, "chat.voice.packet_loss_pct"), 0, 0.5)
    _in(values(m, CHAT, "chat.voice.cpu_throttled_pct"), 0, 3)
    assert _op_attr(logs, CHAT, "mixer_status", "deployment.cpu_limit") == {"2000m"}

    logs, m = _run(8, "during")
    _in(values(m, CHAT, "chat.voice.mix_latency_ms"), 35, 80)
    _in(values(m, CHAT, "chat.voice.packet_loss_pct"), 6, 15)
    _in(values(m, CHAT, "chat.voice.cpu_throttled_pct"), 45, 75)
    assert attr_values(logs, CHAT, "deployment.cpu_limit") == {"1000m"}
    assert _op_attr(logs, CHAT, "mixer_status", "voice.status") == {"DEGRADED"}
    assert bodies(logs, CHAT, "VOICE-CHANNEL-OVERLOAD")
    _assert_baseline_chat_lb(m, skip={8})

    logs, m = _run(8, "after", since=_within(8))
    _in(values(m, CHAT, "chat.voice.mix_latency_ms"), 4, 9)
    _in(values(m, CHAT, "chat.voice.packet_loss_pct"), 0, 0.5)
    _in(values(m, CHAT, "chat.voice.cpu_throttled_pct"), 0, 3)
    assert _op_attr(logs, CHAT, "mixer_status", "deployment.cpu_limit") == {"2000m"}
    assert not bodies(logs, None, "VOICE-CHANNEL-OVERLOAD")
    _assert_fix_lines(logs, 8)


def test_ch08_converges_over_rollout():
    _, m = _run(8, "after", since=10)
    assert min(values(m, CHAT, "chat.voice.mix_latency_ms")) > 20


# ── ch09: lb-redis-prod 16 GB noeviction -> 4 GB allkeys-lru ───────────────

def _policy(logs, instance="lb-redis-prod"):
    return _op_attr(logs, LB, "redis_status", "cache.maxmemory_policy", **{"cache.instance": instance})


def test_ch09_phases():
    ev, mm, mem = ("leaderboard.redis.evicted_keys_per_s", "leaderboard.consistency.mismatched_entries",
                   "leaderboard.redis.memory_used_pct")
    logs, m = _run(9, "before")
    assert set(values(m, LB, ev)) == {0.0}
    assert set(values(m, LB, mm)) == {0.0}
    _in(values(m, LB, mem), 33, 42)
    assert _policy(logs) == {"noeviction"}

    logs, m = _run(9, "during")
    _in(values(m, LB, ev), 200, 500)
    _in(values(m, LB, mm), 5000, 40000)
    _in(values(m, LB, mem), 97, 100)
    assert _policy(logs) == {"allkeys-lru"}
    # the tournament instance was not part of the Terraform workspace
    assert _policy(logs, "lb-redis-tournament") == {"noeviction"}
    assert {int(v) for v in _op_attr(logs, LB, "redis_status", "leaderboard.mismatched_entries",
                                     **{"cache.instance": "lb-redis-tournament"})} == {0}
    assert bodies(logs, LB, "LB-DATA-CORRUPT")
    assert all("season=S14" in b for b in bodies(logs, LB, "LB-DATA-CORRUPT"))
    assert bodies(logs, "game-server", "post-match rank lookup failed")
    _assert_baseline_chat_lb(m, skip={9})

    logs, m = _run(9, "after", since=_within(9))
    assert set(values(m, LB, ev)) == {0.0}
    assert set(values(m, LB, mm)) == {0.0}
    _in(values(m, LB, mem), 33, 42)
    assert _policy(logs) == {"noeviction"}
    assert not bodies(logs, None, "LB-DATA-CORRUPT")
    _assert_fix_lines(logs, 9)


def test_ch09_rebuild_takes_time():
    _, m = _run(9, "during", elapsed=10)
    assert set(values(m, LB, "leaderboard.redis.evicted_keys_per_s")) == {0.0}  # onset delay
    _, m = _run(9, "after", since=30)
    assert min(values(m, LB, "leaderboard.consistency.mismatched_entries")) > 20000
    _, m = _run(9, "after", since=120)
    assert set(values(m, LB, "leaderboard.redis.evicted_keys_per_s")) == {0.0}
    assert min(values(m, LB, "leaderboard.consistency.mismatched_entries")) > 0


def test_ch09_season_pinned():
    logs, _ = _run(9, "during")
    assert attr_values(logs, LB, "leaderboard.season") == {"S14"}
    assert attr_values(logs, LB, "season.id") == {"S14"}


def test_ch09_redis_generator_impact_breaches():
    spec = GENERATORS["redis_otel"]
    impact = REG[9]["generator_impact"]["redis_otel"]
    assert set(impact) <= set(spec.impact_keys)
    assert impact["high_eviction_rate"] > 100.0
    assert impact["memory_approaching_maxmemory"] > 90.0
    assert set(REG[9]["generator_hold_seconds"]) <= set(REG[9]["generator_impact"])


# ── ch10: leaderboard-api v4.8.0 (auto-commit + OOM) ───────────────────────

def test_ch10_phases():
    dr, rb, oos = ("leaderboard.season_pass.xp_events_dropped_per_min", "leaderboard.consumer.rebalances_per_min",
                   "leaderboard.season_pass.players_out_of_sync")
    logs, m = _run(10, "before")
    assert set(values(m, LB, dr)) == {0.0}
    assert set(values(m, LB, rb)) == {0.0}
    assert set(values(m, LB, oos)) == {0.0}
    assert _op_attr(logs, LB, "xp_consumer_status", "deployment.version") == {"v4.7.3"}
    assert _op_attr(logs, LB, "xp_consumer_status", "kafka.enable_auto_commit") == {"false"}

    logs, m = _run(10, "during")
    _in(values(m, LB, dr), 300, 1200)
    _in(values(m, LB, rb), 4, 12)
    _in(values(m, LB, oos), 5000, 15000)
    _in([float(v) for v in _op_attr(logs, LB, "xp_consumer_status", "kafka.consumer_group.lag")], 0, 40)
    assert attr_values(logs, LB, "deployment.version") == {"v4.8.0"}
    assert _op_attr(logs, LB, "xp_consumer_status", "kafka.enable_auto_commit") == {"true"}
    assert bodies(logs, LB, "SEASON-PASS-SYNC-FAIL")
    _assert_baseline_chat_lb(m, skip={10})

    logs, m = _run(10, "after", since=_within(10))
    assert set(values(m, LB, dr)) == {0.0}
    assert set(values(m, LB, rb)) == {0.0}
    assert set(values(m, LB, oos)) == {0.0}
    assert _op_attr(logs, LB, "xp_consumer_status", "deployment.version") == {"v4.7.3"}
    assert not bodies(logs, None, "SEASON-PASS-SYNC-FAIL")
    _assert_fix_lines(logs, 10)


def test_ch10_replay_takes_time():
    _, m = _run(10, "after", since=30)
    assert min(values(m, LB, "leaderboard.season_pass.players_out_of_sync")) > 5000
    _, m = _run(10, "after", since=90)
    assert set(values(m, LB, "leaderboard.season_pass.xp_events_dropped_per_min")) == {0.0}


# ── shared: cause events, commands and opsctl end to end ───────────────────

CHANNELS = [7, 8, 9, 10]


def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


@pytest.mark.parametrize("ch", CHANNELS)
def test_cause_event_precedes_symptoms(ch):
    causes = [e for e in REG[ch]["infrastructure_events"]
              if e["offset_seconds"] < 0 and e["attributes"].get("change.request")]
    assert causes
    assert all(-240 <= e["offset_seconds"] <= -60 for e in causes)


def test_change_requests_unique():
    ids = [e["attributes"]["change.request"] for ch in CHANNELS
           for e in REG[ch]["infrastructure_events"] if e["attributes"].get("change.request")]
    assert len(ids) == len(set(ids)) == len(CHANNELS)


@pytest.mark.parametrize("ch", CHANNELS)
def test_expected_commands_parse(ch):
    expected = ops.expected_commands(REG[ch]["remediation_command"], PROFILE)
    assert expected
    assert len(expected) == len(_cmd_lines(ch))


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
    ctl.trigger(ch, user_email="liveops@example.com")
    lines = _cmd_lines(ch)
    for line in lines:
        assert ctl.get_channel_status(ch)["state"] != "STANDBY"
        ops.execute(inst, line)
    assert ctl.get_channel_status(ch)["state"] == "STANDBY"
    with ctl._lock:
        evs, ctl._pending_events = list(ctl._pending_events), []
    audits = [e for e in evs if 'msg="change applied"' in e["body"]]
    assert len(audits) == len(lines), [e["body"] for e in evs]
    owner = {7: CHAT, 8: CHAT, 9: LB, 10: LB}[ch]
    for line, audit in zip(lines, audits):
        assert audit["service"] == owner
        assert line.split()[2] in audit["body"] and "change_id=" in audit["body"]
