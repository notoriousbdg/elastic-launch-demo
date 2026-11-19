"""Gaming matchmaking-engine and content-delivery channels (04, 05, 06, 20): before / during / after.

Each channel's signal sits in its baseline band before the fault, leaves it during the fault
(consistent with fault_params) while the status line carries the bad config value, and
converges back after the opsctl fix with the restored value. Also covers the shared
matchmaking.queue_depth / avg_wait_time_s contract for channels 03 and 19.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from tests._gaming_harness import (
    OPS_ACTOR, PROFILE, REG, SCN, attr_values, bodies, controller, phase, records, values,
)

MM, CDN, GS, LB = "matchmaking-engine", "content-delivery", "game-server", "leaderboard-api"


def _within(ch):
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _run(ch, which, **kw):
    return records(phase(ch, which, **kw))


def _m(metrics, svc, name, **attrs):
    v = [m["value"] for m in metrics if m["service"] == svc and m["name"] == name
         and all(m["attrs"].get(k) == want for k, want in attrs.items())]
    assert v, (svc, name, attrs)
    return v


def _op_attr(logs, svc, op, key, **match):
    return {r["attrs"][key] for r in logs if r["service"] == svc
            and r["attrs"].get("operation") == op and key in r["attrs"]
            and all(r["attrs"].get(k) == want for k, want in match.items())}


def _between(vals, lo, hi):
    return all(lo <= x <= hi for x in vals)


def _cause_logged(logs, ch, svc):
    chg = {e["attributes"]["change.request"] for e in REG[ch]["infrastructure_events"]}
    assert len(chg) == 1
    assert any(r["service"] == svc and r["attrs"].get("change.request") in chg for r in logs), chg


def _my_signals_at_baseline(logs, m, skip=()):
    if 4 not in skip:
        assert set(_m(m, MM, "matchmaking.rating.out_of_range_updates_per_min")) == {0.0}
        assert _between(_m(m, MM, "matchmaking.rating.solver_iterations_p99"), 8, 15)
    if 5 not in skip:
        assert _between(_m(m, CDN, "cdn.cache_hit_rate"), 92.0, 99.0)
        assert _between(_m(m, CDN, "cdn.origin_requests_per_s"), 50, 150)
    if 6 not in skip:
        assert _between(_m(m, CDN, "cdn.bundle_integrity_failures_per_min"), 0, 1)
    if 20 not in skip:
        assert _between(_m(m, MM, "matchmaking.migration.success_rate_pct", **{"migration.route": "use1-usw2"}), 99.0, 100.0)
        assert set(_m(m, MM, "matchmaking.migration.transfer_timeouts_per_min")) == {0.0}
    if 3 not in skip:
        assert _between(_m(m, MM, "matchmaking.queue_depth"), 80, 400)
        assert _between(_m(m, MM, "matchmaking.avg_wait_time_s"), 8, 25)


def test_before_everything_at_baseline():
    logs, m = _run(4, "before")
    _my_signals_at_baseline(logs, m)
    assert _op_attr(logs, MM, "rating_status", "deployment.version") == {"v3.0.0"}
    assert {int(v) for v in _op_attr(logs, CDN, "cache_status", "cache.memcached_limit_mb", **{"cache.cluster": "asset-memcached"})} == {12288}
    assert _op_attr(logs, CDN, "publisher_status", "config.publish_path_mode") == {"versioned"}
    assert {int(v) for v in _op_attr(logs, MM, "migration_status", "network.tunnel_mtu", **{"migration.route": "use1-usw2"})} == {1380}
    assert not [b for b in bodies(logs) if any(c in b for c in (
        "MM-SKILL-RATING-DIVERGE", "MEMCACHED-MISS-STORM", "CDN-ASSET-CORRUPT", "MM-MIGRATION-FAIL"))]


# ── ch04: matchmaking-engine v3.0.1 Glicko-2 solver ────────────────────────

def test_ch04_phases():
    oor, p99 = "matchmaking.rating.out_of_range_updates_per_min", "matchmaking.rating.solver_iterations_p99"
    logs, m = _run(4, "during")
    assert _between(_m(m, MM, oor), 40, 120), _m(m, MM, oor)
    assert set(_m(m, MM, p99)) == {100.0}
    assert _op_attr(logs, MM, "rating_status", "deployment.version") == {"v3.0.1"}
    assert _op_attr(logs, MM, "rating_status", "rating.status") == {"degraded"}
    assert bodies(logs, MM, "MM-SKILL-RATING-DIVERGE")
    assert not bodies(logs, LB, "MM-SKILL-RATING-DIVERGE")
    assert bodies(logs, LB, "rank_write rejected")
    _cause_logged(logs, 4, MM)
    _my_signals_at_baseline(logs, m, skip=(4,))

    logs, m = _run(4, "after", since=_within(4))
    assert set(_m(m, MM, oor)) == {0.0}
    assert _between(_m(m, MM, p99), 8, 15)
    assert _op_attr(logs, MM, "rating_status", "deployment.version") == {"v3.0.0"}
    assert _op_attr(logs, MM, "rating_status", "rating.status") == {"healthy"}
    assert not bodies(logs, None, "MM-SKILL-RATING-DIVERGE")
    assert bodies(logs, MM, "players_corrected=")


def test_ch04_converges_not_instant():
    _, m = _run(4, "after", since=10)
    assert min(_m(m, MM, "matchmaking.rating.solver_iterations_p99")) > 50
    assert min(_m(m, MM, "matchmaking.rating.out_of_range_updates_per_min")) > 0


# ── ch05: asset-memcached at the 1024 MB default ───────────────────────────

def test_ch05_phases():
    hit, origin = "cdn.cache_hit_rate", "cdn.origin_requests_per_s"
    lim = ("cache_status", "cache.memcached_limit_mb")
    logs, m = _run(5, "during")
    assert _between(_m(m, CDN, hit), 30.0, 55.0), _m(m, CDN, hit)
    assert _between(_m(m, CDN, origin), 2000, 4000), _m(m, CDN, origin)
    assert {int(v) for v in _op_attr(logs, CDN, *lim, **{"cache.cluster": "asset-memcached"})} == {1024}
    assert {int(v) for v in _op_attr(logs, CDN, *lim, **{"cache.cluster": "manifest-memcached"})} == {4096}
    assert _op_attr(logs, CDN, "cache_status", "cache.status", **{"cache.cluster": "asset-memcached"}) == {"degraded"}
    assert bodies(logs, CDN, "MEMCACHED-MISS-STORM")
    assert not bodies(logs, GS, "MEMCACHED-MISS-STORM")
    assert bodies(logs, GS, "asset_stream timeout")
    _cause_logged(logs, 5, CDN)
    _my_signals_at_baseline(logs, m, skip=(5,))

    logs, m = _run(5, "after", since=_within(5))
    assert _between(_m(m, CDN, hit), 92.0, 99.0)
    assert _between(_m(m, CDN, origin), 50, 150)
    assert {int(v) for v in _op_attr(logs, CDN, *lim, **{"cache.cluster": "asset-memcached"})} == {12288}
    assert not bodies(logs, None, "MEMCACHED-MISS-STORM")


def test_ch05_cache_warms_over_minutes():
    _, m = _run(5, "after", since=60)
    assert max(_m(m, CDN, "cdn.cache_hit_rate")) < 80.0
    assert min(_m(m, CDN, "cdn.origin_requests_per_s")) > 800
    _, m = _run(5, "after", since=200)
    assert max(_m(m, CDN, "cdn.cache_hit_rate")) < 92.0


def test_ch05_generator_hold_matches_warm_up():
    assert REG[5]["generator_hold_seconds"] == {"memcached_otel": 300}
    assert REG[5]["remediation_effects"]["transition_seconds"] == 300


# ── ch06: asset-publisher mutable path ─────────────────────────────────────

def test_ch06_phases():
    fail = "cdn.bundle_integrity_failures_per_min"
    logs, m = _run(6, "during")
    assert _between(_m(m, CDN, fail), 200, 600), _m(m, CDN, fail)
    assert _op_attr(logs, CDN, "publisher_status", "config.publish_path_mode") == {"mutable"}
    assert _op_attr(logs, CDN, "publisher_status", "cdn.publish_prefix") == {"bundles/latest/"}
    assert bodies(logs, CDN, "CDN-ASSET-CORRUPT")
    assert not bodies(logs, GS, "CDN-ASSET-CORRUPT")
    assert bodies(logs, GS, "client patch verify failed")
    _cause_logged(logs, 6, CDN)
    _my_signals_at_baseline(logs, m, skip=(6,))

    logs, m = _run(6, "after", since=_within(6))
    assert _between(_m(m, CDN, fail), 0, 1)
    assert _op_attr(logs, CDN, "publisher_status", "config.publish_path_mode") == {"versioned"}
    assert not bodies(logs, None, "CDN-ASSET-CORRUPT")


def test_ch06_converges_not_instant():
    _, m = _run(6, "after", since=20)
    assert min(_m(m, CDN, "cdn.bundle_integrity_failures_per_min")) > 100


# ── ch20: wg-use1-usw2 MTU 1440 ────────────────────────────────────────────

USW2, EUW1 = {"migration.route": "use1-usw2"}, {"migration.route": "use1-euw1"}


def test_ch20_phases():
    sr, to = "matchmaking.migration.success_rate_pct", "matchmaking.migration.transfer_timeouts_per_min"
    logs, m = _run(20, "during")
    assert _between(_m(m, MM, sr, **USW2), 5.0, 20.0), _m(m, MM, sr, **USW2)
    assert _between(_m(m, MM, sr, **EUW1), 99.0, 100.0)
    assert _between(_m(m, MM, to, **USW2), 60, 200)
    assert set(_m(m, MM, to, **EUW1)) == {0.0}
    assert {int(v) for v in _op_attr(logs, MM, "migration_status", "network.tunnel_mtu", **USW2)} == {1440}
    assert {int(v) for v in _op_attr(logs, MM, "migration_status", "network.tunnel_mtu", **EUW1)} == {1380}
    assert bodies(logs, MM, "MM-MIGRATION-FAIL")
    assert not bodies(logs, GS, "MM-MIGRATION-FAIL")
    assert bodies(logs, GS, "state_export to us-west-2 stalled")
    _cause_logged(logs, 20, MM)
    _my_signals_at_baseline(logs, m, skip=(20,))

    logs, m = _run(20, "after", since=_within(20))
    assert _between(_m(m, MM, sr, **USW2), 99.0, 100.0)
    assert set(_m(m, MM, to, **USW2)) == {0.0}
    assert {int(v) for v in _op_attr(logs, MM, "migration_status", "network.tunnel_mtu", **USW2)} == {1380}
    assert not bodies(logs, None, "MM-MIGRATION-FAIL")


def test_ch20_converges_not_instant():
    _, m = _run(20, "after", since=10)
    assert max(_m(m, MM, "matchmaking.migration.success_rate_pct", **USW2)) < 80.0


# ── shared contract: queue depth / wait for ch03 and ch19 ───────────────────

def test_queue_contract_ch03():
    q, w = "matchmaking.queue_depth", "matchmaking.avg_wait_time_s"
    _, m = _run(3, "during")
    assert _between(_m(m, MM, q), 3000, 6000), _m(m, MM, q)
    assert _between(_m(m, MM, w), 120.0, 300.0)
    _, m = _run(3, "during", elapsed=20)
    assert max(_m(m, MM, q)) < 3000  # still ramping over 90 s
    _, m = _run(3, "after", since=60)
    assert min(_m(m, MM, q)) > 1000  # drains over 240 s, not a snap
    _, m = _run(3, "after", since=240)
    assert _between(_m(m, MM, q), 80, 400)
    assert _between(_m(m, MM, w), 8.0, 25.0)


def test_queue_contract_ch19():
    q, w = "matchmaking.queue_depth", "matchmaking.avg_wait_time_s"
    _, m = _run(19, "during")
    assert _between(_m(m, MM, q), 600, 1200), _m(m, MM, q)
    assert _between(_m(m, MM, w), 8.0, 25.0)
    _, m = _run(19, "after", since=120)
    assert _between(_m(m, MM, q), 80, 400)


@pytest.mark.parametrize("other", [1, 2, 7, 11, 15])
def test_other_channels_leave_my_signals_alone(other):
    logs, m = _run(other, "during")
    _my_signals_at_baseline(logs, m)


# ── opsctl: commands parse and leave one audit line each on the owning service ──

CHANNELS = {4: MM, 5: CDN, 6: CDN, 20: MM}


def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _ops_inst():
    ctl = controller()
    return SimpleNamespace(
        chaos_controller=ctl,
        ctx=SimpleNamespace(services={s: {} for s in SCN.services}, scenario=SCN),
        resolve_channel=lambda ch, **kw: ctl.resolve(ch, **kw),
    )


@pytest.mark.parametrize("ch", sorted(CHANNELS))
def test_expected_commands_parse(ch):
    exp = ops.expected_commands(REG[ch]["remediation_command"], PROFILE)
    assert exp and len(exp) == len(_cmd_lines(ch))


@pytest.mark.parametrize("ch", sorted(CHANNELS))
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
        evs = list(ctl._pending_events)
    audits = [e for e in evs if 'msg="change applied"' in e["body"]]
    assert len(audits) == len(lines), [e["body"] for e in evs]
    for line, a in zip(lines, audits):
        assert a["service"] == CHANNELS[ch]
        assert f'cmd="{line}"' in a["body"] and f"actor={OPS_ACTOR}" in a["body"]
        assert a["attributes"]["change.target"] == line.split()[2]
