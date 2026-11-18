"""Gaming analytics and moderation channels (15, 16, 17, 18): before / during / after a fault.

Each channel's signal sits in its normal band before the fault, moves out of band during it
(consistent with fault_params) while the status log says DEGRADED with the bad config value,
and converges back after the opsctl fix with the restored value on the status line. The
other channels leave these signals at baseline.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from tests._gaming_harness import (
    OPS_ACTOR, PROFILE, REG, SCN, bodies, controller, phase, records, values,
)

AP, ME = "analytics-pipeline", "moderation-engine"
CHANNELS = [15, 16, 17, 18]
ERROR_TYPE = {ch: REG[ch]["error_type"] for ch in CHANNELS}


def _within(ch):
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _run(ch, which, **kw):
    return records(phase(ch, which, **kw))


def _v(metrics, svc, name):
    v = values(metrics, svc, name)
    assert v, (svc, name)
    return v


def _status(logs, svc, op, key):
    return {r["attrs"][key] for r in logs
            if r["service"] == svc and r["attrs"].get("operation") == op and key in r["attrs"]}


def _between(xs, lo, hi):
    return all(lo <= x <= hi for x in xs)


# ── ch15: analytics-consumer max.poll.records 500 -> 5000 ──────────────────

def _cg(logs, key):
    return {r["attrs"][key] for r in logs if r["service"] == AP
            and r["attrs"].get("operation") == "pipeline_health"
            and r["attrs"].get("pipeline.name") == "events-primary"}


def test_ch15_consumer_group_phases():
    lag, mem, reb = "analytics.pipeline_lag_s", "analytics.consumer.active_members", "analytics.consumer.rebalances_per_min"
    logs, m = _run(15, "before")
    assert _between(_v(m, AP, lag), 1.0, 5.0)
    assert set(_v(m, AP, mem)) == {8.0}
    assert set(_v(m, AP, reb)) == {0.0}
    assert {int(v) for v in _cg(logs, "config.max_poll_records")} == {500}
    assert _cg(logs, "analytics.consumer_status") == {"HEALTHY"}

    logs, m = _run(15, "during", elapsed=900)
    assert _between(_v(m, AP, lag), 600.0, 1800.0), _v(m, AP, lag)
    assert _between(_v(m, AP, mem), 2, 5)
    assert _between(_v(m, AP, reb), 6, 15)
    assert {int(v) for v in _cg(logs, "config.max_poll_records")} == {5000}
    assert _cg(logs, "analytics.consumer_status") == {"DEGRADED"}
    assert bodies(logs, AP, "Commit cannot be completed")
    assert bodies(logs, AP, ERROR_TYPE[15])
    # the peer consumer groups keep all their members
    peers = [r for r in logs if r["service"] == AP and r["attrs"].get("pipeline.name") in ("events-secondary", "replay-pipeline")]
    assert peers and {r["attrs"]["analytics.consumer_status"] for r in peers} == {"HEALTHY"}

    logs, m = _run(15, "after", since=_within(15))
    assert _between(_v(m, AP, lag), 1.0, 5.0), _v(m, AP, lag)
    assert set(_v(m, AP, mem)) == {8.0}
    assert set(_v(m, AP, reb)) == {0.0}
    assert {int(v) for v in _cg(logs, "config.max_poll_records")} == {500}
    assert _cg(logs, "analytics.consumer_status") == {"HEALTHY"}
    assert not bodies(logs, None, ERROR_TYPE[15])
    _assert_audit(logs, 15, AP)


def test_ch15_lag_grows_then_drains():
    _, m = _run(15, "during", elapsed=10)  # before the onset
    assert max(_v(m, AP, "analytics.pipeline_lag_s")) <= 5.0
    _, m = _run(15, "during", elapsed=300)
    early = min(_v(m, AP, "analytics.pipeline_lag_s"))
    _, m = _run(15, "during", elapsed=900)
    assert min(_v(m, AP, "analytics.pipeline_lag_s")) > early + 500
    # 60 s into a 300 s drain: still far above baseline
    _, m = _run(15, "after", since=60)
    assert min(_v(m, AP, "analytics.pipeline_lag_s")) > 300


# ── ch16: game-telemetry-sampling player_move 1/5 -> 1/1 ──────────────────

def test_ch16_ingest_buffer_phases():
    buf, drop, recv, thr = ("analytics.buffer_usage_pct", "analytics.events_dropped_per_s",
                            "analytics.events_received_per_s", "analytics.throughput")
    logs, m = _run(16, "before")
    assert _between(_v(m, AP, buf), 20.0, 45.0)
    assert set(_v(m, AP, drop)) == {0.0}
    assert _between(_v(m, AP, recv), 5000, 7000)
    assert _status(logs, AP, "buffer_status", "config.player_move_sample_rate") == {"1/5"}
    assert _status(logs, AP, "buffer_status", "analytics.buffer_status") == {"HEALTHY"}

    logs, m = _run(16, "during")
    assert _between(_v(m, AP, buf), 95.0, 100.0)
    assert _between(_v(m, AP, drop), 5000, 20000), _v(m, AP, drop)
    assert _between(_v(m, AP, recv), 13000, 19000)
    assert _between(_v(m, AP, thr), 7800, 8000)
    assert _status(logs, AP, "buffer_status", "config.player_move_sample_rate") == {"1/1"}
    assert _status(logs, AP, "buffer_status", "analytics.buffer_status") == {"DEGRADED"}
    assert bodies(logs, AP, ERROR_TYPE[16])
    assert bodies(logs, "game-server", "returned 503")
    assert not bodies(logs, "game-server", ERROR_TYPE[16])

    logs, m = _run(16, "after", since=_within(16))
    assert _between(_v(m, AP, buf), 20.0, 45.0), _v(m, AP, buf)
    assert set(_v(m, AP, drop)) == {0.0}
    assert _between(_v(m, AP, recv), 5000, 7000)
    assert _status(logs, AP, "buffer_status", "config.player_move_sample_rate") == {"1/5"}
    assert _status(logs, AP, "buffer_status", "analytics.buffer_status") == {"HEALTHY"}
    assert not bodies(logs, None, ERROR_TYPE[16])
    _assert_audit(logs, 16, "game-server")


def test_ch16_buffer_drains_not_instant():
    _, m = _run(16, "during", elapsed=5)
    assert max(_v(m, AP, "analytics.buffer_usage_pct")) <= 45.0
    _, m = _run(16, "after", since=10)
    assert min(_v(m, AP, "analytics.buffer_usage_pct")) >= 95.0
    _, m = _run(16, "after", since=70)
    assert 45.0 < min(_v(m, AP, "analytics.buffer_usage_pct")) < 95.0
    assert set(_v(m, AP, "analytics.events_dropped_per_s")) == {0.0}


# ── ch17: automod-text v6.2 (hate_speech threshold 0.85 -> 0.55) ──────────

def test_ch17_automod_phases():
    act, fp = "moderation.actions_taken", "moderation.false_positive_rate"
    logs, m = _run(17, "before")
    assert _between(_v(m, ME, act), 50, 150)
    assert _between(_v(m, ME, fp), 1.0, 4.0)
    assert _status(logs, ME, "automod_status", "model.version") == {"v6.1"}
    assert _status(logs, ME, "automod_status", "model.hate_speech_threshold") == {0.85}
    assert _status(logs, ME, "content_scanned", "moderation.model") == {"v6.1"}

    logs, m = _run(17, "during")
    assert _between(_v(m, ME, act), 2000, 5000)
    assert _between(_v(m, ME, fp), 30.0, 60.0)
    assert _status(logs, ME, "automod_status", "model.version") == {"v6.2"}
    assert _status(logs, ME, "automod_status", "moderation.status") == {"DEGRADED"}
    assert bodies(logs, ME, ERROR_TYPE[17])
    assert bodies(logs, "chat-service", "message withheld by moderation-engine")

    logs, m = _run(17, "after", since=_within(17))
    assert _between(_v(m, ME, act), 50, 150)
    assert _between(_v(m, ME, fp), 1.0, 4.0)
    assert _status(logs, ME, "automod_status", "model.version") == {"v6.1"}
    assert _status(logs, ME, "automod_status", "moderation.status") == {"HEALTHY"}
    assert not bodies(logs, None, ERROR_TYPE[17])
    _assert_audit(logs, 17, ME)


def test_ch17_overturn_rate_converges_slower_than_actions():
    _, m = _run(17, "after", since=60)
    assert _between(_v(m, ME, "moderation.actions_taken"), 50, 150)
    assert min(_v(m, ME, "moderation.false_positive_rate")) > 20.0


# ── ch18: report-classifier inference batch size 32 -> 128 ────────────────

def test_ch18_report_queue_phases():
    q, w = "moderation.report_queue_depth", "moderation.classifier.workers_ready"
    logs, m = _run(18, "before")
    assert _between(_v(m, ME, q), 50, 300)
    assert set(_v(m, ME, w)) == {12.0}
    assert {int(v) for v in _status(logs, ME, "classifier_status", "config.inference_batch_size")} == {32}
    assert _status(logs, ME, "classifier_status", "moderation.classifier_status") == {"HEALTHY"}

    logs, m = _run(18, "during", elapsed=900)
    assert _between(_v(m, ME, q), 20000, 60000), _v(m, ME, q)
    assert _between(_v(m, ME, w), 0, 3)
    assert {int(v) for v in _status(logs, ME, "classifier_status", "config.inference_batch_size")} == {128}
    assert _status(logs, ME, "classifier_status", "moderation.classifier_status") == {"DEGRADED"}
    assert bodies(logs, ME, "CUDA out of memory")
    assert bodies(logs, ME, ERROR_TYPE[18])

    logs, m = _run(18, "after", since=_within(18))
    assert _between(_v(m, ME, q), 50, 300), _v(m, ME, q)
    assert set(_v(m, ME, w)) == {12.0}
    assert {int(v) for v in _status(logs, ME, "classifier_status", "config.inference_batch_size")} == {32}
    assert _status(logs, ME, "classifier_status", "moderation.classifier_status") == {"HEALTHY"}
    assert not bodies(logs, None, ERROR_TYPE[18])
    assert not bodies(logs, ME, "CUDA out of memory")
    _assert_audit(logs, 18, ME)


def test_ch18_queue_drains_not_instant():
    _, m = _run(18, "after", since=10)
    assert min(_v(m, ME, "moderation.report_queue_depth")) > 10000
    assert max(_v(m, ME, "moderation.classifier.workers_ready")) < 12
    _, m = _run(18, "after", since=150)
    assert 300 < min(_v(m, ME, "moderation.report_queue_depth")) < 30000


# ── isolation: other channels leave these signals at baseline ──────────────

BASELINE = [
    (AP, "analytics.pipeline_lag_s", 1.0, 5.0),
    (AP, "analytics.consumer.active_members", 8, 8),
    (AP, "analytics.buffer_usage_pct", 20.0, 45.0),
    (AP, "analytics.events_dropped_per_s", 0, 0),
    (ME, "moderation.actions_taken", 50, 150),
    (ME, "moderation.false_positive_rate", 1.0, 4.0),
    (ME, "moderation.report_queue_depth", 50, 300),
    (ME, "moderation.classifier.workers_ready", 12, 12),
]
OWN = {15: {"analytics.pipeline_lag_s", "analytics.consumer.active_members"},
       16: {"analytics.buffer_usage_pct", "analytics.events_dropped_per_s"},
       17: {"moderation.actions_taken", "moderation.false_positive_rate"},
       18: {"moderation.report_queue_depth", "moderation.classifier.workers_ready"}}


@pytest.mark.parametrize("ch", sorted(REG))
def test_other_channels_leave_signals_at_baseline(ch):
    _, m = _run(ch, "during")
    for svc, name, lo, hi in BASELINE:
        if name in OWN.get(ch, set()):
            continue
        assert _between(_v(m, svc, name), lo, hi), (ch, name, _v(m, svc, name))


# ── shared: audit lines and opsctl end to end ──────────────────────────────

def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _assert_audit(logs, ch, svc):
    for line in _cmd_lines(ch):
        target = line.split()[2]
        audits = bodies(logs, svc, f'cmd="{line}"')
        assert any(f"actor={OPS_ACTOR}" in a and target in a for a in audits), (ch, audits)


def _ops_inst():
    ctl = controller()
    return SimpleNamespace(
        chaos_controller=ctl,
        ctx=SimpleNamespace(services={s: {} for s in SCN.services}, scenario=SCN),
        resolve_channel=lambda ch, **kw: ctl.resolve(ch, **kw),
    )


@pytest.mark.parametrize("ch", CHANNELS)
def test_expected_commands_parse(ch):
    parsed = ops.expected_commands(REG[ch]["remediation_command"], PROFILE)
    assert parsed and len(parsed) == len(_cmd_lines(ch))


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
        for e in ctl._pending_events:
            e["emit_at"] = 0
        evs, ctl._pending_events = list(ctl._pending_events), []
    audits = [e["body"] for e in evs if 'logger=changelog msg="change applied"' in e["body"]]
    assert len(audits) == len(lines), [e["body"] for e in evs]
    assert all("change_id=chg-" in a for a in audits)
