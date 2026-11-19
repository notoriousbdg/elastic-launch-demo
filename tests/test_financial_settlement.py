"""Financial settlement channels (06, 07, 08, 09): before / during / after a fault.

Each channel's signal metric on settlement-processor sits in its band before the fault,
deviates during it while the matching status line says ANOMALY with the bad configuration,
and converges back after the opsctl fix (not instantly), with the good configuration only.
"""
from __future__ import annotations

from tests._financial_harness import REG, phase, records, values, within

SVC = "settlement-processor"


def _signal(ch, which, metric, **kw):
    logs, metrics = records(phase(ch, which, **kw))
    vals = values(metrics, SVC, metric)
    assert vals, f"{SVC} emitted no {metric}"
    return logs, metrics, vals


def _status(logs, contains, key="settlement.health"):
    return {r["attrs"].get(key) for r in logs if r["service"] == SVC and contains in r["body"]}


# ── ch06: fills repartitioned 12 -> 24, trade capture pinned to 0-11 ───────

M06 = "settlement.unbooked_fills"
L06 = "trade_capture_health"


def test_ch06_unbooked_fills_phases():
    logs, _, v = _signal(6, "before", M06)
    assert all(0 <= x <= 3 for x in v), v
    assert _status(logs, L06) == {"NOMINAL"}
    assert _status(logs, L06, "kafka.topic.partitions") == {"12"}

    logs, _, v = _signal(6, "during", M06)
    assert all(10000 <= x <= 13500 for x in v), v
    assert _status(logs, L06) == {"ANOMALY"}
    assert _status(logs, L06, "kafka.consumer.assignment") == {"manual:0-11"}
    assert _status(logs, L06, "kafka.topic.partitions_seen") == {"12"}
    assert _status(logs, L06, "kafka.topic.partitions") == {"24"}

    logs, _, v = _signal(6, "after", M06, since=within(6))
    assert all(x < 5 for x in v), v
    assert _status(logs, L06) == {"NOMINAL"}
    assert _status(logs, L06, "kafka.consumer.assignment") == {"group:settlement-fills"}
    assert _status(logs, L06, "kafka.topic.partitions_seen") == {"24"}


def test_ch06_drains_rather_than_snaps():
    _, _, early = _signal(6, "after", M06, since=10)
    assert min(early) > 5000, early


def test_ch06_ramps_after_onset():
    _, _, early = _signal(6, "during", M06, elapsed=5)
    assert max(early) <= 3, early


# ── ch07: ssi-virtu DTC participant 7351 -> 7315 ───────────────────────────

M07 = "settlement.cns_unmatched_instructions"
L07 = "dtc_match_status"


def test_ch07_virtu_unmatched_phases():
    logs, _, v = _signal(7, "before", M07)
    assert all(0 <= x <= 5 for x in v), v
    assert _status(logs, L07) == {"NOMINAL"}

    logs, _, v = _signal(7, "during", M07)
    assert all(400 <= x <= 600 for x in v), v
    assert _status(logs, L07) == {"ANOMALY"}
    assert _status(logs, L07, "ssi.virtu.dtc_participant") == {"7315"}
    assert _status(logs, L07, "ssi.revision") == {"42"}
    others = _status(logs, L07, "settlement.other_unmatched_instructions")
    assert others and all(int(x) <= 4 for x in others), others

    logs, _, v = _signal(7, "after", M07, since=within(7))
    assert all(x < 5 for x in v), v
    assert _status(logs, L07) == {"NOMINAL"}
    assert _status(logs, L07, "ssi.virtu.dtc_participant") == {"7351"}
    assert _status(logs, L07, "ssi.revision") == {"41"}


def test_ch07_converges_rather_than_snaps():
    _, _, early = _signal(7, "after", M07, since=10)
    assert min(early) > 100, early
    _, _, mid = _signal(7, "after", M07, since=75)
    assert all(100 < x < 450 for x in mid), mid


def test_ch07_pending_queue_follows_virtu_backlog():
    _, metrics = records(phase(7, "before"))
    assert all(x <= 260 for x in values(metrics, SVC, "settlement.pending_count"))
    _, metrics = records(phase(7, "during"))
    assert all(x >= 400 for x in values(metrics, SVC, "settlement.pending_count"))
    assert all(x >= 5.0 for x in values(metrics, SVC, "settlement.fail_rate"))
    _, metrics = records(phase(7, "after", since=within(7)))
    assert all(x <= 260 for x in values(metrics, SVC, "settlement.pending_count"))
    assert all(x <= 1.5 for x in values(metrics, SVC, "settlement.fail_rate"))


# ── ch08: equilend-ngt-api-key v14 expired, 401 reported as UNAVAILABLE ────

M08 = "settlement.auto_borrow_failures_per_min"
M08_LACK = "settlement.lack_fails_pending"
L08 = "auto_borrow_client provider="


def test_ch08_auto_borrow_failures_phases():
    logs, metrics, v = _signal(8, "before", M08)
    assert all(0.0 <= x <= 1.0 for x in v), v
    assert all(x < 10 for x in values(metrics, SVC, M08_LACK))
    assert _status(logs, L08) == {"NOMINAL"}
    assert _status(logs, L08, "borrow.http_status") == {"200"}

    logs, metrics, v = _signal(8, "during", M08)
    assert all(20.0 <= x <= 61.0 for x in v), v
    assert all(300 <= x <= 400 for x in values(metrics, SVC, M08_LACK))
    assert _status(logs, L08) == {"ANOMALY"}
    assert _status(logs, L08, "secret.version") == {"v14"}
    assert _status(logs, L08, "borrow.http_status") == {"401"}
    assert _status(logs, L08, "borrow.result") == {"UNAVAILABLE"}

    logs, metrics, v = _signal(8, "after", M08, since=within(8))
    assert all(0.0 <= x <= 1.0 for x in v), v
    assert all(x < 10 for x in values(metrics, SVC, M08_LACK))
    assert _status(logs, L08) == {"NOMINAL"}
    assert _status(logs, L08, "secret.version") == {"v15"}
    assert _status(logs, L08, "borrow.http_status") == {"200"}


def test_ch08_converges_rather_than_snaps():
    _, metrics, early = _signal(8, "after", M08, since=10)
    assert min(early) >= 20.0, early
    assert min(values(metrics, SVC, M08_LACK)) > 300
    _, metrics, fails_gone = _signal(8, "after", M08, since=50)
    assert max(fails_gone) <= 1.0, fails_gone
    assert min(values(metrics, SVC, M08_LACK)) > 100


# ── ch09: 4.3.0 netting cutoff in UTC -> exactly $1,000,000.00 ─────────────

M09 = "settlement.net_mismatch_usd"
L09 = "netting_integrity"


def test_ch09_net_mismatch_phases():
    logs, _, v = _signal(9, "before", M09)
    assert all(x < 0.01 for x in v), v
    assert _status(logs, L09) == {"NOMINAL"}
    assert _status(logs, L09, "deployment.version") == {"4.2.1"}

    logs, _, v = _signal(9, "during", M09)
    assert set(v) == {1000000.0}, v
    assert _status(logs, L09) == {"ANOMALY"}
    assert _status(logs, L09, "deployment.version") == {"4.3.0"}
    assert _status(logs, L09, "netting.cutoff_tz") == {"UTC"}

    logs, _, v = _signal(9, "after", M09, since=within(9))
    assert all(x < 0.01 for x in v), v
    assert _status(logs, L09) == {"NOMINAL"}
    assert _status(logs, L09, "deployment.version") == {"4.2.1"}
    assert _status(logs, L09, "netting.cutoff_tz") == {"America/New_York"}


def test_ch09_steps_not_ramps():
    _, _, before_onset = _signal(9, "during", M09, elapsed=5)
    assert set(before_onset) == {0.0}, before_onset
    _, _, first_run = _signal(9, "during", M09, elapsed=REG[9]["onset_delay_seconds"] + 1)
    assert set(first_run) == {1000000.0}, first_run


def test_ch09_holds_until_recompute():
    _, _, early = _signal(9, "after", M09, since=10)
    assert set(early) == {1000000.0}, early


def test_ch09_error_line_mismatch_matches_metric():
    logs, _ = records(phase(9, "during"))
    errs = [r["body"] for r in logs if "SETTLE-NETTING-CALC" in r["body"]]
    assert errs and all("net_mismatch=$1,000,000.00" in b for b in errs), errs
