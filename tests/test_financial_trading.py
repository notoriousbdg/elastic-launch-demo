"""Financial trading channels (01, 16, 19, 20): before / during / after a fault.

Each channel's signal metric sits in its band before the fault, leaves it during the fault
while the component's status line says ANOMALY and carries the bad configuration, and
converges back after the opsctl fix (not instantly) with the good configuration on the
status line. The order-gateway metrics other channels rely on (kill switch, fraud holds,
session expiry cancels) move with those channels too.
"""
from __future__ import annotations

import pytest

from tests._financial_harness import REG, attr_values, bodies, phase, records, within

ME = "matching-engine"
OG = "order-gateway"


def _vals(metrics, svc, name, key=None, value=None):
    out = [m["value"] for m in metrics if m["service"] == svc and m["name"] == name
           and (key is None or m["attrs"].get(key) == value)]
    assert out, f"{svc} emitted no {name} ({key}={value})"
    return out


def _lines(logs, svc, contains):
    return [r for r in logs if r["service"] == svc and contains in r["body"]]


def _attr(logs, svc, contains, key):
    return {r["attrs"].get(key) for r in _lines(logs, svc, contains)}


# ── ch01: P-03 primary on 5.2.0 (journal v14), standby 5.1.1 cannot replay ───

P03 = "replication_status partition=P-03"
LAG = "matching_engine.standby_seq_lag_msgs"


def test_ch01_standby_lag_phases():
    logs, m = records(phase(1, "before"))
    assert all(v <= 5 for v in _vals(m, ME, LAG, "me.partition", "P-03"))
    assert _attr(logs, ME, P03, "replication.status") == {"NOMINAL"}
    assert _attr(logs, ME, P03, "deployment.version") == {"5.1.1"}

    logs, m = records(phase(1, "during"))
    assert all(v > 50000 for v in _vals(m, ME, LAG, "me.partition", "P-03"))
    assert _attr(logs, ME, P03, "replication.status") == {"ANOMALY"}
    assert _attr(logs, ME, P03, "deployment.version") == {"5.2.0"}
    assert _attr(logs, ME, P03, "me.journal_schema") == {"v14"}
    assert bodies(logs, ME, "OMS-BOOK-IMBALANCE")
    assert not bodies(logs, OG, "OMS-BOOK-IMBALANCE")
    assert bodies(logs, OG, "drop copy reconciliation break")
    others = [r for r in logs if r["service"] == ME and "replication_status" in r["body"]
              and r["attrs"].get("me.partition") != "P-03"]
    assert others and {r["attrs"]["replication.status"] for r in others} == {"NOMINAL"}
    assert all(v > 100 for v in _vals(m, OG, "order_gateway.drop_copy_breaks"))

    logs, m = records(phase(1, "after", since=within(1)))
    assert all(v < 10 for v in _vals(m, ME, LAG, "me.partition", "P-03"))
    assert _attr(logs, ME, P03, "replication.status") == {"NOMINAL"}
    assert attr_values(logs, ME, "deployment.version") == {"5.1.1"}
    assert all(v == 0 for v in _vals(m, OG, "order_gateway.drop_copy_breaks"))
    assert not bodies(logs, None, "OMS-BOOK-IMBALANCE")


def test_ch01_converges_rather_than_snaps():
    _, m = records(phase(1, "after", since=10))
    assert all(v > 1000 for v in _vals(m, ME, LAG, "me.partition", "P-03"))
    _, m = records(phase(1, "after", since=60))
    assert all(10 < v < 14000 for v in _vals(m, ME, LAG, "me.partition", "P-03"))


# ── ch16: P-07 Burstable, no exclusive cores ──────────────────────────────

P07 = "partition_health partition=P-07"
P99 = "matching_engine.match_latency_p99_us"
NVCSW = "matching_engine.nonvoluntary_ctx_switches_per_s"


def test_ch16_match_latency_phases():
    logs, m = records(phase(16, "before"))
    assert all(40 <= v <= 90 for v in _vals(m, ME, P99, "me.partition", "P-07"))
    assert all(v < 50 for v in _vals(m, ME, NVCSW, "me.partition", "P-07"))
    assert _attr(logs, ME, P07, "partition.status") == {"NOMINAL"}
    assert _attr(logs, ME, P07, "k8s.pod.qos_class") == {"Guaranteed"}

    logs, m = records(phase(16, "during"))
    assert all(12700 <= v <= 50000 for v in _vals(m, ME, P99, "me.partition", "P-07"))
    assert all(8000 <= v <= 20000 for v in _vals(m, ME, NVCSW, "me.partition", "P-07"))
    assert all(v <= 2.0 for v in _vals(m, ME, "matching_engine.cpu_throttled_pct", "me.partition", "P-07"))
    assert all(8800 <= v <= 12000 for v in _vals(m, OG, "order_gateway.orders_per_min"))
    assert _attr(logs, ME, P07, "partition.status") == {"ANOMALY"}
    assert _attr(logs, ME, P07, "k8s.pod.qos_class") == {"Burstable"}
    assert _attr(logs, ME, P07, "cpu.exclusive_cores") == {"0"}
    others = [m_ for m_ in m if m_["service"] == ME and m_["name"] == P99
              and m_["attrs"].get("me.partition") != "P-07"]
    assert others and all(o["value"] <= 90 for o in others)
    assert bodies(logs, ME, "ME-LATENCY-SLA")
    assert not bodies(logs, OG, "ME-LATENCY-SLA")

    logs, m = records(phase(16, "after", since=within(16)))
    assert all(v < 200 for v in _vals(m, ME, P99, "me.partition", "P-07"))
    assert all(v < 50 for v in _vals(m, ME, NVCSW, "me.partition", "P-07"))
    assert _attr(logs, ME, P07, "partition.status") == {"NOMINAL"}
    assert attr_values(logs, ME, "k8s.pod.qos_class") == {"Guaranteed"}
    assert _attr(logs, ME, P07, "cpu.exclusive_cores") == {"8"}
    assert not bodies(logs, None, "ME-LATENCY-SLA")


def test_ch16_converges_rather_than_snaps():
    _, m = records(phase(16, "after", since=10))
    assert all(v > 10000 for v in _vals(m, ME, P99, "me.partition", "P-07"))
    _, m = records(phase(16, "after", since=35))
    assert all(200 < v < 50000 for v in _vals(m, ME, P99, "me.partition", "P-07"))


# ── ch19: FIX-NSDQ-07 canary with LANG=de_DE.UTF-8 ────────────────────────

NSDQ7 = "fix_session_status session=FIX-NSDQ-07"
REJ = "order_gateway.fix_session_rejects_per_min"


def test_ch19_fix_session_rejects_phases():
    logs, m = records(phase(19, "before"))
    assert all(v <= 1 for v in _vals(m, OG, REJ, "fix.session", "FIX-NSDQ-07"))
    assert _attr(logs, OG, NSDQ7, "session.status") == {"NOMINAL"}
    assert _attr(logs, OG, NSDQ7, "process.locale") == {"C.UTF-8"}

    logs, m = records(phase(19, "during"))
    assert all(300 <= v <= 900 for v in _vals(m, OG, REJ, "fix.session", "FIX-NSDQ-07"))
    others = [m_ for m_ in m if m_["service"] == OG and m_["name"] == REJ
              and m_["attrs"].get("fix.session") != "FIX-NSDQ-07"]
    assert others and all(o["value"] <= 1 for o in others)
    assert _attr(logs, OG, NSDQ7, "session.status") == {"ANOMALY"}
    assert _attr(logs, OG, NSDQ7, "process.locale") == {"de_DE.UTF-8"}
    assert _attr(logs, OG, NSDQ7, "deployment.version") == {"3.5.0"}
    errs = bodies(logs, OG, "FIX-PARSE-REJECT")
    assert errs and all("35=3" in b and "373=6" in b and "44=" in b and "," in b for b in errs)
    assert not bodies(logs, "audit-logger", "FIX-PARSE-REJECT")
    assert all(v > 3.0 for v in _vals(m, OG, "order_gateway.reject_rate"))

    logs, m = records(phase(19, "after", since=within(19)))
    assert all(v <= 1 for v in _vals(m, OG, REJ, "fix.session", "FIX-NSDQ-07"))
    assert _attr(logs, OG, NSDQ7, "session.status") == {"NOMINAL"}
    assert attr_values(logs, OG, "process.locale") == {"C.UTF-8"}
    assert attr_values(logs, OG, "deployment.version") == {"3.4.1"}
    assert not bodies(logs, None, "FIX-PARSE-REJECT")


def test_ch19_converges_rather_than_snaps():
    _, m = records(phase(19, "after", since=10))
    assert all(v >= 300 for v in _vals(m, OG, REJ, "fix.session", "FIX-NSDQ-07"))
    _, m = records(phase(19, "after", since=45))
    assert all(1 < v < 900 for v in _vals(m, OG, REJ, "fix.session", "FIX-NSDQ-07"))


def test_ch19_session_list_has_the_canary_session():
    from tests._financial_harness import SCN  # noqa: F401
    logs, _ = records(phase(19, "before"), cycles=40)
    assert "FIX-NSDQ-07" in attr_values(logs, OG, "fix.session")


# ── ch20: sor-venue-profiles min_qty rename -> SIGMA-X minimum 0 ──────────

SIGX = "venue_status venue=SIGMA-X"
DP = "matching_engine.darkpool_rejects_per_min"


def test_ch20_darkpool_rejects_phases():
    logs, m = records(phase(20, "before"))
    assert all(v <= 2 for v in _vals(m, ME, DP, "sor.venue", "SIGMA-X"))
    assert _attr(logs, ME, SIGX, "router.status") == {"NOMINAL"}
    assert _attr(logs, ME, SIGX, "config.revision") == {"57"}
    assert _attr(logs, ME, SIGX, "sor.sigma_x_min_qty") == {"10000"}

    logs, m = records(phase(20, "during"))
    assert all(40 <= v <= 120 for v in _vals(m, ME, DP, "sor.venue", "SIGMA-X"))
    others = [m_ for m_ in m if m_["service"] == ME and m_["name"] == DP
              and m_["attrs"].get("sor.venue") != "SIGMA-X"]
    assert others and all(o["value"] <= 2 for o in others)
    assert _attr(logs, ME, SIGX, "router.status") == {"ANOMALY"}
    assert _attr(logs, ME, SIGX, "config.revision") == {"58"}
    assert _attr(logs, ME, SIGX, "sor.sigma_x_min_qty") == {"0"}
    assert bodies(logs, ME, "ME-DARKPOOL-REJECT")
    assert not bodies(logs, OG, "ME-DARKPOOL-REJECT")

    logs, m = records(phase(20, "after", since=within(20)))
    assert all(v <= 2 for v in _vals(m, ME, DP, "sor.venue", "SIGMA-X"))
    assert _attr(logs, ME, SIGX, "router.status") == {"NOMINAL"}
    assert attr_values(logs, ME, "config.revision") == {"57"}
    assert _attr(logs, ME, SIGX, "sor.sigma_x_min_qty") == {"10000"}
    assert not bodies(logs, None, "ME-DARKPOOL-REJECT")


def test_ch20_converges_rather_than_snaps():
    _, m = records(phase(20, "after", since=5))
    assert all(v >= 40 for v in _vals(m, ME, DP, "sor.venue", "SIGMA-X"))
    _, m = records(phase(20, "after", since=25))
    assert all(2 < v < 120 for v in _vals(m, ME, DP, "sor.venue", "SIGMA-X"))


# ── order-gateway metrics other channels name in their verification ───────

@pytest.mark.parametrize("ch, metric, lo, hi, after_s", [
    (4, "order_gateway.kill_switch_blocked_per_min", 150, 402, 45),
    (10, "order_gateway.fraud_hold_queue_depth", 50, 500, 90),
    (13, "order_gateway.session_expire_cancels_per_min", 20, 122, 30),
])
def test_cross_group_order_gateway_metrics(ch, metric, lo, hi, after_s):
    baseline = 5 if ch == 10 else 2
    _, m = records(phase(ch, "before"))
    assert all(v <= baseline for v in _vals(m, OG, metric))
    _, m = records(phase(ch, "during"))
    assert all(lo <= v <= hi for v in _vals(m, OG, metric)), _vals(m, OG, metric)
    _, m = records(phase(ch, "after", since=5))
    assert all(v > baseline for v in _vals(m, OG, metric))
    _, m = records(phase(ch, "after", since=after_s))
    assert all(v <= baseline for v in _vals(m, OG, metric))


def _mean(xs):
    return sum(xs) / len(xs)


@pytest.mark.parametrize("ch, rise", [
    (4, 1.0), (10, 0.8), (12, 0.15), (13, 0.15), (16, 0.15), (19, 2.5), (20, 0.15),
])
def test_reject_rate_rises_with_the_channels_that_reject_orders(ch, rise):
    _, m = records(phase(ch, "before"), cycles=100)
    before = _vals(m, OG, "order_gateway.reject_rate")
    assert max(before) <= 2.6
    _, m = records(phase(ch, "during"), cycles=100)
    assert _mean(_vals(m, OG, "order_gateway.reject_rate")) > _mean(before) + rise, ch


def test_reroutes_follow_darkpool_and_nasdaq_rejects():
    _, m = records(phase(19, "during"))
    assert all(v > 250 for v in _vals(m, ME, "matching_engine.venue_reroutes_per_min"))
    _, m = records(phase(20, "during"))
    assert all(v > 40 for v in _vals(m, ME, "matching_engine.venue_reroutes_per_min"))
    _, m = records(phase(19, "before"))
    assert all(v <= 12 for v in _vals(m, ME, "matching_engine.venue_reroutes_per_min"))


def test_error_type_is_in_error_message():
    for ch in (1, 16, 19, 20):
        assert REG[ch]["error_type"] in REG[ch]["error_message"]
