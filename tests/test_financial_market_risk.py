"""Financial market data + risk channels (03, 04, 05, 18): before / during / after a fault.

Each channel's signal metric sits in its normal band before the fault, is out of band during
it while the status log says ANOMALY and carries the bad configuration, and converges back
after the opsctl fix (not instantly) with only the good configuration on the status log.
"""
from __future__ import annotations

from tests._financial_harness import attr_values, phase, records, values, within

MDF = "market-data-feed"
RISK = "risk-calculator"


def _signal(ch, which, svc, metric, **kw):
    logs, metrics = records(phase(ch, which, **kw))
    vals = values(metrics, svc, metric)
    assert vals, f"{svc} emitted no {metric}"
    return logs, metrics, vals


def _lines(logs, svc, contains):
    return [r for r in logs if r["service"] == svc and contains in r["body"]]


def _attr(logs, svc, contains, key):
    return {r["attrs"].get(key) for r in _lines(logs, svc, contains)}


def _ints(vals):
    """OTLP carries int attributes as strings (intValue)."""
    return {int(v) for v in vals}


# ── ch03: mdf-c2-pool rmem_max back to default -> UDP rcvbuf drops ─────────

C2 = "pool=mdf-c2-pool so_rcvbuf="


def test_ch03_udp_rcvbuf_errors_phases():
    m = "market_data.udp_rcvbuf_errors_per_s"
    logs, metrics, v = _signal(3, "before", MDF, m)
    assert v == [0.0] * len(v), v
    assert all(g <= 1 for g in values(metrics, MDF, "market_data.sequence_gaps_per_min"))
    assert _attr(logs, MDF, C2, "feed.health_status") == {"NOMINAL"}
    assert _ints(_attr(logs, MDF, C2, "host.sysctl.net_core_rmem_max")) == {16777216}

    logs, metrics, v = _signal(3, "during", MDF, m)
    assert all(1200 <= x <= 4000 for x in v), v
    gaps = values(metrics, MDF, "market_data.sequence_gaps_per_min")
    assert all(20 <= g <= 80 for g in gaps), gaps
    assert _attr(logs, MDF, C2, "feed.health_status") == {"ANOMALY"}
    assert _ints(_attr(logs, MDF, C2, "host.sysctl.net_core_rmem_max")) == {212992}
    assert _ints(_attr(logs, MDF, C2, "feed.socket_rcvbuf_bytes")) == {425984}
    # the other pools keep the 16 MiB buffer
    other = [r for r in _lines(logs, MDF, "feed_health") if "pool=mdf-c2-pool" not in r["body"]]
    assert other and _ints(r["attrs"]["host.sysctl.net_core_rmem_max"] for r in other) == {16777216}

    logs, metrics, v = _signal(3, "after", MDF, m, since=within(3))
    assert v == [0.0] * len(v), v
    assert all(g <= 1 for g in values(metrics, MDF, "market_data.sequence_gaps_per_min"))
    assert _attr(logs, MDF, C2, "feed.health_status") == {"NOMINAL"}
    assert _ints(_attr(logs, MDF, C2, "host.sysctl.net_core_rmem_max")) == {16777216}


def test_ch03_recovers_node_by_node():
    m = "market_data.udp_rcvbuf_errors_per_s"
    _, _, early = _signal(3, "after", MDF, m, since=10)
    assert min(early) >= 1200, early            # 3 nodes still on the old config
    _, _, one = _signal(3, "after", MDF, m, since=70)
    assert all(800 <= x <= 2666 for x in one), one   # 2 of 3 nodes still capped
    _, _, two = _signal(3, "after", MDF, m, since=130)
    assert all(400 <= x <= 1333 for x in two), two


# ── ch04: limits file drops a zero on EQ-PROP -> 10x utilization ───────────

EQP = "pre_trade_check desk=EQ-PROP"


def test_ch04_limit_utilization_phases():
    m = "risk_calculator.limit_utilization_pct"
    logs, _, v = _signal(4, "before", RISK, m)
    assert all(30.0 <= x <= 40.0 for x in v), v
    assert _attr(logs, RISK, EQP, "risk.status") == {"NOMINAL"}
    assert attr_values(logs, RISK, "risk.limits_file_revision") == {"r4411"}

    logs, _, v = _signal(4, "during", RISK, m)
    assert all(310.0 <= x <= 400.0 for x in v), v
    assert _attr(logs, RISK, EQP, "risk.status") == {"ANOMALY"}
    assert _ints(_attr(logs, RISK, EQP, "risk.limit_m")) == {40}
    assert "r4412" in attr_values(logs, RISK, "risk.limits_file_revision")
    # exposure is unchanged and the other desks still pass
    exp = _attr(logs, RISK, EQP, "risk.exposure_m")
    assert all(124.0 <= e <= 160.0 for e in exp), exp
    others = [r for r in _lines(logs, RISK, "pre_trade_check") if "desk=EQ-PROP" not in r["body"]]
    assert others and {r["attrs"]["risk.result"] for r in others} == {"PASS"}

    logs, _, v = _signal(4, "after", RISK, m, since=within(4))
    assert all(30.0 <= x <= 40.0 for x in v), v
    assert _attr(logs, RISK, EQP, "risk.status") == {"NOMINAL"}
    assert _ints(_attr(logs, RISK, EQP, "risk.limit_m")) == {400}
    assert attr_values(logs, RISK, "risk.limits_file_revision") == {"r4411"}


def test_ch04_converges_rather_than_snaps():
    m = "risk_calculator.limit_utilization_pct"
    _, _, early = _signal(4, "after", RISK, m, since=10)
    assert min(early) > 100.0, early
    _, _, mid = _signal(4, "after", RISK, m, since=30)
    assert all(100.0 < x < 300.0 for x in mid), mid


# ── ch05: collateral-reval cronjob left suspended -> stale collateral ──────

SWEEP = "margin_sweep"


def test_ch05_collateral_age_phases():
    m = "risk_calculator.collateral_valuation_age_s"
    logs, metrics, v = _signal(5, "before", RISK, m)
    assert all(30 <= x <= 120 for x in v), v
    assert all(x <= 3 for x in values(metrics, RISK, "risk_calculator.margin_calls_issued_per_min"))
    assert _attr(logs, RISK, SWEEP, "margin.status") == {"NOMINAL"}
    assert _attr(logs, RISK, SWEEP, "collateral_reval.suspended") == {False}

    logs, metrics, v = _signal(5, "during", RISK, m)
    assert all(320 <= x <= 1800 for x in v), v
    calls = values(metrics, RISK, "risk_calculator.margin_calls_issued_per_min")
    assert all(40 <= x <= 150 for x in calls), calls
    assert _attr(logs, RISK, SWEEP, "margin.status") == {"ANOMALY"}
    assert _attr(logs, RISK, SWEEP, "collateral_reval.suspended") == {True}

    logs, metrics, v = _signal(5, "after", RISK, m, since=within(5))
    assert all(30 <= x <= 120 for x in v), v
    assert all(x <= 3 for x in values(metrics, RISK, "risk_calculator.margin_calls_issued_per_min"))
    assert _attr(logs, RISK, SWEEP, "margin.status") == {"NOMINAL"}
    assert attr_values(logs, RISK, "collateral_reval.suspended") == {False}


def test_ch05_ages_until_the_run_completes():
    m = "risk_calculator.collateral_valuation_age_s"
    _, _, early = _signal(5, "after", RISK, m, since=10)
    assert min(early) > 300, early
    _, _, mid = _signal(5, "after", RISK, m, since=60)
    assert all(120 < x < 900 for x in mid), mid


# ── ch18: conflation.interval 30 read as seconds -> 30 s stale sawtooth ────

QC = "quote_cache"


def test_ch18_quote_age_phases():
    m = "market_data.quote_age_ms"
    logs, metrics, v = _signal(18, "before", MDF, m)
    assert all(5 <= x <= 80 for x in v), v
    assert _attr(logs, MDF, QC, "quote_cache.status") == {"NOMINAL"}
    assert _attr(logs, MDF, QC, "config.conflation_interval") == {"30ms"}
    assert all(x <= 2 for x in values(metrics, RISK, "risk_calculator.stale_price_calcs_per_min"))

    logs, metrics, v = _signal(18, "during", MDF, m)
    assert all(9000 <= x <= 31000 for x in v), v
    assert _attr(logs, MDF, QC, "quote_cache.status") == {"ANOMALY"}
    assert _attr(logs, MDF, QC, "config.conflation_interval") == {"30s"}
    assert _attr(logs, MDF, QC, "feed.status") == {"CONNECTED"}
    stale = values(metrics, RISK, "risk_calculator.stale_price_calcs_per_min")
    assert all(40 <= x <= 122 for x in stale), stale

    logs, metrics, v = _signal(18, "after", MDF, m, since=within(18))
    assert all(x < 100 for x in v), v
    assert _attr(logs, MDF, QC, "quote_cache.status") == {"NOMINAL"}
    assert attr_values(logs, MDF, "config.conflation_interval") == {"30ms"}
    assert all(x <= 2 for x in values(metrics, RISK, "risk_calculator.stale_price_calcs_per_min"))


def test_ch18_converges_rather_than_snaps():
    _, _, early = _signal(18, "after", MDF, "market_data.quote_age_ms", since=10)
    assert min(early) > 3000, early


# ── risk-calculator stays at baseline for channels it has no part in ──────

def test_var_and_margin_utilization_flat_for_limit_file_fault():
    _, metrics = records(phase(4, "during"))
    assert all(11.0 <= x <= 18.0 for x in values(metrics, RISK, "risk_calculator.var_95"))
    assert all(50.0 <= x <= 75.0 for x in values(metrics, RISK, "risk_calculator.margin_utilization"))
