"""Financial fraud and compliance channels (10, 11, 12): before / during / after a fault.

Each channel's signal metric sits in its baseline band before the fault, is out of band
during it while the status log says ANOMALY with the bad configuration, and converges back
after the opsctl fix (still partly out of band shortly after it) with the restored
configuration on the status log.
"""
from __future__ import annotations

from tests._financial_harness import attr_values, phase, records, values, within


def _signal(ch, which, svc, metric, **kw):
    logs, metrics = records(phase(ch, which, **kw))
    vals = values(metrics, svc, metric)
    assert vals, f"{svc} emitted no {metric}"
    return logs, metrics, vals


def _status(logs, svc, contains, key):
    return {r["attrs"].get(key) for r in logs if r["service"] == svc and contains in r["body"]}


# ── ch10: fraud-velocity v3.2.1 feature-scale mismatch -> false positive storm ──

FP = "fraud_detector.false_positive_rate"
BLOCKED = "fraud_detector.blocked_orders_per_min"


def test_ch10_false_positive_rate_phases():
    logs, metrics, v = _signal(10, "before", "fraud-detector", FP)
    assert all(0.5 <= x <= 3.0 for x in v), v
    assert all(0.0 <= x <= 3.0 for x in values(metrics, "fraud-detector", BLOCKED))
    assert _status(logs, "fraud-detector", "model_monitor", "model.status") == {"NOMINAL"}
    assert attr_values(logs, "fraud-detector", "fraud.model_version") == {"v3.2.0"}

    logs, metrics, v = _signal(10, "during", "fraud-detector", FP)
    assert all(90.0 <= x <= 100.0 for x in v), v
    assert all(50.0 <= x <= 500.0 for x in values(metrics, "fraud-detector", BLOCKED))
    assert _status(logs, "fraud-detector", "model_monitor", "model.status") == {"ANOMALY"}
    assert "v3.2.1" in attr_values(logs, "fraud-detector", "fraud.model_version")
    drift = [m for m in metrics if m["service"] == "fraud-detector"
             and m["name"] == "fraud_detector.feature_drift_score"]
    assert all(0.81 <= m["value"] <= 0.99 for m in drift if m["attrs"]["fraud.feature"] == "txn_velocity_5m")
    assert all(m["value"] < 0.15 for m in drift if m["attrs"]["fraud.feature"] != "txn_velocity_5m")

    logs, metrics, v = _signal(10, "after", "fraud-detector", FP, since=within(10))
    assert all(x < 3.0 for x in v), v
    assert all(x <= 3.0 for x in values(metrics, "fraud-detector", BLOCKED))
    assert _status(logs, "fraud-detector", "model_monitor", "model.status") == {"NOMINAL"}
    assert attr_values(logs, "fraud-detector", "fraud.model_version") == {"v3.2.0"}


def test_ch10_converges_rather_than_snaps():
    _, _, early = _signal(10, "after", "fraud-detector", FP, since=10)
    assert min(early) > 50.0, early


# ── ch11: compliance-enrichment reads LEIs from the stale DR replica ──────────

VAL = "compliance.report_validation_errors"


def test_ch11_report_validation_errors_phases():
    logs, metrics, v = _signal(11, "before", "compliance-monitor", VAL)
    assert all(0 <= x <= 5 for x in v), v
    assert all(x < 24 for x in values(metrics, "compliance-monitor", "compliance.refdata_age_h"))
    assert _status(logs, "compliance-monitor", "report_validation report=", "compliance.validation_status") == {"NOMINAL"}
    assert attr_values(logs, "compliance-monitor", "compliance.lei_source") == {"refdata-primary"}

    logs, metrics, v = _signal(11, "during", "compliance-monitor", VAL)
    assert all(1500 <= x <= 3505 for x in v), v
    assert all(72 <= x <= 240 for x in values(metrics, "compliance-monitor", "compliance.refdata_age_h"))
    assert _status(logs, "compliance-monitor", "report_validation report=", "compliance.validation_status") == {"ANOMALY"}
    assert _status(logs, "compliance-monitor", "report_validation report=", "compliance.lei_source") == {"refdata-dr-westus"}
    assert not any("invalid_UTI" in r["body"] for r in logs)

    logs, metrics, v = _signal(11, "after", "compliance-monitor", VAL, since=within(11))
    assert all(x < 5 for x in v), v
    assert all(x < 24 for x in values(metrics, "compliance-monitor", "compliance.refdata_age_h"))
    assert _status(logs, "compliance-monitor", "report_validation report=", "compliance.validation_status") == {"NOMINAL"}
    assert attr_values(logs, "compliance-monitor", "compliance.lei_source") == {"refdata-primary"}


def test_ch11_converges_rather_than_snaps():
    _, _, early = _signal(11, "after", "compliance-monitor", VAL, since=10)
    assert min(early) > 500, early


# ── ch12: aml-screening-routing sends EU screenings to us-east ────────────────

P95 = "compliance.aml_screening_p95_ms"


def _p95(metrics, jurisdiction):
    return [m["value"] for m in metrics if m["service"] == "compliance-monitor"
            and m["name"] == P95 and m["attrs"].get("aml.jurisdiction") == jurisdiction]


def _eu_endpoints(logs):
    return {r["attrs"]["aml.provider_endpoint"] for r in logs if r["service"] == "compliance-monitor"
            and r["attrs"].get("aml.jurisdiction") == "EU" and "aml.provider_endpoint" in r["attrs"]}


def _health(logs):
    return _status(logs, "compliance-monitor", "aml_screening_health", "aml.screening_status")


def test_ch12_eu_screening_p95_phases():
    logs, metrics = records(phase(12, "before"))
    assert all(800 <= x <= 1600 for x in _p95(metrics, "EU")), _p95(metrics, "EU")
    assert _health(logs) == {"NOMINAL"}
    assert _eu_endpoints(logs) == {"eu-west"}

    logs, metrics = records(phase(12, "during"))
    eu = _p95(metrics, "EU")
    assert eu and all(12700 <= x <= 30000 for x in eu), eu
    for j in ("US", "UK", "SG", "JP"):
        assert all(x <= 1600 for x in _p95(metrics, j)), (j, _p95(metrics, j))
    assert _health(logs) == {"ANOMALY"}
    assert "us-east" in _eu_endpoints(logs)
    assert _status(logs, "compliance-monitor", "aml_screening_health", "aml.routing_revision") == {"19"}
    others = [r for r in logs if r["service"] == "compliance-monitor" and "aml_screening txn=" in r["body"]
              and r["attrs"].get("aml.jurisdiction") != "EU"]
    assert others and all(r["attrs"]["screening.sla_met"] is True for r in others)

    logs, metrics = records(phase(12, "after", since=within(12)))
    eu = _p95(metrics, "EU")
    assert eu and all(x < 2000 for x in eu), eu
    assert _health(logs) == {"NOMINAL"}
    assert _eu_endpoints(logs) == {"eu-west"}
    assert attr_values(logs, "compliance-monitor", "aml.routing_revision") == {"18"}


def test_ch12_converges_rather_than_snaps():
    _, metrics = records(phase(12, "after", since=10))
    eu = _p95(metrics, "EU")
    assert eu and min(eu) > 5000, eu
    _, metrics = records(phase(12, "after", since=45))
    eu = _p95(metrics, "EU")
    assert eu and all(2000 < x < 20000 for x in eu), eu


def test_ch12_held_queue_drains_after_fix():
    _, metrics = records(phase(12, "during"))
    assert min(values(metrics, "compliance-monitor", "compliance.aml_held_transactions")) >= 300
    assert min(values(metrics, "fraud-detector", "fraud_detector.decisions_pending_aml")) >= 150
    _, metrics = records(phase(12, "after", since=within(12)))
    assert max(values(metrics, "compliance-monitor", "compliance.aml_held_transactions")) <= 3
    assert max(values(metrics, "fraud-detector", "fraud_detector.decisions_pending_aml")) <= 4


# ── compliance-monitor stays at baseline for unrelated incidents ──────────────

def test_compliance_signals_quiet_for_other_channels():
    logs, metrics = records(phase(4, "during"))
    assert all(x <= 5 for x in values(metrics, "compliance-monitor", VAL))
    assert all(x <= 1600 for x in _p95(metrics, "EU"))
    assert all(x <= 3.0 for x in values(metrics, "fraud-detector", FP))
