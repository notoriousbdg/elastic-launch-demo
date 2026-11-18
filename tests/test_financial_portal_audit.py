"""Financial portal + audit channels (02, 13, 14, 15, 17): before / during / after a fault.

Each channel's signal metric sits in its normal band before the fault, is out of band during
it while the status line says ANOMALY with the bad configuration, and converges back after the
opsctl fix (not instantly), with the good configuration on the status line.
"""
from __future__ import annotations

from statistics import mean

import pytest
import yaml

from tests._financial_harness import (
    AUDIT_MARK, OPS_ACTOR, REG, ROOT, attr_values, bodies, cmd_lines, phase, records, values, within,
)

PORTAL = "customer-portal"
AUDIT = "audit-logger"


def _signal(ch, which, svc, metric, **kw):
    logs, metrics = records(phase(ch, which, **kw))
    vals = values(metrics, svc, metric)
    assert vals, f"{svc} emitted no {metric}"
    return logs, metrics, vals


def _line_attr(logs, svc, contains, key):
    """Values of *key* on that service's lines containing *contains* (OTLP ints arrive as strings)."""
    return {str(r["attrs"].get(key)) for r in logs if r["service"] == svc and contains in r["body"]}


def _yaml(rel):
    return yaml.safe_load((ROOT / "scenarios" / "financial" / rel).read_text())


def _channel_yaml(stem):
    return _yaml(f"channels/{stem}.yaml")


def _assert_cmd_lines(logs, ch):
    audit = bodies(logs, AUDIT, AUDIT_MARK)
    for line in cmd_lines(ch):
        hits = [b for b in audit if f'cmd="{line}"' in b]
        assert len(hits) == 1, (line, audit)
        assert f"actor={OPS_ACTOR}" in hits[0]


# ── ch02: WORM sink overflow_policy=drop -> sequence gaps ──────────────────

GAPS = "audit_logger.sequence_gaps_per_min"
OPEN = "audit_logger.open_sequence_gaps"
CLAG = "audit_logger.kafka_consumer_lag"


def test_ch02_sequence_gaps_phases():
    logs, metrics, v = _signal(2, "before", AUDIT, GAPS)
    assert set(v) == {0.0}, v
    assert set(values(metrics, AUDIT, OPEN)) == {0.0}
    assert _line_attr(logs, AUDIT, "integrity_check", "integrity.status") == {"NOMINAL"}
    assert _line_attr(logs, AUDIT, "integrity_check", "integrity.hash_chain") == {"VALID"}
    assert attr_values(logs, AUDIT, "audit.sink.overflow_policy") == {"block"}

    logs, metrics, v = _signal(2, "during", AUDIT, GAPS)
    assert all(30.0 <= x <= 80.0 for x in v), v
    assert all(x > 0 for x in values(metrics, AUDIT, "audit_logger.dropped_records_per_min"))
    assert all(x > 100 for x in values(metrics, AUDIT, OPEN))
    # the differential: the consumer keeps up because it drops
    assert all(x < 1000 for x in values(metrics, AUDIT, CLAG))
    assert _line_attr(logs, AUDIT, "integrity_check", "integrity.status") == {"ANOMALY"}
    assert _line_attr(logs, AUDIT, "integrity_check", "integrity.hash_chain") == {"BROKEN"}
    assert "drop" in attr_values(logs, AUDIT, "audit.sink.overflow_policy")
    assert _line_attr(logs, AUDIT, "integrity_check", "config.revision") == {"47"}

    logs, metrics, v = _signal(2, "after", AUDIT, GAPS, since=within(2))
    assert set(v) == {0.0}, v
    assert set(values(metrics, AUDIT, OPEN)) == {0.0}
    assert all(x < 1000 for x in values(metrics, AUDIT, CLAG))
    assert _line_attr(logs, AUDIT, "integrity_check", "integrity.status") == {"NOMINAL"}
    assert _line_attr(logs, AUDIT, "integrity_check", "integrity.hash_chain") == {"VALID"}
    assert _line_attr(logs, AUDIT, "integrity_check", "audit.sink.overflow_policy") == {"block"}
    assert _line_attr(logs, AUDIT, "integrity_check", "config.revision") == {"46"}
    _assert_cmd_lines(logs, 2)


def test_ch02_converges_and_replay_backpressures_consumer():
    _, metrics, early = _signal(2, "after", AUDIT, GAPS, since=10)
    assert min(early) > 0, early
    assert min(values(metrics, AUDIT, OPEN)) > 100
    _, metrics = records(phase(2, "after", since=45))
    assert set(values(metrics, AUDIT, GAPS)) == {0.0}
    assert min(values(metrics, AUDIT, CLAG)) > 5000


def test_ch02_has_no_kafka_impact():
    assert "kafka_otel" not in (REG[2].get("generator_impact") or {})


# ── ch13: Redis allkeys-lru evicts sessions ────────────────────────────────

EVICT = "customer_portal.session_evictions_per_min"
ACTIVE = "customer_portal.active_sessions"


def test_ch13_session_evictions_phases():
    logs, metrics, v = _signal(13, "before", PORTAL, EVICT)
    assert set(v) == {0.0}, v
    assert all(300 <= x <= 400 for x in values(metrics, PORTAL, ACTIVE))
    assert _line_attr(logs, PORTAL, "session_summary", "session.status") == {"NOMINAL"}
    assert attr_values(logs, PORTAL, "redis.maxmemory_policy") == {"volatile-lru"}

    logs, metrics, v = _signal(13, "during", PORTAL, EVICT)
    assert all(60.0 <= x <= 300.0 for x in v), v
    assert all(x < 280 for x in values(metrics, PORTAL, ACTIVE))
    assert _line_attr(logs, PORTAL, "session_summary", "session.status") == {"ANOMALY"}
    assert "allkeys-lru" in attr_values(logs, PORTAL, "redis.maxmemory_policy")
    # idle-timeout expiries stay at their normal rate
    assert all(0 <= int(x) <= 5 for x in _line_attr(logs, PORTAL, "session_summary", "session.expired"))

    logs, metrics, v = _signal(13, "after", PORTAL, EVICT, since=within(13))
    assert set(v) == {0.0}, v
    assert all(300 <= x <= 400 for x in values(metrics, PORTAL, ACTIVE))
    assert _line_attr(logs, PORTAL, "session_summary", "session.status") == {"NOMINAL"}
    assert attr_values(logs, PORTAL, "redis.maxmemory_policy") == {"volatile-lru"}
    _assert_cmd_lines(logs, 13)


def test_ch13_converges_rather_than_snaps():
    _, metrics, early = _signal(13, "after", PORTAL, EVICT, since=10)
    assert min(early) > 0, early
    assert max(values(metrics, PORTAL, ACTIVE)) < 300


def test_ch13_session_age_is_under_the_idle_limit():
    lo, hi = _channel_yaml("13-customer-session-timeout")["fault_params"]["session_age_s"]["randint"]
    assert 0 < lo <= hi < 900
    assert REG[13]["generator_impact"]["redis_otel"]["high_eviction_rate"] > 100.0


# ── ch14: FX cache TTL 0 -> valuation lag ──────────────────────────────────

VLAG = "customer_portal.valuation_lag_s"
FX = "customer_portal.fx_conversion_ms"
R429 = "customer_portal.fx_provider_429_per_min"


def test_ch14_valuation_lag_phases():
    logs, metrics, v = _signal(14, "before", PORTAL, VLAG)
    assert all(1.0 <= x <= 5.0 for x in v), v
    assert all(5 <= x <= 20 for x in values(metrics, PORTAL, FX))
    assert set(values(metrics, PORTAL, R429)) == {0.0}
    assert _line_attr(logs, PORTAL, "valuation_status", "valuation.status") == {"NOMINAL"}
    assert _line_attr(logs, PORTAL, "valuation_status", "valuation.fx_cache_ttl_s") == {"60"}

    logs, metrics, v = _signal(14, "during", PORTAL, VLAG)
    assert all(30.0 <= x <= 300.0 for x in v), v
    assert all(3000 <= x <= 10000 for x in values(metrics, PORTAL, FX))
    assert all(x >= 250 for x in values(metrics, PORTAL, R429))
    assert _line_attr(logs, PORTAL, "valuation_status", "valuation.status") == {"ANOMALY"}
    assert _line_attr(logs, PORTAL, "valuation_status", "valuation.fx_cache_ttl_s") == {"0"}

    logs, metrics, v = _signal(14, "after", PORTAL, VLAG, since=within(14))
    assert all(x < 15.0 for x in v), v
    assert all(x < 50 for x in values(metrics, PORTAL, FX))
    assert set(values(metrics, PORTAL, R429)) == {0.0}
    assert _line_attr(logs, PORTAL, "valuation_status", "valuation.status") == {"NOMINAL"}
    assert attr_values(logs, PORTAL, "valuation.fx_cache_ttl_s") == {"60"}
    _assert_cmd_lines(logs, 14)


def test_ch14_converges_rather_than_snaps():
    _, _, early = _signal(14, "after", PORTAL, VLAG, since=10)
    assert min(early) > 15.0, early


# ── ch15: dispatchers scaled 6 -> 2 -> confirmation lag ────────────────────

CLAG15 = "customer_portal.confirmation_queue_lag"


def test_ch15_confirmation_lag_phases():
    logs, metrics, v = _signal(15, "before", PORTAL, CLAG15)
    assert all(x <= 200 for x in v), v
    assert _line_attr(logs, PORTAL, "confirmation_status", "confirmation.status") == {"NOMINAL"}
    assert _line_attr(logs, PORTAL, "confirmation_status", "deployment.replicas") == {"6"}

    logs, metrics, v = _signal(15, "during", PORTAL, CLAG15)
    # 600 s at +70 msg/s
    assert all(35000 <= x <= 50000 for x in v), v
    assert all(140 <= x <= 160 for x in values(metrics, PORTAL, "customer_portal.confirmations_consumed_per_s"))
    assert all(x > 120 for x in values(metrics, PORTAL, "customer_portal.confirmation_delay_s"))
    assert _line_attr(logs, PORTAL, "confirmation_status", "confirmation.status") == {"ANOMALY"}
    assert _line_attr(logs, PORTAL, "confirmation_status", "deployment.replicas") == {"2"}
    assert _line_attr(logs, PORTAL, "confirmation_status", "kafka.consumer.count") == {"2"}

    logs, metrics, v = _signal(15, "after", PORTAL, CLAG15, since=within(15))
    assert all(x < 500 for x in v), v
    assert _line_attr(logs, PORTAL, "confirmation_status", "confirmation.status") == {"NOMINAL"}
    assert _line_attr(logs, PORTAL, "confirmation_status", "deployment.replicas") == {"6"}
    assert _line_attr(logs, PORTAL, "confirmation_status", "kafka.consumer.count") == {"6"}
    _assert_cmd_lines(logs, 15)


def test_ch15_drains_rather_than_snaps():
    _, _, early = _signal(15, "after", PORTAL, CLAG15, since=10)
    assert min(early) > 40000, early
    _, _, mid = _signal(15, "after", PORTAL, CLAG15, since=120)
    assert 10000 < min(mid) and max(mid) < 40000, mid


def test_ch15_kafka_impact_breaches():
    assert REG[15]["generator_impact"]["kafka_otel"]["high_consumer_lag_by_topic"] > 50000.0


# ── ch17: replication compression none -> WAN saturation ───────────────────

RLAG = "audit_logger.replication_lag_ms"
UTIL = "audit_logger.wan_link_utilization_pct"
RATIO = "audit_logger.replication_compression_ratio"
WESTUS = "replication_status source=eastus dest=westus"
NEU = "replication_status source=eastus dest=northeurope"


def test_ch17_replication_lag_phases():
    logs, metrics, v = _signal(17, "before", AUDIT, RLAG)
    assert all(50 <= x <= 800 for x in v), v
    assert all(25 <= x <= 40 for x in values(metrics, AUDIT, UTIL))
    assert all(x > 3.5 for x in values(metrics, AUDIT, RATIO))
    assert _line_attr(logs, AUDIT, WESTUS, "replication.status") == {"NOMINAL"}
    assert attr_values(logs, AUDIT, "replication.compression") == {"zstd"}

    logs, metrics, v = _signal(17, "during", AUDIT, RLAG)
    assert all(5000 <= x <= 60000 for x in v), v
    assert all(91 <= x <= 99 for x in values(metrics, AUDIT, UTIL))
    assert all(x < 1.1 for x in values(metrics, AUDIT, RATIO))
    assert _line_attr(logs, AUDIT, WESTUS, "replication.status") == {"ANOMALY"}
    assert "none" in attr_values(logs, AUDIT, "replication.compression")
    # the northeurope copy on its own link stays in sync
    assert _line_attr(logs, AUDIT, NEU, "replication.state") == {"SYNCED"}

    logs, metrics, v = _signal(17, "after", AUDIT, RLAG, since=within(17))
    assert all(x < 3000 for x in v), v
    assert all(25 <= x <= 40 for x in values(metrics, AUDIT, UTIL))
    assert all(x > 3.5 for x in values(metrics, AUDIT, RATIO))
    assert _line_attr(logs, AUDIT, WESTUS, "replication.status") == {"NOMINAL"}
    assert attr_values(logs, AUDIT, "replication.compression") == {"zstd"}
    _assert_cmd_lines(logs, 17)


def test_ch17_converges_rather_than_snaps():
    _, metrics, early = _signal(17, "after", AUDIT, RLAG, since=10)
    assert min(early) > 3000, early
    assert all(x < 90 for x in values(metrics, AUDIT, UTIL))


def test_replication_destinations_are_azure_regions():
    spec = _yaml("services/audit-logger.yaml")
    assert spec["constants"]["REPLICATION_DESTS"] == ["westus", "northeurope"]


# ── cascades: portal and audit metrics react to other channels ─────────────

@pytest.mark.parametrize("ch", [3, 5, 10, 12])
def test_portal_api_errors_rise_on_upstream_faults(ch):
    _, before = records(phase(ch, "before"))
    _, during = records(phase(ch, "during"))
    _, after = records(phase(ch, "after", since=300))
    b = max(values(before, PORTAL, "customer_portal.api_error_rate"))
    assert min(values(during, PORTAL, "customer_portal.api_error_rate")) > b
    assert max(values(after, PORTAL, "customer_portal.api_error_rate")) <= 0.5


@pytest.mark.parametrize("ch", [1, 7, 11, 13, 19, 20])
def test_audit_ingest_rises_with_recorded_events(ch):
    m = "audit_logger.ingest_events_per_s"
    _, before = records(phase(ch, "before"), cycles=8)
    _, during = records(phase(ch, "during"), cycles=8)
    assert mean(values(during, AUDIT, m)) > mean(values(before, AUDIT, m)) + 50
