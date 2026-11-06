import random
"""Technology-specific tests for the ``oracle`` generator.

Standard emit/registry tests live in tests/test_generators_emit.py
(parametrized over GENERATORS) and run for this generator automatically.
Add tests here only for behaviour that is special to oracle.
"""
import sys
import os

# Allow importing from the shared test harness.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "tests"))

from generator_engine.db_common import build_signal_resource
from generators.oracle_otel import generator as oracle
from generators.oracle_otel.generator import (
    _RATIO_COUNTER_DYNAMIC_TEMPLATES,
    _TABLESPACE_GAUGE_DYNAMIC_TEMPLATES,
    build_buffer_cache_ratio_doc,
    build_hard_parse_ratio_doc,
    build_tablespace_utilization_doc,
)


def _resource():
    return build_signal_resource(
        ns="test",
        signal_type="metrics",
        dataset="oracledbreceiver",
        service_name="oracle-db",
        service_version="19.3.0",
        service_instance_id="oracle-db-001",
        host_name="test-oracle-host",
        cloud={
            "provider": "aws",
            "platform": "aws_rds",
            "region": "us-east-1",
            "availability_zone": "us-east-1a",
        },
        extra_attrs={"oracledb.instance.name": "TESTDB"},
    )


def test_tablespace_utilization_doc_colocates_usage_and_limit():
    """Tablespace panels AND-filter both metrics; they must share one document."""
    doc = build_tablespace_utilization_doc(
        _resource(),
        "APP_DATA",
        usage_bytes=40_000_000_000,
        limit_bytes=100_000_000_000,
        timestamp_ms=1_700_000_000_000,
    )
    assert doc["metrics"]["oracledb.tablespace_size.usage"] == 40_000_000_000
    assert doc["metrics"]["oracledb.tablespace_size.limit"] == 100_000_000_000
    assert doc["attributes"]["tablespace_name"] == "APP_DATA"
    assert doc["data_stream"]["dataset"] == "oracledbreceiver.otel"
    assert "data_stream.type" not in doc["resource"]["attributes"]
    assert doc["resource"]["attributes"]["oracledb.instance.name"] == "TESTDB"


def test_buffer_cache_ratio_doc_colocates_physical_and_logical():
    doc = build_buffer_cache_ratio_doc(_resource(), physical_reads=1_000, logical_reads=100_000)
    assert doc["metrics"]["oracledb.physical_reads"] == 1_000
    assert doc["metrics"]["oracledb.logical_reads"] == 100_000


def test_hard_parse_ratio_doc_colocates_hard_and_parse_calls():
    doc = build_hard_parse_ratio_doc(_resource(), hard_parses=50, parse_calls=5_000)
    assert doc["metrics"]["oracledb.hard_parses"] == 50
    assert doc["metrics"]["oracledb.parse_calls"] == 5_000


def test_ratio_counter_dynamic_templates_use_counter_long():
    """Bulk co-located ratio docs must request counter_long (RATE() requirement)."""
    assert _RATIO_COUNTER_DYNAMIC_TEMPLATES == {
        "metrics.oracledb.physical_reads": "counter_long",
        "metrics.oracledb.logical_reads": "counter_long",
        "metrics.oracledb.hard_parses": "counter_long",
        "metrics.oracledb.parse_calls": "counter_long",
    }
    assert _TABLESPACE_GAUGE_DYNAMIC_TEMPLATES == {
        "metrics.oracledb.tablespace_size.usage": "gauge_long",
        "metrics.oracledb.tablespace_size.limit": "gauge_long",
    }


def test_ratio_counter_detection_reads_field_caps():
    """_ratio_counters_missing_counter_mapping is True when mapped as plain long."""
    from unittest.mock import patch

    class _Resp:
        status_code = 200

        def json(self):
            return {
                "fields": {
                    "oracledb.physical_reads": {
                        "long": {},  # plain long — no time_series_metric
                    },
                    "oracledb.logical_reads": {
                        "long": {"time_series_metric": "counter"},
                    },
                }
            }

    with patch("httpx.get", return_value=_Resp()):
        assert oracle._ratio_counters_missing_counter_mapping(
            "https://es.example", "key"
        ) is True


def test_ratio_counter_detection_false_when_counters():
    from unittest.mock import patch

    class _Resp:
        status_code = 200

        def json(self):
            return {
                "fields": {
                    "oracledb.physical_reads": {
                        "long": {"time_series_metric": "counter"},
                    },
                    "oracledb.logical_reads": {
                        "long": {"time_series_metric": "counter"},
                    },
                    "oracledb.hard_parses": {
                        "long": {"time_series_metric": "counter"},
                    },
                    "oracledb.parse_calls": {
                        "long": {"time_series_metric": "counter"},
                    },
                }
            }

    with patch("httpx.get", return_value=_Resp()):
        assert oracle._ratio_counters_missing_counter_mapping(
            "https://es.example", "key"
        ) is False


from generators.oracle_otel import SPEC
from generators.oracle_otel.generator import OracleMetricState, _generate_metrics


def _gauge(m):
    dps = m.get("gauge", {}).get("dataPoints") or []
    return float(dps[0].get("asDouble", dps[0].get("asInt", 0)))


def _sum(m):
    dps = m.get("sum", {}).get("dataPoints") or []
    return float(dps[0].get("asDouble", dps[0].get("asInt", 0)))


def test_healthy_baseline_no_chaos():
    rng = random.Random(0)
    state = OracleMetricState(rng)
    before = state.enqueue_deadlocks
    metrics, _ = _generate_metrics(state, ["USERS", "SYSTEM"], rng, "ORCL", None)
    assert state.enqueue_deadlocks - before == 0
    proc = next(m for m in metrics if m["name"] == "oracledb.processes.usage")
    plim = next(m for m in metrics if m["name"] == "oracledb.processes.limit")
    assert _gauge(proc) / _gauge(plim) < 0.85


def test_hit_ratio_and_hard_parse_ratio_healthy_from_start():
    """Independently-random init could put ratios past alert thresholds on
    the very first tick; both must be safely healthy immediately."""
    for seed in range(20):
        rng = random.Random(seed)
        state = OracleMetricState(rng)
        hit_ratio = 1.0 - (state.physical_reads / state.logical_reads)
        assert hit_ratio >= 0.9, f"seed={seed} hit_ratio={hit_ratio}"
        hard_parse_ratio = state.hard_parses / state.parse_calls
        assert hard_parse_ratio <= 0.1, f"seed={seed} hard_parse_ratio={hard_parse_ratio}"


def test_hit_ratio_stays_healthy_across_ticks_without_chaos():
    """Healthy default used to be 0.95; ``1.0 - 0.95`` float noise took the
    breach branch (forced physical_frac≥0.15 → constant 85% hit)."""
    for seed in range(20):
        rng = random.Random(seed)
        state = OracleMetricState(rng)
        for _ in range(30):
            before_p, before_l = state.physical_reads, state.logical_reads
            state.tick(rng, None)
            dp = state.physical_reads - before_p
            dl = state.logical_reads - before_l
            hit = 1.0 - (dp / dl)
            assert hit >= 0.9, f"seed={seed} tick_hit={hit}"


def test_deadlock_rate_applied():
    rng = random.Random(1)
    state = OracleMetricState(rng)
    before = state.enqueue_deadlocks
    _generate_metrics(state, ["USERS"], rng, "ORCL", {"enqueue_deadlocks_detected": 5})
    assert state.enqueue_deadlocks - before >= 5


def test_session_utilization_alone():
    rng = random.Random(2)
    state = OracleMetricState(rng)
    metrics, _ = _generate_metrics(state, ["USERS"], rng, "ORCL", {"session_utilization": 0.9})
    slim = next(m for m in metrics if m["name"] == "oracledb.sessions.limit")
    # usage split across types; total of gauges can exceed — check limit present
    assert _gauge(slim) == 1200

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.oracle_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['buffer_cache_hit_ratio_low', 'dml_lock_utilisation_high', 'enqueue_deadlocks_detected', 'enqueue_lock_utilisation_high', 'hard_parse_ratio_high', 'process_utilisation_high', 'rollback_to_commit_ratio_high', 'session_utilisation_high', 'tablespace_utilisation_high', 'transaction_limit_utilisation_high']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


def test_usage_limit_doc_colocates_fields():
    from generators.oracle_otel.generator import build_usage_limit_doc
    doc = build_usage_limit_doc(
        _resource(),
        "oracledb.processes.usage",
        "oracledb.processes.limit",
        usage=100,
        limit=800,
        unit="{processes}",
    )
    assert doc["metrics"]["oracledb.processes.usage"] == 100
    assert doc["metrics"]["oracledb.processes.limit"] == 800
