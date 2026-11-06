"""MySQL OTel generator chaos / alert coverage tests."""

from __future__ import annotations

import random

import pytest

from generator_engine.db_common import CounterState
from generators.mysql_otel import SPEC
from generators.mysql_otel.generator import (
    BUFFER_POOL_OPERATIONS,
    BUFFER_POOL_PAGE_KINDS,
    HANDLER_KINDS,
    MySQLMetricState,
    PAGE_OPERATIONS,
    TABLE_IO_OPERATIONS,
    _generate_mysql_metrics,
)


def _sum_val(m):
    dps = m.get("sum", {}).get("dataPoints") or []
    return float(dps[0].get("asDouble", dps[0].get("asInt", 0)))


def _gauge(m):
    dps = m.get("gauge", {}).get("dataPoints") or []
    return float(dps[0].get("asDouble", dps[0].get("asInt", 0)))


def _attr_ops(metrics, name, attr_key="operation"):
    ops = set()
    for m in metrics:
        if m["name"] != name:
            continue
        kind = "sum" if "sum" in m else "gauge"
        dp = m[kind]["dataPoints"][0]
        for a in dp.get("attributes", []):
            if a.get("key") == attr_key:
                ops.add(next(iter(a["value"].values())))
    return ops


def test_resolve_active_chaos_importable():
    from generators.mysql_otel.generator import resolve_active_chaos
    assert callable(resolve_active_chaos)


def test_metric_state_is_counter_state():
    rng = random.Random(0)
    state = MySQLMetricState(rng)
    assert isinstance(state, CounterState)
    assert state.start_time_ns


def test_counter_start_time_stable_across_scrapes():
    rng = random.Random(0)
    state = MySQLMetricState(rng)
    tables = {"db": ["t1", "t2"]}
    m1 = _generate_mysql_metrics(state, tables, rng, None)
    m2 = _generate_mysql_metrics(state, tables, rng, None)
    q1 = next(m for m in m1 if m["name"] == "mysql.query.count")
    q2 = next(m for m in m2 if m["name"] == "mysql.query.count")
    start1 = q1["sum"]["dataPoints"][0]["startTimeUnixNano"]
    start2 = q2["sum"]["dataPoints"][0]["startTimeUnixNano"]
    assert start1 == start2 == state.start_time_ns
    assert int(q2["sum"]["dataPoints"][0]["asInt"]) > int(q1["sum"]["dataPoints"][0]["asInt"])


def test_healthy_baseline_no_chaos():
    rng = random.Random(0)
    state = MySQLMetricState(rng)
    tables = {"db": ["t1", "t2"]}
    metrics = _generate_mysql_metrics(state, tables, rng, None)
    lag = next(m for m in metrics if m["name"] == "mysql.replica.time_behind_source")
    sql_delay = next(m for m in metrics if m["name"] == "mysql.replica.sql_delay")
    assert "sum" in lag and lag["sum"]["isMonotonic"] is False
    assert "sum" in sql_delay and sql_delay["sum"]["isMonotonic"] is False
    assert lag["unit"] == "s"
    assert 0 <= _sum_val(lag) <= 2
    assert 0 <= _sum_val(sql_delay) <= 2
    dirty = [
        m for m in metrics
        if m["name"] == "mysql.buffer_pool.usage"
        and any(a.get("key") == "status" and a.get("value", {}).get("stringValue") == "dirty"
                for a in m.get("gauge", {}).get("dataPoints", [{}])[0].get("attributes", []))
    ]
    clean = [
        m for m in metrics
        if m["name"] == "mysql.buffer_pool.usage"
        and any(a.get("key") == "status" and a.get("value", {}).get("stringValue") == "clean"
                for a in m.get("gauge", {}).get("dataPoints", [{}])[0].get("attributes", []))
    ]
    assert dirty and clean
    if _gauge(dirty[0]) + _gauge(clean[0]) > 0:
        assert _gauge(dirty[0]) / (_gauge(dirty[0]) + _gauge(clean[0])) < 0.75


def test_healthy_replication_lag_meets_slo_every_tick():
    """Package SLO "[MySQL OTel] Replication lag": last_value < 5 s per 1m slice."""
    rng = random.Random(7)
    state = MySQLMetricState(rng)
    for _ in range(200):
        metrics = _generate_mysql_metrics(state, {"db": ["t"]}, rng, None)
        lag = next(m for m in metrics if m["name"] == "mysql.replica.time_behind_source")
        assert 0 <= _sum_val(lag) < 5


def test_replication_lag_and_dirty_ratio_keys():
    rng = random.Random(1)
    state = MySQLMetricState(rng)
    metrics = _generate_mysql_metrics(
        state, {"db": ["t"]}, rng,
        {"replication_lag": 120, "high_buffer_pool_dirty_page_ratio": 0.8},
    )
    lag = next(m for m in metrics if m["name"] == "mysql.replica.time_behind_source")
    sql_delay = next(m for m in metrics if m["name"] == "mysql.replica.sql_delay")
    assert "sum" in lag and lag["sum"]["isMonotonic"] is False
    assert _sum_val(lag) == 120.0
    assert _sum_val(sql_delay) == 120.0
    dirty = [
        m for m in metrics
        if m["name"] == "mysql.buffer_pool.usage"
        and any(a.get("key") == "status" and a.get("value", {}).get("stringValue") == "dirty"
                for a in m["gauge"]["dataPoints"][0].get("attributes", []))
    ]
    clean = [
        m for m in metrics
        if m["name"] == "mysql.buffer_pool.usage"
        and any(a.get("key") == "status" and a.get("value", {}).get("stringValue") == "clean"
                for a in m["gauge"]["dataPoints"][0].get("attributes", []))
    ]
    assert dirty and clean
    assert "asDouble" in dirty[0]["gauge"]["dataPoints"][0]
    assert "asDouble" in clean[0]["gauge"]["dataPoints"][0]
    ratio = _gauge(dirty[0]) / (_gauge(dirty[0]) + _gauge(clean[0]))
    assert ratio > 0.75


def test_dirty_ratio_accepts_percent_probe():
    rng = random.Random(11)
    state = MySQLMetricState(rng)
    metrics = _generate_mysql_metrics(
        state, {"db": ["t"]}, rng,
        {"high_buffer_pool_dirty_page_ratio": 75.0},
    )
    dirty = next(
        m for m in metrics
        if m["name"] == "mysql.buffer_pool.usage"
        and any(a.get("key") == "status" and a.get("value", {}).get("stringValue") == "dirty"
                for a in m["gauge"]["dataPoints"][0].get("attributes", []))
    )
    clean = next(
        m for m in metrics
        if m["name"] == "mysql.buffer_pool.usage"
        and any(a.get("key") == "status" and a.get("value", {}).get("stringValue") == "clean"
                for a in m["gauge"]["dataPoints"][0].get("attributes", []))
    )
    ratio = _gauge(dirty) / (_gauge(dirty) + _gauge(clean))
    assert ratio > 0.75


def test_page_operations_enum():
    rng = random.Random(2)
    state = MySQLMetricState(rng)
    metrics = _generate_mysql_metrics(state, {"db": ["t"]}, rng, None)
    ops = _attr_ops(metrics, "mysql.page_operations")
    assert ops == set(PAGE_OPERATIONS)
    assert "flushed" not in ops
    assert "created" in ops


def test_buffer_pool_operations_enum_and_hit_ratio_ops():
    rng = random.Random(3)
    state = MySQLMetricState(rng)
    metrics = _generate_mysql_metrics(state, {"db": ["t"]}, rng, None)
    ops = _attr_ops(metrics, "mysql.buffer_pool.operations")
    assert ops == set(BUFFER_POOL_OPERATIONS)
    assert {"read_requests", "reads"}.issubset(ops)
    assert "writes" not in ops
    assert "flush_pages" not in ops


def test_handlers_include_external_lock_and_mrr_init():
    rng = random.Random(4)
    state = MySQLMetricState(rng)
    metrics = _generate_mysql_metrics(state, {"db": ["t"]}, rng, None)
    kinds = _attr_ops(metrics, "mysql.handlers", attr_key="kind")
    assert kinds == set(HANDLER_KINDS)
    assert "external_lock" in kinds
    assert "mrr_init" in kinds


def test_buffer_pool_pages_kinds_no_dirty():
    rng = random.Random(5)
    state = MySQLMetricState(rng)
    metrics = _generate_mysql_metrics(state, {"db": ["t"]}, rng, None)
    kinds = _attr_ops(metrics, "mysql.buffer_pool.pages", attr_key="kind")
    assert kinds == set(BUFFER_POOL_PAGE_KINDS)
    assert "dirty" not in kinds


def test_row_locks_both_kinds_are_cumulative_sums():
    rng = random.Random(6)
    state = MySQLMetricState(rng)
    metrics = _generate_mysql_metrics(state, {"db": ["t"]}, rng, None)
    row_locks = [m for m in metrics if m["name"] == "mysql.row_locks"]
    assert len(row_locks) == 2
    kinds = set()
    for m in row_locks:
        assert "sum" in m
        assert m["sum"]["isMonotonic"] is True
        assert m["unit"] == "1"
        dp = m["sum"]["dataPoints"][0]
        assert dp["startTimeUnixNano"] == state.start_time_ns
        kinds.add(next(
            next(iter(a["value"].values()))
            for a in dp["attributes"]
            if a["key"] == "kind"
        ))
    assert kinds == {"waits", "time"}


def test_table_io_wait_ops_and_cumulative():
    rng = random.Random(7)
    state = MySQLMetricState(rng)
    tables = {"db": ["t1"]}
    m1 = _generate_mysql_metrics(state, tables, rng, None)
    m2 = _generate_mysql_metrics(state, tables, rng, None)
    ops = _attr_ops(m1, "mysql.table.io.wait.count")
    assert ops == set(TABLE_IO_OPERATIONS)
    c1 = next(
        m for m in m1
        if m["name"] == "mysql.table.io.wait.count"
        and any(
            a.get("key") == "operation" and a.get("value", {}).get("stringValue") == "fetch"
            for a in m["sum"]["dataPoints"][0].get("attributes", [])
        )
    )
    c2 = next(
        m for m in m2
        if m["name"] == "mysql.table.io.wait.count"
        and any(
            a.get("key") == "operation" and a.get("value", {}).get("stringValue") == "fetch"
            for a in m["sum"]["dataPoints"][0].get("attributes", [])
        )
    )
    assert c1["sum"]["isMonotonic"] is True
    assert c1["sum"]["dataPoints"][0]["startTimeUnixNano"] == state.start_time_ns
    assert int(c2["sum"]["dataPoints"][0]["asInt"]) > int(c1["sum"]["dataPoints"][0]["asInt"])


def test_rate_panel_metrics_use_unit_one():
    rng = random.Random(8)
    state = MySQLMetricState(rng)
    metrics = _generate_mysql_metrics(state, {"db": ["t"]}, rng, None)
    for name in (
        "mysql.row_operations",
        "mysql.handlers",
        "mysql.locks",
        "mysql.page_operations",
        "mysql.buffer_pool.operations",
    ):
        for m in metrics:
            if m["name"] == name:
                assert m["unit"] == "1", name
                assert m["sum"]["dataPoints"][0]["startTimeUnixNano"] == state.start_time_ns


def test_counters_start_near_zero_for_rate_charts():
    state = MySQLMetricState(random.Random(42))
    assert state.query_count < 10_000
    assert max(state.row_operations.values()) < 10_000
    assert max(state.handlers.values()) < 10_000


def test_sum_timer_wait_is_delta_seconds_not_ns():
    """Queries KPIs treat sum_timer_wait as seconds then *1000 for ms."""
    rng = random.Random(3)
    state = MySQLMetricState(rng)
    metrics = _generate_mysql_metrics(state, {"db": ["t"]}, rng, None)
    waits = [m for m in metrics if m["name"] == "mysql.events_statements_summary_by_digest.sum_timer_wait"]
    assert waits
    for m in waits:
        assert m["unit"] == "s"
        # Realistic delta seconds — not ns-scale (1e9+)
        assert 0.01 <= _gauge(m) < 10_000

    timer = next(m for m in metrics if m["name"] == "mysql.events_waits_current.timer_wait")
    assert timer["unit"] == "s"
    assert 0.001 <= _gauge(timer) <= 5.0


def test_top_query_log_timer_is_seconds():
    from unittest.mock import MagicMock

    from generators.mysql_otel.generator import _generate_top_query_event

    client = MagicMock()
    client.build_log_record.side_effect = lambda **kw: kw
    rng = random.Random(1)
    event = _generate_top_query_event(
        client, rng, ["app_db"], {"app_db": ["orders"]}, "app",
    )
    attrs = event["attributes"]
    count = attrs["mysql.events_statements_summary_by_digest.count_star"]
    total = attrs["mysql.events_statements_summary_by_digest.sum_timer_wait"]
    assert count >= 20
    assert isinstance(total, float)
    assert 0.01 <= total < 10_000
    avg_ms = (total / count) * 1000
    assert 0.1 <= avg_ms < 5_000



def _dirty_ratio(metrics) -> float:
    """dirty / (dirty + clean) of mysql.buffer_pool.usage, as the package alert computes it."""
    usage = {}
    for m in metrics:
        if m["name"] != "mysql.buffer_pool.usage":
            continue
        dp = m["gauge"]["dataPoints"][0]
        status = next(a["value"]["stringValue"] for a in dp["attributes"] if a["key"] == "status")
        usage[status] = _gauge(m)
    return usage["dirty"] / (usage["dirty"] + usage["clean"])


def _pool_metrics(chaos, seed):
    rng = random.Random(seed)
    return _generate_mysql_metrics(MySQLMetricState(rng), {"db": ["t"]}, rng, chaos)


@pytest.mark.parametrize("probe", [0.8, 0.9, 1.0, 75.0, 90.0])
def test_dirty_ratio_breaches_alert_for_every_pool_size(probe):
    """Alert: dirty/(dirty+clean) > 0.75. Pool size is random per cycle, so check many seeds."""
    below = [
        (seed, round(_dirty_ratio(_pool_metrics({"high_buffer_pool_dirty_page_ratio": probe}, seed)), 3))
        for seed in range(50)
    ]
    assert all(ratio > 0.75 for _seed, ratio in below), [b for b in below if b[1] <= 0.75]


def test_healthy_dirty_ratio_stays_low_for_every_pool_size():
    ratios = [_dirty_ratio(_pool_metrics(None, seed)) for seed in range(50)]
    assert max(ratios) <= 0.1, max(ratios)


def test_buffer_pool_usage_is_emitted_as_double():
    """The alert divides without TO_DOUBLE, so long/long would truncate to 0."""
    for m in _pool_metrics({"high_buffer_pool_dirty_page_ratio": 0.9}, 1):
        if m["name"] == "mysql.buffer_pool.usage":
            assert "asDouble" in m["gauge"]["dataPoints"][0]

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.mysql_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['high_buffer_pool_dirty_page_ratio', 'high_connection_error_rate', 'high_row_lock_contention', 'high_slow_query_rate', 'replication_lag', 'thread_saturation']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
