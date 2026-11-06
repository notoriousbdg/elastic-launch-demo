"""Technology-specific tests for the ``postgresql_otel`` generator.

Standard emit/registry tests live in tests/test_generators_emit.py
(parametrized over GENERATORS) and run for this generator automatically.

These tests pin the dashboard field contracts that were blank/erroring:
  - resource.attributes.postgresql.database.name on per-database metrics
  - lock_type / mode / relation on postgresql.database.locks
  - temp_files / temp.io as monotonic cumulative counters
  - bgwriter.duration type = sync|write
  - query_sample wait_event / wait_event_type / network.peer.address
"""

from __future__ import annotations

import random

from generators.postgresql_otel import generator as pg


def _attr_map(dp_or_metric_attrs: list[dict]) -> dict:
    out = {}
    for a in dp_or_metric_attrs or []:
        val = a.get("value", {})
        out[a["key"]] = (
            val.get("stringValue")
            or val.get("intValue")
            or val.get("doubleValue")
        )
    return out


def test_db_resource_carries_database_name():
    base = {
        "attributes": [
            {"key": "service.name", "value": {"stringValue": "postgresql-primary"}},
        ],
        "schemaUrl": "https://example",
    }
    res = pg._db_metrics_resource(base, "orders")
    attrs = _attr_map(res["attributes"])
    assert attrs["postgresql.database.name"] == "orders"
    assert attrs["service.name"] == "postgresql-primary"


def test_locks_emit_dashboard_attributes():
    rng = random.Random(0)
    state = pg.PostgreSQLMetricState(rng, n_databases=1)
    metrics = pg._generate_db_metrics(
        state, 0, "orders", ["accounts", "ledger"], rng, load=0.8,
    )
    locks = [m for m in metrics if m["name"] == "postgresql.database.locks"]
    assert locks, "expected postgresql.database.locks datapoints"
    for m in locks:
        attrs = _attr_map(m["gauge"]["dataPoints"][0]["attributes"])
        assert attrs["lock_type"] in pg.LOCK_TYPES
        assert attrs["mode"] in pg.LOCK_MODES
        assert attrs["relation"] in ("accounts", "ledger")
        assert attrs["db.namespace"] == "orders"


def test_temp_metrics_are_monotonic_counters():
    rng = random.Random(1)
    state = pg.PostgreSQLMetricState(rng, n_databases=1)
    metrics = pg._generate_db_metrics(
        state, 0, "orders", ["accounts"], rng, load=0.5,
    )
    by_name = {m["name"]: m for m in metrics}
    for name in ("postgresql.temp_files", "postgresql.temp.io"):
        m = by_name[name]
        assert "sum" in m, f"{name} must be a cumulative sum for INCREASE/RATE"
        assert m["sum"]["isMonotonic"] is True
        assert m["sum"]["aggregationTemporality"] == 2


def test_bgwriter_duration_types_are_sync_write():
    rng = random.Random(2)
    state = pg.PostgreSQLMetricState(rng, n_databases=1)
    metrics = pg._generate_instance_metrics(state, n_databases=1, rng=rng)
    durations = [m for m in metrics if m["name"] == "postgresql.bgwriter.duration"]
    types = {
        _attr_map(m["sum"]["dataPoints"][0]["attributes"])["type"]
        for m in durations
    }
    assert types == {"sync", "write"}
    maxwritten = [m for m in metrics if m["name"] == "postgresql.bgwriter.maxwritten"]
    assert len(maxwritten) == 1
    assert "attributes" not in maxwritten[0]["sum"]["dataPoints"][0]


class _MiniClient:
    def build_log_record(self, **kwargs):
        return kwargs

    def build_span(self, **kwargs):
        return kwargs


def test_query_sample_has_active_queries_fields():
    client = _MiniClient()
    rng = random.Random(3)
    log_record, _span = pg._generate_query_sample(
        client,
        rng,
        databases=["orders"],
        tables={"orders": ["accounts"]},
        query_templates=pg.DEFAULT_QUERY_TEMPLATES,
        cfg={"users": ["app_user"]},
        ns="test",
    )
    assert log_record["event_name"] == "db.server.query_sample"
    attrs = log_record["attributes"]
    assert "postgresql.state" in attrs
    assert "postgresql.wait_event_type" in attrs
    assert "postgresql.wait_event" in attrs
    assert "network.peer.address" in attrs
    assert attrs["network.peer.address"]


def test_query_sample_wait_fields_never_empty():
    """Active Queries table renders empty wait_* as (null); always populate."""
    client = _MiniClient()
    for seed in range(40):
        log_record, _span = pg._generate_query_sample(
            client,
            random.Random(seed),
            databases=["orders"],
            tables={"orders": ["accounts"]},
            query_templates=pg.DEFAULT_QUERY_TEMPLATES,
            cfg={"users": ["app_user"]},
            ns="test",
        )
        attrs = log_record["attributes"]
        assert attrs["postgresql.wait_event_type"], attrs
        assert attrs["postgresql.wait_event"], attrs
        assert attrs["postgresql.wait_event_type"] in pg.WAIT_EVENT_TYPES
        assert attrs["postgresql.wait_event"] in pg.WAIT_EVENTS[attrs["postgresql.wait_event_type"]]


def test_temp_gauge_detection_reads_field_caps():
    """_temp_fields_mapped_as_gauge returns True when time_series_metric=gauge."""
    from unittest.mock import patch

    class _Resp:
        status_code = 200

        def json(self):
            return {
                "fields": {
                    "postgresql.temp_files": {
                        "long": {"time_series_metric": "gauge"},
                    },
                    "postgresql.temp.io": {
                        "long": {"time_series_metric": "counter"},
                    },
                }
            }

    with patch("httpx.get", return_value=_Resp()):
        assert pg._temp_fields_mapped_as_gauge("https://es.example", "key") is True


def test_temp_gauge_detection_false_when_counters():
    from unittest.mock import patch

    class _Resp:
        status_code = 200

        def json(self):
            return {
                "fields": {
                    "postgresql.temp_files": {
                        "long": {"time_series_metric": "counter"},
                    },
                    "postgresql.temp.io": {
                        "long": {"time_series_metric": "counter"},
                    },
                }
            }

    with patch("httpx.get", return_value=_Resp()):
        assert pg._temp_fields_mapped_as_gauge("https://es.example", "key") is False


def _gauge(metric: dict) -> float:
    dps = metric.get("gauge", {}).get("dataPoints") or []
    return float(dps[0].get("asDouble", dps[0].get("asInt", 0)))


def _sum_val(metric: dict) -> float:
    dps = metric.get("sum", {}).get("dataPoints") or []
    return float(dps[0].get("asDouble", dps[0].get("asInt", 0)))


def test_healthy_baseline_no_chaos():
    rng = random.Random(0)
    state = pg.PostgreSQLMetricState(rng, n_databases=1)
    before = dict(state.deadlocks)
    state.tick(rng, 1, None)
    assert state.deadlocks[0] - before[0] == 0
    metrics = pg._generate_db_metrics(state, 0, "orders", ["t"], rng, 0.5, None)
    backends = next(m for m in metrics if m["name"] == "postgresql.backends")
    # connection.max is ~100-200; healthy util should stay under 80%
    assert _gauge(backends) < 160
    inst = pg._generate_instance_metrics(state, 1, rng, None)
    lag = next(m for m in inst if m["name"] == "postgresql.wal.lag")
    assert _gauge(lag) == 0.0


def test_connection_utilization_alone():
    rng = random.Random(2)
    state = pg.PostgreSQLMetricState(rng, n_databases=1)
    metrics = pg._generate_db_metrics(
        state, 0, "orders", ["t"], rng, 0.5, {"connection_utilization_high": 0.9},
    )
    backends = next(m for m in metrics if m["name"] == "postgresql.backends")
    max_conn = state._last_max_conn
    # OOTB integer-divides SUM/MAX; a few ticks of this tip must exceed max_conn.
    assert max_conn == 10_000
    assert _gauge(backends) * 3 > max_conn


def test_multi_db_backends_sum_within_max_conn():
    """Panel is SUM(backends)/MAX(max_conn) — multi-DB total must stay ~<= max."""
    rng = random.Random(4)
    n = 4
    state = pg.PostgreSQLMetricState(rng, n_databases=n)
    for chaos in (None, {"connection_utilization_high": 0.9}):
        state._backends_per_db = None
        pg._allocate_connection_budget(state, n, rng, chaos)
        total = sum(state._backends_per_db)
        max_conn = state._last_max_conn
        assert total <= max_conn + 1  # +epsilon for int rounding
        assert max_conn == 10_000
        if chaos:
            # ~3 ticks must clear integer-division bar (SUM > MAX).
            assert total * 3 > max_conn
        else:
            # Healthy windowed SUM must stay under max_conn.
            assert total * 50 < max_conn
        # Per-DB emit must consume the shared partition, not re-roll max_conn.
        emitted = 0
        for i in range(n):
            metrics = pg._generate_db_metrics(
                state, i, f"db{i}", ["t"], rng, 0.5, chaos, n_databases=n,
            )
            backends = next(m for m in metrics if m["name"] == "postgresql.backends")
            emitted += _gauge(backends)
        assert emitted == total
        assert state._last_max_conn == max_conn


def test_no_exclusive_locks_without_chaos():
    """OOTB alert: SUM(locks) WHERE mode IN Exclusive*/AccessExclusive* > 5."""
    rng = random.Random(5)
    state = pg.PostgreSQLMetricState(rng, n_databases=1)
    for _ in range(20):
        metrics = pg._generate_db_metrics(state, 0, "orders", ["t"], rng, 0.5, None)
        locks = [m for m in metrics if m["name"] == "postgresql.database.locks"]
        for m in locks:
            attrs = _attr_map(m["gauge"]["dataPoints"][0]["attributes"])
            if attrs.get("mode") in ("ExclusiveLock", "AccessExclusiveLock"):
                assert _gauge(m) == 0


def test_backend_buffer_writes_flat_without_chaos():
    """OOTB alert: SUM(bgwriter.buffers.writes) WHERE source=='backend' > 100."""
    rng = random.Random(6)
    state = pg.PostgreSQLMetricState(rng, n_databases=1)
    before = state.bgwriter_writes["backend"]
    assert before == 0
    for _ in range(10):
        pg._generate_instance_metrics(state, 1, rng, None)
        state.tick(rng, 1, None)
    assert state.bgwriter_writes["backend"] == 0


def test_checkpoint_duration_stays_under_threshold_without_chaos():
    """OOTB alert: SUM(bgwriter.duration) > 30000 (ms)."""
    rng = random.Random(7)
    state = pg.PostgreSQLMetricState(rng, n_databases=1)
    for _ in range(200):
        state.tick(rng, 1, None)
    assert sum(state.bgwriter_duration.values()) < 30000


def test_query_sample_never_lock_wait_without_chaos():
    """OOTB alert fires on any wait_event_type == Lock row; must never occur healthy."""
    client = _MiniClient()
    for seed in range(60):
        log_record, _span = pg._generate_query_sample(
            client,
            random.Random(seed),
            databases=["orders"],
            tables={"orders": ["accounts"]},
            query_templates=pg.DEFAULT_QUERY_TEMPLATES,
            cfg={"users": ["app_user"]},
            ns="test",
            active_chaos=None,
        )
        assert log_record["attributes"]["postgresql.wait_event_type"] != "Lock"


def test_exclusive_lock_count_alone():
    rng = random.Random(3)
    state = pg.PostgreSQLMetricState(rng, n_databases=1)
    metrics = pg._generate_db_metrics(
        state, 0, "orders", ["t"], rng, 0.5, {"exclusive_lock_contention": 12},
    )
    locks = [m for m in metrics if m["name"] == "postgresql.database.locks"]
    exclusive = [
        m for m in locks
        if _attr_map(m["gauge"]["dataPoints"][0]["attributes"]).get("mode")
        in ("ExclusiveLock", "AccessExclusiveLock")
    ]
    assert exclusive
    assert max(_gauge(m) for m in exclusive) >= 12



def test_absolute_sum_counters_reset_when_chaos_clears():
    """OOTB alerts SUM absolute snapshots — elevated tips must drop to 0 on resolve."""
    rng = random.Random(11)
    state = pg.PostgreSQLMetricState(rng, n_databases=1)
    chaos = {
        "bgwriter_maxwritten_stops": 5,
        "backend_buffer_writes_high": 40,
        "high_sequential_scans": 10,
        "temp_files_created": 3,
    }
    for _ in range(3):
        state.tick(rng, 1, chaos)
    assert state.bgwriter_maxwritten >= 5
    assert state.bgwriter_writes["backend"] >= 40
    assert state.sequential_scans[0] >= 10
    assert state.temp_files[0] >= 3
    state.tick(rng, 1, None)
    assert state.bgwriter_maxwritten == 0
    assert state.bgwriter_writes["backend"] == 0
    assert state.sequential_scans[0] == 0
    assert state.temp_files[0] == 0
    assert state.temp_io[0] == 0

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.postgresql_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['backend_buffer_writes_high', 'bgwriter_maxwritten_stops', 'connection_utilization_high', 'deadlocks_detected', 'exclusive_lock_contention', 'high_rollback_rate', 'high_sequential_scans', 'high_temp_i_o_volume', 'long_checkpoint_duration', 'long_running_queries', 'low_buffer_hit_ratio', 'queries_waiting_on_locks', 'slow_top_queries', 'temp_files_created']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
