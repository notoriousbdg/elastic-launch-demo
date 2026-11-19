"""CockroachDB-specific tests for the cockroachdb_otel generator."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

from generator_engine.db_common import build_signal_resource
from generators.cockroachdb_otel.generator import (
    METRIC_NAMES,
    build_block_cache_hit_rate_doc,
    build_capacity_utilization_doc,
    build_sql_metrics_by_node_doc,
    build_store_summary_doc,
    run,
)


# Written only as ES bulk docs (build_capacity_utilization_doc), never via OTLP.
_BULK_ONLY = {"capacity", "capacity_used"}


def _resource():
    return build_signal_resource(
        ns="testns",
        signal_type="metrics",
        dataset="cockroachdb",
        service_name="cockroachdb",
        service_version="24.3.0",
        service_instance_id="cockroachdb-1",
        host_name="testns-crdb-1",
        cloud={
            "provider": "gcp",
            "platform": "gcp_compute_engine",
            "region": "us-central1",
            "availability_zone": "us-central1-a",
        },
    )


def _metric_names(calls) -> set[str]:
    names: set[str] = set()
    for _args, kwargs in ((c.args, c.kwargs) for c in calls):
        metrics = _args[2] if len(_args) >= 3 else kwargs.get("metrics", [])
        for m in metrics:
            names.add(m["name"])
    return names


def _datapoint_attrs(metric: dict) -> dict:
    dps = metric.get("sum", metric.get("gauge", {})).get("dataPoints", [])
    if not dps:
        return {}
    attrs = {}
    for a in dps[0].get("attributes", []):
        val = a["value"]
        attrs[a["key"]] = next(iter(val.values()))
    return attrs


def test_emits_all_dashboard_metrics():
    client = MagicMock()
    stop = threading.Event()
    sends = {"n": 0}

    def _after_send(*_a, **_k):
        sends["n"] += 1
        # node_count=2 → stop once both nodes in the first cycle have emitted
        if sends["n"] >= 2:
            stop.set()

    client.send_metrics_scoped.side_effect = _after_send

    run(client, stop, scenario_data={
        "namespace": "testns",
        "generator_config": {"node_count": 2, "service_name": "cockroachdb"},
        "channel_registry": {},
    })

    assert client.send_metrics_scoped.call_count >= 2
    names = _metric_names(client.send_metrics_scoped.call_args_list)
    missing = set(METRIC_NAMES) - _BULK_ONLY - names
    assert not missing, f"missing metrics: {sorted(missing)}"
    # Emitting these over OTLP too would poison their mapping as gauge_long.
    assert not (_BULK_ONLY & names), "capacity metrics must only go out as bulk docs"


def test_node_vs_store_attributes():
    client = MagicMock()
    stop = threading.Event()
    client.send_metrics_scoped.side_effect = lambda *_a, **_k: stop.set()

    run(client, stop, scenario_data={
        "namespace": "testns",
        "generator_config": {"node_count": 1},
        "channel_registry": {},
    })

    metrics = client.send_metrics_scoped.call_args.args[2]
    by_name = {m["name"]: m for m in metrics}

    # Node-level: must have node_id, must NOT have store
    sql = _datapoint_attrs(by_name["sql_conns"])
    assert sql.get("node_id") == "1"
    assert "store" not in sql

    cpu = _datapoint_attrs(by_name["sys_cpu_combined_percent_normalized"])
    assert "store" not in cpu
    # Normalized 0–1 gauge (asDouble)
    dp = by_name["sys_cpu_combined_percent_normalized"]["gauge"]["dataPoints"][0]
    assert "asDouble" in dp
    assert 0.0 <= dp["asDouble"] <= 1.0

    # Store-level capacity is a co-located bulk doc, never an OTLP metric
    assert not (_BULK_ONLY & by_name.keys())
    cap_doc = build_capacity_utilization_doc(
        _resource(), node_id="1", store="1", capacity=1e9, capacity_used=5e8,
    )
    assert cap_doc["attributes"] == {"node_id": "1", "store": "1"}

    ranges = _datapoint_attrs(by_name["ranges_unavailable"])
    assert ranges.get("store") == "1"


def test_resource_filters_match_dashboards():
    client = MagicMock()
    stop = threading.Event()
    client.send_metrics_scoped.side_effect = lambda *_a, **_k: stop.set()

    run(client, stop, scenario_data={
        "namespace": "testns",
        "generator_config": {"node_count": 1, "service_name": "cockroachdb"},
        "channel_registry": {},
    })

    resource = client.send_metrics_scoped.call_args.args[0]
    attrs = {a["key"]: next(iter(a["value"].values())) for a in resource["attributes"]}
    # Package SLOs + alerts filter service.name == "cockroachdb" (receiver's fixed name);
    # scenario uniqueness lives on service.instance.id / host.name.
    assert attrs["service.name"] == "cockroachdb"
    assert attrs["service.instance.id"].startswith("testns-cockroachdb-")
    assert attrs["data_stream.dataset"] == "cockroachdb"
    assert attrs["host.name"].startswith("testns-crdb-")


def test_capacity_utilization_doc_colocates_capacity_and_used():
    """Capacity panels AND-filter both metrics; they must share one document."""
    doc = build_capacity_utilization_doc(
        _resource(),
        node_id="2",
        store="1",
        capacity=500 * 1024 ** 3,
        capacity_used=200 * 1024 ** 3,
        timestamp_ms=1_700_000_000_000,
    )
    assert doc["metrics"]["capacity"] == float(500 * 1024 ** 3) + 0.001
    assert doc["metrics"]["capacity_used"] == float(200 * 1024 ** 3) + 0.001
    assert doc["attributes"] == {"node_id": "2", "store": "1"}
    assert doc["data_stream"]["dataset"] == "cockroachdb.otel"
    assert "data_stream.type" not in doc["resource"]["attributes"]
    assert doc["resource"]["attributes"]["service.name"] == "testns-cockroachdb"


def test_block_cache_hit_rate_doc_colocates_hits_and_misses():
    """Hit-rate panel AND-filters both counters; they must share one document."""
    doc = build_block_cache_hit_rate_doc(
        _resource(),
        node_id="1",
        store="1",
        hits=1_000_000,
        misses=25_000,
        timestamp_ms=1_700_000_000_000,
        start_timestamp_ms=1_699_999_000_000,
    )
    assert doc["metrics"]["rocksdb_block_cache_hits"] == 1_000_000
    assert doc["metrics"]["rocksdb_block_cache_misses"] == 25_000
    assert doc["attributes"] == {"node_id": "1", "store": "1"}
    assert doc["start_timestamp"] == 1_699_999_000_000
    assert doc["data_stream"]["dataset"] == "cockroachdb.otel"


def test_store_summary_doc_colocates_range_and_replica_fields():
    """Store Summary needs ranges + replicas on one doc (OTLP is one-per-doc)."""
    doc = build_store_summary_doc(
        _resource(),
        node_id="2",
        store="1",
        ranges=1200,
        replicas=3600,
        replicas_leaseholders=1200,
        ranges_unavailable=0,
        ranges_underreplicated=12,
        timestamp_ms=1_700_000_000_000,
    )
    assert doc["metrics"] == {
        "ranges": 1200,
        "replicas": 3600,
        "replicas_leaseholders": 1200,
        "ranges_unavailable": 0,
        "ranges_underreplicated": 12,
    }
    assert doc["attributes"] == {"node_id": "2", "store": "1"}
    assert doc["data_stream"]["dataset"] == "cockroachdb.otel"
    assert "data_stream.type" not in doc["resource"]["attributes"]
    assert doc["resource"]["attributes"]["service.name"] == "testns-cockroachdb"


def test_sql_metrics_by_node_doc_colocates_txn_and_memory_fields():
    """SQL Metrics by Node needs open txns + SQL memory on the conns doc."""
    doc = build_sql_metrics_by_node_doc(
        _resource(),
        node_id="1",
        sql_conns=42,
        sql_statements_active=7,
        sql_txns_open=3,
        sql_mem_root_current=128 * 1024 * 1024,
        timestamp_ms=1_700_000_000_000,
    )
    assert doc["metrics"] == {
        "sql_conns": 42,
        "sql_statements_active": 7,
        "sql_txns_open": 3,
        "sql_mem_root_current": 128 * 1024 * 1024,
    }
    assert doc["attributes"] == {"node_id": "1"}
    assert "store" not in doc["attributes"]
    assert doc["data_stream"]["dataset"] == "cockroachdb.otel"


def test_counters_start_near_zero_for_rate_charts():
    """Avoid RATE/INCREASE spike-then-flatline from multi-million initials."""
    import random

    from generators.cockroachdb_otel.generator import _NodeState

    state = _NodeState(random.Random(42))
    assert state.sql_select.value < 10_000
    assert state.cache_hits.value < 50_000
    assert state.lease_xfer_ok.value < 1_000
    assert state.liveness_ok.value < 1_000
    assert state.rebalance_ranges.value < 1_000


def test_replication_counters_keep_advancing():
    import random

    from generators.cockroachdb_otel.generator import _NodeState

    rng = random.Random(7)
    state = _NodeState(rng)
    before = (
        state.lease_xfer_ok.value,
        state.rebalance_leases.value,
        state.range_splits.value,
        state.liveness_ok.value,
    )
    for _ in range(5):
        state.tick(rng, load=1.0)
    assert state.lease_xfer_ok.value > before[0]
    assert state.rebalance_leases.value > before[1]
    assert state.range_splits.value > before[2]
    assert state.liveness_ok.value > before[3]


from generators.cockroachdb_otel import SPEC

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.cockroachdb_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['cpu_utilization_high', 'deadlocks_detected', 'disk_i_o_operations_queued_high', 'io_admission_control_overload', 'l0_file_count_high', 'liveness_heartbeat_failures', 'lsm_read_amplification_high', 'runnable_goroutines_per_cpu_high', 'storage_capacity_usage_high', 'storage_write_stalls_detected', 'unavailable_ranges_detected', 'under_replicated_ranges_detected', 'unhealthy_rpc_connections']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


def test_sql_query_count_includes_failures_and_healthy_success_rate():
    """SLO: (Δsql_query_count - Δsql_failure_count) / Δsql_query_count >= 0.995."""
    import random
    from generators.cockroachdb_otel.generator import _NodeState, sql_query_count

    rng = random.Random(3)
    state = _NodeState(rng)
    q0, f0 = sql_query_count(state), state.sql_failure.value
    for _ in range(10):
        state.tick(rng)
    dq, df = sql_query_count(state) - q0, state.sql_failure.value - f0
    assert dq > 0 and (dq - df) / dq >= 0.995

    for _ in range(10):
        state.tick(rng, unavail_rate=0.9)
    dq2, df2 = sql_query_count(state) - q0 - dq, state.sql_failure.value - f0 - df
    assert df2 > 0 and (dq2 - df2) / dq2 < 0.995


def test_sql_query_count_emitted_per_node():
    client = MagicMock()
    stop = threading.Event()
    client.send_metrics_scoped.side_effect = lambda *_a, **_k: stop.set()
    run(client, stop, scenario_data={
        "namespace": "testns", "generator_config": {"node_count": 1}, "channel_registry": {},
    })
    metrics = client.send_metrics_scoped.call_args.args[2]
    m = next(x for x in metrics if x["name"] == "sql_query_count")
    assert m["sum"]["isMonotonic"] is True
    assert _datapoint_attrs(m) == {"node_id": "1"}
