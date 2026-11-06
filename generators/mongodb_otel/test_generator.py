"""Technology-specific tests for the ``mongodb_otel`` generator.

Standard emit/registry tests live in tests/test_generators_emit.py
(parametrized over GENERATORS) and run for this generator automatically.
Add tests here only for behaviour that is special to mongodb.
"""

from __future__ import annotations

import random
import threading

from generators.mongodb_otel import SPEC
from generators.mongodb_otel.generator import (
    CACHE_TYPES,
    CONNECTION_TYPES,
    MEMORY_TYPES,
    OPERATION_NAMES,
    MongoDBMetricState,
    _generate_metrics,
    run,
)


def _sum_value(metric: dict) -> float:
    dps = metric["sum"]["dataPoints"]
    return float(dps[0].get("asDouble", dps[0].get("asInt")))


def _metrics(chaos=None, seed=1):
    instance = "demo-mongodb-host:27017"
    databases = ["demo_primary"]
    rng = random.Random(seed)
    state = MongoDBMetricState(random.Random(0))
    return state, _generate_metrics(state, databases, instance, rng, chaos)


class _CaptureClient:
    def __init__(self):
        self.batches: list[tuple[dict, dict, list[dict]]] = []

    def send_metrics_scoped(self, resource, scope, metrics):
        self.batches.append((resource, scope, metrics))

    def close(self):
        pass


def _attr_map(metric: dict) -> dict[str, str]:
    dps = (metric.get("sum") or metric.get("gauge") or {}).get("dataPoints") or []
    if not dps:
        return {}
    out = {}
    for a in dps[0].get("attributes") or []:
        key = a.get("key")
        val = a.get("value", {})
        out[key] = val.get("stringValue") or val.get("intValue") or str(val)
    return out


def _resource_attr_map(resource: dict) -> dict[str, str]:
    out = {}
    for a in resource.get("attributes") or []:
        key = a.get("key")
        val = a.get("value", {})
        out[key] = val.get("stringValue") or val.get("intValue") or str(val)
    return out


def test_generate_metrics_dashboard_contract():
    """Attributes and types required by mongodb_otel Overview/Capacity/Operations."""
    instance = "demo-mongodb-host:27017"
    databases = ["demo_primary", "demo_analytics"]
    state = MongoDBMetricState(random.Random(0))
    metrics = _generate_metrics(state, databases, instance, random.Random(1))

    by_name: dict[str, list[dict]] = {}
    for m in metrics:
        by_name.setdefault(m["name"], []).append(m)

    assert set(by_name) >= {
        "mongodb.cache.operations",
        "mongodb.connection.count",
        "mongodb.cursor.count",
        "mongodb.cursor.timeout.count",
        "mongodb.data.size",
        "mongodb.global_lock.time",
        "mongodb.index.size",
        "mongodb.memory.usage",
        "mongodb.network.io.receive",
        "mongodb.network.io.transmit",
        "mongodb.operation.count",
        "mongodb.operation.time",
        "mongodb.session.count",
        "mongodb.storage.size",
    }

    # Server control reads attributes.mongodb.instance on every datapoint.
    for m in metrics:
        assert _attr_map(m).get("mongodb.instance") == instance, m["name"]

    # Operations panels call DELTA() — must be non-monotonic sums, not counters.
    for name in ("mongodb.network.io.receive", "mongodb.network.io.transmit"):
        assert "sum" in by_name[name][0]
        assert by_name[name][0]["sum"]["isMonotonic"] is False

    for name in (
        "mongodb.connection.count",
        "mongodb.cursor.count",
        "mongodb.cursor.timeout.count",
        "mongodb.session.count",
        "mongodb.data.size",
        "mongodb.storage.size",
        "mongodb.index.size",
        "mongodb.memory.usage",
    ):
        assert "sum" in by_name[name][0]
        assert by_name[name][0]["sum"]["isMonotonic"] is False

    for name in (
        "mongodb.cache.operations",
        "mongodb.global_lock.time",
        "mongodb.operation.count",
        "mongodb.operation.time",
    ):
        assert by_name[name][0]["sum"]["isMonotonic"] is True

    conn_types = {_attr_map(m).get("type") for m in by_name["mongodb.connection.count"]}
    assert conn_types == set(CONNECTION_TYPES)

    cache_types = {_attr_map(m).get("type") for m in by_name["mongodb.cache.operations"]}
    assert cache_types == set(CACHE_TYPES)

    ops = {_attr_map(m).get("operation") for m in by_name["mongodb.operation.count"]}
    assert ops == set(OPERATION_NAMES)

    namespaces = {_attr_map(m).get("db.namespace") for m in by_name["mongodb.data.size"]}
    assert namespaces == set(databases)

    mem_types = {_attr_map(m).get("type") for m in by_name["mongodb.memory.usage"]}
    assert mem_types == set(MEMORY_TYPES)


def test_run_emits_server_address_and_instance():
    client = _CaptureClient()
    stop = threading.Event()

    def _target():
        run(
            client,
            stop,
            scenario_data={
                "namespace": "inttest",
                "generator_config": {
                    "service_name": "mongodb-primary",
                    "host_name": "inttest-mongodb-host",
                    "server_port": 27017,
                    "databases": ["inttest_primary"],
                },
                "channel_registry": {},
            },
        )

    t = threading.Thread(target=_target)
    t.start()
    # Wait for at least one batch.
    for _ in range(50):
        if client.batches:
            stop.set()
            break
        t.join(0.1)
    else:
        stop.set()
    t.join(2)

    assert client.batches, "expected at least one metrics batch"
    resource, _scope, metrics = client.batches[0]
    res = _resource_attr_map(resource)
    assert res.get("server.address") == "inttest-mongodb-host"
    assert res.get("server.port") in ("27017", 27017) or str(res.get("server.port")) == "27017"
    assert all(_attr_map(m).get("mongodb.instance") == "inttest-mongodb-host:27017" for m in metrics)


def test_healthy_baseline_no_chaos():
    state, metrics = _metrics(None)
    avail = next(
        m for m in metrics
        if m["name"] == "mongodb.connection.count" and _attr_map(m).get("type") == "available"
    )
    assert _sum_value(avail) >= 10
    assert state.last_cursor_timeout_delta == 0
    assert state.last_global_lock_delta <= 500
    miss_share = state.last_cache_miss_delta / max(
        1, state.last_cache_hit_delta + state.last_cache_miss_delta
    )
    assert miss_share * 100 <= 5.0
    cursors = next(m for m in metrics if m["name"] == "mongodb.cursor.count")
    assert _sum_value(cursors) <= 1000
    sessions = next(m for m in metrics if m["name"] == "mongodb.session.count")
    assert _sum_value(sessions) <= 500
    if state.last_op_count_delta > 0:
        assert state.last_op_time_delta / state.last_op_count_delta <= 100.0


def test_connection_count_emitted_as_double():
    """Capacity utilization does long/long without TO_DOUBLE → 0% if asInt."""
    _, metrics = _metrics(None)
    conns = [m for m in metrics if m["name"] == "mongodb.connection.count"]
    assert len(conns) == 3
    for m in conns:
        dp = m["sum"]["dataPoints"][0]
        assert "asDouble" in dp, m
        assert "asInt" not in dp
    by_type = {_attr_map(m).get("type"): _sum_value(m) for m in conns}
    util = by_type["current"] / (by_type["current"] + by_type["available"])
    assert 0.05 < util < 0.95


def test_op_multiplier_alone_breaches_latency_alert():
    state, _ = _metrics({"high_operation_latency": 3.0})
    assert state.last_op_count_delta > 0
    assert state.last_op_time_delta / state.last_op_count_delta > 100.0


def test_connection_multiplier_alone_breaches_exhaustion_alert():
    _, metrics = _metrics({"connection_exhaustion": 2.0})
    avail = next(
        m for m in metrics
        if m["name"] == "mongodb.connection.count" and _attr_map(m).get("type") == "available"
    )
    assert _sum_value(avail) < 10


def test_cursor_timeout_delta_alone():
    state, _ = _metrics({"cursor_timeouts": 3.0})
    assert state.last_cursor_timeout_delta > 0


def test_global_lock_rate_alone():
    state, _ = _metrics({"global_lock_contention": 800.0})
    # Impact key is ms/s; bump is rate × assumed tick seconds (>500/s threshold).
    assert state.last_global_lock_delta >= int(800.0 * 15.0)


def test_cache_miss_rate_alone():
    state, _ = _metrics({"high_cache_miss_rate": 0.2})
    miss_share = state.last_cache_miss_delta / max(
        1, state.last_cache_hit_delta + state.last_cache_miss_delta
    )
    assert miss_share * 100 > 5.0


def test_cursor_count_alone():
    _, metrics = _metrics({"high_cursor_count": 1500.0})
    cursors = next(m for m in metrics if m["name"] == "mongodb.cursor.count")
    assert _sum_value(cursors) > 1000


def test_resident_memory_alone():
    _, metrics = _metrics({"high_memory_usage": 5_000_000_000})
    resident = next(
        m for m in metrics
        if m["name"] == "mongodb.memory.usage" and _attr_map(m).get("type") == "resident"
    )
    assert _sum_value(resident) > 4_294_967_296


def test_session_count_alone():
    _, metrics = _metrics({"high_session_count": 600.0})
    sessions = next(m for m in metrics if m["name"] == "mongodb.session.count")
    assert _sum_value(sessions) > 500

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.mongodb_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['connection_exhaustion', 'cursor_timeouts', 'global_lock_contention', 'high_cache_miss_rate', 'high_cursor_count', 'high_memory_usage', 'high_operation_latency', 'high_session_count']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
