"""Technology-specific tests for the ``redis`` generator."""

from __future__ import annotations

import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "tests"))

from generator_engine.db_common import build_signal_resource
from generators.redis_otel import SPEC
from generators.redis_otel.generator import (
    RedisMetricState,
    _bulk_dynamic_templates,
    _generate_metrics,
    build_cpu_time_docs,
    build_db_keys_doc,
    build_memory_utilization_doc,
    build_overview_metrics_doc,
)

MAXMEM = 2 * 1024 * 1024 * 1024


def _sample_resource():
    return build_signal_resource(
        ns="fanatics",
        signal_type="metrics",
        dataset="redisreceiver",
        service_name="redis-cache",
        service_version="7.2.3",
        service_instance_id="redis-cache-001",
        host_name="fanatics-redis-host",
        cloud={
            "provider": "aws",
            "platform": "aws_ec2",
            "region": "us-east-1",
            "availability_zone": "us-east-1a",
        },
    )


def _gauge(metrics, name: str) -> float:
    m = next(x for x in metrics if x["name"] == name)
    dp = m["gauge"]["dataPoints"][0]
    return float(dp.get("asDouble", dp.get("asInt")))


def _metrics(chaos=None, seed=1, keyspaces=None):
    rng = random.Random(seed)
    state = RedisMetricState(rng)
    metrics, mem_used, maxmem, ks_stats, overview = _generate_metrics(
        state, keyspaces or [0], rng, MAXMEM, chaos,
    )
    return state, metrics, mem_used, maxmem, ks_stats, overview


def test_memory_utilization_doc_colocates_used_and_maxmemory():
    """Gauge panel AND-filters both metrics; they must share one document."""
    resource = _sample_resource()
    doc = build_memory_utilization_doc(resource, mem_used=1_610_612_736, maxmemory=2_147_483_648)

    assert doc["metrics"]["redis.memory.used"] == 1_610_612_736
    assert doc["metrics"]["redis.maxmemory"] == 2_147_483_648
    assert doc["data_stream"]["dataset"] == "redisreceiver.otel"
    assert "data_stream.type" not in doc["resource"]["attributes"]
    assert doc["resource"]["attributes"]["service.name"] == "fanatics-redis-cache"


def test_db_keys_doc_colocates_keys_expires_avg_ttl():
    """Keys per Database With TTL / Avg TTL need siblings on one document."""
    resource = _sample_resource()
    doc = build_db_keys_doc(resource, db=0, keys=10_000, expires=2_500, avg_ttl=120_000)

    assert doc["attributes"]["db"] == "0"
    assert doc["metrics"]["redis.db.keys"] == 10_000
    assert doc["metrics"]["redis.db.expires"] == 2_500
    assert doc["metrics"]["redis.db.avg_ttl"] == 120_000
    assert doc["data_stream"]["dataset"] == "redisreceiver.otel"


def test_generate_metrics_returns_keyspace_stats_for_bulk():
    _, metrics, _, _, ks_stats, overview = _metrics(None, keyspaces=[0, 1])
    assert [db for db, *_ in ks_stats] == [0, 1]
    for _db, keys, expires, avg_ttl in ks_stats:
        assert keys > 0
        assert 0 < expires <= keys
        assert avg_ttl > 0
    # OTLP gauges still emitted (bulk is additive, not a replacement).
    assert sum(1 for m in metrics if m["name"] == "redis.db.keys") == 2
    assert sum(1 for m in metrics if m["name"] == "redis.db.expires") == 2
    assert sum(1 for m in metrics if m["name"] == "redis.db.avg_ttl") == 2
    assert "clients.connected" in overview
    assert overview["clients.connected"] > 0


def test_healthy_baseline_no_chaos():
    state, metrics, mem_used, maxmem, _, _ = _metrics(None)
    assert state.last_evicted_delta == 0
    assert state.last_rejected_delta == 0
    assert mem_used / maxmem * 100.0 <= 90.0
    assert _gauge(metrics, "redis.memory.fragmentation_ratio") >= 1.0
    assert _gauge(metrics, "redis.slaves.connected") >= 1


def test_evictions_and_rejections_stay_zero_absolute_without_chaos():
    """OOTB alerts fire on absolute SUM/MAX > 0 — seeds must start at 0."""
    rng = random.Random(2)
    state = RedisMetricState(rng)
    assert state.keys_evicted == 0
    assert state.connections_rejected == 0
    for _ in range(15):
        _generate_metrics(state, [0], rng, MAXMEM, None)
    assert state.keys_evicted == 0
    assert state.connections_rejected == 0


def test_eviction_multiplier_alone_breaches_eviction_alert():
    state, _metrics_out, _, _, _, _ = _metrics({"high_eviction_rate": 3.0})
    assert state.last_evicted_delta > 100
    assert state.last_rejected_delta == 0


def test_connection_multiplier_alone_breaches_rejected_alert():
    state, metrics, mem_used, maxmem, _, _ = _metrics({"rejected_connections": 2.0})
    assert state.last_rejected_delta > 0
    assert state.last_evicted_delta == 0
    assert mem_used / maxmem * 100.0 <= 90.0
    assert _gauge(metrics, "redis.clients.connected") > 0


def test_memory_pressure_alone_breaches_memory_alert():
    state, metrics, mem_used, maxmem, _, _ = _metrics({"memory_approaching_maxmemory": 1.0})
    # OOTB ES|QL integer-divides used/max — need used >= maxmemory.
    assert mem_used >= maxmem
    assert mem_used / maxmem * 100.0 >= 100.0
    assert _gauge(metrics, "redis.memory.fragmentation_ratio") >= 1.0
    assert state.last_evicted_delta == 0


def test_replica_disconnect_alone_breaches_replica_alert():
    _, metrics, _, _, _, _ = _metrics({"primary_has_no_connected_replicas": 1.0})
    assert _gauge(metrics, "redis.slaves.connected") == 0


def test_cpu_time_docs_per_state():
    resource = _sample_resource()
    docs = build_cpu_time_docs(resource, {"user": 12.5, "sys": 3.25}, timestamp_ms=1)
    assert len(docs) == 2
    by_state = {d["attributes"]["state"]: d["metrics"]["redis.cpu.time"] for d in docs}
    assert by_state == {"user": 12.5, "sys": 3.25}


def test_bulk_templates_mark_rate_fields_as_counters():
    resource = _sample_resource()
    overview = {
        "clients.connected": 10,
        "connections.rejected": 0,
        "keys.evicted": 0,
        "net.input": 100,
        "memory.fragmentation_ratio": 1.1,
    }
    docs = [
        build_overview_metrics_doc(resource, overview, timestamp_ms=1),
        *build_cpu_time_docs(resource, {"user": 1.0}, timestamp_ms=1),
    ]
    templates = _bulk_dynamic_templates(docs)
    assert templates["metrics.redis.connections.rejected"] == "counter_long"
    assert templates["metrics.redis.keys.evicted"] == "counter_long"
    assert templates["metrics.redis.net.input"] == "counter_long"
    assert templates["metrics.redis.cpu.time"] == "counter_double"
    assert templates["metrics.redis.clients.connected"] == "gauge_long"
    assert templates["metrics.redis.memory.fragmentation_ratio"] == "gauge_double"


# --- 1:1 alert impact_keys coverage (auto) ---
from generators.redis_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['high_eviction_rate', 'memory_approaching_maxmemory', 'memory_fragmentation_indicates_swapping', 'primary_has_no_connected_replicas', 'rejected_connections']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


def _cmd_latency(metrics) -> dict[tuple[str, str], float]:
    out = {}
    for m in metrics:
        if m["name"] != "redis.cmd.latency":
            continue
        for dp in m["gauge"]["dataPoints"]:
            attrs = {a["key"]: a["value"]["stringValue"] for a in dp["attributes"]}
            out[(attrs["cmd"], attrs["percentile"])] = dp["asDouble"]
    return out


def test_cmd_latency_p99_series_emitted_and_healthy_under_slo():
    """SLO "P99 command latency" filters attributes.percentile: "p99"; MAX < 0.01 s."""
    lat = _cmd_latency(_metrics()[1])
    p99 = [v for (_, pct), v in lat.items() if pct == "p99"]
    assert p99 and max(p99) < 0.01
    for (cmd, pct), v in lat.items():
        if pct == "p99":
            assert lat[(cmd, "p50")] <= v <= lat[(cmd, "p99.9")]


def test_cmd_latency_p99_degrades_when_swapping():
    lat = _cmd_latency(_metrics(chaos={"memory_fragmentation_indicates_swapping": 10.0})[1])
    assert max(v for (_, pct), v in lat.items() if pct == "p99") >= 0.01
