"""Tests for the gcp_cloudsql_mysql_otel generator."""

import random

import pytest

from generator_engine.gcp_cloudsql_common import build_instances, default_cloud, instance_resource
from generator_engine.identity import ScenarioIdentity
from generators.gcp_cloudsql_mysql_otel import SPEC
from generators.gcp_cloudsql_mysql_otel.generator import METRIC_NAMES, generate_metrics

NS = "gtgcpsqlmysql"
MY = "cloudsql.googleapis.com/database/mysql/"
DB = "cloudsql.googleapis.com/database/"

DELTA_METRICS = {
    MY + "queries", MY + "slow_queries_count", MY + "connections_count",
    MY + "innodb/row_lock_waits_count", MY + "innodb/deadlocks_count",
    DB + "uptime", DB + "disk/read_ops_count", DB + "network/received_bytes_count",
}
GAUGE_METRICS = {
    DB + "up", DB + "instance_state", DB + "cpu/utilization", DB + "disk/quota",
    MY + "max_connections", MY + "threads", DB + "replication/replica_lag",
}


def _instances(seed=7):
    rng = random.Random(seed)
    return build_instances(
        ScenarioIdentity(NS), {}, engine_role="mysql", default_tier="db-custom-4-16384",
        disk_gb=500, project_id=ScenarioIdentity(NS).gcp_project_id, cloud=default_cloud(), rng=rng,
    )


def _emit(chaos=None, seed=3, ticks=1):
    rng = random.Random(seed)
    insts = _instances(seed)
    out = None
    for _ in range(ticks):
        out = generate_metrics(insts, rng, chaos, 4000, load=0.8)
    return {inst.role: metrics for inst, metrics in out}


def _attrs(dp):
    return {a["key"]: next(iter(a["value"].values())) for a in dp.get("attributes", [])}


def _val(metric):
    body = metric.get("gauge") or metric.get("sum")
    dp = body["dataPoints"][0]
    return float(dp["asDouble"]) if "asDouble" in dp else float(dp["asInt"])


def _one(metrics, name, **attrs):
    for m in metrics:
        if m["name"] != name:
            continue
        body = m.get("gauge") or m.get("sum")
        if all(_attrs(body["dataPoints"][0]).get(k) == v for k, v in attrs.items()):
            return _val(m)
    raise AssertionError(f"{name} {attrs} not emitted")


def _state(metrics, name="cloudsql.googleapis.com/database/instance_state"):
    on = [_attrs((m.get("gauge"))["dataPoints"][0])["state"] for m in metrics
          if m["name"] == name and _val(m) == 1]
    return on


# ── SPEC / shape ──────────────────────────────────────────────────────────────

def test_spec_sanity():
    assert SPEC.id == "gcp_cloudsql_mysql_otel"
    assert SPEC.client == "otlp" and SPEC.signals == ("metrics",)
    assert SPEC.datasets == ("gcp.cloudsql_mysql.otel",)
    assert SPEC.integrations == ("gcp_cloudsql_mysql_otel",)
    assert SPEC.wants_chaos and SPEC.chaos_mode == "explicit" and SPEC.family == "database"
    assert not SPEC.default_enabled and not SPEC.core and not SPEC.always_install
    assert all(not f.get("required") for f in SPEC.config_fields.values())
    assert len(SPEC.impact_keys) >= 3
    for key, meta in SPEC.impact_keys.items():
        assert meta["default"] == 0.0, key


def test_every_dashboard_metric_is_emitted():
    emitted = {m["name"] for ms in _emit().values() for m in ms}
    assert set(METRIC_NAMES) <= emitted


def test_metric_kinds_match_cloud_monitoring():
    metrics = _emit()["replica"] + _emit()["primary"]
    by_name = {m["name"]: m for m in metrics}
    for name in DELTA_METRICS:
        s = by_name[name]["sum"]
        assert s["aggregationTemporality"] == 1 and s["isMonotonic"] is False, name
    for name in GAUGE_METRICS:
        assert "gauge" in by_name[name], name


def test_resource_routes_to_package_stream():
    inst = _instances()[0]
    res = instance_resource(ScenarioIdentity(NS), inst, dataset="gcp.cloudsql_mysql",
                            service_role="cloudsql-mysql", cloud=default_cloud())
    attrs = {a["key"]: next(iter(a["value"].values())) for a in res["attributes"]}
    assert attrs["data_stream.dataset"] == "gcp.cloudsql_mysql"
    assert attrs["gcp.resource_type"] == "cloudsql_database"
    assert attrs["database_id"] == f"{NS}-project:{NS}-mysql-primary"
    assert attrs["project_id"] == f"{NS}-project" and attrs["region"] == "us-central1"


# ── healthy baseline ─────────────────────────────────────────────────────────

def test_healthy_baseline():
    out = _emit()
    for role, ms in out.items():
        assert _one(ms, DB + "up") == 1
        assert _one(ms, DB + "uptime") == 60
        assert _state(ms) == ["RUNNING"]
        assert _one(ms, DB + "cpu/utilization") < 0.6
        assert _one(ms, DB + "memory/utilization") < 0.75
        assert _one(ms, DB + "disk/utilization") < 0.6
        assert _one(ms, MY + "queries") >= 60  # marker > 0 and hexagon >= 50
        assert _one(ms, MY + "slow_queries_count") / _one(ms, MY + "queries") < 0.01
        assert _one(ms, MY + "innodb/deadlocks_count") == 0
    p = out["primary"]
    assert _one(p, DB + "network/connections") / _one(p, MY + "max_connections") < 0.1
    r = out["replica"]
    assert _one(r, DB + "replication/replica_lag") < 1.0
    assert _one(r, MY + "replication/slave_io_running_state", state="Yes") == 1
    assert _one(r, MY + "replication/seconds_behind_master") < 5


# ── each impact key ──────────────────────────────────────────────────────────

def test_cpu_utilization_high():
    p = _emit({"cpu_utilization_high": 95.0})["primary"]
    assert _one(p, DB + "cpu/utilization") >= 0.9


def test_memory_utilization_high():
    p = _emit({"memory_utilization_high": 96.0})["primary"]
    assert _one(p, DB + "memory/utilization") >= 0.9


def test_disk_utilization_high():
    p = _emit({"disk_utilization_high": 93.0})["primary"]
    assert _one(p, DB + "disk/utilization") >= 0.9
    assert _one(p, DB + "disk/bytes_used") / _one(p, DB + "disk/quota") >= 0.9


def test_connections_high():
    p = _emit({"connections_high": 3800.0})["primary"]
    assert _one(p, DB + "network/connections") / _one(p, MY + "max_connections") >= 0.9
    assert _one(p, MY + "threads", thread_kind="THREADS_CONNECTED") >= 3800
    assert _one(p, MY + "aborted_connects_count") > 10


def test_replica_lag_high():
    out = _emit({"replica_lag_high": 300.0})
    r = out["replica"]
    assert _one(r, DB + "replication/replica_lag") >= 280
    assert _one(r, MY + "replication/seconds_behind_master") >= 280
    assert _one(r, MY + "replication/last_sql_errno") != 0
    assert not any(m["name"].endswith("replica_lag") for m in out["primary"])


def test_instance_unavailable():
    p = _emit({"instance_unavailable": 1.0})["primary"]
    assert _one(p, DB + "up") == 0
    assert _state(p) == ["FAILED"]
    assert 0 < _one(p, DB + "uptime") < 55  # partial downtime minute
    assert _one(p, MY + "queries") > 0  # instance still counted by the marker


def test_slow_queries_high():
    p = _emit({"slow_queries_high": 15.0})["primary"]
    ratio = _one(p, MY + "slow_queries_count") / _one(p, MY + "queries")
    assert 0.14 <= ratio <= 0.16
    assert _one(p, MY + "tmp_disk_tables_created_count") >= 300


def test_innodb_lock_waits_high():
    p = _emit({"innodb_lock_waits_high": 40.0})["primary"]
    assert _one(p, MY + "innodb/row_lock_waits_count") / 60 >= 35
    assert _one(p, MY + "innodb/lock_timeout_count") > 0
    assert _one(p, MY + "innodb/deadlocks_count") > 0


def test_seeded_emit_is_deterministic():
    assert _values(_emit(None, seed=11)) == _values(_emit(None, seed=11))


@pytest.mark.parametrize("key", sorted(SPEC.impact_keys))
def test_each_key_changes_emit(key):
    meta = SPEC.impact_keys[key]
    hot = {k: v["default"] for k, v in SPEC.impact_keys.items()}
    hot[key] = meta["range"][1] * 0.9 if meta["range"][1] > 1 else 1.0
    assert _values(_emit(hot, seed=11)) != _values(_emit(None, seed=11))


def _values(out):
    """Emit content without timestamps, so only values and attributes compare."""
    return sorted(
        (role, m["name"], tuple(sorted(_attrs((m.get("gauge") or m.get("sum"))["dataPoints"][0]).items())), _val(m))
        for role, ms in out.items() for m in ms
    )
