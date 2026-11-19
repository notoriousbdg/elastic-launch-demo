"""Tests for the gcp_cloudsql_sqlserver_otel generator."""

import random

import pytest

from generator_engine.gcp_cloudsql_common import build_instances, default_cloud, instance_resource
from generator_engine.identity import ScenarioIdentity
from generators.gcp_cloudsql_sqlserver_otel import SPEC
from generators.gcp_cloudsql_sqlserver_otel.generator import METRIC_NAMES, generate_metrics

NS = "gtgcpsqlsqlserver"
SS = "cloudsql.googleapis.com/database/sqlserver/"
DB = "cloudsql.googleapis.com/database/"
DATABASES = ScenarioIdentity(NS).databases(["orders", "inventory", "billing"])


def _instances(seed):
    return build_instances(
        ScenarioIdentity(NS), {}, engine_role="sqlserver", default_tier="db-custom-8-32768",
        disk_gb=750, project_id=ScenarioIdentity(NS).gcp_project_id, cloud=default_cloud(),
        rng=random.Random(seed),
    )


def _emit(chaos=None, seed=3):
    rng = random.Random(seed)
    return {inst.role: ms for inst, ms in generate_metrics(_instances(seed), DATABASES, rng, chaos, load=0.8)}


def _attrs(m):
    body = m.get("gauge") or m.get("sum")
    return {a["key"]: next(iter(a["value"].values())) for a in body["dataPoints"][0].get("attributes", [])}


def _val(m):
    dp = (m.get("gauge") or m.get("sum"))["dataPoints"][0]
    return float(dp["asDouble"]) if "asDouble" in dp else float(dp["asInt"])


def _sum(metrics, name, **attrs):
    hits = [_val(m) for m in metrics
            if m["name"] == name and all(_attrs(m).get(k) == v for k, v in attrs.items())]
    assert hits, f"{name} {attrs} not emitted"
    return sum(hits)


def _values(out):
    return sorted((k, m["name"], tuple(sorted(_attrs(m).items())), _val(m))
                  for k, ms in out.items() for m in ms)


def test_spec_sanity():
    assert SPEC.id == "gcp_cloudsql_sqlserver_otel"
    assert SPEC.client == "otlp" and SPEC.signals == ("metrics",)
    assert SPEC.datasets == ("gcp.cloudsql_sqlserver.otel",)
    assert SPEC.integrations == ("gcp_cloudsql_sqlserver_otel",)
    assert SPEC.wants_chaos and SPEC.chaos_mode == "explicit" and SPEC.family == "database"
    assert not SPEC.default_enabled and not SPEC.core
    assert all(not f.get("required") for f in SPEC.config_fields.values())
    assert {"blocked_processes_high", "deadlocks_high"} <= set(SPEC.impact_keys)


def test_every_dashboard_metric_is_emitted():
    emitted = {m["name"] for ms in _emit().values() for m in ms}
    assert set(METRIC_NAMES) <= emitted


def test_metric_kinds():
    by = {m["name"]: m for ms in _emit().values() for m in ms}
    for name in (SS + "transactions/batch_request_count", SS + "transactions/deadlock_count",
                 SS + "transactions/lock_wait_time", DB + "auto_failover_request_count"):
        assert by[name]["sum"]["aggregationTemporality"] == 1 and not by[name]["sum"]["isMonotonic"]
    for name in (SS + "memory/page_life_expectancy", SS + "connections/processes_blocked",
                 SS + "memory/buffer_cache_hit_ratio", DB + "available_for_failover"):
        assert "gauge" in by[name]


def test_resource():
    inst = _instances(1)[1]
    res = instance_resource(ScenarioIdentity(NS), inst, dataset="gcp.cloudsql_sqlserver",
                            service_role="cloudsql-sqlserver", cloud=default_cloud())
    attrs = {a["key"]: next(iter(a["value"].values())) for a in res["attributes"]}
    assert attrs["data_stream.dataset"] == "gcp.cloudsql_sqlserver"
    assert attrs["gcp.resource_type"] == "cloudsql_database"
    assert attrs["database_id"] == f"{NS}-project:{NS}-sqlserver-replica"


def test_healthy_baseline():
    out = _emit()
    for role, ms in out.items():
        assert _sum(ms, DB + "up") == 1
        assert _sum(ms, DB + "instance_state", state="RUNNING") == 1
        assert _sum(ms, SS + "memory/page_life_expectancy") > 300  # marker + healthy PLE
        assert _sum(ms, SS + "memory/buffer_cache_hit_ratio") > 99  # percent, 0-100
        assert _sum(ms, SS + "transactions/deadlock_count") == 0
        assert _sum(ms, SS + "connections/processes_blocked") <= 1
    p = out["primary"]
    avg_wait = _sum(p, SS + "transactions/lock_wait_time") / max(1, _sum(p, SS + "transactions/lock_wait_count"))
    assert avg_wait < 100
    assert _sum(p, DB + "available_for_failover") == 1
    assert _sum(p, DB + "auto_failover_request_count") == 0
    assert _sum(out["replica"], DB + "replication/replica_lag") < 1


def test_cpu_memory_disk_high():
    p = _emit({"cpu_utilization_high": 95.0, "memory_utilization_high": 96.0,
               "disk_utilization_high": 93.0})["primary"]
    assert _sum(p, DB + "cpu/utilization") >= 0.9
    assert _sum(p, DB + "memory/utilization") >= 0.9
    assert _sum(p, DB + "disk/utilization") >= 0.9
    assert _sum(p, SS + "memory/page_life_expectancy") < 300
    assert _sum(p, SS + "schedulers/runnable_tasks") >= 8 * 4


def test_connections_high():
    p = _emit({"connections_high": 4000.0})["primary"]
    assert _sum(p, DB + "network/connections") >= 4000


def test_replica_lag_high():
    assert _sum(_emit({"replica_lag_high": 300.0})["replica"], DB + "replication/replica_lag") >= 280


def test_instance_unavailable():
    p = _emit({"instance_unavailable": 1.0})["primary"]
    assert _sum(p, DB + "up") == 0
    assert _sum(p, DB + "instance_state", state="FAILED") == 1
    assert 0 < _sum(p, DB + "uptime") < 55
    assert _sum(p, DB + "available_for_failover") == 0
    assert _sum(p, DB + "auto_failover_request_count") == 1


def test_blocked_processes_high():
    p = _emit({"blocked_processes_high": 40.0})["primary"]
    assert _sum(p, SS + "connections/processes_blocked") >= 36
    avg_wait = _sum(p, SS + "transactions/lock_wait_time") / _sum(p, SS + "transactions/lock_wait_count")
    assert avg_wait > 1000
    assert _sum(p, SS + "memory/memory_grants_pending") > 0


def test_deadlocks_high():
    p = _emit({"deadlocks_high": 20.0})["primary"]
    assert _sum(p, SS + "transactions/deadlock_count") >= 18
    assert _sum(p, SS + "transactions/deadlock_count", locked_resource="Key") >= 1


def test_seeded_emit_is_deterministic():
    assert _values(_emit(None, seed=11)) == _values(_emit(None, seed=11))


@pytest.mark.parametrize("key", sorted(SPEC.impact_keys))
def test_each_key_changes_emit(key):
    hot = {k: v["default"] for k, v in SPEC.impact_keys.items()}
    top = SPEC.impact_keys[key]["range"][1]
    hot[key] = top * 0.9 if top > 1 else 1.0
    assert _values(_emit(hot, seed=11)) != _values(_emit(None, seed=11))
