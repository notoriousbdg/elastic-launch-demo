"""Tests for the gcp_cloudsql_postgresql_otel generator."""

import random

import pytest

from generator_engine.gcp_cloudsql_common import (
    build_instances,
    default_cloud,
    insights_resource,
    instance_resource,
)
from generator_engine.identity import ScenarioIdentity
from generators.gcp_cloudsql_postgresql_otel import SPEC
from generators.gcp_cloudsql_postgresql_otel.generator import (
    METRIC_NAMES,
    InsightsCounters,
    generate_metrics,
)

NS = "gtgcpsqlpostgresql"
PG = "cloudsql.googleapis.com/database/postgresql/"
DB = "cloudsql.googleapis.com/database/"
DATABASES = ScenarioIdentity(NS).databases(["orders", "inventory", "billing"])
USERS = ["app_rw", "reporting_ro", "etl_batch"]


def _instances(seed):
    return build_instances(
        ScenarioIdentity(NS), {}, engine_role="postgresql", default_tier="db-custom-4-16384",
        disk_gb=500, project_id=ScenarioIdentity(NS).gcp_project_id, cloud=default_cloud(),
        rng=random.Random(seed),
    )


def _emit(chaos=None, seed=3, ticks=1):
    rng = random.Random(seed)
    insts = _instances(seed)
    counters = InsightsCounters(rng)
    batches = None
    for _ in range(ticks):
        batches = generate_metrics(insts, DATABASES, USERS, counters, rng, chaos, 1000, load=0.8)
    out: dict[str, list] = {"primary": [], "replica": [], "insights": []}
    for inst, db, metrics in batches:
        out["insights" if db else inst.role] += metrics
    return out


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
    assert SPEC.id == "gcp_cloudsql_postgresql_otel"
    assert SPEC.client == "otlp" and SPEC.signals == ("metrics",)
    assert SPEC.datasets == ("gcp.cloudsql_postgresql.otel",)
    assert SPEC.integrations == ("gcp_cloudsql_postgresql_otel",)
    assert SPEC.wants_chaos and SPEC.chaos_mode == "explicit" and SPEC.family == "database"
    assert not SPEC.default_enabled and not SPEC.core
    assert all(not f.get("required") for f in SPEC.config_fields.values())
    assert {"deadlocks_high", "transaction_id_utilization_high"} <= set(SPEC.impact_keys)


def test_every_dashboard_metric_is_emitted():
    emitted = {m["name"] for ms in _emit().values() for m in ms}
    assert set(METRIC_NAMES) <= emitted


def test_metric_kinds():
    out = _emit()
    by = {m["name"]: m for ms in out.values() for m in ms}
    for name in (PG + "transaction_count", PG + "deadlock_count", PG + "blocks_read_count", DB + "uptime"):
        assert by[name]["sum"]["aggregationTemporality"] == 1 and not by[name]["sum"]["isMonotonic"]
    for name in (PG + "num_backends", PG + "transaction_id_utilization", DB + "instance_state"):
        assert "gauge" in by[name]
    # Query Insights are CUMULATIVE counters read with RATE().
    insight = by[PG + "insights/aggregate/execution_time"]["sum"]
    assert insight["aggregationTemporality"] == 2 and insight["isMonotonic"] is True


def test_insights_counters_grow_from_near_zero():
    first = _emit(ticks=1)["insights"]
    later = _emit(ticks=3)["insights"]
    name = PG + "insights/aggregate/execution_time"
    assert _sum(later, name) > _sum(first, name) > 0
    assert _sum(first, name) < 2_000_000_000  # starts near zero, no RATE spike


def test_resources():
    ident = ScenarioIdentity(NS)
    inst = _instances(1)[0]
    res = instance_resource(ident, inst, dataset="gcp.cloudsql_postgresql",
                            service_role="cloudsql-postgresql", cloud=default_cloud())
    attrs = {a["key"]: next(iter(a["value"].values())) for a in res["attributes"]}
    assert attrs["gcp.resource_type"] == "cloudsql_database"
    assert attrs["database_id"] == f"{NS}-project:{NS}-postgresql-primary"
    ins = insights_resource(ident, inst, DATABASES[0], dataset="gcp.cloudsql_postgresql",
                            service_role="cloudsql-postgresql", cloud=default_cloud())
    iattrs = {a["key"]: next(iter(a["value"].values())) for a in ins["attributes"]}
    assert iattrs["gcp.resource_type"] == "cloudsql_instance_database"
    assert iattrs["resource_id"] == attrs["database_id"]  # drilldown filter
    assert iattrs["database"] == DATABASES[0]


def test_healthy_baseline():
    out = _emit()
    for role in ("primary", "replica"):
        ms = out[role]
        assert _sum(ms, DB + "up") == 1
        assert _sum(ms, DB + "instance_state", state="RUNNING") == 1
        assert _sum(ms, DB + "uptime") == 60
        assert _sum(ms, PG + "num_backends") > 0  # overview marker
        assert _sum(ms, PG + "deadlock_count") == 0
        assert _sum(ms, PG + "transaction_id_utilization") < 0.2
        assert _sum(ms, DB + "cpu/utilization") < 0.6
    p = out["primary"]
    assert _sum(p, PG + "num_backends") <= 1000  # partitions one instance budget
    assert _sum(p, PG + "num_backends") == _sum(p, PG + "num_backends_by_state")
    rb = _sum(p, PG + "transaction_count", transaction_type="rollback")
    assert rb / _sum(p, PG + "transaction_count") < 0.02
    assert _sum(out["replica"], DB + "replication/replica_lag") < 1
    assert _sum(p, PG + "replication/replica_byte_lag", replica_lag_type="replay_location") < 1_000_000


def test_cpu_memory_disk_high():
    p = _emit({"cpu_utilization_high": 95.0, "memory_utilization_high": 96.0,
               "disk_utilization_high": 93.0})["primary"]
    assert _sum(p, DB + "cpu/utilization") >= 0.9
    assert _sum(p, DB + "memory/utilization") >= 0.9
    assert _sum(p, DB + "disk/utilization") >= 0.9


def test_connections_high():
    p = _emit({"connections_high": 950.0})["primary"]
    assert _sum(p, PG + "num_backends") >= 900
    assert _sum(p, PG + "num_backends") <= 1000


def test_replica_lag_high():
    out = _emit({"replica_lag_high": 300.0})
    assert _sum(out["replica"], DB + "replication/replica_lag") >= 280
    assert _sum(out["primary"], PG + "replication/replica_byte_lag",
                replica_lag_type="replay_location") > 100 * 1024 * 1024


def test_instance_unavailable():
    p = _emit({"instance_unavailable": 1.0})["primary"]
    assert _sum(p, DB + "up") == 0
    assert _sum(p, DB + "instance_state", state="FAILED") == 1
    assert _sum(p, DB + "instance_state", state="RUNNING") == 0
    assert 0 < _sum(p, DB + "uptime") < 55
    assert _sum(p, DB + "network/connection_attempt_count", login_status="failed") >= 20


def test_deadlocks_high():
    out = _emit({"deadlocks_high": 25.0})
    p = out["primary"]
    assert _sum(p, PG + "deadlock_count") >= 20
    assert _sum(p, PG + "backends_in_wait", wait_event_type="Lock") >= 10
    rb = _sum(p, PG + "transaction_count", transaction_type="rollback")
    assert rb / _sum(p, PG + "transaction_count") > 0.05
    lock = _sum(out["insights"], PG + "insights/aggregate/lock_time")
    exe = _sum(out["insights"], PG + "insights/aggregate/execution_time")
    assert lock / exe > 0.2


def test_transaction_id_utilization_high():
    p = _emit({"transaction_id_utilization_high": 85.0})["primary"]
    assert _sum(p, PG + "transaction_id_utilization") >= 0.84
    assert _sum(p, PG + "vacuum/oldest_transaction_age", oldest_transaction_type="running") > 1e9


def test_seeded_emit_is_deterministic():
    assert _values(_emit(None, seed=11)) == _values(_emit(None, seed=11))


@pytest.mark.parametrize("key", sorted(SPEC.impact_keys))
def test_each_key_changes_emit(key):
    hot = {k: v["default"] for k, v in SPEC.impact_keys.items()}
    top = SPEC.impact_keys[key]["range"][1]
    hot[key] = top * 0.9 if top > 1 else 1.0
    assert _values(_emit(hot, seed=11)) != _values(_emit(None, seed=11))
