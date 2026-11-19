"""Tests for the temporal_otel generator.

Each helper evaluates one package alert / SLO the way its ES|QL does, over a
single scrape (``AVG_OVER_TIME`` of one sample is the sample itself).
"""

from __future__ import annotations

import random
from collections import defaultdict

import pytest

from generators.temporal_otel import SPEC
from generators.temporal_otel.generator import (
    METRIC_NAMES,
    TemporalMetricState,
    _generate_metrics,
    build_topology,
    temporal_account_id,
)
from generator_engine.identity import ScenarioIdentity

NS = "gttemporal"


def _attrs(metric: dict) -> dict:
    dp = metric["gauge"]["dataPoints"][0]
    return {a["key"]: next(iter(a["value"].values())) for a in dp.get("attributes", [])}


def _value(metric: dict) -> float:
    dp = metric["gauge"]["dataPoints"][0]
    return float(dp["asDouble"] if "asDouble" in dp else dp["asInt"])


def _scrape(chaos=None, seed=7, load=0.6):
    rng = random.Random(seed)
    ident = ScenarioIdentity(NS)
    account = temporal_account_id(ident)
    topo = build_topology(ident, account)
    state = TemporalMetricState(rng, topo)
    return _generate_metrics(state, topo, account, rng, chaos, load=load), topo


def _sum_by(metrics, name, keys, where=None):
    out: dict[tuple, float] = defaultdict(float)
    for m in metrics:
        if m["name"] != f"temporal_cloud_v1_{name}":
            continue
        a = _attrs(m)
        if where and any(a.get(k) != v for k, v in where.items()):
            continue
        out[tuple(a.get(k) for k in keys)] += _value(m)
    return dict(out)


def _max_by(metrics, name, keys):
    out: dict[tuple, float] = {}
    for m in metrics:
        if m["name"] != f"temporal_cloud_v1_{name}":
            continue
        a = _attrs(m)
        key = tuple(a.get(k) for k in keys)
        out[key] = max(out.get(key, float("-inf")), _value(m))
    return out


# ── alert evaluators (mirror the OOTB ES|QL) ──

def action_utilization(metrics):
    actions = _sum_by(metrics, "total_action_count", ["temporal_namespace"],
                      where={"is_background": "false"})
    limits = _max_by(metrics, "action_limit", ["temporal_namespace"])
    return {k: actions[k] / limits[k] * 100.0 for k in limits if limits[k] > 0}


def activity_timeout(metrics):
    return _sum_by(metrics, "activity_task_timeout_count",
                   ["temporal_namespace", "temporal_task_queue", "timeout_type"])


def activity_fail_pct(metrics):
    keys = ["temporal_namespace", "temporal_task_queue"]
    fail = _sum_by(metrics, "activity_task_fail_count", keys)
    ok = _sum_by(metrics, "activity_success_count", keys)
    out = {}
    for k in ok:
        total = fail.get(k, 0.0) + ok[k]
        if total > 0.1:
            out[k] = fail.get(k, 0.0) / total * 100.0
    return out


def no_poller(metrics):
    return _sum_by(metrics, "no_poller_tasks_count",
                   ["temporal_namespace", "temporal_task_queue", "task_type"])


def open_workflows(metrics):
    return _max_by(metrics, "namespace_open_workflows", ["temporal_namespace"])


def backlog(metrics):
    return _max_by(metrics, "approximate_backlog_count",
                   ["temporal_namespace", "temporal_task_queue", "task_type"])


def workflow_timeout(metrics):
    return _sum_by(metrics, "workflow_timeout_count",
                   ["temporal_namespace", "temporal_task_queue"])


def workflow_success_sli(metrics):
    """SLO: avg(success) / (avg(success)+avg(failed)+avg(timeout)) per namespace."""
    out = {}
    keys = ["temporal_namespace"]
    names = ("workflow_success_count", "workflow_failed_count", "workflow_timeout_count")
    sums = {n: _sum_by(metrics, n, keys) for n in names}
    for ns, s in sums["workflow_success_count"].items():
        f = sums["workflow_failed_count"].get(ns, 0.0)
        t = sums["workflow_timeout_count"].get(ns, 0.0)
        out[ns] = s / (s + f + t)
    return out


def latency_p99(metrics):
    return _max_by(metrics, "service_latency_p99", ["temporal_namespace", "operation"])


# ── emit shape ──

def test_every_metric_name_is_emitted_with_required_attributes():
    metrics, topo = _scrape()
    emitted = {m["name"] for m in metrics}
    assert set(METRIC_NAMES) <= emitted
    for m in metrics:
        a = _attrs(m)
        assert a["temporal_namespace"] in {t["name"] for t in topo}
        assert a["temporal_account"]
        assert a["region"].startswith("aws-")
        assert "gauge" in m  # Temporal Cloud v1 publishes rates/values, never counters
        assert "asDouble" in m["gauge"]["dataPoints"][0]


def test_entities_are_realistic_and_namespace_qualified():
    metrics, topo = _scrape()
    assert len(topo) == 2
    for t in topo:
        assert t["name"].startswith(f"{NS}-")
        assert len(t["task_queues"]) == 3
    queues = {_attrs(m).get("temporal_task_queue") for m in metrics} - {None}
    wtypes = {_attrs(m).get("temporal_workflow_type") for m in metrics} - {None}
    assert len(queues) == 6
    assert len(wtypes) >= 6
    assert {_attrs(m).get("task_type") for m in metrics} >= {"Workflow", "Activity"}
    assert {_attrs(m).get("is_background") for m in metrics} >= {"true", "false"}
    assert len({_attrs(m).get("operation") for m in metrics} - {None}) >= 6
    assert {_attrs(m).get("action_type") for m in metrics} - {None}


# ── healthy baseline ──

@pytest.mark.parametrize("seed,load", [(1, 0.15), (2, 0.6), (3, 1.0), (4, 1.0), (5, 0.3)])
def test_healthy_baseline_stays_under_every_alert_and_meets_slos(seed, load):
    metrics, _ = _scrape(None, seed=seed, load=load)
    assert all(v <= 80.0 for v in action_utilization(metrics).values())
    assert all(v <= 0.05 for v in activity_timeout(metrics).values())
    assert all(v <= 10.0 for v in activity_fail_pct(metrics).values())
    assert all(v == 0.0 for v in no_poller(metrics).values())
    assert all(v <= 10_000 for v in open_workflows(metrics).values())
    assert all(v <= 500 for v in backlog(metrics).values())
    assert all(v <= 0.01 for v in workflow_timeout(metrics).values())
    # SLO targets
    assert all(v < 1000 for v in backlog(metrics).values())
    assert all(v > 0.95 for v in workflow_success_sli(metrics).values())
    assert all(v < 0.1 for v in latency_p99(metrics).values())


def test_healthy_state_drift_stays_bounded_over_many_ticks():
    rng = random.Random(11)
    ident = ScenarioIdentity(NS)
    account = temporal_account_id(ident)
    topo = build_topology(ident, account)
    state = TemporalMetricState(rng, topo)
    for _ in range(500):
        metrics = _generate_metrics(state, topo, account, rng, None, load=1.0)
    assert all(v <= 10_000 for v in open_workflows(metrics).values())
    assert all(v <= 500 for v in backlog(metrics).values())


# ── one breach per alert key ──

def test_action_rate_limit_breach():
    metrics, _ = _scrape({"namespace_approaching_action_rate_limit": 95.0})
    util = action_utilization(metrics)
    assert max(util.values()) > 80.0
    assert sum(v > 80.0 for v in util.values()) == 1  # one hot namespace
    assert max(latency_p99(metrics).values()) > 0.1  # throttling burns the latency SLO


def test_action_rate_limit_low_probe_is_lifted_over_threshold():
    metrics, _ = _scrape({"namespace_approaching_action_rate_limit": 5.0})
    assert max(action_utilization(metrics).values()) > 80.0


def test_activity_task_timeout_breach():
    metrics, _ = _scrape({"activity_task_timeout_rate_elevated": 0.2})
    rates = activity_timeout(metrics)
    hot = [k for k, v in rates.items() if v > 0.05]
    assert len(hot) == 1 and hot[0][2] == "StartToClose"


def test_high_activity_failure_rate_breach():
    metrics, _ = _scrape({"high_activity_task_failure_rate": 40.0})
    assert max(activity_fail_pct(metrics).values()) > 10.0


def test_no_pollers_breach():
    metrics, _ = _scrape({"no_pollers_on_task_queue": 2.0})
    hot = [k for k, v in no_poller(metrics).items() if v > 0]
    assert len(hot) == 1 and hot[0][2] == "Activity"


def test_open_workflow_count_anomaly_breach():
    metrics, _ = _scrape({"namespace_open_workflow_count_anomaly": 40_000.0})
    assert max(open_workflows(metrics).values()) > 10_000


def test_task_queue_backlog_breach_also_burns_backlog_slo():
    metrics, _ = _scrape({"task_queue_backlog_growing": 600.0})
    assert max(backlog(metrics).values()) > 1000


def test_workflow_timeout_breach_burns_success_slo():
    metrics, _ = _scrape({"workflow_timeout_rate_elevated": 0.5})
    assert max(workflow_timeout(metrics).values()) > 0.01
    assert min(workflow_success_sli(metrics).values()) < 0.95


def test_spec_defaults_alone_stay_healthy():
    """resolve_active_chaos fills every key with its SPEC default on sibling channels."""
    defaults = {k: meta["default"] for k, meta in SPEC.impact_keys.items()}
    metrics, _ = _scrape(defaults)
    assert all(v <= 80.0 for v in action_utilization(metrics).values())
    assert all(v == 0.0 for v in no_poller(metrics).values())
    assert all(v <= 0.01 for v in workflow_timeout(metrics).values())


# --- 1:1 alert impact_keys coverage ---

def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = [
        "activity_task_timeout_rate_elevated",
        "high_activity_task_failure_rate",
        "namespace_approaching_action_rate_limit",
        "namespace_open_workflow_count_anomaly",
        "no_pollers_on_task_queue",
        "task_queue_backlog_growing",
        "workflow_timeout_rate_elevated",
    ]
    assert sorted(SPEC.impact_keys) == expected
    assert SPEC.wants_chaos is True
