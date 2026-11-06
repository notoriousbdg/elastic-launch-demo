"""Tests for the otel_collector_internal_telemetry generator.

Alert helpers mirror the OOTB ES|QL: ``SUM(RATE(send_failed_*)) BY service.name,
service.instance.id > 0`` — evaluated here as the counter delta between two
reporting intervals.
"""

from __future__ import annotations

import random
from collections import defaultdict

import pytest

from generators.otel_collector_internal_telemetry import SPEC
from generators.otel_collector_internal_telemetry.generator import (
    METRIC_NAMES,
    CollectorCounterState,
    _generate_metrics,
    build_fleet,
)
from generator_engine.identity import ScenarioIdentity

NS = "gtotelcollector"
DT = 12.0


def _attrs(metric: dict) -> dict:
    body = metric.get("sum") or metric.get("gauge")
    dp = body["dataPoints"][0]
    return {a["key"]: next(iter(a["value"].values())) for a in dp.get("attributes", [])}


def _value(metric: dict) -> float:
    body = metric.get("sum") or metric.get("gauge")
    dp = body["dataPoints"][0]
    return float(dp["asDouble"] if "asDouble" in dp else dp["asInt"])


def _flatten(batches):
    """Yield (instance_id, metric) for every emitted metric."""
    for iid, scopes in batches.items():
        for metrics in scopes.values():
            for m in metrics:
                yield iid, m


def _setup(seed=3):
    rng = random.Random(seed)
    fleet = build_fleet(ScenarioIdentity(NS), rng, 2)
    state = CollectorCounterState(rng, fleet)
    return fleet, state, rng


def _two_ticks(chaos=None, seed=3, load=0.6, warm_chaos=None):
    """Return (first, second) batches; the alert window is the delta between them."""
    fleet, state, rng = _setup(seed)
    first = _generate_metrics(fleet, state, rng, warm_chaos, DT, load=load)
    first_vals = {(iid, m["name"], tuple(sorted(_attrs(m).items()))): _value(m) for iid, m in _flatten(first)}
    second = _generate_metrics(fleet, state, rng, chaos, DT, load=load)
    return first_vals, second, fleet


def _rate_by_instance(first_vals, second, name):
    out: dict[str, float] = defaultdict(float)
    for iid, m in _flatten(second):
        if m["name"] != name:
            continue
        key = (iid, m["name"], tuple(sorted(_attrs(m).items())))
        out[iid] += (_value(m) - first_vals.get(key, 0.0)) / DT
    return dict(out)


FAILED = {
    "exporter_send_failed_log_records": "otelcol_exporter_send_failed_log_records",
    "exporter_send_failed_metric_points": "otelcol_exporter_send_failed_metric_points",
    "exporter_send_failed_spans": "otelcol_exporter_send_failed_spans",
}


def _queue_util(batches):
    cap: dict[str, float] = defaultdict(float)
    size: dict[str, float] = defaultdict(float)
    for iid, m in _flatten(batches):
        if m["name"] == "otelcol_exporter_queue_capacity":
            cap[iid] += _value(m)
        elif m["name"] == "otelcol_exporter_queue_size":
            size[iid] += _value(m)
    return {iid: size[iid] / cap[iid] for iid in cap}


# ── emit shape ──

def test_every_metric_name_is_emitted():
    _, second, _ = _two_ticks()
    names = {m["name"] for _, m in _flatten(second)}
    assert set(METRIC_NAMES) <= names


def test_fleet_is_one_gateway_and_two_agents_with_qualified_identity():
    fleet, _, _ = _setup()
    assert [c.role for c in fleet] == ["gateway", "agent", "agent"]
    assert len({c.instance_id for c in fleet}) == 3
    for c in fleet:
        assert c.service_name.startswith(f"{NS}-")
        assert c.instance_id.startswith(f"{NS}-")
        assert c.host_name.startswith(f"{NS}-")


def test_counter_vs_gauge_types_match_collector_metadata():
    _, second, _ = _two_ticks()
    gauges = {
        "otelcol_exporter_queue_size", "otelcol_exporter_queue_capacity",
        "otelcol_process_memory_rss", "otelcol_process_runtime_heap_alloc_bytes",
        "otelcol_process_runtime_total_sys_memory_bytes",
        "otelcol_processor_tail_sampling_sampling_traces_on_memory",
    }
    for _, m in _flatten(second):
        if m["name"] in gauges:
            assert "gauge" in m, m["name"]
        else:
            assert m["sum"]["isMonotonic"] and m["sum"]["aggregationTemporality"] == 2, m["name"]


def test_dashboard_attributes_are_present():
    _, second, _ = _two_ticks()
    seen: dict[str, set] = defaultdict(set)
    for _, m in _flatten(second):
        for k, v in _attrs(m).items():
            seen[k].add(v)
    assert {"otlp", "hostmetrics", "kubeletstats", "filelog"} <= seen["receiver"]
    assert {"grpc", "http"} <= seen["transport"]
    assert {"traces", "metrics", "logs"} <= seen["otel.signal"]
    assert {"traces", "metrics", "logs"} <= seen["data_type"]
    assert {"otlp/elastic", "otlp/gateway", "otlphttp/archive"} <= seen["exporter"]
    assert {"batch", "memory_limiter", "tail_sampling", "k8sattributes"} <= seen["processor"]
    assert {"cpu", "memory", "filesystem"} <= seen["scraper"]
    assert {"sampled", "not_sampled", "dropped"} <= seen["decision"]
    assert len(seen["policy"]) >= 3


def test_every_instance_reports_all_three_signals_for_overview_failure_rates():
    _, second, fleet = _two_ticks()
    for inst in fleet:
        names = {m["name"] for iid, m in _flatten(second) if iid == inst.instance_id}
        for suffix in ("spans", "metric_points", "log_records"):
            assert f"otelcol_receiver_accepted_{suffix}" in names
            assert f"otelcol_exporter_sent_{suffix}" in names


def test_traffic_counters_start_near_zero_and_grow():
    first_vals, second, _ = _two_ticks()
    for (_, name, _), v in first_vals.items():
        if name.startswith(("otelcol_receiver_accepted", "otelcol_exporter_sent")):
            assert v < 100_000
    rates = _rate_by_instance(first_vals, second, "otelcol_exporter_sent_spans")
    assert all(r > 0 for r in rates.values())


# ── healthy baseline ──

@pytest.mark.parametrize("seed,load", [(1, 0.15), (2, 1.0), (3, 0.6)])
def test_healthy_baseline_has_no_failures_and_low_queues(seed, load):
    first_vals, second, _ = _two_ticks(None, seed=seed, load=load)
    for name in FAILED.values():
        assert all(r == 0.0 for r in _rate_by_instance(first_vals, second, name).values())
        # Series exist (column mapped) even though they never grow.
        assert any(m["name"] == name for _, m in _flatten(second))
    for name in ("otelcol_receiver_refused_spans", "otelcol_receiver_refused_metric_points",
                 "otelcol_receiver_refused_log_records"):
        assert all(r == 0.0 for r in _rate_by_instance(first_vals, second, name).values())
    assert all(u < 0.1 for u in _queue_util(second).values())


def test_spec_defaults_alone_stay_healthy():
    defaults = {k: meta["default"] for k, meta in SPEC.impact_keys.items()}
    first_vals, second, _ = _two_ticks(defaults)
    for name in FAILED.values():
        assert all(r == 0.0 for r in _rate_by_instance(first_vals, second, name).values())
    assert all(u < 0.1 for u in _queue_util(second).values())


# ── one breach per alert key ──

@pytest.mark.parametrize("key", sorted(FAILED))
def test_send_failed_key_breaches_only_its_alert_on_the_gateway(key):
    first_vals, second, fleet = _two_ticks({key: 50.0})
    gateway = fleet[0].instance_id
    rates = _rate_by_instance(first_vals, second, FAILED[key])
    assert rates[gateway] > 0
    assert all(r == 0.0 for iid, r in rates.items() if iid != gateway)
    for other_key, other in FAILED.items():
        if other_key != key:
            assert all(r == 0.0 for r in _rate_by_instance(first_vals, second, other).values())


def test_send_failed_small_probe_still_breaches():
    first_vals, second, fleet = _two_ticks({"exporter_send_failed_spans": 0.01})
    rates = _rate_by_instance(first_vals, second, "otelcol_exporter_send_failed_spans")
    assert rates[fleet[0].instance_id] >= 1.0


def test_queue_saturation_fills_gateway_queue_panel():
    first_vals, second, fleet = _two_ticks({"queue_saturation": 95.0})
    util = _queue_util(second)
    gateway = fleet[0].instance_id
    assert util[gateway] > 0.6
    assert all(u < 0.1 for iid, u in util.items() if iid != gateway)
    refused = _rate_by_instance(first_vals, second, "otelcol_receiver_refused_spans")
    assert refused[gateway] > 0
    # Saturation alone must not fire the send-failed alerts.
    for name in FAILED.values():
        assert all(r == 0.0 for r in _rate_by_instance(first_vals, second, name).values())


def test_failure_stops_growing_after_resolve():
    fleet, state, rng = _setup()
    _generate_metrics(fleet, state, rng, {"exporter_send_failed_spans": 50.0}, DT, load=0.6)
    a = _generate_metrics(fleet, state, rng, None, DT, load=0.6)
    b = _generate_metrics(fleet, state, rng, None, DT, load=0.6)
    va = {(i, m["name"], tuple(sorted(_attrs(m).items()))): _value(m) for i, m in _flatten(a)}
    assert all(r == 0.0 for r in _rate_by_instance(va, b, "otelcol_exporter_send_failed_spans").values())


# --- 1:1 alert impact_keys coverage ---

def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be 1:1 with package alert rules, plus the queue panel knob."""
    expected = [
        "exporter_send_failed_log_records",
        "exporter_send_failed_metric_points",
        "exporter_send_failed_spans",
        "queue_saturation",
    ]
    assert sorted(SPEC.impact_keys) == expected
    alert_keys = [k for k, meta in SPEC.impact_keys.items() if meta["doc"].startswith("Drive alert")]
    assert sorted(alert_keys) == expected[:3]
    assert SPEC.wants_chaos is True
