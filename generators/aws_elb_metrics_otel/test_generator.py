"""Technology-specific tests for the ``aws_elb_metrics_otel`` generator.

Standard emit/registry tests live in tests/test_generators_emit.py.
"""
from __future__ import annotations

import random
import threading

import pytest

from generators.aws_elb_metrics_otel.generator import METRIC_NAMES, _generate_metrics


def _attr_map(metric: dict) -> dict:
    dps = metric.get("gauge", {}).get("dataPoints") or metric.get("sum", {}).get("dataPoints") or []
    if not dps:
        return {}
    return {a["key"]: next(iter(a["value"].values())) for a in dps[0].get("attributes", [])}


def _lb_metrics(metrics: list[dict]) -> list[dict]:
    return [m for m in metrics if "TargetGroup" not in _attr_map(m)]


def test_all_metrics_are_gauges_not_counters():
    """Dashboard SUM/sum_over_time rejects counter_long; CloudWatch stats are gauges."""
    metrics = _generate_metrics(
        "app/demo/abc",
        ["targetgroup/api/xyz"],
        "us-east-1a",
        "us-east-1",
        "123456789012",
        4,
        random.Random(1),
    )
    assert metrics
    for m in metrics:
        assert "gauge" in m, f"{m['name']} must be a gauge, got {list(m.keys())}"
        assert "sum" not in m


def test_lb_level_sum_metrics_omit_tg_and_az():
    metrics = _generate_metrics(
        "app/demo/abc",
        ["targetgroup/api/xyz"],
        "us-east-1a",
        "us-east-1",
        "123456789012",
        4,
        random.Random(1),
    )
    # Key by name among the Sum series: a metric can also have Average/Maximum rows.
    by_name = {m["name"]: m for m in _lb_metrics(metrics) if _attr_map(m).get("stat") == "Sum"}
    for name in (
        "amazonaws.com/AWS/ApplicationELB/RequestCount",
        "amazonaws.com/AWS/ApplicationELB/ProcessedBytes",
        "amazonaws.com/AWS/ApplicationELB/RejectedConnectionCount",
        "amazonaws.com/AWS/ApplicationELB/HTTPCode_Target_2XX_Count",
        "amazonaws.com/AWS/ApplicationELB/HTTPCode_Target_4XX_Count",
        "amazonaws.com/AWS/ApplicationELB/HTTPCode_Target_5XX_Count",
        "amazonaws.com/AWS/ApplicationELB/ActiveConnectionCount",
    ):
        m = by_name[name]
        attrs = _attr_map(m)
        assert attrs.get("stat") == "Sum"
        assert "TargetGroup" not in attrs
        assert "AvailabilityZone" not in attrs


def test_lb_level_health_and_latency_stats():
    """Overview panels filter TargetGroup IS NULL and require Maximum/Average."""
    metrics = _generate_metrics(
        "app/demo/abc",
        ["targetgroup/api/xyz", "targetgroup/web/def"],
        "us-east-1a",
        "us-east-1",
        "123456789012",
        4,
        random.Random(2),
    )
    lb = _lb_metrics(metrics)
    stats: dict[str, set[str]] = {}
    for m in lb:
        stats.setdefault(m["name"], set()).add(_attr_map(m).get("stat"))

    assert "Maximum" in stats["amazonaws.com/AWS/ApplicationELB/HealthyHostCount"]
    assert "Maximum" in stats["amazonaws.com/AWS/ApplicationELB/UnHealthyHostCount"]
    assert "Average" in stats["amazonaws.com/AWS/ApplicationELB/TargetResponseTime"]


# ── helpers shared by the alert and dashboard tests ──────────────────────────

LB = "app/demo/abc"
TGS = ["targetgroup/api/xyz", "targetgroup/web/def"]


def _rows(metrics: list[dict]) -> list[dict]:
    """Flatten emitted gauges to {metric, stat, tg, az, lb, ns, value, name}."""
    rows = []
    for m in metrics:
        dp = m["gauge"]["dataPoints"][0]
        attrs = {a["key"]: next(iter(a["value"].values())) for a in dp.get("attributes", [])}
        rows.append({
            "name": m["name"], "metric": attrs.get("MetricName"), "stat": attrs.get("stat"),
            "tg": attrs.get("TargetGroup"), "az": attrs.get("AvailabilityZone"),
            "lb": attrs.get("LoadBalancer"), "ns": attrs.get("Namespace"),
            "value": float(dp["asDouble"] if "asDouble" in dp else dp["asInt"]),
        })
    return rows


def _alb_cycle(chaos: dict | None, seed: int) -> list[dict]:
    return _rows(_generate_metrics(
        LB, TGS, "us-east-1a", "us-east-1", "123456789012", 4, random.Random(seed), chaos,
    ))


def _sum(rows, metric, tg="ANY"):
    return int(sum(
        r["value"] for r in rows
        if r["metric"] == metric and r["stat"] == "Sum" and (tg == "ANY" or r["tg"] == tg)
    ))


# Mirrors the six aws_elb_metrics_otel@0.12.1 alert templates. SUM() of long gauges is a
# long, so the ratio alerts integer-divide; the 5XX rate alert does not filter TargetGroup,
# so its denominator includes load-balancer and target-group RequestCount rows.

def _elb_5xx(rows):
    requests = _sum(rows, "RequestCount")
    return requests > 100 and (_sum(rows, "HTTPCode_ELB_5XX_Count") // requests) * 100.0 > 0.5


def _target_5xx(rows):
    for tg in {r["tg"] for r in rows}:  # None is the load-balancer-level group
        requests = _sum(rows, "RequestCount", tg)
        if requests > 100 and (_sum(rows, "HTTPCode_Target_5XX_Count", tg) // requests) * 100.0 > 1.0:
            return True
    return False


def _rt(rows, stat, threshold):
    return any(
        r["metric"] == "TargetResponseTime" and r["stat"] == stat and r["value"] > threshold
        for r in rows
    )


ALERTS = {
    "high_application_load_balancer_5xx_error_rate": _elb_5xx,
    "application_load_balancer_rejected_connections_detected":
        lambda rows: _sum(rows, "RejectedConnectionCount") > 0,
    "high_application_load_balancer_target_5xx_error_rate": _target_5xx,
    "high_application_load_balancer_target_response_time_average":
        lambda rows: _rt(rows, "Average", 2.0),
    "high_application_load_balancer_target_response_time_tail":
        lambda rows: _rt(rows, "Maximum", 5.0),
    "application_load_balancer_unhealthy_targets_detected": lambda rows: any(
        r["metric"] == "UnHealthyHostCount" and r["stat"] == "Average" and r["value"] > 0
        for r in rows
    ),
}

GT_VALUES = {  # scenarios_testing/gt-awselbmetrics/channels
    "high_application_load_balancer_5xx_error_rate": 750.0,
    "application_load_balancer_rejected_connections_detected": 50.0,
    "high_application_load_balancer_target_5xx_error_rate": 750.0,
    "high_application_load_balancer_target_response_time_average": 75.0,
    "high_application_load_balancer_target_response_time_tail": 75.0,
    "application_load_balancer_unhealthy_targets_detected": 50.0,
}


def _breached(rows) -> set[str]:
    return {key for key, fires in ALERTS.items() if fires(rows)}


def test_alert_table_covers_every_impact_key():
    assert set(ALERTS) == set(_SPEC_FOR_ALERT_KEYS.impact_keys) == set(GT_VALUES)


def test_healthy_cycle_breaches_no_alert():
    for seed in range(50):
        assert _breached(_alb_cycle(None, seed)) == set(), seed


@pytest.mark.parametrize("key", sorted(GT_VALUES))
def test_fault_alone_breaches_only_its_own_alert(key):
    for seed in range(30):
        assert _breached(_alb_cycle({key: GT_VALUES[key]}, seed)) == {key}, (key, seed)


# ── dashboard requirements (aws_elb_metrics_otel@0.12.1) ──────────────────────

def _lb_level(rows, metric, stat):
    return [r for r in rows if r["metric"] == metric and r["stat"] == stat
            and r["tg"] is None and r["az"] is None]


@pytest.mark.parametrize("metric,stat", [
    ("ActiveConnectionCount", "Sum"),      # ALB detail panel
    ("ActiveConnectionCount", "Average"),  # overview "Active connections over time"
    ("NewConnectionCount", "Sum"),
    ("HealthyHostCount", "Maximum"),
    ("HealthyHostCount", "Average"),       # ALB detail "Healthy hosts" (AvailabilityZone IS NULL)
    ("UnHealthyHostCount", "Maximum"),
    ("UnHealthyHostCount", "Average"),
    ("TargetResponseTime", "Average"),
    ("TargetResponseTime", "Maximum"),
])
def test_alb_load_balancer_level_series_exist(metric, stat):
    assert _lb_level(_alb_cycle(None, 1), metric, stat), f"missing LB-level {metric}/{stat}"


def test_new_connection_count_is_a_declared_metric():
    assert "amazonaws.com/AWS/ApplicationELB/NewConnectionCount" in METRIC_NAMES


NLB_REQUIRED = {
    "ActiveFlowCount": {"Average"}, "ConsumedLCUs": {"Average"},
    "HealthyHostCount": {"Average", "Maximum", "Sum"},
    "UnHealthyHostCount": {"Average", "Maximum", "Sum"},
    "NewFlowCount": {"Maximum", "Sum"}, "ProcessedBytes": {"Maximum", "Sum"},
    "TCP_Client_Reset_Count": {"Maximum", "Sum"}, "TCP_ELB_Reset_Count": {"Maximum", "Sum"},
    "TCP_Target_Reset_Count": {"Sum"},
}
GWLB_REQUIRED = {
    "ActiveFlowCount": {"Average", "Maximum", "Sum"}, "BytesProcessed": {"Maximum", "Sum"},
    "ConsumedLCUs": {"Average"},
    "HealthyHostCount": {"Average", "Maximum", "Sum"},
    "UnHealthyHostCount": {"Average", "Maximum", "Sum"},
    "NewFlowCount": {"Maximum", "Sum"}, "ProcessedBytes": {"Sum"},
}


def _stats_by_metric(rows):
    out: dict[str, set[str]] = {}
    for r in rows:
        out.setdefault(r["metric"], set()).add(r["stat"])
    return out


@pytest.mark.parametrize("builder,required,namespace,prefix", [
    ("_generate_network_metrics", NLB_REQUIRED, "AWS/NetworkELB", "net/"),
    ("_generate_gateway_metrics", GWLB_REQUIRED, "AWS/GatewayELB", "gateway/"),
])
def test_nlb_and_gwlb_emit_the_series_their_dashboards_read(builder, required, namespace, prefix):
    from generators.aws_elb_metrics_otel import generator

    build = getattr(generator, builder)
    rows = _rows(build(f"{prefix}demo/abc", "us-east-1", "123456789012", 4, random.Random(1)))
    seen = _stats_by_metric(rows)
    for metric, stats in required.items():
        assert stats <= seen.get(metric, set()), (metric, stats - seen.get(metric, set()))
    for r in rows:
        assert r["ns"] == namespace and r["lb"].startswith(prefix)
        assert r["tg"] is None and r["az"] is None, "dashboards filter TargetGroup/AZ IS NULL"
        assert r["name"] == f"amazonaws.com/{namespace}/{r['metric']}"


def test_run_sends_one_resource_per_elb_type_dataset():
    from unittest.mock import MagicMock

    from generators.aws_elb_metrics_otel import generator

    class _OneCycle(threading.Event):
        def wait(self, timeout=None):
            self.set()
            return True

    client = MagicMock()
    generator.run(client, _OneCycle(), scenario_data={
        "namespace": "demo", "generator_config": {}, "channel_registry": {},
    })
    datasets = set()
    for call in client.send_metrics_scoped.call_args_list:
        resource = call.args[0]
        attrs = {a["key"]: next(iter(a["value"].values())) for a in resource["attributes"]}
        datasets.add(attrs["data_stream.dataset"])
    # The resource carries the pre-routing dataset; the OTLP endpoint appends ".otel".
    assert datasets == {"aws.elb", "aws.elb_network", "aws.elb_gateway"}
    assert {d.removesuffix(".otel") for d in _SPEC_FOR_ALERT_KEYS.datasets} == datasets

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.aws_elb_metrics_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['application_load_balancer_rejected_connections_detected', 'application_load_balancer_unhealthy_targets_detected', 'high_application_load_balancer_5xx_error_rate', 'high_application_load_balancer_target_5xx_error_rate', 'high_application_load_balancer_target_response_time_average', 'high_application_load_balancer_target_response_time_tail']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


# --- ALB 5xx investigation workflow shape (Template Library aws-alb-5xx-investigation-otel) ---

def _lb_sum(rows, metric):
    return int(sum(r["value"] for r in rows if r["metric"] == metric and r["stat"] == "Sum"
                   and r["tg"] is None and r["az"] is None))


@pytest.mark.parametrize("chaos,dominant", [
    ({"high_application_load_balancer_5xx_error_rate": 300.0}, "502"),
    ({"high_application_load_balancer_5xx_error_rate": 300.0,
      "application_load_balancer_unhealthy_targets_detected": 4.0}, "503"),
])
def test_elb_5xx_splits_into_per_code_counts(chaos, dominant):
    rows = _alb_cycle(chaos, 3)
    total = _lb_sum(rows, "HTTPCode_ELB_5XX_Count")
    codes = {c: _lb_sum(rows, f"HTTPCode_ELB_{c}_Count") for c in ("500", "502", "503", "504")}
    assert total > 0 and sum(codes.values()) == total
    assert max(codes, key=codes.get) == dominant


def test_healthy_cycle_has_zero_sparse_error_counts():
    rows = _alb_cycle(None, 1)
    for metric in ("HTTPCode_ELB_503_Count", "TargetConnectionErrorCount", "TargetTLSNegotiationErrorCount"):
        assert _lb_sum(rows, metric) == 0


def test_target_health_agrees_at_every_dimension_level():
    rows = _alb_cycle({"application_load_balancer_unhealthy_targets_detected": 4.0}, 2)
    for metric, expected in (("HealthyHostCount", 0), ("UnHealthyHostCount", 4)):
        levels = {(r["tg"] is not None, r["az"] is not None) for r in rows if r["metric"] == metric}
        assert levels == {(False, False), (True, False), (True, True)}
        assert {r["value"] for r in rows if r["metric"] == metric and r["stat"] == "Average"} == {expected}
