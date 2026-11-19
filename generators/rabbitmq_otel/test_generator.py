"""RabbitMQ-specific tests for the rabbitmq_otel generator."""

from __future__ import annotations

import random
from unittest.mock import patch

from generator_engine.db_common import build_signal_resource, otel_dataset
from generators.rabbitmq_otel.generator import (
    NODE_FD_TOTAL,
    NODE_MEMORY_LIMIT,
    NODE_PROC_TOTAL,
    NODE_SOCKET_TOTAL,
    RabbitMQNodeState,
    build_node_utilization_doc,
    _NODE_GAUGE_FIELDS,
    _node_gauges_need_mapping_reset,
)


def test_node_utilization_doc_co_locates_used_and_total_pairs():
    """Utilization panels AND-filter used+limit/total; both must share a doc."""
    import random

    from generators.rabbitmq_otel.generator import _NODE_GAUGE_DYNAMIC_TEMPLATES

    resource = build_signal_resource(
        ns="test",
        signal_type="metrics",
        dataset=otel_dataset("rabbitmqreceiver"),
        service_name="rabbitmq",
        service_version="3.12.0",
        service_instance_id="rabbitmq-001",
        host_name="test-rabbitmq",
        cloud={"provider": "gcp", "platform": "gcp_compute_engine",
               "region": "us-central1", "availability_zone": "us-central1-b"},
        extra_attrs={"rabbitmq.node.name": "rabbit@test-rabbitmq"},
    )
    state = RabbitMQNodeState(random.Random(0))
    state.tick(random.Random(0))
    doc = build_node_utilization_doc(resource, state, timestamp_ms=1_700_000_000_000)

    assert doc["data_stream"]["dataset"] == "rabbitmqreceiver.otel"
    assert doc["resource"]["attributes"]["rabbitmq.node.name"] == "rabbit@test-rabbitmq"
    metrics = doc["metrics"]
    assert metrics["rabbitmq.node.mem_used"] == state.mem_used
    assert metrics["rabbitmq.node.mem_limit"] == NODE_MEMORY_LIMIT
    assert metrics["rabbitmq.node.mem_alarm"] in (0, 1)
    assert metrics["rabbitmq.node.disk_free"] == state.disk_free
    assert metrics["rabbitmq.node.disk_free_alarm"] in (0, 1)
    assert metrics["rabbitmq.node.fd_used"] == state.fd_used
    assert metrics["rabbitmq.node.fd_total"] == NODE_FD_TOTAL
    assert metrics["rabbitmq.node.sockets_used"] == state.sockets_used
    assert metrics["rabbitmq.node.sockets_total"] == NODE_SOCKET_TOTAL
    assert metrics["rabbitmq.node.proc_used"] == state.proc_used
    assert metrics["rabbitmq.node.proc_total"] == NODE_PROC_TOTAL
    # Summary panel needs the full set on every co-located doc.
    for name in _NODE_GAUGE_FIELDS:
        assert name in metrics
    # data_stream.* must not be duplicated under resource.attributes
    assert "data_stream.dataset" not in doc["resource"]["attributes"]
    assert _NODE_GAUGE_DYNAMIC_TEMPLATES["metrics.rabbitmq.node.mem_used"] == "gauge_long"
    assert _NODE_GAUGE_DYNAMIC_TEMPLATES["metrics.rabbitmq.node.mem_alarm"] == "gauge_long"
    assert _NODE_GAUGE_DYNAMIC_TEMPLATES["metrics.rabbitmq.node.disk_free"] == "gauge_long"
    assert all(
        _NODE_GAUGE_DYNAMIC_TEMPLATES[f"metrics.{name}"] == "gauge_long"
        for name in _NODE_GAUGE_FIELDS
    )


def test_node_gauge_reset_detects_plain_long_without_gauge():
    class _Resp:
        status_code = 200

        def json(self):
            return {
                "fields": {
                    "metrics.rabbitmq.node.mem_used": {
                        "long": {"type": "long"},
                    },
                    "metrics.rabbitmq.node.mem_alarm": {
                        "long": {"type": "long", "time_series_metric": "gauge"},
                    },
                }
            }

    with patch("httpx.get", return_value=_Resp()) as get:
        assert _node_gauges_need_mapping_reset("https://es.example", "key") is True
        params = get.call_args.kwargs.get("params") or {}
        assert "metrics.rabbitmq.node.mem_used" in params.get("fields", "")


def test_node_gauge_reset_ok_when_all_gauges():
    class _Resp:
        status_code = 200

        def json(self):
            return {
                "fields": {
                    f"metrics.{name}": {
                        "long": {"type": "long", "time_series_metric": "gauge"},
                    }
                    for name in _NODE_GAUGE_FIELDS
                }
            }

    with patch("httpx.get", return_value=_Resp()):
        assert _node_gauges_need_mapping_reset("https://es.example", "key") is False


from generators.rabbitmq_otel import SPEC
from generators.rabbitmq_otel.generator import RabbitMQQueueState, RabbitMQNodeState, _generate_node_metrics, _generate_queue_metrics


def _gauge(m):
    dps = m.get("gauge", {}).get("dataPoints") or []
    return float(dps[0].get("asDouble", dps[0].get("asInt", 0)))


def test_healthy_baseline_no_chaos():
    rng = random.Random(0)
    q = RabbitMQQueueState(rng)
    q.tick(rng, None)
    assert q.ready < 1000
    assert q.consumers >= 1
    n = RabbitMQNodeState(rng)
    n.tick(rng, None)
    metrics = _generate_node_metrics(n)
    by = {m["name"]: m for m in metrics}
    assert _gauge(by["rabbitmq.node.mem_alarm"]) == 0
    assert _gauge(by["rabbitmq.node.disk_free_alarm"]) == 0


def test_ready_and_alarms_keys():
    rng = random.Random(1)
    q = RabbitMQQueueState(rng)
    # A positive probe (GT sends 75.0) means "zero consumers with ready messages";
    # 0 means healthy.
    q.tick(rng, {"high_ready_queue_depth": 2500, "queue_with_zero_consumers_and_ready_messages": 75.0})
    assert q.ready == 2500
    assert q.consumers == 0
    n = RabbitMQNodeState(rng)
    n.tick(rng, {"memory_alarm_triggered": 1, "disk_alarm_triggered": 1})
    metrics = _generate_node_metrics(n)
    by = {m["name"]: m for m in metrics}
    assert _gauge(by["rabbitmq.node.mem_alarm"]) == 1
    assert _gauge(by["rabbitmq.node.disk_free_alarm"]) == 1

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.rabbitmq_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['disk_alarm_triggered', 'high_ready_queue_depth', 'high_unacknowledged_message_count', 'memory_alarm_triggered', 'queue_with_zero_consumers_and_ready_messages']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


# --- per-alert behaviour: each key alone breaches only its own alert ------------------
# Calls the per-cycle state ticks and metric builders directly and evaluates the package
# alert ES|QL (v0.3.0) over the last WINDOW cycles.

import pytest  # noqa: E402

DEFAULTS = {key: meta["default"] for key, meta in _SPEC_FOR_ALERT_KEYS.impact_keys.items()}
SEEDS = range(10)
WINDOW = 6


def _simulate(chaos_by_cycle, seed):
    rng = random.Random(seed)
    queue = RabbitMQQueueState(rng)
    node = RabbitMQNodeState(rng)
    series: dict[tuple, list[float]] = {}
    for chaos in chaos_by_cycle:
        queue.tick(rng, chaos)
        node.tick(rng, chaos)
        for m in _generate_queue_metrics(queue) + _generate_node_metrics(node):
            dp = m["gauge"]["dataPoints"][0] if "gauge" in m else None
            if dp is None:
                continue
            state = next((a["value"]["stringValue"] for a in dp.get("attributes", [])
                          if a["key"] == "state"), "")
            series.setdefault((m["name"], state), []).append(
                float(dp.get("asDouble", dp.get("asInt", 0))))
    return {k: v[-WINDOW:] for k, v in series.items()}


def _breached(series):
    ready = series[("rabbitmq.message.current", "ready")]
    return {
        "disk_alarm_triggered": max(series[("rabbitmq.node.disk_free_alarm", "")]) > 0,
        "high_ready_queue_depth": max(ready) > 1000,
        "high_unacknowledged_message_count": max(
            series[("rabbitmq.message.current", "unacknowledged")]) > 500,
        "memory_alarm_triggered": max(series[("rabbitmq.node.mem_alarm", "")]) > 0,
        "queue_with_zero_consumers_and_ready_messages": (
            max(ready) > 0 and max(series[("rabbitmq.consumer.count", "")]) == 0),
    }


# GT channel values.
FAULTS = {
    "disk_alarm_triggered": 75.0,
    "high_ready_queue_depth": 7500.0,
    "high_unacknowledged_message_count": 3750.0,
    "memory_alarm_triggered": 75.0,
    "queue_with_zero_consumers_and_ready_messages": 75.0,
}


def _hits(breached):
    return sorted(k for k, hit in breached.items() if hit)


def _faulted(key, value, cycles=WINDOW + 2):
    return [{**DEFAULTS, key: value}] * cycles


def test_fault_table_covers_every_impact_key():
    assert sorted(FAULTS) == sorted(_SPEC_FOR_ALERT_KEYS.impact_keys)


@pytest.mark.parametrize("key", sorted(FAULTS))
def test_key_alone_breaches_only_its_own_alert(key):
    for seed in SEEDS:
        breached = _breached(_simulate(_faulted(key, FAULTS[key]), seed))
        assert _hits(breached) == [key], f"seed {seed}: {key} fired {_hits(breached)}"


def test_healthy_run_breaches_nothing():
    for seed in SEEDS:
        for chaos in (dict(DEFAULTS), None):
            breached = _breached(_simulate([chaos] * (WINDOW + 2), seed))
            assert not _hits(breached), f"seed {seed} chaos={chaos is not None}: {_hits(breached)}"


@pytest.mark.parametrize("key", sorted(FAULTS))
def test_alert_clears_after_the_fault_resolves(key):
    cycles = _faulted(key, FAULTS[key], 6) + [dict(DEFAULTS)] * (WINDOW + 2)
    for seed in SEEDS:
        breached = _breached(_simulate(cycles, seed))
        assert not _hits(breached), f"seed {seed}: {_hits(breached)} still firing after {key} resolved"


@pytest.mark.parametrize("key,breaching,healthy", [
    ("disk_alarm_triggered", 0.5, 0.0),   # documented: any value above 0
    ("memory_alarm_triggered", 0.5, 0.0),
    ("high_ready_queue_depth", 1001, 900),
    ("high_unacknowledged_message_count", 501, 400),
    ("queue_with_zero_consumers_and_ready_messages", 0.5, 0.0),
])
def test_key_units_match_the_documented_thresholds(key, breaching, healthy):
    for seed in SEEDS:
        hit = _breached(_simulate(_faulted(key, breaching), seed))[key]
        miss = _breached(_simulate(_faulted(key, healthy), seed))[key]
        assert hit and not miss, f"seed {seed}: {key} breaching={hit} healthy={miss}"


# --- Package SLO templates (consumer availability, acknowledgment rate) ---

def _point_docs(metrics):
    """Group data points the way Elasticsearch does: one document per attribute set."""
    docs: dict[tuple, dict] = {}
    for m in metrics:
        for kind in ("gauge", "sum"):
            for dp in (m.get(kind) or {}).get("dataPoints") or []:
                key = tuple(sorted((a["key"], str(a["value"])) for a in dp.get("attributes", [])))
                docs.setdefault(key, {})[m["name"]] = float(dp.get("asDouble", dp.get("asInt", 0)))
    return list(docs.values())


def _sum_value(metrics, name):
    m = next(x for x in metrics if x["name"] == name)
    assert m["sum"]["isMonotonic"] is True
    return int(m["sum"]["dataPoints"][0]["asInt"])


def test_consumer_count_is_emitted_once_per_queue_scrape():
    """Like the rabbitmq receiver: consumer.count on the queue document only.

    The package SLO "Consumer availability" counts every queue document (including
    the two message.current state series) and so reads about 33% against real
    receiver data too; it is a reasoned skip in tests/fixtures/template_emit_skips.json
    rather than a reason to duplicate the gauge and triple the dashboards' sums.
    """
    rng = random.Random(3)
    q = RabbitMQQueueState(rng)
    q.tick(rng, None)
    docs = _point_docs(_generate_queue_metrics(q))
    assert len(docs) == 3  # queue counters + ready + unacknowledged
    assert sum("rabbitmq.consumer.count" in d for d in docs) == 1


def test_acknowledged_is_cumulative_and_tracks_delivered():
    """SLO "Message acknowledgment rate": (Δacked)/(Δdelivered) >= 0.995 when healthy."""
    rng = random.Random(5)
    q = RabbitMQQueueState(rng)
    q.tick(rng, None)
    first = _generate_queue_metrics(q)
    for _ in range(10):
        q.tick(rng, None)
    last = _generate_queue_metrics(q)
    acked = _sum_value(last, "rabbitmq.message.acknowledged") - _sum_value(first, "rabbitmq.message.acknowledged")
    delivered = _sum_value(last, "rabbitmq.message.delivered") - _sum_value(first, "rabbitmq.message.delivered")
    assert delivered > 0
    assert acked / delivered >= 0.995
    assert _sum_value(last, "rabbitmq.message.acknowledged") <= _sum_value(last, "rabbitmq.message.delivered")


def test_unacked_backlog_fault_lowers_ack_rate():
    rng = random.Random(6)
    q = RabbitMQQueueState(rng)
    q.tick(rng, None)
    acked0, delivered0 = q.acknowledged, q.delivered
    q.tick(rng, {"high_unacknowledged_message_count": 5000})
    assert (q.acknowledged - acked0) < (q.delivered - delivered0)
