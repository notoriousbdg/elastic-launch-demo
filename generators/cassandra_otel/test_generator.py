"""Technology-specific tests for the ``cassandra_otel`` generator.

Standard emit/registry tests live in tests/test_generators_emit.py
(parametrized over GENERATORS) and run for this generator automatically.
"""

from __future__ import annotations

import random
import threading

from generators.cassandra_otel.generator import (
    ERROR_STATUSES,
    OPERATIONS,
    CassandraMetricState,
    _generate_metrics,
    run,
)


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


def _dp_value(metric: dict):
    dps = (metric.get("sum") or metric.get("gauge") or {}).get("dataPoints") or []
    assert dps, metric
    dp = dps[0]
    if "asDouble" in dp:
        return float(dp["asDouble"])
    if "asInt" in dp:
        return int(dp["asInt"])
    raise AssertionError(f"no value in datapoint: {dp}")


def test_generate_metrics_dashboard_contract():
    """Overview panels need counters, UpDownCounters, double gauges, op/status dims."""
    state = CassandraMetricState(random.Random(0), storage_bytes=50_000_000_000)
    metrics = _generate_metrics(state, random.Random(1))

    by_name: dict[str, list[dict]] = {}
    for m in metrics:
        by_name.setdefault(m["name"], []).append(m)

    assert set(by_name) == {
        "cassandra.client.request.count",
        "cassandra.client.request.error.count",
        "cassandra.client.request.range_slice.latency.99p",
        "cassandra.client.request.read.latency.99p",
        "cassandra.client.request.write.latency.99p",
        "cassandra.compaction.tasks.pending",
        "cassandra.compaction.tasks.completed",
        "cassandra.storage.load.count",
        "cassandra.storage.total_hints.in_progress.count",
    }

    # RATE/INCREASE panels require monotonic counters.
    for name in (
        "cassandra.client.request.count",
        "cassandra.client.request.error.count",
        "cassandra.compaction.tasks.completed",
    ):
        assert by_name[name][0]["sum"]["isMonotonic"] is True

    # Storage + hints are UpDownCounters (LAST_OVER_TIME panels).
    for name in (
        "cassandra.storage.load.count",
        "cassandra.storage.total_hints.in_progress.count",
    ):
        assert "sum" in by_name[name][0]
        assert by_name[name][0]["sum"]["isMonotonic"] is False

    # Latency gauges must be doubles (groovy doubleValueCallback).
    for name in (
        "cassandra.client.request.read.latency.99p",
        "cassandra.client.request.write.latency.99p",
        "cassandra.client.request.range_slice.latency.99p",
    ):
        m = by_name[name][0]
        assert "gauge" in m
        dp = m["gauge"]["dataPoints"][0]
        assert "asDouble" in dp, name
        assert isinstance(dp["asDouble"], float)

    ops = {_attr_map(m).get("operation") for m in by_name["cassandra.client.request.count"]}
    assert ops == set(OPERATIONS)

    err_ops = {_attr_map(m).get("operation") for m in by_name["cassandra.client.request.error.count"]}
    err_status = {_attr_map(m).get("status") for m in by_name["cassandra.client.request.error.count"]}
    assert err_ops == set(OPERATIONS)
    assert err_status == set(ERROR_STATUSES)


def test_run_emits_multiple_server_addresses():
    client = _CaptureClient()
    stop = threading.Event()
    nodes = ["cass-a", "cass-b", "cass-c"]

    def _target():
        run(
            client,
            stop,
            scenario_data={
                "namespace": "inttest",
                "generator_config": {
                    "service_name": "cassandra-node",
                    "node_names": nodes,
                },
                "channel_registry": {},
            },
        )

    t = threading.Thread(target=_target)
    t.start()
    for _ in range(50):
        if len(client.batches) >= len(nodes):
            stop.set()
            break
        t.join(0.1)
    else:
        stop.set()
    t.join(2)

    assert len(client.batches) >= len(nodes)
    addresses = {
        _resource_attr_map(resource).get("server.address")
        for resource, _scope, _metrics in client.batches
    }
    assert addresses == set(nodes)
    datasets = {
        _resource_attr_map(resource).get("data_stream.dataset")
        for resource, _scope, _metrics in client.batches
    }
    assert datasets == {"cassandra"}

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.cassandra_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['compaction_falling_behind_pending_tasks', 'high_error_rate_by_node', 'high_range_slice_latency_p99', 'high_read_latency_p99', 'high_write_latency_p99', 'hints_in_progress_replicas_unreachable', 'request_errors_timeout_unavailable_failure']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
