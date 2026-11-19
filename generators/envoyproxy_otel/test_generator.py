"""Envoy-specific tests for the envoyproxy_otel generator."""

from __future__ import annotations

from unittest.mock import MagicMock

from generators.envoyproxy_otel.generator import (
    DATA_STREAM,
    DATASET,
    METRIC_NAMES,
    _RATIO_FLOAT_FIELDS,
    build_metrics_doc,
    run,
)


def _run_ticks(n_ticks: int = 1) -> list[dict]:
    client = MagicMock()
    client.configured = True
    stop = MagicMock()
    # False for each tick, then True so the loop exits without waiting EMIT_INTERVAL_S.
    stop.is_set.side_effect = [False] * n_ticks + [True]
    stop.wait.return_value = None
    docs: list[dict] = []

    def _after_bulk(stream, batch, **_kwargs):
        assert stream == DATA_STREAM
        docs.extend(batch)

    client.send_bulk.side_effect = _after_bulk
    run(client, stop, scenario_data={
        "namespace": "testns",
        "generator_config": {},
        "channel_registry": {},
    })
    return docs


def test_metric_names_match_dashboard_authority():
    assert "envoy.http.ingress_http.downstream_rq_total" in METRIC_NAMES
    assert "envoy.server.live" in METRIC_NAMES
    assert "envoy.cluster.upstream_rq_total" not in METRIC_NAMES
    # The OOTB error-rate alert COALESCE-sums ingress_http + admin
    assert "envoy.http.admin.downstream_rq_completed" in METRIC_NAMES
    assert "envoy.http.admin.downstream_rq_2xx" in METRIC_NAMES
    assert len(METRIC_NAMES) == 28


def test_build_metrics_doc_uses_stock_dataset_and_colocates_fields():
    doc = build_metrics_doc(
        host_name="testns-envoy",
        service_name="envoy",
        metrics={
            "envoy.server.live": 1,
            "envoy.http.ingress_http.downstream_rq_2xx": 980,
            "envoy.http.ingress_http.downstream_rq_completed": 1000,
            "envoy.server.memory_allocated": 100,
            "envoy.server.memory_heap_size": 200,
        },
        timestamp_ms=1_700_000_000_000,
    )
    assert doc["data_stream"]["dataset"] == DATASET
    assert doc["data_stream"]["dataset"] == "statsdreceiver"
    assert "otel" not in doc["data_stream"]["dataset"]
    assert doc["envoy.server.live"] == 1
    # Ratio fields must be floats so stock ES|QL long/long panels do not truncate.
    assert doc["envoy.http.ingress_http.downstream_rq_2xx"] == 980.0
    assert isinstance(doc["envoy.http.ingress_http.downstream_rq_2xx"], float)
    assert doc["envoy.http.ingress_http.downstream_rq_completed"] == 1000.0
    assert doc["envoy.server.memory_allocated"] == 100.0
    assert doc["envoy.server.memory_heap_size"] == 200.0


def test_emits_stock_data_stream_via_bulk():
    docs = _run_ticks(n_ticks=2)
    assert len(docs) >= 2
    for doc in docs:
        assert doc["data_stream"]["dataset"] == "statsdreceiver"
        assert set(METRIC_NAMES) <= set(doc)
        for name in _RATIO_FLOAT_FIELDS:
            assert isinstance(doc[name], float)


def test_emits_cumulative_totals_for_max_kpis():
    docs = _run_ticks(n_ticks=1)
    doc = docs[0]
    assert doc["envoy.http.ingress_http.downstream_rq_total"] >= 50_000
    assert doc["envoy.server.uptime"] >= 86_400
    assert doc["envoy.cluster_manager.cluster_added"] >= 1


def test_skips_when_bulk_client_not_configured():
    client = MagicMock()
    client.configured = False
    stop = MagicMock()
    stop.is_set.return_value = True
    run(client, stop, scenario_data={
        "namespace": "testns",
        "generator_config": {},
        "channel_registry": {},
    })
    assert not client.send_bulk.called


def test_healthy_baseline_no_warming_clusters():
    docs = _run_ticks(n_ticks=2)
    for doc in docs:
        assert doc["envoy.cluster_manager.warming_clusters"] == 0
        assert doc["envoy.listener_manager.total_listeners_warming"] == 0
        assert doc["envoy.server.live"] == 1
        assert doc["envoy.server.state"] == 0
        assert doc["envoy.server.days_until_first_cert_expiring"] >= 7
        ratio = doc["envoy.server.memory_allocated"] / doc["envoy.server.memory_heap_size"]
        assert ratio < 0.9

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.envoyproxy_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['certificate_expiring_soon', 'high_downstream_error_rate', 'high_memory_pressure', 'listeners_warming', 'server_not_live', 'server_state_not_live', 'warming_clusters_stuck']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
