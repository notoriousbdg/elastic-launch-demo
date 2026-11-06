"""InfluxDB tests for impact key apply + healthy baseline."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from generators.influxdb_otel import SPEC
from generators.influxdb_otel import generator as mod


class _FakeChaos:
    def get_active_channels(self):
        return ["ch1"]


def _run(impact: dict | None = None) -> list[dict]:
    client = MagicMock()
    stop = MagicMock()
    stop.is_set.side_effect = [False, True]
    stop.wait.return_value = None
    registry = {}
    controller = None
    if impact is not None:
        controller = _FakeChaos()
        registry = {"ch1": {"generator_impact": {"influxdb_otel": impact}}}
    with patch.object(mod, "_reset_counter_long_mapping"):
        mod.run(client, stop, scenario_data={
            "namespace": "testns",
            "generator_config": {"chaos": "explicit"},
            "channel_registry": registry,
        }, chaos_controller=controller)
    metrics: list[dict] = []
    for call in client.send_metrics_scoped.call_args_list:
        metrics.extend(call.args[2])
    return metrics


def _gauge(metrics, name) -> float:
    for m in metrics:
        if m["name"] == name and "gauge" in m:
            dp = m["gauge"]["dataPoints"][0]
            return float(dp.get("asDouble", dp.get("asInt")))
    raise AssertionError(f"missing {name}")


def test_healthy_baseline_no_chaos():
    m = _run(None)
    assert _gauge(m, "up") == 1
    assert _gauge(m, "go_goroutines") <= 10000
    assert _gauge(m, "qc_queueing_active") < 5
    assert _gauge(m, "storage_compactions_queued") < 10


def test_scrape_down_and_goroutines_and_compaction():
    assert _gauge(_run({"scrape_target_down": 1.0}), "up") == 0
    assert _gauge(_run({"go_goroutine_leak_suspected": 15000}), "go_goroutines") > 10000
    assert _gauge(_run({"compaction_queue_backlog": 15}), "storage_compactions_queued") >= 10


def test_query_controller_saturation():
    m = _run({"query_controller_saturated": 8})
    assert _gauge(m, "qc_queueing_active") >= 5
    assert _gauge(m, "qc_memory_unused_bytes") < 10 * 1024 * 1024


def test_write_error_rate_increases_errors():
    m = _run({"write_error_rate": 0.9})
    assert any(x["name"] == "storage_cache_writes_err" for x in m)

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.influxdb_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['cache_write_errors', 'compaction_queue_backlog', 'dropped_writes', 'go_goroutine_leak_suspected', 'http_api_high_error_rate', 'query_controller_saturated', 'scrape_target_down', 'shard_write_errors', 'storage_writer_timeouts', 'task_execution_failures', 'wal_write_errors']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


# --- Package SLO templates: latency histograms (seconds) ---
import random as _random


def _hist(metrics, name):
    m = next(x for x in metrics if x["name"] == name)
    assert m["unit"] == "s"
    assert m["histogram"]["aggregationTemporality"] == 1  # delta per scrape
    return m["histogram"]["dataPoints"][0]


def _good_fraction(samples, bounds, to):
    """Fraction of observations in buckets whose ES midpoint is below ``to``."""
    dp = mod._histogram("h", "s", samples, bounds, "0")
    counts, total, good = dp["histogram"]["dataPoints"][0]["bucketCounts"], len(samples), 0
    for i, c in enumerate(counts):
        mid = bounds[0] if i == 0 else bounds[-1] if i >= len(bounds) else (bounds[i - 1] + bounds[i]) / 2
        good += c if mid < to else 0
    return good / total


def test_latency_histograms_emitted_in_seconds():
    m = _run(None)
    for name in ("http_api_request_duration_seconds", "qc_executing_duration_seconds"):
        dp = _hist(m, name)
        assert dp["count"] == sum(dp["bucketCounts"])
        assert dp["count"] > 0


def test_healthy_latency_meets_slo_thresholds():
    """SLOs: HTTP < 0.2 s and query execution < 5 s for >= 99.5% of observations."""
    rng = _random.Random(11)
    http = mod._sample_latencies(rng, 20_000, mod._HTTP_LATENCY_SHAPE)
    qc = mod._sample_latencies(rng, 20_000, mod._QC_LATENCY_SHAPE)
    assert _good_fraction(http, mod._HTTP_LATENCY_BOUNDS, 0.2) >= 0.999
    assert _good_fraction(qc, mod._QC_LATENCY_BOUNDS, 5.0) >= 0.999


def test_http_error_fault_pushes_latency_past_slo():
    rng = _random.Random(12)
    mult = 1.0 + 0.9 * 4.0  # http_api_high_error_rate=0.9, as run() scales it
    http = mod._sample_latencies(rng, 20_000, mod._HTTP_LATENCY_SHAPE, mult)
    qc = mod._sample_latencies(rng, 20_000, mod._QC_LATENCY_SHAPE, mult)
    assert _good_fraction(http, mod._HTTP_LATENCY_BOUNDS, 0.2) < 0.995
    assert _good_fraction(qc, mod._QC_LATENCY_BOUNDS, 5.0) < 0.995
