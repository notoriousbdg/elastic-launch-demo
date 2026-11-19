"""RDS-specific tests for the aws_rds_otel generator."""

import random

from generators.aws_rds_otel import SPEC
from generators.aws_rds_otel.generator import (
    METRIC_NAMES,
    RDSMetricState,
    _generate_metrics,
)


def _attr_map(metric: dict) -> dict:
    dps = metric.get("gauge", {}).get("dataPoints") or metric.get("sum", {}).get("dataPoints") or []
    if not dps:
        return {}
    return {a["key"]: next(iter(a["value"].values())) for a in dps[0].get("attributes", [])}


def _gauge_value(metric: dict) -> float:
    dps = metric.get("gauge", {}).get("dataPoints") or []
    return float(dps[0]["asDouble"] if "asDouble" in dps[0] else dps[0]["asInt"])


def _metrics(chaos=None, seed=1):
    rng = random.Random(seed)
    state = RDSMetricState(rng, 500 * 1024 * 1024 * 1024)
    return _generate_metrics(
        state, "us-east-1", "123456789012", rng, chaos,
    )


def _by_name(metrics, suffix: str):
    return [m for m in metrics if m["name"].endswith(suffix)]


def test_metric_names_include_burst_tempdb_and_queue():
    for name in (
        "amazonaws.com/AWS/RDS/BurstBalance",
        "amazonaws.com/AWS/RDS/CheckpointLag",
        "amazonaws.com/AWS/RDS/DiskQueueDepth",
        "amazonaws.com/AWS/RDS/FailedSQLServerAgentJobsCount",
        "amazonaws.com/AWS/RDS/TempDbAvailableDataSpace",
        "amazonaws.com/AWS/RDS/TempDbAvailableLogSpace",
        "amazonaws.com/AWS/RDS/TempDbDataFileUsage",
        "amazonaws.com/AWS/RDS/TempDbLogFileUsage",
    ):
        assert name in METRIC_NAMES


def test_rds_emits_average_and_maximum_for_peak_panels():
    metrics = _metrics(None)
    cpu = _by_name(metrics, "CPUUtilization")
    assert len(cpu) == 2
    assert {_attr_map(m)["stat"] for m in cpu} == {"Average", "Maximum"}

    conns = _by_name(metrics, "DatabaseConnections")
    assert {_attr_map(m)["stat"] for m in conns} == {"Average", "Maximum"}

    for suffix in ("ReadLatency", "WriteLatency", "ReplicaLag"):
        series = _by_name(metrics, suffix)
        assert {_attr_map(m)["stat"] for m in series} == {"Average", "Maximum"}

    checkpoint = next(m for m in metrics if m["name"].endswith("CheckpointLag"))
    assert _attr_map(checkpoint)["stat"] == "Maximum"


def test_healthy_baseline_no_chaos():
    metrics = _metrics(None)
    cpu = next(
        m for m in _by_name(metrics, "CPUUtilization") if _attr_map(m)["stat"] == "Average"
    )
    assert _gauge_value(cpu) <= 80.0
    replica = next(
        m for m in _by_name(metrics, "ReplicaLag") if _attr_map(m)["stat"] == "Maximum"
    )
    assert _gauge_value(replica) <= 60.0
    storage = next(m for m in metrics if m["name"].endswith("FreeStorageSpace"))
    assert _gauge_value(storage) >= 10_737_418_240
    burst = next(m for m in metrics if m["name"].endswith("BurstBalance"))
    assert _gauge_value(burst) >= 20.0
    checkpoint = next(m for m in metrics if m["name"].endswith("CheckpointLag"))
    assert _gauge_value(checkpoint) <= 60.0
    conns = next(
        m for m in _by_name(metrics, "DatabaseConnections") if _attr_map(m)["stat"] == "Average"
    )
    assert _gauge_value(conns) <= 100.0
    freeable = next(m for m in metrics if m["name"].endswith("FreeableMemory"))
    assert _gauge_value(freeable) >= 268_435_456
    swap = next(m for m in metrics if m["name"].endswith("SwapUsage"))
    assert _gauge_value(swap) <= 104_857_600
    queue = next(m for m in metrics if m["name"].endswith("DiskQueueDepth"))
    assert _gauge_value(queue) <= 5.0
    for suffix in ("ReadLatency", "WriteLatency"):
        lat = next(m for m in _by_name(metrics, suffix) if _attr_map(m)["stat"] == "Maximum")
        assert _gauge_value(lat) <= 0.05


def test_cpu_spike_alone_breaches_cpu_alert():
    metrics = _metrics({"cpu_utilization_high": 90.0})
    cpu = next(
        m for m in _by_name(metrics, "CPUUtilization") if _attr_map(m)["stat"] == "Average"
    )
    assert _gauge_value(cpu) > 80.0
    replica = next(
        m for m in _by_name(metrics, "ReplicaLag") if _attr_map(m)["stat"] == "Maximum"
    )
    assert _gauge_value(replica) <= 60.0


def test_replica_lag_alone_breaches_replica_alert():
    metrics = _metrics({"replica_lag_high": 120.0})
    replica = next(
        m for m in _by_name(metrics, "ReplicaLag") if _attr_map(m)["stat"] == "Maximum"
    )
    assert _gauge_value(replica) > 60.0
    cpu = next(
        m for m in _by_name(metrics, "CPUUtilization") if _attr_map(m)["stat"] == "Average"
    )
    assert _gauge_value(cpu) <= 80.0


def test_storage_drain_alone_breaches_free_storage_alert():
    metrics = _metrics({"free_storage_space_low": 5_000_000_000})
    storage = next(m for m in metrics if m["name"].endswith("FreeStorageSpace"))
    assert _gauge_value(storage) < 10_737_418_240


def test_iops_multiplier_alone_breaches_queue_and_latency():
    metrics = _metrics(
        {"disk_queue_depth_high": 10.0, "read_latency_high": 0.1, "write_latency_high": 0.1}
    )
    queue = next(m for m in metrics if m["name"].endswith("DiskQueueDepth"))
    assert _gauge_value(queue) > 5.0
    for suffix in ("ReadLatency", "WriteLatency"):
        lat = next(m for m in _by_name(metrics, suffix) if _attr_map(m)["stat"] == "Maximum")
        assert _gauge_value(lat) > 0.05


def test_burst_balance_alone_breaches_burst_alert():
    metrics = _metrics({"burst_balance_low": 10.0})
    burst = next(m for m in metrics if m["name"].endswith("BurstBalance"))
    assert _gauge_value(burst) < 20.0


def test_checkpoint_lag_alone_breaches_checkpoint_alert():
    metrics = _metrics({"checkpoint_lag_high": 90.0})
    checkpoint = next(m for m in metrics if m["name"].endswith("CheckpointLag"))
    assert _gauge_value(checkpoint) > 60.0


def test_connection_target_alone_breaches_connections_alert():
    metrics = _metrics({"database_connections_high": 150.0})
    conns = next(
        m for m in _by_name(metrics, "DatabaseConnections") if _attr_map(m)["stat"] == "Average"
    )
    assert _gauge_value(conns) > 100.0


def test_freeable_memory_alone_breaches_memory_alert():
    metrics = _metrics({"freeable_memory_low": 100_000_000})
    freeable = next(m for m in metrics if m["name"].endswith("FreeableMemory"))
    assert _gauge_value(freeable) < 268_435_456


def test_swap_usage_alone_breaches_swap_alert():
    metrics = _metrics({"swap_usage_high": 200_000_000})
    swap = next(m for m in metrics if m["name"].endswith("SwapUsage"))
    assert _gauge_value(swap) > 104_857_600

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.aws_rds_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['burst_balance_low', 'checkpoint_lag_high', 'cpu_utilization_high', 'database_connections_high', 'disk_queue_depth_high', 'free_storage_space_low', 'freeable_memory_low', 'read_latency_high', 'replica_lag_high', 'swap_usage_high', 'write_latency_high']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


# --- RDS investigation workflow shape (Template Library aws-rds-connection-exhaustion) ---
from generators.aws_rds_otel.generator import _instance_id, workflow_input_values


def _stat(metrics, suffix: str, stat: str) -> float:
    return _gauge_value(next(m for m in _by_name(metrics, suffix) if _attr_map(m)["stat"] == stat))


def test_cloudwatch_dimensions_are_data_point_attributes():
    rng = random.Random(1)
    metrics = _generate_metrics(
        RDSMetricState(rng, 500 * 1024 ** 3), "us-east-1", "123456789012", rng, None,
        instance_id="demo-orders-db", engine="postgres",
    )
    for metric in metrics:
        attrs = _attr_map(metric)
        assert attrs["DBInstanceIdentifier"] == "demo-orders-db"
        assert attrs["Engine"] == "postgres"
        assert attrs["MetricName"] and attrs["stat"]


def test_connection_exhaustion_plateaus_with_an_io_footprint_under_the_alerts():
    for seed in range(20):
        healthy, flood = _metrics(None, seed), _metrics({"database_connections_high": 117.0}, seed)
        assert _stat(flood, "DatabaseConnections", "Average") == 117
        assert _stat(flood, "DatabaseConnections", "Maximum") == 117
        assert _stat(flood, "CPUUtilization", "Average") == _stat(healthy, "CPUUtilization", "Average")
        assert _stat(flood, "WriteLatency", "Maximum") < 0.05
        assert _stat(flood, "DiskQueueDepth", "Average") < 5.0
        assert _stat(flood, "WriteLatency", "Average") > _stat(healthy, "WriteLatency", "Average")


def test_connections_baseline_is_configurable():
    rng = random.Random(3)
    for _ in range(10):
        metrics = _generate_metrics(
            RDSMetricState(rng, 500 * 1024 ** 3), "us-east-1", "1", rng, None, connections_baseline=(0, 2),
        )
        assert _stat(metrics, "DatabaseConnections", "Average") <= 2


def test_configured_instance_id_is_scoped_to_the_scenario():
    assert _instance_id("awsorders", {"instance_id": "orders-db"}) == "awsorders-orders-db"
    assert _instance_id("awsorders", {}) == "awsorders-rds-01"
    assert workflow_input_values({"namespace": "awsorders", "generator_config": {"instance_id": "orders-db"}}) == {
        "db_instance_identifier": "awsorders-orders-db"
    }
