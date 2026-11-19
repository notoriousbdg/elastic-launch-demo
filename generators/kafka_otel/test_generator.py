"""Technology-specific tests for the ``kafka`` generator.

Standard emit/registry tests live in tests/test_generators_emit.py
(parametrized over GENERATORS) and run for this generator automatically.
Add tests here only for behaviour that is special to kafka.
"""
import sys
import os

# Allow importing from the shared test harness.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "tests"))

from generator_engine.db_common import build_signal_resource
from generators.kafka_otel.generator import build_partition_metrics_doc


def test_partition_metrics_doc_colocates_offset_and_replica_gauges():
    """Offset and replication panels AND-filter metric pairs on one document."""
    resource = build_signal_resource(
        ns="finserv",
        signal_type="metrics",
        dataset="kafkametricsreceiver",
        service_name="kafka-broker",
        service_version="3.6.1",
        service_instance_id="kafka-broker-001",
        host_name="finserv-kafka-broker",
        cloud={
            "provider": "aws",
            "platform": "aws_msk",
            "region": "us-east-1",
            "availability_zone": "us-east-1a",
        },
    )
    doc = build_partition_metrics_doc(
        resource,
        topic="finserv.events.created",
        partition="2",
        current_offset=1_000_000,
        oldest_offset=900_000,
        replicas=3,
        replicas_in_sync=2,
    )

    assert doc["attributes"] == {"topic": "finserv.events.created", "partition": "2"}
    assert doc["metrics"]["kafka.partition.current_offset"] == 1_000_000
    assert doc["metrics"]["kafka.partition.oldest_offset"] == 900_000
    assert doc["metrics"]["kafka.partition.replicas"] == 3
    assert doc["metrics"]["kafka.partition.replicas_in_sync"] == 2
    assert doc["data_stream"]["dataset"] == "kafkametricsreceiver.otel"
    assert "data_stream.type" not in doc["resource"]["attributes"]


def test_healthy_baseline_all_partitions_full_rf():
    """No chaos → every partition stays at full RF (0=healthy alert)."""
    import random
    from generators.kafka_otel.generator import (
        KafkaMetricState,
        REPLICATION_FACTOR,
        _generate_metrics,
    )

    topics = ["t.a", "t.b", "t.c"]
    rng = random.Random(7)
    state = KafkaMetricState(rng, topics, partitions_per_topic=6)
    assert all(
        sync == REPLICATION_FACTOR
        for syncs in state.replicas_in_sync.values()
        for sync in syncs
    )

    for _ in range(10):
        _generate_metrics(state, topics, ["g"], 6, 3, rng, None)
    assert all(
        sync == REPLICATION_FACTOR
        for syncs in state.replicas_in_sync.values()
        for sync in syncs
    )


def test_sticky_under_replication_survives_max_over_window():
    """KPI panels use MAX(in_sync) over the window — must stay under RF
    while the under_replicated_partitions fault is active, then recover
    once chaos clears."""
    import random
    from generators.kafka_otel.generator import (
        KafkaMetricState,
        REPLICATION_FACTOR,
        _generate_metrics,
    )

    topics = ["t.a", "t.b", "t.c"]
    rng = random.Random(7)
    state = KafkaMetricState(rng, topics, partitions_per_topic=6)
    chaos = {"under_replicated_partitions": 50.0}

    for _ in range(10):
        _generate_metrics(state, topics, ["g"], 6, 3, rng, chaos)
    under = [
        (t, p, sync)
        for t, syncs in state.replicas_in_sync.items()
        for p, sync in enumerate(syncs)
        if sync < REPLICATION_FACTOR
    ]
    assert under, "expected under-replication while chaos is active"

    # Simulate a few more ticks while still faulted; sticky partitions must
    # not heal on their own before the fault clears.
    for _ in range(5):
        _generate_metrics(state, topics, ["g"], 6, 3, rng, chaos)
    for topic, part, sync in under:
        assert state.replicas_in_sync[topic][part] == sync

    # Clearing chaos must recover every partition back to full RF.
    _generate_metrics(state, topics, ["g"], 6, 3, rng, None)
    assert all(
        sync == REPLICATION_FACTOR
        for syncs in state.replicas_in_sync.values()
        for sync in syncs
    )


def test_generate_metrics_returns_overview_bulk_rows():
    import random
    from generators.kafka_otel.generator import (
        KafkaMetricState,
        _generate_metrics,
        build_cluster_overview_doc,
        build_consumer_group_docs,
    )

    topics = ["orders"]
    rng = random.Random(3)
    state = KafkaMetricState(rng, topics, partitions_per_topic=2)
    resource = {
        "attributes": [
            {"key": "service.name", "value": {"stringValue": "kafka"}},
        ]
    }
    metrics, partition_rows, brokers, consumer_rows = _generate_metrics(
        state, topics, ["cg-1"], 2, 3, rng, None,
    )
    assert brokers == 3
    assert partition_rows
    assert any("kafka.consumer_group.lag" in r["metrics"] for r in consumer_rows)
    assert any(m["name"] == "kafka.brokers" for m in metrics)
    overview = build_cluster_overview_doc(resource, brokers=brokers)
    assert overview["metrics"]["kafka.brokers"] == 3
    cg_docs = build_consumer_group_docs(resource, consumer_rows)
    assert any("kafka.consumer_group.members" in d["metrics"] for d in cg_docs)
    assert any("kafka.consumer_group.lag" in d["metrics"] for d in cg_docs)


# --- 1:1 alert impact_keys coverage (auto) ---
from generators.kafka_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['broker_count_below_threshold', 'empty_consumer_groups', 'high_consumer_lag_by_topic', 'high_consumer_lag_per_partition', 'under_replicated_partitions']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
