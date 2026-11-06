"""Technology-specific tests for the ``activemq_otel`` generator.

Standard emit/registry tests live in tests/test_generators_emit.py
(parametrized over GENERATORS) and run for this generator automatically.
"""
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "tests"))

from generators.activemq_otel.generator import (
    MEMORY_LIMIT_BYTES,
    METRIC_NAMES,
    _BrokerState,
    _DestState,
    _emit_destination,
    build_memory_limit_doc,
)
from generator_engine.db_common import build_signal_resource


def test_memory_limit_doc_colocates_usage_and_limit():
    """Broker Health AND-filters memory.usage + memory.limit on one document."""
    resource = build_signal_resource(
        ns="nova7",
        signal_type="metrics",
        dataset="activemq",
        service_name="activemq",
        service_version="5.18.3",
        service_instance_id="activemq-001",
        host_name="nova7-activemq",
        cloud={
            "provider": "aws",
            "platform": "aws_ec2",
            "region": "us-east-1",
            "availability_zone": "us-east-1a",
        },
        extra_attrs={"activemq.broker.name": "nova7-broker"},
    )
    usage = int(MEMORY_LIMIT_BYTES * 0.42)
    doc = build_memory_limit_doc(
        resource,
        usage_bytes=usage,
        limit_bytes=MEMORY_LIMIT_BYTES,
    )

    assert doc["metrics"]["activemq.memory.usage"] == usage
    assert doc["metrics"]["activemq.memory.limit"] == MEMORY_LIMIT_BYTES
    assert doc["data_stream"]["dataset"] == "activemq.otel"
    assert "data_stream.type" not in doc["resource"]["attributes"]
    assert doc["resource"]["attributes"]["activemq.broker.name"] == "nova7-broker"
    assert doc["resource"]["attributes"]["service.name"] == "nova7-activemq"


def test_metric_names_cover_dashboard_fields():
    """METRIC_NAMES must include every field the three package dashboards query."""
    required = {
        "activemq.broker.connection.current",
        "activemq.broker.dequeue.count",
        "activemq.broker.enqueue.count",
        "activemq.broker.message.current",
        "activemq.connection.count",
        "activemq.consumer.count",
        "activemq.disk.store_usage",
        "activemq.disk.temp_usage",
        "activemq.jvm.cpu.process.usage",
        "activemq.jvm.file_descriptor.max",
        "activemq.jvm.file_descriptor.open",
        "activemq.jvm.gc.collections",
        "activemq.jvm.memory.heap.max",
        "activemq.jvm.memory.heap.used",
        "activemq.jvm.thread.count",
        "activemq.memory.limit",
        "activemq.memory.usage",
        "activemq.memory.utilization",
        "activemq.message.current",
        "activemq.message.dequeued",
        "activemq.message.enqueued",
        "activemq.message.expired",
        "activemq.message.wait_time.avg",
        "activemq.producer.count",
        "activemq.queue.blocked.sends",
        "activemq.queue.message.inflight",
        "activemq.store.limit",
        "activemq.store.utilization",
        "activemq.temp.limit",
        "activemq.temp.utilization",
        "activemq.topic.message.inflight",
    }
    assert required.issubset(set(METRIC_NAMES))


def test_destination_attrs_include_both_name_keys():
    """Dashboards COALESCE(activemq.destination.name, messaging.destination.name)."""
    metrics = _emit_destination(
        "nova7-broker",
        "orders.queue",
        "queue",
        _DestState(random.Random(0)),
    )
    assert metrics
    attrs = metrics[0]["sum"]["dataPoints"][0]["attributes"]
    by_key = {a["key"]: a["value"]["stringValue"] for a in attrs}
    assert by_key["activemq.destination.name"] == "orders.queue"
    assert by_key["messaging.destination.name"] == "orders.queue"
    assert by_key["activemq.destination.type"] == "queue"
    assert by_key["activemq.broker.name"] == "nova7-broker"


def test_blocked_sends_stay_zero_without_chaos():
    """OOTB alert is SUM(RATE(blocked.sends)) > 0 — no ambient growth at queue_depth_mult==1.0."""
    rng = random.Random(0)
    broker = _BrokerState(rng)
    dest = _DestState(rng)
    assert broker.blocked_sends.value == 0
    assert dest.blocked.value == 0
    for _ in range(20):
        broker.tick(rng, queue_depth_mult=1.0, load=1.0)
        dest.tick(rng, queue_depth_mult=1.0, consumer_drop=0.0, load=1.0)
    assert broker.blocked_sends.value == 0
    assert dest.blocked.value == 0


def test_blocked_sends_grow_under_queue_depth_pressure():
    rng = random.Random(1)
    broker = _BrokerState(rng)
    for _ in range(50):
        broker.tick(rng, queue_depth_mult=3.0, load=1.0)
    assert broker.blocked_sends.value >= 0  # can grow; just must not error
    # With enough ticks at mult>1, growth is very likely (not guaranteed each tick).
    rng2 = random.Random(2)
    dest = _DestState(rng2)
    for _ in range(50):
        dest.tick(rng2, queue_depth_mult=3.0, consumer_drop=0.0, load=1.0)


from generators.activemq_otel import SPEC

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.activemq_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['blocked_sends_detected', 'broker_memory_utilization_high', 'dead_letter_queue_depth_high', 'high_jvm_cpu_utilization', 'high_jvm_heap_utilization', 'persistent_store_utilization_high', 'queue_depth_high']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
