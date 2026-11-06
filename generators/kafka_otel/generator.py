#!/usr/bin/env python3
"""Kafka Generator — sends kafkametricsreceiver OTel metrics via OTLP.

Emits data matching the kafka_otel content package
(discovery.datasets: kafkametricsreceiver.otel), which populates the bundled
Overview, Consumer Groups, Topics & Partitions, and Replication dashboards.

Signal types:
  - Metrics: kafka.brokers, kafka.consumer_group.lag,
             kafka.consumer_group.lag_sum, kafka.consumer_group.members,
             kafka.consumer_group.offset, kafka.consumer_group.offset_sum,
             kafka.partition.current_offset, kafka.partition.oldest_offset,
             kafka.partition.replicas, kafka.partition.replicas_in_sync,
             kafka.topic.partitions

Also bulk-indexes co-located partition metric documents each tick so panels
that AND-filter current_offset+oldest_offset or replicas+replicas_in_sync
are populated — OTLP intake stores one metric per document.

Field names and attribute keys come from:
  - opentelemetry-collector-contrib/receiver/kafkametricsreceiver/metadata.yaml
  - elastic/integrations/packages/kafka_otel/kibana/dashboard/*.json

Usage (standalone):
    python3 -m generators.kafka_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/kafka_otel@0.3.1"

import logging
import os
import random
import signal
import threading
import time
from typing import Any

from app.telemetry import ESBulkClient, OTLPClient
from app.config import NAMESPACE
from generator_engine.db_common import (
    CounterState,
    build_cumulative_sum,
    build_gauge,
    build_signal_resource,
    business_load,
    resolve_active_chaos,
)

# ── Grounding constants — do not change names ─────────────────────────────────

METRIC_NAMES = (
    "kafka.brokers",
    "kafka.consumer_group.lag",
    "kafka.consumer_group.lag_sum",
    "kafka.consumer_group.members",
    "kafka.consumer_group.offset",
    "kafka.consumer_group.offset_sum",
    "kafka.partition.current_offset",
    "kafka.partition.oldest_offset",
    "kafka.partition.replicas",
    "kafka.partition.replicas_in_sync",
    "kafka.topic.partitions",
)

KAFKA_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkametricsreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("kafka-generator")

BATCH_INTERVAL_MIN = 10
BATCH_INTERVAL_MAX = 20

# Replicas per partition: all brokers replicate, one is the leader.
REPLICATION_FACTOR = 3


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_msk",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


class KafkaMetricState(CounterState):
    """Cumulative counter offsets for Kafka partitions."""

    def __init__(self, rng: random.Random, topics: list[str], partitions_per_topic: int):
        super().__init__(rng)
        # current_offset[topic][partition] — increases monotonically
        self.current_offset: dict[str, list[int]] = {
            t: [rng.randint(100_000, 50_000_000) for _ in range(partitions_per_topic)]
            for t in topics
        }
        # Sticky ISR counts. Replication Health KPI / detail / by-topic panels
        # use MAX(replicas_in_sync) over the whole time window — intermittent
        # under-replication is wiped out by healthy samples (MAX → RF), so
        # tick_replicas keeps a fault sticky while active. Healthy baseline
        # starts (and stays) at full RF for every partition.
        self.replicas_in_sync: dict[str, list[int]] = {
            t: [REPLICATION_FACTOR] * partitions_per_topic for t in topics
        }
        # consumer offsets trail the partition offset
        # cg_offset[group][topic][partition]
        self.cg_offset: dict[str, dict[str, list[int]]] = {}

    def init_consumer_group(self, group: str, topics: list[str], partitions_per_topic: int, rng: random.Random):
        self.cg_offset[group] = {
            t: [
                max(0, self.current_offset[t][p] - rng.randint(0, 1_000))
                for p in range(partitions_per_topic)
            ]
            for t in topics
        }

    def tick(self, rng: random.Random, active_chaos: dict | None = None):
        load = business_load()
        for topic in self.current_offset:
            for p in range(len(self.current_offset[topic])):
                delta = int(rng.randint(50, 5_000) * load)
                self.current_offset[topic][p] += delta

    def tick_replicas(self, rng: random.Random, active_chaos: dict | None = None):
        """Widen sticky under-replication while a replica fault is active;
        recover to full RF once the fault clears so the alert can resolve.
        """
        c = active_chaos or {}
        ur_key = float(c.get("under_replicated_partitions", 0.0))
        fault = max(0.25, ur_key) if ur_key > 0 else 0.0
        if fault <= 0:
            for syncs in self.replicas_in_sync.values():
                for p in range(len(syncs)):
                    syncs[p] = REPLICATION_FACTOR
            return
        for topic, syncs in self.replicas_in_sync.items():
            for p in range(len(syncs)):
                if syncs[p] < REPLICATION_FACTOR:
                    continue
                if rng.random() < fault:
                    syncs[p] = rng.randint(1, REPLICATION_FACTOR - 1)

    def tick_consumers(self, rng: random.Random, active_chaos: dict | None = None):
        """Advance consumer offsets, but slower when there is a fault (lag grows)."""
        c = active_chaos or {}
        lag_by_topic = float(c.get("high_consumer_lag_by_topic", 0.0))
        lag_per_part = float(c.get("high_consumer_lag_per_partition", 0.0))
        empty_groups = float(c.get("empty_consumer_groups", 0.0))
        lag_factor = 1.0
        if lag_by_topic > 50000:
            lag_factor = max(lag_factor, lag_by_topic / 50000.0)
        if lag_per_part > 10000:
            lag_factor = max(lag_factor, lag_per_part / 10000.0)
        if empty_groups > 0:
            return
        for group in self.cg_offset:
            for topic in self.cg_offset[group]:
                for p in range(len(self.cg_offset[group][topic])):
                    max_advance = self.current_offset[topic][p] - self.cg_offset[group][topic][p]
                    if max_advance <= 0:
                        continue
                    # Divide the available gap by the lag factor — higher factor means slower catch-up
                    advance = int(max_advance / max(1.0, lag_factor * rng.uniform(0.8, 1.2)))
                    self.cg_offset[group][topic][p] += max(0, advance)


def _resource_attr_map(resource: dict) -> dict[str, Any]:
    """Flatten OTLP resource attributes; drop data_stream.* (set on the doc root)."""
    out: dict[str, Any] = {}
    for attr in resource.get("attributes", []):
        key = attr["key"]
        if key.startswith("data_stream."):
            continue
        val = attr["value"]
        if "stringValue" in val:
            out[key] = val["stringValue"]
        elif "intValue" in val:
            out[key] = int(val["intValue"])
        elif "doubleValue" in val:
            out[key] = float(val["doubleValue"])
        elif "boolValue" in val:
            out[key] = bool(val["boolValue"])
    return out


def build_partition_metrics_doc(
    resource: dict,
    *,
    topic: str,
    partition: str,
    current_offset: int,
    oldest_offset: int,
    replicas: int,
    replicas_in_sync: int,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc with all per-partition gauges co-located.

    Topics & Partitions "Partition offset details" AND-filters
    current_offset + oldest_offset. Replication Health panels AND-filter
    replicas + replicas_in_sync. OTLP stores one metric per document, so
    those filters never match OTLP-only data.
    """
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "1",
        "data_stream": {
            "type": "metrics",
            "dataset": "kafkametricsreceiver.otel",
            "namespace": "default",
        },
        "attributes": {"topic": topic, "partition": str(partition)},
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": KAFKA_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "kafka.partition.current_offset": int(current_offset),
            "kafka.partition.oldest_offset": int(oldest_offset),
            "kafka.partition.replicas": int(replicas),
            "kafka.partition.replicas_in_sync": int(replicas_in_sync),
        },
    }


def build_cluster_overview_doc(
    resource: dict,
    *,
    brokers: int,
    topic_partitions: list[tuple[str, int]] | None = None,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """ES bulk companion for Overview ``kafka.brokers`` when OTLP fails."""
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "1",
        "data_stream": {
            "type": "metrics",
            "dataset": "kafkametricsreceiver.otel",
            "namespace": "default",
        },
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": KAFKA_SCOPE, "version": SCOPE_VERSION},
        "metrics": {"kafka.brokers": int(brokers)},
    }


def build_topic_partitions_doc(
    resource: dict,
    topic: str,
    partitions: int,
    *,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "1",
        "data_stream": {
            "type": "metrics",
            "dataset": "kafkametricsreceiver.otel",
            "namespace": "default",
        },
        "attributes": {"topic": topic},
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": KAFKA_SCOPE, "version": SCOPE_VERSION},
        "metrics": {"kafka.topic.partitions": int(partitions)},
    }


def build_consumer_group_docs(
    resource: dict,
    rows: list[dict[str, Any]],
    *,
    timestamp_ms: int | None = None,
) -> list[dict[str, Any]]:
    """ES bulk companions for consumer-group Overview / Consumer Groups panels."""
    ts = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
    docs: list[dict[str, Any]] = []
    for row in rows:
        attrs = {k: str(v) for k, v in row.items() if k not in ("metrics",)}
        docs.append({
            "@timestamp": ts,
            "unit": "1",
            "data_stream": {
                "type": "metrics",
                "dataset": "kafkametricsreceiver.otel",
                "namespace": "default",
            },
            "attributes": attrs,
            "resource": {"attributes": _resource_attr_map(resource)},
            "scope": {"name": KAFKA_SCOPE, "version": SCOPE_VERSION},
            "metrics": row["metrics"],
        })
    return docs


def _generate_metrics(
    state: KafkaMetricState,
    topics: list[str],
    consumer_groups: list[str],
    partitions_per_topic: int,
    broker_count: int,
    rng: random.Random,
    active_chaos: dict | None = None,
) -> tuple[list[dict], list[dict[str, Any]], int, list[dict[str, Any]]]:
    """Return ``(otlp_metrics, partition_bulk_rows, brokers, consumer_bulk_rows)``.

    Each ``partition_bulk_rows`` entry holds the per-partition values needed
    to build a co-located ES document (see :func:`build_partition_metrics_doc`).
    ``consumer_bulk_rows`` feeds Overview / Consumer Groups ES bulk companions
    when OTLP metrics never land on the demo ES.
    """
    state.tick(rng, active_chaos)
    for group in consumer_groups:
        if group not in state.cg_offset:
            state.init_consumer_group(group, topics, partitions_per_topic, rng)
    state.tick_consumers(rng, active_chaos)
    state.tick_replicas(rng, active_chaos)
    metrics: list[dict] = []
    partition_rows: list[dict[str, Any]] = []
    consumer_rows: list[dict[str, Any]] = []

    # ── Broker count ──
    # OOTB alert: MAX(kafka.brokers) < 2. GT may pass a large probe value
    # (e.g. 20.0) meaning "activate", not an absolute broker count — any
    # positive chaos value must emit below the threshold.
    c = active_chaos or {}
    broker_override = float(c.get("broker_count_below_threshold", 0.0) or 0.0)
    if broker_override > 0:
        emit_brokers = int(broker_override) if broker_override < 2 else 1
    else:
        emit_brokers = broker_count
    metrics.append(build_gauge("kafka.brokers", "{brokers}", emit_brokers))
    lag_by_topic = float(c.get("high_consumer_lag_by_topic", 0.0))
    lag_per_part = float(c.get("high_consumer_lag_per_partition", 0.0))

    # ── Per-topic partition metrics ──
    for topic in topics:
        topic_attr = {"topic": topic}
        metrics.append(build_gauge("kafka.topic.partitions", "{partitions}", partitions_per_topic, topic_attr))

        for p in range(partitions_per_topic):
            tp_attr = {"topic": topic, "partition": str(p)}
            current_off = state.current_offset[topic][p]
            oldest_off = max(0, current_off - rng.randint(100_000, 10_000_000))
            replicas = REPLICATION_FACTOR
            in_sync = state.replicas_in_sync[topic][p]
            metrics.append(build_gauge("kafka.partition.current_offset", "1", current_off, tp_attr))
            metrics.append(build_gauge("kafka.partition.oldest_offset", "1", oldest_off, tp_attr))
            metrics.append(build_gauge("kafka.partition.replicas", "{replicas}", replicas, tp_attr))
            metrics.append(build_gauge("kafka.partition.replicas_in_sync", "{replicas}", in_sync, tp_attr))
            partition_rows.append({
                "topic": topic,
                "partition": str(p),
                "current_offset": current_off,
                "oldest_offset": oldest_off,
                "replicas": replicas,
                "replicas_in_sync": in_sync,
            })

    # ── Per-consumer-group metrics ──
    for group in consumer_groups:
        if group not in state.cg_offset:
            state.init_consumer_group(group, topics, partitions_per_topic, rng)
        grp_attr = {"group": group}
        empty_groups = float(c.get("empty_consumer_groups", 0.0))
        member_count = 0 if empty_groups > 0 else rng.randint(1, 5)
        metrics.append(build_gauge("kafka.consumer_group.members", "{members}", member_count, grp_attr))
        consumer_rows.append({
            "group": group,
            "metrics": {"kafka.consumer_group.members": int(member_count)},
        })

        for topic in topics:
            topic_grp_attr = {"group": group, "topic": topic}
            lag_sum = 0

            for p in range(partitions_per_topic):
                tgp_attr = {"group": group, "topic": topic, "partition": str(p)}
                current_off = state.current_offset[topic][p]
                cg_off = state.cg_offset[group][topic][p]
                lag = max(0, current_off - cg_off)
                if lag_per_part > 10000:
                    lag = max(lag, int(lag_per_part))
                if lag_by_topic > 50000:
                    lag = max(lag, int(lag_by_topic / max(1, partitions_per_topic)))
                lag_sum += lag

                metrics.append(build_gauge("kafka.consumer_group.lag", "1", lag, tgp_attr))
                metrics.append(build_gauge("kafka.consumer_group.offset", "1", cg_off, tgp_attr))
                consumer_rows.append({
                    "group": group,
                    "topic": topic,
                    "partition": str(p),
                    "metrics": {
                        "kafka.consumer_group.lag": int(lag),
                        "kafka.consumer_group.offset": int(cg_off),
                    },
                })

            metrics.append(build_gauge("kafka.consumer_group.lag_sum", "1", lag_sum, topic_grp_attr))
            cg_offset_sum = sum(state.cg_offset[group][topic])
            metrics.append(build_gauge("kafka.consumer_group.offset_sum", "1", cg_offset_sum, topic_grp_attr))
            consumer_rows.append({
                "group": group,
                "topic": topic,
                "metrics": {
                    "kafka.consumer_group.lag_sum": int(lag_sum),
                    "kafka.consumer_group.offset_sum": int(cg_offset_sum),
                },
            })

    return metrics, partition_rows, emit_brokers, consumer_rows


from generators.kafka_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop

def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run Kafka generator loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {})
    else:
        ns = NAMESPACE
        cfg = {}

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    ident = ScenarioIdentity.from_scenario(scenario_data, ns)
    service_name = ident.service("kafka-broker")
    host_name = ident.host("kafka-broker")
    cluster = ident.label(cfg.get("cluster") or "events")
    broker_count: int = cfg.get("broker_count", 3)
    partitions_per_topic: int = cfg.get("partitions_per_topic", 6)

    topics = [ident.label(name) for name in (cfg.get("topics") or [
        "events-created", "events-processed", "events-failed",
    ])]
    consumer_groups = [ident.label(name) for name in (cfg.get("consumer_groups") or [
        "app-consumer", "analytics-consumer",
    ])]

    cloud = cfg.get("cloud") or _default_cloud()


    resource = build_signal_resource(
        ns=ns,
        signal_type="metrics",
        dataset="kafkametricsreceiver",
        service_name=service_name,
        service_version="3.6.1",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name,
        cloud=cloud,
        extra_attrs={
            "server.address": host_name,
            "server.port": 9092,
            "messaging.system": "kafka",
            "kafka.cluster.alias": cluster,
        },
    )

    metric_state = KafkaMetricState(rng, topics, partitions_per_topic)
    for group in consumer_groups:
        metric_state.init_consumer_group(group, topics, partitions_per_topic, rng)

    scope = {"name": KAFKA_SCOPE, "version": SCOPE_VERSION}
    es_bulk = ESBulkClient()

    logger.info(
        "Kafka generator started (namespace=%s, topics=%s, groups=%s)",
        ns, topics, consumer_groups,
    )

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="kafka_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        metrics, partition_rows, emit_brokers, consumer_rows = _generate_metrics(
            metric_state, topics, consumer_groups,
            partitions_per_topic, broker_count, rng, active_chaos,
        )
        client.send_metrics_scoped(resource, scope, metrics)
        if es_bulk.configured:
            ts_ms = int(time.time() * 1000)
            docs = [
                build_cluster_overview_doc(
                    resource, brokers=emit_brokers, topic_partitions=[], timestamp_ms=ts_ms,
                ),
            ]
            docs.extend(
                build_topic_partitions_doc(
                    resource, topic, partitions_per_topic, timestamp_ms=ts_ms,
                )
                for topic in topics
            )
            if partition_rows:
                docs.extend(
                    build_partition_metrics_doc(resource, timestamp_ms=ts_ms, **row)
                    for row in partition_rows
                )
            if consumer_rows:
                docs.extend(build_consumer_group_docs(resource, consumer_rows, timestamp_ms=ts_ms))
            es_bulk.send_bulk("metrics-kafkametricsreceiver.otel-default", docs)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    es_bulk.close()
    logger.info("Kafka generator stopped.")


def main() -> None:  # pragma: no cover
    """Standalone entry point for local testing."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "60"))

    def _stop(sig, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    client = OTLPClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
