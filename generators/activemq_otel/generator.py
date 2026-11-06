#!/usr/bin/env python3
"""ActiveMQ Generator — sends ActiveMQ broker/JVM/destination metrics via OTLP.

Emits data matching the activemq_otel content package
(discovery.datasets: activemq.otel), which populates the bundled
Overview, Broker Health, and Destinations dashboards.

Metric names are grounded in the package dashboard ES|QL panels (authoritative
for this content pack). The upstream JMX Scraper ``activemq.yaml`` covers a
subset; dashboards also query broker aggregates, utilization ratios, JVM
metrics, blocked sends, and inflight counts.

AND-filtered panels (memory usage+limit) need co-located bulk documents —
OTLP stores one metric per document.

Usage (standalone):
    python3 -m generators.activemq_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/activemq_otel@0.3.1"

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
    impact_value,
    CounterState,
    build_cumulative_sum,
    build_gauge,
    build_non_monotonic_sum,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

# Intersection of dashboard-queried fields (overview + broker-health + destinations).
METRIC_NAMES = (
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
)

# JMX Scraper / jmx-metrics instrumentation scope (package docs).
ACTIVEMQ_SCOPE = "io.opentelemetry.contrib.jmxmetrics"
SCOPE_VERSION = "1.53.0"

logger = logging.getLogger("activemq-generator")
EMIT_INTERVAL_S = 15.0

MEMORY_LIMIT_BYTES = 1024 * 1024 * 1024  # 1 GiB broker memory limit
STORE_LIMIT_BYTES = 50 * 1024 * 1024 * 1024  # 50 GiB
TEMP_LIMIT_BYTES = 10 * 1024 * 1024 * 1024  # 10 GiB
HEAP_MAX_BYTES = 512 * 1024 * 1024
FD_MAX = 65536

_DEST_NAMES = (
    "ActiveMQ.Advisory.Queue",
    "ActiveMQ.DLQ",
    "orders.queue",
    "payments.queue",
    "notifications.queue",
    "inventory.queue",
    "audit.queue",
    "deadletter.queue",
    "retry.queue",
)

_GC_NAMES = ("G1 Young Generation", "G1 Old Generation")


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_ec2",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


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


class _BrokerState(CounterState):
    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.connections = rng.randint(20, 120)
        self.memory_util = rng.uniform(0.15, 0.55)
        self.store_util = rng.uniform(0.10, 0.45)
        self.temp_util = rng.uniform(0.02, 0.20)
        self.broker_messages = rng.randint(100, 5_000)
        self.enqueue = CounterState(rng, initial=rng.randint(100_000, 5_000_000))
        self.dequeue = CounterState(rng, initial=rng.randint(100_000, 5_000_000))
        self.blocked_sends = CounterState(rng, initial=0)
        self.heap_used = int(HEAP_MAX_BYTES * rng.uniform(0.3, 0.7))
        self.cpu = rng.uniform(0.05, 0.35)
        self.threads = rng.randint(30, 80)
        self.fd_open = rng.randint(200, 2_000)
        self.gc = {name: CounterState(rng, initial=rng.randint(100, 5_000)) for name in _GC_NAMES}

    def tick(self, rng: random.Random, queue_depth_mult: float, load: float) -> None:
        self.connections = max(1, int(rng.randint(20, 150) * load))
        self.memory_util = min(0.98, max(0.05, rng.uniform(0.15, 0.55) * queue_depth_mult))
        self.store_util = min(0.98, max(0.05, rng.uniform(0.10, 0.45) * queue_depth_mult))
        self.temp_util = min(0.95, max(0.01, rng.uniform(0.02, 0.20) * load))
        self.broker_messages = max(0, int(rng.randint(50, 8_000) * queue_depth_mult * load))
        enq = max(1, int(rng.randint(50, 500) * load))
        deq = max(1, int(enq * rng.uniform(0.85, 1.05)))
        self.enqueue.value += enq
        self.dequeue.value += deq
        blocked = int(rng.randint(0, 5) * max(0.0, queue_depth_mult - 1.0))
        self.blocked_sends.value += blocked
        self.heap_used = int(HEAP_MAX_BYTES * min(0.95, self.memory_util * rng.uniform(0.9, 1.1)))
        self.cpu = min(0.99, max(0.02, rng.uniform(0.05, 0.40) * load))
        self.threads = max(10, int(rng.randint(30, 90) * load))
        self.fd_open = max(50, int(rng.randint(200, 3_000) * load))
        for counter in self.gc.values():
            counter.value += rng.randint(0, 3)


class _DestState(CounterState):
    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.consumers = rng.randint(1, 8)
        self.producers = rng.randint(1, 4)
        # DLQ alert: LAST(activemq.message.current) > 100 on ActiveMQ.DLQ.
        self.current = 0
        self.enqueued = CounterState(rng, initial=rng.randint(10_000, 500_000))
        self.dequeued = CounterState(rng, initial=rng.randint(10_000, 500_000))
        self.expired = CounterState(rng, initial=rng.randint(0, 200))
        self.blocked = CounterState(rng, initial=0)
        self.inflight = rng.randint(0, 50)
        self.wait_ms = rng.uniform(0.5, 40.0)

    def tick(
        self,
        rng: random.Random,
        queue_depth_mult: float,
        consumer_drop: float,
        load: float,
    ) -> None:
        enq = max(1, int(rng.randint(5, 120) * load))
        self.enqueued.value += enq
        # Under depth pressure, dequeue lags.
        deq = max(0, int(enq / max(1.0, queue_depth_mult) * rng.uniform(0.7, 1.0)))
        self.dequeued.value += deq
        self.current = max(0, int(self.current + enq - deq))
        # OOTB queue-depth alert: LAST(message.current) > 1000. Healthy deq
        # lag (~15%) would climb forever past the threshold — clamp when not
        # under depth pressure (queue_depth_mult <= 1).
        if queue_depth_mult <= 1.0:
            self.current = min(self.current, rng.randint(20, 200))
        self.expired.value += rng.randint(0, 3 if queue_depth_mult > 2 else 1)
        self.blocked.value += int(rng.randint(0, 2) * max(0.0, queue_depth_mult - 1.0))
        self.inflight = max(0, int(rng.randint(0, 80) * load))
        self.wait_ms = round(rng.uniform(0.5, 50.0) * max(1.0, queue_depth_mult * 0.5), 2)
        self.producers = max(0, int(rng.randint(1, 4) * load))
        if rng.random() < consumer_drop:
            self.consumers = 0
        else:
            self.consumers = max(0, int(rng.randint(1, 8) * load))


def build_memory_limit_doc(
    resource: dict,
    *,
    usage_bytes: int,
    limit_bytes: int,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc with memory.usage + memory.limit co-located.

    Broker Health "Memory Usage vs Limit" AND-filters both metrics. OTLP
    stores one metric per document, so those filters never match OTLP-only data.
    """
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "By",
        "data_stream": {
            "type": "metrics",
            "dataset": "activemq.otel",
            "namespace": "default",
        },
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": ACTIVEMQ_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "activemq.memory.usage": int(usage_bytes),
            "activemq.memory.limit": int(limit_bytes),
        },
    }


def build_heap_util_doc(
    resource: dict,
    *,
    heap_used: int,
    heap_max: int,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc with heap.used + heap.max co-located.

    The OOTB heap alert ANDs both fields before STATS; OTLP one-metric-per-doc
    never satisfies that filter, so co-locate via ES bulk.
    """
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "By",
        "data_stream": {
            "type": "metrics",
            "dataset": "activemq.otel",
            "namespace": "default",
        },
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": ACTIVEMQ_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "activemq.jvm.memory.heap.used": int(heap_used),
            "activemq.jvm.memory.heap.max": int(heap_max),
        },
    }


def _as_ratio(value: float) -> float:
    """Normalize impact values to a 0–1 ratio.

    GT channels historically used percent-like values (80.0 / 85.0). OOTB alerts
    compare against ratios (e.g. ``> 0.85``), so map percent → ratio when needed.
    """
    v = float(value or 0.0)
    if v > 1.0:
        v = v / 100.0
    return max(0.0, min(0.99, v))


def _emit_broker(broker_name: str, state: _BrokerState) -> list[dict]:
    attrs = {"activemq.broker.name": broker_name}
    mem_usage = int(MEMORY_LIMIT_BYTES * state.memory_util)
    store_pct = round(state.store_util * 100.0, 1)
    temp_pct = round(state.temp_util * 100.0, 1)
    metrics: list[dict] = [
        build_non_monotonic_sum(
            "activemq.broker.connection.current", "{connections}", state.connections, state.start_time_ns, attrs,
        ),
        build_non_monotonic_sum(
            "activemq.connection.count", "{connections}", state.connections, state.start_time_ns, attrs,
        ),
        build_gauge("activemq.memory.utilization", "1", round(state.memory_util, 4), attrs),
        build_gauge("activemq.store.utilization", "1", round(state.store_util, 4), attrs),
        build_gauge("activemq.temp.utilization", "1", round(state.temp_util, 4), attrs),
        build_gauge("activemq.memory.usage", "By", mem_usage, attrs),
        build_gauge("activemq.memory.limit", "By", MEMORY_LIMIT_BYTES, attrs),
        build_gauge("activemq.store.limit", "By", STORE_LIMIT_BYTES, attrs),
        build_gauge("activemq.temp.limit", "By", TEMP_LIMIT_BYTES, attrs),
        build_gauge("activemq.disk.store_usage", "%", store_pct, attrs),
        build_gauge("activemq.disk.temp_usage", "%", temp_pct, attrs),
        build_non_monotonic_sum(
            "activemq.broker.message.current", "{messages}", state.broker_messages, state.start_time_ns, attrs,
        ),
        build_cumulative_sum(
            "activemq.broker.enqueue.count", "{messages}",
            state.enqueue.value, state.enqueue.start_time_ns, attrs,
        ),
        build_cumulative_sum(
            "activemq.broker.dequeue.count", "{messages}",
            state.dequeue.value, state.dequeue.start_time_ns, attrs,
        ),
        # Do not emit broker-level activemq.queue.blocked.sends (destination=null).
        # OOTB alert groups BY destination.name; a null series creates sticky
        # group instances that linger after RATE returns to 0. Destination
        # emit covers the alert under chaos only (see _emit_destination).
        # JVM heap.used/max are emitted via build_heap_util_doc (co-located bulk)
        # so the OOTB AND-filter alert can see both fields on one document.
        build_gauge("activemq.jvm.cpu.process.usage", "1", round(state.cpu, 4), attrs),
        build_non_monotonic_sum(
            "activemq.jvm.thread.count", "{threads}", state.threads, state.start_time_ns, attrs,
        ),
        build_non_monotonic_sum(
            "activemq.jvm.file_descriptor.open", "{files}", state.fd_open, state.start_time_ns, attrs,
        ),
        build_gauge("activemq.jvm.file_descriptor.max", "{files}", FD_MAX, attrs),
    ]
    for gc_name, counter in state.gc.items():
        gc_attrs = {**attrs, "gc.name": gc_name}
        metrics.append(
            build_cumulative_sum(
                "activemq.jvm.gc.collections", "{collections}",
                counter.value, counter.start_time_ns, gc_attrs,
            )
        )
    return metrics


def _emit_destination(
    broker_name: str,
    dest_name: str,
    dest_type: str,
    state: _DestState,
    *,
    emit_blocked_sends: bool = False,
) -> list[dict]:
    # Emit both names: OTel JMX Scraper uses messaging.destination.name
    # (semconv); package dashboards COALESCE(activemq.destination.name,
    # messaging.destination.name). ES|QL rejects COALESCE when either
    # column is absent from the index mapping.
    attrs = {
        "activemq.broker.name": broker_name,
        "activemq.destination.name": dest_name,
        "messaging.destination.name": dest_name,
        "activemq.destination.type": dest_type,
    }
    metrics: list[dict] = [
        build_non_monotonic_sum(
            "activemq.consumer.count", "{consumers}", state.consumers, state.start_time_ns, attrs,
        ),
        build_non_monotonic_sum(
            "activemq.producer.count", "{producers}", state.producers, state.start_time_ns, attrs,
        ),
        build_non_monotonic_sum(
            "activemq.message.current", "{messages}", state.current, state.start_time_ns, attrs,
        ),
        build_cumulative_sum(
            "activemq.message.enqueued", "{messages}",
            state.enqueued.value, state.enqueued.start_time_ns, attrs,
        ),
        build_cumulative_sum(
            "activemq.message.dequeued", "{messages}",
            state.dequeued.value, state.dequeued.start_time_ns, attrs,
        ),
        build_cumulative_sum(
            "activemq.message.expired", "{messages}",
            state.expired.value, state.expired.start_time_ns, attrs,
        ),
        build_gauge("activemq.message.wait_time.avg", "ms", state.wait_ms, attrs),
    ]
    # Always emit for queue destinations (not Advisory topics) so the field
    # exists in the index mapping — ES|QL raises "Unknown column" at compile time
    # when the field has never appeared in any doc. RATE of a constant-zero
    # cumulative counter = 0, so the OOTB blocked-sends alert stays quiet at rest.
    if "Advisory" not in dest_name:
        metrics.append(
            build_cumulative_sum(
                "activemq.queue.blocked.sends", "{messages}",
                state.blocked.value, state.blocked.start_time_ns, attrs,
            )
        )
    if dest_type == "queue":
        metrics.append(
            build_non_monotonic_sum(
                "activemq.queue.message.inflight", "{messages}", state.inflight, state.start_time_ns, attrs,
            )
        )
    else:
        metrics.append(
            build_non_monotonic_sum(
                "activemq.topic.message.inflight", "{messages}", state.inflight, state.start_time_ns, attrs,
            )
        )
    return metrics



from generators.activemq_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def _resolve_merged_chaos(
    chaos_controller,
    channel_registry: dict,
    *,
    chaos_mode: str,
    impact_keys: dict,
) -> dict | None:
    """Merge ``generator_impact.activemq_otel`` across all active channels.

    ``resolve_active_chaos`` historically returned only the first matching
    channel, which zeroed sibling impact_keys when multiple GT channels were
    active. Prefer the shared helper when it already merges (``channel_ids``),
    otherwise merge locally so PVC-mounted generator code works without an
    image rebuild.
    """
    active_chaos = resolve_active_chaos(
        chaos_controller=chaos_controller,
        channel_registry=channel_registry,
        generator_id="activemq_otel",
        chaos_mode=chaos_mode,
        impact_keys=impact_keys,
    )
    if active_chaos and "channel_ids" in active_chaos:
        return active_chaos
    if not chaos_controller:
        return active_chaos

    matched: list[tuple[int, dict, dict]] = []
    for ch_id in chaos_controller.get_active_channels():
        ch = channel_registry.get(ch_id)
        if not ch:
            continue
        gen_impact = (ch.get("generator_impact") or {}).get("activemq_otel")
        if chaos_mode == "explicit" and gen_impact is None:
            continue
        matched.append((ch_id, ch, gen_impact or {}))
    if not matched:
        return None

    primary_id, primary_ch, _ = matched[0]
    raw_impact: dict = {}
    for _, _, impact in matched:
        raw_impact.update(impact)

    from scenario_engine.fault_spec import resolve_dict

    defaulted = {
        k: raw_impact.get(k, v["default"])
        for k, v in impact_keys.items()
    }
    try:
        resolved = resolve_dict(defaulted) if raw_impact else defaulted
    except Exception:
        resolved = defaulted

    return {
        "channel_id": primary_id,
        "channel_ids": [ch_id for ch_id, _, _ in matched],
        "name": primary_ch.get("name"),
        "subsystem": primary_ch.get("subsystem"),
        "error_type": primary_ch.get("error_type"),
        "affected_services": primary_ch.get("affected_services", []),
        **resolved,
    }


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    rng = random.Random()
    ns = scenario_data["namespace"] if scenario_data else NAMESPACE
    cfg = scenario_data.get("generator_config", {}) if scenario_data else {}
    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("activemq")
    broker_name = cfg.get("broker_name") or ScenarioIdentity(ns).label("broker")
    dest_count: int = min(cfg.get("destination_count", 4), len(_DEST_NAMES))
    destinations = _DEST_NAMES[:dest_count]



    cloud = cfg.get("cloud") or _default_cloud()
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("activemq")
    resource = build_signal_resource(
        ns=ns,
        signal_type="metrics",
        dataset=otel_dataset("activemq"),
        service_name=service_name,
        service_version="5.18.3",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name,
        cloud=cloud,
        extra_attrs={"activemq.broker.name": broker_name},
    )
    scope = {"name": ACTIVEMQ_SCOPE, "version": SCOPE_VERSION}

    broker_state = _BrokerState(rng)
    dest_states = {d: _DestState(rng) for d in destinations}
    try:
        es_bulk = ESBulkClient()
    except Exception as exc:
        es_bulk = None
        logger.warning("ESBulkClient unavailable (%s); co-located docs disabled", exc)

    logger.info("ActiveMQ generator started (namespace=%s, broker=%s)", ns, broker_name)

    def _emit_tick() -> None:
        active_chaos = _resolve_merged_chaos(
            chaos_controller,
            _channel_registry,
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        chaos = active_chaos or {}
        blocked_sends_detected = float(impact_value(chaos, "blocked_sends_detected", default=0.0) or 0.0)
        mem_util = _as_ratio(float(impact_value(chaos, "broker_memory_utilization_high", default=0.0) or 0.0))
        store_util = _as_ratio(float(impact_value(chaos, "persistent_store_utilization_high", default=0.0) or 0.0))
        jvm_cpu = _as_ratio(float(impact_value(chaos, "high_jvm_cpu_utilization", default=0.0) or 0.0))
        heap_util = _as_ratio(float(impact_value(chaos, "high_jvm_heap_utilization", default=0.0) or 0.0))
        dlq_depth = int(impact_value(chaos, "dead_letter_queue_depth_high", default=0) or 0)
        queue_depth_high = int(impact_value(chaos, "queue_depth_high", default=0) or 0)
        load = business_load()
        # Scale tick volume from absolute queue_depth_high when set.
        queue_depth_factor = max(1.0, queue_depth_high / 100.0) if queue_depth_high > 0 else 1.0

        broker_state.tick(rng, queue_depth_factor * load, load)
        if mem_util > 0:
            broker_state.memory_util = mem_util
        if store_util > 0:
            broker_state.store_util = store_util
        if jvm_cpu > 0:
            broker_state.cpu = jvm_cpu
        if heap_util > 0:
            # OOTB alert does ``EVAL heap_util = heap_used / heap_max`` on long
            # gauges — ES|QL integer-divides to 0 unless used >= max. Emit a
            # full heap so the ratio becomes 1 and clears ``> 0.85``.
            broker_state.heap_used = HEAP_MAX_BYTES
        emit_blocked = blocked_sends_detected > 0
        if emit_blocked:
            broker_state.blocked_sends.value += int(blocked_sends_detected)
        elif broker_state.blocked_sends.value:
            # Chaos cleared — reset so the next activation starts a clean tip.
            broker_state.blocked_sends = CounterState(rng, initial=0)
        metrics: list[dict] = []
        metrics.extend(_emit_broker(broker_name, broker_state))
        # consumer_drop is a probability; blocked_sends impact is a rate count.
        consumer_drop = 0.85 if emit_blocked else 0.0
        for dest_name, state in dest_states.items():
            dest_type = "topic" if "Advisory" in dest_name else "queue"
            state.tick(rng, queue_depth_factor * load, consumer_drop, load)
            if queue_depth_high > 0 and dest_type == "queue" and "DLQ" not in dest_name and "Advisory" not in dest_name:
                state.current = queue_depth_high
            if dlq_depth > 0 and dest_name == "ActiveMQ.DLQ":
                state.current = dlq_depth
            elif dest_name == "ActiveMQ.DLQ":
                # OOTB DLQ alert: LAST(message.current) > 100 — keep healthy depth low.
                state.current = min(state.current, 10)
            if emit_blocked:
                state.blocked.value += int(blocked_sends_detected)
            elif state.blocked.value:
                state.blocked = CounterState(rng, initial=0)
            metrics.extend(
                _emit_destination(
                    broker_name, dest_name, dest_type, state,
                    emit_blocked_sends=emit_blocked,
                )
            )

        client.send_metrics_scoped(resource, scope, metrics)

        if es_bulk is not None and getattr(es_bulk, "configured", False):
            usage_bytes = int(MEMORY_LIMIT_BYTES * broker_state.memory_util)
            ts_ms = int(time.time() * 1000)
            es_bulk.send_bulk(
                "metrics-activemq.otel-default",
                [
                    build_memory_limit_doc(
                        resource,
                        usage_bytes=usage_bytes,
                        limit_bytes=MEMORY_LIMIT_BYTES,
                        timestamp_ms=ts_ms,
                    ),
                    build_heap_util_doc(
                        resource,
                        heap_used=broker_state.heap_used,
                        heap_max=HEAP_MAX_BYTES,
                        # Distinct timestamp avoids TSDB _id collision with the
                        # memory co-located doc written in the same cycle.
                        timestamp_ms=ts_ms + 1,
                    ),
                ],
            )
        else:
            # Fallback when bulk is unavailable: still emit heap via OTLP
            # (alert will not fire until co-located docs are available).
            client.send_metrics_scoped(
                resource,
                scope,
                [
                    build_gauge(
                        "activemq.jvm.memory.heap.used", "By", broker_state.heap_used,
                        {"activemq.broker.name": broker_name},
                    ),
                    build_gauge(
                        "activemq.jvm.memory.heap.max", "By", HEAP_MAX_BYTES,
                        {"activemq.broker.name": broker_name},
                    ),
                ],
            )

    run_tick_loop(stop_event, _emit_tick, interval=lambda: EMIT_INTERVAL_S)

    if es_bulk is not None:
        es_bulk.close()
    logger.info("ActiveMQ generator stopped.")


def main() -> None:  # pragma: no cover
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
