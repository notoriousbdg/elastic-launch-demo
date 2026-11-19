#!/usr/bin/env python3
"""RabbitMQ Metrics Generator — sends RabbitMQ OTel receiver metrics via OTLP.

Emits data matching the rabbitmq_otel content package
(discovery.datasets: rabbitmqreceiver.otel), which populates the bundled
Overview, Queues, and Nodes dashboards.

Queue metrics and node metrics use separate OTLP resources because the
dashboards filter on ``resource.attributes.rabbitmq.queue.name``,
``resource.attributes.rabbitmq.vhost.name``, and
``resource.attributes.rabbitmq.node.name`` (not datapoint attributes).

Also bulk-indexes co-located node utilization documents each tick so panels
that AND-filter used+limit/total (memory, FD, socket, proc) are populated —
OTLP intake stores one metric per document.

Metric names verified against:
  - opentelemetry-collector-contrib/receiver/rabbitmqreceiver/metadata.yaml
  - elastic/integrations/packages/rabbitmq_otel/kibana/dashboard/*.json

Usage (standalone):
    python3 -m generators.rabbitmq_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/rabbitmq_otel@0.3.0"

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
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

# Intersection of receiver metrics and dashboard-queried fields only.
METRIC_NAMES = (
    "rabbitmq.consumer.count",
    "rabbitmq.message.acknowledged",
    "rabbitmq.message.current",
    "rabbitmq.message.delivered",
    "rabbitmq.message.published",
    "rabbitmq.node.channel_created_details.rate",
    "rabbitmq.node.connection_created_details.rate",
    "rabbitmq.node.disk_free",
    "rabbitmq.node.disk_free_alarm",
    "rabbitmq.node.fd_total",
    "rabbitmq.node.fd_used",
    "rabbitmq.node.mem_alarm",
    "rabbitmq.node.mem_limit",
    "rabbitmq.node.mem_used",
    "rabbitmq.node.proc_total",
    "rabbitmq.node.proc_used",
    "rabbitmq.node.sockets_total",
    "rabbitmq.node.sockets_used",
)

RABBITMQ_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/rabbitmqreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("rabbitmq-generator")
BATCH_INTERVAL_MIN = 10
BATCH_INTERVAL_MAX = 30

NODE_MEMORY_LIMIT = 4 * 1024 * 1024 * 1024  # 4 GiB
NODE_FD_TOTAL = 65536
NODE_SOCKET_TOTAL = 58893
NODE_PROC_TOTAL = 1048576
NODE_DISK_FREE_INITIAL = 50 * 1024 * 1024 * 1024  # 50 GiB


def _default_cloud() -> dict:
    return {
        "provider": "gcp",
        "platform": "gcp_compute_engine",
        "region": "us-central1",
        "availability_zone": "us-central1-b",
    }


class RabbitMQQueueState(CounterState):
    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.published = rng.randint(10_000, 5_000_000)
        # Delivered tracks published with a small ready backlog.
        self.ready = rng.randint(0, 500)
        self.delivered = max(0, self.published - self.ready)
        self.unacked = rng.randint(0, 50)
        # Acked trails delivered by the messages still in flight (unacked).
        self.acknowledged = max(0, self.delivered - self.unacked)
        self.consumers = rng.randint(1, 10)

    def tick(
        self,
        rng: random.Random,
        active_chaos: dict | None = None,
    ) -> None:
        load = business_load()
        pub_delta = int(rng.randint(10, 2_000) * load)
        self.published += pub_delta
        # Delivered tracks published; backlog faults come from the declared keys below.
        self.delivered += pub_delta
        lag = max(0, self.published - self.delivered)
        chaos = active_chaos or {}
        ready_override = int(impact_value(chaos, "high_ready_queue_depth", default=0) or 0)
        unack_override = int(impact_value(chaos, "high_unacknowledged_message_count", default=0) or 0)
        # OOTB: ready_count > 0 AND consumer_count == 0. GT probe is a positive
        # severity/ready hint (e.g. 75.0), NOT a consumer count — never assign it
        # to consumers.
        zero_cons_probe = float(
            impact_value(chaos, "queue_with_zero_consumers_and_ready_messages", default=0) or 0
        )
        self.ready = ready_override if ready_override > 0 else max(0, int(lag))
        prev_unacked = self.unacked
        self.unacked = unack_override if unack_override > 0 else rng.randint(0, 50)
        # Consumers ack what they were delivered; a growing unacked backlog is
        # exactly the deliveries that were not acknowledged this interval.
        # Healthy in-flight churn (0–50) is not counted against acks so the
        # package SLO (acked/delivered >= 99.5%) holds on the baseline.
        backlog_growth = max(0, self.unacked - prev_unacked) if unack_override > 0 else 0
        self.acknowledged += max(0, pub_delta - backlog_growth)
        if zero_cons_probe > 0:
            self.consumers = 0
            if self.ready <= 0:
                self.ready = max(75, int(zero_cons_probe) if zero_cons_probe >= 1 else 75)
        else:
            self.consumers = max(1, int(rng.randint(1, 10) * load))


class RabbitMQNodeState(CounterState):
    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.disk_free = NODE_DISK_FREE_INITIAL
        self.mem_used = int(NODE_MEMORY_LIMIT * rng.uniform(0.2, 0.5))
        self.fd_used = rng.randint(50, 2_000)
        self.sockets_used = rng.randint(10, 500)
        self.proc_used = rng.randint(200, 5_000)
        self.conn_rate = rng.uniform(0.5, 20.0)
        self.channel_rate = rng.uniform(1.0, 50.0)

    def tick(self, rng: random.Random, active_chaos: dict | None = None) -> None:
        load = business_load()
        chaos = active_chaos or {}
        # OOTB Disk alarm: disk_free_alarm=1 when disk_free < 2GiB.
        # Healthy must stay well above that floor; only chaos dips below.
        if float(impact_value(chaos, "disk_alarm_triggered", default=0.0) or 0.0) > 0:
            self.disk_free = 500 * 1024 * 1024  # below 2GiB alarm floor
        else:
            # Re-seed if a prior fault left disk under the alarm threshold.
            if self.disk_free < 2 * 1024 * 1024 * 1024:
                self.disk_free = rng.randint(50, 200) * 1024 * 1024 * 1024
            else:
                self.disk_free = max(
                    20 * 1024 * 1024 * 1024,
                    self.disk_free - rng.randint(0, 1_000_000),
                )
        mem_frac = rng.uniform(0.2, 0.6)
        if float(impact_value(chaos, "memory_alarm_triggered", default=0.0) or 0.0) > 0:
            mem_frac = 0.95
        self.mem_used = int(NODE_MEMORY_LIMIT * mem_frac)
        self.fd_used = int(rng.randint(50, 2_000) * load)
        self.sockets_used = int(rng.randint(10, 500) * load)
        self.proc_used = int(rng.randint(200, 5_000) * load)
        self.conn_rate = round(rng.uniform(0.5, 20.0) * load, 2)
        self.channel_rate = round(rng.uniform(1.0, 50.0) * load, 2)


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


_METRICS_DATA_STREAM = "metrics-rabbitmqreceiver.otel-default"

# Co-located node gauges must map as gauge_long. Plain ESBulk create (no
# dynamic_templates) maps them as long without time_series_metric, and TS
# LAST_OVER_TIME panels that mix those fields with OTLP gauges (mem_alarm /
# disk_free) return zero rows — Node Resource Summary shows "No results found".
_NODE_GAUGE_FIELDS = (
    "rabbitmq.node.mem_used",
    "rabbitmq.node.mem_limit",
    "rabbitmq.node.mem_alarm",
    "rabbitmq.node.disk_free",
    "rabbitmq.node.disk_free_alarm",
    "rabbitmq.node.fd_used",
    "rabbitmq.node.fd_total",
    "rabbitmq.node.sockets_used",
    "rabbitmq.node.sockets_total",
    "rabbitmq.node.proc_used",
    "rabbitmq.node.proc_total",
)
_NODE_GAUGE_DYNAMIC_TEMPLATES = {
    f"metrics.{name}": "gauge_long" for name in _NODE_GAUGE_FIELDS
}


def build_node_utilization_doc(
    resource: dict,
    state: RabbitMQNodeState,
    *,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc with node gauges co-located for Nodes dashboard panels.

    Memory / FD / Socket / Proc utilization panels AND-filter both halves of
    each pair (e.g. ``mem_used IS NOT NULL AND mem_limit IS NOT NULL``). OTLP
    stores one metric per document, so those filters never match OTLP-only
    data. Node Resource Summary also needs mem_alarm / disk_* on the same
    series as mem_used when TS LAST_OVER_TIME mixes fields.
    """
    mem_alarm = 1 if state.mem_used > NODE_MEMORY_LIMIT * 0.9 else 0
    disk_alarm = 1 if state.disk_free < 2 * 1024 * 1024 * 1024 else 0
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "1",
        "data_stream": {
            "type": "metrics",
            "dataset": "rabbitmqreceiver.otel",
            "namespace": "default",
        },
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": RABBITMQ_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "rabbitmq.node.mem_used": int(state.mem_used),
            "rabbitmq.node.mem_limit": int(NODE_MEMORY_LIMIT),
            "rabbitmq.node.mem_alarm": int(mem_alarm),
            "rabbitmq.node.disk_free": int(state.disk_free),
            "rabbitmq.node.disk_free_alarm": int(disk_alarm),
            "rabbitmq.node.fd_used": int(state.fd_used),
            "rabbitmq.node.fd_total": int(NODE_FD_TOTAL),
            "rabbitmq.node.sockets_used": int(state.sockets_used),
            "rabbitmq.node.sockets_total": int(NODE_SOCKET_TOTAL),
            "rabbitmq.node.proc_used": int(state.proc_used),
            "rabbitmq.node.proc_total": int(NODE_PROC_TOTAL),
        },
    }


def _node_gauges_need_mapping_reset(elastic_url: str, api_key: str) -> bool:
    """True when any node gauge lacks time_series_metric=gauge or is plain long.

    Node Resource Summary mixes mem/disk/fd/socket fields in one
    ``LAST_OVER_TIME``. Plain ``long`` (no gauge annotation) from older bulk
    docs poisons the panel even when mem_alarm/disk_free are healthy gauges.
    """
    import httpx

    field_list = ",".join(f"metrics.{name}" for name in _NODE_GAUGE_FIELDS)
    try:
        resp = httpx.get(
            f"{elastic_url.rstrip('/')}/{_METRICS_DATA_STREAM}/_field_caps",
            params={"fields": field_list},
            headers={"Authorization": f"ApiKey {api_key}"},
            timeout=15,
        )
    except Exception as exc:
        logger.warning("Could not inspect %s field caps: %s", _METRICS_DATA_STREAM, exc)
        return False
    if resp.status_code == 404:
        return False
    if resp.status_code >= 300:
        logger.warning(
            "field_caps for %s failed (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )
        return False

    fields = resp.json().get("fields") or {}
    for name in _NODE_GAUGE_FIELDS:
        caps = fields.get(f"metrics.{name}") or fields.get(name) or {}
        for type_meta in caps.values():
            if not isinstance(type_meta, dict):
                continue
            mtype = type_meta.get("type")
            tsm = type_meta.get("time_series_metric")
            if tsm == "counter" or mtype == "counter_long":
                return True
            # Missing gauge annotation, or plain long without gauge → poison.
            if tsm != "gauge":
                return True
    return False


def _reset_poisoned_node_gauge_mapping() -> None:
    """Delete the metrics data stream when bulk docs poisoned gauge mappings."""
    try:
        from app.telemetry import ESBulkClient
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for rabbitmq mapping check: %s", exc)
        return

    client = ESBulkClient()
    if not client.configured:
        return
    if not _node_gauges_need_mapping_reset(client.elastic_url, client.api_key):
        logger.info(
            "rabbitmq.node.* gauges already have time_series_metric=gauge; leaving %s alone",
            _METRICS_DATA_STREAM,
        )
        return

    import httpx

    try:
        resp = httpx.delete(
            f"{client.elastic_url.rstrip('/')}/_data_stream/{_METRICS_DATA_STREAM}",
            headers={
                "Authorization": f"ApiKey {client.api_key}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )
    except Exception as exc:
        logger.warning("Could not delete %s: %s", _METRICS_DATA_STREAM, exc)
        return

    if resp.status_code < 300:
        logger.info(
            "Deleted %s so rabbitmq.node.* gauges remap with time_series_metric",
            _METRICS_DATA_STREAM,
        )
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist", _METRICS_DATA_STREAM)
    else:
        logger.warning(
            "Could not delete %s: %s %s",
            _METRICS_DATA_STREAM,
            resp.status_code,
            resp.text[:200],
        )


def _generate_node_metrics(state: RabbitMQNodeState) -> list[dict]:
    """Node-level gauges queried by the Nodes and Overview dashboards."""
    mem_alarm = 1 if state.mem_used > NODE_MEMORY_LIMIT * 0.9 else 0
    disk_alarm = 1 if state.disk_free < 2 * 1024 * 1024 * 1024 else 0
    return [
        build_gauge("rabbitmq.node.mem_used", "{bytes}", state.mem_used),
        build_gauge("rabbitmq.node.mem_limit", "{bytes}", NODE_MEMORY_LIMIT),
        build_gauge("rabbitmq.node.mem_alarm", "{status}", mem_alarm),
        build_gauge("rabbitmq.node.disk_free", "{bytes}", state.disk_free),
        build_gauge("rabbitmq.node.disk_free_alarm", "{status}", disk_alarm),
        build_gauge("rabbitmq.node.fd_used", "{fd}", state.fd_used),
        build_gauge("rabbitmq.node.fd_total", "{fd}", NODE_FD_TOTAL),
        build_gauge("rabbitmq.node.sockets_used", "{sockets}", state.sockets_used),
        build_gauge("rabbitmq.node.sockets_total", "{sockets}", NODE_SOCKET_TOTAL),
        build_gauge("rabbitmq.node.proc_used", "{proc}", state.proc_used),
        build_gauge("rabbitmq.node.proc_total", "{proc}", NODE_PROC_TOTAL),
        build_gauge(
            "rabbitmq.node.connection_created_details.rate",
            "{connections}/s",
            state.conn_rate,
        ),
        build_gauge(
            "rabbitmq.node.channel_created_details.rate",
            "{channels}/s",
            state.channel_rate,
        ),
    ]


def _generate_queue_metrics(qstate: RabbitMQQueueState) -> list[dict]:
    """Queue-level metrics queried by the Overview and Queues dashboards."""
    return [
        build_gauge("rabbitmq.consumer.count", "{consumers}", qstate.consumers),
        # message.current uses the datapoint attribute ``state`` (ready|unacknowledged)
        build_gauge(
            "rabbitmq.message.current",
            "{messages}",
            qstate.ready,
            {"state": "ready"},
        ),
        build_gauge(
            "rabbitmq.message.current",
            "{messages}",
            qstate.unacked,
            {"state": "unacknowledged"},
        ),
        build_cumulative_sum(
            "rabbitmq.message.published",
            "{messages}",
            qstate.published,
            qstate.start_time_ns,
        ),
        build_cumulative_sum(
            "rabbitmq.message.delivered",
            "{messages}",
            qstate.delivered,
            qstate.start_time_ns,
        ),
        # Receiver sum (monotonic, cumulative per queue); package SLO
        # "Message acknowledgment rate" reads max−min against delivered.
        build_cumulative_sum(
            "rabbitmq.message.acknowledged",
            "{messages}",
            qstate.acknowledged,
            qstate.start_time_ns,
        ),
    ]



from generators.rabbitmq_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


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

    ident = ScenarioIdentity.from_scenario(scenario_data, ns)
    service_name = ident.service("rabbitmq")
    host_name = ident.host("rabbitmq")
    cloud = cfg.get("cloud") or _default_cloud()
    raw_vhost = cfg.get("vhost")
    vhost = "/" if raw_vhost == "/" else ident.label(raw_vhost or "vhost")
    node_name = cfg.get("node_name") or f"rabbit@{host_name}"

    queue_names = [ident.label(name) for name in (cfg.get("queue_names") or [
        "orders", "events", "notifications",
    ])]

    queue_states = {q: RabbitMQQueueState(rng) for q in queue_names}
    node_state = RabbitMQNodeState(rng)



    # Shared identity for every resource; per-entity attrs added below.
    base_resource_kwargs = dict(
        ns=ns,
        signal_type="metrics",
        dataset=otel_dataset("rabbitmqreceiver"),
        service_name=service_name,
        service_version="3.12.0",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name,
        cloud=cloud,
    )
    scope = {"name": RABBITMQ_SCOPE, "version": SCOPE_VERSION}
    # Secondary transport for co-located used+limit/total utilization docs.
    es_bulk = ESBulkClient()
    _reset_poisoned_node_gauge_mapping()
    logger.info(
        "RabbitMQ metrics generator started (namespace=%s, node=%s, queues=%d)",
        ns,
        node_name,
        len(queue_names),
    )

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="rabbitmq_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )

        node_state.tick(rng, active_chaos)
        node_resource = build_signal_resource(
            **base_resource_kwargs,
            extra_attrs={"rabbitmq.node.name": node_name},
        )
        client.send_metrics_scoped(
            node_resource, scope, _generate_node_metrics(node_state)
        )
        if es_bulk.configured:
            if es_bulk.consecutive_failures >= es_bulk.max_failures_before_backoff:
                logger.warning(
                    "Clearing ESBulk backoff so rabbitmq node co-location can retry"
                )
                es_bulk.consecutive_failures = 0
            es_bulk.send_bulk(
                _METRICS_DATA_STREAM,
                [build_node_utilization_doc(node_resource, node_state)],
                dynamic_templates=_NODE_GAUGE_DYNAMIC_TEMPLATES,
            )

        for qname, qstate in queue_states.items():
            qstate.tick(rng, active_chaos)
            queue_resource = build_signal_resource(
                **base_resource_kwargs,
                extra_attrs={
                    "rabbitmq.node.name": node_name,
                    "rabbitmq.queue.name": qname,
                    "rabbitmq.vhost.name": vhost,
                },
            )
            client.send_metrics_scoped(
                queue_resource, scope, _generate_queue_metrics(qstate)
            )

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    es_bulk.close()
    logger.info("RabbitMQ metrics generator stopped.")


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
