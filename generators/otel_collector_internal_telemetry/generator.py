#!/usr/bin/env python3
"""OTel Collector internal telemetry generator — sends ``otelcol_*`` metrics via OTLP.

Models a small collector fleet reporting its own telemetry (the
``service::telemetry::metrics`` OTLP reader): one gateway collector plus
``agent_count`` agent collectors that forward traces, metrics, and logs to it.

Emits data matching the otel_collector_internal_telemetry content package
(discovery.datasets: collectortelemetry.otel), which populates the Overview,
Traces, Metrics, Logs, and both Tail sampling processor dashboards, and the
three "Exporter send failed …" alert rule templates.

Series (datapoint attributes in brackets):
  - receiver accepted/refused spans|metric_points|log_records [receiver, transport]
  - scraper scraped/errored metric_points                     [receiver, scraper]
  - processor incoming/outgoing items                         [processor, otel.signal]
  - exporter sent/send_failed spans|metric_points|log_records [exporter]
  - exporter queue size / capacity                            [exporter, data_type]
  - process uptime, cpu seconds, memory RSS, runtime heap / total alloc / sys memory
  - tail sampling (gateway): count_traces_sampled [policy, decision, sampled],
    global_count_traces_sampled [decision, sampled], new_trace_id_received,
    sampling_traces_on_memory, sampling_trace_dropped_too_early,
    sampling_policy_evaluation_error

Counter vs gauge types follow the collector's own metadata: every ``*_spans``,
``*_metric_points``, ``*_log_records``, ``*_items``, cpu seconds, uptime and
total alloc bytes is a cumulative monotonic sum; queue size/capacity, RSS,
heap alloc, sys memory and traces-on-memory are gauges.

Usage (standalone):
    python3 -m generators.otel_collector_internal_telemetry.generator
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/otel_collector_internal_telemetry@1.2.3"

import logging
import os
import random
import signal
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from app.config import NAMESPACE
from app.telemetry import OTLPClient
from generator_engine.db_common import (
    CounterState,
    build_cumulative_sum,
    build_gauge,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)
from generator_engine.identity import ScenarioIdentity

# ── Grounding constants — do not change names ─────────────────────────────────

SIGNALS = ("traces", "metrics", "logs")
# signal → item suffix used in receiver/exporter metric names
ITEM_SUFFIX = {"traces": "spans", "metrics": "metric_points", "logs": "log_records"}

METRIC_NAMES = (
    "otelcol_receiver_accepted_spans",
    "otelcol_receiver_refused_spans",
    "otelcol_receiver_accepted_metric_points",
    "otelcol_receiver_refused_metric_points",
    "otelcol_receiver_accepted_log_records",
    "otelcol_receiver_refused_log_records",
    "otelcol_scraper_scraped_metric_points",
    "otelcol_scraper_errored_metric_points",
    "otelcol_processor_incoming_items",
    "otelcol_processor_outgoing_items",
    "otelcol_exporter_sent_spans",
    "otelcol_exporter_send_failed_spans",
    "otelcol_exporter_sent_metric_points",
    "otelcol_exporter_send_failed_metric_points",
    "otelcol_exporter_sent_log_records",
    "otelcol_exporter_send_failed_log_records",
    "otelcol_exporter_queue_size",
    "otelcol_exporter_queue_capacity",
    "otelcol_process_uptime",
    "otelcol_process_cpu_seconds",
    "otelcol_process_memory_rss",
    "otelcol_process_runtime_heap_alloc_bytes",
    "otelcol_process_runtime_total_alloc_bytes",
    "otelcol_process_runtime_total_sys_memory_bytes",
    "otelcol_processor_tail_sampling_count_traces_sampled",
    "otelcol_processor_tail_sampling_global_count_traces_sampled",
    "otelcol_processor_tail_sampling_new_trace_id_received",
    "otelcol_processor_tail_sampling_sampling_traces_on_memory",
    "otelcol_processor_tail_sampling_sampling_trace_dropped_too_early",
    "otelcol_processor_tail_sampling_sampling_policy_evaluation_error",
)

SCOPE_VERSION = "0.121.0"
SCOPE_RECEIVER = "go.opentelemetry.io/collector/receiver/receiverhelper"
SCOPE_SCRAPER = "go.opentelemetry.io/collector/scraper/scraperhelper"
SCOPE_PROCESSOR = "go.opentelemetry.io/collector/processor/processorhelper"
SCOPE_EXPORTER = "go.opentelemetry.io/collector/exporter/exporterhelper"
SCOPE_SERVICE = "go.opentelemetry.io/collector/service"
SCOPE_TAIL_SAMPLING = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"
)

# Datapoint attribute keys (collector internal telemetry).
ATTR_RECEIVER = "receiver"
ATTR_TRANSPORT = "transport"
ATTR_SCRAPER = "scraper"
ATTR_PROCESSOR = "processor"
ATTR_SIGNAL = "otel.signal"
ATTR_EXPORTER = "exporter"
ATTR_DATA_TYPE = "data_type"
ATTR_POLICY = "policy"
ATTR_DECISION = "decision"
ATTR_SAMPLED = "sampled"

QUEUE_CAPACITY = 1000          # exporterhelper sending_queue default (batches)
HEALTHY_QUEUE_FILL = (0.0, 0.06)
GATEWAY_FAIL_EXPORTER = "otlp/elastic"

# Tail-sampling policies on the gateway: (policy, sampled share, dropped share).
TAIL_POLICIES = (
    ("errors", 0.03, 0.0),
    ("slow-traces", 0.08, 0.0),
    ("probabilistic-10pct", 0.10, 0.0),
    ("drop-health-checks", 0.0, 0.15),
)
SPANS_PER_TRACE = 12.0
DECISION_WAIT_S = 30.0

logger = logging.getLogger("otel-collector-internal-telemetry-generator")

BATCH_INTERVAL_MIN = 10
BATCH_INTERVAL_MAX = 15


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_eks",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


@dataclass
class CollectorInstance:
    """One collector process and its pipeline components."""

    role: str                      # "gateway" | "agent"
    service_name: str
    instance_id: str
    host_name: str
    # (receiver, transport or None, signals)
    receivers: list[tuple[str, str | None, tuple[str, ...]]]
    # (receiver, scraper)
    scrapers: list[tuple[str, str]]
    # (processor, signals)
    processors: list[tuple[str, tuple[str, ...]]]
    # (exporter, signals)
    exporters: list[tuple[str, tuple[str, ...]]]
    # per-signal items/sec arriving at this collector
    base_rate: dict[str, float] = field(default_factory=dict)
    rss_base: float = 0.0
    cpu_cores: float = 0.0


def build_fleet(ident: ScenarioIdentity, rng: random.Random, agent_count: int = 2) -> list[CollectorInstance]:
    """One gateway plus ``agent_count`` agents; names come from ScenarioIdentity."""
    fleet: list[CollectorInstance] = []
    gw_service = ident.service("otel-gateway")
    fleet.append(CollectorInstance(
        role="gateway",
        service_name=gw_service,
        instance_id=ident.instance("otel-gateway", 1),
        host_name=ident.host("otel-gateway", 1),
        receivers=[
            ("otlp", "grpc", SIGNALS),
            ("otlp", "http", SIGNALS),
        ],
        scrapers=[],
        processors=[
            ("memory_limiter", SIGNALS),
            ("tail_sampling", ("traces",)),
            ("batch", SIGNALS),
        ],
        exporters=[
            (GATEWAY_FAIL_EXPORTER, SIGNALS),
            ("otlphttp/archive", ("logs",)),
        ],
        rss_base=rng.uniform(700e6, 1.0e9),
        cpu_cores=rng.uniform(1.0, 1.8),
    ))
    for i in range(1, max(1, int(agent_count)) + 1):
        fleet.append(CollectorInstance(
            role="agent",
            service_name=ident.service("otel-agent"),
            instance_id=ident.instance("otel-agent", i),
            host_name=ident.host("otel-agent", i),
            receivers=[
                ("otlp", "grpc", SIGNALS),
                ("hostmetrics", None, ("metrics",)),
                ("kubeletstats", "http", ("metrics",)),
                ("filelog", None, ("logs",)),
            ],
            scrapers=[
                ("hostmetrics", "cpu"),
                ("hostmetrics", "memory"),
                ("hostmetrics", "disk"),
                ("hostmetrics", "filesystem"),
                ("hostmetrics", "network"),
                ("hostmetrics", "load"),
                ("kubeletstats", "kubeletstats"),
            ],
            processors=[
                ("memory_limiter", SIGNALS),
                ("k8sattributes", SIGNALS),
                ("batch", SIGNALS),
            ],
            exporters=[("otlp/gateway", SIGNALS)],
            base_rate={
                "traces": rng.uniform(300.0, 800.0),
                "metrics": rng.uniform(1500.0, 3000.0),
                "logs": rng.uniform(400.0, 1200.0),
            },
            rss_base=rng.uniform(150e6, 300e6),
            cpu_cores=rng.uniform(0.2, 0.6),
        ))
    agents = [c for c in fleet if c.role == "agent"]
    fleet[0].base_rate = {
        s: sum(a.base_rate[s] for a in agents) * rng.uniform(1.2, 1.5) for s in SIGNALS
    }
    return fleet


def fault_levels(active_chaos: dict | None) -> dict[str, float]:
    """Translate impact keys into breach values (0.0 = healthy)."""
    chaos = active_chaos or {}

    def _pos(key: str) -> float:
        try:
            value = float(chaos.get(key) or 0.0)
        except (TypeError, ValueError):
            return 0.0
        return value if value > 0 else 0.0

    failed = {
        "logs": _pos("exporter_send_failed_log_records"),
        "metrics": _pos("exporter_send_failed_metric_points"),
        "traces": _pos("exporter_send_failed_spans"),
    }
    # Any positive key fails at least 5 items/s so RATE() > 0 is unmistakable.
    failed = {s: (max(v, 5.0) if v else 0.0) for s, v in failed.items()}
    queue = _pos("queue_saturation")
    if queue > 1.0:
        queue = queue / 100.0
    if queue:
        queue = min(1.0, max(queue, 0.5))
    return {"failed": failed, "queue_fill": queue}


class CollectorCounterState(CounterState):
    """Cumulative counters for every collector series.

    Traffic counters start near zero so ``RATE()`` charts are not dominated by
    an initial spike (GUARDRAILS #17).  Failure / refusal / error counters
    start at **0** and only grow under the matching impact key, so the OOTB
    ``SUM(RATE(send_failed)) > 0`` alerts stay quiet on the healthy path.
    """

    def __init__(self, rng: random.Random, fleet: list[CollectorInstance]):
        super().__init__(rng, initial=0)
        self.counters: dict[tuple, float] = {}
        self.uptime: dict[str, float] = {c.instance_id: rng.uniform(3_600.0, 5 * 86_400.0) for c in fleet}
        self.rng = rng

    def seed(self, key: tuple, *, zero: bool = False) -> float:
        if key not in self.counters:
            self.counters[key] = 0.0 if zero else float(self.rng.randint(0, 1_000))
        return self.counters[key]

    def add(self, key: tuple, delta: float, *, zero: bool = False) -> float:
        self.seed(key, zero=zero)
        self.counters[key] += max(0.0, float(delta))
        return self.counters[key]


def _sum(name: str, unit: str, value: float, state: CounterState, attrs: dict | None = None) -> dict:
    return build_cumulative_sum(name, unit, int(value), state.start_time_ns, attrs)


def _sum_double(name: str, unit: str, value: float, state: CounterState, attrs: dict | None = None) -> dict:
    return build_cumulative_sum(name, unit, float(round(value, 3)), state.start_time_ns, attrs)


def _generate_instance(
    inst: CollectorInstance,
    state: CollectorCounterState,
    rng: random.Random,
    dt: float,
    faults: dict[str, Any],
    traffic: float,
) -> dict[str, list[dict]]:
    """Return ``{scope_name: [metrics]}`` for one collector instance."""
    iid = inst.instance_id
    is_gw = inst.role == "gateway"
    out: dict[str, list[dict]] = {
        SCOPE_RECEIVER: [], SCOPE_SCRAPER: [], SCOPE_PROCESSOR: [],
        SCOPE_EXPORTER: [], SCOPE_SERVICE: [], SCOPE_TAIL_SAMPLING: [],
    }
    queue_fill = faults["queue_fill"] if is_gw else 0.0
    rate = {s: inst.base_rate[s] * traffic * rng.uniform(0.92, 1.08) for s in SIGNALS}

    # ── Receivers ──
    for s in SIGNALS:
        receivers = [r for r in inst.receivers if s in r[2]]
        weights = [3.0 if r[0] == "otlp" and r[1] == "grpc" else 1.0 for r in receivers]
        total_w = sum(weights) or 1.0
        for (recv, transport, _), w in zip(receivers, weights):
            attrs = {ATTR_RECEIVER: recv}
            if transport:
                attrs[ATTR_TRANSPORT] = transport
            key = (iid, "recv", s, recv, transport)
            accepted = state.add(key + ("accepted",), rate[s] * dt * w / total_w)
            refused_delta = rate[s] * dt * w / total_w * rng.uniform(0.02, 0.06) if queue_fill else 0.0
            refused = state.add(key + ("refused",), refused_delta, zero=True)
            suffix = ITEM_SUFFIX[s]
            out[SCOPE_RECEIVER].append(_sum(f"otelcol_receiver_accepted_{suffix}", "{items}", accepted, state, attrs))
            out[SCOPE_RECEIVER].append(_sum(f"otelcol_receiver_refused_{suffix}", "{items}", refused, state, attrs))

    # ── Scrapers (agents) ──
    scraped_share = rate["metrics"] * 0.25 / max(1, len(inst.scrapers))
    for recv, scraper in inst.scrapers:
        attrs = {ATTR_RECEIVER: recv, ATTR_SCRAPER: scraper}
        key = (iid, "scraper", recv, scraper)
        scraped = state.add(key + ("scraped",), scraped_share * dt * rng.uniform(0.9, 1.1))
        # Rare, tiny scrape errors (e.g. a filesystem mount that vanished).
        errored_delta = rng.choice((0, 0, 0, 0, 1)) if scraper == "filesystem" else 0
        errored = state.add(key + ("errored",), errored_delta, zero=True)
        out[SCOPE_SCRAPER].append(_sum("otelcol_scraper_scraped_metric_points", "{datapoints}", scraped, state, attrs))
        out[SCOPE_SCRAPER].append(_sum("otelcol_scraper_errored_metric_points", "{datapoints}", errored, state, attrs))

    # ── Processors ──
    sample_ratio = 0.21
    for proc, signals in inst.processors:
        for s in signals:
            attrs = {ATTR_PROCESSOR: proc, ATTR_SIGNAL: s}
            incoming = rate[s] * dt
            outgoing = incoming * (sample_ratio if proc == "tail_sampling" else 1.0)
            key = (iid, "proc", proc, s)
            inc = state.add(key + ("in",), incoming)
            outc = state.add(key + ("out",), outgoing)
            out[SCOPE_PROCESSOR].append(_sum("otelcol_processor_incoming_items", "{items}", inc, state, attrs))
            out[SCOPE_PROCESSOR].append(_sum("otelcol_processor_outgoing_items", "{items}", outc, state, attrs))

    # ── Exporters ──
    for exp, signals in inst.exporters:
        for s in signals:
            attrs = {ATTR_EXPORTER: exp}
            volume = rate[s] * dt * (sample_ratio if (is_gw and s == "traces") else 1.0)
            if exp == "otlphttp/archive":
                volume *= 0.4
            failed_rate = faults["failed"][s] if (is_gw and exp == GATEWAY_FAIL_EXPORTER) else 0.0
            failed_delta = failed_rate * dt
            key = (iid, "exp", exp, s)
            sent = state.add(key + ("sent",), max(0.0, volume - failed_delta))
            failed = state.add(key + ("failed",), failed_delta, zero=True)
            suffix = ITEM_SUFFIX[s]
            out[SCOPE_EXPORTER].append(_sum(f"otelcol_exporter_sent_{suffix}", "{items}", sent, state, attrs))
            out[SCOPE_EXPORTER].append(_sum(f"otelcol_exporter_send_failed_{suffix}", "{items}", failed, state, attrs))

            # Sending queue: retries back up the failing signal's queue.
            fill = rng.uniform(*HEALTHY_QUEUE_FILL)
            if is_gw and exp == GATEWAY_FAIL_EXPORTER:
                if queue_fill:
                    fill = min(1.0, queue_fill * rng.uniform(0.97, 1.0))
                elif failed_rate:
                    fill = rng.uniform(0.3, 0.6)
            q_attrs = {ATTR_EXPORTER: exp, ATTR_DATA_TYPE: s}
            out[SCOPE_EXPORTER].append(build_gauge(
                "otelcol_exporter_queue_size", "{batches}", int(round(QUEUE_CAPACITY * fill)), q_attrs,
            ))
            out[SCOPE_EXPORTER].append(build_gauge(
                "otelcol_exporter_queue_capacity", "{batches}", QUEUE_CAPACITY, q_attrs,
            ))

    # ── Process ──
    state.uptime[iid] += dt
    pressure = 1.0 + (0.6 if queue_fill else 0.0)
    rss = inst.rss_base * pressure * rng.uniform(0.95, 1.05)
    cpu = state.add((iid, "cpu"), inst.cpu_cores * traffic * dt * rng.uniform(0.85, 1.15))
    alloc = state.add((iid, "alloc"), rss * 0.08 * traffic * dt * rng.uniform(0.8, 1.2))
    out[SCOPE_SERVICE].extend([
        _sum_double("otelcol_process_uptime", "s", state.uptime[iid], state),
        _sum_double("otelcol_process_cpu_seconds", "s", cpu, state),
        build_gauge("otelcol_process_memory_rss", "By", int(rss)),
        build_gauge("otelcol_process_runtime_heap_alloc_bytes", "By", int(rss * rng.uniform(0.45, 0.6))),
        _sum("otelcol_process_runtime_total_alloc_bytes", "By", alloc, state),
        build_gauge("otelcol_process_runtime_total_sys_memory_bytes", "By", int(rss * rng.uniform(1.1, 1.25))),
    ])

    # ── Tail sampling (gateway) ──
    if is_gw:
        traces = rate["traces"] / SPANS_PER_TRACE * dt
        ts = out[SCOPE_TAIL_SAMPLING]
        new_ids = state.add((iid, "ts", "new"), traces)
        ts.append(_sum("otelcol_processor_tail_sampling_new_trace_id_received", "{traces}", new_ids, state))
        for policy, sampled_share, dropped_share in TAIL_POLICIES:
            shares = {"sampled": sampled_share, "dropped": dropped_share,
                      "not_sampled": 1.0 - sampled_share - dropped_share}
            for decision, share in shares.items():
                if share <= 0:
                    continue
                attrs = {ATTR_POLICY: policy, ATTR_DECISION: decision,
                         ATTR_SAMPLED: "true" if decision == "sampled" else "false"}
                v = state.add((iid, "ts", "policy", policy, decision), traces * share)
                ts.append(_sum("otelcol_processor_tail_sampling_count_traces_sampled", "{traces}", v, state, attrs))
        for decision, share in (("sampled", sample_ratio), ("not_sampled", 1.0 - sample_ratio)):
            attrs = {ATTR_DECISION: decision, ATTR_SAMPLED: "true" if decision == "sampled" else "false"}
            v = state.add((iid, "ts", "global", decision), traces * share)
            ts.append(_sum("otelcol_processor_tail_sampling_global_count_traces_sampled", "{traces}", v, state, attrs))
        on_memory = rate["traces"] / SPANS_PER_TRACE * DECISION_WAIT_S * rng.uniform(0.9, 1.1)
        ts.append(build_gauge("otelcol_processor_tail_sampling_sampling_traces_on_memory", "{traces}",
                              int(on_memory)))
        early_delta = traces * rng.uniform(0.01, 0.03) if queue_fill else 0.0
        early = state.add((iid, "ts", "early"), early_delta, zero=True)
        ts.append(_sum("otelcol_processor_tail_sampling_sampling_trace_dropped_too_early", "{traces}",
                       early, state))
        eval_err = state.add((iid, "ts", "eval_err"), 0.0, zero=True)
        ts.append(_sum("otelcol_processor_tail_sampling_sampling_policy_evaluation_error", "{errors}",
                       eval_err, state))

    return out


def _generate_metrics(
    fleet: list[CollectorInstance],
    state: CollectorCounterState,
    rng: random.Random,
    active_chaos: dict | None,
    dt: float,
    *,
    load: float | None = None,
) -> dict[str, dict[str, list[dict]]]:
    """Return ``{instance_id: {scope: [metrics]}}`` for one reporting interval."""
    faults = fault_levels(active_chaos)
    traffic = 0.5 + 0.5 * (business_load() if load is None else load)
    return {
        inst.instance_id: _generate_instance(inst, state, rng, dt, faults, traffic)
        for inst in fleet
    }


from generators.otel_collector_internal_telemetry import SPEC as _ALERT_SPEC  # noqa: E402
from generator_engine.tick import run_tick_loop  # noqa: E402


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run the collector internal telemetry loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {}) or {}
    else:
        ns = NAMESPACE
        cfg = {}

    channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    ident = ScenarioIdentity.from_scenario(scenario_data, ns)
    fleet = build_fleet(ident, rng, int(cfg.get("agent_count", 2) or 2))
    version = str(cfg.get("collector_version") or SCOPE_VERSION)
    cloud = cfg.get("cloud") or _default_cloud()

    resources = {
        inst.instance_id: build_signal_resource(
            identity=ident,
            signal_type="metrics",
            dataset=otel_dataset("collectortelemetry"),
            service_name=inst.service_name,
            service_version=version,
            service_instance_id=inst.instance_id,
            host_name=inst.host_name,
            cloud=cloud,
            extra_attrs={"collector.mode": inst.role},
        )
        for inst in fleet
    }

    state = CollectorCounterState(rng, fleet)
    last = [time.time()]
    logger.info(
        "OTel collector internal telemetry generator started (namespace=%s, instances=%s)",
        ns, [inst.instance_id for inst in fleet],
    )

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=channel_registry,
            generator_id="otel_collector_internal_telemetry",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        now = time.time()
        dt = min(60.0, max(1.0, now - last[0])) if now > last[0] else float(BATCH_INTERVAL_MIN)
        last[0] = now
        batches = _generate_metrics(fleet, state, rng, active_chaos, dt)
        for iid, scopes in batches.items():
            for scope_name, metrics in scopes.items():
                client.send_metrics_scoped(
                    resources[iid], {"name": scope_name, "version": version}, metrics,
                )

    run_tick_loop(
        stop_event, _emit_tick,
        interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX),
    )
    logger.info("OTel collector internal telemetry generator stopped.")


def main() -> None:  # pragma: no cover
    """Standalone entry point for local testing."""
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(name)s %(levelname)s %(message)s")
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
