#!/usr/bin/env python3
"""Apache HTTP Server Metrics Generator — sends Apache OTel receiver metrics via OTLP.

Emits data matching the apache_otel content package
(discovery.datasets: apachereceiver.otel), which populates the bundled
Apache HTTP Server dashboards.

Signal types:
  - Metrics: Apache receiver metrics from the apachereceiver

Metric names verified against:
  - elastic/integrations/packages/apache_otel/kibana/dashboard/*.json

Source: elastic/integrations packages/apache_otel@0.6.1

Usage (standalone):
    python3 -m generators.apache_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/apache_otel@0.6.1"

import logging
import os
import random
import signal
import threading
from typing import Any

from app.telemetry import OTLPClient, ESBulkClient
from app.config import NAMESPACE
from generator_engine.db_common import (
    CounterState,
    build_cumulative_sum,
    build_gauge,
    build_non_monotonic_sum,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

try:
    from generators.apache_otel import SPEC
except Exception:  # pragma: no cover
    SPEC = None

APACHE_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/apachereceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("apache-generator")
BATCH_INTERVAL_MIN = 10
BATCH_INTERVAL_MAX = 20

METRIC_NAMES = (
    "apache.requests",
    "apache.uptime",
    "apache.request.time",
    "apache.workers",
    "apache.worker.limit",
    "apache.cpu.load",
    "apache.cpu.time",
    "apache.cpu.time.kernelmode",
    "apache.cpu.time.usermode",
    "apache.bytes",
    "apache.traffic",
    "apache.scoreboard",
    "apache.current_connections",
    "apache.connections.total",
    "apache.connections.async",
    "apache.connections.async.closing",
    "apache.connections.async.keep_alive",
    "apache.connections.async.writing",
    "apache.load.1",
    "apache.load.5",
    "apache.load.15",
)

# OOTB restart + High request rate use MAX/MIN — those reject counter_long.
# OOTB No requests received uses RATE(apache.requests) — needs counter_long.
# Default emit requests as gauge (cumulative values); rematerialize to counter
# only while no_requests_received chaos is active.
_METRICS_DATA_STREAM = "metrics-apachereceiver.otel-default"
_UPTIME_GAUGE_FIELDS = ("apache.uptime", "metrics.apache.uptime")
_REQUESTS_FIELDS = ("apache.requests", "metrics.apache.requests")


def _default_cloud() -> dict:
    return {
        "provider": "gcp",
        "platform": "gcp_compute_engine",
        "region": "us-central1",
        "availability_zone": "us-central1-b",
    }


def _field_mapped_as_counter(elastic_url: str, api_key: str, fields: tuple[str, ...]) -> bool:
    """Return True if any named field is stuck as counter_long."""
    import httpx

    url = (
        f"{elastic_url.rstrip('/')}/{_METRICS_DATA_STREAM}/_field_caps"
        f"?fields={','.join(fields)}"
    )
    try:
        resp = httpx.get(
            url,
            headers={
                "Authorization": f"ApiKey {api_key}",
                "Content-Type": "application/json",
            },
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
    caps = resp.json().get("fields", {})
    for name in fields:
        for meta in caps.get(name, {}).values():
            if meta.get("time_series_metric") == "counter" or meta.get("type") == "counter_long":
                return True
    return False


def _delete_metrics_stream(elastic_url: str, api_key: str, reason: str) -> None:
    import httpx

    try:
        resp = httpx.delete(
            f"{elastic_url.rstrip('/')}/_data_stream/{_METRICS_DATA_STREAM}",
            headers={
                "Authorization": f"ApiKey {api_key}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )
    except Exception as exc:
        logger.warning("Could not delete %s: %s", _METRICS_DATA_STREAM, exc)
        return

    if resp.status_code < 300:
        logger.info("Deleted %s (%s)", _METRICS_DATA_STREAM, reason)
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist yet", _METRICS_DATA_STREAM)
    else:
        logger.warning(
            "Could not delete %s (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )


def _reset_uptime_counter_mapping() -> None:
    """Delete metrics stream if apache.uptime is counter_long so MAX/MIN alerts work."""
    try:
        from app.telemetry import ESBulkClient as _Bulk
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for uptime mapping check: %s", exc)
        return

    client = _Bulk()
    if not client.configured:
        return

    if not _field_mapped_as_counter(client.elastic_url, client.api_key, _UPTIME_GAUGE_FIELDS):
        logger.info(
            "apache.uptime is not mapped as counter; leaving %s alone",
            _METRICS_DATA_STREAM,
        )
        return

    _delete_metrics_stream(
        client.elastic_url, client.api_key,
        "apache.uptime remaps as gauge (was counter_long)",
    )


def _ensure_requests_mapping(*, as_counter: bool) -> None:
    """Rematerialize stream so apache.requests matches RATE vs MAX/MIN needs."""
    try:
        from app.telemetry import ESBulkClient as _Bulk
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for requests mapping check: %s", exc)
        return

    client = _Bulk()
    if not client.configured:
        return

    is_counter = _field_mapped_as_counter(
        client.elastic_url, client.api_key, _REQUESTS_FIELDS,
    )
    if as_counter and not is_counter:
        _delete_metrics_stream(
            client.elastic_url, client.api_key,
            "apache.requests remaps as counter for RATE(No requests)",
        )
    elif not as_counter and is_counter:
        _delete_metrics_stream(
            client.elastic_url, client.api_key,
            "apache.requests remaps as gauge for MAX/MIN(High request rate)",
        )


def _resource_attr_map(resource: dict) -> dict[str, Any]:
    """Flatten OTLP resource attributes for an ES bulk companion doc."""
    out: dict[str, Any] = {}
    for attr in resource.get("attributes") or []:
        key = attr.get("key")
        val = attr.get("value") or {}
        if not key:
            continue
        if "stringValue" in val:
            out[key] = val["stringValue"]
        elif "intValue" in val:
            out[key] = int(val["intValue"])
        elif "doubleValue" in val:
            out[key] = float(val["doubleValue"])
        elif "boolValue" in val:
            out[key] = bool(val["boolValue"])
    return out


# apachereceiver scoreboard attribute `state` enum (not busy/idle).
_SCOREBOARD_STATES = (
    "open",
    "waiting",
    "starting",
    "reading",
    "sending",
    "keepalive",
    "dnslookup",
    "closing",
    "logging",
    "finishing",
    "idle_cleanup",
    "unknown",
)


class ApacheState(CounterState):
    def __init__(self, rng: random.Random):
        super().__init__(rng)
        # Start near zero so ES|QL RATE charts are not dominated by a one-time
        # spike from a multi-million initial counter value (looks like flat 0).
        self.requests = rng.randint(0, 500)
        self.bytes_sent = rng.randint(0, 1_000_000)
        # OOTB restart alert: min_uptime < 300 AND (max-min) > 300. Healthy
        # seed must stay >= 300 so startup samples never false-positive.
        self.uptime = rng.randint(3_000, 10_000)
        self.request_time_ms = rng.randint(0, 50_000)
        self.cpu_user = rng.uniform(0.0, 10.0)
        self.cpu_kernel = rng.uniform(0.0, 5.0)

    def tick(self, rng: random.Random, active_chaos: dict | None) -> None:
        load = business_load()
        c = active_chaos or {}
        no_requests = float(c.get("no_requests_received", 0.0) or 0.0)
        high_rate = float(c.get("high_request_rate", 0.0) or 0.0)
        restart_uptime = float(c.get("server_restart_detected", 3000.0))
        # High request rate OOTB: (max-min)/elapsed*60 > 1000. Batches are
        # ~15s → delta/tick ≈ rate/4. Keep healthy well under; any positive
        # chaos key (GT often sets 85.0) must tip far above the threshold.
        if no_requests > 0:
            req_delta = 0
        elif high_rate > 0:
            req_delta = max(3000, int(high_rate) if high_rate > 1000 else 3000)
        else:
            req_delta = int(rng.randint(30, 120) * max(0.5, load))
        self.requests += req_delta
        self.bytes_sent += int(req_delta * rng.randint(500, 50_000))
        # ~1–50ms per request accumulated into apache.request.time
        self.request_time_ms += int(req_delta * rng.uniform(1.0, 50.0))
        if restart_uptime < 300:
            self.uptime = rng.randint(10, max(11, int(restart_uptime)))
        else:
            self.uptime += 15
        self.cpu_user += rng.uniform(0.0, 1.0) * load
        self.cpu_kernel += rng.uniform(0.0, 0.2) * load


def _scoreboard_counts(
    rng: random.Random, max_workers: int, busy: int, idle: int,
) -> dict[str, int]:
    """Distribute workers across apachereceiver scoreboard ``state`` values."""
    open_slots = max(0, max_workers - busy - idle)
    waiting = idle
    # Split busy across active scoreboard states (reading/sending/keepalive…).
    remaining = busy
    parts: dict[str, int] = {s: 0 for s in _SCOREBOARD_STATES}
    parts["open"] = open_slots
    parts["waiting"] = waiting
    active = ("reading", "sending", "keepalive", "closing", "starting", "logging")
    for i, state_name in enumerate(active):
        if remaining <= 0:
            break
        if i == len(active) - 1:
            parts[state_name] = remaining
            remaining = 0
        else:
            take = rng.randint(0, remaining)
            parts[state_name] = take
            remaining -= take
    if remaining:
        parts["sending"] += remaining
    return parts


def _generate_metrics(
    state: ApacheState,
    rng: random.Random,
    max_workers: int,
    active_chaos: dict | None,
) -> list[dict]:
    load = business_load()
    c = active_chaos or {}
    low_idle = float(c.get("low_idle_workers", 100.0))
    high_load = float(c.get("high_server_load", 0.0))
    high_load2 = float(c.get("high_server_load_2", 0.0))
    busy_ratio_key = float(c.get("high_busy_workers_ratio", 0.0))
    worker_exhaust = float(c.get("worker_pool_exhaustion", 0.0))

    if worker_exhaust > 0 or busy_ratio_key > 0.85:
        target_ratio = max(0.81, busy_ratio_key) if busy_ratio_key > 0 else 0.85
        busy = min(max_workers, max(int(max_workers * target_ratio), max_workers - 4))
    elif "low_idle_workers" in c and low_idle < 10:
        # OOTB alert: AVG(idle workers) < 5. GT channels often set the key to
        # the threshold (5.0); clamp to <=4 so the breach is unambiguous.
        idle = max(0, min(4, int(low_idle)))
        busy = max_workers - idle
    else:
        busy = int(max_workers * max(0.05, 0.2 + 0.4 * load))
        busy = min(max_workers - 10, busy)
    idle = max(0, max_workers - busy)

    conn_total = int(rng.randint(10, 200) * load)
    async_closing = int(rng.randint(0, 5))
    async_keep_alive = int(rng.randint(5, 50) * load)
    async_writing = int(rng.randint(1, 20) * load)

    if high_load > 5.0:
        load1 = max(high_load, 5.1)
        load5 = load1 * 0.9
        load15 = load1 * 0.8
    elif high_load2 > 0.8:
        load1 = max(high_load2, 0.81)
        load5 = round(load1 * rng.uniform(0.8, 1.1), 3)
        load15 = round(load1 * rng.uniform(0.7, 1.0), 3)
    else:
        load1 = round(rng.uniform(0.05, 0.4) * load, 3)
        load5 = round(load1 * rng.uniform(0.8, 1.1), 3)
        load15 = round(load1 * rng.uniform(0.7, 1.0), 3)

    scoreboard = _scoreboard_counts(rng, max_workers, busy, idle)

    # Overview dashboard uses RATE(apache.requests) → must be counter_long always.
    requests_metric = build_cumulative_sum(
        "apache.requests", "{requests}", state.requests, state.start_time_ns,
    )

    # apachereceiver: workers/scoreboard/current_connections are non-monotonic
    # cumulative sums; datapoint attr is ``state`` (name_override), not
    # workers.state / scoreboard.state.
    metrics: list[dict] = [
        requests_metric,
        # Gauge: OOTB restart alert uses MAX/MIN(uptime); counters are rejected.
        build_gauge("apache.uptime", "s", float(state.uptime)),
        build_cumulative_sum(
            "apache.request.time", "ms", state.request_time_ms, state.start_time_ns,
        ),
        build_non_monotonic_sum(
            "apache.workers", "{workers}", busy, state.start_time_ns, {"state": "busy"},
        ),
        build_non_monotonic_sum(
            "apache.workers", "{workers}", idle, state.start_time_ns, {"state": "idle"},
        ),
        build_gauge("apache.worker.limit", "{worker}", max_workers),
        build_gauge("apache.cpu.load", "%", round(load * 10.0, 4)),
        build_cumulative_sum(
            "apache.cpu.time", "{jiff}", int(state.cpu_kernel), state.start_time_ns,
            {"level": "self", "mode": "system"},
        ),
        build_cumulative_sum(
            "apache.cpu.time", "{jiff}", int(state.cpu_user), state.start_time_ns,
            {"level": "self", "mode": "user"},
        ),
        build_cumulative_sum(
            "apache.cpu.time", "{jiff}", int(state.cpu_kernel * 0.3), state.start_time_ns,
            {"level": "children", "mode": "system"},
        ),
        build_cumulative_sum(
            "apache.cpu.time", "{jiff}", int(state.cpu_user * 0.4), state.start_time_ns,
            {"level": "children", "mode": "user"},
        ),
        build_cumulative_sum("apache.cpu.time.kernelmode", "jiff", int(state.cpu_kernel), state.start_time_ns),
        build_cumulative_sum("apache.cpu.time.usermode", "jiff", int(state.cpu_user), state.start_time_ns),
        build_cumulative_sum("apache.bytes", "By", state.bytes_sent, state.start_time_ns),
        build_cumulative_sum("apache.traffic", "By", state.bytes_sent, state.start_time_ns),
    ]
    for sb_state, count in scoreboard.items():
        metrics.append(
            build_non_monotonic_sum(
                "apache.scoreboard", "{workers}", count, state.start_time_ns,
                {"state": sb_state},
            )
        )
    metrics.extend([
        build_non_monotonic_sum(
            "apache.current_connections", "{connections}", conn_total, state.start_time_ns,
        ),
        build_cumulative_sum("apache.connections.total", "{connection}", conn_total, state.start_time_ns),
        build_gauge("apache.connections.async", "{connections}", async_writing, {"connection_state": "writing"}),
        build_gauge("apache.connections.async", "{connections}", async_keep_alive, {"connection_state": "keepalive"}),
        build_gauge("apache.connections.async", "{connections}", async_closing, {"connection_state": "closing"}),
        build_gauge("apache.connections.async.closing", "{connections}", async_closing),
        build_gauge("apache.connections.async.keep_alive", "{connections}", async_keep_alive),
        build_gauge("apache.connections.async.writing", "{connections}", async_writing),
        build_gauge("apache.load.1", "%", load1),
        build_gauge("apache.load.5", "%", load5),
        build_gauge("apache.load.15", "%", load15),
    ])
    return metrics



from generators.apache_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop

def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run Apache metrics generator loop until stop_event is set."""
    rng = random.Random()
    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {})
    else:
        ns = NAMESPACE
        cfg = {}

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("apache-httpd")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("apache")
    cloud = cfg.get("cloud") or _default_cloud()

    state = ApacheState(rng)
    max_workers = 256
    impact_keys = SPEC.impact_keys if SPEC is not None else {
        "error_rate": {"type": "float", "default": 0.0, "range": [0.0, 1.0],
                       "doc": "Fraction of requests that return 5xx during fault."},
        "worker_saturation": {"type": "float", "default": 0.0, "range": [0.0, 1.0],
                              "doc": "Fraction of workers in busy state during fault."},
    }

    server_name = cfg.get("apache_server_name") or host_name
    server_port = str(cfg.get("apache_server_port") or "80")
    resource = build_signal_resource(
        ns=ns,
        signal_type="metrics",
        dataset=otel_dataset("apachereceiver"),
        service_name=service_name,
        service_version="2.4.57",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name,
        cloud=cloud,
        extra_attrs={
            "apache.server.name": server_name,
            "apache.server.port": server_port,
        },
    )
    scope = {"name": APACHE_SCOPE, "version": SCOPE_VERSION}
    _reset_uptime_counter_mapping()
    # Ensure apache.requests is counter_long (dashboard uses RATE; delete the
    # data stream if it was previously written as gauge/double).
    _ensure_requests_mapping(as_counter=True)
    es_bulk = ESBulkClient()
    logger.info("Apache metrics generator started (namespace=%s)", ns)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="apache_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        state.tick(rng, active_chaos)
        metrics = _generate_metrics(state, rng, max_workers, active_chaos)
        client.send_metrics_scoped(resource, scope, metrics)
    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    es_bulk.close()
    logger.info("Apache metrics generator stopped.")


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
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
