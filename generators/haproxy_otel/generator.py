#!/usr/bin/env python3
"""HAProxy Metrics Generator — sends HAProxy OTel receiver metrics via OTLP.

Emits data matching the haproxy_otel content package
(discovery.datasets: haproxyreceiver.otel), which populates the bundled
HAProxy Overview / Frontend / Backend / Server dashboards.

Resource attributes (per frontend, backend, or server scrape target):
  haproxy.proxy_name, haproxy.service_name, haproxy.addr

Metric names and types verified against:
  - opentelemetry-collector-contrib/receiver/haproxyreceiver/metadata.yaml
  - elastic/integrations/packages/haproxy_otel/kibana/dashboard/*.json
  - elastic/integrations/packages/haproxy_otel/kibana/alerting_rule_template/*.json

Usage (standalone):
    python3 -m generators.haproxy_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/haproxy_otel@0.3.1"

import logging
import os
import random
import signal
import threading
import time
from typing import Any

from app.telemetry import OTLPClient, ESBulkClient
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

# ── Grounding constants — do not change names ─────────────────────────────────

METRIC_NAMES = (
    "haproxy.active",
    "haproxy.bytes.input",
    "haproxy.bytes.output",
    "haproxy.clients.canceled",
    "haproxy.connections.average_time",
    "haproxy.connections.errors",
    "haproxy.connections.rate",
    "haproxy.connections.retries",
    "haproxy.failed_checks",
    "haproxy.requests.average_time",
    "haproxy.requests.denied",
    "haproxy.requests.errors",
    "haproxy.requests.queued",
    "haproxy.requests.redispatched",
    "haproxy.requests.total",
    "haproxy.responses.average_time",
    "haproxy.responses.denied",
    "haproxy.responses.errors",
    "haproxy.server_selected.total",
    "haproxy.sessions.average",
    "haproxy.sessions.count",
    "haproxy.sessions.limit",
    "haproxy.weight",
)

STATUS_CODES = ("1xx", "2xx", "3xx", "4xx", "5xx", "other")

HAPROXY_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/haproxyreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("haproxy-generator")
BATCH_INTERVAL_MIN = 10
BATCH_INTERVAL_MAX = 30
SERVERS_PER_BACKEND = 3
SESSION_LIMIT = 2000


def _default_cloud() -> dict:
    return {
        "provider": "gcp",
        "platform": "gcp_compute_engine",
        "region": "us-central1",
        "availability_zone": "us-central1-b",
    }


class EntityState(CounterState):
    """Cumulative counters for one HAProxy scrape target (fe / be / server)."""

    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.requests_by_status = {
            code: rng.randint(1_000, 5_000_000) for code in STATUS_CODES
        }
        # INCREASE-watched error counters must seed at 0 — a non-zero seed
        # entering the alert window is counted as growth from 0.
        self.request_errors = 0
        self.request_denied = 0
        self.response_errors = 0
        self.response_denied = 0
        self.connection_errors = 0
        self.connection_retries = 0
        self.requests_redispatched = 0
        self.bytes_in = rng.randint(1_000_000_000, 100_000_000_000)
        self.bytes_out = rng.randint(5_000_000_000, 500_000_000_000)
        self.server_selected = rng.randint(100_000, 50_000_000)
        self.failed_checks = 0
        self.clients_canceled = 0

    def tick(
        self,
        rng: random.Random,
        *,
        conn_errors: int = 0,
        req_errors: int = 0,
        resp_errors: int = 0,
        retries: int = 0,
        check_failures: int = 0,
    ) -> int:
        """Advance one emit cycle. Each fault argument is a per-cycle increment feeding
        exactly one alert; at 0 (healthy) none of the watched counters move."""
        load = business_load()
        delta = int(rng.randint(100, 10_000) * load)

        # Mostly 2xx, with a long tail of other status classes. A healthy proxy
        # serves well under 0.5% 5xx: the package "Request success rate 99.5%"
        # SLO counts every non-5xx class as good, so the baseline 5xx share stays
        # at 0.2% and only response_errors_spike pushes it over.
        weights = {
            "1xx": 0.01,
            "2xx": 0.878,
            "3xx": 0.05,
            "4xx": 0.05,
            "5xx": 0.002,
            "other": 0.01,
        }
        remaining = delta
        for code in STATUS_CODES[:-1]:
            portion = int(delta * weights[code])
            self.requests_by_status[code] += portion
            remaining -= portion
        self.requests_by_status["other"] += max(0, remaining)
        if resp_errors > 0:  # upstream 5xx show up in the status breakdown only
            self.requests_by_status["5xx"] += int(delta * 0.25)

        self.request_errors += req_errors
        self.request_denied += req_errors // 5
        self.response_errors += resp_errors
        self.response_denied += resp_errors // 5
        self.connection_errors += conn_errors
        self.clients_canceled += conn_errors // 3
        self.connection_retries += retries
        self.requests_redispatched += max(0, retries // 2)
        self.failed_checks += check_failures
        self.bytes_in += int(delta * rng.randint(200, 5_000))
        self.bytes_out += int(delta * rng.randint(1_000, 30_000))
        self.server_selected += delta
        return delta


def _base_resource_kwargs(
    ns: str,
    service_name: str,
    host_name: str,
    cloud: dict,
) -> dict[str, Any]:
    return {
        "ns": ns,
        "signal_type": "metrics",
        "dataset": otel_dataset("haproxyreceiver"),
        "service_name": service_name,
        "service_version": "2.8.0",
        "service_instance_id": f"{service_name}-001",
        "host_name": host_name,
        "cloud": cloud,
    }


def _entity_resource(
    base: dict[str, Any],
    *,
    proxy_name: str,
    service_name: str,
    addr: str,
) -> dict[str, Any]:
    return build_signal_resource(
        **base,
        extra_attrs={
            "haproxy.proxy_name": proxy_name,
            "haproxy.service_name": service_name,
            "haproxy.addr": addr,
        },
    )


def _resource_attr_map(resource: dict) -> dict[str, Any]:
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


def build_session_utilization_doc(
    resource: dict,
    *,
    sessions: int,
    limit: int,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Co-locate sessions.count + sessions.limit for OOTB saturation ES|QL."""
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "{sessions}",
        "data_stream": {
            "type": "metrics",
            "dataset": "haproxyreceiver.otel",
            "namespace": "default",
        },
        "attributes": {},
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": HAPROXY_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "haproxy.sessions.count": int(sessions),
            "haproxy.sessions.limit": int(limit),
        },
    }


def _latency_ms(rng: random.Random, load: float, base: float) -> float:
    return round(base * load * rng.uniform(0.7, 1.4), 2)


def _frontend_metrics(
    state: EntityState,
    rng: random.Random,
    load: float,
    *,
    sessions: int | None = None,
) -> list[dict]:
    metrics: list[dict] = []
    for code, value in state.requests_by_status.items():
        metrics.append(
            build_cumulative_sum(
                "haproxy.requests.total",
                "{requests}",
                value,
                state.start_time_ns,
                {"status_code": code},
            )
        )
    session_count = (
        sessions if sessions is not None else int(rng.randint(10, 500) * load)
    )
    metrics.extend(
        [
            build_gauge(
                "haproxy.connections.rate",
                "{connections}",
                int(rng.randint(5, 300) * load),
            ),
            build_cumulative_sum(
                "haproxy.requests.errors",
                "{errors}",
                state.request_errors,
                state.start_time_ns,
            ),
            build_cumulative_sum(
                "haproxy.requests.denied",
                "{requests}",
                state.request_denied,
                state.start_time_ns,
            ),
            build_cumulative_sum(
                "haproxy.bytes.input", "By", state.bytes_in, state.start_time_ns,
            ),
            build_cumulative_sum(
                "haproxy.bytes.output", "By", state.bytes_out, state.start_time_ns,
            ),
            build_gauge(
                "haproxy.sessions.count",
                "{sessions}",
                session_count,
            ),
            build_gauge("haproxy.sessions.limit", "{sessions}", SESSION_LIMIT),
            build_gauge(
                "haproxy.sessions.average",
                "ms",
                _latency_ms(rng, load, 25.0),
            ),
            build_gauge(
                "haproxy.responses.average_time",
                "ms",
                _latency_ms(rng, load, 18.0),
            ),
        ]
    )
    return metrics


def _backend_metrics(
    state: EntityState,
    rng: random.Random,
    load: float,
    *,
    active_servers: int,
    queued: int = 0,
    connection_time_ms: float = 0.0,
    queue_time_ms: float = 0.0,
    response_time_ms: float = 0.0,
    sessions: int | None = None,
) -> list[dict]:
    session_count = (
        sessions if sessions is not None else int(rng.randint(5, 400) * load)
    )
    metrics: list[dict] = [
        build_gauge("haproxy.active", "{servers}", active_servers),
        build_gauge("haproxy.requests.queued", "{requests}", queued),
        build_gauge(
            "haproxy.sessions.count",
            "{sessions}",
            session_count,
        ),
        build_gauge("haproxy.sessions.limit", "{sessions}", SESSION_LIMIT),
        build_gauge(
            "haproxy.sessions.average",
            "ms",
            _latency_ms(rng, load, 40.0),
        ),
        build_gauge(
            "haproxy.responses.average_time",
            "ms",
            response_time_ms if response_time_ms > 0 else _latency_ms(rng, load, 30.0),
        ),
        build_gauge(
            "haproxy.connections.average_time",
            "ms",
            connection_time_ms if connection_time_ms > 0 else _latency_ms(rng, load, 8.0),
        ),
        build_gauge(
            "haproxy.requests.average_time",
            "ms",
            queue_time_ms if queue_time_ms > 0 else _latency_ms(rng, load, 5.0),
        ),
        build_gauge("haproxy.weight", "1", 100 if active_servers else 0),
    ]
    for code, value in state.requests_by_status.items():
        metrics.append(
            build_cumulative_sum(
                "haproxy.requests.total",
                "{requests}",
                value,
                state.start_time_ns,
                {"status_code": code},
            )
        )
    metrics.extend(
        [
            build_cumulative_sum(
                "haproxy.connections.errors",
                "{errors}",
                state.connection_errors,
                state.start_time_ns,
            ),
            build_cumulative_sum(
                "haproxy.connections.retries",
                "{retries}",
                state.connection_retries,
                state.start_time_ns,
            ),
            build_cumulative_sum(
                "haproxy.responses.errors",
                "{errors}",
                state.response_errors,
                state.start_time_ns,
            ),
            build_cumulative_sum(
                "haproxy.responses.denied",
                "{responses}",
                state.response_denied,
                state.start_time_ns,
            ),
            build_cumulative_sum(
                "haproxy.requests.redispatched",
                "{requests}",
                state.requests_redispatched,
                state.start_time_ns,
            ),
            build_cumulative_sum(
                "haproxy.server_selected.total",
                "{selections}",
                state.server_selected,
                state.start_time_ns,
            ),
            build_cumulative_sum(
                "haproxy.bytes.input", "By", state.bytes_in, state.start_time_ns,
            ),
            build_cumulative_sum(
                "haproxy.bytes.output", "By", state.bytes_out, state.start_time_ns,
            ),
            build_cumulative_sum(
                "haproxy.failed_checks",
                "{checks}",
                state.failed_checks,
                state.start_time_ns,
            ),
        ]
    )
    return metrics


def _server_metrics(
    state: EntityState,
    rng: random.Random,
    load: float,
    *,
    active: bool = True,
    connection_time_ms: float = 0.0,
    queue_time_ms: float = 0.0,
    response_time_ms: float = 0.0,
) -> list[dict]:
    return [
        build_gauge("haproxy.active", "{servers}", 1 if active else 0),
        build_gauge("haproxy.weight", "1", rng.choice([50, 100]) if active else 0),
        build_gauge(
            "haproxy.sessions.count",
            "{sessions}",
            int(rng.randint(1, 150) * load) if active else 0,
        ),
        build_gauge("haproxy.sessions.limit", "{sessions}", SESSION_LIMIT // 3),
        build_gauge(
            "haproxy.responses.average_time",
            "ms",
            response_time_ms if response_time_ms > 0 else _latency_ms(rng, load, 28.0),
        ),
        build_gauge(
            "haproxy.connections.average_time",
            "ms",
            connection_time_ms if connection_time_ms > 0 else _latency_ms(rng, load, 7.0),
        ),
        build_gauge(
            "haproxy.requests.average_time",
            "ms",
            queue_time_ms if queue_time_ms > 0 else _latency_ms(rng, load, 4.0),
        ),
        build_cumulative_sum(
            "haproxy.failed_checks",
            "{checks}",
            state.failed_checks,
            state.start_time_ns,
        ),
        build_cumulative_sum(
            "haproxy.clients.canceled",
            "{cancellations}",
            state.clients_canceled,
            state.start_time_ns,
        ),
        build_cumulative_sum(
            "haproxy.connections.errors",
            "{errors}",
            state.connection_errors,
            state.start_time_ns,
        ),
        build_cumulative_sum(
            "haproxy.connections.retries",
            "{retries}",
            state.connection_retries,
            state.start_time_ns,
        ),
        build_cumulative_sum(
            "haproxy.responses.errors",
            "{errors}",
            state.response_errors,
            state.start_time_ns,
        ),
        build_cumulative_sum(
            "haproxy.server_selected.total",
            "{selections}",
            state.server_selected,
            state.start_time_ns,
        ),
    ]



from generators.haproxy_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("haproxy")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("haproxy")
    cloud = cfg.get("cloud") or _default_cloud()

    default_frontends = [ScenarioIdentity(ns).label("http"), ScenarioIdentity(ns).label("https")]
    default_backends = [ScenarioIdentity(ns).label("api-backend"), ScenarioIdentity(ns).label("web-backend")]
    frontend_names: list[str] = cfg.get("frontend_names") or default_frontends
    backend_names: list[str] = cfg.get("backend_names") or default_backends

    frontend_states = {name: EntityState(rng) for name in frontend_names}
    backend_states = {name: EntityState(rng) for name in backend_names}
    server_states: dict[tuple[str, str], EntityState] = {}
    server_addrs: dict[tuple[str, str], str] = {}
    for be in backend_names:
        for i in range(1, SERVERS_PER_BACKEND + 1):
            srv = f"{be}-srv{i}"
            key = (be, srv)
            server_states[key] = EntityState(rng)
            server_addrs[key] = f"10.0.{backend_names.index(be) + 1}.{i}:8080"

    frontend_addrs = {
        name: f"0.0.0.0:{80 if i == 0 else 443}"
        for i, name in enumerate(frontend_names)
    }
    backend_addrs = {name: f"unix@{name}.sock" for name in backend_names}


    base = _base_resource_kwargs(ns, service_name, host_name, cloud)
    scope = {"name": HAPROXY_SCOPE, "version": SCOPE_VERSION}
    try:
        es_bulk = ESBulkClient()
    except Exception as exc:  # pragma: no cover
        logger.warning("ESBulkClient unavailable (%s); session co-locate disabled", exc)
        es_bulk = None
    logger.info("HAProxy metrics generator started (namespace=%s)", ns)

    def _emit_tick() -> None:
        nonlocal be, i, key, srv
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="haproxy_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        c = active_chaos or {}

        def _key(name: str, default: float = 0.0) -> float:
            value = impact_value(c, name, default=default)
            return default if value is None else float(value)

        # Each key moves only the signal its own package alert reads. The error and retry
        # keys are errors added per emit cycle (the alerts compare the increase over their
        # window); the latency keys are absolute milliseconds.
        conn_errors = int(max(0.0, _key("connection_errors_spike")))
        req_errors = int(max(0.0, _key("request_errors_spike")))
        resp_errors = int(max(0.0, _key("response_errors_spike")))
        retries = int(max(0.0, _key("connection_retries_or_redispatches_spike")))
        check_failures = int(max(0.0, _key("server_health_check_failures")))
        queued = int(max(0.0, _key("requests_queued_at_backend")))
        # Desired haproxy.active on BACKEND rows (SPEC: <1 breach, default 100.0 = healthy
        # sentinel). Do NOT treat 0.0 as "falsy absent".
        backends_down = _key("backend_has_no_active_servers", 100.0) < 1.0
        connection_time_ms = max(0.0, _key("high_backend_connection_time"))
        queue_time_ms = max(0.0, _key("high_backend_queue_time"))
        response_time_ms = max(0.0, _key("high_backend_response_time"))
        # Percent (85.0) or fraction (0.85) of the session limit; the alert fires above 80%.
        sat = max(0.0, _key("session_saturation_approaching_limit"))
        if sat > 1.0:
            sat = sat / 100.0
        sessions_override = int(SESSION_LIMIT * sat) if sat > 0 else None
        load = business_load()
        ts_ms = int(time.time() * 1000)
        bulk_docs: list[dict[str, Any]] = []

        for fe in frontend_names:
            state = frontend_states[fe]
            state.tick(rng, req_errors=req_errors)
            resource = _entity_resource(
                base,
                proxy_name=fe,
                service_name="FRONTEND",
                addr=frontend_addrs[fe],
            )
            client.send_metrics_scoped(
                resource, scope,
                _frontend_metrics(state, rng, load, sessions=sessions_override),
            )
            fe_sessions = (
                sessions_override
                if sessions_override is not None
                else int(rng.randint(10, 500) * load)
            )
            bulk_docs.append(build_session_utilization_doc(
                resource, sessions=fe_sessions, limit=SESSION_LIMIT, timestamp_ms=ts_ms,
            ))

        for i, be in enumerate(backend_names):
            # OOTB "[HAProxy OTel] Backend has no active servers" uses
            # MIN(haproxy.active) < 1 on service_name==BACKEND; only the first backend goes down.
            is_down = backends_down and i == 0
            active_servers = 0 if is_down else SERVERS_PER_BACKEND
            state = backend_states[be]
            state.tick(
                rng, conn_errors=conn_errors, resp_errors=resp_errors, retries=retries,
            )
            resource = _entity_resource(
                base,
                proxy_name=be,
                service_name="BACKEND",
                addr=backend_addrs[be],
            )
            client.send_metrics_scoped(
                resource,
                scope,
                _backend_metrics(
                    state, rng, load, active_servers=active_servers, queued=queued,
                    connection_time_ms=connection_time_ms,
                    queue_time_ms=queue_time_ms,
                    response_time_ms=response_time_ms,
                    sessions=sessions_override,
                ),
            )
            be_sessions = (
                sessions_override
                if sessions_override is not None
                else int(rng.randint(5, 400) * load)
            )
            bulk_docs.append(build_session_utilization_doc(
                resource, sessions=be_sessions, limit=SESSION_LIMIT, timestamp_ms=ts_ms,
            ))

            for j in range(1, SERVERS_PER_BACKEND + 1):
                srv = f"{be}-srv{j}"
                key = (be, srv)
                srv_state = server_states[key]
                srv_state.tick(
                    rng,
                    conn_errors=conn_errors, resp_errors=resp_errors, retries=retries,
                    check_failures=check_failures,
                )
                srv_resource = _entity_resource(
                    base,
                    proxy_name=be,
                    service_name=srv,
                    addr=server_addrs[key],
                )
                client.send_metrics_scoped(
                    srv_resource,
                    scope,
                    _server_metrics(
                        srv_state, rng, load, active=not is_down,
                        connection_time_ms=connection_time_ms,
                        queue_time_ms=queue_time_ms,
                        response_time_ms=response_time_ms,
                    ),
                )

        if es_bulk is not None and es_bulk.configured and bulk_docs:
            es_bulk.send_bulk(
                "metrics-haproxyreceiver.otel-default",
                bulk_docs,
                dynamic_templates={
                    "metrics.haproxy.sessions.count": "gauge_long",
                    "metrics.haproxy.sessions.limit": "gauge_long",
                },
            )

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    if es_bulk is not None:
        es_bulk.close()
    logger.info("HAProxy metrics generator stopped.")


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
