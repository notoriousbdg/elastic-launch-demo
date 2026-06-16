#!/usr/bin/env python3
"""Nginx Log Generator — sends synthetic nginx access & error logs via OTLP.

Imports and reuses the existing OTLPClient from app.telemetry to send
structured nginx-style log records directly to the Elastic OTLP endpoint.

Usage (standalone):
    python3 -m log_generators.nginx_log_generator
"""

from __future__ import annotations

import logging
import os
import random
import secrets
import signal
import threading
import time

from app.telemetry import (
    OTLPClient,
    _format_attributes,
    SCHEMA_URL,
    SCOPE_NAME,
    _now_ns,
)
from app.config import SEVERITY_MAP, NAMESPACE
from app.trace_context import _trace_context_store

# Nginx is the front door for the demo: any active fault on any service is
# observable as elevated 5xx + correlated logs at the nginx layer.
# This permissive relevance keeps the trace<->log channel pivot demonstrable
# across every scenario without requiring scenario-specific subsystem strings.

# Span kind constants
SPAN_KIND_SERVER = 2
SPAN_KIND_CLIENT = 3
STATUS_OK = 1
STATUS_ERROR = 2

logger = logging.getLogger("nginx-log-generator")

# ── Configuration ─────────────────────────────────────────────────────────────
BATCH_INTERVAL_MIN = 2
BATCH_INTERVAL_MAX = 5
BATCH_SIZE_MIN = 5
BATCH_SIZE_MAX = 20

# ── Realistic nginx data pools ────────────────────────────────────────────────
ENDPOINTS = [
    "/api/v1/telemetry",
    "/api/v1/health",
    "/api/v1/metrics",
    "/api/v1/traces",
    "/api/v1/logs",
    f"/api/v1/agents/{NAMESPACE}",
    "/api/v1/channels/status",
    "/api/v1/operations/status",
    "/api/v1/operations/emergency",
    "/api/v2/telemetry/stream",
    "/static/app.js",
    "/static/app.css",
    "/static/dashboard.js",
    "/static/favicon.ico",
    "/dashboard",
    "/dashboard/operations",
    "/dashboard/overview",
    "/login",
    "/logout",
    "/healthz",
    "/readyz",
]

METHODS = [
    "GET",
    "GET",
    "GET",
    "GET",
    "POST",
    "POST",
    "PUT",
    "DELETE",
    "HEAD",
    "OPTIONS",
]

STATUS_WEIGHTS = {
    200: 60,
    301: 3,
    304: 8,
    400: 5,
    401: 3,
    403: 2,
    404: 8,
    405: 1,
    500: 4,
    502: 3,
    503: 2,
    504: 1,
}
STATUS_CODES = []
for code, weight in STATUS_WEIGHTS.items():
    STATUS_CODES.extend([code] * weight)

CLIENT_IPS = [
    "10.0.1.42",
    "10.0.1.87",
    "10.0.2.15",
    "10.0.2.200",
    "10.0.3.55",
    "172.16.0.10",
    "172.16.0.25",
    "172.16.1.100",
    "192.168.1.1",
    "192.168.1.50",
    "203.0.113.42",
    "203.0.113.99",
    "198.51.100.23",
    "198.51.100.77",
]

USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 Safari/605.1.15",
    "python-httpx/0.27.0",
    "curl/8.4.0",
    "Go-http-client/2.0",
    "Platform-Monitor/1.0",
    "Elastic-Heartbeat/8.12.0",
    "kube-probe/1.28",
]

SERVER_NAMES = [f"{NAMESPACE}-nginx-01", f"{NAMESPACE}-nginx-02"]

ERROR_MESSAGES = [
    ("error", "upstream timed out (110: Connection timed out) while connecting to upstream"),
    ("error", "upstream prematurely closed connection while reading response header from upstream"),
    ("error", "connect() failed (111: Connection refused) while connecting to upstream"),
    ("error", "no live upstreams while connecting to upstream"),
    ("error", "recv() failed (104: Connection reset by peer)"),
    ("crit", "SSL_do_handshake() failed (SSL: error:0A000126:SSL routines::unexpected eof while reading)"),
    ("crit", "client intended to send too large body: 10485760 bytes"),
    ("warn", "access forbidden by rule"),
    ("warn", 'open() "/usr/share/nginx/html/missing" failed (2: No such file or directory)'),
    ("warn", "upstream sent too big header while reading response header from upstream"),
    ("warn", "could not build optimal types_hash, you should increase either types_hash_max_size or types_hash_bucket_size"),
    ("notice", "graceful shutdown requested"),
    ("notice", "signal process started"),
]

UPSTREAM_ADDRS = [
    "10.0.1.100:8080",
    "10.0.1.101:8080",
    "10.0.1.102:8080",
    "10.0.2.100:8080",
    "10.0.2.101:8080",
]


# Two nginx host configs — must match nginx_metrics_generator.py for dashboard
# "Nginx instance" filter to correlate logs with metrics by host.name.
NGINX_HOST_CONFIGS = [
    {
        "service.instance.id": "nginx-proxy-001",
        "cloud.provider": "aws",
        "cloud.platform": "aws_ec2",
        "cloud.region": "us-east-1",
        "cloud.availability_zone": "us-east-1a",
    },
    {
        "service.instance.id": "nginx-proxy-002",
        "cloud.provider": "gcp",
        "cloud.platform": "gcp_compute_engine",
        "cloud.region": "us-central1",
        "cloud.availability_zone": "us-central1-a",
    },
]


# ── Log record generators ────────────────────────────────────────────────────
def _channel_relevant_to_nginx(ch: dict) -> bool:
    """Any active channel is reflected at the front-door nginx tier."""
    return bool(ch)


def _generate_access_log(
    client: OTLPClient,
    rng: random.Random,
    endpoints: list | None = None,
    server_names: list | None = None,
    namespace: str | None = None,
    active_chaos: dict | None = None,
) -> tuple[dict, dict | None]:
    """Generate an access log record and optionally an HTTP trace span.

    Returns (log_record, span_or_None).
    When ``active_chaos`` is provided, force a 5xx status, tag with chaos.* attrs,
    and source trace_id/span_id from the shared trace context store so the
    access log links back to a real APM error transaction.
    """
    _endpoints = endpoints or ENDPOINTS
    _server_names = server_names or SERVER_NAMES
    _ns = namespace or NAMESPACE

    method = rng.choice(METHODS)
    path = rng.choice(_endpoints)
    if active_chaos:
        status = rng.choice([500, 502, 503, 504])
    else:
        status = rng.choice(STATUS_CODES)
    body_bytes = rng.randint(0, 50000) if status == 200 else rng.randint(0, 500)
    client_ip = rng.choice(CLIENT_IPS)
    ua = rng.choice(USER_AGENTS)
    server = rng.choice(_server_names)
    upstream = rng.choice(UPSTREAM_ADDRS)
    request_time = round(rng.uniform(0.001, 0.3), 3)

    # Slower requests for error statuses
    if status >= 500:
        request_time = round(rng.uniform(0.2, 0.8), 3)

    severity = "INFO"
    if status >= 500:
        severity = "ERROR"
    elif status >= 400:
        severity = "WARN"

    # Trace/span IDs: prefer the affected service's last error trace from the shared store
    trace_id = None
    span_id = None
    if active_chaos:
        for svc in active_chaos.get("affected_services", []):
            t, s = _trace_context_store.get(svc)
            if t and s:
                trace_id, span_id = t, s
                break
    if trace_id is None:
        trace_id = secrets.token_hex(16)
        span_id = secrets.token_hex(8)

    body = (
        f'{client_ip} - - "{method} {path} HTTP/1.1" {status} {body_bytes} '
        f'"{ua}" rt={request_time}'
    )

    ua_name = ua.split("/")[0]
    attrs = {
        "http.request.method": method,
        "url.original": path,
        "http.response.status_code": status,
        "http.response.body.bytes": body_bytes,
        "source.address": client_ip,
        "user_agent.original": ua,
        "user_agent.name": ua_name,
        "http.version": "1.1",
        "server.address": server,
        "url.domain": f"{_ns}.internal",
        "network.protocol.name": "http",
        "network.protocol.version": "1.1",
        "upstream.address": upstream,
        "nginx.request_time": request_time,
    }
    if active_chaos:
        attrs["chaos.channel"] = active_chaos["channel_id"]
        if active_chaos.get("name"):
            attrs["chaos.fault_type"] = active_chaos["name"]
        if active_chaos.get("subsystem"):
            attrs["chaos.subsystem"] = active_chaos["subsystem"]
        if status >= 500 and active_chaos.get("error_type"):
            attrs["error.type"] = active_chaos["error_type"]

    log_record = client.build_log_record(
        severity=severity,
        body=body,
        attributes=attrs,
        trace_id=trace_id,
        span_id=span_id,
    )

    # Build a correlated HTTP span
    span_status = STATUS_ERROR if status >= 500 else STATUS_OK
    duration_ms = int(request_time * 1000)
    _span_attrs = {
        "http.request.method": method,
        "url.path": path,
        "http.response.status_code": status,
        "server.address": server,
        "server.port": 80,
        "client.address": client_ip,
        "user_agent.original": ua,
        "network.protocol.version": "1.1",
    }
    if active_chaos:
        _span_attrs["chaos.channel"] = active_chaos["channel_id"]
        if active_chaos.get("name"):
            _span_attrs["chaos.fault_type"] = active_chaos["name"]
        if active_chaos.get("subsystem"):
            _span_attrs["chaos.subsystem"] = active_chaos["subsystem"]
        if span_status == STATUS_ERROR and active_chaos.get("error_type"):
            _span_attrs["error.type"] = active_chaos["error_type"]
    span = client.build_span(
        name=f"{method} {path}",
        trace_id=trace_id,
        span_id=span_id,
        kind=SPAN_KIND_SERVER,
        duration_ms=max(1, duration_ms),
        status_code=span_status,
        attributes=_span_attrs,
    )

    return log_record, span


def _generate_error_log(
    client: OTLPClient,
    rng: random.Random,
    endpoints: list | None = None,
    server_names: list | None = None,
    active_chaos: dict | None = None,
) -> dict:
    _endpoints = endpoints or ENDPOINTS
    _server_names = server_names or SERVER_NAMES

    if active_chaos and active_chaos.get("error_message_short"):
        log_level = "error"
        error_msg = active_chaos["error_message_short"]
    else:
        log_level, error_msg = rng.choice(ERROR_MESSAGES)
    server = rng.choice(_server_names)
    upstream = rng.choice(UPSTREAM_ADDRS)
    client_ip = rng.choice(CLIENT_IPS)
    path = rng.choice(_endpoints)

    body = f'[{log_level}] {error_msg}, client: {client_ip}, server: {server}, request: "GET {path} HTTP/1.1", upstream: "http://{upstream}{path}"'

    severity_map = {"error": "ERROR", "crit": "FATAL", "warn": "WARN", "notice": "INFO"}
    severity = severity_map.get(log_level, "ERROR")

    attrs = {
        "error.message": error_msg,
        "source.address": client_ip,
        "server.address": server,
        "url.original": path,
        "upstream.address": upstream,
        "log.level": log_level,
        "process.pid": rng.randint(10000, 99999),
        "event.category": "web",
        "event.type": "error",
        "event.kind": "event",
    }
    trace_id = None
    span_id = None
    if active_chaos:
        attrs["chaos.channel"] = active_chaos["channel_id"]
        if active_chaos.get("name"):
            attrs["chaos.fault_type"] = active_chaos["name"]
        if active_chaos.get("subsystem"):
            attrs["chaos.subsystem"] = active_chaos["subsystem"]
        if active_chaos.get("error_type"):
            attrs["error.type"] = active_chaos["error_type"]
        for svc in active_chaos.get("affected_services", []):
            t, s = _trace_context_store.get(svc)
            if t and s:
                trace_id, span_id = t, s
                break

    return client.build_log_record(
        severity=severity, body=body, attributes=attrs,
        trace_id=trace_id, span_id=span_id,
    )


# ── Run loop (used by ServiceManager and standalone) ──────────────────────────
def run(
    client: OTLPClient, stop_event: threading.Event, scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run nginx log generator loop until stop_event is set."""
    rng = random.Random()

    # Rebuild namespace-dependent data from scenario_data to avoid import-time freezing
    if scenario_data:
        ns = scenario_data["namespace"]
    else:
        ns = NAMESPACE

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}

    server_names = [f"{ns}-nginx-01", f"{ns}-nginx-02"]
    endpoints = [
        "/api/v1/telemetry",
        "/api/v1/health",
        "/api/v1/metrics",
        "/api/v1/traces",
        "/api/v1/logs",
        f"/api/v1/agents/{ns}",
        "/api/v1/channels/status",
        "/api/v1/operations/status",
        "/api/v1/operations/emergency",
        "/api/v2/telemetry/stream",
        "/static/app.js",
        "/static/app.css",
        "/static/dashboard.js",
        "/static/favicon.ico",
        "/dashboard",
        "/dashboard/operations",
        "/dashboard/overview",
        "/login",
        "/logout",
        "/healthz",
        "/readyz",
    ]

    # Two nginx host configs matching nginx_metrics_generator.py (same host.name values)
    nginx_hosts = [
        {**cfg, "host.name": f"{ns}-nginx-{i + 1:02d}"}
        for i, cfg in enumerate(NGINX_HOST_CONFIGS)
    ]

    def _build_host_resource(host_cfg: dict, dataset: str, data_stream_type: str = "logs") -> dict:
        return {
            "attributes": _format_attributes(
                {
                    "service.name": "nginx-proxy",
                    "service.namespace": ns,
                    "service.version": "1.25.4",
                    "service.instance.id": host_cfg["service.instance.id"],
                    "telemetry.sdk.language": "python",
                    "telemetry.sdk.name": "opentelemetry",
                    "telemetry.sdk.version": "1.24.0",
                    "cloud.provider": host_cfg["cloud.provider"],
                    "cloud.platform": host_cfg["cloud.platform"],
                    "cloud.region": host_cfg["cloud.region"],
                    "cloud.availability_zone": host_cfg["cloud.availability_zone"],
                    "deployment.environment": f"production-{ns}",
                    "host.name": host_cfg["host.name"],
                    "host.architecture": "amd64",
                    "os.type": "linux",
                    "data_stream.type": data_stream_type,
                    "data_stream.dataset": dataset,
                    "data_stream.namespace": "default",
                }
            ),
            "schemaUrl": SCHEMA_URL,
        }

    # Two access resources and two error resources — one per nginx instance
    access_resources = [_build_host_resource(h, "nginx.access") for h in nginx_hosts]
    error_resources = [_build_host_resource(h, "nginx.error") for h in nginx_hosts]
    trace_resources = [_build_host_resource(h, "generic", "traces") for h in nginx_hosts]

    # Only emit traces if nginx-proxy is in the scenario's services (avoids
    # disconnected Service Map nodes when the scenario doesn't include it).
    _emit_traces = True
    if scenario_data and "services" in scenario_data:
        _emit_traces = "nginx-proxy" in scenario_data["services"]

    # Only emit traces if nginx-proxy is in the scenario's services (avoids
    # disconnected Service Map nodes when the scenario doesn't include it).
    _emit_traces = True
    if scenario_data and "services" in scenario_data:
        _emit_traces = "nginx-proxy" in scenario_data["services"]

    total_sent = 0
    total_spans = 0
    error_spike_active = False

    logger.info(
        "Nginx log generator started (namespace=%s, chaos_aware=%s)",
        ns, chaos_controller is not None,
    )

    while not stop_event.is_set():
        batch_size = rng.randint(BATCH_SIZE_MIN, BATCH_SIZE_MAX)

        # 10% chance of an error spike each cycle
        if rng.random() < 0.10:
            error_spike_active = True
        elif error_spike_active and rng.random() < 0.5:
            error_spike_active = False

        # Resolve a single nginx-relevant active channel (if any) for this batch.
        # When present, a fraction of access logs become 5xx with chaos.* attrs,
        # and error logs are tagged + correlated via the trace context store.
        active_chaos: dict | None = None
        if chaos_controller and _channel_registry:
            for ch_id in chaos_controller.get_active_channels():
                ch = _channel_registry.get(ch_id)
                if ch and _channel_relevant_to_nginx(ch):
                    short_msg = (ch.get("error_message") or "").split("\n", 1)[0][:240]
                    active_chaos = {
                        "channel_id": ch_id,
                        "name": ch.get("name"),
                        "subsystem": ch.get("subsystem"),
                        "error_type": ch.get("error_type"),
                        "affected_services": ch.get("affected_services", []),
                        "error_message_short": short_msg,
                    }
                    break

        # Pick a random nginx host for this batch (distributes across instances)
        host_idx = rng.randint(0, len(nginx_hosts) - 1)
        access_resource = access_resources[host_idx]
        error_resource = error_resources[host_idx]
        trace_resource = trace_resources[host_idx]

        # Generate access logs + correlated trace spans
        access_records = []
        spans = []
        for _ in range(batch_size):
            # During active chaos, ~40% of access logs reflect the fault
            req_chaos = active_chaos if (active_chaos and rng.random() < 0.4) else None
            log_record, span = _generate_access_log(
                client, rng, endpoints, server_names, ns, active_chaos=req_chaos,
            )
            access_records.append(log_record)
            if span:
                spans.append(span)
        client.send_logs(access_resource, access_records)

        # Send correlated trace spans (only if nginx-proxy is in scenario topology)
        if spans and _emit_traces:
            client.send_traces(trace_resource, spans)
            total_spans += len(spans)

        # Generate error logs (more during spikes or active chaos)
        if active_chaos:
            error_count = rng.randint(4, 12)
        elif error_spike_active:
            error_count = rng.randint(3, 10)
        else:
            error_count = rng.randint(0, 2)
        if error_count > 0:
            error_records = []
            for _ in range(error_count):
                error_records.append(
                    _generate_error_log(
                        client, rng, endpoints, server_names,
                        active_chaos=active_chaos,
                    )
                )
            client.send_logs(error_resource, error_records)

        total_sent += batch_size + error_count
        logger.info(
            "Sent %d access + %d error logs, %d spans (total=%d logs, %d spans)",
            batch_size,
            error_count,
            len(spans),
            total_sent,
            total_spans,
        )

        sleep_time = rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX)
        stop_event.wait(sleep_time)

    logger.info(
        "Nginx log generator stopped. Total: %d logs, %d spans", total_sent, total_spans
    )


# ── Standalone entry point ────────────────────────────────────────────────────
def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    client = OTLPClient()
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    duration = int(os.environ.get("RUN_DURATION", "60"))
    timer = threading.Timer(duration, stop_event.set)
    timer.daemon = True
    timer.start()
    logger.info("Running for %ds (standalone mode)", duration)

    run(client, stop_event)
    timer.cancel()
    client.close()


if __name__ == "__main__":
    main()
