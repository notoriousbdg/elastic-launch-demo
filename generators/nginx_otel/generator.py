#!/usr/bin/env python3
"""Nginx Log Generator — sends synthetic nginx access & error logs via OTLP.

Imports and reuses the existing OTLPClient from app.telemetry to send
structured nginx-style log records directly to the Elastic OTLP endpoint.

Usage (standalone):
    python3 -m generators.nginx_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/nginx_otel@0.9.0"

import logging
import os
import random
import secrets
import signal
import threading
import time

from app.telemetry import (
    OTLPClient,
    ESBulkClient,
    _format_attributes,
    SCHEMA_URL,
    SCOPE_NAME,
    _now_ns,
)
from app.config import SEVERITY_MAP, NAMESPACE
from app.trace_context import _trace_context_store
from generator_engine.db_common import (
    impact_value,
    build_cumulative_sum,
    build_gauge,
    build_signal_resource,
    otel_dataset,
    resolve_active_chaos,
)

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


def _positive_fault_fraction(chaos: dict | None, *keys: str) -> float:
    """First positive impact among keys; ignore SPEC-defaulted sibling zeros.

    ``resolve_active_chaos`` fills every impact_key default (often ``0.0``).
    ``impact_value`` with several keys then returns the first, ``0.0``, and never
    sees GT ``high_error_rate_by_service: 75.0``.
    Values ``> 1`` are treated as percents (``75.0`` → ``0.75``).
    """
    if not chaos:
        return 0.0
    for key in keys:
        if key not in chaos or chaos[key] is None:
            continue
        try:
            val = float(chaos[key])
        except (TypeError, ValueError):
            continue
        if val <= 0:
            continue
        if val > 1.0:
            return min(val / 100.0, 1.0)
        return val
    return 0.0


# ── Configuration ─────────────────────────────────────────────────────────────
BATCH_INTERVAL_MIN = 2
BATCH_INTERVAL_MAX = 5
BATCH_SIZE_MIN = 5
BATCH_SIZE_MAX = 20

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

# Healthy mix stays under alert floors (5xx >5%, 4xx >15%).
STATUS_WEIGHTS = {
    200: 80,
    301: 3,
    304: 8,
    400: 2,
    401: 1,
    403: 1,
    404: 3,
    405: 1,
    500: 1,
    502: 0,
    503: 0,
    504: 0,
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
# OOTB Error log spike counts log.level IN (error,crit,alert,emerg). Keep those
# for chaos only; healthy baseline uses warn/notice so the Request Health panels
# (root log.level, an alias of severity_text; attributes.process.pid) populate
# without firing the spike alert.
_SEVERE_LEVELS = frozenset({"error", "crit", "alert", "emerg"})
SEVERE_ERROR_MESSAGES = [m for m in ERROR_MESSAGES if m[0] in _SEVERE_LEVELS]
HEALTHY_ERROR_MESSAGES = [m for m in ERROR_MESSAGES if m[0] not in _SEVERE_LEVELS]

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
        "cloud.provider": "aws",
        "cloud.platform": "aws_ec2",
        "cloud.region": "us-east-1",
        "cloud.availability_zone": "us-east-1a",
    },
    {
        "cloud.provider": "gcp",
        "cloud.platform": "gcp_compute_engine",
        "cloud.region": "us-central1",
        "cloud.availability_zone": "us-central1-a",
    },
]


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
    _endpoints = endpoints
    _server_names = server_names
    _ns = namespace or NAMESPACE

    method = rng.choice(METHODS)
    path = rng.choice(_endpoints)
    error_fraction = _positive_fault_fraction(
        active_chaos, "high_5xx_error_rate"
    )
    client_error_fraction = float(
        impact_value(active_chaos, "high_4xx_error_rate", default=0.0) or 0.0
    )
    if client_error_fraction > 1.0:
        client_error_fraction = min(client_error_fraction / 100.0, 1.0)
    if active_chaos and (error_fraction > 0 or client_error_fraction > 0):
        # Caller already selected this request as faulted; pick 5xx vs 4xx by weight.
        total_f = error_fraction + client_error_fraction
        if total_f > 0 and rng.random() < (error_fraction / total_f):
            status = rng.choice([500, 502, 503, 504])
        else:
            status = rng.choice([400, 401, 403, 404])
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
    # nginx $upstream_connect_time <= $upstream_header_time <= $upstream_response_time
    # <= $request_time (seconds); the rest of request_time is nginx and the client.
    upstream_response = round(request_time * rng.uniform(0.7, 0.95), 4)
    upstream_header = round(upstream_response * rng.uniform(0.5, 0.95), 4)
    upstream_connect = round(min(upstream_header, rng.uniform(0.0003, 0.004)), 4)

    severity = "INFO"
    if status >= 500:
        severity = "ERROR"
    elif status >= 400:
        severity = "WARN"

    # Trace/span IDs: prefer the affected service's last error trace from the shared store
    trace_id = None
    span_id = None
    _ns = namespace or ""
    if active_chaos:
        for svc in active_chaos.get("affected_services", []):
            t, s = _trace_context_store.get(svc, namespace=_ns)
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
        "url.path": path,
        "http.response.status_code": status,
        # 0.9.0 dashboards read body.size and the semconv request duration (seconds);
        # body.bytes and nginx.request_time stay for data read by older versions.
        "http.response.body.bytes": body_bytes,
        "http.response.body.size": body_bytes,
        "http.server.request.duration": request_time,
        "nginx.upstream.connect.duration": upstream_connect,
        "nginx.upstream.header.duration": upstream_header,
        "nginx.upstream.response.duration": upstream_response,
        "source.address": client_ip,
        "user_agent.original": ua,
        "user_agent.name": ua_name,
        "server.address": server,
        "url.domain": ScenarioIdentity(_ns).domain(),
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


def nginx_error_line(
    rng: random.Random,
    level: str,
    pid: int,
    message: str,
    client_ip: str,
    server: str,
    path: str,
    *,
    upstream: str | None = None,
) -> str:
    """One line in nginx's error_log format.

    ``YYYY/MM/DD HH:MM:SS [level] pid#tid: *cid message, client: …`` — the 0.9.0
    error panels GROK ``[level] pid#tid: *cid`` to extract the message.
    """
    stamp = time.strftime("%Y/%m/%d %H:%M:%S", time.gmtime())
    line = (
        f'{stamp} [{level}] {pid}#0: *{rng.randint(1, 999_999)} {message}, client: {client_ip}, '
        f'server: {server}, request: "GET {path} HTTP/1.1"'
    )
    return f'{line}, upstream: "http://{upstream}{path}"' if upstream else line


def _generate_error_log(
    client: OTLPClient,
    rng: random.Random,
    endpoints: list | None = None,
    server_names: list | None = None,
    active_chaos: dict | None = None,
    namespace: str | None = None,
    error_message: str = "",
) -> dict:
    _endpoints = endpoints
    _server_names = server_names

    if active_chaos and error_message:
        log_level = "error"
        error_msg = error_message
    elif active_chaos:
        log_level, error_msg = rng.choice(SEVERE_ERROR_MESSAGES)
    else:
        log_level, error_msg = rng.choice(HEALTHY_ERROR_MESSAGES)
    server = rng.choice(_server_names)
    upstream = rng.choice(UPSTREAM_ADDRS)
    client_ip = rng.choice(CLIENT_IPS)
    path = rng.choice(_endpoints)

    pid = rng.randint(10000, 99999)
    body = nginx_error_line(
        rng, log_level, pid, error_msg, client_ip, server, path, upstream=upstream,
    )

    # OOTB Error log spike: ``log.level IN ("error","crit","alert","emerg")``
    # (alias → severity_text). Emit lowercase nginx levels, not OTel ERROR.
    severity_map = {
        "error": "error",
        "crit": "crit",
        "alert": "alert",
        "emerg": "emerg",
        "warn": "warn",
        "notice": "notice",
    }
    severity = severity_map.get(log_level, "error")

    # As of nginx_otel@0.7.0 the Request Health panels read root log.level, which is an
    # alias of severity_text (set above). Never write the alias as a concrete root field:
    # that lands the document in the failure store. attributes.log.level is kept only
    # for data written against 0.6.x dashboards.
    attrs = {
        "log.level": log_level,
        "error.message": error_msg,
        "source.address": client_ip,
        "server.address": server,
        "url.original": path,
        "upstream.address": upstream,
        "process.pid": pid,
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
        _ns = namespace or ""
        for svc in active_chaos.get("affected_services", []):
            t, s = _trace_context_store.get(svc, namespace=_ns)
            if t and s:
                trace_id, span_id = t, s
                break

    return client.build_log_record(
        severity=severity, body=body, attributes=attrs,
        trace_id=trace_id, span_id=span_id,
    )


def build_error_log_bulk_doc(
    host_name: str,
    *,
    log_level: str,
    error_msg: str,
    process_pid: int,
    body: str,
    timestamp_iso: str,
    namespace: str = "default",
    service_name: str = "nginx-proxy",
) -> dict:
    """ES bulk companion for Request Health error panels (ALERTS #106).

    OTLP error logs were only emitted during ``error_log_spike`` chaos, so
    ``attributes.process.pid`` (and, before nginx_otel@0.7.0, ``attributes.log.level``)
    never mapped and Lens reported "Field was not found". ``severity_text`` carries the
    level the 0.7.0 panels read as root ``log.level``. Dual-write with ``pipeline=_none``.
    """
    return {
        "@timestamp": timestamp_iso,
        "data_stream": {
            "type": "logs",
            "dataset": "nginx.error.otel",
            "namespace": namespace,
        },
        "severity_text": log_level,
        "body": {"text": body},
        "attributes": {
            "log.level": log_level,
            "process.pid": int(process_pid),
            "error.message": error_msg,
            "event.category": "web",
            "event.type": "error",
            "event.kind": "event",
        },
        "resource": {
            "attributes": {
                "host.name": host_name,
                "service.name": service_name,
            }
        },
    }


# ── Run loop (used by ServiceManager and standalone) ──────────────────────────

from generators.nginx_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
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
    cfg = scenario_data.get("generator_config", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "any")


    ident = ScenarioIdentity.from_scenario(scenario_data, ns)
    server_names = [ident.host("nginx", 1), ident.host("nginx", 2)]
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
    nginx_hosts = []
    for i, host_cfg in enumerate(NGINX_HOST_CONFIGS, start=1):
        nginx_hosts.append({
            **host_cfg,
            "host.name": ident.host("nginx", i),
            "service.name": ident.service("nginx-proxy"),
            "service.instance.id": ident.instance("nginx-proxy", i),
        })

    def _build_host_resource(host_cfg: dict, dataset: str, data_stream_type: str = "logs") -> dict:
        return {
            "attributes": _format_attributes(
                {
                    "service.name": host_cfg.get("service.name", ident.service("nginx-proxy")),
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
    # Miswired OOTB rule "High error rate by service" (id nginx-active-connections-high)
    # queries logs-myservicereceiver.otel-* + log.level — emit companion logs so
    # the rule verifies without editing the package template.
    # Pass the bare dataset name: OTLP intake appends `.otel` (passing
    # ``myservicereceiver.otel`` would create ``…otel.otel…``).
    myservice_resources = [
        _build_host_resource(h, "myservicereceiver") for h in nginx_hosts
    ]

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
    # Start well above any prior series tip so MAX(accepted/handled) in the
    # OOTB 5m window is dominated by the synced healthy series (alert has no
    # series identity — only host-level min/max).
    conn_accepted = [rng.randint(5_000_000, 8_000_000) for _ in nginx_hosts]
    conn_handled = list(conn_accepted)  # healthy: handled == accepted (no drops)
    conn_requests = [rng.randint(8_000_000, 12_000_000) for _ in nginx_hosts]
    # Stable start times (PVC-persisted via CounterState helper) so restarts
    # do not create overlapping series that phantom-trip INCREASE alerts.
    from generator_engine.db_common import stable_counter_start_ns
    conn_start_ns = [stable_counter_start_ns() for _ in nginx_hosts]

    nginx_metric_scope = {
        "name": "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/nginxreceiver",
        "version": "0.115.0",
    }
    metric_resources = [
        build_signal_resource(
            ns=ns,
            signal_type="metrics",
            dataset=otel_dataset("nginxreceiver"),
            service_name=ident.service("nginx-proxy"),
            service_version="1.25.4",
            service_instance_id=h["service.instance.id"],
            host_name=h["host.name"],
            cloud={
                "provider": h["cloud.provider"],
                "platform": h["cloud.platform"],
                "region": h["cloud.region"],
                "availability_zone": h["cloud.availability_zone"],
            },
        )
        for h in nginx_hosts
    ]

    logger.info(
        "Nginx log generator started (namespace=%s, chaos_aware=%s)",
        ns, chaos_controller is not None,
    )

    try:
        es_bulk = ESBulkClient()
    except Exception as exc:
        logger.warning("ESBulkClient unavailable (%s); myservice companion via OTLP only", exc)
        es_bulk = None

    sleep_time = None
    def _emit_tick() -> None:
        nonlocal sleep_time, total_sent, total_spans
        batch_size = rng.randint(BATCH_SIZE_MIN, BATCH_SIZE_MAX)

        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="nginx_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        short_msg = ""
        if active_chaos:
            # Channel error text, for log body correlation.
            ch = _channel_registry.get(active_chaos.get("channel_id"), {})
            short_msg = (ch.get("error_message") or "").split("\n", 1)[0][:240]

        # Prefer any positive probe; do not let a SPEC-defaulted sibling 0.0
        # (e.g. high_5xx_error_rate) mask high_error_rate_by_service: 75.0.
        error_fraction = _positive_fault_fraction(
            active_chaos, "high_5xx_error_rate"
        )
        # Companion-only probe must not also trip the 5xx access-log alert
        # during quiet windows — that key is handled via myservice bulk docs.
        error_log_count = int(impact_value(active_chaos, "error_log_spike", default=0) or 0)
        dropped = int(impact_value(active_chaos, "dropped_connections_detected", default=0) or 0)
        active_conns_override = int(impact_value(active_chaos, "high_active_connections", default=0) or 0)

        # Pick a random nginx host for this batch (distributes across instances)
        host_idx = rng.randint(0, len(nginx_hosts) - 1)
        access_resource = access_resources[host_idx]
        error_resource = error_resources[host_idx]
        trace_resource = trace_resources[host_idx]

        # Generate access logs + correlated trace spans
        access_records = []
        spans = []
        client_error_fraction = _positive_fault_fraction(
            active_chaos, "high_4xx_error_rate"
        )
        fault_p = max(error_fraction, client_error_fraction)
        for _ in range(batch_size):
            # Pass chaos dict only for the faulted share so healthy requests stay clean.
            req_chaos = (
                active_chaos
                if (active_chaos and fault_p > 0 and rng.random() < fault_p)
                else None
            )
            log_record, span = _generate_access_log(
                client, rng, endpoints, server_names, ns, active_chaos=req_chaos,
            )
            access_records.append(log_record)
            if span:
                spans.append(span)
        client.send_logs(access_resource, access_records)

        # Companion myservicereceiver logs for the miswired OOTB error-rate rule.
        # OOTB checks ``log.level == "error"`` (alias → ``severity_text``) and
        # ``BY service.name`` (alias → ``resource.attributes.service.name``).
        # Do NOT write alias fields (``log.level``, ``message``, ``service.name``)
        # — they land in the failure store. Also OOTB does ``errors/total*100``
        # with integer division, so only a ~100% error share yields pct > 5.
        if es_bulk is not None and es_bulk.configured:
            now_iso = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
            # When the companion probe is active, emit all errors so int division
            # yields 100%; otherwise keep a healthy all-info baseline.
            companion_err = _positive_fault_fraction(
                active_chaos, "high_error_rate_by_service"
            ) > 0
            bulk_docs = []
            for _ in range(max(batch_size, 20)):
                is_err = companion_err or (fault_p > 0 and rng.random() < fault_p)
                # For 5xx-only faults without the companion key, still allow a
                # partial mix; companion key forces all-error for int-div.
                if companion_err:
                    is_err = True
                bulk_docs.append(
                    {
                        "@timestamp": now_iso,
                        "severity_text": "error" if is_err else "info",
                        "resource": {
                            "attributes": {"service.name": ident.service("nginx-proxy")},
                        },
                        "body": {
                            "text": f"request completed level={'error' if is_err else 'info'}",
                        },
                    }
                )
            es_bulk.send_bulk(
                "logs-myservicereceiver.otel-default",
                bulk_docs,
                params={"pipeline": "_none"},
            )
        else:
            myservice_resource = myservice_resources[host_idx]
            myservice_records = []
            for _ in range(batch_size):
                is_err = fault_p > 0 and rng.random() < fault_p
                level = "error" if is_err else "info"
                myservice_records.append(
                    client.build_log_record(
                        severity="error" if is_err else "info",
                        body=f"request completed level={level}",
                        attributes={
                            "service.name": ident.service("nginx-proxy"),
                        },
                    )
                )
            client.send_logs(myservice_resource, myservice_records)

        # Send correlated trace spans (only if nginx-proxy is in scenario topology)
        if spans and _emit_traces:
            client.send_traces(trace_resource, spans)
            total_spans += len(spans)

        # Error logs for Request Health (severity + pid panels) and Error log spike.
        # Healthy baseline: warn/notice only (maps attrs without >50 severe/15m).
        # Spike probe: severe levels at chaos volume.
        _ERROR_BASELINE = 4
        error_count = error_log_count if error_log_count > 0 else _ERROR_BASELINE
        host_name = nginx_hosts[host_idx]["host.name"]
        if es_bulk is not None and es_bulk.configured:
            now_iso = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
            error_bulk = []
            for _ in range(error_count):
                if active_chaos and error_log_count > 0 and short_msg:
                    lvl, msg = "error", short_msg
                elif error_log_count > 0:
                    lvl, msg = rng.choice(SEVERE_ERROR_MESSAGES)
                else:
                    lvl, msg = rng.choice(HEALTHY_ERROR_MESSAGES)
                pid = rng.randint(10000, 99999)
                body = nginx_error_line(
                    rng, lvl, pid, msg, rng.choice(CLIENT_IPS), rng.choice(server_names),
                    rng.choice(endpoints),
                )
                error_bulk.append(
                    build_error_log_bulk_doc(
                        host_name,
                        log_level=lvl,
                        error_msg=msg,
                        process_pid=pid,
                        body=body,
                        timestamp_iso=now_iso,
                        service_name=ident.service("nginx-proxy"),
                    )
                )
            es_bulk.send_bulk(
                "logs-nginx.error.otel-default",
                error_bulk,
                params={"pipeline": "_none"},
            )
        elif error_count > 0:
            error_records = []
            for _ in range(error_count):
                error_records.append(
                    _generate_error_log(
                        client, rng, endpoints, server_names,
                        active_chaos=active_chaos if error_log_count > 0 else None,
                        namespace=ns,
                        error_message=short_msg,
                    )
                )
            client.send_logs(error_resource, error_records)

        # nginxreceiver connection metrics (dropped / active alerts).
        accepted_delta = rng.randint(5, 40)
        handled_delta = accepted_delta - (dropped if dropped > 0 else 0)
        if handled_delta < 0:
            handled_delta = 0
        conn_accepted[host_idx] += accepted_delta
        conn_requests[host_idx] += batch_size
        # Keep handled identical to accepted when not dropping — OOTB alert is
        # (Δaccepted - Δhandled) > 0 and drifts if the two counters diverge.
        if dropped > 0:
            conn_handled[host_idx] += handled_delta
        else:
            conn_handled[host_idx] = conn_accepted[host_idx]
        active_conns = (
            max(active_conns_override, 300)
            if active_conns_override > 0
            else rng.randint(10, 120)
        )
        reading = max(0, active_conns // 4)
        writing = max(0, active_conns // 3)
        waiting = max(0, active_conns // 5)
        start_ns = conn_start_ns[host_idx]
        metric_list = [
            build_cumulative_sum(
                "nginx.requests", "{request}",
                conn_requests[host_idx], start_ns,
            ),
            build_cumulative_sum(
                "nginx.connections_accepted", "{connections}",
                conn_accepted[host_idx], start_ns,
            ),
            build_cumulative_sum(
                "nginx.connections_handled", "{connections}",
                conn_handled[host_idx], start_ns,
            ),
            build_gauge(
                "nginx.connections_current", "{connections}",
                active_conns, {"state": "active"},
            ),
            build_gauge(
                "nginx.connections_current", "{connections}",
                reading, {"state": "reading"},
            ),
            build_gauge(
                "nginx.connections_current", "{connections}",
                writing, {"state": "writing"},
            ),
            build_gauge(
                "nginx.connections_current", "{connections}",
                waiting, {"state": "waiting"},
            ),
        ]
        client.send_metrics_scoped(
            metric_resources[host_idx], nginx_metric_scope, metric_list,
        )

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
    run_tick_loop(stop_event, _emit_tick, interval=lambda: sleep_time)

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
