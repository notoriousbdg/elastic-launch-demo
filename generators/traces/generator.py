#!/usr/bin/env python3
"""Trace Generator — sends distributed traces for APM Service Map via OTLP.

Generates realistic distributed traces across the active scenario's services with proper
parent-child span relationships, service topology, and APM-compatible attributes.

Usage (standalone):
    python3 -m generators.traces.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

import logging
import os
import random
import secrets
import signal
import threading
import time
from typing import Any

from app.telemetry import OTLPClient, _format_attributes, SCHEMA_URL
from app.config import SERVICES, CHANNEL_REGISTRY, ACTIVE_SCENARIO, NAMESPACE
from app.trace_context import _trace_context_store
from generator_engine.es_doc_common import resolve_active_chaos

# Generic exceptions by language for baseline error spans (no active chaos channel)
_GENERIC_EXCEPTIONS = {
    "python": [
        ("RuntimeError", "internal server error"),
        ("TimeoutError", "upstream request timed out"),
        ("ConnectionError", "connection refused"),
    ],
    "java": [
        ("java.lang.RuntimeException", "internal server error"),
        ("java.net.SocketTimeoutException", "connect timed out"),
        ("java.sql.SQLException", "connection pool exhausted"),
    ],
    "go": [
        ("runtime.Error", "internal server error"),
        ("net.OpError", "dial tcp: connection refused"),
        ("context.DeadlineExceeded", "context deadline exceeded"),
    ],
    "dotnet": [
        ("System.Exception", "internal server error"),
        ("System.TimeoutException", "operation has timed out"),
        ("System.Net.Sockets.SocketException", "connection refused"),
    ],
    "rust": [
        ("std::io::Error", "connection refused"),
        ("anyhow::Error", "internal server error"),
    ],
    "cpp": [
        ("std::runtime_error", "internal server error"),
        ("std::system_error", "connection refused"),
    ],
}

logger = logging.getLogger("trace-generator")

# Latency multiplier while a channel's HPAs are capped at max replicas (Chaos UI toggle).
HPA_CAPPED_LATENCY_MULTIPLIER = 2.5


# Per-trace error probability for services downstream of a fault
# (``cascade_services``).  A service's tile shows error *spans* / all spans, which
# is roughly a third to half of the per-trace rate, so 0.35 lands cascade tiles in
# the ops-dashboard yellow band (12-20 %) while directly affected services (70 %
# per trace) go red (>20 %).  Calibrated by simulation, see tests/test_trace_cascade.py.
CASCADE_ERROR_RATE = 0.35


def apply_apm_augmentations(
    error_rate: float, latency_mult: float, spikes: dict, channel_active: bool,
) -> tuple[float, float]:
    """Apply the Chaos UI APM controls on top of the channel's own error rate and latency.

    The failed-transaction-rate slider *replaces* the channel's ``error_rate`` (so it can
    dial the fault down as well as up); an HPA capped at max slows requests down. Both act
    only while a channel is active, because trace errors are scoped to its services.
    """
    if not channel_active:
        return error_rate, latency_mult
    apm_error_pct = float(spikes.get("apm_error_rate", 0) or 0)
    if apm_error_pct > 0:
        error_rate = min(1.0, apm_error_pct / 100.0)
    if float(spikes.get("k8s_hpa_capped", 0) or 0) > 0:
        latency_mult = max(latency_mult, HPA_CAPPED_LATENCY_MULTIPLIER)
    return error_rate, latency_mult


def rollup_error_rate(spikes: dict, channel_active: bool, baseline: float = 0.01) -> float:
    """Failed-transaction rate for the APM rollup (what the APM ML jobs read)."""
    slider = float(spikes.get("apm_error_rate", 0) or 0)
    if not channel_active or slider <= 0:
        return baseline
    return max(baseline, min(1.0, slider / 100.0))

# ── Configuration ─────────────────────────────────────────────────────────────
# Batches are sent every BATCH_INTERVAL_MIN..BATCH_INTERVAL_MAX seconds. Shorter
# intervals yield richer per-bucket data for APM ML jobs (especially with smaller
# bucket spans like 1m) — important for demo scenarios where you want anomaly
# scores to surface within a few minutes of fault onset.
BATCH_INTERVAL_MIN = 1
BATCH_INTERVAL_MAX = 2

# Span kind constants
SPAN_KIND_INTERNAL = 1
SPAN_KIND_SERVER = 2
SPAN_KIND_CLIENT = 3

# Status codes
STATUS_OK = 1
STATUS_ERROR = 2


def _load_topology():
    """Load topology data from the active scenario."""
    from scenario_engine import get_scenario
    scenario = get_scenario(ACTIVE_SCENARIO)
    return scenario.service_topology, scenario.entry_endpoints, scenario.db_operations


# ── Module-level topology (loaded once) ──────────────────────────────────────
SERVICE_TOPOLOGY, ENTRY_ENDPOINTS, DB_OPERATIONS = _load_topology()


# ── Helpers ───────────────────────────────────────────────────────────────────
def _gen_trace_id() -> str:
    return secrets.token_hex(16)


def _gen_span_id() -> str:
    return secrets.token_hex(8)


def _build_resource(
    service_name: str,
    services: dict | None = None,
    namespace: str | None = None,
    topology: dict | None = None,
) -> dict:
    _services = services or SERVICES
    _namespace = namespace or NAMESPACE
    cfg = _services[service_name]
    language = cfg.get("language", "python")
    attrs = {
        "service.name": service_name,
        "service.namespace": _namespace,
        "service.version": "1.0.0",
        "service.instance.id": f"{service_name}-001",
        "telemetry.sdk.language": language,
        "telemetry.sdk.name": "opentelemetry",
        "telemetry.sdk.version": "1.24.0",
        "cloud.provider": cfg["cloud_provider"],
        "cloud.platform": cfg["cloud_platform"],
        "cloud.region": cfg["cloud_region"],
        "cloud.availability_zone": cfg["cloud_availability_zone"],
        "deployment.environment": f"production-{_namespace}",
        "host.name": f"{service_name}-host",
        "host.architecture": "amd64",
        "os.type": "linux",
        "data_stream.type": "traces",
        "data_stream.dataset": "generic",
        "data_stream.namespace": "default",
    }
    if topology:
        from generator_engine.infra_topology import get_resource_attrs
        infra = get_resource_attrs(topology, service_name)
        if infra:
            attrs.update(infra)
            entry = topology.get(service_name, {})
            attrs["service.instance.id"] = entry.get("_service_instance_id", f"{service_name}-001")
    # Add process.runtime attributes so Elastic APM can identify the runtime
    _RUNTIME_ATTRS = {
        "java": {
            "process.runtime.name": "OpenJDK Runtime Environment",
            "process.runtime.version": "21.0.5+11-LTS",
            "process.runtime.description": "Eclipse Adoptium OpenJDK 64-Bit Server VM 21.0.5+11-LTS",
        },
        "python": {
            "process.runtime.name": "CPython",
            "process.runtime.version": "3.12.3",
            "process.runtime.description": "CPython 3.12.3",
        },
        "go": {
            "process.runtime.name": "go",
            "process.runtime.version": "go1.22.4",
            "process.runtime.description": "go1.22.4 linux/amd64",
        },
        "dotnet": {
            "process.runtime.name": ".NET",
            "process.runtime.version": "8.0.6",
            "process.runtime.description": ".NET 8.0.6",
        },
        "rust": {
            "process.runtime.name": "rustc",
            "process.runtime.version": "1.79.0",
            "process.runtime.description": "rustc 1.79.0",
        },
        "cpp": {
            "process.runtime.name": "gcc",
            "process.runtime.version": "13.2.0",
            "process.runtime.description": "GCC 13.2.0",
        },
    }
    if language in _RUNTIME_ATTRS:
        attrs.update(_RUNTIME_ATTRS[language])
    return {
        "attributes": _format_attributes(attrs),
        "schemaUrl": SCHEMA_URL,
    }


def _resolve_exception(
    service_name: str,
    services: dict,
    rng: random.Random,
    channel_registry: dict | None = None,
    active_channels: list[int] | None = None,
    scenario=None,
) -> tuple[str, str, str | None]:
    """Return ``(exception_type, message, stacktrace)`` for an error span.

    If the service is affected by an active chaos channel, uses the channel's
    error_type/error_message/stack_trace. Otherwise uses a generic exception
    for the service's language.
    """
    if active_channels and channel_registry and scenario:
        for ch_id in active_channels:
            ch = channel_registry.get(ch_id, {})
            if service_name in ch.get("affected_services", []):
                fault_params = scenario.get_fault_params(ch_id)
                error_type = ch.get("error_type", "Error")
                import string

                class SafeDict(dict):
                    def __missing__(self, key):
                        return f"{{{key}}}"

                fmt = string.Formatter()
                message = fmt.vformat(
                    ch.get("error_message", "error"), (), SafeDict(fault_params)
                )
                stacktrace = fmt.vformat(
                    ch.get("stack_trace", ""), (), SafeDict(fault_params)
                )
                return error_type, message, stacktrace or None

    language = services.get(service_name, {}).get("language", "python")
    exceptions = _GENERIC_EXCEPTIONS.get(language, _GENERIC_EXCEPTIONS["python"])
    exc_type, exc_msg = rng.choice(exceptions)
    return exc_type, exc_msg, None


def _build_exception_log(
    client: OTLPClient,
    *,
    exc_type: str,
    message: str,
    stacktrace: str | None,
    trace_id: str,
    span_id: str,
) -> dict:
    """OTLP log with ``body`` set so ``message`` (alias of ``body.text``) is non-null.

    Elastic indexes span ``exception`` events into ``logs-generic.otel`` as APM
    error docs without ``body.text``. The system_otel Host Details Logs panel
    KEEP's ``message`` (→ ``body.text``), so those rows render as (null). Emitting
    a real log with the exception message keeps that panel populated.
    """
    attrs: dict[str, Any] = {
        "exception.type": exc_type,
        "exception.message": message,
        "event.name": "exception",
    }
    if stacktrace:
        attrs["exception.stacktrace"] = stacktrace
    return client.build_log_record(
        severity="ERROR",
        body=message,
        attributes=attrs,
        trace_id=trace_id,
        span_id=span_id,
        event_name="exception",
    )


def _get_db_chaos_info(
    service_name: str,
    channel_registry: dict | None,
    active_channels: list[int] | None,
) -> dict | None:
    """Check if a service is affected by a database-subsystem chaos channel.

    Returns the channel dict if active, else None.
    Generic: uses channel_registry 'subsystem' field, not hardcoded channel IDs.
    """
    if not active_channels or not channel_registry:
        return None
    for ch_id in active_channels:
        ch = channel_registry.get(ch_id, {})
        if ch.get("subsystem") != "database":
            continue
        if service_name in ch.get("affected_services", []):
            return ch
    return None


def _get_cascade_http_status(
    service_name: str,
    channel_registry: dict | None,
    active_channels: list[int] | None,
    rng: random.Random,
) -> int:
    """Return a role-appropriate HTTP error status for a service in an active chaos channel.

    - affected_services → 504 (Gateway Timeout — waiting for resource)
    - cascade_services (mid-tier) → 503 (Service Unavailable)
    - cascade_services (last/edge) → 502 (Bad Gateway)
    - No match → random from [500, 502, 503]
    """
    if not active_channels or not channel_registry:
        return rng.choice([500, 502, 503])

    for ch_id in active_channels:
        ch = channel_registry.get(ch_id, {})
        affected = ch.get("affected_services", [])
        cascade = ch.get("cascade_services", [])

        if service_name in affected:
            return 504
        if service_name in cascade:
            idx = cascade.index(service_name)
            return 502 if idx == len(cascade) - 1 else 503

    return rng.choice([500, 502, 503])


def _build_db_connection_map(
    services: dict, topology: dict, db_operations: dict, namespace: str,
    db_system: str = "postgresql",
) -> dict[str, dict]:
    """Build a mapping of (service, table) → DB connection info.

    Derives which database service each app service connects to from
    the service_topology, then maps each table to that database's attributes.
    Returns {service_name: {table_name: {db_system, db_name, host, port}}}.

    db_system: the value for the db.system span attribute, derived from the
    enabled database generator's config (defaults to "postgresql" when no
    generator config is present, preserving historical behaviour).
    """
    # Find database services
    db_services = {
        name for name, cfg in services.items()
        if cfg.get("subsystem") == "database"
    }
    if not db_services:
        return {}

    # Build reverse map: app_service → database_service from topology
    svc_to_db: dict[str, str] = {}
    for caller, calls in topology.items():
        if caller in db_services:
            continue
        for callee, _ep, _method in calls:
            if callee in db_services:
                svc_to_db[caller] = callee

    # Build per-(service, table) connection info
    result: dict[str, dict] = {}
    for svc, ops in db_operations.items():
        if svc in db_services:
            continue
        db_svc = svc_to_db.get(svc)
        if not db_svc:
            continue  # no DB service in topology for this service
        table_map = {}
        for op, table, statement in ops:
            if table not in table_map:
                table_map[table] = {
                    "db_system": db_system,
                    "db_name": ScenarioIdentity(namespace).database(table),
                    "host": f"{db_svc}-host",
                    "port": 5432,
                    "db_service": db_svc,
                }
        result[svc] = table_map

    return result


def _generate_trace(client: OTLPClient, resources: dict, rng: random.Random,
                    chaos_affected: set[str] | None = None,
                    *, services: dict | None = None, namespace: str | None = None,
                    service_topology: dict | None = None,
                    entry_endpoints: dict | None = None,
                    db_operations: dict | None = None,
                    latency_multiplier: float = 1.0,
                    error_rate: float = 0.0,
                    scenario=None,
                    active_channels: list[int] | None = None,
                    channel_registry: dict | None = None,
                    db_connection_map: dict | None = None,
                    chaos_cascade: set[str] | None = None) -> tuple[dict[str, list], list[tuple[int, str, str, str]], dict[str, list]]:
    """Generate a single distributed trace across multiple services.

    Returns (spans_by_service, error_publish, logs_by_service) where:
      * spans_by_service maps service_name -> list of spans for that service
      * error_publish is a list of (channel_id, svc, trace_id, span_id) tuples
        for error spans tied to active channels — used to populate the per-channel
        trace context store so fault logs can pivot to the actual error trace.
      * logs_by_service maps service_name -> exception logs with ``body`` set so
        system_otel Host Details Logs ``message`` (alias of ``body.text``) is
        non-null. Span exception events alone are indexed without body.text.

    When chaos_affected is provided, those services use ``error_rate`` for elevated
    errors and elevated latency; all others use a healthy 3% baseline.
    """
    _services = services or SERVICES
    _namespace = namespace or NAMESPACE
    _topology = service_topology or SERVICE_TOPOLOGY
    _endpoints = entry_endpoints or ENTRY_ENDPOINTS
    _db_ops = db_operations or DB_OPERATIONS
    _db_conn = db_connection_map or {}
    logs_by_service: dict[str, list] = {}

    trace_id = _gen_trace_id()
    spans_by_service: dict[str, list] = {}
    error_publish: list[tuple[int, str, str, str]] = []

    def _record_error_span(svc: str, span_id_: str) -> None:
        """Track this error span for per-channel trace context publishing."""
        if _svc_channels.get(svc):
            for ch_id in _svc_channels[svc]:
                error_publish.append((ch_id, svc, trace_id, span_id_))

    def _emit_exception_log(
        svc: str,
        span_id_: str,
        *,
        exc_type: str | None = None,
        message: str | None = None,
        stacktrace: str | None = None,
    ) -> None:
        """Record an exception log with body.text for Host Details Logs."""
        if exc_type is None or message is None:
            exc_type, message, stacktrace = _resolve_exception(
                svc,
                _services,
                rng,
                channel_registry=channel_registry,
                active_channels=active_channels,
                scenario=scenario,
            )
        logs_by_service.setdefault(svc, []).append(
            _build_exception_log(
                client,
                exc_type=exc_type,
                message=message,
                stacktrace=stacktrace,
                trace_id=trace_id,
                span_id=span_id_,
            )
        )

    # Pick a random entry-point service (weighted toward first service)
    service_names = list(_services.keys())
    first_service = service_names[0]
    entry_services = [first_service] * 4 + [
        s for s in service_names[1:] if s in _endpoints
    ]
    entry_service = rng.choice(entry_services)
    entry_endpoint, entry_method = rng.choice(_endpoints[entry_service])

    # Determine if this trace has errors — chaos-aware probability
    effective_error_rate = max(error_rate, 0.03) if error_rate > 0 else 0.03
    if chaos_affected and entry_service in chaos_affected and error_rate > 0:
        is_error_trace = rng.random() < effective_error_rate
    elif chaos_cascade and entry_service in chaos_cascade:
        is_error_trace = rng.random() < CASCADE_ERROR_RATE
    else:
        is_error_trace = rng.random() < 0.03

    error_service = None
    if is_error_trace:
        # If entry service is affected by chaos, it is the error source
        if (chaos_affected and entry_service in chaos_affected) or (
            chaos_cascade and entry_service in chaos_cascade
        ):
            error_service = entry_service
        else:
            downstream = _topology.get(entry_service, [])
            if downstream:
                error_service = rng.choice(downstream)[0]

    # Latency: affected services get 200-2000ms, normal get 50-500ms
    if chaos_affected and entry_service in chaos_affected:
        total_duration = rng.randint(200, 2000)
    else:
        total_duration = rng.randint(50, 500)
    # Apply latency multiplier from infra spikes
    total_duration = int(total_duration * latency_multiplier)

    # Build per-service -> channel mapping for RCA clues
    _svc_channels: dict[str, list[int]] = {}
    if active_channels and channel_registry:
        for ch_id in active_channels:
            ch = channel_registry.get(ch_id, {})
            for svc in ch.get("affected_services", []) + ch.get("cascade_services", []):
                _svc_channels.setdefault(svc, []).append(ch_id)

    # Root SERVER span for the entry-point service
    root_span_id = _gen_span_id()
    root_status = STATUS_ERROR if (is_error_trace and error_service == entry_service) else STATUS_OK
    root_http_status = _get_cascade_http_status(entry_service, channel_registry, active_channels, rng) if root_status == STATUS_ERROR else 200

    # Helper: build extra scenario attrs for a span
    def _extra_attrs(svc: str, is_err: bool) -> dict:
        extra = {}
        if scenario:
            extra.update(scenario.get_trace_attributes(svc, rng))
            if _svc_channels.get(svc):
                for ch in _svc_channels[svc]:
                    extra.update(scenario.get_rca_clues(ch, svc, rng))
            if active_channels:
                # Use high probability (90%) for both errors AND affected services
                # (affected services have elevated latency, so attribute must
                # correlate with slow traces too, not just failed ones)
                is_affected = bool(_svc_channels.get(svc))
                for ch in active_channels:
                    extra.update(scenario.get_correlation_attribute(ch, is_err or is_affected, rng))
        # Canonical channel join key — single attribute filterable across spans, logs, metrics
        if _svc_channels.get(svc) and channel_registry:
            primary_ch = _svc_channels[svc][0]
            ch_entry = channel_registry.get(primary_ch, {})
            extra["chaos.channel"] = primary_ch
            if ch_entry.get("name"):
                extra["chaos.fault_type"] = ch_entry["name"]
            if ch_entry.get("subsystem"):
                extra["chaos.subsystem"] = ch_entry["subsystem"]
            if is_err and ch_entry.get("error_type"):
                extra["error.type"] = ch_entry["error_type"]
        return extra

    root_attrs = {
        "http.request.method": entry_method,
        "url.path": entry_endpoint,
        "http.response.status_code": root_http_status,
        "server.address": f"{entry_service}-host",
        "server.port": 8080,
        "network.protocol.version": "1.1",
    }
    root_attrs.update(_extra_attrs(entry_service, root_status == STATUS_ERROR))

    # Do NOT attach OTel span exception events: Elastic indexes them into
    # logs-generic.otel without body.text, and system_otel Host Details Logs
    # KEEP's message (alias of body.text) → (null). Emit an error log with body
    # instead; span status + exception.* attrs still mark the failed span.
    if root_status == STATUS_ERROR:
        exc_type, exc_msg, exc_stack = _resolve_exception(
            entry_service,
            _services,
            rng,
            channel_registry=channel_registry,
            active_channels=active_channels,
            scenario=scenario,
        )
        root_attrs["exception.type"] = exc_type
        root_attrs["exception.message"] = exc_msg
        _emit_exception_log(
            entry_service,
            root_span_id,
            exc_type=exc_type,
            message=exc_msg,
            stacktrace=exc_stack,
        )

    root_span = client.build_span(
        name=f"{entry_method} {entry_endpoint}",
        trace_id=trace_id,
        span_id=root_span_id,
        kind=SPAN_KIND_SERVER,
        duration_ms=total_duration,
        status_code=root_status,
        attributes=root_attrs,
    )
    spans_by_service.setdefault(entry_service, []).append(root_span)
    if root_status == STATUS_ERROR:
        _record_error_span(entry_service, root_span_id)

    # Add DB span if this service does DB operations
    if entry_service in _db_ops and rng.random() < 0.6:
        op, table, statement = rng.choice(_db_ops[entry_service])
        db_info = _db_conn.get(entry_service, {}).get(table)
        db_span_id = _gen_span_id()

        # Chaos-aware: DB spans dominate trace when DB subsystem is under fault
        _db_chaos = _get_db_chaos_info(entry_service, channel_registry, active_channels)
        if _db_chaos:
            db_duration = rng.randint(int(total_duration * 0.6), max(int(total_duration * 0.6) + 1, int(total_duration * 0.9)))
            db_status = STATUS_ERROR
            _pool_active = rng.randint(95, 100)
            _wait_queue = rng.randint(200, 500)
            _wait_ms = rng.randint(3000, 5000)
            _db_exc_type = "org.postgresql.util.PSQLException"
            _db_exc_msg = (
                f"Cannot acquire connection from pool: active={_pool_active}/100, "
                f"waitQueue={_wait_queue}, timeout after {_wait_ms}ms"
            )
            _db_exc_stack = (
                "org.postgresql.util.PSQLException: Cannot acquire connection from pool\n"
                "  at com.zaxxer.hikari.pool.HikariPool.createTimeoutException(HikariPool.java:696)\n"
                "  at com.zaxxer.hikari.pool.HikariPool.getConnection(HikariPool.java:197)\n"
                "  at com.zaxxer.hikari.HikariDataSource.getConnection(HikariDataSource.java:100)"
            )
        else:
            db_duration = rng.randint(2, min(30, total_duration // 3))
            db_status = STATUS_OK
            _db_exc_type = _db_exc_msg = _db_exc_stack = None

        _db_attrs = {
            "db.system": db_info["db_system"] if db_info else "postgresql",
            "db.name": db_info["db_name"] if db_info else ScenarioIdentity(_namespace).database("telemetry"),
            "db.statement": statement,
            "db.operation": op,
            "db.sql.table": table,
            "server.address": db_info["host"] if db_info else ScenarioIdentity(_namespace).label("db-host"),
            "server.port": db_info["port"] if db_info else 5432,
        }
        _db_attrs.update(_extra_attrs(entry_service, db_status == STATUS_ERROR))
        if db_status == STATUS_ERROR and _db_exc_type:
            _db_attrs["exception.type"] = _db_exc_type
            _db_attrs["exception.message"] = _db_exc_msg
        db_span = client.build_span(
            name=f"{op} {table}",
            trace_id=trace_id,
            span_id=db_span_id,
            parent_span_id=root_span_id,
            kind=SPAN_KIND_CLIENT,
            duration_ms=db_duration,
            status_code=db_status,
            attributes=_db_attrs,
        )
        spans_by_service.setdefault(entry_service, []).append(db_span)
        if db_status == STATUS_ERROR:
            _record_error_span(entry_service, db_span_id)
            _emit_exception_log(
                entry_service,
                db_span_id,
                exc_type=_db_exc_type,
                message=_db_exc_msg,
                stacktrace=_db_exc_stack,
            )

    # Generate downstream CLIENT+SERVER spans based on topology
    downstream_calls = _topology.get(entry_service, [])
    if downstream_calls:
        # Pick 1-3 downstream calls
        num_calls = min(len(downstream_calls), rng.randint(1, 3))
        selected_calls = rng.sample(downstream_calls, num_calls)

        for callee_service, callee_endpoint, callee_method in selected_calls:
            # Skip database-subsystem callees — they appear as DB deps, not HTTP services
            callee_cfg = _services.get(callee_service, {})
            if callee_cfg.get("subsystem") == "database":
                continue

            # Chaos-aware: affected callee services get elevated latency + error chance
            if chaos_affected and callee_service in chaos_affected and error_rate > 0:
                call_duration = rng.randint(100, max(200, total_duration // 2))
                callee_error = rng.random() < effective_error_rate
            elif chaos_cascade and callee_service in chaos_cascade:
                call_duration = rng.randint(30, max(60, total_duration // 3))
                callee_error = rng.random() < CASCADE_ERROR_RATE
            else:
                call_duration = rng.randint(10, max(20, total_duration // 2))
                callee_error = False

            is_this_error = (is_error_trace and callee_service == error_service) or callee_error
            call_status = STATUS_ERROR if is_this_error else STATUS_OK
            call_http_status = _get_cascade_http_status(callee_service, channel_registry, active_channels, rng) if is_this_error else 200

            # CLIENT span on the caller side
            client_span_id = _gen_span_id()
            _client_attrs = {
                "http.request.method": callee_method,
                "url.path": callee_endpoint,
                "http.response.status_code": call_http_status,
                "server.address": f"{callee_service}-host",
                "server.port": 8080,
                "net.peer.name": f"{callee_service}-host",
                "net.peer.port": 8080,
            }
            # Tag with caller's channel if affected; falls back to callee if caller isn't
            _client_attrs.update(_extra_attrs(entry_service if _svc_channels.get(entry_service) else callee_service, is_this_error))
            client_span = client.build_span(
                name=f"{callee_method} {callee_endpoint}",
                trace_id=trace_id,
                span_id=client_span_id,
                parent_span_id=root_span_id,
                kind=SPAN_KIND_CLIENT,
                duration_ms=call_duration,
                status_code=call_status,
                attributes=_client_attrs,
            )
            spans_by_service.setdefault(entry_service, []).append(client_span)

            # SERVER span on the callee side
            server_span_id = _gen_span_id()
            server_duration = call_duration - rng.randint(1, max(1, call_duration // 5))
            callee_attrs = {
                "http.request.method": callee_method,
                "url.path": callee_endpoint,
                "http.response.status_code": call_http_status,
                "server.address": f"{callee_service}-host",
                "server.port": 8080,
            }
            callee_attrs.update(_extra_attrs(callee_service, is_this_error))

            if call_status == STATUS_ERROR:
                exc_type, exc_msg, exc_stack = _resolve_exception(
                    callee_service,
                    _services,
                    rng,
                    channel_registry=channel_registry,
                    active_channels=active_channels,
                    scenario=scenario,
                )
                callee_attrs["exception.type"] = exc_type
                callee_attrs["exception.message"] = exc_msg
                _emit_exception_log(
                    callee_service,
                    server_span_id,
                    exc_type=exc_type,
                    message=exc_msg,
                    stacktrace=exc_stack,
                )

            server_span = client.build_span(
                name=f"{callee_method} {callee_endpoint}",
                trace_id=trace_id,
                span_id=server_span_id,
                parent_span_id=client_span_id,
                kind=SPAN_KIND_SERVER,
                duration_ms=max(1, server_duration),
                status_code=call_status,
                attributes=callee_attrs,
            )
            spans_by_service.setdefault(callee_service, []).append(server_span)
            if call_status == STATUS_ERROR:
                _record_error_span(callee_service, server_span_id)

            # DB span on the callee side (if applicable)
            if callee_service in _db_ops and rng.random() < 0.5:
                op, table, statement = rng.choice(_db_ops[callee_service])
                db_info = _db_conn.get(callee_service, {}).get(table)
                db_span_id = _gen_span_id()

                # Chaos-aware: DB spans dominate trace when DB subsystem is under fault
                _db_chaos = _get_db_chaos_info(callee_service, channel_registry, active_channels)
                if _db_chaos:
                    db_duration = rng.randint(int(server_duration * 0.6), max(int(server_duration * 0.6) + 1, int(server_duration * 0.9)))
                    db_status = STATUS_ERROR
                    _pool_active = rng.randint(95, 100)
                    _wait_queue = rng.randint(200, 500)
                    _wait_ms = rng.randint(3000, 5000)
                    _db_exc_type = "org.postgresql.util.PSQLException"
                    _db_exc_msg = (
                        f"Cannot acquire connection from pool: active={_pool_active}/100, "
                        f"waitQueue={_wait_queue}, timeout after {_wait_ms}ms"
                    )
                    _db_exc_stack = (
                        "org.postgresql.util.PSQLException: Cannot acquire connection from pool\n"
                        "  at com.zaxxer.hikari.pool.HikariPool.createTimeoutException(HikariPool.java:696)\n"
                        "  at com.zaxxer.hikari.pool.HikariPool.getConnection(HikariPool.java:197)\n"
                        "  at com.zaxxer.hikari.HikariDataSource.getConnection(HikariDataSource.java:100)"
                    )
                else:
                    db_duration = rng.randint(1, max(1, server_duration // 3))
                    db_status = STATUS_OK
                    _db_exc_type = _db_exc_msg = _db_exc_stack = None

                _db_attrs2 = {
                    "db.system": db_info["db_system"] if db_info else "postgresql",
                    "db.name": db_info["db_name"] if db_info else ScenarioIdentity(_namespace).database("telemetry"),
                    "db.statement": statement,
                    "db.operation": op,
                    "db.sql.table": table,
                    "server.address": db_info["host"] if db_info else ScenarioIdentity(_namespace).label("db-host"),
                    "server.port": db_info["port"] if db_info else 5432,
                }
                _db_attrs2.update(_extra_attrs(callee_service, db_status == STATUS_ERROR))
                if db_status == STATUS_ERROR and _db_exc_type:
                    _db_attrs2["exception.type"] = _db_exc_type
                    _db_attrs2["exception.message"] = _db_exc_msg
                db_span = client.build_span(
                    name=f"{op} {table}",
                    trace_id=trace_id,
                    span_id=db_span_id,
                    parent_span_id=server_span_id,
                    kind=SPAN_KIND_CLIENT,
                    duration_ms=db_duration,
                    status_code=db_status,
                    attributes=_db_attrs2,
                )
                spans_by_service.setdefault(callee_service, []).append(db_span)
                if db_status == STATUS_ERROR:
                    _record_error_span(callee_service, db_span_id)
                    _emit_exception_log(
                        callee_service,
                        db_span_id,
                        exc_type=_db_exc_type,
                        message=_db_exc_msg,
                        stacktrace=_db_exc_stack,
                    )

            # Second-level downstream calls (e.g., navigation -> sensor-validator)
            second_downstream = _topology.get(callee_service, [])
            if second_downstream and rng.random() < 0.4:
                second_callee, second_endpoint, second_method = rng.choice(second_downstream)
                # Skip database callees
                second_callee_cfg = _services.get(second_callee, {})
                if second_callee_cfg.get("subsystem") == "database":
                    continue
                second_duration = rng.randint(5, max(5, server_duration // 2))
                second_status = STATUS_OK

                # CLIENT span
                second_client_id = _gen_span_id()
                _second_client_attrs = {
                    "http.request.method": second_method,
                    "url.path": second_endpoint,
                    "http.response.status_code": 200,
                    "server.address": f"{second_callee}-host",
                    "server.port": 8080,
                    "net.peer.name": f"{second_callee}-host",
                    "net.peer.port": 8080,
                }
                _second_client_attrs.update(_extra_attrs(callee_service if _svc_channels.get(callee_service) else second_callee, False))
                second_client_span = client.build_span(
                    name=f"{second_method} {second_endpoint}",
                    trace_id=trace_id,
                    span_id=second_client_id,
                    parent_span_id=server_span_id,
                    kind=SPAN_KIND_CLIENT,
                    duration_ms=second_duration,
                    status_code=second_status,
                    attributes=_second_client_attrs,
                )
                spans_by_service.setdefault(callee_service, []).append(second_client_span)

                # SERVER span
                second_server_id = _gen_span_id()
                _second_server_attrs = {
                    "http.request.method": second_method,
                    "url.path": second_endpoint,
                    "http.response.status_code": 200,
                    "server.address": f"{second_callee}-host",
                    "server.port": 8080,
                }
                _second_server_attrs.update(_extra_attrs(second_callee, False))
                second_server_span = client.build_span(
                    name=f"{second_method} {second_endpoint}",
                    trace_id=trace_id,
                    span_id=second_server_id,
                    parent_span_id=second_client_id,
                    kind=SPAN_KIND_SERVER,
                    duration_ms=max(1, second_duration - 2),
                    status_code=second_status,
                    attributes=_second_server_attrs,
                )
                spans_by_service.setdefault(second_callee, []).append(second_server_span)

    return spans_by_service, error_publish, logs_by_service


# ── Run loop (used by ServiceManager and standalone) ──────────────────────────

from generators.traces import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop

def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run trace generator loop until stop_event is set."""
    rng = random.Random()

    # Host Details Logs Log Rate Analysis: Claude Code MCP writes
    # attributes.error as a string; passthrough promotes it to keyword ``error``,
    # conflicting with Bedrock's object ``error`` (ALERTS #115).
    try:
        from generator_engine.logs_otel_error_heal import (
            ensure_error_object_mapping_from_env,
        )
        ensure_error_object_mapping_from_env()
    except Exception:
        logger.exception("logs-otel error-field heal at traces start failed")

    # Keep metrics-transaction.1m.otel fed after deploy-time backfill so
    # managed APM ML jobs do not starve (ALERTS #118). Pass chaos latency so
    # APM ML sees infra latency spikes (rollup is what those jobs read).
    _scenario_obj = (scenario_data or {}).get("scenario")
    if _scenario_obj is not None and getattr(client, "elastic_url", None) and getattr(client, "api_key", None):
        try:
            from elastic_config.apm_rollup import start_live_rollup_thread

            def _lat_mult() -> float:
                if not chaos_controller:
                    return 1.0
                spikes = chaos_controller.get_infra_spikes() or {}
                return float(spikes.get("latency_multiplier", 1.0) or 1.0)

            def _err_rate() -> float:
                # Small baseline; channel chaos is reflected in traces. The Chaos UI
                # failed-transaction-rate slider overrides it while a channel is active.
                if not chaos_controller:
                    return 0.01
                return rollup_error_rate(
                    chaos_controller.get_infra_spikes() or {},
                    bool(chaos_controller.get_active_channels()),
                )

            start_live_rollup_thread(
                _scenario_obj, client.elastic_url, client.api_key, stop_event,
                get_latency_multiplier=_lat_mult,
                get_error_rate=_err_rate,
            )
        except Exception:
            logger.exception("APM live rollup thread start failed")

    # Use scenario_data overrides or fall back to module-level globals
    _all_services = scenario_data["services"] if scenario_data else SERVICES
    _namespace = scenario_data["namespace"] if scenario_data else NAMESPACE
    _channel_registry = scenario_data["channel_registry"] if scenario_data else CHANNEL_REGISTRY
    _all_topology = scenario_data["service_topology"] if scenario_data else SERVICE_TOPOLOGY
    _all_endpoints = scenario_data["entry_endpoints"] if scenario_data else ENTRY_ENDPOINTS
    _db_ops = scenario_data["db_operations"] if scenario_data else DB_OPERATIONS
    _scenario = scenario_data.get("scenario") if scenario_data else None

    # Filter out services that don't generate traces (infra devices, databases)
    excluded = {
        name for name, cfg in _all_services.items()
        if cfg.get("generates_traces") is False or cfg.get("subsystem") == "database"
    }
    _services = {name: cfg for name, cfg in _all_services.items() if name not in excluded}
    _topology = {
        caller: [
            (callee, ep, method) for callee, ep, method in calls
            # A callee with no service definition has no OTLP resource; building a
            # span for it used to KeyError and kill the whole generator thread.
            if callee not in excluded and callee in _all_services
        ]
        for caller, calls in _all_topology.items() if caller not in excluded
    }
    _dangling = sorted({
        callee for calls in _all_topology.values() for callee, _ep, _m in calls
        if callee not in _all_services
    })
    if _dangling:
        logger.warning(
            "topology references undefined service(s) %s - skipping those edges "
            "(define services/<name>.yaml or remove the edge)", _dangling,
        )
    _endpoints = {name: eps for name, eps in _all_endpoints.items() if name not in excluded}

    if excluded:
        logger.info("Excluding %d infra services from traces: %s", len(excluded), ", ".join(sorted(excluded)))

    # Determine db.system from the enabled database generator config.
    # The first enabled database generator that declares db_system wins.
    # Defaults to "postgresql" to preserve historical behaviour when no
    # generators: block is present.
    _db_system = "postgresql"
    if scenario_data:
        _gen_cfg = scenario_data.get("generator_config") or {}
        for _db_gen_id in ("postgresql_otel", "mysql_otel", "microsoft_sqlserver_otel", "oracle_otel", "mongodb_otel"):
            _gc = _gen_cfg.get(_db_gen_id)
            if isinstance(_gc, dict) and _gc.get("db_system"):
                _db_system = _gc["db_system"]
                break
            elif isinstance(_gc, dict) and _db_gen_id in ("postgresql_otel", "mysql_otel", "microsoft_sqlserver_otel", "oracle_otel"):
                # Generator is enabled without an explicit db_system: use its id
                _db_system = _db_gen_id
                break

    # Build DB connection map for per-database span attributes
    _db_conn_map = _build_db_connection_map(_all_services, _all_topology, _db_ops, _namespace, db_system=_db_system)
    if _db_conn_map:
        logger.info("DB connection map (db.system=%s): %s", _db_system, {svc: list(tables.keys()) for svc, tables in _db_conn_map.items()})

    _topology_data = None
    if scenario_data:
        from generator_engine.infra_topology import build_topology as _build_infra_topology
        _topology_data = _build_infra_topology(scenario_data)

    resources = {svc: _build_resource(svc, services=_services, namespace=_namespace, topology=_topology_data) for svc in _services}
    total_traces = 0
    total_spans = 0

    logger.info("Trace generator started (chaos_aware=%s)", chaos_controller is not None)

    sleep_time = None
    def _emit_tick() -> None:
        nonlocal sleep_time, total_spans, total_traces
        chaos_affected: set[str] = set()
        chaos_cascade: set[str] = set()
        _active_channels: list[int] = []
        if chaos_controller:
            _active_channels = chaos_controller.get_active_channels()
            for ch_id in _active_channels:
                ch = _channel_registry.get(ch_id)
                if ch:
                    chaos_affected.update(ch["affected_services"])
                    chaos_cascade.update(ch.get("cascade_services", []))
        # A service that is directly affected keeps the full fault error rate.
        chaos_cascade -= chaos_affected

        _trace_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="traces",
            chaos_mode="any",
            impact_keys=_ALERT_SPEC.impact_keys,
        ) if chaos_controller else None

        # resolve_active_chaos only applies SPEC defaults when a channel declares
        # generator_impact.traces; with chaos_mode="any" a channel that doesn't
        # would leave affected services at the healthy baseline. Fall back to the
        # SPEC default so every active channel visibly errors its services.
        _error_rate = float(
            (_trace_chaos or {}).get(
                "error_rate",
                _ALERT_SPEC.impact_keys["error_rate"]["default"] if _trace_chaos else 0.0,
            )
        )

        # Read latency multiplier and APM augmentations from infra spikes and generator impact
        _latency_mult = 1.0
        _spikes = chaos_controller.get_infra_spikes() if chaos_controller else {}
        if chaos_controller:
            _latency_mult = float(_spikes.get("latency_multiplier", 1.0))
            _error_rate, _latency_mult = apply_apm_augmentations(
                _error_rate, _latency_mult, _spikes, bool(chaos_affected),
            )
        if _trace_chaos:
            _latency_mult = max(_latency_mult, float(_trace_chaos.get("latency_multiplier", 1.0)))

        num_traces = rng.randint(8, 14)

        batch_by_service: dict[str, list] = {}
        logs_batch_by_service: dict[str, list] = {}
        for _ in range(num_traces):
            trace_spans, error_publish, trace_logs = _generate_trace(
                client, resources, rng, chaos_affected or None,
                chaos_cascade=chaos_cascade or None,
                services=_services, namespace=_namespace,
                service_topology=_topology, entry_endpoints=_endpoints,
                db_operations=_db_ops,
                latency_multiplier=_latency_mult,
                error_rate=_error_rate,
                scenario=_scenario,
                active_channels=_active_channels or None,
                channel_registry=_channel_registry,
                db_connection_map=_db_conn_map,
            )
            for svc, spans in trace_spans.items():
                batch_by_service.setdefault(svc, []).extend(spans)
                # Publish latest trace context for log-trace correlation
                if spans:
                    _trace_context_store.set(svc, spans[0]["traceId"], spans[0]["spanId"], namespace=_namespace)
            for svc, logs in trace_logs.items():
                logs_batch_by_service.setdefault(svc, []).extend(logs)
            # Publish per-channel error trace context so fault logs can pivot to the
            # actual error trace rather than the last-seen-per-service trace.
            for ch_id, svc, t_id, s_id in error_publish:
                _trace_context_store.set_for_channel(ch_id, svc, t_id, s_id, namespace=_namespace)

        # Send ALL services in one OTLP POST. Each service's spans become a
        # separate `resourceSpans` entry in the same request body.
        multi_batch = [(resources[svc], spans) for svc, spans in batch_by_service.items() if spans]
        batch_span_count = sum(len(spans) for _, spans in multi_batch)
        if multi_batch:
            client.send_traces_multi(multi_batch)

        # Exception logs with body.text → message on system_otel Host Details Logs.
        for svc, log_records in logs_batch_by_service.items():
            if log_records and svc in resources:
                client.send_logs(resources[svc], log_records)

        total_traces += num_traces
        total_spans += batch_span_count
        logger.info(
            "Sent %d traces (%d spans) — total: %d traces, %d spans",
            num_traces, batch_span_count, total_traces, total_spans,
        )

        sleep_time = rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX)
    run_tick_loop(stop_event, _emit_tick, interval=lambda: sleep_time)

    logger.info("Trace generator stopped. Total: %d traces, %d spans", total_traces, total_spans)


# ── Standalone entry point ────────────────────────────────────────────────────
def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

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
