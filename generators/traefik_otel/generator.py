#!/usr/bin/env python3
"""Traefik Generator — sends Traefik Prometheus metrics via OTLP.

Emits data matching the traefik_otel content package
(discovery.datasets: traefik.otel).

Metric names match the traefik_otel dashboard ES|QL queries (snake_case
Prometheus style from Traefik's built-in ``/metrics`` endpoint).

Dashboard panels call ``SUM`` / ``MAX`` on Prometheus counter names.
Those aggregations reject ``counter_long``, so counter-shaped series are
emitted as gauges (period deltas for ``SUM``/duration rate panels;
cumulative totals for ``MAX`` KPIs) — same pattern as influxdb_otel /
etcd_otel. Sticky ``counter_long`` mappings from earlier cumulative
sums are cleared on startup by deleting the data stream when needed.

The Process dashboard FD-utilization panel AND-filters
``process_open_fds`` with ``process_max_fds`` on the same document. OTLP
stores one metric per document, so that filter never matches OTLP-only
data — each tick also bulk-indexes a co-located doc via ``ESBulkClient``.

Package dashboards also ship ``CASE(cond, long / long, 0.0)`` which fails
ES|QL type checks (and truncates rates to 0). Deploy rewrites numerators
to ``TO_DOUBLE(...)`` via ``_patch_traefik_otel_dashboards``.

Usage (standalone):
    python3 -m generators.traefik_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity
from generator_engine.scrape_clock import scrape_now_ns, scrape_start_ns

SOURCE_INTEGRATION = "elastic/integrations packages/traefik_otel@0.3.1"

import bisect
import logging
import math
import os
import random
import signal
import threading
import time
from typing import Any

from app.telemetry import ESBulkClient, OTLPClient, _format_attributes
from app.config import NAMESPACE
try:
    from generators.traefik_otel import SPEC
except Exception:  # pragma: no cover
    SPEC = None

from generator_engine.db_common import (
    CounterState,
    build_gauge,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

TRAEFIK_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("traefik-generator")
EMIT_INTERVAL_S = 15.0

_METRICS_DATA_STREAM = "metrics-traefik.otel-default"

# Metric names matching the traefik_otel dashboard ES|QL queries.
METRIC_NAMES = (
    # Overview
    "traefik_entrypoint_requests_total",
    "traefik_router_requests_total",
    "traefik_open_connections",
    "traefik_entrypoint_responses_bytes_total",
    # Services
    "traefik_service_requests_total",
    "traefik_service_responses_bytes_total",
    "traefik_service_request_duration_seconds",
    # TLS / config
    "traefik_entrypoint_requests_tls_total",
    "traefik_tls_certs_not_after",
    "traefik_config_reloads_total",
    "traefik_config_last_reload_success",
    # Process
    "process_cpu_seconds_total",
    "process_resident_memory_bytes",
    "process_open_fds",
    "process_max_fds",
    "go_goroutines",
    "go_memstats_heap_sys_bytes",
    "go_memstats_heap_inuse_bytes",
)

# Panels call SUM/MAX on these — must map as gauges, not counter_long.
_SUM_MAX_AGG_FIELDS = (
    "traefik_entrypoint_requests_total",
    "traefik_entrypoint_responses_bytes_total",
    "traefik_router_requests_total",
    "traefik_service_requests_total",
    "traefik_service_responses_bytes_total",
    "traefik_entrypoint_requests_tls_total",
    "traefik_config_reloads_total",
    "process_cpu_seconds_total",
)

_ENTRYPOINTS = ("web", "websecure")
_SERVICES = ("api@internal", "app-svc@docker", "auth-svc@docker")
_ROUTERS = ("api@internal", "app@docker", "auth@docker", "dashboard@internal")
_HTTP_CODES_OK = ("200", "201", "204", "301", "304")
_HTTP_CODES_4XX = ("400", "404")
_HTTP_CODES_5XX = ("500", "502", "503")
_HTTP_CODES_ERR = _HTTP_CODES_4XX + _HTTP_CODES_5XX
_TLS_VERSIONS = ("TLS 1.2", "TLS 1.3")
_TLS_CIPHERS = (
    "TLS_AES_128_GCM_SHA256",
    "TLS_AES_256_GCM_SHA384",
    "TLS_CHACHA20_POLY1305_SHA256",
    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
)
_CERTS = (
    {"cn": "traefik.example.com", "serial": "a1b2c3d4e5f6"},
    {"cn": "*.apps.example.com", "serial": "f6e5d4c3b2a1"},
    {"cn": "api.example.com", "serial": "112233445566"},
)

PROCESS_MAX_FDS = 1024


def _default_cloud() -> dict:
    return {"provider": "aws", "platform": "aws_ec2",
            "region": "us-east-1", "availability_zone": "us-east-1a"}


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


def build_fd_utilization_doc(
    resource: dict,
    *,
    open_fds: int,
    max_fds: int = PROCESS_MAX_FDS,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc with open + max FDs co-located.

    The Process dashboard FD% panel AND-filters both metrics on the same
    document. OTLP stores one metric per document, so that filter never
    matches OTLP-only data.
    """
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "1",
        "data_stream": {
            "type": "metrics",
            "dataset": "traefik.otel",
            "namespace": "default",
        },
        "attributes": {},
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": TRAEFIK_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "process_open_fds": float(open_fds),
            "process_max_fds": float(max_fds),
        },
    }


def _period_delta_gauge(
    name: str,
    unit: str,
    counter: CounterState,
    attrs: dict | None,
    last_emitted: dict[tuple, int],
) -> dict:
    """Emit the tick-over-tick increase as a gauge (mimics cumulativetodelta + SUM)."""
    key = (name, tuple(sorted((attrs or {}).items())))
    prev = last_emitted.get(key)
    if prev is None:
        last_emitted[key] = counter.value
        delta = 0.0
    else:
        # Emit as float so SUM(field) / DATE_DIFF(...) returns double, which is
        # compatible with the 0.0 (double) fallback in CASE() expressions in
        # the OOTB Traefik dashboards.  long/long integer-divides and mismatches
        # 0.0, causing a verification_exception in ES|QL 9.6.
        delta = float(max(0, int(counter.value) - int(prev)))
        last_emitted[key] = counter.value
    return build_gauge(name, unit, delta, attrs or {})


def _sum_max_fields_mapped_as_counter(elastic_url: str, api_key: str) -> bool:
    """Return True if any SUM/MAX-agg field is still mapped as a TSDB counter."""
    import httpx

    url = (
        f"{elastic_url.rstrip('/')}/{_METRICS_DATA_STREAM}/_field_caps"
        f"?fields={','.join(_SUM_MAX_AGG_FIELDS)}"
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

    fields = resp.json().get("fields", {})
    for name in _SUM_MAX_AGG_FIELDS:
        for meta in fields.get(name, {}).values():
            if meta.get("time_series_metric") == "counter" or meta.get("type") == "counter_long":
                return True
    return False


def _reset_counter_long_mapping() -> None:
    """Delete the metrics data stream if SUM/MAX-agg fields are stuck as counters.

    Fresh gauge samples then recreate the stream without ``counter_long``,
    unblocking Overview / Services / TLS / Process ``SUM``/``MAX`` panels.
    No-op when ES credentials are absent or mapping is already correct.
    """
    try:
        from app.telemetry import ESBulkClient as _Bulk
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for counter mapping check: %s", exc)
        return

    client = _Bulk()
    if not client.configured:
        return

    if not _sum_max_fields_mapped_as_counter(client.elastic_url, client.api_key):
        logger.info(
            "traefik SUM/MAX-agg fields are not mapped as counters; leaving %s alone",
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
            "Deleted %s so SUM/MAX-agg fields remap as gauges (were counter_long)",
            _METRICS_DATA_STREAM,
        )
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist yet", _METRICS_DATA_STREAM)
    else:
        logger.warning(
            "Could not delete %s (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )


# Healthy per-tick chance that one 4xx / 5xx code advances by one request on
# a given entrypoint / service / router. ~0.06 5xx per service per tick against
# ~25-130 OK requests keeps the 5xx share around 0.1%.
_HEALTHY_4XX_P = 0.3
_HEALTHY_5XX_P = 0.02


def _healthy_error_drift(rng: random.Random, code: str) -> int:
    p = _HEALTHY_5XX_P if code in _HTTP_CODES_5XX else _HEALTHY_4XX_P
    return 1 if rng.random() < p else 0


# traefik_service_request_duration_seconds buckets (Traefik ``buckets`` option).
# 0.2 is a bound so the package "Service latency under 200 ms" histogram SLO
# (range 0-0.2) splits cleanly at the bucket edge.
DURATION_BOUNDS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 1.0, 2.5, 5.0]
# Healthy latency: lognormal around 30 ms (P(>200 ms) ~0.1%).
_HEALTHY_LATENCY_MEDIAN_S = 0.03
_HEALTHY_LATENCY_SIGMA = 0.6


def latency_samples(rng: random.Random, n: int) -> list[float]:
    """Healthy per-request service durations (seconds) for one scrape interval."""
    mu = math.log(_HEALTHY_LATENCY_MEDIAN_S)
    return [rng.lognormvariate(mu, _HEALTHY_LATENCY_SIGMA) for _ in range(max(0, n))]


def build_duration_histogram(
    samples: list[float], attributes: dict, start_time_ns: str,
) -> dict:
    """DELTA OTLP explicit-bucket histogram of one interval's request durations.

    Delta (the shape after the EDOT ``cumulativetodelta`` processor) so each
    document holds one interval's latencies and the SLO range / value_count sums
    reflect the current distribution.
    """
    counts = [0] * (len(DURATION_BOUNDS) + 1)
    for value in samples:
        counts[bisect.bisect_left(DURATION_BOUNDS, value)] += 1
    dp: dict = {
        "timeUnixNano": scrape_now_ns(),
        "startTimeUnixNano": start_time_ns,
        "count": len(samples),
        "sum": float(sum(samples)),
        "bucketCounts": counts,
        "explicitBounds": list(DURATION_BOUNDS),
        "attributes": _format_attributes(attributes),
    }
    if samples:
        dp["min"] = float(min(samples))
        dp["max"] = float(max(samples))
    return {
        "name": "traefik_service_request_duration_seconds",
        "unit": "s",
        "histogram": {"dataPoints": [dp], "aggregationTemporality": 1},  # DELTA
    }


class _TraefikState:
    """Cumulative counters + gauge baselines for one Traefik instance."""

    def __init__(self, rng: random.Random):
        self.entrypoint_reqs: dict[tuple[str, str], CounterState] = {}
        self.entrypoint_bytes: dict[str, CounterState] = {}
        self.service_reqs: dict[tuple[str, str], CounterState] = {}
        self.service_bytes: dict[str, CounterState] = {}
        self.router_reqs: dict[tuple[str, str], CounterState] = {}
        self.tls_reqs: dict[tuple[str, str], CounterState] = {}
        self.config_reloads = CounterState(rng, initial=rng.randint(5, 50))
        self.cpu_s = CounterState(rng, initial=rng.randint(1_000, 50_000))
        self.last_reload_success = time.time() - rng.randint(60, 3_600)

        for ep in _ENTRYPOINTS:
            self.entrypoint_bytes[ep] = CounterState(rng)
            for code in _HTTP_CODES_OK + _HTTP_CODES_ERR:
                self.entrypoint_reqs[(ep, code)] = CounterState(
                    rng, initial=rng.randint(1_000, 100_000) if code in _HTTP_CODES_OK else rng.randint(0, 500),
                )
        for svc in _SERVICES:
            self.service_bytes[svc] = CounterState(rng)
            for code in _HTTP_CODES_OK + _HTTP_CODES_ERR:
                self.service_reqs[(svc, code)] = CounterState(
                    rng, initial=rng.randint(1_000, 80_000) if code in _HTTP_CODES_OK else rng.randint(0, 400),
                )
        for router in _ROUTERS:
            for code in _HTTP_CODES_OK + _HTTP_CODES_ERR:
                self.router_reqs[(router, code)] = CounterState(
                    rng, initial=rng.randint(1_000, 80_000) if code in _HTTP_CODES_OK else rng.randint(0, 400),
                )
        for version in _TLS_VERSIONS:
            for cipher in _TLS_CIPHERS:
                self.tls_reqs[(version, cipher)] = CounterState(rng)

        # Cert expiry as unix epoch seconds (not_after). All healthy (>=90
        # days, well above the alert's 30-day breach floor); chaos rewrites
        # cert_not_after in tick() below to breach on demand.
        now = time.time()
        self.cert_not_after = {
            0: now + 90 * 86400,
            1: now + 120 * 86400,
            2: now + 300 * 86400,
        }

        self.open_connections = {ep: rng.randint(20, 100) for ep in _ENTRYPOINTS}
        self.open_fds = rng.randint(40, 120)
        self.rss = rng.randint(80, 250) * 1024 * 1024
        self.heap_inuse = rng.randint(30, 120) * 1024 * 1024
        self.heap_sys = self.heap_inuse + rng.randint(20, 80) * 1024 * 1024
        self.goroutines = rng.randint(60, 200)
        self.last_service_ok_ops = 1

    def tick(self, rng: random.Random, load: float = 1.0, error_rate: float = 0.0) -> None:
        ops = max(1, int(rng.randint(80, 400) * load))
        # Chaos sets a real error rate (4xx + 5xx). The healthy baseline (0)
        # keeps every OK code moving plus a sparse error trickle from
        # _healthy_error_drift so error panels are not empty, while the 5xx
        # share stays far under the package "Service availability 99.5%" SLO
        # budget and the 5% 5xx alerts.
        effective_error_rate = max(0.0, float(error_rate))
        ok_ops = max(0, int(ops * (1.0 - effective_error_rate)))
        err_ops = max(0, ops - ok_ops)
        self.last_service_ok_ops = max(1, ok_ops // len(_SERVICES))

        for ep in _ENTRYPOINTS:
            share = max(1, ok_ops // len(_ENTRYPOINTS))
            for code in _HTTP_CODES_OK:
                self.entrypoint_reqs[(ep, code)].tick(
                    rng, drift=max(0, share // len(_HTTP_CODES_OK) + rng.randint(0, 3)),
                )
            if err_ops:
                err_share = max(1, err_ops // len(_ENTRYPOINTS))
                for code in _HTTP_CODES_ERR:
                    # max(1, …) so ambient ~2% rates still advance each code
                    # (err_share // 5 floors to 0 for small err_ops).
                    self.entrypoint_reqs[(ep, code)].tick(
                        rng, drift=max(1, err_share // len(_HTTP_CODES_ERR)),
                    )
            else:
                for code in _HTTP_CODES_ERR:
                    self.entrypoint_reqs[(ep, code)].tick(rng, drift=_healthy_error_drift(rng, code))
            self.entrypoint_bytes[ep].tick(rng, drift=ops * rng.randint(800, 4_000))

        for svc in _SERVICES:
            share = max(1, ok_ops // len(_SERVICES))
            for code in _HTTP_CODES_OK:
                self.service_reqs[(svc, code)].tick(
                    rng, drift=max(0, share // len(_HTTP_CODES_OK) + rng.randint(0, 2)),
                )
            if err_ops:
                err_share = max(1, err_ops // len(_SERVICES))
                for code in _HTTP_CODES_ERR:
                    self.service_reqs[(svc, code)].tick(
                        rng, drift=max(1, err_share // len(_HTTP_CODES_ERR)),
                    )
            else:
                for code in _HTTP_CODES_ERR:
                    self.service_reqs[(svc, code)].tick(rng, drift=_healthy_error_drift(rng, code))
            self.service_bytes[svc].tick(rng, drift=ops * rng.randint(600, 3_000))

        for router in _ROUTERS:
            share = max(1, ok_ops // len(_ROUTERS))
            for code in _HTTP_CODES_OK:
                self.router_reqs[(router, code)].tick(
                    rng, drift=max(0, share // len(_HTTP_CODES_OK) + rng.randint(0, 2)),
                )
            if err_ops:
                err_share = max(1, err_ops // len(_ROUTERS))
                for code in _HTTP_CODES_ERR:
                    self.router_reqs[(router, code)].tick(
                        rng, drift=max(1, err_share // len(_HTTP_CODES_ERR)),
                    )
            else:
                for code in _HTTP_CODES_ERR:
                    self.router_reqs[(router, code)].tick(rng, drift=_healthy_error_drift(rng, code))

        # Prefer TLS 1.3 over 1.2 in the distribution.
        for version, cipher in self.tls_reqs:
            weight = 3 if version == "TLS 1.3" else 1
            self.tls_reqs[(version, cipher)].tick(
                rng, drift=max(0, int(ops * 0.15 * weight / 4)),
            )

        if rng.random() < 0.05:
            self.config_reloads.tick(rng, drift=1)
            self.last_reload_success = time.time()

        # ~0.2–1.0 CPU-seconds per 15s tick (~1–7% of one core) so Process
        # cpu_rate panels are visibly non-zero after TO_DOUBLE CASE rewrites.
        self.cpu_s.tick(rng, drift=max(1, int(ops * 0.05)))

        conn_base = int(rng.randint(20, 200) * load)
        self.open_connections = {
            ep: max(1, conn_base + rng.randint(-10, 30)) for ep in _ENTRYPOINTS
        }
        self.open_fds = max(10, min(PROCESS_MAX_FDS - 10,
                                    int(rng.randint(40, 180) * (0.8 + 0.4 * load))))
        self.rss = max(40 * 1024 * 1024,
                       self.rss + rng.randint(-2, 3) * 1024 * 1024)
        self.heap_inuse = max(20 * 1024 * 1024,
                              self.heap_inuse + rng.randint(-1, 2) * 1024 * 1024)
        self.heap_sys = max(self.heap_inuse + 10 * 1024 * 1024,
                            self.heap_sys + rng.randint(-1, 1) * 1024 * 1024)
        self.goroutines = max(40, min(500, self.goroutines + rng.randint(-5, 8)))



from generators.traefik_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("traefik")
    cloud = cfg.get("cloud") or _default_cloud()
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("traefik")
    entrypoint_count = int(cfg.get("entrypoint_count", 2))
    entrypoints = list(_ENTRYPOINTS[: max(1, min(entrypoint_count, len(_ENTRYPOINTS)))])

    impact_keys = SPEC.impact_keys if SPEC is not None else {
        "backend_error_rate": {"type": "float", "default": 0.0},
        "open_connections_multiplier": {"type": "float", "default": 1.0},
    }

    resource = build_signal_resource(
        ns=ns, signal_type="metrics", dataset=otel_dataset("traefik"),
        service_name=service_name, service_version="3.0.0",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name, cloud=cloud,
    )
    scope = {"name": TRAEFIK_SCOPE, "version": SCOPE_VERSION}
    state = _TraefikState(rng)
    last_emitted: dict[tuple, int] = {}

    def _seed_delta(name: str, counter: CounterState, attrs: dict | None = None) -> None:
        key = (name, tuple(sorted((attrs or {}).items())))
        last_emitted[key] = counter.value

    # Seed baselines so the first tick emits a real period delta, not zeros.
    for ep in entrypoints:
        _seed_delta("traefik_entrypoint_responses_bytes_total",
                    state.entrypoint_bytes[ep], {"entrypoint": ep})
        for code in _HTTP_CODES_OK + _HTTP_CODES_ERR:
            _seed_delta(
                "traefik_entrypoint_requests_total",
                state.entrypoint_reqs[(ep, code)],
                {"entrypoint": ep, "code": code},
            )
    for svc in _SERVICES:
        _seed_delta("traefik_service_responses_bytes_total",
                    state.service_bytes[svc], {"service": svc})
        for code in _HTTP_CODES_OK + _HTTP_CODES_ERR:
            _seed_delta(
                "traefik_service_requests_total",
                state.service_reqs[(svc, code)],
                {"service": svc, "code": code},
            )
    for (version, cipher), counter in state.tls_reqs.items():
        _seed_delta(
            "traefik_entrypoint_requests_tls_total", counter,
            {"tls_version": version, "tls_cipher": cipher},
        )
    _seed_delta("process_cpu_seconds_total", state.cpu_s)

    try:
        es_bulk = ESBulkClient()
    except Exception as exc:  # pragma: no cover - missing http2/deps in some host envs
        logger.warning("ESBulkClient unavailable (%s); co-located docs disabled", exc)
        es_bulk = None

    _reset_counter_long_mapping()

    logger.info("Traefik generator started (namespace=%s)", ns)

    def _emit_tick() -> None:
        nonlocal cipher, code, counter, ep, svc, version
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="traefik_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        c = active_chaos or {}
        err_entrypoint = float(c.get("high_5xx_error_rate_by_entrypoint", 0.0))
        err_service = float(c.get("high_5xx_error_rate_by_service", 0.0))
        err_router = float(c.get("high_5xx_error_rate_by_router", 0.0))
        fd_key = float(c.get("file_descriptor_pressure", 0.0))
        goroutine_count = float(c.get("high_goroutine_count", 0.0))
        memory_gb = float(c.get("high_memory_usage", 0.0))
        open_conns_key = float(c.get("high_open_connections", 0.0))
        stale_hours = float(c.get("stale_config_reload_success", 0.0))
        cert_days = float(c.get("tls_certificate_expiry_imminent", 300.0))
        # OOTB is typically days_until_expiry < 30; GT probe 30.0 sits on the
        # boundary — clamp active probes to strictly below 30.
        if active_chaos is not None and cert_days <= 30.0:
            cert_days = min(cert_days, 29.0)
        # OOTB 5xx alerts: SUM period-deltas, THEN long/long ``errors/total``
        # before ``*100``. GT probes (750/375) are absolute volume hints, not
        # percent — force a full-fault tip and emit ONLY ``5*`` codes so
        # errors_5xx >= total (pct >= 100). Emitting 4xx under "error_rate"
        # dilutes the 5xx filter and keeps integer-div pct at 0.
        entrypoint_5xx_fault = err_entrypoint > 0
        router_5xx_fault = err_router > 0
        service_5xx_fault = err_service > 0
        any_5xx_fault = entrypoint_5xx_fault or router_5xx_fault or service_5xx_fault
        ep_bump = max(500, int(err_entrypoint)) if entrypoint_5xx_fault else 0
        router_bump = max(500, int(err_router)) if router_5xx_fault else 0
        service_bump = max(500, int(err_service)) if service_5xx_fault else 0
        load = business_load()
        state.tick(
            rng,
            load=load,
            error_rate=(1.0 if any_5xx_fault else 0.0),
        )
        if fd_key > 0:
            # Integer-div util alerts need open >= max (quot >= 1) for >80%.
            state.open_fds = int(PROCESS_MAX_FDS * max(0.85, min(1.05, fd_key / 100.0 if fd_key > 1 else fd_key)))
            if fd_key >= 80:
                state.open_fds = max(state.open_fds, PROCESS_MAX_FDS)
        elif state.open_fds >= PROCESS_MAX_FDS:
            # Snap sticky FD pressure after resolve.
            state.open_fds = rng.randint(40, 120)
        if goroutine_count > 0:
            state.goroutines = int(goroutine_count)
        elif state.goroutines > 400:
            # Snap sticky goroutine tip after resolve.
            state.goroutines = rng.randint(60, 200)
        if memory_gb > 0:
            state.rss = int(memory_gb * 1024 * 1024 * 1024)
        elif state.rss > 500 * 1024 * 1024:
            # Snap sticky RSS after high_memory_usage resolve (tick only drifts).
            state.rss = rng.randint(80, 250) * 1024 * 1024
        if stale_hours > 0:
            state.last_reload_success = time.time() - stale_hours * 3600
        elif state.last_reload_success < time.time() - 3600:
            # Snap stale reload tip back to recent success after resolve.
            state.last_reload_success = time.time() - rng.uniform(60, 900)
        if cert_days < 300:
            for idx in range(len(state.cert_not_after)):
                state.cert_not_after[idx] = time.time() + cert_days * 86400
        elif active_chaos is None or float(c.get("tls_certificate_expiry_imminent", 300.0)) >= 300:
            # Healthy certs: restore multi-month expiry after TLS fault resolve.
            now = time.time()
            if min(state.cert_not_after.values()) < now + 40 * 86400:
                state.cert_not_after = {
                    0: now + 90 * 86400,
                    1: now + 120 * 86400,
                    2: now + 300 * 86400,
                }
        ts_ms = int(time.time() * 1000)

        metrics: list[dict] = []

        def _emit_req(
            name: str,
            counter: CounterState,
            attrs: dict,
            *,
            fault: bool,
            code: str,
            bump: int = 0,
            suppress_errors: bool = False,
        ) -> None:
            """Emit period-delta request gauge for OOTB 5xx rate alerts.

            OOTB: ``ROUND(errors_5xx / total * 100.0, 2) > 5`` with long/long
            division first. Under fault emit ONLY ``5*`` codes (skip OK + 4xx)
            and boost absolute deltas so errors_5xx >= total after soft-trim.
            Sibling dimensions suppress ERR so a single channel cannot dilute
            another dimension's window or cross-fire.
            """
            if fault:
                if code not in _HTTP_CODES_5XX:
                    return
            elif suppress_errors and code in _HTTP_CODES_ERR:
                return
            elif counter.value == 0 and code in _HTTP_CODES_ERR:
                return
            gauge = _period_delta_gauge(name, "{requests}", counter, attrs, last_emitted)
            if fault and code in _HTTP_CODES_5XX:
                try:
                    cur = int(gauge["gauge"]["dataPoints"][0]["asInt"])
                except Exception:
                    cur = 0
                gauge = build_gauge(name, "{requests}", max(cur, int(bump) or 500), attrs)
            metrics.append(gauge)

        for ep in entrypoints:
            for code in _HTTP_CODES_OK + _HTTP_CODES_ERR:
                _emit_req(
                    "traefik_entrypoint_requests_total",
                    state.entrypoint_reqs[(ep, code)],
                    {"entrypoint": ep, "code": code},
                    fault=entrypoint_5xx_fault,
                    code=code,
                    bump=ep_bump,
                    suppress_errors=any_5xx_fault and not entrypoint_5xx_fault,
                )
            metrics.append(_period_delta_gauge(
                "traefik_entrypoint_responses_bytes_total", "By",
                state.entrypoint_bytes[ep], {"entrypoint": ep}, last_emitted,
            ))
            open_conns = int(open_conns_key) if open_conns_key > 0 else int(state.open_connections[ep])
            metrics.append(build_gauge(
                "traefik_open_connections", "{conns}", open_conns,
                # OOTB open-connections alert groups by entrypoint + protocol.
                {"entrypoint": ep, "protocol": "http"},
            ))

        for router in _ROUTERS:
            for code in _HTTP_CODES_OK + _HTTP_CODES_ERR:
                _emit_req(
                    "traefik_router_requests_total",
                    state.router_reqs[(router, code)],
                    {"router": router, "code": code, "protocol": "http"},
                    fault=router_5xx_fault,
                    code=code,
                    bump=router_bump,
                    suppress_errors=any_5xx_fault and not router_5xx_fault,
                )

        for svc in _SERVICES:
            for code in _HTTP_CODES_OK + _HTTP_CODES_ERR:
                _emit_req(
                    "traefik_service_requests_total",
                    state.service_reqs[(svc, code)],
                    {"service": svc, "code": code},
                    fault=service_5xx_fault,
                    code=code,
                    bump=service_bump,
                    suppress_errors=any_5xx_fault and not service_5xx_fault,
                )
            metrics.append(_period_delta_gauge(
                "traefik_service_responses_bytes_total", "By",
                state.service_bytes[svc], {"service": svc}, last_emitted,
            ))
            metrics.append(build_duration_histogram(
                latency_samples(rng, state.last_service_ok_ops),
                {"service": svc, "protocol": "http"},
                scrape_start_ns(int(EMIT_INTERVAL_S)),
            ))

        for (version, cipher), counter in state.tls_reqs.items():
            metrics.append(_period_delta_gauge(
                "traefik_entrypoint_requests_tls_total", "{requests}", counter,
                {"tls_version": version, "tls_cipher": cipher}, last_emitted,
            ))

        for idx, cert in enumerate(_CERTS):
            metrics.append(build_gauge(
                "traefik_tls_certs_not_after", "s",
                state.cert_not_after[idx],
                {"cn": cert["cn"], "serial": cert["serial"]},
            ))

        # MAX(config_reloads_total) KPI needs the cumulative total as a gauge.
        metrics.append(build_gauge(
            "traefik_config_reloads_total", "{reloads}",
            state.config_reloads.value, {},
        ))
        metrics.append(build_gauge(
            "traefik_config_last_reload_success", "s",
            state.last_reload_success, {},
        ))

        metrics.append(_period_delta_gauge(
            "process_cpu_seconds_total", "s", state.cpu_s, {}, last_emitted,
        ))
        metrics.append(build_gauge(
            "process_resident_memory_bytes", "By", state.rss, {},
        ))
        # process_open_fds / process_max_fds are sent only via the ES bulk path
        # below with gauge_double mapping so that open_fds / max_fds returns
        # double and matches the 0.0 fallback in the CASE() dashboard query.
        # Sending them here via OTLP would create gauge_long (OTLP coerces
        # whole-number floats to long), causing a type mismatch in ES|QL 9.6.
        metrics.append(build_gauge("go_goroutines", "1", state.goroutines, {}))
        metrics.append(build_gauge(
            "go_memstats_heap_sys_bytes", "By", state.heap_sys, {},
        ))
        metrics.append(build_gauge(
            "go_memstats_heap_inuse_bytes", "By", state.heap_inuse, {},
        ))

        client.send_metrics_scoped(resource, scope, metrics)

        if es_bulk is not None and es_bulk.configured:
            es_bulk.send_bulk(
                _METRICS_DATA_STREAM,
                [build_fd_utilization_doc(
                    resource,
                    open_fds=state.open_fds,
                    max_fds=PROCESS_MAX_FDS,
                    timestamp_ms=ts_ms,
                )],
                dynamic_templates={
                    "metrics.process_open_fds": "gauge_double",
                    "metrics.process_max_fds": "gauge_double",
                },
            )

    run_tick_loop(stop_event, _emit_tick, interval=lambda: EMIT_INTERVAL_S)

    if es_bulk is not None:
        es_bulk.close()
    logger.info("Traefik generator stopped.")


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "60"))
    def _stop(sig, frame): stop_event.set()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    client = OTLPClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
