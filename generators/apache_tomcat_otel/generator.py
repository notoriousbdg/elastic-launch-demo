#!/usr/bin/env python3
"""Apache Tomcat Generator — sends Tomcat JMX metrics via OTLP.

Emits data matching the apache_tomcat_otel content package
(discovery.datasets: tomcat.otel).

Metric names follow raw JMX MBean naming used by the Elastic package
dashboards (Catalina_* / java_lang_*), matching Prometheus JMX Exporter
output scraped by the prometheusreceiver.

Prometheus scrapes JMX as float64. Dashboard ES|QL does ``busy / max_threads``
(and CPU ``* 100``) without ``TO_DOUBLE``. On current ES, ``long / long`` is
integer division (ratios → 0) and float CPU samples indexed as ``long``
truncate to 0 — so every series is emitted as an OTLP gauge ``asDouble`` and
co-located bulk docs use ``gauge_double``.

Dashboard panels call ``DELTA()`` on cumulative JMX counters. Monotonic
OTLP sums map to ``counter_long`` and reject ``DELTA()``, so those series
are gauges (same pattern as etcd_otel / traefik_otel). Sticky ``counter_long``
or plain ``long`` mappings from earlier runs are cleared on startup by
deleting the data stream when needed.

Multi-metric panels filter on one field then ``STATS`` siblings
(``LAST_OVER_TIME`` / ``DELTA``). OTLP stores one metric per document, so
those siblings are null unless co-located — each tick also bulk-indexes
grouped docs via ``ESBulkClient``.

Field names verified against:
  - elastic/integrations/packages/apache_tomcat_otel/kibana/dashboard/*.json

Usage (standalone):
    python3 -m generators.apache_tomcat_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/apache_tomcat_otel@0.3.2"

import logging
import math
import os
import random
import signal
import threading
import time
from typing import Any

from app.telemetry import ESBulkClient, OTLPClient
from app.config import NAMESPACE
try:
    from generators.apache_tomcat_otel import SPEC
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

# Package docs scrape Prometheus JMX Exporter via prometheusreceiver.
TOMCAT_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("tomcat-generator")
EMIT_INTERVAL_S = 30.0

_METRICS_DATA_STREAM = "metrics-tomcat.otel-default"

# Intersection of JMX fields and apache_tomcat_otel dashboard/alert queries.
METRIC_NAMES = (
    # Global request processor
    "Catalina_GlobalRequestProcessor_requestCount",
    "Catalina_GlobalRequestProcessor_errorCount",
    "Catalina_GlobalRequestProcessor_processingTime",
    "Catalina_GlobalRequestProcessor_maxTime",
    "Catalina_GlobalRequestProcessor_bytesSent",
    "Catalina_GlobalRequestProcessor_bytesReceived",
    # Thread pool
    "Catalina_ThreadPool_currentThreadCount",
    "Catalina_ThreadPool_currentThreadsBusy",
    "Catalina_ThreadPool_maxThreads",
    "Catalina_ThreadPool_minSpareThreads",
    "Catalina_ThreadPool_connectionCount",
    "Catalina_ThreadPool_maxConnections",
    "Catalina_ThreadPool_keepAliveCount",
    "Catalina_ThreadPool_acceptCount",
    # Sessions
    "Catalina_Manager_activeSessions",
    "Catalina_Manager_sessionCounter",
    "Catalina_Manager_maxActive",
    "Catalina_Manager_rejectedSessions",
    "Catalina_Manager_expiredSessions",
    "Catalina_Manager_sessionAverageAliveTime",
    "Catalina_Manager_sessionMaxAliveTime",
    "Catalina_Manager_sessionCreateRate",
    "Catalina_Manager_sessionExpireRate",
    # Servlets
    "Catalina_Servlet_requestCount",
    "Catalina_Servlet_errorCount",
    "Catalina_Servlet_processingTime",
    "Catalina_Servlet_maxTime",
    "Catalina_Servlet_minTime",
    "Catalina_Servlet_countAllocated",
    "Catalina_Servlet_loadTime",
    # Web modules
    "Catalina_WebModule_requestCount",
    "Catalina_WebModule_errorCount",
    "Catalina_WebModule_processingTime",
    "Catalina_WebModule_maxTime",
    "Catalina_WebModule_startupTime",
    # Realm / cache
    "Catalina_Realm_failureCount",
    "Catalina_WebResourceRoot_hitCount",
    "Catalina_WebResourceRoot_lookupCount",
    "Catalina_WebResourceRoot_size",
    "Catalina_WebResourceRoot_maxSize",
    # JVM memory
    "java_lang_Memory_HeapMemoryUsage_used",
    "java_lang_Memory_HeapMemoryUsage_committed",
    "java_lang_Memory_HeapMemoryUsage_max",
    "java_lang_Memory_NonHeapMemoryUsage_used",
    "java_lang_Memory_NonHeapMemoryUsage_committed",
    "java_lang_G1_Eden_Space_Usage_used",
    "java_lang_G1_Eden_Space_Usage_committed",
    "java_lang_G1_Survivor_Space_Usage_used",
    "java_lang_G1_Survivor_Space_Usage_committed",
    "java_lang_G1_Old_Gen_Usage_used",
    "java_lang_G1_Old_Gen_Usage_committed",
    "java_lang_G1_Old_Gen_Usage_max",
    # JVM GC
    "java_lang_G1_Young_Generation_CollectionCount",
    "java_lang_G1_Young_Generation_CollectionTime",
    "java_lang_G1_Young_Generation_LastGcInfo_duration",
    "java_lang_G1_Old_Generation_CollectionCount",
    "java_lang_G1_Old_Generation_CollectionTime",
    "java_lang_G1_Concurrent_GC_CollectionCount",
    "java_lang_G1_Concurrent_GC_CollectionTime",
    # JVM runtime / threads
    "java_lang_Runtime_Uptime",
    "java_lang_Threading_ThreadCount",
    "java_lang_Threading_DaemonThreadCount",
    "java_lang_Threading_PeakThreadCount",
    "java_lang_Threading_TotalStartedThreadCount",
    # JVM OS
    "java_lang_OperatingSystem_CpuLoad",
    "java_lang_OperatingSystem_ProcessCpuLoad",
    "java_lang_OperatingSystem_SystemLoadAverage",
    "java_lang_OperatingSystem_AvailableProcessors",
    "java_lang_OperatingSystem_FreeMemorySize",
    "java_lang_OperatingSystem_TotalMemorySize",
    "java_lang_OperatingSystem_FreeSwapSpaceSize",
    "java_lang_OperatingSystem_TotalSwapSpaceSize",
    "java_lang_OperatingSystem_OpenFileDescriptorCount",
    "java_lang_OperatingSystem_MaxFileDescriptorCount",
)

# Panels call DELTA() on these — must map as gauges, not counter_long.
_DELTA_GAUGE_FIELDS = (
    "Catalina_GlobalRequestProcessor_requestCount",
    "Catalina_GlobalRequestProcessor_errorCount",
    "Catalina_GlobalRequestProcessor_processingTime",
    "Catalina_GlobalRequestProcessor_bytesSent",
    "Catalina_GlobalRequestProcessor_bytesReceived",
    "Catalina_Manager_sessionCounter",
    "Catalina_Manager_expiredSessions",
    "Catalina_Manager_rejectedSessions",
    "Catalina_Servlet_requestCount",
    "Catalina_Servlet_errorCount",
    "Catalina_Realm_failureCount",
    "Catalina_WebResourceRoot_lookupCount",
    "java_lang_G1_Young_Generation_CollectionCount",
    "java_lang_G1_Young_Generation_CollectionTime",
    "java_lang_G1_Old_Generation_CollectionCount",
    "java_lang_G1_Old_Generation_CollectionTime",
    "java_lang_G1_Concurrent_GC_CollectionCount",
    "java_lang_G1_Concurrent_GC_CollectionTime",
    "java_lang_Threading_TotalStartedThreadCount",
)

# FD util does Open/Max without TO_DOUBLE — must be double, not long/gauge_long.
_FD_DOUBLE_FIELDS = (
    "java_lang_OperatingSystem_OpenFileDescriptorCount",
    "java_lang_OperatingSystem_MaxFileDescriptorCount",
)

_CONNECTORS = ["http-nio-8080", "http-nio-8443"]
_SERVLETS = (
    ("default", "/"),
    ("jsp", "/"),
    ("ROOT", "/app"),
)
_WEB_MODULES = ["/", "/app", "/manager"]


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


def _bulk_doc(
    resource: dict,
    metrics: dict[str, int | float],
    *,
    attributes: dict[str, Any] | None = None,
    unit: str = "1",
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES metrics doc with several fields co-located.

    Values are coerced to ``float`` so JSON numbers map via ``gauge_double``
    (Prometheus JMX path) rather than sticky ``long``.
    """
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": unit,
        "data_stream": {
            "type": "metrics",
            "dataset": "tomcat.otel",
            "namespace": "default",
        },
        "attributes": dict(attributes or {}),
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": TOMCAT_SCOPE, "version": SCOPE_VERSION},
        "metrics": {name: float(value) for name, value in metrics.items()},
    }


def _tomcat_gauge(name: str, unit: str, value: int | float, attributes: dict | None = None) -> dict:
    """OTLP gauge with ``asDouble`` (Prometheus JMX / ratio-panel compatible)."""
    return build_gauge(name, unit, float(value), attributes or {})


def _fields_need_mapping_reset(elastic_url: str, api_key: str) -> bool:
    """True when any Tomcat metric is stuck as counter_long or plain long.

    ``counter_long`` rejects ``DELTA()``. Plain / gauge ``long`` makes ratio
    panels integer-divide to 0 and truncates CPU / load averages. Open/Max FD
    fields are poison unless mapped as ``double`` / ``gauge_double`` (sticky
    ``gauge_long`` still integer-divides File Descriptor Utilization to 0%).
    """
    import httpx

    field_list = ",".join(f"metrics.{name}" for name in METRIC_NAMES)
    url = f"{elastic_url.rstrip('/')}/{_METRICS_DATA_STREAM}/_field_caps?fields={field_list}"
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
    for name in METRIC_NAMES:
        caps = fields.get(f"metrics.{name}") or fields.get(name) or {}
        for meta in caps.values():
            mtype = meta.get("type")
            if name in _FD_DOUBLE_FIELDS:
                if mtype not in ("double", "gauge_double"):
                    return True
                continue
            if (
                meta.get("time_series_metric") == "counter"
                or mtype in ("counter_long", "long")
            ):
                return True
    return False


def _reset_sticky_long_mapping() -> None:
    """Delete the metrics data stream when fields are stuck as long/counter.

    Fresh ``gauge_double`` samples recreate the stream so ``DELTA()`` panels
    and ratio / CPU panels work. No-op when ES credentials are absent or
    mapping is already double gauges.
    """
    try:
        from app.telemetry import ESBulkClient as _Bulk
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for mapping check: %s", exc)
        return

    client = _Bulk()
    if not client.configured:
        return

    if not _fields_need_mapping_reset(client.elastic_url, client.api_key):
        logger.info(
            "tomcat metrics are not stuck as long/counter; leaving %s alone",
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
            "Deleted %s so metrics remap as gauge_double (were long/counter_long)",
            _METRICS_DATA_STREAM,
        )
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist yet", _METRICS_DATA_STREAM)
    else:
        logger.warning(
            "Could not delete %s (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )


class TomcatMetricState(CounterState):
    """Cumulative counters shared across Tomcat scrapes."""

    def __init__(self, rng: random.Random):
        super().__init__(rng)
        # Seed DELTA() gauges from wall-clock so a process restart never emits a
        # lower cumulative than the previous scrape (negative panel DELTAs).
        base = int(time.time())
        self.request_count = {c: base + rng.randint(50_000, 5_000_000) for c in _CONNECTORS}
        self.error_count = {c: base + rng.randint(10, 5_000) for c in _CONNECTORS}
        self.processing_time = {c: base + rng.randint(100_000, 50_000_000) for c in _CONNECTORS}
        self.bytes_sent = {c: base + rng.randint(10**9, 10**12) for c in _CONNECTORS}
        self.bytes_received = {c: base + rng.randint(10**8, 10**11) for c in _CONNECTORS}
        self.session_counter = base + rng.randint(10_000, 500_000)
        self.expired_sessions = base + rng.randint(1_000, 50_000)
        # Session rejections and old-gen GC alerts fire on DELTA/INCREASE > 0;
        # unlike the other DELTA gauges above, these must start at (and stay
        # at) an absolute 0 baseline rather than a wall-clock-seeded value —
        # a nonzero seed here is itself indistinguishable from "already
        # rejecting" / "already GC-thrashing" on the very first scrape.
        self.rejected_sessions = 0
        self.realm_failures = base + rng.randint(0, 200)
        self.cache_hits = base + rng.randint(10_000, 1_000_000)
        self.cache_lookups = base + rng.randint(20_000, 2_000_000)
        self.young_gc = base + rng.randint(100, 10_000)
        self.old_gc = 0
        self.concurrent_gc = base + rng.randint(50, 5_000)
        self.young_gc_time = self.young_gc + rng.randint(5, 40) * 100
        self.old_gc_time = 0
        self.concurrent_gc_time = self.concurrent_gc + rng.randint(5, 30) * 50
        self.total_started_threads = base + rng.randint(500, 5_000)
        self.servlet_requests = {
            s: base + rng.randint(10_000, 1_000_000) for s, _ in _SERVLETS
        }
        self.servlet_errors = {s: base + rng.randint(0, 5_000) for s, _ in _SERVLETS}
        self.servlet_processing = {
            s: base + rng.randint(50_000, 20_000_000) for s, _ in _SERVLETS
        }
        self.webmodule_requests = {
            m: base + rng.randint(10_000, 1_000_000) for m in _WEB_MODULES
        }
        self.webmodule_errors = {m: base + rng.randint(0, 5_000) for m in _WEB_MODULES}
        self.webmodule_processing = {
            m: base + rng.randint(50_000, 20_000_000) for m in _WEB_MODULES
        }
        self.start_monotonic = time.monotonic()
        self.base_uptime_ms = rng.randint(3_600_000, 30 * 24 * 3_600_000)

    def tick(
        self,
        rng: random.Random,
        load: float,
        err_mult: float,
        thread_pool_exhausted: bool,
        gc_activity: float = 0.0,
    ) -> None:
        for connector in _CONNECTORS:
            req_delta = max(1, int(80 * load) + rng.randint(0, 40))
            # Healthy err_mult stays at 1.0 — keep deltas near-zero and safely
            # under the 0.05 error-rate alert floor; chaos raises err_mult.
            err_delta = max(0, int(1 * load * err_mult) + rng.randint(0, 1))
            self.request_count[connector] += req_delta
            self.error_count[connector] += err_delta
            self.processing_time[connector] += int(req_delta * rng.uniform(8, 80))
            sent = int(req_delta * rng.uniform(8_000, 64_000))
            self.bytes_sent[connector] += sent
            self.bytes_received[connector] += int(sent * rng.uniform(0.1, 0.4))

        session_delta = max(1, int(8 * load) + rng.randint(0, 5))
        self.session_counter += session_delta
        self.expired_sessions += max(0, int(session_delta * 0.2) + rng.randint(0, 2))
        if thread_pool_exhausted or err_mult > 5:
            self.rejected_sessions += rng.randint(1, 8)
        self.realm_failures += rng.randint(0, max(1, int(err_mult)))
        lookups = max(10, int(200 * load) + rng.randint(0, 100))
        self.cache_lookups += lookups
        self.cache_hits += int(lookups * rng.uniform(0.7, 0.95))

        young = rng.randint(1, 3)
        self.young_gc += young
        self.young_gc_time += young * rng.randint(5, 40)
        # Old-gen GC only advances under the old_generation_gc_activity chaos
        # key — any healthy-baseline increment breaches the alert (>0.0).
        if gc_activity > 0:
            self.old_gc += 1
            self.old_gc_time += rng.randint(50, 400)
        concurrent = rng.randint(0, 2)
        self.concurrent_gc += concurrent
        self.concurrent_gc_time += concurrent * rng.randint(5, 30)
        self.total_started_threads += rng.randint(0, 3)

        for servlet, _ in _SERVLETS:
            delta = max(1, int(30 * load / len(_SERVLETS)) + rng.randint(0, 10))
            self.servlet_requests[servlet] += delta
            self.servlet_errors[servlet] += max(
                0, int(delta * 0.01 * err_mult) + rng.randint(0, 1)
            )
            self.servlet_processing[servlet] += int(delta * rng.uniform(5, 60))

        for module in _WEB_MODULES:
            delta = max(1, int(40 * load / len(_WEB_MODULES)) + rng.randint(0, 15))
            self.webmodule_requests[module] += delta
            self.webmodule_errors[module] += max(
                0, int(delta * 0.01 * err_mult) + rng.randint(0, 1)
            )
            self.webmodule_processing[module] += int(delta * rng.uniform(5, 60))

    @property
    def uptime_ms(self) -> int:
        elapsed_ms = int((time.monotonic() - self.start_monotonic) * 1000)
        return self.base_uptime_ms + elapsed_ms


def _build_metrics_and_docs(
    state: TomcatMetricState,
    rng: random.Random,
    load: float,
    err_mult: float,
    thread_pool_exhausted: bool,
    max_threads: int,
    resource: dict,
    *,
    timestamp_ms: int | None = None,
    connection_pool_saturation: float = 0.0,
    fd_pressure: float = 0.0,
    cpu_load: float = 0.0,
    heap_pressure: float = 0.0,
    gc_activity: float = 0.0,
    latency_ms: float = 0.0,
    session_rejections: float = 0.0,
) -> tuple[list[dict], list[dict[str, Any]]]:
    """Build OTLP metrics + co-located bulk docs for multi-metric panels."""
    metrics: list[dict] = []
    bulk_docs: list[dict[str, Any]] = []
    ts = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)

    for connector in _CONNECTORS:
        attrs = {"name": connector}
        current_threads = (
            int(max_threads * 0.95)
            if thread_pool_exhausted
            else rng.randint(10, int(max_threads * 0.55))
        )
        threads_busy = (
            current_threads
            if thread_pool_exhausted
            else min(current_threads, rng.randint(1, max(2, int(current_threads * 0.8))))
        )
        max_connections = 10_000
        if connection_pool_saturation > 0:
            connection_count = int(max_connections * min(1.0, connection_pool_saturation))
        else:
            connection_count = (
                int(max_connections * 0.9)
                if thread_pool_exhausted
                else rng.randint(20, int(800 * load) + 50)
            )
        keep_alive = rng.randint(5, 80)
        max_time = int(latency_ms) if latency_ms > 0 else rng.randint(200, int(5000 * max(1.0, err_mult)))
        if latency_ms > 0:
            state.processing_time[connector] = max(
                state.processing_time[connector],
                int(max(state.request_count[connector], 20) * latency_ms),
            )
        accept_count = 100
        min_spare = 10

        # DELTA() panels — gauges carrying cumulative totals.
        metrics.append(_tomcat_gauge(
            "Catalina_GlobalRequestProcessor_requestCount", "{requests}",
            state.request_count[connector], attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_GlobalRequestProcessor_errorCount", "{errors}",
            state.error_count[connector], attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_GlobalRequestProcessor_processingTime", "ms",
            state.processing_time[connector], attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_GlobalRequestProcessor_maxTime", "ms", max_time, attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_GlobalRequestProcessor_bytesSent", "By",
            state.bytes_sent[connector], attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_GlobalRequestProcessor_bytesReceived", "By",
            state.bytes_received[connector], attrs,
        ))

        metrics.append(_tomcat_gauge(
            "Catalina_ThreadPool_currentThreadCount", "{threads}",
            current_threads, attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_ThreadPool_currentThreadsBusy", "{threads}",
            threads_busy, attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_ThreadPool_maxThreads", "{threads}", max_threads, attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_ThreadPool_minSpareThreads", "{threads}", min_spare, attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_ThreadPool_connectionCount", "{conns}",
            connection_count, attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_ThreadPool_maxConnections", "{conns}",
            max_connections, attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_ThreadPool_keepAliveCount", "{conns}", keep_alive, attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_ThreadPool_acceptCount", "{conns}", accept_count, attrs,
        ))

        # Request + thread-pool siblings share attributes.name for connector panels.
        bulk_docs.append(_bulk_doc(
            resource,
            {
                "Catalina_GlobalRequestProcessor_requestCount": state.request_count[connector],
                "Catalina_GlobalRequestProcessor_errorCount": state.error_count[connector],
                "Catalina_GlobalRequestProcessor_processingTime": state.processing_time[connector],
                "Catalina_GlobalRequestProcessor_maxTime": max_time,
                "Catalina_GlobalRequestProcessor_bytesSent": state.bytes_sent[connector],
                "Catalina_GlobalRequestProcessor_bytesReceived": state.bytes_received[connector],
                "Catalina_ThreadPool_currentThreadCount": current_threads,
                "Catalina_ThreadPool_currentThreadsBusy": threads_busy,
                "Catalina_ThreadPool_maxThreads": max_threads,
                "Catalina_ThreadPool_minSpareThreads": min_spare,
                "Catalina_ThreadPool_connectionCount": connection_count,
                "Catalina_ThreadPool_maxConnections": max_connections,
                "Catalina_ThreadPool_keepAliveCount": keep_alive,
                "Catalina_ThreadPool_acceptCount": accept_count,
            },
            attributes=attrs,
            timestamp_ms=ts,
        ))

    # Sessions (Manager MBean uses context + host)
    mgr_attrs = {"context": "/", "host": "localhost"}
    active_sessions = (
        rng.randint(50, 500) if not thread_pool_exhausted else rng.randint(400, 900)
    )
    max_active = max(active_sessions, rng.randint(active_sessions, active_sessions + 200))
    avg_alive = rng.randint(300, 1800)
    max_alive = rng.randint(1800, 7200)
    create_rate = round(8.0 * load + rng.uniform(0, 5), 2)
    expire_rate = round(2.0 * load + rng.uniform(0, 2), 2)

    metrics.append(_tomcat_gauge(
        "Catalina_Manager_activeSessions", "{sessions}", active_sessions, mgr_attrs,
    ))
    metrics.append(_tomcat_gauge(
        "Catalina_Manager_sessionCounter", "{sessions}",
        state.session_counter, mgr_attrs,
    ))
    metrics.append(_tomcat_gauge(
        "Catalina_Manager_maxActive", "{sessions}", max_active, mgr_attrs,
    ))
    # OOTB Session rejection alerts on MAX - MIN of the counter in the window, so
    # rejections must keep accruing every scrape while the fault is active.
    if session_rejections > 0:
        state.rejected_sessions += max(1, math.ceil(session_rejections))
    metrics.append(_tomcat_gauge(
        "Catalina_Manager_rejectedSessions", "{sessions}",
        state.rejected_sessions, mgr_attrs,
    ))
    metrics.append(_tomcat_gauge(
        "Catalina_Manager_expiredSessions", "{sessions}",
        state.expired_sessions, mgr_attrs,
    ))
    metrics.append(_tomcat_gauge(
        "Catalina_Manager_sessionAverageAliveTime", "s", avg_alive, mgr_attrs,
    ))
    metrics.append(_tomcat_gauge(
        "Catalina_Manager_sessionMaxAliveTime", "s", max_alive, mgr_attrs,
    ))
    metrics.append(_tomcat_gauge(
        "Catalina_Manager_sessionCreateRate", "{sessions}/min", create_rate, mgr_attrs,
    ))
    metrics.append(_tomcat_gauge(
        "Catalina_Manager_sessionExpireRate", "{sessions}/min", expire_rate, mgr_attrs,
    ))
    bulk_docs.append(_bulk_doc(
        resource,
        {
            "Catalina_Manager_activeSessions": active_sessions,
            "Catalina_Manager_sessionCounter": state.session_counter,
            "Catalina_Manager_maxActive": max_active,
            "Catalina_Manager_rejectedSessions": state.rejected_sessions,
            "Catalina_Manager_expiredSessions": state.expired_sessions,
            "Catalina_Manager_sessionAverageAliveTime": avg_alive,
            "Catalina_Manager_sessionMaxAliveTime": max_alive,
            "Catalina_Manager_sessionCreateRate": create_rate,
            "Catalina_Manager_sessionExpireRate": expire_rate,
        },
        attributes=mgr_attrs,
        timestamp_ms=ts,
    ))

    # Servlets
    for servlet, web_module in _SERVLETS:
        s_attrs = {"name": servlet, "WebModule": web_module}
        s_max = rng.randint(50, 2000)
        s_min = rng.randint(1, 20)
        s_alloc = rng.randint(1, 8)
        s_load = rng.randint(10, 500)
        metrics.append(_tomcat_gauge(
            "Catalina_Servlet_requestCount", "{requests}",
            state.servlet_requests[servlet], s_attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_Servlet_errorCount", "{errors}",
            state.servlet_errors[servlet], s_attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_Servlet_processingTime", "ms",
            state.servlet_processing[servlet], s_attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_Servlet_maxTime", "ms", s_max, s_attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_Servlet_minTime", "ms", s_min, s_attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_Servlet_countAllocated", "{servlets}", s_alloc, s_attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_Servlet_loadTime", "ms", s_load, s_attrs,
        ))
        bulk_docs.append(_bulk_doc(
            resource,
            {
                "Catalina_Servlet_requestCount": state.servlet_requests[servlet],
                "Catalina_Servlet_errorCount": state.servlet_errors[servlet],
                "Catalina_Servlet_processingTime": state.servlet_processing[servlet],
                "Catalina_Servlet_maxTime": s_max,
                "Catalina_Servlet_minTime": s_min,
                "Catalina_Servlet_countAllocated": s_alloc,
                "Catalina_Servlet_loadTime": s_load,
            },
            attributes=s_attrs,
            timestamp_ms=ts,
        ))

    # Web modules
    for module in _WEB_MODULES:
        w_attrs = {"name": module}
        w_max = rng.randint(100, 3000)
        w_startup = rng.randint(200, 5000)
        metrics.append(_tomcat_gauge(
            "Catalina_WebModule_requestCount", "{requests}",
            state.webmodule_requests[module], w_attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_WebModule_errorCount", "{errors}",
            state.webmodule_errors[module], w_attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_WebModule_processingTime", "ms",
            state.webmodule_processing[module], w_attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_WebModule_maxTime", "ms", w_max, w_attrs,
        ))
        metrics.append(_tomcat_gauge(
            "Catalina_WebModule_startupTime", "ms", w_startup, w_attrs,
        ))
        bulk_docs.append(_bulk_doc(
            resource,
            {
                "Catalina_WebModule_requestCount": state.webmodule_requests[module],
                "Catalina_WebModule_errorCount": state.webmodule_errors[module],
                "Catalina_WebModule_processingTime": state.webmodule_processing[module],
                "Catalina_WebModule_maxTime": w_max,
                "Catalina_WebModule_startupTime": w_startup,
            },
            attributes=w_attrs,
            timestamp_ms=ts,
        ))

    # Realm / static resource cache
    metrics.append(_tomcat_gauge(
        "Catalina_Realm_failureCount", "{failures}",
        state.realm_failures, {"host": "localhost"},
    ))
    cache_max = 64 * 1024 * 1024
    cache_size = rng.randint(cache_max // 4, cache_max)
    metrics.append(_tomcat_gauge(
        "Catalina_WebResourceRoot_hitCount", "{hits}", state.cache_hits, {},
    ))
    metrics.append(_tomcat_gauge(
        "Catalina_WebResourceRoot_lookupCount", "{lookups}", state.cache_lookups, {},
    ))
    metrics.append(_tomcat_gauge(
        "Catalina_WebResourceRoot_size", "By", cache_size, {},
    ))
    metrics.append(_tomcat_gauge(
        "Catalina_WebResourceRoot_maxSize", "By", cache_max, {},
    ))
    bulk_docs.append(_bulk_doc(
        resource,
        {
            "Catalina_Realm_failureCount": state.realm_failures,
            "Catalina_WebResourceRoot_hitCount": state.cache_hits,
            "Catalina_WebResourceRoot_lookupCount": state.cache_lookups,
            "Catalina_WebResourceRoot_size": cache_size,
            "Catalina_WebResourceRoot_maxSize": cache_max,
        },
        attributes={"host": "localhost"},
        timestamp_ms=ts,
    ))

    # JVM memory
    heap_max = 2 * 1024 * 1024 * 1024
    if heap_pressure > 0:
        heap_used = int(heap_max * min(0.99, heap_pressure))
    else:
        heap_used = rng.randint(512 * 1024 * 1024, int(heap_max * 0.8))
    heap_committed = max(heap_used, int(heap_max * 0.9))
    nonheap = rng.randint(64 * 1024 * 1024, 192 * 1024 * 1024)
    nonheap_committed = nonheap + 16 * 1024 * 1024
    eden_committed = 256 * 1024 * 1024
    eden_used = rng.randint(0, eden_committed)
    survivor_committed = 32 * 1024 * 1024
    survivor_used = rng.randint(0, survivor_committed)
    old_max = heap_max
    old_used = rng.randint(256 * 1024 * 1024, int(old_max * 0.6))
    old_committed = max(old_used, int(old_max * 0.7))

    for name, unit, value in (
        ("java_lang_Memory_HeapMemoryUsage_used", "By", heap_used),
        ("java_lang_Memory_HeapMemoryUsage_committed", "By", heap_committed),
        ("java_lang_Memory_HeapMemoryUsage_max", "By", heap_max),
        ("java_lang_Memory_NonHeapMemoryUsage_used", "By", nonheap),
        ("java_lang_Memory_NonHeapMemoryUsage_committed", "By", nonheap_committed),
        ("java_lang_G1_Eden_Space_Usage_used", "By", eden_used),
        ("java_lang_G1_Eden_Space_Usage_committed", "By", eden_committed),
        ("java_lang_G1_Survivor_Space_Usage_used", "By", survivor_used),
        ("java_lang_G1_Survivor_Space_Usage_committed", "By", survivor_committed),
        ("java_lang_G1_Old_Gen_Usage_used", "By", old_used),
        ("java_lang_G1_Old_Gen_Usage_committed", "By", old_committed),
        ("java_lang_G1_Old_Gen_Usage_max", "By", old_max),
    ):
        metrics.append(_tomcat_gauge(name, unit, value, {}))

    bulk_docs.append(_bulk_doc(
        resource,
        {
            "java_lang_Memory_HeapMemoryUsage_used": heap_used,
            "java_lang_Memory_HeapMemoryUsage_committed": heap_committed,
            "java_lang_Memory_HeapMemoryUsage_max": heap_max,
            "java_lang_Memory_NonHeapMemoryUsage_used": nonheap,
            "java_lang_Memory_NonHeapMemoryUsage_committed": nonheap_committed,
            "java_lang_G1_Eden_Space_Usage_used": eden_used,
            "java_lang_G1_Eden_Space_Usage_committed": eden_committed,
            "java_lang_G1_Survivor_Space_Usage_used": survivor_used,
            "java_lang_G1_Survivor_Space_Usage_committed": survivor_committed,
            "java_lang_G1_Old_Gen_Usage_used": old_used,
            "java_lang_G1_Old_Gen_Usage_committed": old_committed,
            "java_lang_G1_Old_Gen_Usage_max": old_max,
        },
        unit="By",
        timestamp_ms=ts,
    ))

    # JVM GC — DELTA() panels need gauges; co-locate count+time families.
    last_gc = rng.randint(5, 80)
    metrics.append(_tomcat_gauge(
        "java_lang_G1_Young_Generation_CollectionCount", "{collections}",
        state.young_gc, {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_G1_Young_Generation_CollectionTime", "ms",
        state.young_gc_time, {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_G1_Young_Generation_LastGcInfo_duration", "ms", last_gc, {},
    ))
    if gc_activity > 0:
        state.old_gc = max(state.old_gc, int(gc_activity))
    metrics.append(_tomcat_gauge(
        "java_lang_G1_Old_Generation_CollectionCount", "{collections}",
        state.old_gc, {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_G1_Old_Generation_CollectionTime", "ms",
        state.old_gc_time, {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_G1_Concurrent_GC_CollectionCount", "{collections}",
        state.concurrent_gc, {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_G1_Concurrent_GC_CollectionTime", "ms",
        state.concurrent_gc_time, {},
    ))
    bulk_docs.append(_bulk_doc(
        resource,
        {
            "java_lang_G1_Young_Generation_CollectionCount": state.young_gc,
            "java_lang_G1_Young_Generation_CollectionTime": state.young_gc_time,
            "java_lang_G1_Young_Generation_LastGcInfo_duration": last_gc,
            "java_lang_G1_Old_Generation_CollectionCount": state.old_gc,
            "java_lang_G1_Old_Generation_CollectionTime": state.old_gc_time,
            "java_lang_G1_Concurrent_GC_CollectionCount": state.concurrent_gc,
            "java_lang_G1_Concurrent_GC_CollectionTime": state.concurrent_gc_time,
        },
        timestamp_ms=ts,
    ))

    # JVM runtime / threads
    thread_count = rng.randint(50, 300)
    daemon = rng.randint(30, min(100, thread_count))
    peak = max(thread_count, rng.randint(100, 400))
    metrics.append(_tomcat_gauge("java_lang_Runtime_Uptime", "ms", state.uptime_ms, {}))
    metrics.append(_tomcat_gauge(
        "java_lang_Threading_ThreadCount", "{threads}", thread_count, {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_Threading_DaemonThreadCount", "{threads}", daemon, {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_Threading_PeakThreadCount", "{threads}", peak, {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_Threading_TotalStartedThreadCount", "{threads}",
        state.total_started_threads, {},
    ))
    bulk_docs.append(_bulk_doc(
        resource,
        {
            "java_lang_Runtime_Uptime": state.uptime_ms,
            "java_lang_Threading_ThreadCount": thread_count,
            "java_lang_Threading_DaemonThreadCount": daemon,
            "java_lang_Threading_PeakThreadCount": peak,
            "java_lang_Threading_TotalStartedThreadCount": state.total_started_threads,
        },
        timestamp_ms=ts,
    ))

    # JVM OS — split co-located docs by unit / panel family.
    # A single mixed OS doc was never indexed alongside OTLP singles (FD + CPU
    # sibling panels stayed null); dedicated pairs match traefik_otel.
    cpu_sys = min(rng.uniform(0.05, 0.70) * load, 1.0)
    if cpu_load > 0:
        cpu_proc = min(1.0, float(cpu_load))
        cpu_sys = max(cpu_sys, cpu_proc)
    else:
        cpu_proc = min(cpu_sys * rng.uniform(0.5, 0.9), 1.0)
    load_avg = round(rng.uniform(0.5, 4.0) * load, 2)
    free_mem = rng.randint(1 * 1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024)
    total_mem = 8 * 1024 * 1024 * 1024
    total_swap = 2 * 1024 * 1024 * 1024
    free_swap = rng.randint(total_swap // 2, total_swap)
    # Healthy util targets the panel gauge_goal (80%); chaos can push higher.
    max_fds = 65536.0
    if fd_pressure > 0:
        open_fds = float(max_fds * min(1.0, fd_pressure))
    else:
        open_fds = float(max_fds * rng.uniform(0.18, 0.45))
    # +0.1 keeps the sample a true float so sticky long mappings cannot collapse
    # Open/Max into integer division (File Descriptor Utilization → 0%).
    open_fds = open_fds + 0.1

    metrics.append(_tomcat_gauge(
        "java_lang_OperatingSystem_CpuLoad", "1", round(cpu_sys, 4), {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_OperatingSystem_ProcessCpuLoad", "1", round(cpu_proc, 4), {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_OperatingSystem_SystemLoadAverage", "1", load_avg, {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_OperatingSystem_AvailableProcessors", "1", 4, {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_OperatingSystem_FreeMemorySize", "By", free_mem, {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_OperatingSystem_TotalMemorySize", "By", total_mem, {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_OperatingSystem_FreeSwapSpaceSize", "By", free_swap, {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_OperatingSystem_TotalSwapSpaceSize", "By", total_swap, {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_OperatingSystem_OpenFileDescriptorCount", "1", open_fds, {},
    ))
    metrics.append(_tomcat_gauge(
        "java_lang_OperatingSystem_MaxFileDescriptorCount", "1", max_fds, {},
    ))

    # FD util panels: WHERE Open IS NOT NULL then STATS Max sibling.
    bulk_docs.append(_bulk_doc(
        resource,
        {
            "java_lang_OperatingSystem_OpenFileDescriptorCount": open_fds,
            "java_lang_OperatingSystem_MaxFileDescriptorCount": max_fds,
        },
        timestamp_ms=ts,
    ))
    # CPU Load Over Time: WHERE CpuLoad IS NOT NULL then AVG ProcessCpuLoad.
    bulk_docs.append(_bulk_doc(
        resource,
        {
            "java_lang_OperatingSystem_CpuLoad": round(cpu_sys, 4),
            "java_lang_OperatingSystem_ProcessCpuLoad": round(cpu_proc, 4),
            "java_lang_OperatingSystem_SystemLoadAverage": load_avg,
            "java_lang_OperatingSystem_AvailableProcessors": 4,
        },
        timestamp_ms=ts,
    ))
    bulk_docs.append(_bulk_doc(
        resource,
        {
            "java_lang_OperatingSystem_FreeMemorySize": free_mem,
            "java_lang_OperatingSystem_TotalMemorySize": total_mem,
            "java_lang_OperatingSystem_FreeSwapSpaceSize": free_swap,
            "java_lang_OperatingSystem_TotalSwapSpaceSize": total_swap,
        },
        unit="By",
        timestamp_ms=ts,
    ))

    return metrics, bulk_docs


def _gauge_templates_for_docs(docs: list[dict[str, Any]]) -> dict[str, str]:
    """Map every metric field in bulk docs to gauge_double (Prometheus JMX)."""
    templates: dict[str, str] = {}
    for doc in docs:
        for name in doc.get("metrics", {}):
            templates[f"metrics.{name}"] = "gauge_double"
    return templates



from generators.apache_tomcat_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("apache-tomcat")
    connector_port = cfg.get("connector_port", 8080)
    cloud = cfg.get("cloud") or _default_cloud()
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("tomcat")
    max_threads = 200

    impact_keys = SPEC.impact_keys if SPEC is not None else {
        "thread_pool_exhaustion": {"type": "bool", "default": False},
        "error_rate_multiplier": {"type": "float", "default": 1.0},
    }

    resource = build_signal_resource(
        ns=ns,
        signal_type="metrics",
        dataset=otel_dataset("tomcat"),
        service_name=service_name,
        service_version="10.1.18",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name,
        cloud=cloud,
    )
    scope = {"name": TOMCAT_SCOPE, "version": SCOPE_VERSION}
    state = TomcatMetricState(rng)

    try:
        es_bulk = ESBulkClient()
    except Exception as exc:  # pragma: no cover - missing http2/deps in some host envs
        logger.warning("ESBulkClient unavailable (%s); co-located docs disabled", exc)
        es_bulk = None

    _reset_sticky_long_mapping()

    logger.info("Tomcat generator started (namespace=%s, port=%d)", ns, connector_port)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="apache_tomcat_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        c = active_chaos or {}
        thread_pool_sat = float(c.get("thread_pool_saturation", 0.0))
        thread_pool_exhausted = thread_pool_sat > 0.8
        err_mult = max(1.0, float(c.get("high_request_error_rate", 0.0)) * 20.0) if c.get("high_request_error_rate", 0.0) else 1.0
        conn_sat = float(c.get("connection_pool_saturation", 0.0))
        fd_pressure = float(c.get("file_descriptor_exhaustion", 0.0))
        cpu_load = float(c.get("high_jvm_process_cpu_usage", 0.0))
        heap_pressure = float(c.get("jvm_heap_memory_pressure", 0.0))
        gc_activity = float(c.get("old_generation_gc_activity", 0.0))
        latency_ms = float(c.get("request_latency_spike", 0.0))
        session_rejections = float(c.get("session_rejection", 0.0))
        load = business_load()

        state.tick(
            rng, load=load, err_mult=err_mult, thread_pool_exhausted=thread_pool_exhausted,
            gc_activity=gc_activity,
        )
        ts_ms = int(time.time() * 1000)
        metrics, bulk_docs = _build_metrics_and_docs(
            state,
            rng,
            load=load,
            err_mult=err_mult,
            thread_pool_exhausted=thread_pool_exhausted,
            max_threads=max_threads,
            resource=resource,
            timestamp_ms=ts_ms,
            connection_pool_saturation=conn_sat,
            fd_pressure=fd_pressure,
            cpu_load=cpu_load,
            heap_pressure=heap_pressure,
            gc_activity=gc_activity,
            latency_ms=latency_ms,
            session_rejections=session_rejections,
        )
        client.send_metrics_scoped(resource, scope, metrics)

        if es_bulk is not None and es_bulk.configured and bulk_docs:
            # Sibling fields for multi-metric panels (error rate, util %, …).
            # Same pattern as etcd_otel / rabbitmq_otel / traefik_otel.
            #
            # TSDS document ids are derived from dimensions + @timestamp. Bulk
            # docs that share empty attributes / the same resource and the
            # exact same ms timestamp collide (409) — only the first create
            # wins, so FD Open+Max and CPU pairs never land and File
            # Descriptor Utilization stays 0% (max_fds null → CASE → 0).
            for i, doc in enumerate(bulk_docs):
                doc["@timestamp"] = ts_ms + i
            if es_bulk.consecutive_failures >= es_bulk.max_failures_before_backoff:
                logger.warning(
                    "Clearing ESBulk backoff so tomcat co-located docs can retry"
                )
                es_bulk.consecutive_failures = 0
            indexed = es_bulk.send_bulk(
                _METRICS_DATA_STREAM,
                bulk_docs,
                dynamic_templates=_gauge_templates_for_docs(bulk_docs),
            )
            if indexed == 0:
                logger.warning(
                    "Tomcat co-located bulk indexed 0/%d docs into %s",
                    len(bulk_docs),
                    _METRICS_DATA_STREAM,
                )

    run_tick_loop(stop_event, _emit_tick, interval=lambda: EMIT_INTERVAL_S)

    if es_bulk is not None:
        es_bulk.close()
    logger.info("Tomcat generator stopped.")


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
