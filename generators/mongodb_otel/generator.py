#!/usr/bin/env python3
"""MongoDB Generator — sends mongodbreceiver OTel metrics via OTLP.

Emits data matching the mongodb_otel content package
(discovery.datasets: mongodbreceiver.otel), which populates the bundled
Overview, Capacity, and Operations dashboards.

Signal types:
  - Metrics: mongodb.cache.operations, mongodb.connection.count,
             mongodb.cursor.count, mongodb.cursor.timeout.count,
             mongodb.data.size, mongodb.global_lock.time,
             mongodb.index.size, mongodb.memory.usage,
             mongodb.network.io.receive, mongodb.network.io.transmit,
             mongodb.operation.count, mongodb.operation.time,
             mongodb.session.count, mongodb.storage.size

Field names and attribute keys come from:
  - opentelemetry-collector-contrib/receiver/mongodbreceiver/metadata.yaml
  - elastic/integrations/packages/mongodb_otel/kibana/dashboard/*.json

The Server control filters on attributes.mongodb.instance — that key is not
emitted by mongodbreceiver itself; the package docs require an attributes
processor to upsert it onto every datapoint. We emit it the same way.

Network I/O metrics are cumulative non-monotonic sums (monotonic: false).
The Operations dashboard calls DELTA() on them; monotonic counters map to
counter_long and reject DELTA().

Capacity "Current Connection Utilization" does
``current / (current + avail)`` without ``TO_DOUBLE``. On current ES,
``long / long`` is integer division (truncates → 0% on the gauge). Emit
connection counts as OTLP ``asDouble`` and clear sticky ``long`` mappings
on startup so the ratio is a real fraction (same pattern as apache_tomcat_otel).

Usage (standalone):
    python3 -m generators.mongodb_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/mongodb_otel@0.3.1"

# Assumed emit cadence for translating lock-held rate (ms/s) → per-tick bump.
# Real ticks are ~10–15s; overestimate slightly so RATE stays above threshold.
_LOCK_TICK_SECONDS = 15.0

import logging
import os
import random
import signal
import threading

from app.telemetry import OTLPClient
from app.config import NAMESPACE
from generator_engine.db_common import (
    impact_value,
    CounterState,
    build_cumulative_sum,
    build_non_monotonic_sum,
    build_signal_resource,
    business_load,
    resolve_active_chaos,
)

_METRICS_DATA_STREAM = "metrics-mongodbreceiver.otel-default"
# Ratio panel only needs this field as double; other mongodb.* stay asInt.
_RATIO_FLOAT_FIELDS = ("mongodb.connection.count",)

# ── Grounding constants — do not change names ─────────────────────────────────

METRIC_NAMES = (
    "mongodb.cache.operations",
    "mongodb.connection.count",
    "mongodb.cursor.count",
    "mongodb.cursor.timeout.count",
    "mongodb.data.size",
    "mongodb.global_lock.time",
    "mongodb.index.size",
    "mongodb.memory.usage",
    "mongodb.network.io.receive",
    "mongodb.network.io.transmit",
    "mongodb.operation.count",
    "mongodb.operation.time",
    "mongodb.session.count",
    "mongodb.storage.size",
)

MONGO_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/mongodbreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("mongodb-generator")

BATCH_INTERVAL_MIN = 5
BATCH_INTERVAL_MAX = 15

CACHE_TYPES = ["hit", "miss"]
CONNECTION_TYPES = ["available", "current", "active"]
MEMORY_TYPES = ["resident", "virtual", "mapped"]
OPERATION_NAMES = ["insert", "query", "update", "delete", "getmore", "command"]

DEFAULT_SERVER_PORT = 27017


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_ec2",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


def _fields_need_mapping_reset(elastic_url: str, api_key: str) -> bool:
    """True when connection.count is stuck as plain long (ratio panel → 0%)."""
    import httpx

    field_list = ",".join(f"metrics.{name}" for name in _RATIO_FLOAT_FIELDS)
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
    for name in _RATIO_FLOAT_FIELDS:
        caps = fields.get(f"metrics.{name}") or fields.get(name) or {}
        for meta in caps.values():
            if meta.get("type") == "long":
                return True
    return False


def _reset_sticky_long_mapping() -> None:
    """Delete the metrics data stream when connection.count is stuck as long.

    Fresh ``asDouble`` samples recreate the stream so Capacity utilization
    is a real fraction. No-op when ES credentials are absent or mapping is
    already double.
    """
    try:
        from app.telemetry import ESBulkClient as _Bulk
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for mapping check: %s", exc)
        return

    try:
        client = _Bulk()
    except Exception as exc:
        logger.warning("ESBulkClient init failed for mapping check: %s", exc)
        return

    if not client.configured:
        return

    if not _fields_need_mapping_reset(client.elastic_url, client.api_key):
        logger.info(
            "mongodb.connection.count is not stuck as long; leaving %s alone",
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
            "Deleted %s so connection.count remaps as double (was long)",
            _METRICS_DATA_STREAM,
        )
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist yet", _METRICS_DATA_STREAM)
    else:
        logger.warning(
            "Could not delete %s (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )


def _instance_attrs(instance: str, extra: dict | None = None) -> dict:
    """Datapoint attrs including mongodb.instance (Server control)."""
    attrs = {"mongodb.instance": instance}
    if extra:
        attrs.update(extra)
    return attrs


class MongoDBMetricState(CounterState):
    """Cumulative counter state for MongoDB."""

    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.cache_ops = {t: rng.randint(100_000, 10_000_000) for t in CACHE_TYPES}
        self.network_recv = rng.randint(10_000_000, 5_000_000_000)
        self.network_xmit = rng.randint(10_000_000, 5_000_000_000)
        self.operations = {op: rng.randint(10_000, 5_000_000) for op in OPERATION_NAMES}
        self.op_time = {op: rng.randint(1_000, 10_000_000) for op in OPERATION_NAMES}
        self.cursor_timeouts = rng.randint(0, 1_000)
        self.global_lock = rng.randint(0, 100_000)
        self.last_cache_hit_delta = 0
        self.last_cache_miss_delta = 0
        self.last_op_count_delta = 0
        self.last_op_time_delta = 0
        self.last_cursor_timeout_delta = 0
        self.last_global_lock_delta = 0
        # Non-monotonic size gauges kept in state so LAST_OVER_TIME is stable.
        self.data_size = {i: rng.randint(1_000_000, 5_000_000_000) for i in range(8)}
        self.storage_size = {}
        self.index_size = {}
        for i, data in self.data_size.items():
            self.storage_size[i] = int(data * rng.uniform(1.1, 2.0))
            self.index_size[i] = int(data * rng.uniform(0.05, 0.3))

    def tick(
        self,
        rng: random.Random,
        *,
        op_multiplier: float = 1.0,
        cursor_timeout_delta: float = 0.0,
        global_lock_rate: float = 0.0,
        cache_miss_rate: float = 0.0,
    ):
        load = business_load()
        # Cache hit/miss — healthy miss share stays well under 5%.
        base_cache = rng.randint(1_000, 5_000)
        if cache_miss_rate > 0:
            miss = max(1, int(base_cache * max(cache_miss_rate, 0.06)))
            hit = max(1, base_cache - miss)
        else:
            miss = max(1, int(base_cache * rng.uniform(0.01, 0.03)))
            hit = base_cache - miss
        self.cache_ops["hit"] += hit
        self.cache_ops["miss"] += miss
        self.last_cache_hit_delta = hit
        self.last_cache_miss_delta = miss

        self.network_recv += rng.randint(10_000, 1_000_000)
        self.network_xmit += rng.randint(10_000, 500_000)
        self.last_op_count_delta = 0
        self.last_op_time_delta = 0
        for op in OPERATION_NAMES:
            delta = int(rng.randint(10, 1_000) * load)
            if op_multiplier > 1.0:
                delta = int(delta * op_multiplier)
            # Healthy avg latency ~1–5ms; fault >100ms when op_multiplier > 1.
            if op_multiplier > 1.0:
                time_delta = int(delta * rng.uniform(120.0, 250.0))
            else:
                time_delta = int(delta * rng.uniform(1.0, 5.0))
            self.operations[op] += delta
            self.op_time[op] += time_delta
            self.last_op_count_delta += delta
            self.last_op_time_delta += time_delta

        timeout_bump = int(cursor_timeout_delta) if cursor_timeout_delta > 0 else 0
        self.cursor_timeouts += timeout_bump
        self.last_cursor_timeout_delta = timeout_bump

        # Alert is SUM(RATE(mongodb.global_lock.time)) > 500 (ms/sec).
        # Impact key is the target held rate (ms/s), not a raw per-tick bump —
        # GT 3750 at a ~12–15s emit cadence only yields ~250–300/s if used raw.
        if global_lock_rate > 0:
            target_rate = max(float(global_lock_rate), 600.0)
            lock_bump = int(target_rate * _LOCK_TICK_SECONDS)
        else:
            lock_bump = rng.randint(0, 100)
        self.global_lock += lock_bump
        self.last_global_lock_delta = lock_bump

        for i in list(self.data_size):
            growth = rng.randint(0, 50_000)
            self.data_size[i] += growth
            self.storage_size[i] += int(growth * rng.uniform(1.0, 1.5))
            self.index_size[i] += int(growth * rng.uniform(0.0, 0.2))


def _generate_metrics(
    state: MongoDBMetricState,
    databases: list[str],
    instance: str,
    rng: random.Random,
    active_chaos: dict | None = None,
) -> list[dict]:
    chaos = active_chaos or {}
    op_mult = float(impact_value(chaos, "high_operation_latency", default=1.0) or 1.0)
    cursor_timeout_delta = float(impact_value(chaos, "cursor_timeouts", default=0.0) or 0.0)
    global_lock_rate = float(impact_value(chaos, "global_lock_contention", default=0.0) or 0.0)
    cache_miss_rate = float(impact_value(chaos, "high_cache_miss_rate", default=0.0) or 0.0)
    cursor_count = float(impact_value(chaos, "high_cursor_count", default=0.0) or 0.0)
    resident_memory_bytes = int(impact_value(chaos, "high_memory_usage", default=0) or 0)
    session_count = float(impact_value(chaos, "high_session_count", default=0.0) or 0.0)

    state.tick(
        rng,
        op_multiplier=op_mult,
        cursor_timeout_delta=cursor_timeout_delta,
        global_lock_rate=global_lock_rate,
        cache_miss_rate=cache_miss_rate,
    )
    metrics: list[dict] = []
    load = business_load()

    # ── Instance-level metrics ──
    for cache_type in CACHE_TYPES:
        metrics.append(build_cumulative_sum(
            "mongodb.cache.operations", "{operation}", state.cache_ops[cache_type],
            state.start_time_ns, _instance_attrs(instance, {"type": cache_type}),
        ))

    # Connections — SPEC default 100.0 = healthy; alert is available < 10.
    # GT probe (e.g. 5.0) is a target available count, not a pool multiplier.
    # Legacy multipliers (1 < v < 100, e.g. 2.0) still shrink available.
    conn_raw = float(impact_value(chaos, "connection_exhaustion", default=100.0) or 100.0)
    total = int(200 * load) + rng.randint(0, 50)
    if conn_raw >= 100.0 or conn_raw <= 0:
        available = max(50, 500 - total)
    elif conn_raw < 10.0:
        available = max(0, min(9, int(conn_raw)))
        total = max(total, 500 - available)
    else:
        total = min(495, int(total * conn_raw * 2))
        available = max(0, min(9, 500 - total))
    for conn_type, value in [
        ("available", float(available)),
        ("current", float(total)),
        ("active", float(int(total * 0.7))),
    ]:
        metrics.append(build_non_monotonic_sum(
            "mongodb.connection.count", "{connection}", value,
            state.start_time_ns, _instance_attrs(instance, {"type": conn_type}),
        ))

    # Cursors
    if cursor_count > 0:
        cursors = int(cursor_count)
    else:
        cursors = rng.randint(0, 50)
    metrics.append(build_non_monotonic_sum(
        "mongodb.cursor.count", "{cursor}", cursors,
        state.start_time_ns, _instance_attrs(instance),
    ))
    metrics.append(build_non_monotonic_sum(
        "mongodb.cursor.timeout.count", "{cursor}", state.cursor_timeouts,
        state.start_time_ns, _instance_attrs(instance),
    ))

    # Global lock time — monotonic cumulative; no datapoint attrs in metadata.
    metrics.append(build_cumulative_sum(
        "mongodb.global_lock.time", "ms", state.global_lock,
        state.start_time_ns, _instance_attrs(instance),
    ))

    # Network I/O — non-monotonic sums so DELTA() works on Operations panels.
    metrics.append(build_non_monotonic_sum(
        "mongodb.network.io.receive", "By", state.network_recv,
        state.start_time_ns, _instance_attrs(instance),
    ))
    metrics.append(build_non_monotonic_sum(
        "mongodb.network.io.transmit", "By", state.network_xmit,
        state.start_time_ns, _instance_attrs(instance),
    ))

    # Operations
    for op in OPERATION_NAMES:
        metrics.append(build_cumulative_sum(
            "mongodb.operation.count", "{operation}", state.operations[op],
            state.start_time_ns, _instance_attrs(instance, {"operation": op}),
        ))
        metrics.append(build_cumulative_sum(
            "mongodb.operation.time", "ms", state.op_time[op],
            state.start_time_ns, _instance_attrs(instance, {"operation": op}),
        ))

    # Sessions
    if session_count > 0:
        sessions = int(session_count)
    else:
        sessions = int(rng.randint(1, 20) * load)
    metrics.append(build_non_monotonic_sum(
        "mongodb.session.count", "{session}", sessions,
        state.start_time_ns, _instance_attrs(instance),
    ))

    # ── Per-database metrics ──
    for db_idx, db in enumerate(databases):
        size_key = db_idx if db_idx in state.data_size else 0
        db_attrs = _instance_attrs(instance, {"db.namespace": db})
        metrics.append(build_non_monotonic_sum(
            "mongodb.data.size", "By", state.data_size[size_key],
            state.start_time_ns, db_attrs,
        ))
        metrics.append(build_non_monotonic_sum(
            "mongodb.storage.size", "By", state.storage_size[size_key],
            state.start_time_ns, db_attrs,
        ))
        metrics.append(build_non_monotonic_sum(
            "mongodb.index.size", "By", state.index_size[size_key],
            state.start_time_ns, db_attrs,
        ))

        for mem_type in MEMORY_TYPES:
            if mem_type == "resident" and resident_memory_bytes > 0:
                usage = resident_memory_bytes
            else:
                usage = rng.randint(10_000_000, 500_000_000)
            metrics.append(build_non_monotonic_sum(
                "mongodb.memory.usage", "By", usage,
                state.start_time_ns, _instance_attrs(instance, {
                    "db.namespace": db,
                    "type": mem_type,
                }),
            ))

    return metrics



from generators.mongodb_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run MongoDB generator loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {})
    else:
        ns = NAMESPACE
        cfg = {}

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    db_prefix = ns.replace("-", "_")
    databases: list[str] = cfg.get("databases") or [
        ScenarioIdentity(ns).database("primary"),
        ScenarioIdentity(ns).database("analytics"),
        ScenarioIdentity(ns).database("sessions"),
    ]

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("mongodb-primary")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("mongodb-host")
    server_port = int(cfg.get("server_port") or DEFAULT_SERVER_PORT)
    # Package docs: set mongodb.instance to the same host:port as the endpoint.
    instance = cfg.get("mongodb_instance") or f"{host_name}:{server_port}"
    cloud = cfg.get("cloud") or _default_cloud()

    resource = build_signal_resource(
        ns=ns,
        signal_type="metrics",
        dataset="mongodbreceiver",
        service_name=service_name,
        service_version="7.0.5",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name,
        cloud=cloud,
        extra_attrs={
            "server.address": host_name,
            "server.port": server_port,
        },
    )

    metric_state = MongoDBMetricState(rng)
    scope = {"name": MONGO_SCOPE, "version": SCOPE_VERSION}

    _reset_sticky_long_mapping()

    logger.info(
        "MongoDB generator started (namespace=%s, instance=%s, databases=%s)",
        ns, instance, databases,
    )

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="mongodb_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        metrics = _generate_metrics(metric_state, databases, instance, rng, active_chaos)
        client.send_metrics_scoped(resource, scope, metrics)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("MongoDB generator stopped.")


def main() -> None:  # pragma: no cover
    """Standalone entry point for local testing."""
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
