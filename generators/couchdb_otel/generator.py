#!/usr/bin/env python3
"""CouchDB Metrics Generator — sends CouchDB OTel receiver metrics via OTLP.

Emits data matching the couchdb_otel content package
(discovery.datasets: couchdbreceiver.otel), which populates the bundled
CouchDB Overview dashboard.

Dashboard contract (couchdb_otel-overview):
  - resource attr ``couchdb.node.name`` (panels BY this dimension)
  - ``couchdb.httpd.requests`` with ``http.method``
  - ``couchdb.httpd.responses`` with ``http.status_code`` (incl. 409)
  - ``couchdb.httpd.views`` with ``view`` (view_reads / temporary_view_reads)
  - ``couchdb.database.operations`` with ``operation`` (reads / writes)
  - ``couchdb.database.open`` / ``couchdb.file_descriptor.open`` as
    non-monotonic cumulative sums (LAST_OVER_TIME panels)

Metric names verified against:
  - opentelemetry-collector-contrib/receiver/couchdbreceiver/metadata.yaml
  - elastic/integrations/packages/couchdb_otel/kibana/dashboard/*.json

Usage (standalone):
    python3 -m generators.couchdb_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/couchdb_otel@0.3.1"

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
    build_gauge,
    build_non_monotonic_sum,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

METRIC_NAMES = (
    "couchdb.database.open",
    "couchdb.file_descriptor.open",
    "couchdb.average_request_time",
    "couchdb.httpd.bulk_requests",
    "couchdb.httpd.requests",
    "couchdb.httpd.responses",
    "couchdb.httpd.views",
    "couchdb.database.operations",
)

# metadata.yaml attribute enums
HTTP_METHODS = ("COPY", "DELETE", "GET", "HEAD", "OPTIONS", "POST", "PUT")
VIEW_TYPES = ("view_reads", "temporary_view_reads")
# Status codes used by overview panels (error-rate WHERE >= "400", 409 series)
HTTP_STATUS_CODES = ("200", "201", "301", "304", "400", "401", "404", "409", "500")

COUCHDB_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/couchdbreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("couchdb-generator")
BATCH_INTERVAL_MIN = 10
BATCH_INTERVAL_MAX = 30


def _default_cloud() -> dict:
    return {"provider": "gcp", "platform": "gcp_compute_engine",
            "region": "us-central1", "availability_zone": "us-central1-b"}


class CouchDBState(CounterState):
    def __init__(self, rng: random.Random):
        # Unused parent .value — keep near zero (RATE spike guard).
        super().__init__(rng, initial=0)
        # Cumulative counters start near zero so ES|QL RATE/INCREASE charts are
        # not dominated by a one-time spike from multi-hundred-thousand initials.
        self.bulk_requests = rng.randint(0, 50)
        self.http_requests = {m: rng.randint(0, 100) for m in HTTP_METHODS}
        self.http_responses = {c: rng.randint(0, 50) for c in HTTP_STATUS_CODES}
        self.views = {v: rng.randint(0, 40) for v in VIEW_TYPES}
        self.db_reads = rng.randint(0, 200)
        self.db_writes = rng.randint(0, 80)
        self.open_databases = rng.randint(5, 50)
        self.open_fds = rng.randint(20, 200)

    def tick(
        self,
        rng: random.Random,
        error_rate: float = 0.0,
        conflict_rate: float = 0.0,
        open_fds: int = 0,
        open_databases: int = 0,
    ):
        load = business_load()
        delta = int(rng.randint(10, 1_000) * load)
        self.bulk_requests += int(delta * 0.1)

        # Weight GET/POST heaviest; keep every method moving for BY method panels.
        method_weights = {
            "GET": 0.55, "POST": 0.20, "PUT": 0.08, "DELETE": 0.05,
            "HEAD": 0.05, "OPTIONS": 0.04, "COPY": 0.03,
        }
        for method, weight in method_weights.items():
            self.http_requests[method] += max(1, int(delta * weight))

        err_frac = max(0.001, min(1.0, error_rate)) if error_rate > 0 else 0.001
        # OOTB Conflict storm: SUM(RATE(couchdb.httpd.responses)) WHERE 409 > 10.
        # SPEC/GT set impact key to a target RATE (e.g. 50.0), not a 0–1 share.
        # Clamping 50→1.0 only scales a small business_load delta and often
        # stays under 10/s — force an absolute 409 increment under chaos.
        if conflict_rate > 1.0:
            target_rps = max(float(conflict_rate), 12.0)
            conflict_delta = int(target_rps * BATCH_INTERVAL_MAX) + rng.randint(0, 50)
            residual = max(0.05, 1.0 - err_frac - 0.03)
            status_weights = {
                "200": residual * 0.85,
                "201": residual * 0.10,
                "301": residual * 0.02,
                "304": residual * 0.03,
                "400": 0.01,
                "401": 0.005,
                "404": 0.01,
                "409": 0.0,
                "500": err_frac,
            }
            for code, weight in status_weights.items():
                self.http_responses[code] += max(0, int(delta * weight))
            self.http_responses["409"] += conflict_delta
        else:
            conflict = max(0.0, min(1.0, conflict_rate))
            residual = max(0.05, 1.0 - err_frac - conflict - 0.03)
            status_weights = {
                "200": residual * 0.85,
                "201": residual * 0.10,
                "301": residual * 0.02,
                "304": residual * 0.03,
                "400": 0.01,
                "401": 0.005,
                "404": 0.01,
                "409": 0.005 + conflict,
                "500": err_frac,
            }
            for code, weight in status_weights.items():
                self.http_responses[code] += max(0, int(delta * weight))

        self.views["view_reads"] += max(1, int(delta * 0.04))
        self.views["temporary_view_reads"] += max(0, int(delta * 0.01))
        self.db_reads += int(delta * 0.8)
        self.db_writes += int(delta * 0.2)
        if open_databases > 0:
            self.open_databases = int(open_databases)
        else:
            self.open_databases = max(1, min(80, self.open_databases + rng.randint(-2, 3)))
        if open_fds > 0:
            self.open_fds = int(open_fds)
        else:
            self.open_fds = max(10, rng.randint(20, 200))


def _build_metrics(state: CouchDBState, rng: random.Random, lat_mult: float) -> list[dict]:
    """Build one scrape's metric list for the overview dashboard."""
    load = business_load()
    # OOTB latency alert: MAX_OVER_TIME(couchdb.average_request_time) > 1000.
    # GT sets impact key 100.0 as a multiplier; under low business_load that
    # often tops out ~300ms. When chaos is active (lat_mult > 1), force ms
    # above the alert threshold.
    if lat_mult > 1.0:
        latency_ms = max(
            1100.0,
            round(rng.uniform(1.0, 20.0) * max(load, 0.5) * lat_mult, 3),
        )
    else:
        latency_ms = round(rng.uniform(1.0, 20.0) * load, 3)
    metrics: list[dict] = [
        build_non_monotonic_sum(
            "couchdb.database.open", "{databases}",
            state.open_databases, state.start_time_ns,
        ),
        build_non_monotonic_sum(
            "couchdb.file_descriptor.open", "{files}",
            state.open_fds, state.start_time_ns,
        ),
        build_gauge(
            "couchdb.average_request_time", "ms",
            latency_ms,
        ),
        build_cumulative_sum(
            "couchdb.httpd.bulk_requests", "{requests}",
            state.bulk_requests, state.start_time_ns,
        ),
        build_cumulative_sum(
            "couchdb.database.operations", "{operations}",
            state.db_reads, state.start_time_ns, {"operation": "reads"},
        ),
        build_cumulative_sum(
            "couchdb.database.operations", "{operations}",
            state.db_writes, state.start_time_ns, {"operation": "writes"},
        ),
    ]
    for method, value in state.http_requests.items():
        metrics.append(build_cumulative_sum(
            "couchdb.httpd.requests", "{requests}",
            value, state.start_time_ns, {"http.method": method},
        ))
    for code, value in state.http_responses.items():
        metrics.append(build_cumulative_sum(
            "couchdb.httpd.responses", "{responses}",
            value, state.start_time_ns, {"http.status_code": code},
        ))
    for view, value in state.views.items():
        metrics.append(build_cumulative_sum(
            "couchdb.httpd.views", "{views}",
            value, state.start_time_ns, {"view": view},
        ))
    return metrics



from generators.couchdb_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("couchdb")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("couchdb")
    # CouchDB node names look like couchdb@hostname — dashboards BY couchdb.node.name.
    node_name = cfg.get("node_name") or f"couchdb@{host_name}"
    cloud = cfg.get("cloud") or _default_cloud()

    state = CouchDBState(rng)

    resource = build_signal_resource(
        ns=ns, signal_type="metrics", dataset=otel_dataset("couchdbreceiver"),
        service_name=service_name, service_version="3.3.3",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name, cloud=cloud,
        extra_attrs={"couchdb.node.name": node_name},
    )
    scope = {"name": COUCHDB_SCOPE, "version": SCOPE_VERSION}
    logger.info(
        "CouchDB metrics generator started (namespace=%s, node=%s)", ns, node_name,
    )

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="couchdb_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        chaos = active_chaos or {}
        # SPEC defaults inject sibling impact keys as 0.0 (and latency as
        # 1000.0=healthy). ``impact_value`` returns the first *present* key, so
        # ``high_5xx_server_error_rate: 0.0`` would hide
        # ``high_error_rate_4xx_and_5xx: 50.0``. Prefer the first non-zero.
        error_rate = 0.0
        for _k in ("high_5xx_server_error_rate", "high_error_rate_4xx_and_5xx"):
            if _k in chaos and chaos[_k] is not None:
                try:
                    _v = float(chaos[_k])
                except (TypeError, ValueError):
                    continue
                if _v != 0.0:
                    error_rate = _v
                    break
        lat_raw = chaos.get("high_average_request_time_latency")
        # SPEC default 1000.0 means healthy; GT chaos sets 100.0.
        try:
            lat_v = float(lat_raw) if lat_raw is not None else 0.0
        except (TypeError, ValueError):
            lat_v = 0.0
        lat_mult = lat_v if 0.0 < lat_v < 1000.0 else 1.0
        conflict_rate = float(impact_value(chaos, "conflict_storm_elevated_409_responses", default=0.0) or 0.0)
        open_fds = int(impact_value(chaos, "file_descriptor_exhaustion_risk", default=0) or 0)
        open_databases = int(impact_value(chaos, "open_databases_count_high", default=0) or 0)
        state.tick(
            rng,
            error_rate=error_rate,
            conflict_rate=conflict_rate,
            open_fds=open_fds,
            open_databases=open_databases,
        )
        client.send_metrics_scoped(resource, scope, _build_metrics(state, rng, lat_mult))
    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("CouchDB metrics generator stopped.")


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
