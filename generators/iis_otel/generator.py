#!/usr/bin/env python3
"""IIS Metrics Generator — sends IIS OTel receiver metrics via OTLP.

Emits data matching the iis_otel content package
(discovery.datasets: iisreceiver.otel), which populates the bundled
IIS dashboards.

The real iisreceiver scrapes three scopes:
  - Site metrics (resource attribute ``iis.site``)
  - Application-pool metrics (resource attribute ``iis.application_pool``)
  - Host-total metrics (no site/pool attribute) — ``iis.thread.active``

Metric names / types / attributes verified against:
  - open-telemetry/opentelemetry-collector-contrib/receiver/iisreceiver/metadata.yaml
  - elastic/integrations/packages/iis_otel/kibana/dashboard/*.json

Usage (standalone):
    python3 -m generators.iis_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/iis_otel@0.5.2"

import logging
import os
import random
import signal
import threading
from typing import Any

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
    stable_counter_start_ns,
)

METRIC_NAMES = (
    "iis.application_pool.state",
    "iis.application_pool.uptime",
    "iis.connection.active",
    "iis.connection.anonymous",
    "iis.connection.attempt.count",
    "iis.request.count",
    "iis.request.queue.age.max",
    "iis.request.queue.count",
    "iis.request.rejected",
    "iis.thread.active",
    "iis.uptime",
    "iis.network.blocked",
    "iis.network.file.count",
    "iis.network.io",
)

# Request method attribute values from iisreceiver metadata.yaml.
REQUEST_METHODS = ("delete", "get", "head", "options", "post", "put", "trace")
# Approximate share of each method in typical IIS traffic.
REQUEST_WEIGHTS = {
    "get": 0.62,
    "post": 0.28,
    "head": 0.04,
    "options": 0.02,
    "put": 0.02,
    "delete": 0.015,
    "trace": 0.005,
}

IIS_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/iisreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("iis-generator")
BATCH_INTERVAL_MIN = 10
BATCH_INTERVAL_MAX = 30

# App-pool state codes from iisreceiver docs (3 = Running).
POOL_STATE_RUNNING = 3


def _default_cloud() -> dict:
    # IIS is Windows — simulate AWS EC2 Windows instance
    return {"provider": "aws", "platform": "aws_ec2",
            "region": "us-east-1", "availability_zone": "us-east-1a"}


class SiteState(CounterState):
    """Per-site cumulative counters (Web Service perf object)."""

    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.connection_anonymous = rng.randint(5_000, 2_000_000)
        self.connection_attempts = rng.randint(5_000, 2_000_000)
        self.request_counts = {
            method: rng.randint(1_000, 2_000_000) for method in REQUEST_METHODS
        }
        self.net_bytes_in = rng.randint(100_000_000, 50_000_000_000)
        self.net_bytes_out = rng.randint(500_000_000, 100_000_000_000)
        self.net_files_in = rng.randint(1_000, 250_000)
        self.net_files_out = rng.randint(1_000, 250_000)
        self.blocked_bytes = 0
        self.uptime = rng.randint(3600, 2_592_000)

    def tick(
        self,
        rng: random.Random,
        active_chaos: dict | None = None,
        connection_error_rate: float = 0.0,
        bandwidth_throttle: float = 0.0,
    ) -> None:
        load = business_load()
        delta = int(rng.randint(10, 2_000) * load)
        self.connection_attempts += int(delta * rng.uniform(0.9, 1.1))
        self.connection_anonymous += int(delta * rng.uniform(0.2, 0.4))
        for method in REQUEST_METHODS:
            share = REQUEST_WEIGHTS[method]
            self.request_counts[method] += max(1, int(delta * share))
        self.net_bytes_in += int(delta * rng.randint(500, 5_000))
        self.net_bytes_out += int(delta * rng.randint(2_000, 20_000))
        self.net_files_in += max(0, int(delta * 0.05))
        self.net_files_out += max(0, int(delta * 0.08))
        # Bandwidth-throttle alert fires on any INCREASE — keep healthy at 0.
        if connection_error_rate > 0 or bandwidth_throttle > 0:
            mult = max(connection_error_rate, bandwidth_throttle)
            self.blocked_bytes += int(
                rng.randint(1_000, 50_000) * (1.0 + 10.0 * mult)
            )
        self.uptime += 15


class PoolState(CounterState):
    """Per-app-pool cumulative counters (HTTP Service Request Queues / APP_POOL_WAS)."""

    def __init__(self, rng: random.Random):
        super().__init__(rng)
        self.rejected = 0
        self.uptime_ms = rng.randint(3_600_000, 2_592_000_000)

    def tick(
        self,
        rng: random.Random,
        active_chaos: dict | None = None,
        connection_error_rate: float = 0.0,
    ) -> None:
        # Rejection alert fires on any INCREASE — keep healthy at 0.
        if connection_error_rate > 0:
            self.rejected += int(rng.randint(1, 20) * (1.0 + 10.0 * connection_error_rate))
        # Uptime is a gauge of ms since last restart; advance ~batch interval.
        self.uptime_ms += 15_000


def _base_resource_kwargs(
    ns: str, service_name: str, host_name: str, cloud: dict,
) -> dict[str, Any]:
    return {
        "ns": ns,
        "signal_type": "metrics",
        "dataset": otel_dataset("iisreceiver"),
        "service_name": service_name,
        "service_version": "10.0",
        "service_instance_id": f"{service_name}-001",
        "host_name": host_name,
        "cloud": cloud,
    }


def _site_resource(base: dict[str, Any], site: str) -> dict[str, Any]:
    return build_signal_resource(
        **base,
        extra_attrs={"iis.site": site, "os.type": "windows"},
    )


def _pool_resource(base: dict[str, Any], pool: str) -> dict[str, Any]:
    return build_signal_resource(
        **base,
        extra_attrs={"iis.application_pool": pool, "os.type": "windows"},
    )


def _host_resource(base: dict[str, Any]) -> dict[str, Any]:
    return build_signal_resource(
        **base,
        extra_attrs={"os.type": "windows"},
    )


def _site_metrics(
    state: SiteState,
    rng: random.Random,
    load: float,
    active_chaos: dict | None,
    connection_error_rate: float = 0.0,
) -> list[dict]:
    """Metrics emitted with resource attribute ``iis.site``."""
    conn_mult = 1.0 + 2.0 * connection_error_rate
    metrics: list[dict] = [
        build_non_monotonic_sum(
            "iis.connection.active", "{connections}",
            int(rng.randint(10, 300) * load * conn_mult),
            state.start_time_ns,
        ),
        build_cumulative_sum(
            "iis.connection.anonymous", "{connections}",
            state.connection_anonymous, state.start_time_ns,
        ),
        build_cumulative_sum(
            "iis.connection.attempt.count", "{attempts}",
            state.connection_attempts, state.start_time_ns,
        ),
        build_gauge("iis.uptime", "s", state.uptime),
        build_cumulative_sum(
            "iis.network.blocked", "By",
            state.blocked_bytes, state.start_time_ns,
        ),
        build_cumulative_sum(
            "iis.network.file.count", "{files}",
            state.net_files_in, state.start_time_ns,
            {"direction": "received"},
        ),
        build_cumulative_sum(
            "iis.network.file.count", "{files}",
            state.net_files_out, state.start_time_ns,
            {"direction": "sent"},
        ),
        build_cumulative_sum(
            "iis.network.io", "By",
            state.net_bytes_in, state.start_time_ns,
            {"direction": "received"},
        ),
        build_cumulative_sum(
            "iis.network.io", "By",
            state.net_bytes_out, state.start_time_ns,
            {"direction": "sent"},
        ),
    ]
    for method, count in state.request_counts.items():
        metrics.append(
            build_cumulative_sum(
                "iis.request.count", "{requests}",
                count, state.start_time_ns,
                {"request": method},
            )
        )
    return metrics


def _pool_metrics(
    state: PoolState,
    rng: random.Random,
    active_chaos: dict | None,
    lat_mult: float,
    connection_error_rate: float = 0.0,
    queue_depth_mult: float = 1.0,
    pool_not_running: float = 0.0,
) -> list[dict]:
    """Metrics emitted with resource attribute ``iis.application_pool``."""
    # Healthy baseline: always Running (3). Chaos can stop the pool.
    # OOTB alert: state != 3. Drive via application_pool_not_running (not
    # only request_rejections / connection_error_rate).
    if pool_not_running > 0 or (
        connection_error_rate > 0 and rng.random() < min(1.0, connection_error_rate)
    ):
        pool_state = rng.choice([4, 5, 6])  # Disabling / Disabled / Shutdown Pending
    else:
        pool_state = POOL_STATE_RUNNING

    # Healthy queue depth stays under alert threshold (10); age under 5000 ms.
    queue_depth = int(rng.randint(0, 8) * queue_depth_mult)
    if connection_error_rate > 0:
        queue_depth = max(queue_depth, int(11 + 40 * connection_error_rate))
    queue_age = int(rng.uniform(0.0, 400.0) * lat_mult)
    if lat_mult > 1.0:
        queue_age = max(queue_age, int(5000 * lat_mult))

    return [
        build_gauge("iis.application_pool.state", "{state}", pool_state),
        build_gauge("iis.application_pool.uptime", "{ms}", state.uptime_ms),
        build_cumulative_sum(
            "iis.request.rejected", "{requests}",
            state.rejected, state.start_time_ns,
        ),
        build_non_monotonic_sum(
            "iis.request.queue.count", "{requests}",
            queue_depth,
            state.start_time_ns,
        ),
        build_gauge(
            "iis.request.queue.age.max", "ms",
            queue_age,
        ),
    ]



from generators.iis_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("iis")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("iis")
    cloud = cfg.get("cloud") or _default_cloud()

    default_sites = [ScenarioIdentity(ns).label("site1"), ScenarioIdentity(ns).label("site2")]
    default_pools = [ScenarioIdentity(ns).label("DefaultAppPool"), ScenarioIdentity(ns).label("api-pool")]
    site_names: list[str] = cfg.get("site_names") or default_sites
    pool_names: list[str] = cfg.get("pool_names") or default_pools

    site_states = {name: SiteState(rng) for name in site_names}
    pool_states = {name: PoolState(rng) for name in pool_names}


    base = _base_resource_kwargs(ns, service_name, host_name, cloud)
    scope = {"name": IIS_SCOPE, "version": SCOPE_VERSION}
    logger.info(
        "IIS metrics generator started (namespace=%s, sites=%s, pools=%s)",
        ns, site_names, pool_names,
    )

    def _emit_tick() -> None:
        # One stable start for every sum keeps each series continuous (GUARDRAILS #23).
        sum_start = stable_counter_start_ns()
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="iis_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        c = active_chaos or {}
        request_queue_age_elevated = float(impact_value(c, "request_queue_age_elevated", default=0.0) or 0.0)
        request_queue_depth_elevated = float(impact_value(c, "request_queue_depth_elevated", default=0.0) or 0.0)
        lat_mult = max(1.0, request_queue_age_elevated) if request_queue_age_elevated > 0 else 1.0
        err_rate = float(impact_value(c, "request_rejections_detected", default=0.0) or 0.0)
        queue_mult = max(request_queue_age_elevated, request_queue_depth_elevated, 1.0)
        pool_not_running = float(c.get("application_pool_not_running", 0.0))
        bandwidth_throttle = float(c.get("bandwidth_throttling_detected", 0.0))
        load = business_load()

        for site, state in site_states.items():
            state.tick(
                rng, active_chaos,
                connection_error_rate=err_rate,
                bandwidth_throttle=bandwidth_throttle,
            )
            client.send_metrics_scoped(
                _site_resource(base, site),
                scope,
                _site_metrics(state, rng, load, active_chaos, err_rate),
            )

        for pool, state in pool_states.items():
            state.tick(rng, active_chaos, connection_error_rate=err_rate)
            client.send_metrics_scoped(
                _pool_resource(base, pool),
                scope,
                _pool_metrics(
                    state, rng, active_chaos, lat_mult,
                    connection_error_rate=err_rate,
                    queue_depth_mult=queue_mult,
                    pool_not_running=pool_not_running,
                ),
            )

        # Host-total metric (Process/_Total Thread Count) — empty site/pool resource.
        client.send_metrics_scoped(
            _host_resource(base),
            scope,
            [
                build_non_monotonic_sum(
                    "iis.thread.active", "{threads}",
                    int(rng.randint(5, 100) * load * len(pool_names)), start_time_ns=sum_start,
                ),
            ],
        )

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("IIS metrics generator stopped.")


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
