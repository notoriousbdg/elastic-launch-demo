#!/usr/bin/env python3
"""Nginx Metrics Generator — sends nginx stub_status metrics via OTLP.

Generates metrics that populate the [Metrics Nginx OTEL] Overview dashboard:
  - nginx.requests (cumulative counter)
  - nginx.connections_handled (cumulative counter)
  - nginx.connections_accepted (cumulative counter)
  - nginx.connections_current (gauge with state: active/reading/writing/waiting)

Usage (standalone):
    python3 -m generators.nginx_metrics.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/nginxreceiver"
)

import logging
import os
import random
import signal
import threading
import time
from typing import Any

from app.telemetry import OTLPClient, ESBulkClient, _format_attributes, SCHEMA_URL
from app.config import NAMESPACE
from generator_engine.db_common import resolve_active_chaos
from generator_engine.scrape_clock import scrape_now_ns, scrape_start_ns

logger = logging.getLogger("nginx-metrics-generator")

METRICS_INTERVAL = int(os.getenv("NGINX_METRICS_INTERVAL", "10"))

SCOPE_NAME = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/nginxreceiver"
)
SCOPE_VERSION = "0.115.0"

_BULK_STREAM = "metrics-nginxreceiver.otel-default"
_BULK_PARAMS = {"pipeline": "_none"}

def _nginx_hosts(ns: str) -> list[dict]:
    """Two nginx instances. Names come from ScenarioIdentity so scenarios do not share them."""
    ident = ScenarioIdentity(ns)
    clouds = (
        ("aws", "aws_ec2", "us-east-1"),
        ("gcp", "gcp_compute_engine", "us-central1"),
    )
    hosts = []
    for index, (provider, platform, region) in enumerate(clouds, start=1):
        hosts.append({
            "host.name": ident.host("nginx", index),
            "service.name": ident.service("nginx-proxy"),
            "service.instance.id": ident.instance("nginx-proxy", index),
            "cloud.provider": provider,
            "cloud.platform": platform,
            "cloud.region": region,
        })
    return hosts


# Import-time fallback for standalone mode. run() rebuilds from the active scenario.
NGINX_HOSTS = _nginx_hosts(NAMESPACE)


def _build_resource(host_cfg: dict) -> dict:
    attrs = {
        **host_cfg,
        "telemetry.sdk.name": "opentelemetry",
        "telemetry.sdk.version": "1.24.0",
        "telemetry.sdk.language": "python",
        "data_stream.type": "metrics",
        "data_stream.dataset": "nginxreceiver",
        "data_stream.namespace": "default",
    }
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def _resource_attr_map(host_cfg: dict) -> dict[str, Any]:
    return {
        "host.name": host_cfg["host.name"],
        "service.name": host_cfg.get("service.name", "nginx-proxy"),
        "service.instance.id": host_cfg["service.instance.id"],
        "cloud.provider": host_cfg.get("cloud.provider"),
        "cloud.platform": host_cfg.get("cloud.platform"),
        "cloud.region": host_cfg.get("cloud.region"),
        "telemetry.sdk.name": "opentelemetry",
        "telemetry.sdk.version": "1.24.0",
        "telemetry.sdk.language": "python",
    }


def build_nginxreceiver_bulk_docs(
    host_cfg: dict,
    *,
    requests: int,
    accepted: int,
    handled: int,
    active: int,
    reading: int,
    writing: int,
    waiting: int,
    timestamp_ms: int | None = None,
) -> list[dict[str, Any]]:
    """ES bulk companions for Nginx OTel Overview when OTLP never indexes.

    Counters share one doc (avoids OTel fingerprint collisions when
    ``handled == accepted``). ``nginx.connections_current`` stays one doc per
    ``attributes.state`` so Active/Reading/Writing/Waiting panels resolve.
    """
    ts = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
    base = {
        "@timestamp": ts,
        "data_stream": {
            "type": "metrics",
            "dataset": "nginxreceiver.otel",
            "namespace": "default",
        },
        "resource": {"attributes": _resource_attr_map(host_cfg)},
        "scope": {
            "name": SCOPE_NAME,
            "version": SCOPE_VERSION,
            "attributes": {},
        },
    }
    docs: list[dict[str, Any]] = [
        {
            **base,
            "start_timestamp": ts - 60_000,
            "attributes": {},
            "unit": "1",
            "temporality": "cumulative",
            "metrics": {
                "nginx.requests": int(requests),
                "nginx.connections_accepted": int(accepted),
                "nginx.connections_handled": int(handled),
            },
        }
    ]
    for state_name, value in (
        ("active", active),
        ("reading", reading),
        ("writing", writing),
        ("waiting", waiting),
    ):
        docs.append(
            {
                **base,
                "attributes": {"state": state_name},
                "unit": "{connection}",
                "metrics": {"nginx.connections_current": int(value)},
            }
        )
    return docs


class NginxState:
    """Tracks cumulative counter values for one nginx instance."""

    def __init__(self, rng: random.Random):
        self._rng = rng
        self.requests = rng.randint(500_000, 2_000_000)
        self.connections_accepted = rng.randint(100_000, 500_000)
        # Healthy baseline: handled == accepted. A seed gap + random 5% drops
        # phantom-trips OOTB "[Nginx OTel] Dropped connections detected"
        # (MAX-MIN delta: dropped = Δaccepted - Δhandled > 0) at steady state.
        self.connections_handled = self.connections_accepted

    def tick(self, request_rate_multiplier: float = 1.0):
        rng = self._rng
        new_requests = max(0, int(rng.randint(50, 500) * max(request_rate_multiplier, 0.0)))
        self.requests += new_requests
        new_accepted = max(0, int(rng.randint(5, 50) * max(request_rate_multiplier, 0.0)))
        self.connections_accepted += new_accepted
        # OOTB alert is host-level MAX-MIN over 5m; any persistent accepted/handled
        # drift flaps the rule, so handled always equals accepted. The dropped-connections
        # alert is driven by nginx_otel's declared dropped_connections_detected key.
        self.connections_handled = self.connections_accepted


def _build_cumulative_sum(name: str, unit: str, value: int) -> dict:
    now = scrape_now_ns()
    return {
        "name": name,
        "unit": unit,
        "sum": {
            "dataPoints": [
                {
                    "startTimeUnixNano": scrape_start_ns(),
                    "timeUnixNano": now,
                    "asInt": str(value),
                }
            ],
            "aggregationTemporality": 2,
            "isMonotonic": True,
        },
    }


def _build_gauge(name: str, unit: str, value, attributes: dict | None = None) -> dict:
    dp: dict = {"timeUnixNano": scrape_now_ns()}
    if isinstance(value, int):
        dp["asInt"] = str(value)
    else:
        dp["asDouble"] = float(value)
    if attributes:
        dp["attributes"] = _format_attributes(attributes)
    return {"name": name, "unit": unit, "gauge": {"dataPoints": [dp]}}


def _generate_metrics(
    state: NginxState,
    rng: random.Random,
    active_chaos: dict | None = None,
) -> tuple[list, dict[str, int]]:
    """Return OTLP metrics plus a flat snapshot for ES bulk companions."""
    request_rate_multiplier = float((active_chaos or {}).get("request_rate_multiplier", 1.0) or 1.0)
    active_conns_override = int((active_chaos or {}).get("active_connections", 0) or 0)

    state.tick(
        request_rate_multiplier=request_rate_multiplier if active_chaos else 1.0,
    )
    metrics = [
        _build_cumulative_sum("nginx.requests", "{request}", state.requests),
        _build_cumulative_sum(
            "nginx.connections_accepted", "{connection}", state.connections_accepted
        ),
        _build_cumulative_sum(
            "nginx.connections_handled", "{connection}", state.connections_handled
        ),
    ]

    # connections_current is a gauge with state attribute
    if active_chaos and active_conns_override > 0:
        active = active_conns_override
        reading = max(1, active // 10)
        writing = max(1, active // 5)
        waiting = max(0, active - reading - writing)
    else:
        active = rng.randint(50, 300)
        reading = rng.randint(1, 20)
        writing = rng.randint(5, 50)
        waiting = active - reading - writing
        if waiting < 0:
            waiting = rng.randint(5, 30)

    for state_name, val in [
        ("active", active),
        ("reading", reading),
        ("writing", writing),
        ("waiting", waiting),
    ]:
        metrics.append(
            _build_gauge(
                "nginx.connections_current",
                "{connection}",
                val,
                attributes={"state": state_name},
            )
        )

    snapshot = {
        "requests": state.requests,
        "accepted": state.connections_accepted,
        "handled": state.connections_handled,
        "active": active,
        "reading": reading,
        "writing": writing,
        "waiting": waiting,
    }
    return metrics, snapshot


from generators.nginx_metrics import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run nginx metrics generator loop until stop_event is set."""
    rng = random.Random()

    # Rebuild host list dynamically from scenario_data to avoid import-time freezing
    if scenario_data:
        ns = scenario_data["namespace"]
    else:
        ns = NAMESPACE
    nginx_hosts = _nginx_hosts(ns)

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    cfg = scenario_data.get("generator_config", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "any")

    resources = [_build_resource(h) for h in nginx_hosts]
    states = [NginxState(rng) for _ in nginx_hosts]

    try:
        es_bulk = ESBulkClient()
        if not es_bulk.configured:
            es_bulk = None
    except Exception as exc:
        logger.warning("ESBulkClient unavailable (%s); nginx metrics via OTLP only", exc)
        es_bulk = None

    logger.info(
        "Nginx metrics generator started (interval=%ds, hosts=%d, chaos_aware=%s, es_bulk=%s)",
        METRICS_INTERVAL,
        len(nginx_hosts),
        chaos_controller is not None,
        es_bulk is not None,
    )

    scrape_count = 0
    def _emit_tick() -> None:
        nonlocal scrape_count
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="nginx_metrics",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        resource_metrics = []
        bulk_docs: list[dict[str, Any]] = []
        for host_cfg, resource, state in zip(nginx_hosts, resources, states):
            metrics, snap = _generate_metrics(state, rng, active_chaos=active_chaos)
            resource_metrics.append(
                {
                    "resource": resource,
                    "scopeMetrics": [
                        {
                            "scope": {"name": SCOPE_NAME, "version": SCOPE_VERSION},
                            "metrics": metrics,
                        }
                    ],
                }
            )
            bulk_docs.extend(
                build_nginxreceiver_bulk_docs(
                    host_cfg,
                    requests=snap["requests"],
                    accepted=snap["accepted"],
                    handled=snap["handled"],
                    active=snap["active"],
                    reading=snap["reading"],
                    writing=snap["writing"],
                    waiting=snap["waiting"],
                )
            )

        payload = {"resourceMetrics": resource_metrics}
        client._send(f"{client.endpoint}/v1/metrics", payload, "nginx-metrics")

        if es_bulk is not None and bulk_docs:
            es_bulk.send_bulk(_BULK_STREAM, bulk_docs, params=_BULK_PARAMS)

        scrape_count += 1
        if scrape_count % 6 == 0:
            logger.info("Nginx metrics scrape %d complete", scrape_count)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: METRICS_INTERVAL)

    logger.info("Nginx metrics generator stopped after %d scrapes", scrape_count)


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
    logger.info("Running for %ds (standalone)", duration)
    run(client, stop_event)
    timer.cancel()
    client.close()


if __name__ == "__main__":
    main()
