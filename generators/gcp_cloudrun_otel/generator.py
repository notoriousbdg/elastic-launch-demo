#!/usr/bin/env python3
"""GCP Cloud Run generator: Cloud Monitoring metrics via OTLP.

Emits what the OpenTelemetry ``googlecloudmonitoring`` receiver produces for
Cloud Run services, shaped for the ``gcp_cloudrun_otel`` content package
(``metrics-gcp.cloudrun.otel-default``): the Cloud Run Overview and Cloud Run
Service Detail dashboards.

Resource (one OTLP resource per ``cloud_run_revision``): ``gcp.resource_type``,
``project_id``, ``service_name``, ``revision_name``, ``configuration_name``,
``location``.

Metric shapes, per 60 s alignment period:
  - DISTRIBUTION metrics (``request_latencies``, ``container/cpu/utilizations``,
    ``container/memory/utilizations``, ``container/max_request_concurrencies``,
    ``container/startup_latencies``) are OTLP delta exponential histograms; the
    dashboards read them with ``::exponential_histogram`` + ``PERCENTILE``:
      run.googleapis.com/request_latencies::exponential_histogram
      run.googleapis.com/container/cpu/utilizations::exponential_histogram
      run.googleapis.com/container/memory/utilizations::exponential_histogram
      run.googleapis.com/container/max_request_concurrencies::exponential_histogram
      run.googleapis.com/container/startup_latencies::exponential_histogram
  - DELTA scalars (``request_count``, ``billable_instance_time``,
    ``*/allocation_time``, ``network/*_bytes_count``) are gauges holding the
    per-interval value (dashboards use ``SUM_OVER_TIME`` and plain ``SUM``).
  - ``container/instance_count`` is a gauge split by ``state`` (active/idle).

Labels: ``response_code`` + ``response_code_class`` on request metrics,
``state`` on instance_count and max_request_concurrencies, ``kind`` on
network metrics. Utilizations are 0-1 ratios; latencies are milliseconds.

Usage (standalone):
    python3 -m generators.gcp_cloudrun_otel.generator
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/gcp_cloudrun_otel@0.3.0"

import hashlib
import logging
import math
import os
import random
import signal
import threading

from app.config import NAMESPACE
from app.telemetry import OTLPClient, _format_attributes
from generator_engine.db_common import (
    CounterState,
    MultiEntityState,
    build_gauge,
    build_signal_resource,
    business_load,
    impact_value,
    otel_dataset,
    resolve_active_chaos,
)
from generator_engine.identity import ScenarioIdentity
from generator_engine.scrape_clock import scrape_now_ns, scrape_start_ns
from generator_engine.tick import run_tick_loop

_P = "run.googleapis.com/"

METRIC_NAMES = (
    "run.googleapis.com/request_count",
    "run.googleapis.com/request_latencies",
    "run.googleapis.com/container/instance_count",
    "run.googleapis.com/container/billable_instance_time",
    "run.googleapis.com/container/cpu/allocation_time",
    "run.googleapis.com/container/memory/allocation_time",
    "run.googleapis.com/container/cpu/utilizations",
    "run.googleapis.com/container/memory/utilizations",
    "run.googleapis.com/container/max_request_concurrencies",
    "run.googleapis.com/container/startup_latencies",
    "run.googleapis.com/container/network/received_bytes_count",
    "run.googleapis.com/container/network/sent_bytes_count",
)

HISTOGRAM_METRICS = (
    "run.googleapis.com/request_latencies",
    "run.googleapis.com/container/cpu/utilizations",
    "run.googleapis.com/container/memory/utilizations",
    "run.googleapis.com/container/max_request_concurrencies",
    "run.googleapis.com/container/startup_latencies",
)

GCM_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/"
    "googlecloudmonitoringreceiver"
)
SCOPE_VERSION = "0.115.0"

INTERVAL_S = 60
EXP_SCALE = 3
MAX_SAMPLES = 400

# Healthy response mix: (code, class, share)
RESPONSE_MIX = (
    ("200", "2xx", 0.962),
    ("201", "2xx", 0.018),
    ("304", "3xx", 0.010),
    ("404", "4xx", 0.0075),
    ("429", "4xx", 0.0015),
    ("500", "5xx", 0.0004),
    ("503", "5xx", 0.0002),
)

# role -> (rps at peak, median latency ms, cpu, memory GiB, cold start median ms)
SERVICE_PROFILES = {
    "checkout-api": (42.0, 48.0, 1.0, 0.5, 900.0),
    "image-resizer": (11.0, 210.0, 2.0, 2.0, 1800.0),
    "notification-worker": (6.0, 28.0, 1.0, 0.5, 650.0),
}
DEFAULT_PROFILE = (8.0, 60.0, 1.0, 0.5, 900.0)

logger = logging.getLogger("gcp-cloudrun-generator")


def _default_cloud() -> dict:
    return {
        "provider": "gcp",
        "platform": "gcp_cloud_run",
        "region": "us-central1",
        "availability_zone": "us-central1-a",
    }


def _suffix(seed: str) -> str:
    digest = hashlib.sha256(seed.encode()).hexdigest()
    letters = "abcdefghijklmnopqrstuvwxyz"
    return "".join(letters[int(digest[i:i + 2], 16) % 26] for i in range(0, 6, 2))


def build_revisions(ident: ScenarioIdentity, roles: list[str], project_id: str,
                    location: str) -> list[dict]:
    """One descriptor per revision. The first service carries a 90/10 canary."""
    revisions: list[dict] = []
    for s_index, role in enumerate(roles):
        service = ident.qualify(role)
        profile = SERVICE_PROFILES.get(role, DEFAULT_PROFILE)
        gen = 40 + 7 * s_index
        splits = [(gen, 0.9), (gen + 1, 0.1)] if s_index == 0 else [(gen, 1.0)]
        for number, share in splits:
            revisions.append({
                "service": service,
                "service_index": s_index,
                "revision": f"{service}-{number:05d}-{_suffix(service + str(number))}",
                "share": share,
                "project_id": project_id,
                "location": location,
                "profile": profile,
            })
    return revisions


class RevisionState(CounterState):
    """Per-revision drift for CPU and memory utilisation."""

    def __init__(self, rng: random.Random):
        super().__init__(rng, initial=0)
        self.cpu = rng.uniform(0.18, 0.35)
        self.mem = rng.uniform(0.38, 0.55)

    def tick(self, rng: random.Random, drift: int = 0) -> None:
        self.cpu = min(0.6, max(0.1, self.cpu + rng.uniform(-0.02, 0.02)))
        self.mem = min(0.7, max(0.3, self.mem + rng.uniform(-0.01, 0.01)))


def _now_ns(now: float) -> str:
    return str(int(now * 1_000_000_000))


def build_exponential_histogram(
    name: str,
    unit: str,
    samples: list[float],
    total_count: int,
    attributes: dict,
    now: float | None = None,
) -> dict:
    """Delta OTLP exponential histogram from samples scaled to ``total_count``.

    ``now`` (epoch seconds) pins the point's time; left out, the point takes
    the scrape's timestamp so it shares one document time with the gauges.
    """
    total_count = max(1, int(total_count))
    factor = (2 ** EXP_SCALE) / math.log(2)
    raw: dict[int, int] = {}
    zero = 0
    for value in samples:
        if value <= 0:
            zero += 1
            continue
        idx = math.ceil(math.log(value) * factor) - 1
        raw[idx] = raw.get(idx, 0) + 1
    n = max(1, len(samples))
    # Largest-remainder scaling so bucket counts sum to total_count exactly.
    parts = {k: c * total_count / n for k, c in raw.items()}
    zero_part = zero * total_count / n
    counts = {k: int(v) for k, v in parts.items()}
    zero_count = int(zero_part)
    remainder = total_count - sum(counts.values()) - zero_count
    order = sorted(parts, key=lambda k: parts[k] - counts[k], reverse=True)
    for k in order[:max(0, remainder)]:
        counts[k] += 1
    if remainder > len(order):
        zero_count += remainder - len(order)
    dp: dict = {
        "startTimeUnixNano": scrape_start_ns(int(INTERVAL_S)) if now is None else _now_ns(now - INTERVAL_S),
        "timeUnixNano": scrape_now_ns() if now is None else _now_ns(now),
        "count": str(total_count),
        "sum": float(sum(samples) / n * total_count),
        "scale": EXP_SCALE,
        "zeroCount": str(zero_count),
        "min": float(min(samples)) if samples else 0.0,
        "max": float(max(samples)) if samples else 0.0,
        "attributes": _format_attributes(attributes),
    }
    if counts:
        lo, hi = min(counts), max(counts)
        dp["positive"] = {
            "offset": lo,
            "bucketCounts": [str(counts.get(i, 0)) for i in range(lo, hi + 1)],
        }
    return {
        "name": name,
        "unit": unit,
        "exponentialHistogram": {"dataPoints": [dp], "aggregationTemporality": 1},
    }


def _lognormal(rng: random.Random, median: float, sigma: float, n: int) -> list[float]:
    return [median * math.exp(rng.gauss(0.0, sigma)) for _ in range(n)]


def _ratio_samples(rng: random.Random, mean: float, spread: float, n: int) -> list[float]:
    return [min(1.0, max(0.005, rng.gauss(mean, spread))) for _ in range(n)]


def generate_revision_metrics(
    rev: dict,
    state: RevisionState,
    rng: random.Random,
    active_chaos: dict | None = None,
    *,
    now: float | None = None,
) -> list[dict]:
    """Metrics for one ``cloud_run_revision`` for one 60 s interval.

    ``now`` pins the histogram times for tests; left out, every point takes the
    scrape's timestamp.
    """
    load = business_load()
    rps_peak, lat_median, cpu_alloc, mem_gib, cold_median = rev["profile"]
    target = rev["service_index"] == 0

    err5 = float(impact_value(active_chaos, "error_5xx_rate", default=0.0) or 0.0) if target else 0.0
    lat_mult = float(impact_value(active_chaos, "latency_multiplier", default=1.0) or 1.0) if target else 1.0
    spike = float(impact_value(active_chaos, "instance_count_spike", default=1.0) or 1.0) if target else 1.0
    err5 = min(1.0, max(0.0, err5))
    lat_mult = max(1.0, lat_mult)
    spike = max(1.0, spike)

    out: list[dict] = []
    rps = rps_peak * rev["share"] * (0.3 + 0.7 * load) * rng.uniform(0.9, 1.1)
    total = max(1, int(rps * INTERVAL_S))

    # request_count by response code
    healthy_ok = sum(share for _, cls, share in RESPONSE_MIX if cls != "5xx")
    by_class: dict[str, int] = {}
    for code, cls, share in RESPONSE_MIX:
        if err5 > 0:
            frac = err5 * (0.6 if code == "500" else 0.4) if cls == "5xx" else share / healthy_ok * (1.0 - err5)
        else:
            frac = share
        exact = total * frac
        count = int(exact) + (1 if rng.random() < exact - int(exact) else 0)
        if count <= 0 and cls != "2xx":
            continue
        count = max(1, count)
        by_class[cls] = by_class.get(cls, 0) + count
        out.append(build_gauge(_P + "request_count", "1", count,
                               {"response_code": code, "response_code_class": cls}))

    # request_latencies per response class
    for cls, count in sorted(by_class.items()):
        median = lat_median * lat_mult
        if cls == "5xx":
            median *= 3.0 if err5 > 0 else 1.5
        elif cls in ("3xx", "4xx"):
            median *= 0.4
        samples = _lognormal(rng, median, 0.55, min(count, MAX_SAMPLES))
        code = {"2xx": "200", "3xx": "304", "4xx": "404", "5xx": "500"}[cls]
        out.append(build_exponential_histogram(
            _P + "request_latencies", "ms", samples, count,
            {"response_code": code, "response_code_class": cls}, now,
        ))

    # Instances
    per_instance_rps = 18.0 if lat_median < 100 else 6.0
    active = max(1, math.ceil(rps / per_instance_rps * min(lat_mult, 4.0)))
    active = max(1, int(round(active * spike)))
    idle = rng.randint(0, 2)
    out.append(build_gauge(_P + "container/instance_count", "1", active, {"state": "active"}))
    out.append(build_gauge(_P + "container/instance_count", "1", idle, {"state": "idle"}))

    instance_seconds = (active + idle * 0.2) * INTERVAL_S * rng.uniform(0.9, 1.0)
    out.append(build_gauge(_P + "container/billable_instance_time", "s",
                           float(round(instance_seconds, 2)), {}))
    out.append(build_gauge(_P + "container/cpu/allocation_time", "s{CPU}",
                           float(round(instance_seconds * cpu_alloc, 2)), {}))
    out.append(build_gauge(_P + "container/memory/allocation_time", "GiBy.s",
                           float(round(instance_seconds * mem_gib, 2)), {}))

    # Utilisation distributions (one sample per instance per ~10 s)
    n_util = max(6, active * 6)
    cpu_mean = min(0.95, state.cpu * (0.6 + 0.6 * load) / max(1.0, spike ** 0.5)
                   * (1.0 + 0.15 * (lat_mult - 1.0)))
    out.append(build_exponential_histogram(
        _P + "container/cpu/utilizations", "10^2.%",
        _ratio_samples(rng, cpu_mean, 0.06, min(n_util, MAX_SAMPLES)), n_util, {}, now,
    ))
    mem_mean = min(0.95, state.mem * (1.0 + 0.04 * (lat_mult - 1.0)))
    out.append(build_exponential_histogram(
        _P + "container/memory/utilizations", "10^2.%",
        _ratio_samples(rng, mem_mean, 0.04, min(n_util, MAX_SAMPLES)), n_util, {}, now,
    ))

    # Max concurrent requests per active instance
    concurrency = max(1.0, rps * (lat_median * lat_mult / 1000.0) / active)
    out.append(build_exponential_histogram(
        _P + "container/max_request_concurrencies", "1",
        [max(1.0, round(v)) for v in _lognormal(rng, concurrency * 1.6, 0.35, min(n_util, MAX_SAMPLES))],
        n_util, {"state": "active"}, now,
    ))

    # Cold starts
    starts = max(1, int(round(rng.randint(1, 2) * spike)))
    out.append(build_exponential_histogram(
        _P + "container/startup_latencies", "ms",
        _lognormal(rng, cold_median, 0.35, min(starts, MAX_SAMPLES)), starts, {}, now,
    ))

    # Network
    rx_internet = int(total * rng.uniform(1500, 3000))
    tx_internet = int(total * rng.uniform(4000, 12000))
    rx_private = int(total * rng.uniform(800, 2000))
    tx_private = int(total * rng.uniform(300, 900))
    for kind, rx, tx in (("internet", rx_internet, tx_internet),
                         ("private", rx_private, tx_private)):
        out.append(build_gauge(_P + "container/network/received_bytes_count", "By", rx, {"kind": kind}))
        out.append(build_gauge(_P + "container/network/sent_bytes_count", "By", tx, {"kind": kind}))
    return out


from generators.gcp_cloudrun_otel import SPEC as _SPEC  # noqa: E402


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Emit Cloud Run metrics until ``stop_event`` is set."""
    rng = random.Random()
    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {}) or {}
    else:
        ns = NAMESPACE
        cfg = {}
    channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    ident = ScenarioIdentity.from_scenario(scenario_data, ns)
    service_name = ident.service("gcp-cloudrun")
    cloud = {**_default_cloud(), **(cfg.get("cloud") or {})}
    location = cloud.get("region", "us-central1")
    project_id = ident.gcp_project(cfg.get("project_id"))
    roles = cfg.get("run_services") or list(SERVICE_PROFILES)
    revisions = build_revisions(ident, [str(r) for r in roles], project_id, location)
    states = MultiEntityState([r["revision"] for r in revisions], RevisionState, rng)
    scope = {"name": GCM_SCOPE, "version": SCOPE_VERSION}

    resources = {}
    for i, rev in enumerate(revisions, start=1):
        resources[rev["revision"]] = build_signal_resource(
            identity=ident,
            signal_type="metrics",
            dataset=otel_dataset("gcp.cloudrun"),
            service_name=service_name,
            service_instance_id=ident.instance(service_name, i),
            host_name=rev["service"],
            cloud=cloud,
            extra_attrs={
                "gcp.resource_type": "cloud_run_revision",
                "project_id": rev["project_id"],
                "service_name": rev["service"],
                "revision_name": rev["revision"],
                "configuration_name": rev["service"],
                "location": rev["location"],
                "cloud.account.id": rev["project_id"],
            },
        )

    logger.info("GCP Cloud Run generator started (namespace=%s, revisions=%d)",
                ns, len(revisions))

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=channel_registry,
            generator_id="gcp_cloudrun_otel",
            chaos_mode=chaos_mode,
            impact_keys=_SPEC.impact_keys,
        )
        states.tick_all(rng)
        for rev in revisions:
            metrics = generate_revision_metrics(rev, states[rev["revision"]], rng,
                                                active_chaos)
            client.send_metrics_scoped(resources[rev["revision"]], scope, metrics)

    run_tick_loop(stop_event, _emit_tick, interval=INTERVAL_S)
    logger.info("GCP Cloud Run generator stopped.")


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "120"))
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    client = OTLPClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
