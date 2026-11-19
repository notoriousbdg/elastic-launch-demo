#!/usr/bin/env python3
"""GCP Cloud Storage generator: Cloud Monitoring metrics via OTLP.

Emits what the OpenTelemetry ``googlecloudmonitoring`` receiver produces for
Cloud Storage buckets, shaped for the ``gcp_cloudstorage_otel`` content
package (``metrics-gcp.cloudstorage.otel-default``): the Cloud Storage
Overview and Bucket Detail dashboards.

Resource (one OTLP resource per ``gcs_bucket``): ``gcp.resource_type``,
``project_id``, ``bucket_name``, ``location``.

Metrics, per 60 s alignment period, all gauges (DELTA counts hold the
per-interval value; dashboards read ``SUM_OVER_TIME`` / ``LAST_OVER_TIME``):
  - ``api/request_count`` and ``network/{sent,received}_bytes_count`` labelled
    ``method`` (ReadObject, WriteObject, ...) and ``response_code`` (gRPC
    canonical codes: OK, NOT_FOUND, PERMISSION_DENIED, UNAVAILABLE, ...).
  - ``storage/total_bytes``, ``storage/object_count``,
    ``storage/total_byte_seconds`` labelled ``storage_class``.
  - ``storage/v2/total_bytes``, ``storage/v2/total_count`` labelled
    ``storage_class`` and ``type`` (live-object, noncurrent-object,
    soft-deleted-object, multipart-upload).
  - ``authz/acl_operations_count`` and ``authz/acl_based_object_access_count``
    for the bucket that still uses fine-grained ACLs.

Cloud Monitoring samples the ``storage/*`` metrics once a day, and four panels
chart them by day (``BUCKET(@timestamp, 1 day)``), so at startup the generator
backfills one sample per day for the past ``BACKFILL_DAYS`` days. The stream is
a TSDS that rejects documents older than ``index.look_back_time`` (2h by
default), so the backfill first makes new OTel metric streams look back 7 days
and recreates its own stream if it was created with a shorter window.

Usage (standalone):
    python3 -m generators.gcp_cloudstorage_otel.generator
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/gcp_cloudstorage_otel@0.1.0"

import datetime
import logging
import os
import random
import signal
import threading

import httpx

from app.config import NAMESPACE
from app.telemetry import OTLPClient
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
from generator_engine.scrape_clock import scrape_clock
from generator_engine.tick import run_tick_loop

_P = "storage.googleapis.com/"

METRIC_NAMES = (
    "storage.googleapis.com/api/request_count",
    "storage.googleapis.com/network/sent_bytes_count",
    "storage.googleapis.com/network/received_bytes_count",
    "storage.googleapis.com/storage/total_bytes",
    "storage.googleapis.com/storage/object_count",
    "storage.googleapis.com/storage/total_byte_seconds",
    "storage.googleapis.com/storage/v2/total_bytes",
    "storage.googleapis.com/storage/v2/total_count",
    "storage.googleapis.com/authz/acl_operations_count",
    "storage.googleapis.com/authz/acl_based_object_access_count",
)

GCM_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/"
    "googlecloudmonitoringreceiver"
)
SCOPE_VERSION = "0.115.0"

INTERVAL_S = 60
TIB = 1024 ** 4

# Daily history for the storage/* panels: one sample per UTC day at noon, inside
# the 7-day TSDS look-back. Stored bytes were this much smaller per day back.
BACKFILL_DAYS = 6
LOOK_BACK = "7d"
DAILY_GROWTH = 0.0015
DATA_STREAM = "metrics-gcp.cloudstorage.otel-default"

CLIENT_FAULT_CODES = ("NOT_FOUND", "PERMISSION_DENIED", "UNAUTHENTICATED",
                      "INVALID_ARGUMENT", "FAILED_PRECONDITION", "CANCELLED")
SERVICE_FAULT_CODES = ("UNAVAILABLE", "INTERNAL", "DEADLINE_EXCEEDED",
                       "ABORTED", "RESOURCE_EXHAUSTED")
OBJECT_TYPES = (
    ("live-object", 0.915),
    ("noncurrent-object", 0.05),
    ("soft-deleted-object", 0.03),
    ("multipart-upload", 0.005),
)
# method -> (avg request bytes in, avg response bytes out)
METHOD_BYTES = {
    "ReadObject": (600, 1_800_000),
    "WriteObject": (2_400_000, 900),
    "ListObjects": (400, 24_000),
    "GetObjectMetadata": (350, 1_200),
    "DeleteObject": (300, 200),
}
# role -> (location kind, {storage_class: TiB}, avg object MiB, {method: req/min at peak}, legacy ACLs)
BUCKET_PROFILES = {
    "data-lake": ("region", {"STANDARD": 46.0, "NEARLINE": 118.0}, 64.0,
                  {"ReadObject": 1200, "WriteObject": 700, "ListObjects": 180,
                   "GetObjectMetadata": 420, "DeleteObject": 30}, False),
    "media-assets": ("multi-region", {"STANDARD": 18.5}, 2.5,
                     {"ReadObject": 2600, "WriteObject": 110, "ListObjects": 40,
                      "GetObjectMetadata": 650, "DeleteObject": 8}, False),
    "backups": ("region", {"COLDLINE": 210.0, "ARCHIVE": 640.0}, 900.0,
                {"ReadObject": 6, "WriteObject": 48, "ListObjects": 12,
                 "GetObjectMetadata": 20, "DeleteObject": 4}, True),
}
DEFAULT_PROFILE = ("region", {"STANDARD": 5.0}, 16.0,
                   {"ReadObject": 300, "WriteObject": 60, "ListObjects": 20,
                    "GetObjectMetadata": 80, "DeleteObject": 5}, False)

logger = logging.getLogger("gcp-cloudstorage-generator")


def _default_cloud() -> dict:
    return {
        "provider": "gcp",
        "platform": "gcp_cloud_storage",
        "region": "us-central1",
        "availability_zone": "us-central1-a",
    }


def build_buckets(ident: ScenarioIdentity, roles: list[str], project_id: str,
                  region: str) -> list[dict]:
    buckets = []
    continent = region.split("-")[0]
    multi_region = {"europe": "eu", "us": "us", "asia": "asia"}.get(continent, continent)
    for index, role in enumerate(roles):
        kind, classes, obj_mib, methods, legacy = BUCKET_PROFILES.get(role, DEFAULT_PROFILE)
        buckets.append({
            "index": index,
            "name": ident.qualify(role),
            "project_id": project_id,
            "location": multi_region if kind == "multi-region" else region,
            "classes": dict(classes),
            "object_mib": obj_mib,
            "methods": dict(methods),
            "legacy_acl": legacy,
        })
    return buckets


class BucketState(CounterState):
    """Stored bytes per storage class, growing slowly each interval."""

    def __init__(self, rng: random.Random):
        super().__init__(rng, initial=0)
        self.stored: dict[str, float] = {}

    def seed(self, classes: dict[str, float], rng: random.Random) -> None:
        if not self.stored:
            self.stored = {c: tib * TIB * rng.uniform(0.97, 1.03) for c, tib in classes.items()}

    def tick(self, rng: random.Random, drift: int = 0) -> None:
        for c in self.stored:
            self.stored[c] *= 1.0 + rng.uniform(0.0, 2e-6)


def _stochastic(rng: random.Random, value: float) -> int:
    whole = int(value)
    return whole + (1 if rng.random() < value - whole else 0)


def generate_bucket_metrics(
    bucket: dict,
    state: BucketState,
    rng: random.Random,
    active_chaos: dict | None = None,
) -> list[dict]:
    """Metrics for one ``gcs_bucket`` resource for one 60 s interval."""
    load = business_load()
    target = bucket["index"] == 0
    e4 = float(impact_value(active_chaos, "error_4xx_rate", default=0.0) or 0.0) if target else 0.0
    e5 = float(impact_value(active_chaos, "error_5xx_rate", default=0.0) or 0.0) if target else 0.0
    spike = float(impact_value(active_chaos, "request_spike", default=1.0) or 1.0) if target else 1.0
    e4 = min(1.0, max(0.0, e4))
    e5 = min(1.0 - e4, max(0.0, e5))
    spike = max(1.0, spike)

    out: list[dict] = []
    state.seed(bucket["classes"], rng)

    for method, peak in bucket["methods"].items():
        total = max(1, int(peak * (0.3 + 0.7 * load) * rng.uniform(0.85, 1.15) * spike))
        codes: dict[str, int] = {}
        # Healthy background: misses on reads/metadata, rare transient 5xx.
        if method in ("ReadObject", "GetObjectMetadata", "DeleteObject"):
            codes["NOT_FOUND"] = _stochastic(rng, total * 0.004)
        codes["PERMISSION_DENIED"] = _stochastic(rng, total * 0.0002)
        codes["UNAVAILABLE"] = _stochastic(rng, total * 0.0002)
        if e4 > 0:
            codes["PERMISSION_DENIED"] = int(round(total * e4))
        if e5 > 0:
            failed = int(round(total * e5))
            codes["UNAVAILABLE"] = int(failed * 0.6)
            codes["INTERNAL"] = int(failed * 0.25)
            codes["DEADLINE_EXCEEDED"] = failed - codes["UNAVAILABLE"] - codes["INTERNAL"]
        errors = sum(codes.values())
        codes["OK"] = max(0, total - errors)
        in_bytes, out_bytes = METHOD_BYTES.get(method, (500, 2000))
        for code, count in codes.items():
            if count <= 0:
                continue
            attrs = {"method": method, "response_code": code}
            out.append(build_gauge(_P + "api/request_count", "1", count, attrs))
            if code == "OK":
                sent = int(count * out_bytes * rng.uniform(0.8, 1.2))
                recv = int(count * in_bytes * rng.uniform(0.8, 1.2))
            else:
                sent = int(count * rng.uniform(150, 400))
                recv = int(count * rng.uniform(200, 600))
            out.append(build_gauge(_P + "network/sent_bytes_count", "By", sent, attrs))
            out.append(build_gauge(_P + "network/received_bytes_count", "By", recv, attrs))

    out += daily_storage_metrics(bucket, state, days_ago=0)

    if bucket["legacy_acl"]:
        out.append(build_gauge(_P + "authz/acl_operations_count", "1",
                               rng.randint(1, 6), {"acl_operation": "GET_OBJECT_ACL"}))
        out.append(build_gauge(_P + "authz/acl_based_object_access_count", "1",
                               rng.randint(2, 20), {}))
    return out


def daily_storage_metrics(bucket: dict, state: BucketState, days_ago: int) -> list[dict]:
    """The once-a-day ``storage/*`` samples for ``days_ago`` days before today.

    Same series and labels as the live emit, with stored bytes scaled back by
    ``DAILY_GROWTH`` per day so the history rises toward today's value.
    """
    shrink = max(0.5, 1.0 - DAILY_GROWTH * days_ago)
    obj_bytes = bucket["object_mib"] * 1024 * 1024
    out: list[dict] = []
    for storage_class, stored_now in state.stored.items():
        stored = stored_now * shrink
        cls = {"storage_class": storage_class}
        live = stored * OBJECT_TYPES[0][1]
        out.append(build_gauge(_P + "storage/total_bytes", "By", float(round(live, 1)), cls))
        out.append(build_gauge(_P + "storage/object_count", "1", int(live / obj_bytes), cls))
        out.append(build_gauge(_P + "storage/total_byte_seconds", "By.s",
                               float(round(live * 86400.0, 1)), cls))
        for obj_type, share in OBJECT_TYPES:
            attrs = {"storage_class": storage_class, "type": obj_type}
            part = stored * share
            divisor = obj_bytes / 8 if obj_type == "multipart-upload" else obj_bytes
            out.append(build_gauge(_P + "storage/v2/total_bytes", "By",
                                   float(round(part, 1)), attrs))
            out.append(build_gauge(_P + "storage/v2/total_count", "1",
                                   max(1, int(part / divisor)), attrs))
    return out


def backfill_days(now: datetime.datetime, days: int = BACKFILL_DAYS) -> list[tuple[int, int]]:
    """``(days_ago, timestamp_ns)`` at noon UTC for each of the past ``days`` days.

    Fixed noon timestamps make a repeat backfill (generator restart) a TSDS
    duplicate that Elasticsearch rejects instead of a second sample.
    """
    today = now.astimezone(datetime.timezone.utc).replace(hour=12, minute=0, second=0,
                                                          microsecond=0)
    return [(d, int((today - datetime.timedelta(days=d)).timestamp() * 1_000_000_000))
            for d in range(days, 0, -1)]


def _es_credentials(client) -> tuple[str, str]:
    elastic_url = getattr(client, "elastic_url", None) or ""
    api_key = getattr(client, "api_key", None) or ""
    if not elastic_url or not api_key:
        elastic_url = os.environ.get("ELASTIC_URL") or os.environ.get("ELASTICSEARCH_URL") or ""
        api_key = os.environ.get("ELASTIC_API_KEY") or os.environ.get("ELASTICSEARCH_API_KEY") or ""
    return elastic_url.rstrip("/"), api_key


def prepare_history_stream(http: httpx.Client, elastic_url: str,
                           oldest: datetime.datetime) -> bool:
    """Make ``DATA_STREAM`` accept documents back to ``oldest``; True when it will.

    Sets ``index.look_back_time`` on ``metrics@custom`` (composed into the OTel
    metrics template; the APM deployer step sets the same value) without
    dropping its other settings, then deletes the stream when its first backing
    index cannot reach back to ``oldest`` so the next write recreates it.
    """
    url = f"{elastic_url}/_component_template/metrics@custom"
    resp = http.get(url)
    template: dict = {}
    if resp.status_code == 200:
        found = resp.json().get("component_templates") or []
        template = (found[0]["component_template"].get("template") or {}) if found else {}
    index = template.setdefault("settings", {}).setdefault("index", {})
    if index.get("look_back_time") != LOOK_BACK:
        index["look_back_time"] = LOOK_BACK
        put = http.put(url, json={"template": template})
        if put.status_code >= 300:
            logger.warning("Could not set look_back_time on metrics@custom: %s %s",
                           put.status_code, put.text[:200])
            return False

    resp = http.get(f"{elastic_url}/_data_stream/{DATA_STREAM}")
    if resp.status_code == 404:
        return True
    if resp.status_code >= 300:
        return False
    first = resp.json()["data_streams"][0]["indices"][0]["index_name"]
    settings = http.get(f"{elastic_url}/{first}/_settings/index.time_series.start_time")
    start = (settings.json().get(first, {}).get("settings", {}).get("index", {})
             .get("time_series", {}).get("start_time")) if settings.status_code == 200 else None
    if start and datetime.datetime.fromisoformat(start.replace("Z", "+00:00")) <= oldest:
        return True
    logger.info("Recreating %s so it accepts %d days of daily storage history",
                DATA_STREAM, BACKFILL_DAYS)
    return http.delete(f"{elastic_url}/_data_stream/{DATA_STREAM}").status_code < 300


def backfill_daily_history(client, resources: dict, buckets: list[dict], states,
                           scope: dict, rng: random.Random) -> int:
    """Send the past days' ``storage/*`` samples; returns the number of days sent."""
    elastic_url, api_key = _es_credentials(client)
    if not elastic_url or not api_key:
        return 0
    days = backfill_days(datetime.datetime.now(datetime.timezone.utc))
    oldest = datetime.datetime.fromtimestamp(days[0][1] / 1e9, datetime.timezone.utc)
    try:
        with httpx.Client(timeout=30, headers={"Authorization": f"ApiKey {api_key}"}) as http:
            if not prepare_history_stream(http, elastic_url, oldest):
                return 0
    except Exception as exc:
        logger.warning("Daily storage backfill skipped: %s", exc)
        return 0
    for bucket in buckets:
        states[bucket["name"]].seed(bucket["classes"], rng)
    for days_ago, ts_ns in days:
        with scrape_clock(at_ns=ts_ns):
            for bucket in buckets:
                metrics = daily_storage_metrics(bucket, states[bucket["name"]], days_ago)
                client.send_metrics_scoped(resources[bucket["name"]], scope, metrics)
    logger.info("Backfilled %d days of daily Cloud Storage samples", len(days))
    return len(days)


from generators.gcp_cloudstorage_otel import SPEC as _SPEC  # noqa: E402


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Emit Cloud Storage metrics until ``stop_event`` is set."""
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
    service_name = ident.service("gcp-cloudstorage")
    cloud = {**_default_cloud(), **(cfg.get("cloud") or {})}
    region = cloud.get("region", "us-central1")
    project_id = ident.gcp_project(cfg.get("project_id"))
    roles = cfg.get("buckets") or list(BUCKET_PROFILES)
    buckets = build_buckets(ident, [str(r) for r in roles], project_id, region)
    states = MultiEntityState([b["name"] for b in buckets], BucketState, rng)
    scope = {"name": GCM_SCOPE, "version": SCOPE_VERSION}

    resources = {}
    for i, bucket in enumerate(buckets, start=1):
        resources[bucket["name"]] = build_signal_resource(
            identity=ident,
            signal_type="metrics",
            dataset=otel_dataset("gcp.cloudstorage"),
            service_name=service_name,
            service_instance_id=ident.instance(service_name, i),
            host_name=bucket["name"],
            cloud=cloud,
            extra_attrs={
                "gcp.resource_type": "gcs_bucket",
                "project_id": bucket["project_id"],
                "bucket_name": bucket["name"],
                "location": bucket["location"],
                "cloud.account.id": bucket["project_id"],
            },
        )

    logger.info("GCP Cloud Storage generator started (namespace=%s, buckets=%d)",
                ns, len(buckets))
    backfill_daily_history(client, resources, buckets, states, scope, rng)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=channel_registry,
            generator_id="gcp_cloudstorage_otel",
            chaos_mode=chaos_mode,
            impact_keys=_SPEC.impact_keys,
        )
        states.tick_all(rng)
        for bucket in buckets:
            metrics = generate_bucket_metrics(bucket, states[bucket["name"]], rng, active_chaos)
            client.send_metrics_scoped(resources[bucket["name"]], scope, metrics)

    run_tick_loop(stop_event, _emit_tick, interval=INTERVAL_S)
    logger.info("GCP Cloud Storage generator stopped.")


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
