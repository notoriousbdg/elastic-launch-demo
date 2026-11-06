#!/usr/bin/env python3
"""NGINX Ingress Controller Log Generator — sends ingress access/error logs via OTLP.

Emits data matching the nginx_ingress_controller_otel content package dashboards.

NOTE: The package declares **no** dataset in manifest.yml, but the dashboards
filter on ``nginx_ingress_controller.access.otel`` and
``nginx_ingress_controller.error.otel`` (same pattern as zookeeper → generic.otel).

Stock dashboards query ``body.structured.*``.  Default OTel intake maps that
field as ``flattened``, which rejects Lens sum/terms/filters.  On startup this
generator overrides ``logs-otel@custom`` so ``body.structured`` is a dynamic
``object``, then resets sticky flattened data streams.  Dashboard fields are
also dual-written to attributes (passthrough) for Discover / ES|QL.

Access body.structured / attribute fields:
  http.request.method, http.response.status_code, http.response.body.size,
  upstream.name, upstream.response.size, url.original, url.path, user_agent.name,
  source.address (attributes), geo.location (attributes geo_point)

Error body.structured / attribute fields:
  log.level, message, source.file.name

Usage (standalone):
    python3 -m generators.nginx_ingress_controller_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/nginx_ingress_controller_otel@0.4.1"

import logging
import os
import random
import signal
import threading

import httpx

from app.telemetry import OTLPClient
from app.config import NAMESPACE
from generator_engine.db_common import (
    build_signal_resource,
    build_structured_body_log,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

NGINX_INGRESS_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("nginx-ingress-generator")
BATCH_INTERVAL_MIN = 5
BATCH_INTERVAL_MAX = 15

_LOGS_OTEL_CUSTOM_TEMPLATE = "logs-otel@custom"
_ACCESS_DATA_STREAM = "logs-nginx_ingress_controller.access.otel-default"
_ERROR_DATA_STREAM = "logs-nginx_ingress_controller.error.otel-default"

# Explicit typed leaves so SUM/terms work immediately after remapping
# (dynamic mapping alone can leave first docs as wrong types).
_BODY_STRUCTURED_PROPERTIES = {
    "http.request.method": {"type": "keyword"},
    "http.request.size": {"type": "long"},
    "http.request.time": {"type": "double"},
    "http.response.status_code": {"type": "long"},
    "http.response.body.size": {"type": "long"},
    "http.version": {"type": "keyword"},
    "upstream.name": {"type": "keyword"},
    "upstream.address": {"type": "keyword"},
    "upstream.response.size": {"type": "long"},
    "upstream.response.time": {"type": "double"},
    "upstream.response.status_code": {"type": "long"},
    "url.original": {"type": "keyword"},
    "url.path": {"type": "keyword"},
    "url.domain": {"type": "keyword"},
    "user_agent.name": {"type": "keyword"},
    "user_agent.original": {"type": "keyword"},
    "user.name": {"type": "keyword"},
    "log.level": {"type": "keyword"},
    "message": {"type": "keyword"},
    "source.file.name": {"type": "keyword"},
    "source.line_number": {"type": "long"},
    "thread_id": {"type": "long"},
}

_HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
_URL_PATHS = [
    "/api/products", "/api/users", "/api/orders", "/api/payments",
    "/api/search", "/api/auth/login", "/api/v2/catalog", "/health",
    "/metrics", "/static/js/app.js",
]
_USER_AGENTS = [
    ("Chrome", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"),
    ("Mobile Safari", "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15"),
    ("python-requests", "python-requests/2.31.0"),
    ("curl", "curl/8.1.2"),
    ("Go-http-client", "Go-http-client/2.0"),
    ("Dalvik", "Dalvik/2.1.0 (Linux; U; Android 14; Pixel 8)"),
]
_DEFAULT_SERVICES = ["backend-svc", "api-svc", "auth-svc", "product-svc"]
_ERROR_MESSAGES = [
    ("E", "error", "upstream timed out (110: Connection timed out) while connecting to upstream"),
    ("E", "error", "upstream prematurely closed connection while reading response header from upstream"),
    ("E", "error", "connect() failed (111: Connection refused) while connecting to upstream"),
    ("E", "error", "no live upstreams while connecting to upstream"),
    ("W", "warn", "access forbidden by rule"),
    ("W", "warn", "upstream sent too big header while reading response header from upstream"),
    ("F", "fatal", "client intended to send too large body: 10485760 bytes"),
]
_SOURCE_FILES = [
    "nginx.go", "controller.go", "store.go", "backend_ssl.go", "status.go",
]

# GeoIP is optional in the package (Contrib geoip processor).  Emit locations
# directly so the map panel (geo.location / attributes.geo.location) has points
# without GeoIP.
_GEO_LOCATIONS = (
    {"city_name": "New York", "region_name": "New York", "country_iso_code": "US",
     "country_name": "United States", "continent_code": "NA", "lat": 40.7128, "lon": -74.0060},
    {"city_name": "London", "region_name": "England", "country_iso_code": "GB",
     "country_name": "United Kingdom", "continent_code": "EU", "lat": 51.5074, "lon": -0.1278},
    {"city_name": "Frankfurt", "region_name": "Hesse", "country_iso_code": "DE",
     "country_name": "Germany", "continent_code": "EU", "lat": 50.1109, "lon": 8.6821},
    {"city_name": "Tokyo", "region_name": "Tokyo", "country_iso_code": "JP",
     "country_name": "Japan", "continent_code": "AS", "lat": 35.6762, "lon": 139.6503},
    {"city_name": "Sydney", "region_name": "New South Wales", "country_iso_code": "AU",
     "country_name": "Australia", "continent_code": "OC", "lat": -33.8688, "lon": 151.2093},
    {"city_name": "São Paulo", "region_name": "São Paulo", "country_iso_code": "BR",
     "country_name": "Brazil", "continent_code": "SA", "lat": -23.5505, "lon": -46.6333},
    {"city_name": "Mumbai", "region_name": "Maharashtra", "country_iso_code": "IN",
     "country_name": "India", "continent_code": "AS", "lat": 19.0760, "lon": 72.8777},
    {"city_name": "Singapore", "region_name": "Singapore", "country_iso_code": "SG",
     "country_name": "Singapore", "continent_code": "AS", "lat": 1.3521, "lon": 103.8198},
)


def _default_cloud() -> dict:
    return {"provider": "aws", "platform": "aws_ec2",
            "region": "us-east-1", "availability_zone": "us-east-1a"}


def _es_headers(api_key: str) -> dict[str, str]:
    return {
        "Authorization": f"ApiKey {api_key}",
        "Content-Type": "application/json",
    }


def _body_structured_mapping_types(elastic_url: str, api_key: str) -> set[str]:
    """Return ES types seen for body.structured on nginx ingress log streams."""
    types: set[str] = set()
    for stream in (_ACCESS_DATA_STREAM, _ERROR_DATA_STREAM):
        url = (
            f"{elastic_url.rstrip('/')}/{stream}"
            f"/_field_caps?fields=body.structured"
        )
        try:
            resp = httpx.get(url, headers=_es_headers(api_key), timeout=15)
        except Exception as exc:
            logger.warning("field_caps for %s failed: %s", stream, exc)
            continue
        if resp.status_code == 404:
            continue
        if resp.status_code >= 300:
            logger.warning(
                "field_caps for %s failed (%s): %s",
                stream, resp.status_code, resp.text[:200],
            )
            continue
        caps = resp.json().get("fields", {}).get("body.structured") or {}
        types.update(caps.keys())
    return types


def _geo_location_mapping_types(elastic_url: str, api_key: str) -> set[str]:
    """Return ES types for geo.location on the access log stream.

    Earlier emits stringified ``{'lat':…,'lon':…}`` dicts, which sticky-mapped
    ``attributes.geo.location`` as keyword and broke the map panel.
    """
    url = (
        f"{elastic_url.rstrip('/')}/{_ACCESS_DATA_STREAM}"
        f"/_field_caps?fields=attributes.geo.location,geo.location"
    )
    try:
        resp = httpx.get(url, headers=_es_headers(api_key), timeout=15)
    except Exception as exc:
        logger.warning("field_caps for geo.location failed: %s", exc)
        return set()
    if resp.status_code == 404:
        return set()
    if resp.status_code >= 300:
        logger.warning(
            "field_caps for geo.location failed (%s): %s",
            resp.status_code, resp.text[:200],
        )
        return set()
    wanted = {"attributes.geo.location", "geo.location"}
    types: set[str] = set()
    for name, field_info in resp.json().get("fields", {}).items():
        if name in wanted:
            types.update(field_info.keys())
    return types


def _merge_body_structured_object_mapping(elastic_url: str, api_key: str) -> None:
    """Override logs-otel@custom so body.structured is an aggregatable object.

    ``logs-otel@mappings`` sets ``body.structured`` to ``flattened``.  Custom
    composes after that template, so this override wins for new backing indices.
    """
    url = f"{elastic_url.rstrip('/')}/_component_template/{_LOGS_OTEL_CUSTOM_TEMPLATE}"
    headers = _es_headers(api_key)
    existing_template: dict = {}
    try:
        existing = httpx.get(url, headers=headers, timeout=30)
        if existing.status_code == 200:
            existing_template = (
                existing.json()
                .get("component_templates", [{}])[0]
                .get("component_template", {})
                .get("template", {})
            )
    except Exception as exc:
        logger.warning("Could not read %s: %s", _LOGS_OTEL_CUSTOM_TEMPLATE, exc)

    mappings = dict(existing_template.get("mappings") or {})
    props = dict(mappings.get("properties") or {})
    body = dict(props.get("body") or {})
    body_props = dict(body.get("properties") or {})
    # Preserve body.text if already present from a prior custom merge.
    if "text" not in body_props:
        body_props["text"] = {"type": "match_only_text"}
    body_props["structured"] = {
        "type": "object",
        "dynamic": True,
        "properties": dict(_BODY_STRUCTURED_PROPERTIES),
    }
    body["type"] = "object"
    body["properties"] = body_props
    props["body"] = body
    template_body: dict = {
        "template": {
            "mappings": {**mappings, "properties": props},
        }
    }
    if existing_template.get("settings"):
        template_body["template"]["settings"] = existing_template["settings"]

    resp = httpx.put(url, headers=headers, json=template_body, timeout=30)
    if resp.status_code >= 300:
        raise RuntimeError(
            f"put {_LOGS_OTEL_CUSTOM_TEMPLATE} failed ({resp.status_code}): "
            f"{resp.text[:300]}"
        )
    logger.info(
        "Ensured body.structured object mapping on %s",
        _LOGS_OTEL_CUSTOM_TEMPLATE,
    )


def _delete_data_stream(elastic_url: str, api_key: str, name: str) -> None:
    resp = httpx.delete(
        f"{elastic_url.rstrip('/')}/_data_stream/{name}",
        headers=_es_headers(api_key),
        timeout=30,
    )
    if resp.status_code < 300:
        logger.info("Deleted data stream %s for remapping", name)
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist", name)
    else:
        logger.warning(
            "Could not delete %s (%s): %s",
            name, resp.status_code, resp.text[:200],
        )


def ensure_dashboard_mappings(
    elastic_url: str,
    api_key: str,
    *,
    reset_streams: bool = True,
) -> None:
    """Map body.structured as object and drop sticky flattened streams.

    Stock nginx_ingress_controller_otel dashboards Lens-sum/filter
    ``body.structured.*``.  Flattened rejects those aggs; object + typed
    leaves make the stock panels work without editing package dashboards.
    """
    _merge_body_structured_object_mapping(elastic_url, api_key)
    if not reset_streams:
        return

    types = _body_structured_mapping_types(elastic_url, api_key)
    # No streams yet → nothing to migrate. Pure object → already good.
    if types and types != {"object"}:
        logger.info(
            "body.structured mapped as %s; resetting nginx ingress log streams",
            sorted(types),
        )
        _delete_data_stream(elastic_url, api_key, _ACCESS_DATA_STREAM)
        _delete_data_stream(elastic_url, api_key, _ERROR_DATA_STREAM)
        return

    # Sticky keyword from stringified lat/lon dicts — access stream only.
    geo_types = _geo_location_mapping_types(elastic_url, api_key)
    if not geo_types or geo_types == {"geo_point"}:
        return
    logger.info(
        "geo.location mapped as %s; resetting nginx ingress access stream",
        sorted(geo_types),
    )
    _delete_data_stream(elastic_url, api_key, _ACCESS_DATA_STREAM)


def ensure_dashboard_mappings_from_env() -> None:
    """Run mapping fix using ELASTIC_URL / ELASTIC_API_KEY from the environment."""
    try:
        from app.telemetry import ESBulkClient
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for mapping fix: %s", exc)
        return
    try:
        client = ESBulkClient()
    except Exception as exc:
        logger.warning("ESBulkClient init failed for mapping fix: %s", exc)
        return
    if not client.configured:
        return
    try:
        ensure_dashboard_mappings(
            client.elastic_url, client.api_key, reset_streams=True,
        )
    except Exception:
        logger.exception("nginx_ingress body.structured mapping fix failed")


def _status_and_level(rng: random.Random, error_rate: float) -> tuple[int, str]:
    if rng.random() < error_rate:
        code = rng.choice([400, 403, 404, 429, 500, 502, 503])
        level = "error" if code >= 500 else "warn"
    else:
        code = rng.choice([200, 200, 200, 201, 204, 301, 304])
        level = "info"
    return code, level


def _geo_attributes(rng: random.Random) -> dict:
    loc = rng.choice(_GEO_LOCATIONS)
    lat, lon = loc["lat"], loc["lon"]
    return {
        "geo.city_name": loc["city_name"],
        "geo.region_name": loc["region_name"],
        "geo.country_iso_code": loc["country_iso_code"],
        "geo.country_name": loc["country_name"],
        "geo.continent_code": loc["continent_code"],
        # Emit "lat,lon" string — dicts are stringified by _format_attributes and
        # sticky-map as keyword. otel@mappings + ECS map *.geo.location geo_point.
        "geo.location": f"{lat},{lon}",
    }


def _access_record(
    rng: random.Random,
    ns: str,
    upstream_services: list[str],
    error_rate: float,
    jitter_ns: int,
) -> dict:
    method = rng.choice(_HTTP_METHODS)
    path = rng.choice(_URL_PATHS)
    status_code, _ = _status_and_level(rng, error_rate)
    upstream = rng.choice(upstream_services)
    ua_name, ua_original = rng.choice(_USER_AGENTS)
    body_size = rng.randint(512, 65536) if status_code < 400 else rng.randint(0, 512)
    upstream_size = max(0, int(body_size * rng.uniform(0.7, 1.0)))
    latency_s = round(rng.uniform(0.002, 0.2) * (3.0 if status_code >= 500 else 1.0), 6)
    remote_addr = f"203.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"

    # Stock dashboards read body.structured.*; attributes dual-write for
    # passthrough / Discover.
    fields = {
        "http.request.method": method,
        "http.request.size": rng.randint(256, 4096),
        "http.request.time": latency_s,
        "http.response.status_code": status_code,
        "http.response.body.size": body_size,
        "http.version": "1.1",
        "upstream.name": upstream,
        "upstream.address": f"10.{rng.randint(0, 9)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}",
        "upstream.response.size": upstream_size,
        "upstream.response.time": round(latency_s * 0.9, 6),
        "upstream.response.status_code": status_code,
        "url.original": path,
        "url.path": path,
        "url.domain": ScenarioIdentity(ns).domain(),
        "user_agent.name": ua_name,
        "user_agent.original": ua_original,
        "user.name": "-",
    }
    attributes = {
        **fields,
        "event.name": "nginx_ingress_controller.access",
        "source.address": remote_addr,
        "log.file.name": ScenarioIdentity(ns).label("nginx-ingress-controller-0_controller.log"),
        **_geo_attributes(rng),
    }
    severity = "ERROR" if status_code >= 500 else ("WARN" if status_code >= 400 else "INFO")
    return build_structured_body_log(
        fields=fields,
        event_name="nginx_ingress_controller.access",
        severity=severity,
        jitter_ns=jitter_ns,
        attributes=attributes,
    )


def _error_record(rng: random.Random, ns: str, jitter_ns: int) -> dict:
    # Collector grok extracts klog level letters (E/W/F) into log.level.
    level_letter, severity_key, message = rng.choice(_ERROR_MESSAGES)
    source_file = rng.choice(_SOURCE_FILES)
    fields = {
        "log.level": level_letter,
        "message": message,
        "source.file.name": source_file,
        "source.line_number": rng.randint(40, 900),
        "thread_id": rng.randint(1, 64),
    }
    attributes = {
        **fields,
        "event.name": "nginx_ingress_controller.error",
        "log.file.name": ScenarioIdentity(ns).label("nginx-ingress-controller-0_controller.log"),
    }
    severity = {"error": "ERROR", "warn": "WARN", "fatal": "FATAL"}.get(severity_key, "ERROR")
    return build_structured_body_log(
        fields=fields,
        event_name="nginx_ingress_controller.error",
        severity=severity,
        jitter_ns=jitter_ns,
        attributes=attributes,
    )



from generators.nginx_ingress_controller_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("nginx-ingress-controller")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("ingress")
    cloud = cfg.get("cloud") or _default_cloud()
    upstream_services: list[str] = cfg.get("upstream_services") or _DEFAULT_SERVICES
    records_per_cycle: int = cfg.get("records_per_cycle", 20)


    common_resource_kwargs = dict(
        ns=ns, signal_type="logs",
        service_name=service_name, service_version="1.9.6",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name, cloud=cloud,
        extra_attrs={"k8s.pod.name": ScenarioIdentity(ns).label("nginx-ingress-controller-0")},
    )
    # Dashboards filter data_stream.dataset == nginx_ingress_controller.{access,error}.otel
    # even though manifest.yml declares no discovery.datasets (zookeeper-style).
    access_resource = build_signal_resource(
        dataset=otel_dataset("nginx_ingress_controller.access"), **common_resource_kwargs,
    )
    error_resource = build_signal_resource(
        dataset=otel_dataset("nginx_ingress_controller.error"), **common_resource_kwargs,
    )
    scope = {"name": NGINX_INGRESS_SCOPE, "version": SCOPE_VERSION}
    ensure_dashboard_mappings_from_env()
    logger.info("nginx-ingress generator started (namespace=%s)", ns)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="nginx_ingress_controller_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        error_rate = float((active_chaos or {}).get("error_rate", 0.0))
        n = int((active_chaos or {}).get("records_per_cycle", records_per_cycle))
        load = business_load()
        n = max(1, int(n * load))

        access_records = []
        for _ in range(n):
            jitter = rng.randint(0, int(BATCH_INTERVAL_MAX * 1e9))
            access_records.append(_access_record(
                rng, ns, upstream_services, error_rate, jitter,
            ))
        client.send_logs_scoped(access_resource, scope, access_records)

        # Baseline error logs; elevate with chaos error_rate.
        error_n = max(1, int(n * max(0.05, error_rate)))
        error_records = [
            _error_record(rng, ns, rng.randint(0, int(BATCH_INTERVAL_MAX * 1e9)))
            for _ in range(error_n)
        ]
        client.send_logs_scoped(error_resource, scope, error_records)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("nginx-ingress generator stopped.")


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
