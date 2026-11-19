"""Heal Log Rate Analysis grouping failures on ``logs-*``.

system_otel Host Details → Logs embeds AIOps Log Rate Analysis. Grouping runs
Elasticsearch ``frequent_item_sets`` over significant keyword fields. Two
generator/data-shape issues break that step with:

    Failed to transform field/value pairs into groups.

**A. Claude Code ``attributes.error`` string (keyword vs object)**
Claude Code MCP events write ``attributes.error`` as a string. OTel
passthrough promotes that to top-level keyword ``error``, conflicting with
Bedrock's object ``error``. Nesting into an object is worse (OTel maps object
``attributes.*`` as ``flattened``). Rename string → ``attributes.error_message``.

**B. Sparse ECS fields across package streams (scripts not supported)**
When a significant field (e.g. ``error.type`` from Bedrock) is **unmapped** on
other ``logs-*`` streams (e.g. ``azure_openai.logs``), ``frequent_item_sets``
fails with ``illegal_argument_exception: scripts are not supported``. Stub
``error.type``, ``event.type``, and ``source.geo.region_name`` as keyword on
``logs@custom`` (composed into Fleet log packages) and recreate sticky streams
that still lack those mappings.

Do **not** redefine ``attributes`` under ``logs-otel@custom`` mappings — that
replaces the passthrough subtree and breaks package aliases such as
``http.response.body.bytes``.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_LOGS_OTEL_CUSTOM = "logs-otel@custom"
_LOGS_CUSTOM = "logs@custom"
_PIPELINE_NAME = "logs-otel-error-object-fix"
_GENERIC_STREAM_PREFIX = "logs-generic.otel-"

# Fields AIOps often groups on that some package streams omit → FIS "scripts" error.
_ECS_STUB_FIELDS = (
    "error.type",
    "event.type",
    "source.geo.region_name",
)

# Known log data streams that historically lack the stubs above.
_STREAMS_NEEDING_ECS_STUBS = (
    "logs-azure_openai.logs-default",
    "logs-azure_openai.logs-xborder",
)


def _es_headers(api_key: str) -> dict[str, str]:
    return {
        "Authorization": f"ApiKey {api_key}",
        "Content-Type": "application/json",
    }


def _put_pipeline(elastic_url: str, api_key: str) -> None:
    """Install ingest pipeline that renames string attributes.error away from ``error``."""
    body = {
        "description": (
            "Rename string attributes.error → attributes.error_message so "
            "passthrough does not promote keyword/flattened 'error' alongside "
            "Bedrock's object error (Log Rate Analysis grouping)."
        ),
        "processors": [
            {
                "script": {
                    "lang": "painless",
                    "ignore_failure": True,
                    "source": """
if (ctx.attributes == null) { return; }
def err = ctx.attributes.error;
if (!(err instanceof String)) { return; }
if (ctx.attributes.error_message == null) {
  ctx.attributes.error_message = err;
}
ctx.attributes.remove('error');
""",
                }
            }
        ],
    }
    url = f"{elastic_url.rstrip('/')}/_ingest/pipeline/{_PIPELINE_NAME}"
    resp = httpx.put(url, headers=_es_headers(api_key), json=body, timeout=30)
    if resp.status_code >= 300:
        raise RuntimeError(
            f"put pipeline {_PIPELINE_NAME} failed ({resp.status_code}): "
            f"{resp.text[:300]}"
        )
    logger.info("Ensured ingest pipeline %s", _PIPELINE_NAME)


def _merge_default_pipeline_setting(elastic_url: str, api_key: str) -> None:
    """Attach default_pipeline on logs-otel@custom; preserve existing mappings."""
    url = f"{elastic_url.rstrip('/')}/_component_template/{_LOGS_OTEL_CUSTOM}"
    headers = _es_headers(api_key)
    existing_template: dict[str, Any] = {}
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
        logger.warning("Could not read %s: %s", _LOGS_OTEL_CUSTOM, exc)

    settings = dict(existing_template.get("settings") or {})
    index_settings = dict(settings.get("index") or {})
    index_settings["default_pipeline"] = _PIPELINE_NAME
    settings["index"] = index_settings

    template_body: dict[str, Any] = {"template": {"settings": settings}}
    if existing_template.get("mappings"):
        template_body["template"]["mappings"] = existing_template["mappings"]

    resp = httpx.put(url, headers=headers, json=template_body, timeout=30)
    if resp.status_code >= 300:
        raise RuntimeError(
            f"put {_LOGS_OTEL_CUSTOM} failed ({resp.status_code}): "
            f"{resp.text[:300]}"
        )
    logger.info(
        "Ensured default_pipeline=%s on %s",
        _PIPELINE_NAME, _LOGS_OTEL_CUSTOM,
    )


def _merge_logs_custom_ecs_stubs(elastic_url: str, api_key: str) -> None:
    """Ensure logs@custom maps sparse ECS fields used by Log Rate Analysis grouping."""
    url = f"{elastic_url.rstrip('/')}/_component_template/{_LOGS_CUSTOM}"
    headers = _es_headers(api_key)
    existing_template: dict[str, Any] = {}
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
        logger.warning("Could not read %s: %s", _LOGS_CUSTOM, exc)

    mappings = dict(existing_template.get("mappings") or {})
    props = dict(mappings.get("properties") or {})

    error = dict(props.get("error") or {})
    error_props = dict(error.get("properties") or {})
    error_props.setdefault("type", {"type": "keyword", "ignore_above": 1024})
    error_props.setdefault("message", {"type": "match_only_text"})
    error["properties"] = error_props
    props["error"] = error

    event = dict(props.get("event") or {})
    event_props = dict(event.get("properties") or {})
    event_props.setdefault("type", {"type": "keyword", "ignore_above": 1024})
    event_props.setdefault("category", {"type": "keyword", "ignore_above": 1024})
    event["properties"] = event_props
    props["event"] = event

    source = dict(props.get("source") or {})
    source_props = dict(source.get("properties") or {})
    geo = dict(source_props.get("geo") or {})
    geo_props = dict(geo.get("properties") or {})
    geo_props.setdefault("region_name", {"type": "keyword", "ignore_above": 1024})
    geo_props.setdefault("city_name", {"type": "keyword", "ignore_above": 1024})
    geo["properties"] = geo_props
    source_props["geo"] = geo
    source["properties"] = source_props
    props["source"] = source

    template_body: dict[str, Any] = {
        "template": {
            "mappings": {**mappings, "properties": props},
        }
    }
    if existing_template.get("settings"):
        template_body["template"]["settings"] = existing_template["settings"]

    resp = httpx.put(url, headers=headers, json=template_body, timeout=30)
    if resp.status_code >= 300:
        raise RuntimeError(
            f"put {_LOGS_CUSTOM} failed ({resp.status_code}): "
            f"{resp.text[:300]}"
        )
    logger.info("Ensured ECS stub mappings on %s", _LOGS_CUSTOM)


def _error_field_types(elastic_url: str, api_key: str) -> set[str]:
    """Return ES types reported for top-level ``error`` across logs-*."""
    resp = httpx.post(
        f"{elastic_url.rstrip('/')}/logs-*/_field_caps",
        headers=_es_headers(api_key),
        json={"fields": ["error"]},
        timeout=60,
    )
    if resp.status_code >= 300:
        logger.warning(
            "field_caps error failed (%s): %s",
            resp.status_code, resp.text[:200],
        )
        return set()
    caps = resp.json().get("fields", {}).get("error") or {}
    return set(caps.keys())


def _field_mapped_on_stream(
    elastic_url: str, api_key: str, stream: str, field: str,
) -> bool:
    """True if ``field`` has a concrete mapping on the data stream."""
    resp = httpx.get(
        f"{elastic_url.rstrip('/')}/{stream}/_mapping/field/{field}",
        headers=_es_headers(api_key),
        timeout=30,
    )
    if resp.status_code == 404:
        return False
    if resp.status_code >= 300:
        return False
    body = resp.json()
    if not isinstance(body, dict):
        return False
    for _idx, meta in body.items():
        if not isinstance(meta, dict):
            continue
        mapping = (meta.get("mappings") or {}).get(field) or {}
        if isinstance(mapping, dict) and mapping.get("mapping"):
            return True
    return False


def _list_generic_otel_streams(elastic_url: str, api_key: str) -> list[str]:
    resp = httpx.get(
        f"{elastic_url.rstrip('/')}/_data_stream/{_GENERIC_STREAM_PREFIX}*",
        headers=_es_headers(api_key),
        timeout=30,
    )
    if resp.status_code == 404:
        return []
    if resp.status_code >= 300:
        logger.warning(
            "list data streams failed (%s): %s",
            resp.status_code, resp.text[:200],
        )
        return []
    return [ds["name"] for ds in resp.json().get("data_streams", [])]


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


def _reset_streams_missing_ecs_stubs(elastic_url: str, api_key: str) -> None:
    """Recreate package streams that still lack AIOps grouping stub fields."""
    for stream in _STREAMS_NEEDING_ECS_STUBS:
        missing = [
            f for f in _ECS_STUB_FIELDS
            if not _field_mapped_on_stream(elastic_url, api_key, stream, f)
        ]
        if not missing:
            continue
        logger.info(
            "%s missing stub fields %s; resetting stream",
            stream, missing,
        )
        _delete_data_stream(elastic_url, api_key, stream)


def ensure_error_object_mapping(
    elastic_url: str,
    api_key: str,
    *,
    reset_streams: bool = True,
) -> None:
    """Heal both Claude Code error collision and sparse ECS FIS failures."""
    _put_pipeline(elastic_url, api_key)
    _merge_default_pipeline_setting(elastic_url, api_key)
    _merge_logs_custom_ecs_stubs(elastic_url, api_key)
    if not reset_streams:
        return

    types = _error_field_types(elastic_url, api_key)
    if len(types) > 1 or ("object" not in types and types & {"keyword", "flattened"}):
        logger.info(
            "error field types %s; resetting logs-generic.otel streams",
            sorted(types),
        )
        for name in _list_generic_otel_streams(elastic_url, api_key):
            _delete_data_stream(elastic_url, api_key, name)

    _reset_streams_missing_ecs_stubs(elastic_url, api_key)


def ensure_error_object_mapping_from_env() -> None:
    """Run heal using ELASTIC_URL / ELASTIC_API_KEY via ESBulkClient."""
    try:
        from app.telemetry import ESBulkClient
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for error-field heal: %s", exc)
        return
    try:
        client = ESBulkClient()
    except Exception as exc:
        logger.warning("ESBulkClient init failed for error-field heal: %s", exc)
        return
    if not client.configured:
        return
    try:
        ensure_error_object_mapping(
            client.elastic_url, client.api_key, reset_streams=True,
        )
    except Exception:
        logger.exception("logs-otel error-field heal failed")
