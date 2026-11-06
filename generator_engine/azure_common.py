"""Shared helpers for Azure OpenAI / Azure Billing / Azure AI Foundry generators.

The ``azure_openai``, ``azure_billing``, and ``azure_ai_foundry`` packages install
under the ``metrics-*`` data view used by Azure dashboards, but they can disagree
on the ``azure.dimensions`` mapping:

- ``azure_openai`` / ``azure_ai_foundry`` → ``object`` with keyword subfields
- ``azure_billing`` → ``flattened``

When both data streams exist, field_caps reports a conflict and Lens shows
"The azure.dimensions.* field can not be used for filtering."

Billing Overview / OpenAI Billing date histograms also need
``azure.billing.usage_start`` / ``usage_end`` as ``date``. Dynamic mapping
otherwise stores ISO strings as ``keyword`` and Lens returns N/A.

This module installs ``@custom`` component templates that force a consistent
object+keyword mapping (and explicit ``azure.subscription_name`` / billing
dates) then rolls over the affected data streams so new backing indices pick
up the mapping.
"""

from __future__ import annotations

import logging

import httpx

from elastic_config.deployer_base import _es_headers

logger = logging.getLogger(__name__)

# Match the azure_openai package shape that Lens can filter/aggregate on.
_DIMENSIONS_OBJECT_MAPPING = {
    "dynamic": True,
    "type": "object",
    "properties": {
        "fingerprint": {"type": "keyword"},
    },
}

_SUBSCRIPTION_NAME_MAPPING = {
    "type": "keyword",
    "ignore_above": 1024,
}

# OpenAI Billing Daily usage panels date_histogram these fields.
_BILLING_DATE_MAPPING = {
    "type": "date",
}

_CUSTOM_TEMPLATES = (
    "metrics-azure.open_ai@custom",
    "metrics-azure.billing@custom",
    "metrics-azure.ai_foundry@custom",
)

_DATA_STREAMS = (
    "metrics-azure.open_ai",
    "metrics-azure.billing",
    "metrics-azure.ai_foundry",
)

# Fleet only composes metrics-azure.billing@custom when the azure_billing
# package is installed. OpenAI Billing still needs that stream when only
# azure_openai is installed — own the index template so date mappings apply.
_BILLING_INDEX_TEMPLATE = "metrics-azure.billing"
_BILLING_INDEX_TEMPLATE_PRIORITY = 250


def subscription_name(profile: dict, ns: str) -> str:
    return profile.get("azure_subscription_name") or f"{ns}-azure-subscription"


def _mapping_template_body(
    settings: dict | None = None,
    *,
    billing_dates: bool = False,
) -> dict:
    azure_props: dict = {
        "dimensions": dict(_DIMENSIONS_OBJECT_MAPPING),
        "subscription_name": dict(_SUBSCRIPTION_NAME_MAPPING),
    }
    if billing_dates:
        azure_props["billing"] = {
            "properties": {
                "usage_start": dict(_BILLING_DATE_MAPPING),
                "usage_end": dict(_BILLING_DATE_MAPPING),
            }
        }
    body: dict = {
        "template": {
            "mappings": {
                "properties": {
                    "azure": {
                        "properties": azure_props,
                    }
                }
            }
        }
    }
    if settings:
        body["template"]["settings"] = settings
    return body


def _billing_index_template_body() -> dict:
    return {
        "index_patterns": ["metrics-azure.billing-*"],
        "priority": _BILLING_INDEX_TEMPLATE_PRIORITY,
        "data_stream": {},
        "composed_of": [
            "metrics@settings",
            "metrics-azure.billing@custom",
            "ecs@mappings",
        ],
        "allow_auto_create": True,
        "_meta": {
            "managed_by": "o11y-launch-next",
            "description": "Demo index template so azure.billing.usage_start/end are date when azure_billing package is not installed.",
        },
    }


def ensure_dashboard_mappings(
    elastic_url: str,
    api_key: str,
    namespace: str,
    *,
    rollover: bool = True,
) -> None:
    """Align azure.dimensions / subscription_name / billing date mappings."""
    with httpx.Client(timeout=30) as http:
        headers = _es_headers(api_key)
        for tmpl in _CUSTOM_TEMPLATES:
            try:
                settings = None
                existing = http.get(
                    f"{elastic_url}/_component_template/{tmpl}",
                    headers=headers,
                )
                if existing.status_code == 200:
                    current = (
                        existing.json()
                        .get("component_templates", [{}])[0]
                        .get("component_template", {})
                        .get("template", {})
                    )
                    settings = current.get("settings")
                resp = http.put(
                    f"{elastic_url}/_component_template/{tmpl}",
                    headers=headers,
                    json=_mapping_template_body(
                        settings,
                        billing_dates=tmpl.startswith("metrics-azure.billing"),
                    ),
                )
                if resp.status_code >= 300:
                    logger.warning(
                        "Failed to put %s (%s): %s",
                        tmpl, resp.status_code, resp.text[:300],
                    )
                else:
                    logger.info("Ensured Azure dashboard mappings on %s", tmpl)
            except Exception as exc:
                logger.warning("Could not update %s: %s", tmpl, exc)

        try:
            resp = http.put(
                f"{elastic_url}/_index_template/{_BILLING_INDEX_TEMPLATE}",
                headers=headers,
                json=_billing_index_template_body(),
            )
            if resp.status_code >= 300:
                logger.warning(
                    "Failed to put index template %s (%s): %s",
                    _BILLING_INDEX_TEMPLATE,
                    resp.status_code,
                    resp.text[:300],
                )
            else:
                logger.info("Ensured index template %s", _BILLING_INDEX_TEMPLATE)
        except Exception as exc:
            logger.warning("Could not put billing index template: %s", exc)

        if not rollover:
            return

        for ds in _DATA_STREAMS:
            target = f"{ds}-{namespace}"
            try:
                resp = http.post(
                    f"{elastic_url}/{target}/_rollover",
                    headers=headers,
                )
                if resp.status_code >= 300:
                    logger.warning(
                        "Rollover of %s failed (%s): %s",
                        target, resp.status_code, resp.text[:200],
                    )
                    continue
                logger.info("Rolled over %s for Azure mapping fix", target)
            except Exception as exc:
                logger.warning("Rollover of %s failed (non-fatal): %s", target, exc)
                continue

            # Old backing indices keep the conflicting flattened mapping and
            # still match metrics-*, so Lens keeps failing until they are gone.
            # Only billing historically used flattened dimensions; prune those.
            if ds != "metrics-azure.billing":
                continue
            try:
                ds_info = http.get(
                    f"{elastic_url}/_data_stream/{target}",
                    headers=headers,
                )
                if ds_info.status_code != 200:
                    continue
                indices = (
                    ds_info.json()
                    .get("data_streams", [{}])[0]
                    .get("indices", [])
                )
                if len(indices) < 2:
                    continue
                # Keep the newest index; delete older generations.
                indices_sorted = sorted(indices, key=lambda i: i.get("index_name", ""))
                stale = [i["index_name"] for i in indices_sorted[:-1]]
                for index_name in stale:
                    del_resp = http.delete(
                        f"{elastic_url}/{index_name}",
                        headers=headers,
                    )
                    if del_resp.status_code >= 300:
                        logger.warning(
                            "Could not delete stale index %s (%s): %s",
                            index_name, del_resp.status_code, del_resp.text[:200],
                        )
                    else:
                        logger.info("Deleted stale Azure billing index %s", index_name)
            except Exception as exc:
                logger.warning("Stale index cleanup for %s failed: %s", target, exc)


def ensure_dashboard_mappings_from_env(namespace: str) -> None:
    """Best-effort mapping fix using ESBulkClient env credentials."""
    try:
        from app.telemetry import ESBulkClient
        client = ESBulkClient()
        if not client.configured:
            return
        ensure_dashboard_mappings(
            client.elastic_url,
            client.api_key,
            namespace,
            rollover=True,
        )
    except Exception as exc:
        logger.warning("Azure dashboard mapping ensure skipped: %s", exc)
