"""Azure Billing telemetry generator.

Writes ``metrics-azure.billing-<ns>`` documents that the ``azure_billing``
Elastic integration package expects. Package dashboards target
``*-default``; when the scenario namespace differs, each batch is also
dual-written there (same pattern as Azure OpenAI / Bedrock / Vertex).

Documents are sent with ``pipeline=_none`` to bypass the package
``default_pipeline``.  Fleet's ``final_pipeline`` still runs and adds
``event.ingested``.

Matches the Metricbeat usage/forecast dual-write shape:

* **Usage docs** — ``pretax_cost`` plus product / meter / resource dims, with
  ``usage_start`` / ``usage_end`` on *completed* UTC day bounds
  (``00:00:00Z`` → ``23:59:59Z``).  No ``actual_cost`` / ``forecast_cost``.
* **Forecast docs** — separate docs with only ``actual_cost`` *or*
  ``forecast_cost``, plus ``usage_date`` at day start ``00:00:00Z``,
  ``currency``, and ``subscription_id``.

Lens Forecast / Daily usage panels histogram those date fields at ``24h`` with
``dropPartials: true``; wall-clock timestamps land in the partial current
bucket and are dropped.  ``@timestamp`` stays recent so the data-view time
filter still matches.

Demo time picker: ``now-24h`` → ``now`` cannot keep any complete UTC day
bucket — panels stay empty by math even with correct dual-write.  Use at
least Last 7 days for Daily usage; for Forecast extend into the future
(e.g. ``now-7d`` → ``now+31d``) so ``forecast_cost`` days are in range.
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/azure_billing@1.13.0"

import datetime
import logging
import os
import random
import signal
import threading
import time
import uuid
from typing import Any

from app.telemetry import ESBulkClient
from generator_engine.azure_common import (
    ensure_dashboard_mappings_from_env,
    subscription_name as _subscription_name,
)
from generator_engine.es_doc_common import (
    build_base_doc,
    business_load,
    emit_namespaces,
    send_dual_bulk,
)
from generator_engine.db_common import resolve_active_chaos

logger = logging.getLogger(__name__)

DATA_STREAMS = ("metrics-azure.billing",)

DOC_FIELDS = (
    "azure.billing.pretax_cost",
    "azure.billing.actual_cost",
    "azure.billing.forecast_cost",
    "azure.billing.currency",
    "azure.billing.product",
    "azure.billing.usage_start",
    "azure.billing.usage_end",
    "azure.billing.usage_date",
    "azure.billing.usage_quantity",
    "azure.subscription_id",
    "azure.subscription_name",
    "azure.resource.type",
)

_BATCH_INTERVAL_MIN = 55.0
_BATCH_INTERVAL_MAX = 65.0

# Complete UTC days of usage history for Daily usage bars.
_USAGE_HISTORY_DAYS = 14
# Forecast window: day-2 .. day+30. azure_billing@1.13.0 collects forecasts over a
# 720h (30 day) window by default (``billing_forecast_window``).
_FORECAST_PAST_DAYS = 2
_FORECAST_FUTURE_DAYS = 30

# Billing overview has no resource-type filter; OpenAI Billing filters on
# Microsoft.CognitiveServices. Keep a CognitiveServices-heavy mix so both
# dashboards stay populated while pies split across subs / RGs / resources.
_OPENAI_RESOURCE_TYPE = "Microsoft.CognitiveServices"

# (resource_type, service_name, product, meter_name, unit, cost_range, weight)
_GENERIC_SKUS: list[tuple[str, str, str, str, str, tuple[float, float], int]] = [
    ("Microsoft.Compute", "Virtual Machines", "Virtual Machines Dv5 Series",
     "D4s v5", "1 Hour", (0.15, 2.5), 18),
    ("Microsoft.Compute", "Virtual Machines", "Virtual Machines Ev5 Series",
     "E8s v5", "1 Hour", (0.40, 4.0), 10),
    ("Microsoft.Storage", "Storage", "Blob Storage - Hot LRS",
     "Hot Stored Data", "1 GB/Month", (0.05, 1.2), 14),
    ("Microsoft.Storage", "Storage", "Blob Storage - Cool LRS",
     "Cool Stored Data", "1 GB/Month", (0.02, 0.6), 8),
    ("Microsoft.Network", "Bandwidth", "Bandwidth - Inter-Region",
     "Data Transfer Out", "1 GB", (0.08, 1.5), 10),
    ("Microsoft.Network", "Load Balancer", "Standard Load Balancer",
     "Standard Included LB Rules", "1 Hour", (0.02, 0.4), 8),
    ("Microsoft.Sql", "SQL Database", "SQL Database Single DB",
     "vCore", "1 Hour", (0.20, 3.0), 8),
    ("Microsoft.KeyVault", "Key Vault", "Key Vault Operations",
     "Operations", "10K", (0.01, 0.25), 6),
    ("Microsoft.ContainerService", "Azure Kubernetes Service",
     "Azure Kubernetes Service", "Cluster Management", "1 Hour", (0.10, 1.0), 8),
]

# Model deployment → relative traffic weight and $/1K tokens for OpenAI lines.
_MODEL_WEIGHTS = {
    "gpt-4o": (45, (0.012, 0.028)),
    "gpt-4.1": (20, (0.010, 0.022)),
    "gpt-4.1-mini": (25, (0.002, 0.008)),
    "text-embedding-3-large": (10, (0.0001, 0.0004)),
}


def _stable_uuid(seed: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, seed))


def _fmt_z(dt: datetime.datetime) -> str:
    return dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _utc_day_start(day: datetime.date) -> datetime.datetime:
    return datetime.datetime(day.year, day.month, day.day, tzinfo=datetime.timezone.utc)


def _utc_day_end(day: datetime.date) -> datetime.datetime:
    return _utc_day_start(day) + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)


def _completed_utc_days(reference: datetime.datetime, n: int) -> list[datetime.date]:
    """Last ``n`` completed UTC calendar days (yesterday backward)."""
    today = reference.astimezone(datetime.timezone.utc).date()
    return [today - datetime.timedelta(days=i) for i in range(1, n + 1)]


def _forecast_utc_days(reference: datetime.datetime) -> list[datetime.date]:
    """UTC dates from day-2 through day+30 inclusive."""
    today = reference.astimezone(datetime.timezone.utc).date()
    start = today - datetime.timedelta(days=_FORECAST_PAST_DAYS)
    end = today + datetime.timedelta(days=_FORECAST_FUTURE_DAYS)
    days: list[datetime.date] = []
    d = start
    while d <= end:
        days.append(d)
        d += datetime.timedelta(days=1)
    return days


def _resource_groups(profile: dict, ns: str) -> list[str]:
    groups = list(profile.get("azure_resource_groups") or [])
    primary = profile.get("azure_resource_group") or ns
    if primary and primary not in groups:
        groups.insert(0, primary)
    # Enrich thin profiles so Resource groups pie is not a single 100% slice.
    for extra in (ScenarioIdentity(ns).label("ai-rg"), ScenarioIdentity(ns).label("platform-rg"), ScenarioIdentity(ns).label("data-rg")):
        if extra not in groups:
            groups.append(extra)
        if len(groups) >= 4:
            break
    return groups


def _subscriptions(profile: dict, ns: str) -> list[tuple[str, str]]:
    """Return [(subscription_id, subscription_name), ...] with ≥2 entries."""
    primary_id = profile.get("azure_subscription_id") or ScenarioIdentity(ns).azure_subscription_id
    primary_name = _subscription_name(profile, ns)
    subs = [(primary_id, primary_name)]
    extras = profile.get("azure_subscriptions") or [
        (_stable_uuid(ScenarioIdentity(ns).label("sub-dev")), f"{ns.title()} Dev Azure"),
        (_stable_uuid(ScenarioIdentity(ns).label("sub-shared")), f"{ns.title()} Shared Services"),
    ]
    for item in extras:
        if isinstance(item, dict):
            sid = item.get("id") or _stable_uuid(ScenarioIdentity(ns).label(str(item.get("name", "sub"))))
            sname = item.get("name") or sid
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            sid, sname = str(item[0]), str(item[1])
        else:
            continue
        if sid != primary_id:
            subs.append((sid, sname))
    return subs


def _openai_resources(profile: dict, ns: str) -> list[str]:
    resources = list(profile.get("azure_openai_resources") or [])
    if not resources:
        resources = [ScenarioIdentity(ns).label("openai-eastus")]
    for extra in (ScenarioIdentity(ns).label("openai-eastus2"), ScenarioIdentity(ns).label("openai-swedencentral")):
        if extra not in resources:
            resources.append(extra)
        if len(resources) >= 3:
            break
    return resources


def _openai_models(profile: dict) -> list[str]:
    models = list(profile.get("azure_openai_models") or [])
    for extra in ("gpt-4o", "gpt-4.1-mini", "gpt-4.1", "text-embedding-3-large"):
        if extra not in models:
            models.append(extra)
        if len(models) >= 4:
            break
    return models


def _pick_weighted(rng: random.Random, items: list, weights: list[int]):
    return rng.choices(items, weights=weights, k=1)[0]


def _pick_line(profile: dict, ns: str, rng: random.Random, calls: int) -> dict:
    """Choose an OpenAI or generic Azure SKU line with uneven cost weights."""
    # ~55% CognitiveServices / OpenAI so OpenAI Billing dashboard stays busy.
    if rng.random() < 0.55:
        models = _openai_models(profile)
        weights = [_MODEL_WEIGHTS.get(m, (12, (0.005, 0.015)))[0] for m in models]
        model = _pick_weighted(rng, models, weights)
        _w, cost_range = _MODEL_WEIGHTS.get(model, (12, (0.005, 0.015)))
        resources = _openai_resources(profile, ns)
        # Uneven resource spend (primary region dominates).
        res_weights = [55, 30, 15][: len(resources)]
        resource = _pick_weighted(rng, resources, res_weights)
        tokens = max(1, calls) * rng.randint(30, 120)
        # Heavier models burn more tokens per "call" batch.
        if "embedding" in model:
            tokens = max(1, int(tokens * 0.4))
        elif model == "gpt-4o":
            tokens = int(tokens * rng.uniform(1.2, 1.8))
        cost = tokens / 1000 * rng.uniform(*cost_range)
        return {
            "resource_type": _OPENAI_RESOURCE_TYPE,
            "service_name": "Azure OpenAI",
            "product": f"Azure OpenAI - {model}",
            "meter_name": f"{model} tokens",
            "unit": "1K Tokens",
            "resource_name": resource,
            "cost": cost,
            "usage_qty": float(tokens),
        }

    sku = _pick_weighted(
        rng,
        _GENERIC_SKUS,
        [s[-1] for s in _GENERIC_SKUS],
    )
    resource_type, service_name, product, meter_name, unit, cost_range, _w = sku
    prefix = {
        "Microsoft.Compute": "vm",
        "Microsoft.Storage": "st",
        "Microsoft.Network": "net",
        "Microsoft.Sql": "sql",
        "Microsoft.KeyVault": "kv",
        "Microsoft.ContainerService": "aks",
    }.get(resource_type, "res")
    resource = ScenarioIdentity(ns).label(prefix, rng.randint(1, 6))
    usage_qty = float(rng.randint(1, 80))
    cost = usage_qty * rng.uniform(*cost_range)
    return {
        "resource_type": resource_type,
        "service_name": service_name,
        "product": product,
        "meter_name": meter_name,
        "unit": unit,
        "resource_name": resource,
        "cost": cost,
        "usage_qty": usage_qty,
    }


def _build_usage_doc(
    ingest_ts: float,
    usage_day: datetime.date,
    ns: str,
    profile: dict,
    rng: random.Random,
    calls: int,
    *,
    cost_multiplier: float = 1.0,
    usage_multiplier: float = 1.0,
    line: dict | None = None,
) -> dict:
    """One usage line for a completed UTC day (Daily usage / pies)."""
    region = profile.get("azure_region", "eastus")
    groups = _resource_groups(profile, ns)
    subs = _subscriptions(profile, ns)
    # Bias spend toward the primary subscription / AI RG so demos stay plausible.
    sub_weights = [70] + [20] + [10] * max(0, len(subs) - 2)
    sub_weights = sub_weights[: len(subs)]
    subscription_id, subscription_name = _pick_weighted(rng, subs, sub_weights)
    group_weights = [50, 25, 15, 10][: len(groups)]
    resource_group = _pick_weighted(rng, groups, group_weights)

    if line is None:
        line = _pick_line(profile, ns, rng, calls)

    resource_type = line["resource_type"]
    resource_name = line["resource_name"]
    product = line["product"]
    service_name = line["service_name"]
    meter_name = line["meter_name"]
    unit = line["unit"]
    cost = round(line["cost"] * max(cost_multiplier, 0.1), 4)
    usage_qty = float(line["usage_qty"]) * max(usage_multiplier, 0.1)

    usage_start = _fmt_z(_utc_day_start(usage_day))
    usage_end = _fmt_z(_utc_day_end(usage_day))
    period_id = usage_day.strftime("%Y%m")

    doc = build_base_doc(
        timestamp=ingest_ts,
        dataset="azure.billing",
        namespace=ns,
        doc_type="metrics",
        agent_type="azure-monitor",
        cloud={
            "provider": "azure",
            "region": region,
            "account_id": subscription_id,
            "account_name": subscription_name,
        },
    )

    doc["azure"] = {
        "billing": {
            "pretax_cost": cost,
            "currency": "USD",
            "product": product,
            "usage_start": usage_start,
            "usage_end": usage_end,
            "usage_date": usage_start,
            "usage_quantity": usage_qty,
            "account_name": resource_group,
            "department_name": resource_group,
            "subscription_id": subscription_id,
            "resource_name": resource_name,
            "service_name": service_name,
            "billing_period_id": (
                f"/subscriptions/{subscription_id}/providers/"
                f"Microsoft.Billing/billingPeriods/{period_id}"
            ),
            "meter_details": {
                "meter_name": meter_name,
                "unit_of_measure": unit,
            },
        },
        "subscription_id": subscription_id,
        "subscription_name": subscription_name,
        "resource": {
            "id": (
                f"/subscriptions/{subscription_id}/"
                f"resourceGroups/{resource_group}/"
                f"providers/{resource_type}/accounts/{resource_name}"
                if resource_type == _OPENAI_RESOURCE_TYPE
                else (
                    f"/subscriptions/{subscription_id}/"
                    f"resourceGroups/{resource_group}/"
                    f"providers/{resource_type}/{resource_name}"
                )
            ),
            "name": resource_name,
            "group": resource_group,
            "type": resource_type,
        },
    }
    doc["metricset"] = {"period": 86400000}
    return doc


def _build_forecast_doc(
    ingest_ts: float,
    usage_day: datetime.date,
    ns: str,
    profile: dict,
    *,
    subscription_id: str,
    kind: str,
    cost: float,
) -> dict:
    """Forecast dual-write doc: only actual_cost *or* forecast_cost + usage_date."""
    if kind not in ("actual", "forecast"):
        raise ValueError(f"kind must be 'actual' or 'forecast', got {kind!r}")

    region = profile.get("azure_region", "eastus")
    usage_date = _fmt_z(_utc_day_start(usage_day))

    doc = build_base_doc(
        timestamp=ingest_ts,
        dataset="azure.billing",
        namespace=ns,
        doc_type="metrics",
        agent_type="azure-monitor",
        cloud={
            "provider": "azure",
            "region": region,
            "account_id": subscription_id,
        },
    )

    billing: dict[str, Any] = {
        "currency": "USD",
        "usage_date": usage_date,
    }
    if kind == "actual":
        billing["actual_cost"] = cost
    else:
        billing["forecast_cost"] = cost

    doc["azure"] = {
        "billing": billing,
        "subscription_id": subscription_id,
    }
    doc["metricset"] = {"period": 86400000}
    return doc


# Backward-compatible alias used by older call sites / chaos helpers.
def _build_billing_doc(
    ts: float,
    ns: str,
    profile: dict,
    rng: random.Random,
    calls: int,
    *,
    cost_multiplier: float = 1.0,
    usage_multiplier: float = 1.0,
    line: dict | None = None,
    usage_day: datetime.date | None = None,
) -> dict:
    """Build a usage doc for ``usage_day`` (default: yesterday UTC)."""
    if usage_day is None:
        ref = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
        usage_day = ref.date() - datetime.timedelta(days=1)
    return _build_usage_doc(
        ts, usage_day, ns, profile, rng, calls,
        cost_multiplier=cost_multiplier,
        usage_multiplier=usage_multiplier,
        line=line,
    )


def _build_billing_batch(
    ingest_ts: float,
    ns: str,
    profile: dict,
    rng: random.Random,
    load: float,
    *,
    cost_multiplier: float = 1.0,
    usage_multiplier: float = 1.0,
    usage_history_days: int = _USAGE_HISTORY_DAYS,
) -> list[dict]:
    """Usage history + forecast dual-write docs for one emit cycle."""
    ref = datetime.datetime.fromtimestamp(ingest_ts, datetime.timezone.utc)
    today = ref.date()
    docs: list[dict] = []

    for day in _completed_utc_days(ref, usage_history_days):
        # Older days slightly quieter so recent history dominates the chart.
        age = (today - day).days
        day_scale = max(0.55, 1.15 - 0.04 * age)
        day_load = load * day_scale * rng.uniform(0.85, 1.15)
        n_lines = max(4, int(8 * day_load))
        for _ in range(n_lines):
            calls = max(1, int(day_load * rng.randint(15, 80)))
            docs.append(
                _build_usage_doc(
                    ingest_ts, day, ns, profile, rng, calls,
                    cost_multiplier=cost_multiplier,
                    usage_multiplier=usage_multiplier,
                )
            )

    subs = _subscriptions(profile, ns)
    for day in _forecast_utc_days(ref):
        for subscription_id, _name in subs:
            # Per-sub daily total; forecast sits above actuals.
            base = rng.uniform(60.0, 350.0) * max(cost_multiplier, 0.1)
            if day < today:
                cost = round(base * rng.uniform(0.90, 1.10), 4)
                docs.append(
                    _build_forecast_doc(
                        ingest_ts, day, ns, profile,
                        subscription_id=subscription_id,
                        kind="actual",
                        cost=cost,
                    )
                )
            else:
                cost = round(base * rng.uniform(1.05, 1.35), 4)
                docs.append(
                    _build_forecast_doc(
                        ingest_ts, day, ns, profile,
                        subscription_id=subscription_id,
                        kind="forecast",
                        cost=cost,
                    )
                )

    return docs


from generators.azure_billing import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def run(
    client: ESBulkClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    if scenario_data and "scenario" in scenario_data:
        scenario = scenario_data["scenario"]
        ns = scenario_data["namespace"]
    else:
        from app.config import ACTIVE_SCENARIO
        from scenario_engine import get_scenario
        scenario = get_scenario(ACTIVE_SCENARIO)
        ns = scenario.namespace

    profile = scenario.llm_profile

    if not client.configured:
        logger.warning("ESBulkClient not configured; Azure Billing generator will not emit.")
        return

    for emit_ns in emit_namespaces(ns):
        ensure_dashboard_mappings_from_env(emit_ns)

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    cfg = scenario_data.get("generator_config", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    rng = random.Random()
    logger.info(
        "Azure Billing generator started (namespace=%s, emit=%s)",
        ns,
        ",".join(emit_namespaces(ns)),
    )

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="azure_billing",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        cost_multiplier = float((active_chaos or {}).get("cost_multiplier", 1.0))
        usage_multiplier = float((active_chaos or {}).get("usage_multiplier", 1.0))
        if not active_chaos:
            cost_multiplier = usage_multiplier = 1.0

        load = business_load()
        now = time.time()
        docs = _build_billing_batch(
            now, ns, profile, rng, load,
            cost_multiplier=cost_multiplier,
            usage_multiplier=usage_multiplier,
        )
        send_dual_bulk(client, "metrics-azure.billing", ns, docs)
        logger.debug("Azure Billing: sent %d billing docs (load=%.2f)", len(docs), load)
    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(_BATCH_INTERVAL_MIN, _BATCH_INTERVAL_MAX))

    logger.info("Azure Billing generator stopped.")


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = ESBulkClient()
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    duration = int(os.environ.get("RUN_DURATION", "60"))
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
