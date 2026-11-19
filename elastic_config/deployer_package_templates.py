"""PackageTemplatesMixin — SLOs and ML jobs from integration package templates.

Integration packages ship ``kibana/slo_template`` and ``kibana/ml_module`` assets
next to their alerting rule templates. Step 21 turns them into live SLOs and
anomaly detection jobs for every package the active scenario installs.

Like the OOTB alert rules, these objects are **cluster-shared**: their ids come
from the template, they carry no scenario namespace, every deploy upserts them,
and scenario teardown never deletes them. Objects whose package the active
scenario does not install are switched off instead (SLO disabled, datafeed
stopped), so two scenarios that share a package also share its SLOs and jobs.
"""

from __future__ import annotations

import fnmatch
import hashlib
import logging
import re
from typing import Any

import httpx

from elastic_config.deployer_base import _es_headers, _retry_http

logger = logging.getLogger("deployer")

# Tags every package SLO carries: one marks it as OOTB, one names its package.
OOTB_TEMPLATE_TAG = "ootb-template"
OOTB_PACKAGE_TAG_PREFIX = "ootb-package:"
# Job custom_settings key naming the package an OOTB ML job came from.
ML_PACKAGE_SETTING = "ootb_package"

# Fields of an slo_template ``attributes`` object the SLO create API accepts.
_SLO_BODY_FIELDS = (
    "name",
    "description",
    "indicator",
    "timeWindow",
    "budgetingMethod",
    "objective",
    "settings",
    "groupBy",
    "artifacts",
)
_SLO_REQUIRED_FIELDS = ("name", "indicator", "timeWindow", "budgetingMethod", "objective")

# Kibana's SLO id rule: 8 to 48 characters matching /^[a-z0-9-_]+$/ — lowercase
# letters, digits, hyphens and underscores only (an uppercase letter is HTTP 400
# ``invalid_format``, and package template ids end in ``-Rolling30Days``).
_SLO_ID_INVALID = re.compile(r"[^a-z0-9_-]")
_SLO_ID_MIN, _SLO_ID_MAX = 8, 48

_ML_SETUP_HEADERS = {"elastic-api-version": "1"}


def slo_id_for_template(template_id: str) -> str:
    """Stable SLO id for a package template id.

    Template ids such as ``aws_rds_otel-avg-read-latency-99.5-Rolling30Days``
    hold characters (``.``, uppercase letters) and lengths the SLO API rejects, so
    the id is the lowercased, sanitized template id, with a short hash of the
    original (case kept) when it has to be shortened or padded.
    """
    clean = _SLO_ID_INVALID.sub("_", template_id.lower())
    if _SLO_ID_MIN <= len(clean) <= _SLO_ID_MAX:
        return clean
    digest = hashlib.sha1(template_id.encode()).hexdigest()[:8]
    return f"{clean[:_SLO_ID_MAX - len(digest) - 1]}-{digest}"


def slo_package(slo: dict[str, Any]) -> str | None:
    """Package named by an OOTB SLO's ``ootb-package:<pkg>`` tag, or None."""
    for tag in slo.get("tags") or []:
        if isinstance(tag, str) and tag.startswith(OOTB_PACKAGE_TAG_PREFIX):
            return tag[len(OOTB_PACKAGE_TAG_PREFIX):]
    return None


def slo_body_from_template(template: dict[str, Any], pkg: str) -> dict[str, Any] | None:
    """Map an ``slo_template`` asset to an SLO create/update body (without ``id``)."""
    attrs = template.get("attributes") if isinstance(template.get("attributes"), dict) else template
    if not all(attrs.get(field) is not None for field in _SLO_REQUIRED_FIELDS):
        return None
    body = {field: attrs[field] for field in _SLO_BODY_FIELDS if attrs.get(field) is not None}
    body.setdefault("description", "")
    tags = [*(attrs.get("tags") or []), OOTB_TEMPLATE_TAG, f"{OOTB_PACKAGE_TAG_PREFIX}{pkg}"]
    body["tags"] = list(dict.fromkeys(tags))
    return body


def ml_module_parts(module: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Return ``(module_id, attributes)`` for an ``ml_module`` asset."""
    attrs = module.get("attributes") if isinstance(module.get("attributes"), dict) else module
    return str(attrs.get("id") or module.get("id") or ""), attrs


def package_datasets(item: dict[str, Any]) -> list[str]:
    """``discovery.datasets[].name`` from a Fleet package-info item."""
    datasets = ((item.get("discovery") or {}).get("datasets")) or []
    return [d["name"] for d in datasets if isinstance(d, dict) and d.get("name")]


def ml_index_pattern(attrs: dict[str, Any], datasets: list[str]) -> str:
    """Index pattern the module's datafeeds should read.

    The module's ``defaultIndexPattern``, unless it matches none of the datasets its
    own package declares (``discovery.datasets``) — then the datafeeds read those
    datasets instead. aws_elb_metrics_otel 0.12.x is such a case: its ML modules read
    ``metrics-aws.applicationelb.otel-*`` while the package (alerts, dashboards,
    SLOs) writes and reads ``aws.elb.otel``. A comma-separated list is accepted by
    both the module setup API and the ES datafeed API.
    """
    default = attrs.get("defaultIndexPattern") or ""
    if not default or not datasets:
        return default
    kind = default.split("-", 1)[0]
    kind = kind if kind in ("metrics", "logs", "traces") else "metrics"
    if any(fnmatch.fnmatchcase(f"{kind}-{ds}-default", default) for ds in datasets):
        return default
    return ",".join(f"{kind}-{ds}-*" for ds in datasets)


def ml_setup_body(attrs: dict[str, Any], pkg: str, datasets: list[str] | None = None) -> dict[str, Any]:
    """Kibana ML module setup body: shipped job ids, live datafeeds, package tag."""
    body: dict[str, Any] = {
        "prefix": "",
        "startDatafeed": True,
        "useDedicatedIndex": False,
        "estimateModelMemory": True,
        "applyToAllSpaces": True,
        "jobOverrides": {"custom_settings": {ML_PACKAGE_SETTING: pkg}},
    }
    index = ml_index_pattern(attrs, datasets or [])
    if index:
        body["indexPatternName"] = index
    return body


class PackageTemplatesMixin:

    # ── SLO templates ────────────────────────────────────────────────

    def _upsert_ootb_slo(
        self,
        client: httpx.Client,
        pkg: str,
        template_id: str,
        template: dict[str, Any],
        headers: dict[str, str],
    ) -> bool:
        """Create or update one package SLO and make sure it is enabled."""
        body = slo_body_from_template(template, pkg)
        if body is None:
            logger.warning("SLO template %s is missing required fields", template_id)
            return False
        slo_id = slo_id_for_template(template_id)
        base = f"{self.kibana_url}/api/observability/slos"

        get_resp = _retry_http(
            lambda: client.get(f"{base}/{slo_id}", headers=headers, timeout=30.0),
            label=f"get OOTB SLO {slo_id}",
            attempts=3,
            base_delay=1.0,
        )
        exists = get_resp is not None and get_resp.status_code == 200

        def _send(payload: dict[str, Any]) -> httpx.Response | None:
            if exists:
                return _retry_http(
                    lambda: client.put(f"{base}/{slo_id}", headers=headers, json=payload, timeout=60.0),
                    label=f"update OOTB SLO {slo_id}",
                    attempts=3,
                )
            return _retry_http(
                lambda: client.post(base, headers=headers, json={**payload, "id": slo_id}, timeout=60.0),
                label=f"create OOTB SLO {slo_id}",
                attempts=2,
            )

        resp = _send(body)
        # Older stacks reject the template-only ``artifacts`` link; it is cosmetic.
        # Retry without it only when the 400 is about it, not for any other 400
        # (a bad id would just fail twice and the second response would be logged).
        if (resp is not None and resp.status_code == 400 and "artifacts" in body
                and "artifacts" in (resp.text or "")):
            body = {k: v for k, v in body.items() if k != "artifacts"}
            resp = _send(body)
        if resp is None or resp.status_code >= 300:
            logger.warning(
                "%s OOTB SLO %s failed: HTTP %s %s",
                "Update" if exists else "Create",
                slo_id,
                resp.status_code if resp is not None else "no-response",
                resp.text[:1000] if resp is not None else "",
            )
            return False

        if exists:
            try:
                enabled = get_resp.json().get("enabled", True)
            except Exception:
                enabled = True
            if enabled is False:
                _retry_http(
                    lambda: client.post(f"{base}/{slo_id}/enable", headers=headers, timeout=30.0),
                    label=f"enable OOTB SLO {slo_id}",
                    attempts=2,
                )
        return True

    def _list_ootb_slos(self, client: httpx.Client, headers: dict[str, str]) -> list[dict[str, Any]]:
        """Every SLO definition tagged ``ootb-template`` (one entry per SLO id)."""
        found: dict[str, dict[str, Any]] = {}
        page = 1
        while True:
            resp = _retry_http(
                lambda p=page: client.get(
                    f"{self.kibana_url}/api/observability/slos",
                    headers=headers,
                    params={"page": p, "perPage": 500},
                    timeout=60.0,
                ),
                label=f"list SLOs page {page}",
                attempts=2,
                base_delay=1.0,
            )
            if resp is None or resp.status_code >= 300:
                break
            body = resp.json()
            results = body.get("results") or []
            for slo in results:
                if OOTB_TEMPLATE_TAG in (slo.get("tags") or []) and slo.get("id"):
                    found.setdefault(slo["id"], slo)
            if not results or page * int(body.get("perPage") or 500) >= int(body.get("total") or 0):
                break
            page += 1
        return list(found.values())

    def _disable_orphaned_ootb_slos(
        self, client: httpx.Client, headers: dict[str, str], active_packages: list[str],
    ) -> int:
        """Disable package SLOs whose package the active scenario does not install."""
        active = set(active_packages)
        disabled = 0
        for slo in self._list_ootb_slos(client, headers):
            pkg = slo_package(slo)
            if pkg is None or pkg in active or slo.get("enabled") is False:
                continue
            resp = _retry_http(
                lambda sid=slo["id"]: client.post(
                    f"{self.kibana_url}/api/observability/slos/{sid}/disable",
                    headers=headers,
                    timeout=30.0,
                ),
                label=f"disable orphaned SLO {slo['id']}",
                attempts=2,
            )
            if resp is not None and resp.status_code < 300:
                disabled += 1
                logger.info("Disabled orphaned OOTB SLO %s (%s)", slo["id"], pkg)
        return disabled

    # ── ML modules ───────────────────────────────────────────────────

    def _setup_ootb_ml_module(
        self,
        client: httpx.Client,
        pkg: str,
        module: dict[str, Any],
        headers: dict[str, str],
        datasets: list[str] | None = None,
    ) -> tuple[int, int]:
        """Set up one package ML module; return ``(jobs running, jobs failed)``.

        Uses Kibana's module setup (job ids as shipped, datafeeds started). Jobs
        that already exist from an earlier deploy are opened and their datafeeds
        started. When Kibana does not know the module (e.g. Fleet did not install
        the ``ml-module`` saved object), the jobs are created through the ES ML API.
        """
        module_id, attrs = ml_module_parts(module)
        jobs = [j for j in attrs.get("jobs") or [] if isinstance(j, dict) and j.get("id")]
        if not module_id or not jobs:
            logger.warning("ML module from %s has no id or jobs", pkg)
            return 0, 0

        resp = _retry_http(
            lambda: client.post(
                f"{self.kibana_url}/internal/ml/modules/setup/{module_id}",
                headers={**headers, **_ML_SETUP_HEADERS},
                json=ml_setup_body(attrs, pkg, datasets),
                timeout=120.0,
            ),
            label=f"setup ML module {module_id}",
            attempts=3,
        )
        if resp is None or resp.status_code >= 300:
            logger.warning(
                "ML module setup %s failed (HTTP %s %s); creating jobs through ES",
                module_id,
                resp.status_code if resp is not None else "no-response",
                resp.text[:200] if resp is not None else "",
            )
            return self._create_ml_module_jobs_in_es(client, pkg, attrs, datasets)

        try:
            result = resp.json()
        except Exception:
            result = {}
        datafeeds = {
            d.get("id"): d for d in result.get("datafeeds") or [] if isinstance(d, dict)
        }
        module_feeds = {
            d.get("job_id"): d.get("id")
            for d in attrs.get("datafeeds") or []
            if isinstance(d, dict) and d.get("job_id") and d.get("id")
        }
        running = failed = 0
        for job in result.get("jobs") or []:
            job_id = job.get("id")
            if not job_id:
                continue
            datafeed_id = module_feeds.get(job_id) or f"datafeed-{job_id}"
            datafeed = datafeeds.get(datafeed_id) or {"id": datafeed_id}
            if job.get("success") and datafeed.get("started"):
                running += 1
                continue
            error = str(job.get("error") or datafeed.get("error") or "")
            if job.get("success") or "already_exists" in error or "already exists" in error:
                if self._ensure_ml_job_running(client, job_id, datafeed["id"]):
                    running += 1
                    continue
            logger.warning("OOTB ML job %s from %s not running: %s", job_id, module_id, error[:200])
            failed += 1
        return running, failed

    def _ensure_ml_job_running(self, client: httpx.Client, job_id: str, datafeed_id: str) -> bool:
        """Open a job and start its datafeed; both are no-ops when already running."""
        es = _es_headers(self.api_key)
        opened = _retry_http(
            lambda: client.post(
                f"{self.elastic_url}/_ml/anomaly_detectors/{job_id}/_open",
                headers=es,
                timeout=60.0,
            ),
            label=f"open ML job {job_id}",
            attempts=1,
        )
        if opened is None or (opened.status_code >= 300 and opened.status_code != 409):
            return False
        started = _retry_http(
            lambda: client.post(
                f"{self.elastic_url}/_ml/datafeeds/{datafeed_id}/_start",
                headers=es,
                timeout=60.0,
            ),
            label=f"start ML datafeed {datafeed_id}",
            attempts=1,
        )
        # 409: the datafeed is already started.
        return started is not None and (started.status_code < 300 or started.status_code == 409)

    def _create_ml_module_jobs_in_es(
        self,
        client: httpx.Client,
        pkg: str,
        attrs: dict[str, Any],
        datasets: list[str] | None = None,
    ) -> tuple[int, int]:
        """Fallback: create a module's jobs and datafeeds through the ES ML API."""
        es = _es_headers(self.api_key)
        index = ml_index_pattern(attrs, datasets or [])
        module_query = attrs.get("query")
        datafeeds = {
            d.get("job_id"): d for d in attrs.get("datafeeds") or [] if isinstance(d, dict)
        }
        running = failed = 0
        for job in attrs.get("jobs") or []:
            job_id = job.get("id") if isinstance(job, dict) else None
            if not job_id:
                continue
            config = dict(job.get("config") or {})
            config["custom_settings"] = {
                **(config.get("custom_settings") or {}),
                ML_PACKAGE_SETTING: pkg,
            }
            put_job = _retry_http(
                lambda c=config, j=job_id: client.put(
                    f"{self.elastic_url}/_ml/anomaly_detectors/{j}", headers=es, json=c, timeout=60.0,
                ),
                label=f"create ML job {job_id}",
                attempts=2,
            )
            if put_job is None or (
                put_job.status_code >= 300 and "already_exists" not in put_job.text
            ):
                failed += 1
                continue

            df = datafeeds.get(job_id) or {}
            df_id = df.get("id") or f"datafeed-{job_id}"
            df_config = {**(df.get("config") or {}), "job_id": job_id}
            if index:
                df_config["indices"] = index.split(",")
            if "query" not in df_config and module_query:
                df_config["query"] = module_query
            put_df = _retry_http(
                lambda c=df_config, d=df_id: client.put(
                    f"{self.elastic_url}/_ml/datafeeds/{d}", headers=es, json=c, timeout=60.0,
                ),
                label=f"create ML datafeed {df_id}",
                attempts=2,
            )
            if put_df is None or (put_df.status_code >= 300 and "already_exists" not in put_df.text):
                failed += 1
                continue
            if self._ensure_ml_job_running(client, job_id, df_id):
                running += 1
            else:
                failed += 1
        return running, failed

    def _stop_orphaned_ootb_ml_jobs(self, client: httpx.Client, active_packages: list[str]) -> int:
        """Stop the datafeeds of package ML jobs whose package is not active."""
        es = _es_headers(self.api_key)
        active = set(active_packages)
        jobs_resp = _retry_http(
            lambda: client.get(f"{self.elastic_url}/_ml/anomaly_detectors", headers=es, timeout=60.0),
            label="list ML jobs",
            attempts=2,
        )
        if jobs_resp is None or jobs_resp.status_code >= 300:
            return 0
        orphan_jobs = {
            job["job_id"]
            for job in jobs_resp.json().get("jobs") or []
            if (job.get("custom_settings") or {}).get(ML_PACKAGE_SETTING)
            and job["custom_settings"][ML_PACKAGE_SETTING] not in active
        }
        if not orphan_jobs:
            return 0

        stats_resp = _retry_http(
            lambda: client.get(f"{self.elastic_url}/_ml/datafeeds/_stats", headers=es, timeout=60.0),
            label="list ML datafeed stats",
            attempts=2,
        )
        if stats_resp is None or stats_resp.status_code >= 300:
            return 0
        feeds_resp = _retry_http(
            lambda: client.get(f"{self.elastic_url}/_ml/datafeeds", headers=es, timeout=60.0),
            label="list ML datafeeds",
            attempts=2,
        )
        if feeds_resp is None or feeds_resp.status_code >= 300:
            return 0
        started = {
            s.get("datafeed_id")
            for s in stats_resp.json().get("datafeeds") or []
            if s.get("state") == "started"
        }
        stopped = 0
        for feed in feeds_resp.json().get("datafeeds") or []:
            df_id = feed.get("datafeed_id")
            if feed.get("job_id") not in orphan_jobs or df_id not in started:
                continue
            resp = _retry_http(
                lambda d=df_id: client.post(
                    f"{self.elastic_url}/_ml/datafeeds/{d}/_stop", headers=es, timeout=60.0,
                ),
                label=f"stop orphaned datafeed {df_id}",
                attempts=2,
            )
            if resp is not None and resp.status_code < 300:
                stopped += 1
                logger.info("Stopped orphaned OOTB ML datafeed %s", df_id)
        return stopped
