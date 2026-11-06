"""StreamsMixin — stream fork, knowledge indicators, significant events, and cleanup."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from elastic_config.deployer_base import _es_headers, _kibana_headers, _retry_http, ProgressCallback, StepIdx
from scenario_engine.runtimes import RUNTIME_BY_LANGUAGE

logger = logging.getLogger("deployer")

# Fork can fail on a cold cluster if logs.otel is not ready yet or Streams is
# still enabling. Match OTLP derivation pacing.
_STREAM_FORK_ROUNDS = 4
_STREAM_FORK_ROUND_DELAY = 5.0

# [serverless] Nightshift sources.  KI, significant-event and onboarding APIs address a
# source (an ES|QL view over a stream) by id.  Writes right after a source is created
# or changed answer 409 while Kibana holds its per-source lease; creating a source
# for a stream that does not resolve yet is rejected, so both are retried.
_SOURCES_PATH = "/internal/nightshift/sources"
_SOURCE_LEASE_ATTEMPTS = 18
_SOURCE_LEASE_DELAY = 10.0
_SOURCE_CREATE_WAIT = 300.0
_SOURCE_CREATE_POLL = 10.0

# Explicit k8s resource-attribute mappings for the scenario OTel log stream.
# Wired streams do not dynamically map nested resource.attributes.*; without
# these declarations the fields exist in _source but are invisible to Streams /
# Discover field caps (same set o11y-metrics puts on logs.otel.otel-demo).
_K8S_WIRED_FIELDS: dict[str, dict[str, str]] = {
    "resource.attributes.k8s.cluster.name": {"type": "keyword"},
    "resource.attributes.k8s.deployment.name": {"type": "keyword"},
    "resource.attributes.k8s.namespace.name": {"type": "keyword"},
    "resource.attributes.k8s.node.name": {"type": "keyword"},
    "resource.attributes.k8s.pod.ip": {"type": "ip"},
    "resource.attributes.k8s.pod.name": {"type": "keyword"},
    "resource.attributes.k8s.pod.start_time": {"type": "date"},
    "resource.attributes.k8s.pod.uid": {"type": "keyword"},
    "resource.attributes.k8s.replicaset.name": {"type": "keyword"},
}

# The fault's error code and telemetry frame dump live in log attributes, which the
# wired stream does not map either: ES|QL reads none of attributes.* without these.
_ERROR_WIRED_FIELDS: dict[str, dict[str, str]] = {
    "attributes.error.type": {"type": "keyword"},
    "attributes.exception.type": {"type": "keyword"},
    "attributes.exception.message": {"type": "match_only_text"},
    "attributes.exception.stacktrace": {"type": "match_only_text"},
}


def _strip_stream_runtime_keys(obj: Any) -> Any:
    """Drop server-owned keys (e.g. processing.updated_at) before PUT _ingest."""
    if isinstance(obj, dict):
        return {
            k: _strip_stream_runtime_keys(v)
            for k, v in obj.items()
            if k not in ("updated_at",)
        }
    if isinstance(obj, list):
        return [_strip_stream_runtime_keys(x) for x in obj]
    return obj


class StreamsMixin:

    @property
    def _stream_name(self) -> str:
        return f"logs.otel.{self.ns}"

    @property
    def _ecs_stream_name(self) -> str:
        return f"logs.ecs.{self.ns}"

    @property
    def _ecs_wired_stream(self) -> str:
        """Wired-stream ingest endpoint. All scenarios POST to `logs.ecs/_bulk`;
        the deployer then forks `logs.ecs` into per-scenario partitions."""
        return "logs.ecs"

    def _stream_exists(self, client: httpx.Client, stream_name: str | None = None) -> bool:
        """Return True if the given stream (default: scenario OTLP child) is present."""
        name = stream_name or self._stream_name
        resp = client.get(
            f"{self.kibana_url}/api/streams/{name}",
            headers=_kibana_headers(self.api_key),
        )
        return resp.status_code == 200

    def _fork_stream(
        self,
        client: httpx.Client,
        *,
        parent: str,
        child: str,
        filter_field: str,
    ) -> bool:
        """Fork a parent stream into a child partition. Retries with backoff."""
        if self._stream_exists(client, child):
            return True

        fork_body = {
            "where": {"field": filter_field, "eq": self.ns},
            "status": "enabled",
            "stream": {"name": child},
        }
        fork_url = f"{self.kibana_url}/api/streams/{parent}/_fork"
        label = f"fork {child} from {parent}"

        for round_idx in range(_STREAM_FORK_ROUNDS):
            if round_idx > 0:
                time.sleep(_STREAM_FORK_ROUND_DELAY)

            resp = _retry_http(
                lambda: client.post(
                    fork_url,
                    headers=_kibana_headers(self.api_key),
                    json=fork_body,
                ),
                label=label,
            )
            if resp is not None and resp.status_code < 300 and self._stream_exists(client, child):
                return True
            if resp is not None and resp.status_code >= 300:
                logger.warning(
                    "%s failed (HTTP %s, round %d/%d): %s",
                    label,
                    resp.status_code,
                    round_idx + 1,
                    _STREAM_FORK_ROUNDS,
                    resp.text[:500],
                )

        return self._stream_exists(client, child)

    def _create_stream(self, client: httpx.Client) -> bool:
        """Fork logs.otel into a scenario-specific child stream."""
        ok = self._fork_stream(
            client,
            parent="logs.otel",
            child=self._stream_name,
            filter_field="resource.attributes.service.namespace",
        )
        if ok:
            self._ensure_k8s_stream_fields(client, self._stream_name)
        return ok

    def _ensure_k8s_stream_fields(
        self, client: httpx.Client, stream_name: str | None = None
    ) -> bool:
        """Merge k8s resource-attribute and error-attribute field mappings onto a wired stream.

        Mirrors o11y-metrics' ``ingest.wired.fields`` declarations so pod /
        node / namespace attrs are queryable on the scenario log partition.
        Idempotent: existing fields are preserved; only missing k8s keys are
        added. Returns True on success (or if the stream is missing).
        """
        name = stream_name or self._stream_name
        resp = client.get(
            f"{self.kibana_url}/api/streams/{name}",
            headers=_kibana_headers(self.api_key),
        )
        if resp.status_code != 200:
            logger.warning(
                "Cannot map k8s fields on %s — stream GET returned HTTP %s",
                name, resp.status_code,
            )
            return False

        ingest = _strip_stream_runtime_keys(
            (resp.json().get("stream") or {}).get("ingest") or {}
        )
        if not isinstance(ingest, dict):
            ingest = {}
        wired = ingest.setdefault("wired", {})
        if not isinstance(wired, dict):
            wired = {}
            ingest["wired"] = wired
        fields = wired.setdefault("fields", {})
        if not isinstance(fields, dict):
            fields = {}
            wired["fields"] = fields

        changed = False
        for field_name, field_def in {**_K8S_WIRED_FIELDS, **_ERROR_WIRED_FIELDS}.items():
            if field_name not in fields:
                fields[field_name] = dict(field_def)
                changed = True

        if not changed:
            return True

        put = client.put(
            f"{self.kibana_url}/api/streams/{name}/_ingest",
            headers=_kibana_headers(self.api_key),
            json={"ingest": ingest},
        )
        if put.status_code >= 300:
            logger.warning(
                "Failed to map k8s fields on %s (HTTP %s): %s",
                name, put.status_code, put.text[:500],
            )
            return False
        logger.info(
            "Mapped %d k8s resource attributes on stream %s",
            len(_K8S_WIRED_FIELDS) + len(_ERROR_WIRED_FIELDS), name,
        )
        return True

    def _create_ecs_stream(self, client: httpx.Client) -> bool:
        """Fork logs.ecs into this scenario's partition."""
        return self._fork_stream(
            client,
            parent=self._ecs_wired_stream,
            child=self._ecs_stream_name,
            filter_field="service.namespace",
        )

    def _delete_ecs_stream(self, client: httpx.Client) -> bool:
        """Delete only this scenario's partition. The base wired stream
        `logs.ecs` is managed by Elastic and shared across all scenarios.

        Returns True if the partition is gone (or never existed); False if it
        is still present after retries.
        """
        # 1. Delete the partition Streams entity (mirrors logs.otel teardown).
        resp = _retry_http(
            lambda: client.delete(
                f"{self.kibana_url}/api/streams/{self._ecs_stream_name}",
                headers=_kibana_headers(self.api_key),
            ),
            label=f"delete ECS partition {self._ecs_stream_name}",
        )
        deleted_ok = resp is not None and resp.status_code in (200, 204, 404)
        if not deleted_ok and resp is not None:
            logger.warning(
                "Delete ECS partition stream %s returned HTTP %s after retries",
                self._ecs_stream_name, resp.status_code,
            )

        # 2. Delete this scenario's docs from the wired stream so co-deployed
        #    scenarios aren't affected.
        try:
            client.post(
                f"{self.elastic_url}/{self._ecs_wired_stream}/_delete_by_query",
                headers=_es_headers(self.api_key),
                params={"refresh": "false", "wait_for_completion": "false"},
                json={"query": {"term": {"service.namespace": self.ns}}},
            )
        except Exception as exc:
            logger.info("ECS docs delete-by-query skipped: %s", exc)

        return deleted_ok

    # ── Knowledge indicators ──────────────────────────────────────────────

    def _ki_now_and_expiry(self) -> tuple[str, str]:
        """Return (last_seen, expires_at) as UTC ISO-8601 strings.

        expires_at is set 1 year out so indicators persist for the life of the
        demo rather than the native 7-day TTL.
        """
        now = datetime.now(timezone.utc)
        expiry = now + timedelta(days=365)
        fmt = "%Y-%m-%dT%H:%M:%S.%f"[:-3] + "Z"  # milliseconds + Z suffix
        return now.strftime(fmt), expiry.strftime(fmt)

    def _build_knowledge_indicators(self) -> list[dict[str, Any]]:
        """Build the full set of Streams feature (knowledge indicator) objects
        for this scenario, derived from its service/host registry and the shared
        runtime metadata in scenario_engine.runtimes.RUNTIME_BY_LANGUAGE.

        Returns a list ready to POST as _bulk index operations.
        """
        now_iso, expiry_iso = self._ki_now_and_expiry()
        services = self.scenario.services
        hosts = self.scenario.hosts
        ns = self.ns

        features: list[dict[str, Any]] = []

        # ── 1. Schema — one static OTel indicator ───────────────────────────
        features.append({
            "id": "otel-schema",
            "type": "schema",
            "subtype": "otel",
            "title": "OpenTelemetry Schema",
            "description": (
                "Logs follow the OpenTelemetry semantic conventions with "
                "resource attributes, span IDs, and trace IDs."
            ),
            "properties": {"schema_family": "otel"},
            "confidence": 98,
            "evidence": [
                "resource.attributes.telemetry.sdk.name=opentelemetry",
                "span.id=...",
                "trace.id=...",
            ],
            "status": "active",
            "tags": ["schema", "otel"],
            "meta": {},
            "last_seen": now_iso,
            "expires_at": expiry_iso,
        })

        # ── 2. Entity — one per service ─────────────────────────────────────
        for svc_name, svc_cfg in services.items():
            language = svc_cfg.get("language", "python")
            rt = RUNTIME_BY_LANGUAGE.get(language, {})
            version = rt.get("version", "")
            process_rt_version = rt.get("process_runtime_version", version)
            display_lang = rt.get("display_name", language)
            cloud = svc_cfg.get("cloud_provider", "")
            subsystem = svc_cfg.get("subsystem", "")
            # "comms-array" → "Comms Array"
            svc_display = svc_name.replace("-", " ").title()
            description = (
                f"{svc_display} service in the {subsystem} subsystem, "
                f"implemented in {display_lang} {version}, "
                f"running on {cloud.upper()}."
            )
            evidence = [
                f"service.name={svc_name}",
                f"resource.attributes.telemetry.sdk.language={language}",
            ]
            if process_rt_version:
                evidence.append(
                    f"resource.attributes.process.runtime.version={process_rt_version}"
                )
            meta: dict[str, Any] = {"kubernetes_namespace": ns, "cloud_provider": cloud}
            if version:
                meta["runtime_version"] = version
            tags = ["entity", "service"]
            if language:
                tags.append(language)
            features.append({
                "id": svc_name,
                "type": "entity",
                "subtype": "service",
                "title": f"{svc_display} Service",
                "description": description,
                "properties": {"name": svc_name, "technology": language},
                "confidence": 95,
                "evidence": evidence,
                "status": "active",
                "tags": tags,
                "meta": meta,
                "last_seen": now_iso,
                "expires_at": expiry_iso,
                "filter": {"field": "service.name", "eq": svc_name},
            })

        # ── 3. Infrastructure — one cloud_deployment per distinct provider ───
        cloud_map: dict[str, dict[str, set]] = {}
        for svc_cfg in services.values():
            provider = svc_cfg.get("cloud_provider", "")
            if not provider:
                continue
            if provider not in cloud_map:
                cloud_map[provider] = {"regions": set(), "azs": set()}
            region = svc_cfg.get("cloud_region", "")
            az = svc_cfg.get("cloud_availability_zone", "")
            if region:
                cloud_map[provider]["regions"].add(region)
            if az:
                cloud_map[provider]["azs"].add(az)

        _CLOUD_DISPLAY = {"aws": "AWS", "gcp": "GCP", "azure": "Azure"}
        for provider, info in sorted(cloud_map.items()):
            display = _CLOUD_DISPLAY.get(provider, provider.upper())
            regions = sorted(info["regions"])
            azs = sorted(info["azs"])
            evidence = [f"resource.attributes.cloud.provider={provider}"]
            if regions:
                evidence.append(f"resource.attributes.cloud.region={regions[0]}")
            features.append({
                "id": f"{provider}-deployment",
                "type": "infrastructure",
                "subtype": "cloud_deployment",
                "title": display,
                "description": (
                    f"{display} cloud deployment observed across "
                    "multiple regions and availability zones."
                ),
                "properties": {"provider": provider},
                "confidence": 93,
                "evidence": evidence,
                "status": "active",
                "tags": ["infrastructure", "cloud", provider],
                "meta": {"regions": regions, "availability_zones": azs},
                "last_seen": now_iso,
                "expires_at": expiry_iso,
            })

        # ── 4. Infrastructure — one operating_system indicator per OS+arch ──
        os_seen: set[str] = set()
        for host in hosts:
            os_type = host.get("os.type", "linux")
            arch = host.get("host.arch", "amd64")
            key = f"{os_type}-{arch}"
            if key in os_seen:
                continue
            os_seen.add(key)
            features.append({
                "id": key,
                "type": "infrastructure",
                "subtype": "operating_system",
                "title": f"{os_type.title()} ({arch})",
                "description": (
                    f"Hosts running {os_type.title()} on {arch} architecture."
                ),
                "properties": {"os": os_type, "architecture": arch},
                "confidence": 90,
                "evidence": [
                    f"resource.attributes.host.architecture={arch}",
                    f"os.type={os_type}",
                ],
                "status": "active",
                "tags": ["infrastructure", "os", os_type],
                "meta": {},
                "last_seen": now_iso,
                "expires_at": expiry_iso,
            })
        # Fallback OS indicator if no hosts defined (telemetry always emits linux/amd64)
        if not os_seen:
            features.append({
                "id": "linux-amd64",
                "type": "infrastructure",
                "subtype": "operating_system",
                "title": "Linux (amd64)",
                "description": "Hosts running Linux on amd64 architecture.",
                "properties": {"os": "linux", "architecture": "amd64"},
                "confidence": 90,
                "evidence": [
                    "resource.attributes.host.architecture=amd64",
                    "os.type=linux",
                ],
                "status": "active",
                "tags": ["infrastructure", "os", "linux"],
                "meta": {},
                "last_seen": now_iso,
                "expires_at": expiry_iso,
            })

        # ── 5. Technology — one programming_language per distinct language ───
        lang_seen: set[str] = set()
        for svc_cfg in services.values():
            language = svc_cfg.get("language", "python")
            if language in lang_seen:
                continue
            lang_seen.add(language)
            rt = RUNTIME_BY_LANGUAGE.get(language, {})
            version = rt.get("version", "")
            runtime_name = rt.get("runtime_name", language)
            display_lang = rt.get("display_name", language)
            process_rt_version = rt.get("process_runtime_version", version)
            # Collect service names using this language for the description
            using_svcs = [
                s for s, c in services.items() if c.get("language") == language
            ]
            desc_svcs = ", ".join(using_svcs[:2])
            if len(using_svcs) > 2:
                desc_svcs += f" and {len(using_svcs) - 2} more"
            description = (
                f"{display_lang} programming language runtime version {version}, "
                f"used by {desc_svcs}."
            ) if version else (
                f"{display_lang} programming language used by {desc_svcs}."
            )
            evidence = [
                f"resource.attributes.telemetry.sdk.language={language}",
            ]
            if process_rt_version:
                evidence.append(
                    f"resource.attributes.process.runtime.version={process_rt_version}"
                )
            title = f"{display_lang} {version}".strip() if version else display_lang
            features.append({
                "id": f"{language}-{version}" if version else language,
                "type": "technology",
                "subtype": "programming_language",
                "title": title,
                "description": description,
                "properties": {"language": language, "version": version},
                "confidence": 92,
                "evidence": evidence,
                "status": "active",
                "tags": ["technology", "programming_language", language],
                "meta": {"runtime_name": runtime_name},
                "last_seen": now_iso,
                "expires_at": expiry_iso,
            })

        # Stamp stream_name. Kibana 9.5 features/_bulk rejects status, last_seen,
        # and uuid as excess write keys (server-owned / removed from write schema).
        # Upserts key off feature id within the stream.
        stream_name = self._stream_name
        for feat in features:
            feat["stream_name"] = stream_name
            feat.pop("status", None)
            feat.pop("last_seen", None)
            feat.pop("uuid", None)

        return features

    def _significant_events_available(self, client: httpx.Client) -> bool:
        """Probe whether significant events are enabled on the target cluster.

        Uses GET /internal/significant_events/availability which returns
        {"available": true} or {"available": false, "reason": "<gate>"}.
        Any error (network, 404 if the plugin isn't loaded) is treated as
        unavailable; any unexpected non-JSON response is fail-open so genuine
        errors still surface rather than being silently masked as skipped.

        Cached on first call per deploy so the probe is made at most once.
        """
        cached = getattr(self, "_se_available_cache", None)
        if cached is not None:
            return cached

        try:
            resp = client.get(
                f"{self.kibana_url}/internal/significant_events/availability",
                headers=_kibana_headers(self.api_key),
            )
            if resp.status_code == 404:
                # Plugin not loaded — treat as unavailable.
                result = False
                self._se_unavailable_reason = "plugin not loaded (HTTP 404)"
            else:
                data = resp.json()
                result = bool(data.get("available", True))
                if not result:
                    self._se_unavailable_reason = data.get("reason", "unknown gate")
        except Exception:
            result = True  # fail-open on unexpected errors
        self._se_available_cache = result  # type: ignore[attr-defined]
        return result

    # Shown when the availability probe says significant events are off.  The gate
    # differs per stack generation, so the hint does too.
    _SE_UNAVAILABLE_DETAIL_BY_GEN = {
        # [9.5] remove with 9.5 support
        "9.5": (
            "requires the observability:streamsEnableSignificantEvents uiSetting "
            "(set by the platform step or the Kibana cluster plan)"
        ),
        # [9.6] update as the Nightshift gates change
        "9.6": (
            "requires the nightshift.enabled feature flag (or streams.significantEventsAvailable "
            "on builds before kibana#292333)"
        ),
        # [serverless] flags are managed by Elastic; nothing to set from here
        "serverless": "the feature flags are managed by Elastic on serverless",
    }

    @property
    def _SE_UNAVAILABLE_DETAIL(self) -> str:
        gate = self._SE_UNAVAILABLE_DETAIL_BY_GEN[self._stack_generation()]
        return (
            f"Significant events not available in this environment — {gate}, and an "
            "Enterprise license. Run GET /internal/significant_events/availability "
            "to see which gate is failing (reason field)."
        )

    def _deploy_scenario_stream(self, client: httpx.Client, notify: ProgressCallback):
        """Step: (re)create the per-scenario stream partition.

        Always runs, regardless of whether significant events are available —
        later steps (data views, ECS backfill) depend on the stream existing.
        Reports its own ok/failed; never silently skipped.
        """
        step = self._step(StepIdx.STREAM_CREATE)
        step.status = "running"
        notify(self.progress)

        self._delete_stream(client)
        if self._create_stream(client):
            step.status = "ok"
            step.detail = f"Forked {self._stream_name} from logs.otel"
        else:
            step.detail = (
                f"Failed to fork {self._stream_name} from logs.otel after "
                f"{_STREAM_FORK_ROUNDS} attempts (namespace={self.ns}). "
                "Check Streams is enabled and OTLP data is flowing into logs.otel."
            )
            step.status = "failed"
        notify(self.progress)

    def _deploy_knowledge_indicators(self, client: httpx.Client, notify: ProgressCallback):
        """Step: populate knowledge indicators on the scenario stream.

        Stream creation is owned by _deploy_scenario_stream (the preceding step).
        This step only writes the features/_bulk payload.
        """
        step = self._step(StepIdx.KNOWLEDGE_INDICATORS)
        step.status = "running"
        notify(self.progress)

        if not self._significant_events_available(client):
            step.status = "skipped"
            reason = getattr(self, "_se_unavailable_reason", None)
            step.detail = self._SE_UNAVAILABLE_DETAIL + (f" Gate: {reason}." if reason else "")
            notify(self.progress)
            return

        features = self._build_knowledge_indicators()
        step.items_total = len(features)

        if features:
            if self._stack_generation() == "serverless":
                # [serverless] KIs belong to the stream's Nightshift source.
                source = self._ensure_source_serverless(client)
                if source is None:
                    step.status = "failed"
                    step.detail = (
                        f"Could not create a Nightshift source for {self._stream_name}"
                    )
                    notify(self.progress)
                    return
                for feat in features:
                    feat.pop("stream_name", None)
                resp = self._source_request(
                    client, "POST",
                    f"/internal/streams/{source['id']}/features/_bulk",
                    label=f"knowledge indicators on {self._stream_name}",
                    json={"operations": [{"index": {"feature": f}} for f in features]},
                )
            else:
                resp = client.post(
                    f"{self.kibana_url}/internal/streams/{self._stream_name}/features/_bulk",
                    headers=_kibana_headers(self.api_key),
                    json={"operations": [{"index": {"feature": f}} for f in features]},
                )
            if resp.status_code < 300:
                step.items_done = len(features)
                step.detail = (
                    f"Created {len(features)} knowledge indicators on {self._stream_name}"
                )
            elif resp.status_code == 403:
                # Backstop: availability changed between probe and write.
                step.status = "skipped"
                step.detail = self._SE_UNAVAILABLE_DETAIL
                notify(self.progress)
                return
            else:
                logger.warning(
                    "Knowledge indicators bulk create failed: %s", resp.text[:500]
                )
                step.detail = f"Bulk create failed (HTTP {resp.status_code})"

        step.status = "ok" if step.items_done > 0 else "failed"
        notify(self.progress)

    # ── Significant events ────────────────────────────────────────────────
    #
    # The creation API differs per stack generation (see _stack_generation):
    #   [9.5]        POST /api/streams/{stream}/queries/_bulk      (_create_se_queries_95)
    #   [9.6]        PUT  /api/streams/{child}/_query, one query stream per channel
    #                (_create_se_query_streams_96)
    #   [serverless] none: the Universal alert rules (deployer_alerting.py) replace them.
    #                The stream's Nightshift source still holds the knowledge indicators.
    # The dispatcher below owns the shared step reporting.

    def _deploy_significant_events(self, client: httpx.Client, notify: ProgressCallback):
        """Step: create the significant-event queries on the scenario stream."""
        step = self._step(StepIdx.SIGNIFICANT_EVENTS)
        step.status = "running"
        notify(self.progress)

        registry = self.scenario.channel_registry
        step.items_total = len(registry)

        gen = self._stack_generation()
        if gen == "9.5":
            self._create_se_queries_95(client, step, registry)
        elif gen == "serverless":
            # [serverless] The chaos-channel Universal alert rules (step 15) are the
            # only per-channel rules; a significant-event query per channel would
            # just mirror them.
            step.items_total = 0
            step.status = "skipped"
            step.detail = "Serverless uses the Universal alert rules (step 15) instead of significant-event queries"
        else:
            self._create_se_query_streams_96(client, step, registry)
        notify(self.progress)

    # ── [9.5] remove when 9.5 support is dropped ──────────────────────────

    def _create_se_queries_95(self, client: httpx.Client, step, registry) -> None:
        """[9.5] Bulk-create ES|QL queries with POST /api/streams/{stream}/queries/_bulk.

        Needs the observability:streamsEnableSignificantEvents uiSetting (set in
        _enable_gates_95 / the plan).  Queries target the scenario stream and its
        children directly.
        """
        operations = []
        for ch_num, ch_data in sorted(registry.items()):
            num_str = f"{int(ch_num):02d}"
            error_type = ch_data["error_type"]
            esql_query = (
                f"FROM {self._stream_name},{self._stream_name}.* METADATA _id, _source"
                f' | WHERE body.text LIKE "*{error_type}*" AND severity_text == "ERROR"'
            )
            operations.append({
                "index": {
                    "id": f"{self.ns}-se-ch{num_str}",
                    "title": f"{self.scenario.scenario_name}: SE CH {num_str}: {ch_data['name']}",
                    "description": f"{ch_data.get('subsystem', 'system')} — {error_type}",
                    "esql": {"query": esql_query},
                }
            })

        if not operations:
            step.status = "failed"
            step.detail = "No fault channels to create queries for"
            return

        resp = client.post(
            f"{self.kibana_url}/api/streams/{self._stream_name}/queries/_bulk",
            headers=_kibana_headers(self.api_key),
            json={"operations": operations},
        )
        if resp.status_code < 300:
            step.items_done = len(operations)
            step.status = "ok"
            step.detail = f"Created {len(operations)} stream queries on {self._stream_name}"
        elif resp.status_code == 403 and "not available in this environment" in resp.text:
            # Backstop: availability changed between probe and write.
            step.status = "skipped"
            step.detail = self._SE_UNAVAILABLE_DETAIL
        else:
            logger.warning("Significant events bulk create failed: %s", resp.text[:500])
            step.status = "failed"
            step.detail = f"Bulk create failed (HTTP {resp.status_code})"

    # ── [serverless] Nightshift sources ───────────────────────────────────

    def _source_request(
        self, client: httpx.Client, method: str, path: str, *, label: str, **kwargs: Any
    ) -> httpx.Response | None:
        """Call a Kibana /internal/ endpoint, retrying while the source lease is held (409)."""
        resp: httpx.Response | None = None
        for attempt in range(_SOURCE_LEASE_ATTEMPTS):
            try:
                resp = client.request(
                    method, f"{self.kibana_url}{path}",
                    headers=_kibana_headers(self.api_key), **kwargs,
                )
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                logger.warning("%s raised %s", label, exc.__class__.__name__)
                resp = None
            else:
                if resp.status_code != 409:
                    return resp
                logger.warning(
                    "%s: source busy (HTTP 409, attempt %d/%d)",
                    label, attempt + 1, _SOURCE_LEASE_ATTEMPTS,
                )
            if attempt < _SOURCE_LEASE_ATTEMPTS - 1:
                time.sleep(_SOURCE_LEASE_DELAY)
        return resp

    def _find_source_serverless(self, client: httpx.Client) -> dict[str, Any] | None:
        """Return the Nightshift source titled with this scenario's stream, if any."""
        page = 1
        while True:
            resp = self._source_request(
                client, "GET", f"{_SOURCES_PATH}?page={page}&per_page=100",
                label="list nightshift sources",
            )
            if resp is None or resp.status_code >= 300:
                return None
            data = resp.json()
            sources = data.get("sources", [])
            for source in sources:
                if source.get("title") == self._stream_name:
                    return source
            if not sources or page * 100 >= data.get("total", 0):
                return None
            page += 1

    def _ensure_source_serverless(self, client: httpx.Client) -> dict[str, Any] | None:
        """[serverless] Get or create the Nightshift source for the scenario stream.

        KI and significant-event APIs are keyed by source id.  Kibana starts an LLM
        onboarding run for every new source revision; the demo ships curated
        knowledge, so that run is cancelled.  Cached for the rest of the deploy.
        """
        cached = getattr(self, "_source_cache", None)
        if cached is not None:
            return cached

        source = self._find_source_serverless(client)
        created = False
        if source is None:
            body = {
                "title": self._stream_name,
                "description": f"{self.scenario.scenario_name} scenario logs",
                "tags": ["otel"],
                "esql": f"FROM {self._stream_name} METADATA _id, _source",
            }
            deadline = time.monotonic() + _SOURCE_CREATE_WAIT
            while True:
                resp = self._source_request(
                    client, "POST", _SOURCES_PATH, label="create nightshift source", json=body,
                )
                if resp is not None and resp.status_code < 300:
                    source = resp.json().get("source")
                    created = True
                    break
                if time.monotonic() >= deadline:
                    logger.warning(
                        "Nightshift source create failed for %s: %s", self._stream_name,
                        resp.text[:300] if resp is not None else "no response",
                    )
                    return None
                time.sleep(_SOURCE_CREATE_POLL)
        elif not source.get("enabled", True):
            self._source_request(
                client, "POST", f"{_SOURCES_PATH}/{source['id']}/_enable",
                label="enable nightshift source",
            )

        if source and created:
            self._cancel_source_onboarding(client, source)
        self._source_cache = source  # type: ignore[attr-defined]
        return source

    def _cancel_source_onboarding(self, client: httpx.Client, source: dict[str, Any]) -> None:
        """Cancel the automatic onboarding run Kibana schedules for a new source."""
        base = f"/internal/streams/{source['id']}/onboarding"
        for _ in range(30):
            resp = self._source_request(
                client, "GET", f"{base}/_status", label="nightshift onboarding status",
            )
            status = resp.json().get("status") if resp is not None and resp.status_code < 300 else None
            if status == "in_progress":
                self._source_request(
                    client, "POST", f"{base}/_execute",
                    label="cancel nightshift onboarding", json={"action": "cancel"},
                )
                return
            if status != "not_started":
                return
            time.sleep(3.0)

    def _delete_source_serverless(self, client: httpx.Client) -> None:
        """[serverless] Delete the stream's source; its KIs and queries go with it."""
        self._source_cache = None  # type: ignore[attr-defined]
        source = self._find_source_serverless(client)
        if source is None:
            return
        resp = self._source_request(
            client, "DELETE", f"{_SOURCES_PATH}/{source['id']}",
            label=f"delete nightshift source {self._stream_name}",
        )
        if resp is None or resp.status_code >= 300:
            logger.warning(
                "Failed to delete nightshift source %s: HTTP %s",
                self._stream_name, resp.status_code if resp is not None else "none",
            )

    # ── [9.6] / [serverless] update as 9.6 matures ────────────────────────

    def _create_se_query_streams_96(self, client: httpx.Client, step, registry) -> None:
        """[9.6] Create one ES|QL query stream per channel with PUT /api/streams/{child}/_query.

        Replaces the removed queries/_bulk endpoint.  Needs the uiSetting
        'observability:streamsEnableQueryStreams: true' in uiSettings.overrides in
        kibana.yml — it is readonly at runtime on 9.6, so the Cloud plan must set it.
        """
        failed = 0

        # Kibana 9.6 validates the ES|QL FROM clause against ES, but ES doesn't
        # natively understand the $.stream_name reference syntax. Create an ES alias
        # named exactly "$.stream_name" so the validation query resolves successfully.
        # This is a workaround for a Kibana 9.6 snapshot validation bug.
        alias_name = f"$.{self._stream_name}"
        self._ensure_dollar_stream_alias(alias_name)

        for ch_num, ch_data in sorted(registry.items()):
            num_str = f"{int(ch_num):02d}"
            error_type = ch_data["error_type"]
            esql_query = (
                f"FROM $.{self._stream_name} METADATA _id, _source"
                f' | WHERE body.text LIKE "*{error_type}*" AND severity_text == "ERROR"'
            )
            # Child stream name: <parent_stream>.<ns>-se-ch<num>
            # e.g. logs.otel.nova7.nova7-se-ch01
            child_name = f"{self._stream_name}.{self.ns}-se-ch{num_str}"

            resp = client.put(
                f"{self.kibana_url}/api/streams/{child_name}/_query",
                headers=_kibana_headers(self.api_key),
                json={"query": {"esql": esql_query}},
            )

            if resp.status_code < 300:
                step.items_done += 1
            elif resp.status_code == 422:
                # 'observability:streamsEnableQueryStreams' is false — it's a readonly
                # uiSetting that must be set via uiSettings.overrides in kibana.yml.
                step.status = "skipped"
                step.detail = (
                    "Query streams not enabled — add "
                    "'observability:streamsEnableQueryStreams: true' "
                    "to uiSettings.overrides in the Kibana cluster plan and redeploy."
                )
                return
            elif resp.status_code == 404:
                # API endpoint absent — older Kibana build without query stream support.
                step.status = "skipped"
                step.detail = "Query stream API not available on this Kibana build (HTTP 404)."
                return
            else:
                logger.warning(
                    "Query stream create failed for ch%s (HTTP %s): %s",
                    num_str, resp.status_code, resp.text[:300],
                )
                failed += 1

        if step.items_done > 0:
            step.status = "ok"
            step.detail = f"Created {step.items_done} query streams on {self._stream_name}"
            if failed:
                step.detail += f" ({failed} failed)"
        else:
            step.status = "failed"
            step.detail = f"All {len(registry)} query stream creates failed"

    def _ensure_dollar_stream_alias(self, alias_name: str) -> None:
        """[9.6] Create an ES alias named '$.stream_name' pointing to the parent stream.

        Kibana 9.6 validates query-stream ES|QL queries against ES, passing the
        literal '$.stream_name' FROM reference. ES doesn't natively understand
        this Kibana-level notation, so validation fails. Creating a real ES alias
        with that exact name lets the validation query resolve successfully.
        This is a workaround for a Kibana 9.6 snapshot validation bug.
        """
        stream_name = alias_name.lstrip("$.")
        try:
            resp = httpx.post(
                f"{self.elastic_url}/_aliases",
                headers=_es_headers(self.api_key),
                json={"actions": [{"add": {"index": stream_name, "alias": alias_name}}]},
                timeout=10.0,
            )
            if resp.status_code < 300:
                logger.debug("Created ES alias %s -> %s", alias_name, stream_name)
            else:
                logger.warning(
                    "Failed to create ES alias %s (HTTP %s): %s",
                    alias_name, resp.status_code, resp.text[:200],
                )
        except Exception as exc:
            logger.warning("ES alias creation raised: %s", exc)

    def _delete_stream(self, client: httpx.Client) -> bool:
        """Delete the scenario-specific stream (also removes its significant events).

        Returns True if the stream is gone (deleted or 404), False if still present.
        """
        if self._stack_generation() == "serverless":
            # [serverless] The source holds the KIs and queries and views this stream.
            self._delete_source_serverless(client)
        resp = _retry_http(
            lambda: client.delete(
                f"{self.kibana_url}/api/streams/{self._stream_name}",
                headers=_kibana_headers(self.api_key),
            ),
            label=f"delete stream {self._stream_name}",
        )
        if resp is None:
            return False
        if resp.status_code == 404 or resp.status_code < 300:
            return True
        logger.warning(
            "Failed to delete stream %s after retries: HTTP %s",
            self._stream_name, resp.status_code,
        )
        return False
