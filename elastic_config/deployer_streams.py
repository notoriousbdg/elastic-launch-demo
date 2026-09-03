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
        return self._fork_stream(
            client,
            parent="logs.otel",
            child=self._stream_name,
            filter_field="resource.attributes.service.namespace",
        )

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

        A 403 carrying "not available in this environment" is the definitive
        "feature disabled" signal — distinct from a privilege 403 or any other
        error.  Any non-403 status is treated as available (fail-open) so genuine
        write errors still surface rather than being silently masked as skipped.

        Cached on first call per deploy so the probe is made at most once.
        """
        cached = getattr(self, "_se_available_cache", None)
        if cached is not None:
            return cached

        try:
            resp = client.get(
                f"{self.kibana_url}/api/streams/{self._stream_name}/queries",
                headers=_kibana_headers(self.api_key),
            )
            if resp.status_code == 403 and "not available in this environment" in resp.text:
                result = False
            else:
                result = True
        except Exception:
            result = True  # fail-open
        self._se_available_cache = result  # type: ignore[attr-defined]
        return result

    _SE_UNAVAILABLE_DETAIL = (
        "Significant events not available in this environment — "
        "requires the streams.significantEventsAvailable feature flag "
        "(enabled Elastic-side; not configurable on serverless)."
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
            step.detail = self._SE_UNAVAILABLE_DETAIL
            notify(self.progress)
            return

        features = self._build_knowledge_indicators()
        step.items_total = len(features)

        if features:
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
            elif resp.status_code == 403 and "not available in this environment" in resp.text:
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

    def _deploy_significant_events(self, client: httpx.Client, notify: ProgressCallback):
        """Step: create ES|QL significant-event queries on the scenario stream.

        Assumes the stream already exists (_deploy_scenario_stream runs first).
        Skipped when the streams.significantEventsAvailable feature flag is off.
        """
        step = self._step(StepIdx.SIGNIFICANT_EVENTS)
        step.status = "running"
        notify(self.progress)

        if not self._significant_events_available(client):
            step.status = "skipped"
            step.detail = self._SE_UNAVAILABLE_DETAIL
            notify(self.progress)
            return

        # Build bulk operations — one ES|QL query per fault channel
        operations = []
        registry = self.scenario.channel_registry
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

        step.items_total = len(operations)

        if operations:
            resp = client.post(
                f"{self.kibana_url}/api/streams/{self._stream_name}/queries/_bulk",
                headers=_kibana_headers(self.api_key),
                json={"operations": operations},
            )
            if resp.status_code < 300:
                step.items_done = len(operations)
                step.detail = f"Created {len(operations)} stream queries on {self._stream_name}"
            elif resp.status_code == 403 and "not available in this environment" in resp.text:
                # Backstop: availability changed between probe and write.
                step.status = "skipped"
                step.detail = self._SE_UNAVAILABLE_DETAIL
                notify(self.progress)
                return
            else:
                logger.warning("Significant events bulk create failed: %s", resp.text[:500])
                step.detail = f"Bulk create failed (HTTP {resp.status_code})"

        step.status = "ok" if step.items_done > 0 else "failed"
        notify(self.progress)

    def _delete_stream(self, client: httpx.Client) -> bool:
        """Delete the scenario-specific stream (also removes its significant events).

        Returns True if the stream is gone (deleted or 404), False if still present.
        """
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
