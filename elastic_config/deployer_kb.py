"""KbMixin — knowledge base deploy methods."""

from __future__ import annotations

import logging
import random
import re
from typing import Any

import httpx

from scenario_engine.base import _substitute_fault_params
from elastic_config.deployer_base import (
    ConcurrentBulkIndexer,
    ProgressCallback,
    StepIdx,
    _es_headers,
    _kibana_headers,
    _retry_http,
)

logger = logging.getLogger("deployer")

# [serverless] Nightshift reads its knowledge from Wiki pages, not the KB index.
_WIKI_PAGES_PATH = "/internal/nightshift/wiki/pages"
# Every scenario's spans land here (generators/traces), whatever the namespace.
_TRACES_INDEX = "traces-generic.otel-default"


class _AlwaysRng(random.Random):
    """random() of 0 so get_correlation_attribute always returns its attribute."""

    def random(self) -> float:
        return 0.0

# [serverless] Project-wide snippets Nightshift adds to every investigation prompt.
_CUSTOM_CONTEXT_PATH = "/internal/nightshift/custom_context"


class KbMixin:

    def _deploy_knowledge_base(self, client: httpx.Client, notify: ProgressCallback):
        step = self._step(StepIdx.KNOWLEDGE_BASE)
        step.status = "running"
        notify(self.progress)

        kb_index = f"{self.ns}-knowledge-base"
        registry = self.scenario.channel_registry

        # Delete and recreate index
        client.delete(
            f"{self.elastic_url}/{kb_index}",
            headers=_es_headers(self.api_key),
        )
        kb_body = {
            "settings": {"number_of_shards": 1, "number_of_replicas": 1},
            "mappings": {
                "properties": {
                    "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "content": {"type": "text"},
                    "category": {"type": "keyword"},
                    "tags": {"type": "keyword"},
                    "channel_number": {"type": "integer"},
                    "error_type": {"type": "keyword"},
                    "subsystem": {"type": "keyword"},
                    "affected_services": {"type": "keyword"},
                }
            },
        }
        if self._is_serverless():
            # Shard and replica counts are ECH-only; serverless rejects them with HTTP 400.
            kb_body.pop("settings")
        resp = client.put(
            f"{self.elastic_url}/{kb_index}",
            headers=_es_headers(self.api_key),
            json=kb_body,
        )
        if resp.status_code >= 300:
            logger.warning(
                "Failed to create knowledge base index %s: HTTP %s %s",
                kb_index, resp.status_code, resp.text[:200],
            )

        indexed = 0
        if registry:
            with ConcurrentBulkIndexer(
                self.elastic_url,
                self.api_key,
                "",
                bulk_path="_bulk?refresh=true",
                label="knowledge-base",
                workers=1,
            ) as indexer:
                for ch_num, ch_data in sorted(registry.items()):
                    doc_id = f"ch{int(ch_num):02d}-{ch_data['error_type'].lower()}"
                    doc = {
                        "title": f"Channel {ch_num}: {ch_data['name']}",
                        "content": self._generate_kb_doc(ch_num, ch_data),
                        "category": "anomaly-rca",
                        "tags": [self.ns, ch_data["error_type"]],
                        "channel_number": int(ch_num),
                        "error_type": ch_data["error_type"],
                        "subsystem": ch_data.get("subsystem", ""),
                        "affected_services": ch_data.get("affected_services", []),
                    }
                    indexer.add(doc, meta={"index": {"_index": kb_index, "_id": doc_id}})
                indexed = indexer.flush()
            if indexed:
                step.items_done = indexed
                step.detail = f"Indexed {indexed} KB documents"
            else:
                step.detail = "Bulk index failed"
        else:
            step.detail = "No KB documents to index"

        step.status = "ok" if step.items_done > 0 else "failed"
        step.items_total = len(registry)
        if registry and self._is_serverless():
            # Same knowledge for Nightshift; the KB index above stays for the agent flow.
            written = self._deploy_wiki_runbooks(client, registry)
            step.detail += f"; {written}/{len(registry)} Wiki runbooks"
            if self._set_nightshift_context(client, self._nightshift_context_text()):
                step.detail += "; Nightshift context set"
        notify(self.progress)

    # ── [serverless] Nightshift Wiki runbooks ───────────────────────────

    # ── [serverless] Nightshift custom context ────────────────────────────
    # One snippet per scenario: several scenarios can share a project, and the
    # list also holds snippets users add by hand, so only ours is ever replaced.

    def _nightshift_context_marker(self) -> str:
        return f"[launch-next:{self.ns}]"

    def _nightshift_context_text(self) -> str:
        sample = next(iter(sorted(self.scenario.channel_registry.items())), (0, {}))[1]
        example = f" (e.g. {sample['error_type']})" if sample.get("error_type") else ""
        return (
            f"{self._nightshift_context_marker()} Demo scenario: {self.scenario.scenario_name} "
            f"(namespace {self.ns}). Applies to alerts and errors from stream "
            f"{self._stream_name} only.\n"
            f'- Query telemetry with the "Nightshift sandbox: this project" External '
            f"Elasticsearch connector.\n"
            f"- Scenario logs: FROM {self._stream_name},{self._stream_name}.*. Fault error "
            f"codes{example} are in body.text, severity in severity_text, and the service in "
            f"resource.attributes.service.name.\n"
            f"- Each fault channel has a Wiki runbook (wiki_runbook_{self.ns}-chNN-*) with "
            f"the error signature, affected and cascade services, investigation steps and "
            f"remediation action."
        )

    def _set_nightshift_context(self, client: httpx.Client, text: str | None) -> bool:
        """Replace this scenario's snippet with ``text`` (None removes it).  Returns success.

        PUT replaces the whole list, so it is a read-modify-write; the write is read
        back once in case another scenario's deploy raced it.
        """
        url = f"{self.kibana_url}{_CUSTOM_CONTEXT_PATH}"
        headers = _kibana_headers(self.api_key)
        marker = self._nightshift_context_marker()
        for _ in range(2):
            resp = _retry_http(lambda: client.get(url, headers=headers), label="get custom context")
            if resp is None or resp.status_code >= 300:
                return False
            current = resp.json()
            snippets = [
                s for s in current.get("snippets", [])
                if not str(s.get("text", "")).startswith(marker)
            ]
            if text is not None:
                snippets.append({"text": text})
            body: dict[str, Any] = {"snippets": snippets}
            if current.get("version") is not None:
                body["version"] = current["version"]
            resp = _retry_http(
                lambda: client.put(url, headers=headers, json=body), label="put custom context",
            )
            if resp is None or resp.status_code >= 300:
                logger.warning(
                    "Nightshift custom context update failed: %s",
                    f"HTTP {resp.status_code} {resp.text[:300]}" if resp is not None else "no response",
                )
                return False
            check = client.get(url, headers=headers)
            if check.status_code >= 300:
                return True
            ours = [
                s for s in check.json().get("snippets", [])
                if str(s.get("text", "")).startswith(marker)
            ]
            if (text is None and not ours) or (text is not None and len(ours) == 1):
                return True
        return False

    def _wiki_slug(self, ch_num: int, ch_data: dict[str, Any]) -> str:
        error_type = re.sub(r"[^a-z0-9]+", "-", ch_data["error_type"].lower()).strip("-")
        return f"{self.ns}-ch{int(ch_num):02d}-{error_type}"

    def _wiki_page_ids(self) -> list[str]:
        """Wiki page ids for every channel runbook this scenario writes."""
        registry = self.scenario.channel_registry or {}
        return [
            f"wiki_runbook_{self._wiki_slug(ch_num, ch_data)}"
            for ch_num, ch_data in sorted(registry.items())
        ]

    def _deploy_wiki_runbooks(self, client: httpx.Client, registry: dict) -> int:
        """Create or update one Wiki runbook page per channel.  Returns pages written."""
        written = 0
        for ch_num, ch_data in sorted(registry.items()):
            slug = self._wiki_slug(ch_num, ch_data)
            page_id = f"wiki_runbook_{slug}"
            description = ch_data.get("description", "").strip().split(". ")[0].rstrip(".")
            body: dict[str, Any] = {
                "entity_type": "runbook",
                "slug": slug,
                "title": f"Runbook: {self.scenario.scenario_name} channel {int(ch_num):02d}: {ch_data['name']}",
                "description": description or f"{ch_data.get('subsystem', 'system')}: {ch_data['error_type']}",
                "content": self._generate_wiki_runbook(ch_num, ch_data),
                "status": "established",
            }
            existing = _retry_http(
                lambda: client.get(
                    f"{self.kibana_url}{_WIKI_PAGES_PATH}/{page_id}",
                    headers=_kibana_headers(self.api_key),
                ),
                label=f"get wiki page {page_id}",
            )
            if existing is not None and existing.status_code < 300:
                body["version"] = existing.json().get("page", {}).get("version")
                method = "PUT"
            else:
                method = "POST"
            resp = _retry_http(
                lambda: client.request(
                    method,
                    f"{self.kibana_url}{_WIKI_PAGES_PATH}",
                    headers=_kibana_headers(self.api_key),
                    json=body,
                ),
                label=f"{method} wiki page {page_id}",
            )
            if resp is not None and resp.status_code < 300:
                written += 1
            else:
                logger.warning(
                    "Failed to write Wiki runbook %s: %s", page_id,
                    f"HTTP {resp.status_code} {resp.text[:300]}" if resp is not None else "no response",
                )
        return written

    def _cleanup_wiki_runbooks(self, client: httpx.Client) -> int:
        """[serverless] Delete this scenario's Wiki runbooks.  Returns pages deleted.

        Wiki DELETE archives the page (status "archived") rather than removing it,
        and needs the current version.  A redeploy PUTs it back to "established".
        """
        deleted = 0
        for page_id in self._wiki_page_ids():
            existing = _retry_http(
                lambda: client.get(
                    f"{self.kibana_url}{_WIKI_PAGES_PATH}/{page_id}",
                    headers=_kibana_headers(self.api_key),
                ),
                label=f"get wiki page {page_id}",
            )
            if existing is None or existing.status_code >= 300:
                continue
            page = existing.json().get("page", {})
            if page.get("status") == "archived":
                continue
            resp = _retry_http(
                lambda: client.delete(
                    f"{self.kibana_url}{_WIKI_PAGES_PATH}/{page_id}",
                    headers=_kibana_headers(self.api_key),
                    params={"version": page.get("version")},
                ),
                label=f"delete wiki page {page_id}",
            )
            if resp is not None and resp.status_code < 300:
                deleted += 1
        return deleted

    def _generate_wiki_runbook(self, ch_num: int, ch_data: dict[str, Any]) -> str:
        """Wiki form of _generate_kb_doc: same facts, no Agent Builder tool or skill names."""
        error_type = ch_data["error_type"]
        subsystem = ch_data.get("subsystem", "unknown")
        affected = ", ".join(ch_data.get("affected_services", [])) or "none listed"
        cascade = ", ".join(ch_data.get("cascade_services", [])) or "none listed"
        description = ch_data.get("description", "").strip()
        remediation_action = ch_data.get("remediation_action", "remediate")
        investigation_notes = _substitute_fault_params(
            ch_data.get("investigation_notes", ""), self.scenario.fault_param_ranges(int(ch_num))
        ).strip()
        investigation_section = ""
        if investigation_notes:
            investigation_section = f"""
## Vendor-specific investigation

{investigation_notes}
"""

        return f"""## Overview

{self.scenario.scenario_name} fault channel {ch_num}: {ch_data['name']} ({subsystem} subsystem).

{description}

## Established facts

- Error signature: `{error_type}` appears in the log body (`body.text`) of ERROR logs on the `{self._stream_name}` stream. Search `body.text`, not `body`.
- Affected services: {affected}.
- Cascade services: {cascade}.
- Investigate by checking the health of the affected services, following the fault to the cascade services, and looking for correlated errors in the same time window.
- Root cause indicators: other channels in the {subsystem} subsystem failing at the same time, and errors that line up with infrastructure events.
- Remediation: run the `{remediation_action}` action for channel {ch_num}.
- Recovery is confirmed when the error and warning counts for `{error_type}` drop to zero or near zero. Errors from before the fix stay visible in 15-minute windows for several minutes and do not mean the fault is still active.
{investigation_section}
{self._evidence_section(ch_num, ch_data)}
## Open questions

- Add a note here when a new symptom is observed.
"""

    def _evidence_section(self, ch_num: int, ch_data: dict[str, Any]) -> str:
        """Where this channel's root-cause evidence lives in the generated telemetry.

        Shared by the KB doc and the Wiki runbook: without it, investigations look
        for the hardware readings named in the notes, find no such fields and stop at
        low confidence, although the generators emit them as span attributes.
        """
        services = list(dict.fromkeys(
            ch_data.get("affected_services", []) + ch_data.get("cascade_services", [])
        ))
        clue_lines = []
        example = None
        for svc in services:
            clues = self.scenario.rca_clue_ranges(int(ch_num), svc)
            if clues:
                clue_lines.append(
                    f"- {svc}: " + ", ".join(f"`attributes.{k}` ({v})" for k, v in clues.items())
                )
                example = example or (svc, next(iter(clues)))
        correlation = self.scenario.get_correlation_attribute(int(ch_num), True, _AlwaysRng())

        lines = ["## Evidence in this environment", ""]
        if example:
            svc, field = example
            lines += [
                f"Span attributes in `{_TRACES_INDEX}` that the services report only while this "
                "fault is active, with the values they take then. They are the readings the "
                "investigation notes refer to; compare them with the nominal values and thresholds there:",
                *clue_lines,
                "",
                f"Example: `FROM {_TRACES_INDEX} | WHERE resource.attributes.service.name == "
                f'"{svc}" AND attributes.{field} IS NOT NULL | STATS '
                f"AVG(attributes.{field}) BY BUCKET(@timestamp, 5 minute)`",
                "",
            ]
        if correlation:
            (key, value), = correlation.items()
            lines += [
                f"- Correlated attribute: `attributes.{key}` = `{value}` is on about 90% of error "
                "spans while this fault is active and about 5% of healthy spans.",
            ]
        lines += [
            "- The ERROR log lines carry `attributes.error.type`, `attributes.exception.message` "
            "and `attributes.exception.stacktrace` (a telemetry frame dump with the measured and "
            "commanded values). Join logs to spans on `trace_id`.",
            "- These fields are the confirming evidence for this channel: when they are present "
            "and outside the thresholds, the cause in the investigation notes is confirmed.",
        ]
        return "\n".join(lines) + "\n"

    def _generate_kb_doc(self, ch_num: int, ch_data: dict[str, Any]) -> str:
        """Generate a knowledge base document for a fault channel."""
        name = ch_data["name"]
        error_type = ch_data["error_type"]
        subsystem = ch_data.get("subsystem", "unknown")
        affected = ", ".join(ch_data.get("affected_services", []))
        cascade = ", ".join(ch_data.get("cascade_services", []))
        description = ch_data.get("description", "")

        remediation_action = ch_data.get("remediation_action", "remediate")
        ch_int = int(ch_num)
        approval_mode = "required" if ch_int <= 15 else "skip"
        investigation_notes = _substitute_fault_params(
            ch_data.get("investigation_notes", ""), self.scenario.fault_param_ranges(int(ch_num))
        )
        investigation_section = ""
        if investigation_notes:
            investigation_section = f"""
## Vendor-Specific Investigation
{investigation_notes}
"""

        return f"""# Channel {ch_num}: {name}

## Error Signature
- **Error Type**: `{error_type}`
- **Subsystem**: {subsystem}
- **Affected Services**: {affected}
- **Cascade Services**: {cascade}

## Description
{description}

## Investigation Procedure
1. Search for `{error_type}` in recent ERROR logs using `{self.scenario.prefixed_tool_id("search_error_logs")}` — this identifier appears in the log body (body.text)
2. Check health of affected services: {affected}
3. Trace anomaly propagation to cascade services: {cascade}
4. Check for correlated errors in the same time window
{investigation_section}
{self._evidence_section(ch_num, ch_data)}
## Root Cause Indicators
- Look for `{error_type}` entries in body.text (this is the indexed field — do NOT search body alone)
- Check if multiple channels in the {subsystem} subsystem are affected
- Verify if errors correlate with infrastructure events

## Remediation
Consult the remediation guide skill (`{self.scenario.namespace}-remediation-guide`) for full decision guidance, then use `{self.scenario.prefixed_tool_id("remediation_action")}` with action_type: {remediation_action}, channel: {ch_num}, approval_mode: {approval_mode}, dry_run: false, and a justification. Channels 1–15 must use approval_mode: required (the workflow includes a human-in-the-loop step that requests operator approval through waitForInput; relay that request and resume with the reply you receive). Only pass approval_mode: skip when the Auto-Remediate notification workflow explicitly requests it. Once the tool returns successfully, remediation has been executed, but do not declare the incident resolved yet.

## Confirming Recovery
Consult the remediation guide skill (`{self.scenario.namespace}-remediation-guide`), then wait briefly and use `{self.scenario.prefixed_tool_id("verify_recovery")}` to confirm the fix propagated. Zero (or near-zero) error/warn counts means the incident is resolved. Do NOT rely on the 15-minute tools in the triage and investigation skills to judge current status immediately after remediation — they will keep showing the pre-fix errors as residual history for several minutes, which does not mean the fault is still active.
"""
