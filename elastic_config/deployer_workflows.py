"""WorkflowsMixin — workflow deploy and cleanup methods."""

from __future__ import annotations

import json
import logging
import os

import httpx

from elastic_config.deployer_base import (
    WORKFLOW_INDEX_SUFFIXES,
    ProgressCallback,
    StepIdx,
    _es_headers,
    _kibana_headers,
)

logger = logging.getLogger("deployer")


_DEFAULT_SONNET_CONNECTOR_ID = ".anthropic-claude-5-sonnet-chat_completion"
_DEFAULT_HAIKU_CONNECTOR_ID = "Anthropic-Claude-Haiku-4-5"


class WorkflowsMixin:

    def _deploy_workflows(self, client: httpx.Client, notify: ProgressCallback):
        step = self._step(StepIdx.WORKFLOWS)
        step.status = "running"
        notify(self.progress)

        # Clean existing workflows for this namespace
        self._cleanup_workflows(client)

        created = self._ensure_workflow_indices(client)
        if created:
            logger.info("Created %s workflow indices for %s", created, self.ns)

        sonnet_id, haiku_id = self._resolve_llm_connector_ids(client)
        workflow_yamls = self._generate_workflow_yamls(
            sonnet_connector_id=sonnet_id,
            haiku_connector_id=haiku_id,
        )
        step.items_total = len(workflow_yamls)

        for name, yaml_content in workflow_yamls.items():
            resp = self._wf_create(client, yaml_content)
            if resp.status_code < 300:
                # Extract workflow ID from response
                try:
                    wf_data = resp.json()
                    wf_id = wf_data.get("id", "")
                    if wf_id:
                        self._workflow_ids[name] = wf_id
                except Exception:
                    pass
                step.items_done += 1
                step.detail = f"Deployed: {name}"
            else:
                step.detail = f"Failed: {name} (HTTP {resp.status_code})"
                logger.warning("Workflow %s deploy failed: %s", name, resp.text[:200])
            notify(self.progress)

        step.status = "ok" if step.items_done > 0 else "failed"
        notify(self.progress)

    def _resolve_llm_connector_ids(
        self, client: httpx.Client
    ) -> tuple[str, str]:
        """Return (sonnet, haiku) connector ids for ai.agent steps.

        Prefers exact o11y-metrics names when present on the project; otherwise
        matches by substring. Falls back to the 9.5 Cloud defaults.
        """
        sonnet = _DEFAULT_SONNET_CONNECTOR_ID
        haiku = _DEFAULT_HAIKU_CONNECTOR_ID
        try:
            resp = client.get(
                f"{self.kibana_url}/api/actions/connectors",
                headers=_kibana_headers(self.api_key),
            )
            if resp.status_code >= 300:
                logger.warning(
                    "Could not list connectors for LLM pin (HTTP %s); using defaults",
                    resp.status_code,
                )
                return sonnet, haiku
            body = resp.json()
            if isinstance(body, list):
                connectors = body
            elif isinstance(body, dict):
                connectors = (
                    body.get("data")
                    or body.get("results")
                    or body.get("connectors")
                    or []
                )
            else:
                connectors = []
        except Exception as exc:
            logger.warning("Connector lookup failed (%s); using defaults", exc)
            return sonnet, haiku

        def _pick(needles: tuple[str, ...], default: str) -> str:
            for conn in connectors:
                cid = str(conn.get("id", ""))
                name = str(conn.get("name", ""))
                if cid == default or name == default:
                    return cid or default
            for conn in connectors:
                blob = f"{conn.get('id', '')} {conn.get('name', '')}".lower()
                if all(n in blob for n in needles):
                    cid = str(conn.get("id", ""))
                    if cid:
                        return cid
            return default

        sonnet = _pick(("claude", "sonnet"), _DEFAULT_SONNET_CONNECTOR_ID)
        haiku = _pick(("claude", "haiku"), _DEFAULT_HAIKU_CONNECTOR_ID)
        logger.info("RCA connectors: sonnet=%s haiku=%s", sonnet, haiku)
        return sonnet, haiku

    def _generate_workflow_yamls(
        self,
        sonnet_connector_id: str = _DEFAULT_SONNET_CONNECTOR_ID,
        haiku_connector_id: str = _DEFAULT_HAIKU_CONNECTOR_ID,
    ) -> dict[str, str]:
        """Generate workflow YAMLs templated for this scenario."""
        ns = self.ns
        scenario_name = self.scenario.scenario_name
        agent_cfg = self.scenario.agent_config
        agent_id = agent_cfg.get("id", f"{ns}-analyst")

        # Read template YAMLs from elastic_config/workflows/ and substitute
        wf_dir = os.path.join(os.path.dirname(__file__), "workflows")

        workflows = {}
        for fname in sorted(os.listdir(wf_dir)):
            if not fname.endswith(".yaml"):
                continue
            with open(os.path.join(wf_dir, fname)) as f:
                yaml_content = f.read()
            # Template substitutions
            yaml_content = yaml_content.replace("__SCENARIO_NAME__", scenario_name)
            yaml_content = yaml_content.replace("__AGENT_ID__", agent_id)
            yaml_content = yaml_content.replace("__NS__", ns)
            yaml_content = yaml_content.replace("__KIBANA_URL__", self.kibana_display_url)
            yaml_content = yaml_content.replace(
                "__SONNET_CONNECTOR_ID__", sonnet_connector_id
            )
            yaml_content = yaml_content.replace(
                "__HAIKU_CONNECTOR_ID__", haiku_connector_id
            )
            key = fname.replace(".yaml", "")
            workflows[key] = yaml_content

        return workflows

    def _ensure_workflow_indices(self, client: httpx.Client) -> int:
        """Create empty audit/queue indices so the Workflows editor can resolve them."""
        created = 0
        headers = _es_headers(self.api_key)
        body = {"settings": {"number_of_shards": 1, "number_of_replicas": 1}}
        for suffix in WORKFLOW_INDEX_SUFFIXES:
            index = f"{self.ns}-{suffix}"
            head = client.head(f"{self.elastic_url}/{index}", headers=headers)
            if head.status_code == 200:
                continue
            resp = client.put(f"{self.elastic_url}/{index}", headers=headers, json=body)
            if resp.status_code < 300:
                created += 1
            else:
                logger.warning(
                    "Failed to create workflow index %s: HTTP %s %s",
                    index,
                    resp.status_code,
                    resp.text[:200],
                )
        return created

    def _wf_create(self, client: httpx.Client, yaml_content: str) -> httpx.Response:
        """POST a single workflow. Tries new path first, falls back to old."""
        body = json.dumps({"yaml": yaml_content})
        resp = client.post(
            f"{self.kibana_url}/api/workflows/workflow",
            headers=_kibana_headers(self.api_key),
            content=body,
        )
        if resp.status_code in (404, 405):
            resp = client.post(
                f"{self.kibana_url}/api/workflows",
                headers=_kibana_headers(self.api_key),
                content=body,
            )
        return resp

    def _wf_search(self, client: httpx.Client) -> list:
        """Return all workflow items. Tries new GET path first, falls back to POST search."""
        resp = client.get(
            f"{self.kibana_url}/api/workflows",
            headers=_kibana_headers(self.api_key),
        )
        if resp.status_code in (404, 405):
            resp = client.post(
                f"{self.kibana_url}/api/workflows/search",
                headers=_kibana_headers(self.api_key),
                json={"page": 1, "size": 100},
            )
        if resp.status_code >= 300:
            return []
        data = resp.json()
        return data if isinstance(data, list) else data.get("results", data.get("items", []))

    def _wf_delete(self, client: httpx.Client, wf_id: str) -> httpx.Response:
        """Delete a workflow by ID. Tries new path first, falls back to old."""
        resp = client.delete(
            f"{self.kibana_url}/api/workflows/workflow/{wf_id}",
            headers=_kibana_headers(self.api_key),
        )
        if resp.status_code in (404, 405):
            resp = client.delete(
                f"{self.kibana_url}/api/workflows/{wf_id}",
                headers=_kibana_headers(self.api_key),
            )
        return resp

    def _cleanup_workflows(self, client: httpx.Client) -> int:
        """Delete workflows matching this scenario's name."""
        deleted = 0
        try:
            items = self._wf_search(client)
            scenario_name = self.scenario.scenario_name
            for item in items:
                if scenario_name in item.get("name", "") or f"{self.ns}-" in item.get("name", "").lower():
                    wf_id = item.get("id", "")
                    if wf_id:
                        r = self._wf_delete(client, wf_id)
                        if r.status_code < 300:
                            deleted += 1
        except Exception:
            pass
        return deleted
