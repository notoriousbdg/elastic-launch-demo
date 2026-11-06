"""WorkflowsMixin — workflow deploy and cleanup methods."""

from __future__ import annotations

import json
import logging
import os
import re

import httpx

from elastic_config.deployer_base import (
    WORKFLOW_INDEX_SUFFIXES,
    ProgressCallback,
    StepIdx,
    _es_headers,
    _kibana_headers,
    _retry_http,
)

logger = logging.getLogger("deployer")


_DEFAULT_SONNET_CONNECTOR_ID = ".anthropic-claude-5-sonnet-chat_completion"
_DEFAULT_HAIKU_CONNECTOR_ID = "Anthropic-Claude-Haiku-4-5"
_DEFAULT_EMAIL_CONNECTOR_ID = "elastic-cloud-email"

# `<name>_serverless.yaml` replaces `<name>.yaml` on serverless (see _generate_workflow_yamls).
SERVERLESS_SUFFIX = "_serverless"


def _apply_connector_pin(yaml_content: str, token: str, connector_id: str | None) -> str:
    """Substitute a connector-id placeholder, or drop the whole line when None.

    Dropping the line leaves the ai.agent step unpinned, so the workflow runs on
    whatever default LLM connector the deployment has configured — needed on
    on-prem stacks that have no Claude connectors.
    """
    if connector_id:
        return yaml_content.replace(token, connector_id)
    pattern = rf'^[ \t]*connector-id:[ \t]*"?{re.escape(token)}"?[ \t]*\r?\n'
    return re.sub(pattern, "", yaml_content, flags=re.MULTILINE)


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
        if getattr(self, "_workflow_index_failures", 0):
            logger.warning(
                "%s workflow indices could not be created for %s",
                self._workflow_index_failures, self.ns,
            )

        sonnet_id, haiku_id = self._resolve_llm_connector_ids(client)
        email_id = self._resolve_email_connector_id(client)
        if sonnet_id is None or haiku_id is None:
            step.detail = "LLM connector not found — workflows imported unpinned"
            notify(self.progress)
        workflow_yamls = self._generate_workflow_yamls(
            sonnet_connector_id=sonnet_id,
            haiku_connector_id=haiku_id,
            email_connector_id=email_id,
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

        # Library investigation workflows for the scenario's integrations (shared,
        # upserted by name; see deployer_workflow_templates.py).
        try:
            library_ok, library_failed = self._deploy_ootb_workflows(client)
        except Exception as exc:  # library workflows are best-effort; never abort the deploy
            logger.warning("Library workflow deploy failed: %s", exc, exc_info=True)
            library_ok, library_failed = 0, 1
        step.items_total += library_ok + library_failed
        step.items_done += library_ok
        if library_ok or library_failed:
            step.detail = (
                f"{step.items_done - library_ok} scenario + {library_ok} library workflows"
                + (f"; {library_failed} library failed" if library_failed else "")
            )

        step.status = "ok" if step.items_done > 0 else "failed"
        notify(self.progress)

    def _resolve_llm_connector_ids(
        self, client: httpx.Client
    ) -> tuple[str | None, str | None]:
        """Return (sonnet, haiku) connector ids for ai.agent steps.

        Prefers exact Cloud defaults when present; otherwise matches by substring
        (e.g. a renamed Claude connector on-prem).  Returns None for any connector
        that cannot be confirmed — the caller will import the workflow unpinned so
        it runs on whatever default LLM the deployment has configured, rather than
        pinning a connector that does not exist.
        """
        try:
            resp = _retry_http(
                lambda: client.get(
                    f"{self.kibana_url}/api/actions/connectors",
                    headers=_kibana_headers(self.api_key),
                ),
                label="connector list for LLM pin",
            )
            if resp is None or resp.status_code >= 300:
                logger.warning(
                    "Could not list connectors for LLM pin (HTTP %s); workflows will be imported unpinned",
                    resp.status_code if resp is not None else "network error",
                )
                return None, None
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
            logger.warning(
                "Connector lookup failed (%s); workflows will be imported unpinned", exc
            )
            return None, None

        def _pick(needles: tuple[str, ...], preferred: str) -> str | None:
            # Exact id or name match against the well-known Cloud default.
            for conn in connectors:
                cid = str(conn.get("id", ""))
                name = str(conn.get("name", ""))
                if cid == preferred or name == preferred:
                    return cid or preferred
            # Substring match — handles renamed connectors on-prem.
            for conn in connectors:
                blob = f"{conn.get('id', '')} {conn.get('name', '')}".lower()
                if all(n in blob for n in needles):
                    cid = str(conn.get("id", ""))
                    if cid:
                        return cid
            return None

        sonnet = _pick(("claude", "sonnet"), _DEFAULT_SONNET_CONNECTOR_ID)
        haiku = _pick(("claude", "haiku"), _DEFAULT_HAIKU_CONNECTOR_ID)
        if sonnet is None:
            logger.warning(
                "No Claude Sonnet connector found (expected '%s'); run_rca step will be imported unpinned",
                _DEFAULT_SONNET_CONNECTOR_ID,
            )
        if haiku is None:
            logger.warning(
                "No Claude Haiku connector found (expected '%s'); create_case_title step will be imported unpinned",
                _DEFAULT_HAIKU_CONNECTOR_ID,
            )
        logger.info(
            "RCA connectors: sonnet=%s haiku=%s",
            sonnet or "<unpinned>",
            haiku or "<unpinned>",
        )
        return sonnet, haiku

    def _resolve_email_connector_id(self, client: httpx.Client) -> str:
        """Return the email connector id for workflow email steps.

        ECH exposes a preconfigured connector with id ``elastic-cloud-email``.
        Serverless exposes a similar preconfigured connector but with a
        different id (e.g. ``Elastic-Cloud-SMTP``).  This method tries the
        ECH default first, then falls back to the first connector whose id or
        name contains 'smtp' or 'email', then returns the ECH default as a
        last resort (so ECH keeps working even if the connector list call fails).
        """
        try:
            resp = client.get(
                f"{self.kibana_url}/api/actions/connectors",
                headers=_kibana_headers(self.api_key),
            )
            if resp.status_code >= 300:
                logger.warning(
                    "Could not list connectors for email pin (HTTP %s); using default",
                    resp.status_code,
                )
                return _DEFAULT_EMAIL_CONNECTOR_ID
            body = resp.json()
            connectors = (
                body
                if isinstance(body, list)
                else body.get("data") or body.get("results") or body.get("connectors") or []
            )
        except Exception as exc:
            logger.warning("Email connector lookup failed (%s); using default", exc)
            return _DEFAULT_EMAIL_CONNECTOR_ID

        ids = {str(c.get("id", "")) for c in connectors}

        # Prefer the well-known ECH preconfigured connector when it exists.
        if _DEFAULT_EMAIL_CONNECTOR_ID in ids:
            return _DEFAULT_EMAIL_CONNECTOR_ID

        # Serverless / custom: match by id or name containing 'smtp' or 'email'.
        for c in connectors:
            cid = str(c.get("id", ""))
            blob = f"{cid} {c.get('name', '')}".lower()
            if "smtp" in blob or "email" in blob:
                logger.info("Email connector resolved to: %s", cid)
                return cid

        logger.info("No email connector found; falling back to %s", _DEFAULT_EMAIL_CONNECTOR_ID)
        return _DEFAULT_EMAIL_CONNECTOR_ID

    def _generate_workflow_yamls(
        self,
        sonnet_connector_id: str | None = None,
        haiku_connector_id: str | None = None,
        email_connector_id: str = _DEFAULT_EMAIL_CONNECTOR_ID,
    ) -> dict[str, str]:
        """Generate workflow YAMLs templated for this scenario.

        When sonnet_connector_id or haiku_connector_id is None the corresponding
        connector-id line is removed from the ai.agent step entirely, leaving the
        step unpinned so the workflow runs on whatever default LLM connector the
        deployment has configured.  This is the correct behaviour for on-prem
        stacks that do not have the Cloud-preconfigured Claude connectors.
        """
        ns = self.ns
        scenario_name = self.scenario.scenario_name
        agent_cfg = self.scenario.agent_config
        agent_id = agent_cfg.get("id", f"{ns}-analyst")

        # Read template YAMLs from elastic_config/workflows/ and substitute
        wf_dir = os.path.join(os.path.dirname(__file__), "workflows")

        # Serverless chaos rules are Universal (v2) rules started by action
        # policies, which hand the workflow inputs.payload instead of event.*.
        # Each `<name>_serverless.yaml` replaces `<name>.yaml` there (same
        # workflow key, so _workflow_ids lookups are unchanged); every other
        # generation deploys the classic file and ignores the variants.
        serverless = self._stack_generation() == "serverless"
        fnames = sorted(f for f in os.listdir(wf_dir) if f.endswith(".yaml"))
        variants = {f for f in fnames if f.endswith(SERVERLESS_SUFFIX + ".yaml")}
        replaced = {f.replace(SERVERLESS_SUFFIX, "") for f in variants}

        workflows = {}
        for fname in fnames:
            if serverless and fname in replaced:
                continue
            if not serverless and fname in variants:
                continue
            with open(os.path.join(wf_dir, fname)) as f:
                yaml_content = f.read()
            # Template substitutions
            yaml_content = yaml_content.replace("__SCENARIO_NAME__", scenario_name)
            yaml_content = yaml_content.replace("__AGENT_ID__", agent_id)
            yaml_content = yaml_content.replace("__NS__", ns)
            yaml_content = yaml_content.replace("__KIBANA_URL__", self.kibana_display_url)
            yaml_content = _apply_connector_pin(
                yaml_content, "__SONNET_CONNECTOR_ID__", sonnet_connector_id
            )
            yaml_content = _apply_connector_pin(
                yaml_content, "__HAIKU_CONNECTOR_ID__", haiku_connector_id
            )
            yaml_content = yaml_content.replace(
                "__EMAIL_CONNECTOR_ID__", email_connector_id
            )
            key = fname.replace(SERVERLESS_SUFFIX, "").replace(".yaml", "")
            workflows[key] = yaml_content

        return workflows

    def _ensure_workflow_indices(self, client: httpx.Client) -> int:
        """Create empty audit/queue indices so the Workflows editor can resolve them."""
        created = 0
        headers = _es_headers(self.api_key)
        # Shard and replica counts are ECH-only; serverless rejects them with HTTP 400,
        # so send no settings there and let the project use its own.
        body = {} if self._is_serverless() else {
            "settings": {"number_of_shards": 1, "number_of_replicas": 1}
        }
        failed = 0
        for suffix in WORKFLOW_INDEX_SUFFIXES:
            index = f"{self.ns}-{suffix}"
            head = client.head(f"{self.elastic_url}/{index}", headers=headers)
            if head.status_code == 200:
                continue
            resp = client.put(f"{self.elastic_url}/{index}", headers=headers, json=body)
            if resp.status_code < 300:
                created += 1
            else:
                failed += 1
                logger.warning(
                    "Failed to create workflow index %s: HTTP %s %s",
                    index,
                    resp.status_code,
                    resp.text[:200],
                )
        self._workflow_index_failures = failed
        return created

    def _wf_create(self, client: httpx.Client, yaml_content: str) -> httpx.Response:
        return client.post(
            f"{self.kibana_url}/api/workflows/workflow",
            headers=_kibana_headers(self.api_key),
            content=json.dumps({"yaml": yaml_content}),
        )

    def _wf_search(self, client: httpx.Client) -> list:
        resp = client.get(
            f"{self.kibana_url}/api/workflows",
            headers=_kibana_headers(self.api_key),
        )
        if resp.status_code >= 300:
            return []
        data = resp.json()
        return data if isinstance(data, list) else data.get("results", data.get("items", []))

    def _wf_delete(self, client: httpx.Client, wf_id: str) -> httpx.Response:
        return client.delete(
            f"{self.kibana_url}/api/workflows/workflow/{wf_id}",
            headers=_kibana_headers(self.api_key),
        )

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
