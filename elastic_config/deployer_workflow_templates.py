"""WorkflowTemplatesMixin — library investigation workflows for the scenario's integrations.

Step 6 deploys every vendored Template Library workflow whose evidence data an
enabled generator writes (``elastic_config/workflow_templates.py``); step 21 puts a
``.workflows`` action on each OOTB package rule that should start one.

The workflows follow the OOTB rules' lifecycle: they are cluster-shared, found by
their template name, upserted on every deploy and not deleted on teardown. Rules of
packages the active scenario does not install are disabled in step 21, so their
workflows simply stop being triggered. The scenario's own chaos-channel rules and
their RCA workflows are not affected.
"""

from __future__ import annotations

import importlib
import json
import logging
from typing import Any

import httpx

from elastic_config.deployer_base import _kibana_headers, _retry_http
from elastic_config.workflow_templates import (
    LibraryWorkflow,
    deployable_yaml,
    eligibility,
    load_library,
    rule_wires_workflow,
    workflow_action,
)

logger = logging.getLogger("deployer")


class WorkflowTemplatesMixin:

    def _enabled_generator_specs(self) -> list:
        from generator_engine.registry import enabled_specs

        return list(enabled_specs(self.scenario))

    def _enabled_datasets(self) -> set[str]:
        return {ds for spec in self._enabled_generator_specs() for ds in spec.datasets}

    def _template_stack_version(self) -> str | None:
        """Stack version for template ``availability`` (None on serverless: always current)."""
        env, version = getattr(self, "_kibana_env", ("", ""))
        if env == "serverless" or not version:
            return None
        return version.split("-")[0]

    def _eligible_library_workflows(self) -> list[LibraryWorkflow]:
        datasets = self._enabled_datasets()
        version = self._template_stack_version()
        selected = []
        for wf in load_library():
            ok, reason = eligibility(wf, datasets, version)
            if ok:
                selected.append(wf)
            elif wf.evidence_step:
                logger.info("Library workflow %s not deployed: %s", wf.slug, reason)
        return selected

    def _workflow_template_input_values(self, wf: LibraryWorkflow) -> dict[str, str]:
        """Generator-provided resource ids, then the scenario's flat and per-slug values."""
        values: dict[str, str] = {}
        for spec in self._enabled_generator_specs():
            try:
                module = importlib.import_module(spec.module)
            except Exception:
                continue
            provide = getattr(module, "workflow_input_values", None)
            if not callable(provide):
                continue
            scenario_data = {
                "namespace": self.scenario.namespace,
                "generator_config": self.scenario.get_generator_config(spec.id),
            }
            try:
                values.update({k: str(v) for k, v in (provide(scenario_data) or {}).items() if v})
            except Exception as exc:
                logger.warning("workflow_input_values of %s failed: %s", spec.id, exc)
        configured = getattr(self.scenario, "workflow_template_inputs", {}) or {}
        values.update({k: str(v) for k, v in configured.items() if not isinstance(v, dict)})
        values.update({k: str(v) for k, v in (configured.get(wf.slug) or {}).items()})
        return values

    def _deploy_ootb_workflows(self, client: httpx.Client) -> tuple[int, int]:
        """Upsert the eligible library workflows; return ``(deployed, failed)``."""
        self._ootb_workflow_ids: dict[str, str] = {}
        self._ootb_workflows: list[LibraryWorkflow] = []
        selected = self._eligible_library_workflows()
        if not selected:
            return 0, 0

        headers = _kibana_headers(self.api_key)
        existing = {item.get("name"): item.get("id") for item in self._wf_search(client) if item.get("id")}
        deployed = failed = 0
        for wf in selected:
            text = deployable_yaml(wf, self._workflow_template_input_values(wf))
            wf_id = existing.get(wf.name)
            if wf_id:
                resp = _retry_http(
                    lambda i=wf_id, t=text: client.put(
                        f"{self.kibana_url}/api/workflows/workflow/{i}",
                        headers=headers,
                        content=json.dumps({"yaml": t, "enabled": True}),
                    ),
                    label=f"update library workflow {wf.slug}",
                    attempts=3,
                )
            else:
                resp = _retry_http(
                    lambda t=text: self._wf_create(client, t),
                    label=f"create library workflow {wf.slug}",
                    attempts=2,
                )
                if resp is not None and resp.status_code < 300:
                    try:
                        wf_id = resp.json().get("id") or ""
                    except Exception:
                        wf_id = ""
            if resp is None or resp.status_code >= 300 or not wf_id:
                failed += 1
                logger.warning(
                    "Library workflow %s deploy failed: HTTP %s %s",
                    wf.slug,
                    resp.status_code if resp is not None else "no-response",
                    resp.text[:200] if resp is not None else "",
                )
                continue
            self._ootb_workflow_ids[wf.slug] = wf_id
            self._ootb_workflows.append(wf)
            deployed += 1
        return deployed, failed

    def _ootb_rule_actions(self, template: dict[str, Any]) -> list[dict[str, Any]]:
        """``.workflows`` actions an OOTB rule built from ``template`` should carry."""
        workflows = getattr(self, "_ootb_workflows", None)
        if not workflows:
            return []
        params = template.get("params") or {}
        esql = (params.get("esqlQuery") or {}).get("esql") or ""
        if not esql:
            return []
        datasets = self._enabled_datasets()
        return [
            workflow_action(self._ootb_workflow_ids[wf.slug])
            for wf in workflows
            if wf.slug in self._ootb_workflow_ids and rule_wires_workflow(esql, wf, datasets)
        ]
