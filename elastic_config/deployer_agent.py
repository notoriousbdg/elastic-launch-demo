"""AgentMixin — tools, agent, and system prompt methods."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from elastic_config.deployer_base import _kibana_headers, _retry_http, ProgressCallback, StepIdx

logger = logging.getLogger("deployer")


class AgentMixin:

    def _deploy_tools(self, client: httpx.Client, notify: ProgressCallback):
        step = self._step(StepIdx.AI_TOOLS)
        step.status = "running"
        notify(self.progress)

        # Use scenario-provided tool definitions + deployer-added workflow tools
        tools = list(self.scenario.tool_definitions)

        # Add workflow tools (need workflow IDs from deployment)
        for name_frag, wf_id in self._workflow_ids.items():
            if "remediation" in name_frag:
                tools.append({
                    "id": self.scenario.prefixed_tool_id("remediation_action"),
                    "type": "workflow",
                    "description": (
                        "Execute remediation actions for anomalies. Triggers the "
                        "Remediation Action workflow. Pass approval_mode (string): "
                        "required for HITL / channels 1–15 (pauses at waitForInput); "
                        "skip only for Significant Event Auto-Remediate. Pass "
                        "dry_run: false to resolve the fault."
                    ),
                    "configuration": {"workflow_id": wf_id},
                })
            elif "escalation" in name_frag:
                tools.append({
                    "id": self.scenario.prefixed_tool_id("escalation_action"),
                    "type": "workflow",
                    "description": (
                        "Escalate critical anomalies and manage operational hold decisions."
                    ),
                    "configuration": {"workflow_id": wf_id},
                })

        skill_defs = self.scenario.skill_definitions
        step.items_total = len(tools) + len(skill_defs)

        for tool_def in tools:
            tool_id = tool_def["id"]
            # Delete first, then create
            client.delete(
                f"{self.kibana_url}/api/agent_builder/tools/{tool_id}",
                headers=_kibana_headers(self.api_key),
            )
            resp = client.post(
                f"{self.kibana_url}/api/agent_builder/tools",
                headers=_kibana_headers(self.api_key),
                json=tool_def,
            )
            if resp.status_code < 300:
                step.items_done += 1
                step.detail = f"Created: {tool_id}"
                self._created_tool_ids.append(tool_id)
            else:
                step.detail = f"Failed: {tool_id} (HTTP {resp.status_code})"
                logger.warning("Tool %s failed: %s", tool_id, resp.text[:200])
            notify(self.progress)

        self._deploy_skills(client, notify)

        step.status = "ok" if step.items_done > 0 else "failed"
        notify(self.progress)

    def _deploy_agent(self, client: httpx.Client, notify: ProgressCallback):
        step = self._step(StepIdx.AI_AGENT)
        step.status = "running"
        notify(self.progress)

        agent_cfg = self.scenario.agent_config
        agent_id = agent_cfg.get("id", f"{self.ns}-analyst")

        # Build full system prompt from scenario properties
        system_prompt = self._generate_system_prompt(agent_cfg)

        # Agent carries only platform builtins; scenario tools are accessed via skills.
        # Defensive fallback: if any custom skill failed to deploy, include the
        # scenario tools directly on the agent so capability is not silently lost.
        custom_skill_ids = {sd["id"] for sd in self.scenario.skill_definitions}
        missing_skills = custom_skill_ids - set(self._created_skill_ids)
        if missing_skills:
            logger.warning(
                "Skills %s failed to deploy — falling back to including scenario "
                "tools directly on the agent",
                missing_skills,
            )
            tool_ids = list(self._created_tool_ids)
        else:
            tool_ids = []
        tool_ids.extend([
            "platform.core.cases",
            "platform.core.resume_workflow_execution",
            "platform.core.get_workflow_execution_status",
            "platform.core.create_visualization",
        ])

        configuration: dict[str, Any] = {
            "instructions": system_prompt,
            "tools": [{"tool_ids": tool_ids}],
        }
        if self._created_skill_ids:
            configuration["skill_ids"] = self._created_skill_ids

        agent_body: dict[str, Any] = {
            "id": agent_id,
            "name": f"{self.scenario.scenario_name}: {agent_cfg.get('name', 'Analyst')}",
            "description": agent_cfg.get(
                "description",
                f"AI-powered analyst for {self.scenario.scenario_name}.",
            ),
            "configuration": configuration,
        }

        # DELETE + POST for reliable update. Retry transient Kibana errors.
        client.delete(
            f"{self.kibana_url}/api/agent_builder/agents/{agent_id}",
            headers=_kibana_headers(self.api_key),
        )
        resp = _retry_http(
            lambda: client.post(
                f"{self.kibana_url}/api/agent_builder/agents",
                headers=_kibana_headers(self.api_key),
                json=agent_body,
            ),
            attempts=4,
            base_delay=2.0,
            label=f"agent {agent_id}",
        )

        if resp is not None and resp.status_code < 300:
            step.status = "ok"
            step.detail = f"Agent {agent_id} created"
        else:
            step.status = "failed"
            status = resp.status_code if resp is not None else "no response"
            body = resp.text[:200] if resp is not None else ""
            step.detail = f"HTTP {status}: {body}"
            logger.warning(
                "Agent %s create failed: HTTP %s — %s",
                agent_id, status, body,
            )
        notify(self.progress)

    def _generate_system_prompt(self, agent_cfg: dict[str, Any]) -> str:
        """Build a comprehensive system prompt from scenario properties."""
        scenario = self.scenario
        svc_list = "\n".join(
            f"- {name} ({cfg['cloud_provider'].upper()}, {cfg['subsystem']})"
            for name, cfg in scenario.services.items()
        )
        svc_names = ", ".join(sorted(scenario.services.keys()))

        # Use the scenario's identity text as opening, then add comprehensive guide
        base_prompt = agent_cfg.get("system_prompt", "")

        # Auto-generate a comprehensive prompt
        subsystems = sorted(set(
            cfg["subsystem"] for cfg in scenario.services.values()
        ))

        # Use scenario-provided identity if available, otherwise generic
        identity = base_prompt if base_prompt else (
            f"You are the {scenario.scenario_name} Operations Analyst, "
            f"an expert AI agent embedded in the Elastic observability platform."
        )

        ns = scenario.namespace

        # Build the skills block dynamically from skill_definitions.agent_prompt_hint.
        # Only include skills that were successfully deployed.
        skills_bullets = []
        for sd in scenario.skill_definitions:
            hint = sd.get("agent_prompt_hint", "")
            if hint and sd["id"] in self._created_skill_ids:
                skills_bullets.append(f"- {hint}")
        skills_block = (
            "\n".join(skills_bullets)
            if skills_bullets
            else "- (No scenario skills deployed — use tools directly.)"
        )

        return f"""{identity}

## Mission Context
- **Scenario**: {scenario.scenario_name}
- **Namespace**: {scenario.namespace}
- **Kibana URL**: {self.kibana_display_url}
- **Subsystems**: {', '.join(subsystems)}
- **Services**:
{svc_list}
- **Fault Channels**: 20 distinct anomaly channels covering all subsystems
- **Telemetry Source**: OpenTelemetry -> Elasticsearch (logs)

## Skills

Your telemetry query and remediation capabilities are provided by these procedures.
Consult the applicable one before beginning any assessment, investigation, or remediation.

{skills_block}

## Creating Visualizations
Use the **visualization-creation** skill to create Lens charts and dashboards. Always provide complete,
specific parameters — vague requests will produce empty panels.

**Data sources — ES|QL FROM targets and Kibana data view IDs**:

IMPORTANT: `metrics.otel.{ns}`, `traces.otel.{ns}` are Kibana **data view IDs** — they are NOT
index names. Never use them in an ES|QL `FROM` clause or as an index pattern in a Lens panel.
Use the real index patterns below instead.

| Signal | ES|QL FROM / Lens index pattern | Kibana data view ID |
|--------|--------------------------------|---------------------|
| Logs | `logs.otel.{ns},logs.otel.{ns}.*` | `logs.otel.{ns}` |
| Traces/APM | `traces-*` | `traces.otel.{ns}` |
| Metrics | `metrics-*` | `metrics.otel.{ns}` |

Key fields:
- Traces (`traces-*`): `@timestamp`, `duration`, `status.code`,
  `resource.attributes.service.name`, `span.name`, `trace.id`;
  scope to namespace with `resource.attributes.service.namespace == "{ns}"`
- Logs (`logs.otel.{ns},logs.otel.{ns}.*`): `@timestamp`, `body.text`, `severity_text`,
  `service.name`, `trace.id`, `span.id`
- Metrics (`metrics-*`): `@timestamp`, `metrics.system.cpu.utilization`,
  `metrics.system.memory.utilization`, `metrics.k8s.node.cpu.utilization`,
  `metrics.k8s.pod.memory.usage`, `metrics.system.cpu.load_average.1m`,
  `metrics.system.filesystem.utilization`, `resource.attributes.service.name`, `host.name`;
  scope to namespace with `resource.attributes.service.namespace == "{ns}"`

**Proven visualization patterns**:
- Error rate over time → bar_stacked on `traces-*`, x=`@timestamp` (30s), y=count with
  `status.code: Error` filter, split by `resource.attributes.service.name`,
  KQL scope: `resource.attributes.service.namespace: "{ns}"`
- P99 latency by service → area on `traces-*`, x=`@timestamp`, y=percentile(duration,99),
  split by `resource.attributes.service.name`,
  KQL scope: `resource.attributes.service.namespace: "{ns}"`
- Log volume → area on `logs.otel.{ns}`, x=`@timestamp` (30s), y=count, filter by `severity_text`
- CPU utilization over time by service → line on `metrics-*`, x=`@timestamp`, y=avg of
  `metrics.system.cpu.utilization` (or `metrics.k8s.node.cpu.utilization`),
  split by `resource.attributes.service.name`,
  KQL scope: `resource.attributes.service.namespace: "{ns}"`
- Memory utilization over time by service → line on `metrics-*`, x=`@timestamp`, y=avg of
  `metrics.system.memory.utilization` (or `metrics.k8s.pod.memory.usage`),
  split by `resource.attributes.service.name`,
  KQL scope: `resource.attributes.service.namespace: "{ns}"`
- Service health → metric tile on `traces-*`, error-rate formula
  `count(kql='status.code: Error') / count()` filtered by
  `resource.attributes.service.name: "<name>" AND resource.attributes.service.namespace: "{ns}"`

When asked to create a dashboard, always specify: index pattern (FROM target), chart type,
x-axis field with interval, y-axis metric, split field (if any), and any KQL filter.
Ask for clarification only if the user's intent is genuinely ambiguous.

## Available Services
{svc_names}"""

    def _cleanup_conversations(self, client: httpx.Client) -> int:
        """Delete all Agent Builder conversations belonging to the scenario's agent."""
        agent_id = self.scenario.agent_config.get("id", f"{self.ns}-analyst")
        resp = client.get(
            f"{self.kibana_url}/api/agent_builder/conversations",
            headers=_kibana_headers(self.api_key),
            params={"agent_id": agent_id},
        )
        if resp.status_code >= 300:
            logger.warning("Failed to list conversations: HTTP %s", resp.status_code)
            return 0
        conversations = resp.json().get("results", [])
        deleted = 0
        for conv in conversations:
            r = client.delete(
                f"{self.kibana_url}/api/agent_builder/conversations/{conv['id']}",
                headers=_kibana_headers(self.api_key),
            )
            if r.status_code < 300:
                deleted += 1
            else:
                logger.warning("Failed to delete conversation %s: HTTP %s", conv["id"], r.status_code)
        return deleted

    def _cleanup_agent(self, client: httpx.Client) -> int:
        """Delete agent, custom tools, skills, and conversations. Returns conversation count deleted."""
        agent_id = self.scenario.agent_config.get("id", f"{self.ns}-analyst")
        # Delete conversations before removing the agent
        deleted_convs = self._cleanup_conversations(client)
        # Delete custom skills before removing the agent (avoid 409 Conflict)
        self._cleanup_skills(client)
        client.delete(
            f"{self.kibana_url}/api/agent_builder/agents/{agent_id}",
            headers=_kibana_headers(self.api_key),
        )
        # Collect tool IDs from scenario's tool_definitions + workflow tools
        tool_ids = [t["id"] for t in self.scenario.tool_definitions]
        tool_ids.extend([
            self.scenario.prefixed_tool_id("remediation_action"),
            self.scenario.prefixed_tool_id("escalation_action"),
        ])
        for tool_id in tool_ids:
            client.delete(
                f"{self.kibana_url}/api/agent_builder/tools/{tool_id}",
                headers=_kibana_headers(self.api_key),
            )
        return deleted_convs
