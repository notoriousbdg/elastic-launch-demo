"""SkillsMixin — Agent Builder skill deploy and discovery methods."""

from __future__ import annotations

import logging

import httpx

from elastic_config.deployer_base import _kibana_headers, _retry_http, ProgressCallback, StepIdx

logger = logging.getLogger("deployer")


class SkillsMixin:

    def _discover_builtin_skill_ids(self, client: httpx.Client) -> list[str]:
        """Return IDs of available built-in skills matching the scenario's wanted names."""
        try:
            resp = client.get(
                f"{self.kibana_url}/api/agent_builder/skills",
                headers=_kibana_headers(self.api_key),
                params={"include_plugins": "true"},
            )
            if resp.status_code >= 300:
                logger.warning("Could not list built-in skills: HTTP %s", resp.status_code)
                return []
            body = resp.json()
            skills = body.get("data", body.get("results", []))
            want = set(self.scenario.builtin_skill_names)
            found = []
            matched: set[str] = set()
            for skill in skills:
                skill_id = skill.get("id", "")
                skill_name = skill.get("name", "")
                if skill_id in want or skill_name in want:
                    found.append(skill_id)
                    matched.add(skill_id)
                    matched.add(skill_name)
            for wanted in want:
                if wanted not in matched:
                    logger.warning(
                        "Built-in skill '%s' not found on this cluster — skipping", wanted
                    )
            return found
        except Exception as exc:
            logger.warning("Built-in skill discovery failed: %s", exc)
            return []

    def _deploy_skills(self, client: httpx.Client, notify: ProgressCallback):
        """Discover built-in skills and deploy custom skills for this scenario.

        Appends all resolved skill IDs to self._created_skill_ids and
        updates step 8 progress items.
        """
        step = self._step(StepIdx.AI_TOOLS)

        builtin_ids = self._discover_builtin_skill_ids(client)
        self._created_skill_ids.extend(builtin_ids)
        if builtin_ids:
            step.detail = f"Attached {len(builtin_ids)} built-in skill(s)"
            notify(self.progress)

        for skill_def in self.scenario.skill_definitions:
            skill_id = skill_def["id"]
            client.delete(
                f"{self.kibana_url}/api/agent_builder/skills/{skill_id}",
                headers=_kibana_headers(self.api_key),
                params={"force": "true"},
            )
            # Build the effective skill definition for this POST.
            effective_def = dict(skill_def)

            # Strip the agent_prompt_hint field — it is used internally to build
            # the agent system prompt but Kibana rejects unknown properties with 400.
            effective_def.pop("agent_prompt_hint", None)

            # Filter tool_ids: drop only IDs that *this deployer* was responsible
            # for creating and failed to create.  Kibana builtins (platform.core.*,
            # etc.) are not in self._created_tool_ids and must be preserved.
            if "tool_ids" in effective_def:
                own = {t["id"] for t in self.scenario.tool_definitions} | {
                    self.scenario.prefixed_tool_id("remediation_action"),
                    self.scenario.prefixed_tool_id("escalation_action"),
                }
                before = effective_def["tool_ids"]
                effective_def["tool_ids"] = [
                    tid for tid in before
                    if tid not in own or tid in self._created_tool_ids
                ]
                dropped = set(before) - set(effective_def["tool_ids"])
                if dropped:
                    logger.warning(
                        "Skill %s: dropped %d tool_id(s) that failed to create: %s",
                        skill_id, len(dropped), dropped,
                    )
                if len(effective_def["tool_ids"]) > 5:
                    logger.error(
                        "Skill %s has %d tool_ids after filtering — exceeds Kibana's "
                        "5-tool limit for user-created skills. Skipping POST.",
                        skill_id, len(effective_def["tool_ids"]),
                    )
                    step.detail = (
                        f"Skill {skill_id} skipped: "
                        f"{len(effective_def['tool_ids'])} tool_ids > 5 limit"
                    )
                    notify(self.progress)
                    continue
            resp = _retry_http(
                lambda: client.post(
                    f"{self.kibana_url}/api/agent_builder/skills",
                    headers=_kibana_headers(self.api_key),
                    json=effective_def,
                ),
                attempts=3,
                base_delay=2.0,
                label=f"skill {skill_id}",
            )
            if resp is not None and resp.status_code < 300:
                self._created_skill_ids.append(skill_id)
                step.items_done += 1
                step.detail = f"Created skill: {skill_id}"
                logger.info("Created skill: %s", skill_id)
            else:
                status = resp.status_code if resp is not None else "no response"
                body = resp.text[:200] if resp is not None else ""
                step.detail = f"Skill {skill_id} failed: HTTP {status}"
                logger.warning(
                    "Skill %s failed: HTTP %s — %s",
                    skill_id, status, body,
                )
            notify(self.progress)

    def _cleanup_skills(self, client: httpx.Client) -> int:
        """Delete custom skills created for this scenario. Returns count deleted."""
        deleted = 0
        for skill_def in self.scenario.skill_definitions:
            skill_id = skill_def["id"]
            resp = client.delete(
                f"{self.kibana_url}/api/agent_builder/skills/{skill_id}",
                headers=_kibana_headers(self.api_key),
                params={"force": "true"},
            )
            if resp.status_code < 300:
                deleted += 1
            elif resp.status_code != 404:
                logger.warning(
                    "Failed to delete skill %s: HTTP %s", skill_id, resp.status_code
                )
        return deleted
