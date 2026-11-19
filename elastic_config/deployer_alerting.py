"""AlertingMixin — alerting deploy and cleanup methods."""

from __future__ import annotations

import concurrent.futures
import json
import logging
from typing import TYPE_CHECKING

import httpx
import yaml

from elastic_config.deployer_base import _kibana_headers, _retry_http, ProgressCallback, StepIdx

if TYPE_CHECKING:
    from scenario_engine.base import BaseScenario

logger = logging.getLogger("deployer")


# ── Serverless: Universal (v2) alert rules + action policies ──────────────────
#
# A v2 rule holds no action; an action policy routes its alerts to a workflow
# and matches them on the rule's ``routing_tags``.  The workflow receives
# ``inputs.payload`` (alert fields + the query's output columns in ``data``) and
# NOT the rule's tags, so every per-channel value is an EVAL column of the query.
# A v2 alert goes pending -> active -> recovering -> inactive and each change is
# dispatched: the RCA policies match ``alert_status: active`` (once per fault)
# and the case-close policy matches ``inactive``.

from app.ops_profile import auto_remediate_channels

V2_RULES = "/api/alerting/v2/rules"
V2_POLICIES = "/api/alerting/v2/action_policies"

# Nightshift automation (one per scenario).  Same daily budget as the hand-built sample.
NIGHTSHIFT_AUTOMATIONS = "/internal/nightshift/automations"
NIGHTSHIFT_DAILY_DISPATCH_LIMIT = 20


def _esql_str(value: str) -> str:
    """ES|QL string literal."""
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def universal_rule_id(ns: str, ch_int: int) -> str:
    return f"{ns}-ch{ch_int:02d}"


def _is_auto(ch_int: int, auto: "bool | None") -> bool:
    """Auto-remediated channel? *auto* comes from the scenario's ops profile
    (``auto_remediate_channels``); without it, the default set (16-20)."""
    if auto is not None:
        return bool(auto)
    from app.ops_profile import DEFAULT_AUTO_CHANNELS
    return int(ch_int) in DEFAULT_AUTO_CHANNELS


def alert_rule_name(scenario_name: str, ch_int: int, name: str, auto: "bool | None" = None) -> str:
    """Display name of a channel's alert rule (also the case title, hence 160 chars).

    No channel number in the name: Nightshift and the case title show it to the
    audience.  The channel stays in the rule id, the tags and the ``channel`` column.
    """
    suffix = " (Auto-Remediate)" if _is_auto(ch_int, auto) else ""
    return f"{scenario_name}: {name}{suffix}"[:160]


def alert_rule_name_prefix(scenario_name: str) -> str:
    """Name prefix of the channel alert rules (and the Nightshift trigger pattern).

    The scenario's Nightshift signal rules are titled "{scenario_name} signal — ..."
    and so don't share it; callers still check ``kind == "alert"`` to be safe.
    """
    return f"{scenario_name}: "


def is_scenario_alert_rule(rule: dict, ns: str, scenario_name: str) -> bool:
    """True for a v2 rule this scenario's alerting step created.

    Current rules: kind alert, the scenario-name prefix and the ``ns`` tag (so a
    user's "{scenario_name}: ..." rule is left alone).  Legacy rules from before
    the rename were named "{scenario_name} CHNN: ...".
    """
    if rule.get("kind") != "alert":
        return False
    meta = rule.get("metadata", {}) or {}
    name = meta.get("name", "")
    if name.startswith(f"{scenario_name} CH"):
        return True
    return name.startswith(alert_rule_name_prefix(scenario_name)) and ns in (meta.get("tags") or [])


def universal_routing_tag(ns: str, ch_int: int, auto: "bool | None" = None) -> str:
    """HITL channels and auto-remediate channels (ops profile) route separately."""
    return f"{ns}-{'auto' if _is_auto(ch_int, auto) else 'hitl'}"


def universal_policy_ids(ns: str) -> dict[str, str]:
    return {
        "hitl": f"{ns}-rca-hitl",
        "auto": f"{ns}-rca-auto",
        "close": f"{ns}-case-close",
        "nightshift": f"{ns}-nightshift",
    }


def universal_rule_body(ns: str, scenario_name: str, ch_int: int, ch_data: dict,
                        auto: "bool | None" = None) -> dict:
    """Request body for one chaos channel's Universal alert rule."""
    error_type = ch_data["error_type"]
    ch_int = int(ch_int)
    severity = "critical" if ch_int >= 19 else "high" if ch_int <= 6 else "medium"
    rule_name = alert_rule_name(scenario_name, ch_int, ch_data["name"], auto)
    kql = 'body.text: "{}" AND severity_text: "ERROR"'.format(
        error_type.replace("\\", "\\\\").replace('"', '\\"')
    )
    base = (
        f"FROM logs.otel.{ns},logs.otel.{ns}.* "
        f"| WHERE KQL({_esql_str(kql)}) "
        "| STATS match_count = COUNT(*) "
        f"| EVAL error_type = {_esql_str(error_type)}, "
        f"remediation_action = {_esql_str(ch_data.get('remediation_action', ''))}, "
        f"channel = {ch_int}, "
        f"subsystem = {_esql_str(ch_data.get('subsystem', ''))}, "
        f"severity = {_esql_str(severity)}, "
        f"rule_name = {_esql_str(rule_name)}"
    )
    return {
        "kind": "alert",
        "metadata": {
            "name": rule_name,
            # Same 4-tag encoding as the classic rule (ns, error_type, action, channel)
            "tags": [ns, error_type, ch_data.get("remediation_action", ""), str(ch_int)],
            "routing_tags": [universal_routing_tag(ns, ch_int, auto)],
        },
        "time_field": "@timestamp",
        "schedule": {"every": "1m", "lookback": "1m"},
        "query": {"base": base, "breach": {"segment": "WHERE match_count > 0"}},
        "recovery": {"strategy": "no_breach"},
        "no_data": {"strategy": "ignore"},
    }


def universal_policy_bodies(
    ns: str, scenario_name: str, hitl_wf_id: str, auto_wf_id: str, close_wf_id: str,
    nightshift_wf_id: str = "",
) -> dict[str, dict]:
    """Action policies keyed by policy id: two RCA policies, one case-close policy and,
    when the Nightshift automation exists, one that starts its investigation.

    A policy with no workflow id is left out (the caller logs it).
    """
    ids = universal_policy_ids(ns)

    def policy(name: str, description: str, wf_id: str, tags: list[str], status: str) -> dict:
        return {
            "name": f"{scenario_name} {name}",
            "description": description,
            "destinations": [{"id": wf_id, "type": "workflow"}],
            "matcher": {"tags": tags, "expression": f"alert_status: {status}"},
            "grouping": {"mode": "per_alert"},
            "throttle": {"strategy": "on_status_change"},
        }

    out: dict[str, dict] = {}
    if hitl_wf_id:
        out[ids["hitl"]] = policy(
            "RCA (HITL)", "Runs the HITL notification workflow once per fault (channels not auto-remediated).",
            hitl_wf_id, [f"{ns}-hitl"], "active",
        )
    if auto_wf_id:
        out[ids["auto"]] = policy(
            "RCA (Auto-Remediate)", "Runs the auto-remediate notification workflow once per fault (the ops profile's auto_remediate_channels).",
            auto_wf_id, [f"{ns}-auto"], "active",
        )
    if nightshift_wf_id:
        out[ids["nightshift"]] = policy(
            "Nightshift Investigation", "Starts a Nightshift investigation once per fault (all channels).",
            nightshift_wf_id, [f"{ns}-hitl", f"{ns}-auto"], "active",
        )
    if close_wf_id:
        out[ids["close"]] = policy(
            "Case Auto-Close", "Closes the fault's case once its alert has recovered and cleared.",
            close_wf_id, [f"{ns}-hitl", f"{ns}-auto"], "inactive",
        )
    return out


def nightshift_automation_name(scenario_name: str) -> str:
    """Unique per scenario, and the key deploy/teardown use to find this scenario's automation."""
    return f"{scenario_name} Investigation"


def nightshift_automation_body(ns: str, scenario_name: str) -> dict:
    """Request body for the scenario's Nightshift automation (all 20 chaos channels)."""
    return {
        "name": nightshift_automation_name(scenario_name),
        "description": f"Investigates the {scenario_name} chaos-channel alerts.",
        "tags": [ns],
        "isEnabled": True,  # the API defaults to disabled
        "automationType": "custom",
        # Matches this scenario's channel rules ("{scenario_name}: <name>").  The
        # "{scenario_name} signal — ..." signal rules no longer share the prefix and are kind
        # "signal", which this kind "alert" row does not match.
        "trigger": {"rows": [{
            "kind": "alert",
            "ruleNamePattern": alert_rule_name_prefix(scenario_name),
            "ruleNameMatchMode": "substring",
        }]},
        "execution": {"reasoningMode": "investigate"},
        "completions": [],
        "runtime": {"dailyDispatchLimit": NIGHTSHIFT_DAILY_DISPATCH_LIMIT},
    }


def nightshift_workflow_yaml(automation_id: str, automation_name: str, run_as: str = "") -> str:
    """The automation's backing workflow, started by an action policy instead of an alert event.

    Nightshift generates it with a classic-alert trigger only, and universal alert status
    changes are not delivered to workflow event triggers on serverless, so a policy runs it
    (``inputs.payload``).  ``nightshift.triggerInvestigation`` only works from the automation's
    own workflow, which is why the policy targets this one.  The concurrency key drops the
    dispatcher's duplicate dispatch of an alert (it shows as a skipped run).
    """
    alert = "inputs.payload.alerts[0]"
    doc = {
        "name": automation_name,
        "enabled": True,
        "tags": ["nightshift", "automation"],
        "settings": {
            **({"run_as": run_as} if run_as else {}),
            "concurrency": {
                "key": f"{automation_id}-{{{{ {alert}.alert_id }}}}",
                "strategy": "drop",
                "max": 1,
            },
        },
        "triggers": [{"type": "manual"}],
        "steps": [{
            "name": "trigger_investigation",
            "type": "nightshift.triggerInvestigation",
            "with": {
                "automation_id": automation_id,
                "subject_type": "alert",
                "subject_id": f"{{{{ {alert}.alert_id }}}}",
                "title": f"{{{{ {alert}.data.rule_name }}}}",
                "summary": automation_name,
                "trigger_type": "automatic",
            },
        }],
    }
    return yaml.safe_dump(doc, sort_keys=False, width=1000)


class AlertingMixin:
    # Attributes supplied by ScenarioDeployer at runtime — declared here for type checkers.
    if TYPE_CHECKING:
        kibana_url: str
        api_key: str
        ns: str
        scenario: BaseScenario
        _workflow_ids: dict[str, str]

    def _deploy_alerting(self, client: httpx.Client, notify: ProgressCallback):
        step = self._step(StepIdx.ALERT_RULES)
        step.status = "running"
        notify(self.progress)

        # Find HITL notification workflow ID
        notification_wf_id = self._workflow_ids.get("significant_event_notification", "")
        auto_remediate_wf_id = self._workflow_ids.get("significant_event_notification_auto_remediate", "")

        if not notification_wf_id or not auto_remediate_wf_id:
            # Search for missing IDs
            try:
                items = self._wf_search(client)
                for item in items:
                    wf_name = item.get("name", "")
                    wf_id = item.get("id", "")
                    if not wf_id:
                        continue
                    if not notification_wf_id and "Auto-Remediate" not in wf_name and (
                        "Notification" in wf_name or "Significant" in wf_name
                    ):
                        notification_wf_id = wf_id
                    elif not auto_remediate_wf_id and "Auto-Remediate" in wf_name and (
                        "Notification" in wf_name or "Significant" in wf_name
                    ):
                        auto_remediate_wf_id = wf_id
            except Exception:
                pass

        if not notification_wf_id:
            step.status = "failed"
            step.detail = "Notification workflow not found"
            notify(self.progress)
            return

        if not auto_remediate_wf_id:
            logger.warning("Auto-remediate workflow not found; auto-remediate channels will use HITL workflow")

        # Note: the Case Auto-Close workflow is NOT wired into alert rule
        # actions — see the notify_when comment below for why, and
        # significant_event_case_close.yaml for how it actually runs
        # (self-scheduled polling, deployed automatically by
        # _deploy_workflows like any other workflow YAML).

        # Clean old rules
        self._cleanup_alerts(client)

        registry = self.scenario.channel_registry

        if self._stack_generation() == "serverless":
            # [serverless] Universal (v2) rules + action policies instead of classic rules.
            self._deploy_universal_rules(
                client, notify, step, registry,
                notification_wf_id, auto_remediate_wf_id or notification_wf_id,
            )
            return

        # Create 20 alert rules
        step.items_total = len(registry)

        for ch_num, ch_data in sorted(registry.items()):
            error_type = ch_data["error_type"]
            name = ch_data["name"]
            subsystem = ch_data.get("subsystem", "")

            # Determine severity
            ch_int = int(ch_num)
            if ch_int >= 19:
                severity = "critical"
            elif ch_int <= 6:
                severity = "high"
            else:
                severity = "medium"

            # The ops profile's auto_remediate_channels remediate themselves; the rest are HITL
            auto_remediate = ch_int in auto_remediate_channels(self.scenario)
            # Kibana Cases API enforces a 160-char title limit; rule name becomes the case title
            rule_name = alert_rule_name(self.scenario.scenario_name, ch_int, name, auto_remediate)

            es_query = json.dumps({
                "query": {
                    "bool": {
                        "filter": [
                            {"range": {"@timestamp": {"gte": "now-1m"}}},
                            {"match_phrase": {"body.text": error_type}},
                            {"term": {"severity_text": "ERROR"}},
                        ]
                    }
                }
            })

            rule = {
                "name": rule_name,
                "rule_type_id": ".es-query",
                "consumer": "alerts",
                # Tag encoding (workflow inputs from alert actions resolve blank,
                # so all per-channel data the workflow needs must live in tags):
                #   tags[0] = namespace
                #   tags[1] = error_type
                #   tags[2] = remediation_action (HITL pauses for approval; auto-remediate skips)
                #   tags[3] = channel number
                "tags": [self.ns, error_type, ch_data.get("remediation_action", ""), str(ch_int)],
                "schedule": {"interval": "1m"},
                # Rule-level notify_when, NOT per-action "frequency": Kibana
                # silently strips a "frequency" block on system-connector
                # actions (id "system-connector-.workflows") — it's accepted by
                # the create API but absent from the stored rule and has no
                # effect.
                #
                # notify_when does NOT meaningfully gate this action either.
                # Confirmed via .kibana-event-log-*: the .workflows
                # system-connector action fires on every rule tick regardless
                # of onActionGroupChange, but only actually reaches the
                # workflow (non-empty request) when the tick's alert summary
                # has a non-empty "new" bucket — "ongoing" (repeat ticks of a
                # still-active alert) and "recovered" summary batches report
                # a successful, silent no-op action execution and never
                # invoke the workflow at all. So in practice this only fires
                # once, when the fault first goes active — which is exactly
                # what we want for case creation, but means the alert's
                # "recovered" transition can NEVER be used to trigger a
                # workflow via this connector. Case-close is therefore a
                # separate, independently *scheduled* workflow
                # (significant_event_case_close.yaml) that polls open cases
                # and closes them once their rule reports
                # execution_status.status != "active" — see that file.
                "notify_when": "onActionGroupChange",
                "throttle": None,
                "params": {
                    "searchType": "esQuery",
                    "esQuery": es_query,
                    "index": ["logs*"],
                    "timeField": "@timestamp",
                    "threshold": [0],
                    "thresholdComparator": ">",
                    "size": 100,
                    "timeWindowSize": 1,
                    "timeWindowUnit": "m",
                },
                "actions": [{
                    "group": "query matched",
                    "id": "system-connector-.workflows",
                    "params": {
                        "subAction": "run",
                        "subActionParams": {
                            "workflowId": (auto_remediate_wf_id or notification_wf_id) if auto_remediate else notification_wf_id,
                            "inputs": {
                                "channel": ch_int,
                                "error_type": error_type,
                                "subsystem": subsystem,
                                "severity": severity,
                            },
                        },
                    },
                }],
            }

            resp = _retry_http(
                lambda r=rule: client.post(
                    f"{self.kibana_url}/api/alerting/rule",
                    headers=_kibana_headers(self.api_key),
                    json=r,
                ),
                label=f"create alert rule {rule_name}",
            )
            if resp is not None and resp.status_code < 300:
                step.items_done += 1
            elif resp is not None:
                logger.warning("Alert rule %s failed: %s", rule_name, resp.text[:200])
            notify(self.progress)

        step.status = "ok" if step.items_done > 0 else "failed"
        step.detail = f"Created {step.items_done}/{step.items_total} alert rules"
        notify(self.progress)

    def _case_close_workflow_id(self, client: httpx.Client) -> str:
        wf_id = self._workflow_ids.get("significant_event_case_close", "")
        if wf_id:
            return wf_id
        try:
            for item in self._wf_search(client):
                if "Case Auto-Close" in item.get("name", "") and item.get("id"):
                    return item["id"]
        except Exception:
            pass
        return ""

    def _deploy_nightshift_automation(self, client: httpx.Client) -> tuple[str, str]:
        """[serverless] Create this scenario's Nightshift automation and hand its workflow to a policy.

        Returns ``(workflow_id, detail)``; ``workflow_id`` is empty on any failure, which is
        logged but never fails the alert-rules step (Nightshift may be unavailable).
        """
        name = nightshift_automation_name(self.scenario.scenario_name)
        headers = _kibana_headers(self.api_key)
        resp = _retry_http(
            lambda: client.post(
                f"{self.kibana_url}{NIGHTSHIFT_AUTOMATIONS}",
                headers=headers,
                json=nightshift_automation_body(self.ns, self.scenario.scenario_name),
            ),
            label=f"create nightshift automation {name}",
        )
        if resp is None or resp.status_code >= 300:
            code = resp.status_code if resp is not None else "no response"
            logger.warning("Nightshift automation %s not created (HTTP %s)", name, code)
            return "", f"Nightshift automation not created (HTTP {code})"
        try:
            created = resp.json()
            automation_id, workflow_id = created["id"], created["workflowId"]
        except Exception:
            logger.warning("Nightshift automation %s: unexpected response %s", name, resp.text[:200])
            return "", "Nightshift automation not created (unexpected response)"

        try:
            # Keep the service account Nightshift bound the generated workflow to.
            current = client.get(
                f"{self.kibana_url}/api/workflows/workflow/{workflow_id}", headers=headers
            )
            run_as = ""
            if current.status_code < 300:
                settings = (yaml.safe_load(current.json().get("yaml", "")) or {}).get("settings") or {}
                run_as = settings.get("run_as", "")
            patched = client.put(
                f"{self.kibana_url}/api/workflows/workflow/{workflow_id}",
                headers=headers,
                json={"yaml": nightshift_workflow_yaml(automation_id, name, run_as)},
            )
            body = patched.json() if patched.status_code < 300 else {}
            if patched.status_code >= 300 or body.get("valid") is False:
                raise RuntimeError(f"HTTP {patched.status_code} {body.get('validationErrors') or patched.text[:200]}")
        except Exception as exc:
            logger.warning("Nightshift automation %s: could not set its workflow (%s); removing it", name, exc)
            client.delete(f"{self.kibana_url}{NIGHTSHIFT_AUTOMATIONS}/{automation_id}", headers=headers)
            return "", "Nightshift automation not created (workflow update failed)"
        return workflow_id, "Nightshift automation created"

    def _find_nightshift_automation_ids(self, client: httpx.Client) -> list[str]:
        """Ids of this scenario's automation only: exact name match, so other scenarios'
        automations (and hand-built ones) are never touched."""
        name = nightshift_automation_name(self.scenario.scenario_name)
        resp = _retry_http(
            lambda: client.get(
                f"{self.kibana_url}{NIGHTSHIFT_AUTOMATIONS}", headers=_kibana_headers(self.api_key)
            ),
            label="list nightshift automations",
        )
        if resp is None or resp.status_code >= 300:
            return []  # Nightshift not available here: nothing of ours to delete
        try:
            return [a["id"] for a in resp.json().get("automations", []) if a.get("name") == name and a.get("id")]
        except Exception:
            return []

    def _deploy_universal_rules(
        self, client: httpx.Client, notify: ProgressCallback, step, registry: dict,
        hitl_wf_id: str, auto_wf_id: str,
    ) -> None:
        """[serverless] One Universal alert rule per channel, routed by 3 action policies."""
        close_wf_id = self._case_close_workflow_id(client)
        if not close_wf_id:
            logger.warning("Case auto-close workflow not found; cases will not close on recovery")
        nightshift_wf_id, nightshift_detail = self._deploy_nightshift_automation(client)
        policies = universal_policy_bodies(
            self.ns, self.scenario.scenario_name, hitl_wf_id, auto_wf_id, close_wf_id,
            nightshift_wf_id,
        )
        step.items_total = len(registry) + len(policies)
        rules_done = 0
        policies_done = 0

        for ch_num, ch_data in sorted(registry.items()):
            ch_int = int(ch_num)
            rule = universal_rule_body(self.ns, self.scenario.scenario_name, ch_int, ch_data,
                                       ch_int in auto_remediate_channels(self.scenario))
            rule_id = universal_rule_id(self.ns, ch_int)
            resp = _retry_http(
                lambda r=rule, i=rule_id: client.put(
                    f"{self.kibana_url}{V2_RULES}/{i}",
                    headers=_kibana_headers(self.api_key),
                    json=r,
                ),
                label=f"create universal alert rule {rule['metadata']['name']}",
            )
            if resp is not None and resp.status_code < 300:
                rules_done += 1
                step.items_done += 1
            elif resp is not None:
                logger.warning(
                    "Universal alert rule %s failed: %s", rule["metadata"]["name"], resp.text[:200]
                )
            notify(self.progress)

        for policy_id, policy in policies.items():
            resp = _retry_http(
                lambda b=policy, i=policy_id: client.put(
                    f"{self.kibana_url}{V2_POLICIES}/{i}",
                    headers=_kibana_headers(self.api_key),
                    json=b,
                ),
                label=f"create action policy {policy_id}",
            )
            if resp is not None and resp.status_code < 300:
                policies_done += 1
                step.items_done += 1
            elif resp is not None:
                logger.warning("Action policy %s failed: %s", policy_id, resp.text[:200])
            notify(self.progress)

        # Rules without their policies never reach a workflow, so that is a failed step.
        step.status = "ok" if rules_done > 0 and policies_done == len(policies) else "failed"
        step.detail = (
            f"Created {rules_done}/{len(registry)} universal alert rules, "
            f"{policies_done}/{len(policies)} action policies; {nightshift_detail}"
        )
        notify(self.progress)

    def _cleanup_universal(self, client: httpx.Client) -> tuple[int, int]:
        """[serverless] Delete this scenario's Universal rules, action policies and Nightshift automation.

        Rules are found by their fixed ids plus is_scenario_alert_rule (kind=alert,
        the "{scenario_name}: " prefix and the ns tag, or the legacy
        "{scenario_name} CH" prefix).  A name-contains match would also hit the
        scenario's Nightshift signal rules ("{scenario_name} signal — …"), which are not
        ours to delete.
        """
        scenario_name = self.scenario.scenario_name
        rule_ids = {universal_rule_id(self.ns, int(n)) for n in self.scenario.channel_registry}
        policy_ids = set(universal_policy_ids(self.ns).values())

        for page in range(1, 11):
            resp = _retry_http(
                lambda: client.get(
                    f"{self.kibana_url}{V2_RULES}",
                    params={"page": page, "per_page": 100, "search": scenario_name},
                    headers=_kibana_headers(self.api_key),
                ),
                label=f"find universal rules page={page}",
            )
            if resp is None or resp.status_code >= 300:
                break
            try:
                items = resp.json().get("items", [])
            except Exception:
                break
            if not items:
                break
            for rule in items:
                if is_scenario_alert_rule(rule, self.ns, scenario_name) and rule.get("id"):
                    rule_ids.add(rule["id"])

        def _delete_one(path: str) -> bool:
            resp = _retry_http(
                lambda: client.delete(f"{self.kibana_url}{path}", headers=_kibana_headers(self.api_key)),
                label=f"delete {path}",
            )
            return resp is not None and (resp.status_code < 300 or resp.status_code == 404)

        paths = (
            [f"{V2_RULES}/{i}" for i in sorted(rule_ids)]
            + [f"{V2_POLICIES}/{i}" for i in sorted(policy_ids)]
            + [f"{NIGHTSHIFT_AUTOMATIONS}/{i}" for i in self._find_nightshift_automation_ids(client)]
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            deleted = sum(pool.map(_delete_one, paths))
        return deleted, len(paths) - deleted

    def _cleanup_alerts(self, client: httpx.Client) -> tuple[int, int]:
        """Delete alert rules belonging to this scenario.

        Primary: name-based search (rules named "{scenario_name}: <channel name>").
        Fallback: tag-based search (old-style rules tagged with namespace).

        IDs are collected across all search phases first, then deleted
        concurrently so retry backoff on one rule doesn't block others.

        Returns ``(deleted, remaining)`` — remaining > 0 indicates rules that
        couldn't be removed (e.g. transient API errors despite retries).
        """
        scenario_name = self.scenario.scenario_name
        rule_ids: set[str] = set()

        # Phase 1: name-based (rules whose name contains the scenario name)
        for page in range(1, 11):
            resp = _retry_http(
                lambda: client.get(
                    f"{self.kibana_url}/api/alerting/rules/_find",
                    params={"per_page": 100, "page": page, "search_fields": "name", "search": scenario_name},
                    headers=_kibana_headers(self.api_key),
                ),
                label=f"find alerts by name page={page}",
            )
            if resp is None or resp.status_code >= 300:
                break
            try:
                rules = resp.json().get("data", [])
            except Exception:
                break
            if not rules:
                break
            for rule in rules:
                if scenario_name in rule.get("name", "") and rule.get("id"):
                    rule_ids.add(rule["id"])

        # Phase 2: tag-based (rules tagged with the namespace)
        for page in range(1, 11):
            resp = _retry_http(
                lambda: client.get(
                    f"{self.kibana_url}/api/alerting/rules/_find?per_page=100&page={page}"
                    f"&filter=alert.attributes.tags:{self.ns}",
                    headers=_kibana_headers(self.api_key),
                ),
                label=f"find alerts by tag page={page}",
            )
            if resp is None or resp.status_code >= 300:
                break
            try:
                rules = resp.json().get("data", [])
            except Exception:
                break
            if not rules:
                break
            for rule in rules:
                if rule.get("id"):
                    rule_ids.add(rule["id"])

        # Delete all collected IDs concurrently — httpx.Client connection pool is
        # thread-safe, and concurrent deletes avoid serial retry-backoff stacking.
        def _delete_one(rule_id: str) -> bool:
            resp = _retry_http(
                lambda: client.delete(
                    f"{self.kibana_url}/api/alerting/rule/{rule_id}",
                    headers=_kibana_headers(self.api_key),
                ),
                label=f"delete alert rule {rule_id}",
            )
            return resp is not None and (resp.status_code < 300 or resp.status_code == 404)

        deleted = 0
        if rule_ids:
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                deleted = sum(pool.map(_delete_one, rule_ids))

        # remaining is derived directly from delete responses — no post-delete
        # search verify. Kibana's saved-object search index is eventually
        # consistent and returns stale results for seconds after deletion,
        # causing false "still present" failures when all deletes succeeded.
        remaining = len(rule_ids) - deleted

        if self._stack_generation() == "serverless":
            v2_deleted, v2_remaining = self._cleanup_universal(client)
            deleted += v2_deleted
            remaining += v2_remaining
        return deleted, remaining
