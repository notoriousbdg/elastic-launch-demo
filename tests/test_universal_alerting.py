"""Serverless chaos rules are Universal (v2) rules routed by action policies.

9.5 and 9.6 keep the classic ``.es-query`` rules with a ``.workflows`` action; these
tests pin both paths and keep the ``*_serverless.yaml`` workflow variants in step with
the classic files they replace.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import httpx
import pytest
import yaml

from elastic_config.deployer import ScenarioDeployer
from elastic_config.deployer_alerting import (
    AlertingMixin,
    nightshift_automation_body,
    nightshift_workflow_yaml,
    universal_policy_bodies,
    universal_policy_ids,
    universal_rule_body,
    universal_rule_id,
)
from elastic_config.deployer_workflows import WorkflowsMixin

WORKFLOWS = Path(__file__).resolve().parent.parent / "elastic_config" / "workflows"

REGISTRY = {
    n: {
        "name": f"Fault {n}",
        "error_type": f"ERR-{n}",
        "subsystem": "core",
        "remediation_action": f"fix_{n}",
    }
    for n in (1, 6, 7, 15, 16, 19, 20)
}


# ── rule / policy bodies ──────────────────────────────────────────────────────


@pytest.mark.parametrize("ch", sorted(REGISTRY))
def test_rule_body_is_a_complete_alert_rule(ch):
    body = universal_rule_body("demo", "Demo", ch, REGISTRY[ch])
    assert body["kind"] == "alert"
    # kind=alert requires both, the API rejects the rule without them
    assert body["recovery"] == {"strategy": "no_breach"}
    assert body["no_data"] == {"strategy": "ignore"}
    assert body["query"]["breach"]["segment"] == "WHERE match_count > 0"
    assert body["metadata"]["routing_tags"] == ["demo-auto" if ch >= 16 else "demo-hitl"]
    # same 4-tag encoding as the classic rule
    assert body["metadata"]["tags"] == ["demo", f"ERR-{ch}", f"fix_{ch}", str(ch)]
    assert ("(Auto-Remediate)" in body["metadata"]["name"]) == (ch >= 16)
    assert len(body["metadata"]["name"]) <= 160


@pytest.mark.parametrize("ch", sorted(REGISTRY))
def test_rule_query_carries_every_value_the_workflow_reads(ch):
    q = universal_rule_body("demo", "Demo", ch, REGISTRY[ch])["query"]["base"]
    assert q.startswith("FROM logs.otel.demo,logs.otel.demo.* | WHERE KQL(")
    for col in ("error_type", "remediation_action", "channel", "subsystem", "severity", "rule_name"):
        assert f"{col} = " in q
    assert f"channel = {ch}," in q
    assert f'error_type = "ERR-{ch}"' in q


def test_rule_severity_matches_the_classic_mapping():
    sev = {
        ch: re.search(r'severity = "(\w+)"', universal_rule_body("d", "D", ch, REGISTRY[ch])["query"]["base"]).group(1)
        for ch in REGISTRY
    }
    assert sev == {1: "high", 6: "high", 7: "medium", 15: "medium", 16: "medium", 19: "critical", 20: "critical"}


def test_rule_query_escapes_quotes_in_values():
    body = universal_rule_body("d", "D", 1, {**REGISTRY[1], "error_type": 'A"B', "name": 'say "hi"'})
    assert 'error_type = "A\\"B"' in body["query"]["base"]
    assert 'KQL("body.text: \\"A\\\\\\"B\\"' in body["query"]["base"]


def test_policies_route_once_per_fault_and_close_on_inactive():
    p = universal_policy_bodies("demo", "Demo", "wf-hitl", "wf-auto", "wf-close", "wf-nightshift")
    ids = universal_policy_ids("demo")
    assert set(p) == set(ids.values())

    hitl, auto, close = p[ids["hitl"]], p[ids["auto"]], p[ids["close"]]
    assert hitl["destinations"] == [{"id": "wf-hitl", "type": "workflow"}]
    assert auto["destinations"] == [{"id": "wf-auto", "type": "workflow"}]
    assert close["destinations"] == [{"id": "wf-close", "type": "workflow"}]
    # only `active` reaches the RCA workflows; pending/recovering/inactive would
    # each open another case
    assert hitl["matcher"] == {"tags": ["demo-hitl"], "expression": "alert_status: active"}
    assert auto["matcher"] == {"tags": ["demo-auto"], "expression": "alert_status: active"}
    assert close["matcher"] == {"tags": ["demo-hitl", "demo-auto"], "expression": "alert_status: inactive"}
    # the Nightshift policy runs the automation's own workflow once per fault, for every channel
    assert p[ids["nightshift"]]["destinations"] == [{"id": "wf-nightshift", "type": "workflow"}]
    assert p[ids["nightshift"]]["matcher"] == {"tags": ["demo-hitl", "demo-auto"], "expression": "alert_status: active"}
    for policy in p.values():
        assert policy["grouping"] == {"mode": "per_alert"}
        assert policy["throttle"] == {"strategy": "on_status_change"}


def test_policy_without_a_workflow_id_is_left_out():
    p = universal_policy_bodies("demo", "Demo", "wf-hitl", "wf-auto", "")
    ids = universal_policy_ids("demo")
    assert ids["close"] not in p and ids["nightshift"] not in p


# ── deploy / cleanup against a mocked Kibana ──────────────────────────────────


class _Step:
    def __init__(self):
        self.status = ""
        self.detail = ""
        self.items_done = 0
        self.items_total = 0


class _Scenario:
    scenario_name = "Demo"
    namespace = "demo"
    channel_registry = REGISTRY
    agent_config = {"id": "demo-analyst"}


class _Deployer(AlertingMixin, WorkflowsMixin):
    kibana_url = "https://kb.example"
    kibana_display_url = "https://kb.example"
    elastic_url = "https://es.example"
    api_key = "k"
    ns = "demo"
    scenario = _Scenario()

    _stack_generation = ScenarioDeployer._stack_generation
    _is_serverless = ScenarioDeployer._is_serverless
    _kibana_version_ge = ScenarioDeployer._kibana_version_ge

    def __init__(self, flavor: str, version: str):
        self._kibana_env = (flavor, version)
        self._step_obj = _Step()
        self.progress = None
        self._workflow_ids = {
            "significant_event_notification": "wf-hitl",
            "significant_event_notification_auto_remediate": "wf-auto",
            "significant_event_case_close": "wf-close",
        }

    def _step(self, _idx):
        return self._step_obj


SERVERLESS = ("serverless", "9.7.0")
ECH_95 = ("traditional", "9.5.4")


RUN_AS = "svc-account-1"
BACKING_YAML = f"name: Demo Investigation\nsettings:\n  run_as: {RUN_AS}\ntriggers:\n- type: alerting.v1.alertStatusChanged\n"


def _kibana(seen: list, *, v2_rules=None, classic_rules=None, status_for=None, automations=None,
            workflow_valid=True):
    def handle(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path, request.content))
        if status_for and status_for(request):
            return httpx.Response(status_for(request), json={})
        path, method = request.url.path, request.method
        if path == "/internal/nightshift/automations" and method == "POST":
            return httpx.Response(200, json={"id": "auto-1", "workflowId": "demo-investigation"})
        if path == "/internal/nightshift/automations" and method == "GET":
            return httpx.Response(200, json={"automations": automations or [], "total": len(automations or [])})
        if path == "/api/workflows/workflow/demo-investigation" and method == "GET":
            return httpx.Response(200, json={"yaml": BACKING_YAML})
        if path == "/api/workflows/workflow/demo-investigation" and method == "PUT":
            return httpx.Response(200, json={"valid": workflow_valid, "validationErrors": [] if workflow_valid else ["bad"]})
        if path == "/api/alerting/v2/rules" and method == "GET":
            return httpx.Response(200, json={"items": v2_rules or []})
        if path == "/api/alerting/rules/_find":
            return httpx.Response(200, json={"data": classic_rules or [], "total": len(classic_rules or [])})
        if method == "PUT":
            return httpx.Response(201, json={})
        if method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(200, json={})

    return httpx.Client(transport=httpx.MockTransport(handle))


def test_serverless_deploy_creates_universal_rules_and_policies():
    seen: list = []
    d = _Deployer(*SERVERLESS)
    d._deploy_alerting(_kibana(seen), lambda _p: None)

    puts = [(p, json.loads(c)) for m, p, c in seen if m == "PUT"]
    rules = {p: b for p, b in puts if p.startswith("/api/alerting/v2/rules/")}
    policies = {p: b for p, b in puts if p.startswith("/api/alerting/v2/action_policies/")}
    assert set(rules) == {f"/api/alerting/v2/rules/{universal_rule_id('demo', n)}" for n in REGISTRY}
    assert set(policies) == {f"/api/alerting/v2/action_policies/{i}" for i in universal_policy_ids("demo").values()}
    assert not [1 for m, p, _ in seen if m == "POST" and p == "/api/alerting/rule"]
    assert d._step_obj.status == "ok"
    assert d._step_obj.detail == (
        "Created 7/7 universal alert rules, 4/4 action policies; Nightshift automation created"
    )


def test_serverless_deploy_fails_when_a_policy_cannot_be_created():
    seen: list = []
    d = _Deployer(*SERVERLESS)
    d._deploy_alerting(
        _kibana(seen, status_for=lambda r: 400 if r.url.path.endswith("/demo-rca-hitl") else 0),
        lambda _p: None,
    )
    # rules without their policy never start a workflow
    assert d._step_obj.status == "failed"
    assert "3/4 action policies" in d._step_obj.detail


def test_95_deploy_still_creates_classic_rules_with_workflow_actions():
    seen: list = []
    d = _Deployer(*ECH_95)
    d._deploy_alerting(_kibana(seen), lambda _p: None)

    assert not [1 for _, p, _ in seen if "/alerting/v2" in p or "/nightshift" in p]
    posts = [json.loads(c) for m, p, c in seen if m == "POST" and p == "/api/alerting/rule"]
    assert len(posts) == len(REGISTRY)
    for body in posts:
        assert body["rule_type_id"] == ".es-query"
        action = body["actions"][0]
        assert action["id"] == "system-connector-.workflows"
        ch = int(body["tags"][3])
        assert action["params"]["subActionParams"]["workflowId"] == ("wf-auto" if ch >= 16 else "wf-hitl")


def test_serverless_cleanup_leaves_the_nightshift_signal_rules_alone():
    seen: list = []
    d = _Deployer(*SERVERLESS)
    listed = [
        {"id": "old-ch03", "kind": "alert", "metadata": {"name": "Demo CH03: Fault 3"}},
        {"id": "sig-1", "kind": "signal", "metadata": {"name": "Demo: SE CH 01: Fault 1 (match count)"}},
        {"id": "user-rule", "kind": "alert", "metadata": {"name": "Demo latency (mine)"}},
    ]
    deleted, remaining = d._cleanup_alerts(_kibana(seen, v2_rules=listed))

    gone = {p for m, p, _ in seen if m == "DELETE"}
    assert "/api/alerting/v2/rules/old-ch03" in gone
    assert {f"/api/alerting/v2/rules/{universal_rule_id('demo', n)}" for n in REGISTRY} <= gone
    assert {f"/api/alerting/v2/action_policies/{i}" for i in universal_policy_ids("demo").values()} <= gone
    assert "/api/alerting/v2/rules/sig-1" not in gone
    assert "/api/alerting/v2/rules/user-rule" not in gone
    assert remaining == 0


def test_95_cleanup_never_calls_the_v2_api():
    seen: list = []
    _Deployer(*ECH_95)._cleanup_alerts(_kibana(seen))
    assert not [1 for _, p, _ in seen if "/alerting/v2" in p or "/nightshift" in p]


# ── Nightshift automation ─────────────────────────────────────────────────────


def test_nightshift_automation_body_is_enabled_and_scoped_to_the_scenario():
    body = nightshift_automation_body("demo", "Demo")
    assert body["name"] == "Demo Investigation"
    assert body["isEnabled"] is True  # the API defaults to disabled
    assert body["trigger"]["rows"] == [
        {"kind": "alert", "ruleNamePattern": "Demo CH", "ruleNameMatchMode": "substring"}
    ]
    assert body["execution"] == {"reasoningMode": "investigate"}
    assert body["completions"] == [] and body["runtime"] == {"dailyDispatchLimit": 20}
    # distinct scenarios never share a name, which is what teardown matches on
    assert nightshift_automation_body("x", "Other")["name"] != body["name"]


def test_nightshift_workflow_is_started_by_a_policy_not_an_alert_event():
    doc = yaml.safe_load(nightshift_workflow_yaml("auto-1", "Demo Investigation", RUN_AS))
    assert doc["name"] == "Demo Investigation"
    assert doc["settings"]["run_as"] == RUN_AS
    assert doc["triggers"] == [{"type": "manual"}]
    step = doc["steps"][0]
    # only the automation's own workflow may start its investigations
    assert step["type"] == "nightshift.triggerInvestigation" and step["with"]["automation_id"] == "auto-1"
    assert step["with"]["subject_type"] == "alert"
    assert step["with"]["subject_id"] == "{{ inputs.payload.alerts[0].alert_id }}"
    # the dispatcher notifies twice; the second run is dropped
    assert doc["settings"]["concurrency"] == {
        "key": "auto-1-{{ inputs.payload.alerts[0].alert_id }}", "strategy": "drop", "max": 1,
    }
    assert not re.search(r"\bevent\.", yaml.safe_dump(doc))


def test_serverless_deploy_replaces_the_generated_workflow_and_keeps_its_service_account():
    seen: list = []
    _Deployer(*SERVERLESS)._deploy_alerting(_kibana(seen), lambda _p: None)

    created = [json.loads(c) for m, p, c in seen if m == "POST" and p == "/internal/nightshift/automations"]
    assert len(created) == 1 and created[0]["name"] == "Demo Investigation"
    put = next(json.loads(c) for m, p, c in seen if m == "PUT" and p == "/api/workflows/workflow/demo-investigation")
    doc = yaml.safe_load(put["yaml"])
    assert doc["settings"]["run_as"] == RUN_AS and doc["triggers"] == [{"type": "manual"}]
    # the 4th policy runs the automation's own workflow
    policy = next(
        json.loads(c) for m, p, c in seen
        if m == "PUT" and p == "/api/alerting/v2/action_policies/demo-nightshift"
    )
    assert policy["destinations"] == [{"id": "demo-investigation", "type": "workflow"}]


def test_nightshift_failure_does_not_fail_the_alert_rules_step():
    seen: list = []
    d = _Deployer(*SERVERLESS)
    d._deploy_alerting(
        _kibana(seen, status_for=lambda r: 403 if r.url.path == "/internal/nightshift/automations" and r.method == "POST" else 0),
        lambda _p: None,
    )
    assert d._step_obj.status == "ok"
    assert d._step_obj.detail == (
        "Created 7/7 universal alert rules, 3/3 action policies; Nightshift automation not created (HTTP 403)"
    )
    assert not [1 for m, p, _ in seen if m == "PUT" and p.endswith("/demo-nightshift")]


def test_a_workflow_that_will_not_save_removes_the_automation_again():
    seen: list = []
    d = _Deployer(*SERVERLESS)
    d._deploy_alerting(_kibana(seen, workflow_valid=False), lambda _p: None)
    assert ("DELETE", "/internal/nightshift/automations/auto-1") in [(m, p) for m, p, _ in seen]
    assert d._step_obj.status == "ok" and "workflow update failed" in d._step_obj.detail
    assert not [1 for m, p, _ in seen if m == "PUT" and p.endswith("/demo-nightshift")]


def test_cleanup_deletes_only_this_scenarios_automation():
    seen: list = []
    listed = [
        {"id": "mine", "name": "Demo Investigation"},
        {"id": "sibling", "name": "Other Scenario Investigation"},
        {"id": "sample", "name": "Test automation"},
    ]
    _, remaining = _Deployer(*SERVERLESS)._cleanup_alerts(_kibana(seen, automations=listed))
    gone = {p for m, p, _ in seen if m == "DELETE"}
    assert "/internal/nightshift/automations/mine" in gone
    assert not gone & {"/internal/nightshift/automations/sibling", "/internal/nightshift/automations/sample"}
    assert "/api/alerting/v2/action_policies/demo-nightshift" in gone
    assert remaining == 0


def test_cleanup_without_nightshift_still_cleans_the_rest():
    seen: list = []
    _, remaining = _Deployer(*SERVERLESS)._cleanup_alerts(
        _kibana(seen, status_for=lambda r: 404 if r.url.path == "/internal/nightshift/automations" else 0)
    )
    assert remaining == 0
    assert not [1 for m, p, _ in seen if m == "DELETE" and "/nightshift/" in p]


# ── workflow variants ─────────────────────────────────────────────────────────


def _yamls(gen):
    d = _Deployer(*gen)
    return d._generate_workflow_yamls(sonnet_connector_id=None, haiku_connector_id=None)


def test_serverless_deploys_the_variants_under_the_classic_keys():
    yamls = _yamls(SERVERLESS)
    assert not [k for k in yamls if k.endswith("_serverless")]
    for key in ("significant_event_notification", "significant_event_notification_auto_remediate", "significant_event_case_close"):
        doc = yaml.safe_load(yamls[key])
        assert [t["type"] for t in doc["triggers"]] == ["manual"], key
        assert not re.search(r"\bevent\.(rule|alerts|spaceId)", yamls[key]), key
        assert "inputs.payload" in yamls[key], key
    # the classic poller would treat every v2 alert as recovered and close all open cases
    assert "/api/alerting/rules/_find" not in yamls["significant_event_case_close"]


def test_95_and_96_deploy_only_the_classic_workflows():
    for gen in (ECH_95, ("traditional", "9.6.0")):
        yamls = _yamls(gen)
        assert not [k for k in yamls if k.endswith("_serverless")]
        assert "type: alert" in yamls["significant_event_notification"]
        assert "execution_status" in yamls["significant_event_case_close"]


# What a *_serverless.yaml notification workflow changes relative to its classic file.
_GATE = "${{ event.alerts[0].kibana.alert.action_group != 'recovered' }}"
_WON = "${{ steps.claim_alert.output.result == 'created' }}"
_P = "inputs.payload.alerts[0]"
_SUBS = [
    ("event.rule.tags[1]", f"{_P}.data.error_type"),
    ("event.rule.tags[2]", f"{_P}.data.remediation_action"),
    ("event.rule.tags[3]", f"{_P}.data.channel"),
    ("event.rule.name", f"{_P}.data.rule_name"),
    ("event.rule.id", f"{_P}.rule_id"),
    ("event.alerts[0]._id", f"{_P}.alert_id"),
    ('"{{ event.alerts[0]._index }}"', '".rule-events"'),
    ("{{ event | json }}", "{{ inputs.payload | json }}"),
]


def _load(name):
    return yaml.safe_load((WORKFLOWS / name).read_text().replace("__", "x"))


@pytest.mark.parametrize("base", ["significant_event_notification", "significant_event_notification_auto_remediate"])
def test_serverless_notification_workflow_matches_its_classic_file(base):
    classic, serverless = _load(f"{base}.yaml"), _load(f"{base}_serverless.yaml")

    steps = [s for s in classic["steps"] if s["name"] != "log_skipped_recovery"]
    for step in steps:
        cond = step.get("if")
        if cond == _GATE:
            step["if"] = _WON
        elif cond:
            step["if"] = cond.replace(
                "event.alerts[0].kibana.alert.action_group != 'recovered' and ",
                "steps.claim_alert.output.result == 'created' and ",
            )
    raw = json.dumps(steps)
    for old, new in _SUBS:
        raw = raw.replace(old, new)

    # the claim step is new; everything after it is the classic workflow, gated on winning the claim
    claim, rest = serverless["steps"][0], serverless["steps"][1:]
    assert claim["name"] == "claim_alert" and claim["type"] == "elasticsearch.index"
    assert claim["with"]["id"] == "claim-{{ inputs.payload.alerts[0].alert_id }}"
    assert rest == json.loads(raw)
    assert all("steps.claim_alert.output.result == 'created'" in s["if"] for s in rest)
    assert serverless["name"] == classic["name"]
    assert serverless["tags"] == classic["tags"]
    assert serverless["enabled"] is True
