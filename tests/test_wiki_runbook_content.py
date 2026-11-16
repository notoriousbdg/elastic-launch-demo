"""Nightshift Wiki runbook text: no demo tells, logs-first evidence, exact fix and checks."""
from __future__ import annotations

import re

from elastic_config.deployer_kb import KbMixin

CH_DATA = {
    "name": "Edge Pool Exhaustion",
    "error_type": "EDGE-POOL-EXHAUSTED",
    "subsystem": "edge",
    "affected_services": ["api"],
    "cascade_services": ["web"],
    "description": "The edge pool runs out of connections.",
    "remediation_action": "restart_edge",
}


class _Scenario:
    namespace = "demo"
    scenario_name = "Demo Corp"
    channel_registry = {7: CH_DATA}

    def prefixed_tool_id(self, name: str) -> str:
        return f"demo.{name}"

    def fault_param_ranges(self, channel: int) -> dict[str, str]:
        return {}

    def rca_clue_ranges(self, channel: int, service_name: str) -> dict[str, str]:
        return {"edge.queue_depth": "40–60"} if service_name == "api" else {}

    def get_correlation_attribute(self, channel: int, is_error: bool, rng) -> dict:
        return {"runtime.pool": "small"}


def _host() -> KbMixin:
    host = KbMixin()
    host.ns = "demo"
    host.scenario = _Scenario()
    host._stream_name = "logs.otel.demo"
    return host


def _runbook(**extra) -> str:
    return _host()._generate_wiki_runbook(7, {**CH_DATA, **extra})


def test_runbook_has_no_channel_or_demo_wording():
    text = _runbook()
    assert not re.search(r"\bchannels?\b", text, re.IGNORECASE)
    assert not re.search(r"\b(demo|scenario|chaos)\b", text.replace("logs.otel.demo", ""), re.IGNORECASE)
    assert text.startswith("## Overview\n\nEdge Pool Exhaustion (edge).\n\nThe edge pool runs out")
    assert "- Remediation: run the `restart_edge` action." in text


def test_remediation_command_is_a_code_block():
    cmd = "opsctl rollback deploy/api --to-revision 41\nopsctl scale deploy/api --replicas 6"
    text = _runbook(remediation_command=cmd)
    assert f"- Remediation: run\n\n```\n{cmd}\n```\n" in text
    assert "restart_edge" not in text


def test_verification_signals_are_listed_before_the_generic_check():
    text = _runbook(remediation_effects={"verification": [
        {"signal": "k8s.container.restarts", "where": "deployment api", "expect": "flat",
         "within_seconds": 120},
        {"signal": "attributes.edge.queue_depth", "where": "service api", "expect": "below 10",
         "within_seconds": 60},
    ]})
    block = text.split("- Recovery is confirmed when:\n", 1)[1]
    lines = block.splitlines()
    assert lines[0] == "  - `k8s.container.restarts` where deployment api: flat (within 120s)"
    assert lines[1] == "  - `attributes.edge.queue_depth` where service api: below 10 (within 60s)"
    assert lines[2].startswith("  - The error and warning counts for `EDGE-POOL-EXHAUSTED` drop")


def test_without_verification_the_generic_recovery_sentence_stays():
    assert "- Recovery is confirmed when the error and warning counts for `EDGE-POOL-EXHAUSTED`" in _runbook()


def test_fault_scope_fact():
    text = _runbook(fault_scope={"cluster": "prod-use1", "node_pool": "edge-c6i"})
    assert (
        "- Scope: cluster prod-use1, node pool edge-c6i. Other clusters, pools and zones are "
        "unaffected and make a useful comparison." in text
    )
    assert "- Scope:" not in _runbook()


def test_evidence_section_leads_with_log_attributes():
    text = _host()._evidence_section(7, CH_DATA)
    log_pos = text.index("ERROR log lines in `logs.otel.demo`")
    assert "- api: `attributes.edge.queue_depth` (40–60)" in text
    assert (
        'FROM logs.otel.demo,logs.otel.demo.* | WHERE resource.attributes.service.name == "api" '
        "AND attributes.edge.queue_depth IS NOT NULL | STATS AVG(attributes.edge.queue_depth) "
        "BY BUCKET(@timestamp, 5 minute)" in text
    )
    # spans are corroboration, after the log evidence
    assert text.index("traces-generic.otel-default") > log_pos
    assert "`attributes.runtime.pool` = `small`" in text
    assert "`attributes.error.type` and, when the component emits one, an exception stack trace" in text
    assert "exception.message" not in text and "exception.stacktrace" not in text
    assert "confirming evidence" in text


def test_nightshift_context_is_neutral():
    text = _host()._nightshift_context_text()
    assert text.startswith("[launch-next:demo]")
    body = text.replace("[launch-next:demo]", "").replace("logs.otel.demo", "").replace(
        "wiki_runbook_demo-", ""
    ).replace("(namespace demo)", "").replace("Demo Corp", "")
    assert not re.search(r"\b(demo|channels?|chaos|scenario)\b", body, re.IGNORECASE)
    assert "Nightshift sandbox: this project" in text
    assert "FROM logs.otel.demo,logs.otel.demo.*" in text
    assert "body.text" in text
    assert "log attributes" in text
    assert "wiki_runbook_demo-" in text and "chNN" not in text
