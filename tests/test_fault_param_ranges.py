"""Docs and skills describe fault parameters as ranges and point at the real evidence."""

from scenario_engine import get_scenario
from scenario_engine.fault_spec import describe


def test_describe_spec_forms():
    assert describe({"uniform": [2.0, 8.0], "round": 2}) == "2.0–8.0"
    assert describe({"randint": [10, 50]}) == "10–50"
    assert describe({"choice": ["a", "b"]}) == "one of a, b"
    assert describe({"choice": [1, 2, 3, 4, 5]}) == "one of 1, 2, 3, 4, ..."
    assert describe(117) == "117"


def test_channel_runbook_skill_uses_ranges_and_lists_evidence():
    sc = get_scenario("aws")
    skills = {s["id"]: s for s in sc.skill_definitions}
    runbook = skills[f"{sc.namespace}-channel-runbook"]["content"]
    assert sc.fault_param_ranges(1)["refused_connections"] == "2400–3200"
    assert "2400–3200" in runbook
    assert "**Evidence** (span attributes reported only while this fault is active)" in runbook


def test_skills_point_traces_at_the_real_index():
    sc = get_scenario("space")
    content = "\n".join(s["content"] for s in sc.skill_definitions)
    assert "pattern with `traces.otel." not in content
    assert "traces-generic.otel-default" in content
    assert "attributes.exception.stacktrace" in content


def test_single_cloud_scenarios_do_not_mention_other_clouds():
    for sid, absent in (("aws", ("GCP", "Azure")), ("gcp", ("AWS", "Azure")), ("azure", ("AWS", "GCP"))):
        sc = get_scenario(sid)
        playbook = {s["id"]: s for s in sc.skill_definitions}[f"{sc.namespace}-investigation-playbook"]
        method = playbook["content"].split("## Root Cause Analysis Methodology", 1)[1].split("## Response", 1)[0]
        assert not any(cloud in method for cloud in absent), sid
        assert "Service Tracing" in method
    space = get_scenario("space")
    playbook = {s["id"]: s for s in space.skill_definitions}["nova7-investigation-playbook"]["content"]
    assert "across AWS, Azure and GCP" in playbook
    assert "**Confirm with evidence**" in playbook


def test_authoring_skills_carry_the_evidence_contract():
    from pathlib import Path
    skills = Path(__file__).resolve().parent.parent / ".claude" / "skills"
    channels = (skills / "add-scenario" / "contract" / "channels.md").read_text()
    assert "### Evidence contract" in channels and "Check 35" in channels
    assert "Check 36" in (skills / "upgrade-scenario" / "SKILL.md").read_text()
