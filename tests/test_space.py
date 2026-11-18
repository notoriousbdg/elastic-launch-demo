"""NOVA-7 (space) at schema 1.3 and the data-driven opsctl profile behind it.

Per-channel before/during/after telemetry is tested in tests/test_space_*.py; this file
covers the scenario-wide contract: the ops profile lives in the scenario folder and
travels with it, scenarios can define their own vocabulary without engine changes,
and the remediation-path pieces (actors, auto-remediated channels, audit lines) come
from data.
"""
from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from app import ops, ops_profile
from tests._space_harness import PROFILE, REG, SCN, controller, phase

ROOT = Path(__file__).resolve().parent.parent


def _inst(ctl, scenario=SCN):
    return SimpleNamespace(
        deployment_id="dep-1",
        chaos_controller=ctl,
        ctx=SimpleNamespace(services={n: {} for n in scenario._service_specs}, scenario=scenario),
        resolve_channel=lambda ch, **kw: ctl.resolve(ch, **kw),
    )


def _drain(ctl) -> list[dict]:
    with ctl._lock:
        for e in ctl._pending_events:
            e["emit_at"] = 0
        out, ctl._pending_events = list(ctl._pending_events), []
    return out


# ── The scenario ───────────────────────────────────────────────────────────


def test_space_is_schema_13_with_runbook_commands():
    assert SCN.schema_version == "1.3"
    for ch, data in REG.items():
        cmds = ops.expected_commands(data.get("remediation_command"), PROFILE)
        lines = [ln for ln in str(data.get("remediation_command") or "").splitlines() if ln.strip()]
        assert cmds and len(cmds) == len(lines), (ch, data.get("remediation_command"))
        assert (data.get("remediation_effects") or {}).get("verification"), ch


def test_every_fix_leaves_one_command_line_per_step_on_mission_control():
    for ch, data in REG.items():
        steps = ops.expected_commands(data["remediation_command"], PROFILE)
        lines = [e for e in data["remediation_effects"].get("log_events") or []
                 if "{actor}" in str(e.get("body"))]
        assert len(lines) == len(steps), (ch, [e["body"] for e in lines])
        for e in lines:
            assert e.get("service") == "mission-control", (ch, e)
            assert str(e["body"]).startswith("[MCC] CMD ACCEPTED opsctl "), (ch, e["body"])


@pytest.mark.parametrize("ch", sorted(REG))
def test_opsctl_runbook_commands_fix_the_channel_with_one_audit_line_each(ch):
    ctl = controller()
    ctl.trigger(ch, user_email="flight@example.com")
    inst = _inst(ctl)
    steps = ops.expected_commands(REG[ch]["remediation_command"], PROFILE)
    for step in steps:
        ops.execute(inst, "opsctl " + step.display())
    assert ctl.get_channel_status(ch)["state"] == "STANDBY"
    cmd_lines = [e for e in _drain(ctl) if e["body"].startswith("[MCC] CMD ACCEPTED")]
    assert len(cmd_lines) == len(steps), [e["body"] for e in cmd_lines]
    for e in cmd_lines:
        assert "opsctl/claude-code" in e["body"] and "change_id=chg-" in e["body"], e["body"]
        assert not any(k.startswith("k8s.") for k in e["attributes"]), e["attributes"]


def test_auto_remediation_names_the_onboard_autonomy():
    ctl = controller()
    assert ctl.auto_actor == "fdir/autonomy-manager"
    for ch in ops_profile.auto_remediate_channels(SCN):
        ctl.trigger(ch, user_email="flight@example.com")
        ctl.resolve(ch, force=True, actor=ctl.auto_actor)
    bodies = [e["body"] for e in _drain(ctl)]
    assert any("fdir/autonomy-manager" in b for b in bodies)
    assert not any("opsctl/claude-code" in b or "system:serviceaccount" in b for b in bodies)


def test_a_wrong_command_is_logged_but_fixes_nothing():
    ctl = controller()
    ctl.trigger(2, user_email="flight@example.com")
    r = ops.execute(_inst(ctl), "opsctl restart fts/fts-b")
    assert r["status"] == "applied" and r["message"] == "fts/fts-b power-cycled"
    assert ctl.get_channel_status(2)["state"] == "ACTIVE"
    (line,) = [e for e in _drain(ctl) if e["body"].startswith("[MCC]")]
    assert line["service"] == "mission-control"
    assert line["body"].startswith("[MCC] CMD ACCEPTED opsctl restart fts/fts-b from opsctl/claude-code change_id=")
    assert line["attributes"]["command.target"] == "fts/fts-b"


def test_space_profile_vocabulary():
    verbs = ops_profile.verbs(PROFILE)
    assert {"rollback", "patch", "run", "drain", "replace", "sync", "restore", "apply"} <= set(verbs)
    assert not {"renew", "rotate", "expand"} & set(verbs)  # removed for this vehicle
    with pytest.raises(ops.OpsError):
        ops.parse_command("opsctl rotate key/x", PROFILE)
    assert "logs.otel.nova7" in ops_profile.guide_text(PROFILE)
    assert ops_profile.auto_remediate_channels(SCN) == {16, 17, 18, 19, 20}


# ── Portability: the profile is in the scenario folder ─────────────────────


def test_exported_scenario_carries_its_ops_profile(tmp_path, monkeypatch):
    import scenario_engine
    from scenario_engine.yaml_scenario import load_yaml_scenario

    data = scenario_engine.export_scenario_zip("space")
    monkeypatch.setattr(scenario_engine, "_SCENARIOS_DIR", tmp_path)
    monkeypatch.setattr(scenario_engine, "reload_registry", lambda *a, **k: {}, raising=False)
    scenario_engine.import_scenario_zip(data)
    restored = load_yaml_scenario(tmp_path / "space")
    assert (tmp_path / "space" / "ops.yaml").is_file()
    assert restored.ops_profile == SCN.ops_profile
    ctl = controller()
    body, _ = ops.audit_record(ops.parse_command("patch link/x-band --active=xb-secondary", restored.ops_profile),
                               "chg-1", user="u", profile=restored.ops_profile)
    assert body == "[MCC] CMD ACCEPTED opsctl patch link/x-band --active=xb-secondary from u change_id=chg-1"
    del ctl


def test_scenario_without_ops_yaml_gets_neutral_operations(tmp_path):
    import shutil

    from scenario_engine.yaml_scenario import load_yaml_scenario

    src = shutil.copytree(ROOT / "scenarios" / "financial", tmp_path / "financial")
    (src / "ops.yaml").unlink()
    prof = load_yaml_scenario(src).ops_profile
    assert prof["extends"] == ["neutral"]
    body, attrs = ops.audit_record(ops.parse_command("restart deployment/x -n y", prof), "chg-1",
                                   user="u", profile=prof)
    assert body.startswith('ops: change applied cmd="opsctl restart deployment/x -n y" actor=u')
    assert not any(k.startswith("k8s.") for k in attrs)
    assert ops._side_effects(ops.parse_command("restart deployment/x -n y", prof), "c", "u", prof) == []


def test_a_scenario_can_define_its_own_verb_without_engine_changes():
    prof = ops_profile.resolve({
        "verbs": {"failover": {"summary": "Switch to the redundant unit",
                               "example": "opsctl failover link/<name> --to <unit>",
                               "describe": "{ref} failed over to {flags.to}",
                               "plan": ["~ {ref}: active unit -> {flags.to}"]}},
    })
    cmd = ops.parse_command("opsctl failover link/x-band --to xb-secondary", prof)
    assert (cmd.verb, cmd.target, cmd.flags) == ("failover", "link/x-band", {"to": "xb-secondary"})
    assert ops.matches(cmd, ops.parse_command("failover link/x-band --to=XB-SECONDARY", prof))
    assert ops.describe(cmd, "", prof) == "link/x-band failed over to xb-secondary"
    assert ops.describe_plan(cmd, prof) == ["~ link/x-band: active unit -> xb-secondary"]
    body, _ = ops.audit_record(cmd, "chg-9", user="u", profile=prof)
    assert 'cmd="opsctl failover link/x-band --to=xb-secondary"' in body
    assert {"verb": "failover", "help": "Switch to the redundant unit"} in ops.list_actions(prof)["verbs"]
    with pytest.raises(ops.OpsError):
        ops.parse_command("opsctl failover link/x", ops_profile.neutral())  # not in other scenarios


def test_auto_remediated_channels_are_scenario_data():
    from elastic_config.deployer_alerting import alert_rule_name, universal_rule_body, universal_routing_tag

    scn = SimpleNamespace(ops_profile=ops_profile.resolve({"auto_remediate_channels": [3, 20]}))
    auto = ops_profile.auto_remediate_channels(scn)
    assert auto == {3, 20}
    assert universal_routing_tag("ns", 3, 3 in auto) == "ns-auto"
    assert universal_routing_tag("ns", 16, 16 in auto) == "ns-hitl"
    assert alert_rule_name("S", 3, "X", True).endswith("(Auto-Remediate)")
    body = universal_rule_body("ns", "S", 16, REG[16], 16 in auto)
    assert body["metadata"]["name"] == f"S: {REG[16]['name']}"
    # without a profile the default set applies
    assert universal_routing_tag("ns", 17) == "ns-auto" and universal_routing_tag("ns", 15) == "ns-hitl"


def test_kb_and_guide_follow_the_profile():
    from app.ops_profile import channel_ranges, hitl_and_auto_ranges
    from elastic_config.deployer_kb import KbMixin

    assert channel_ranges([1, 2, 3, 7, 16, 17]) == "1–3, 7, 16–17"
    scn = SimpleNamespace(channel_registry=REG,
                          ops_profile=ops_profile.resolve({"auto_remediate_channels": [3, 20]}))
    assert hitl_and_auto_ranges(scn) == ("1–2, 4–19", "3, 20")
    assert hitl_and_auto_ranges(SCN) == ("1–15", "16–20")
    host = KbMixin()
    host.ns, host._stream_name = "nova7", "logs.otel.nova7"
    host.scenario = SCN
    assert "approval_mode: skip" in host._generate_kb_doc(16, REG[16])
    assert "approval_mode: required" in host._generate_kb_doc(2, REG[2])
    guide = SCN._remediation_guide_skill("nova7", "a", "b", "c")["content"]
    assert "## HITL Channels (1–15)" in guide and "## Auto-Remediate Channels (16–20)" in guide


def test_state_change_rules_from_the_profile():
    prof = ops_profile.resolve({"state_changes": [{
        "verbs": ["patch"], "kinds": ["deployment"], "generator": "kubernetes_otel",
        "state": {"workloads.{name}.memory_limit": "{flags.memory-limit}"}, "transition_seconds": 30,
    }]})
    inst = SimpleNamespace(ctx=SimpleNamespace(scenario=SimpleNamespace(ops_profile=prof)))
    state, secs = ops.change_state(inst, ops.parse_command("patch deployment/api --memory-limit 2Gi", prof))
    assert state == {"kubernetes_otel": {"workloads": {"api": {"memory_limit": "2Gi"}}}} and secs == 30
    assert ops.change_state(inst, ops.parse_command("patch deployment/api --cpu 2", prof)) == ({}, 0.0)


def test_kubernetes_generator_bindings_keep_platform_changes():
    from generator_engine.registry import GENERATORS
    from scenario_engine.yaml_scenario import load_yaml_scenario

    k8s = load_yaml_scenario(ROOT / "scenarios" / "k8splatform")
    assert callable(GENERATORS["kubernetes_otel"].ops_bindings)
    inst = SimpleNamespace(ctx=SimpleNamespace(scenario=k8s))
    cmd = ops.parse_command("patch deployment/ingress-nginx-controller -n ingress-nginx --memory-limit 3G",
                            k8s.ops_profile)
    state, secs = ops.change_state(inst, cmd)
    assert state == {"kubernetes_otel": {"workloads": {"ingress-nginx-controller": {"memory_limit": "3G"}}}}
    assert secs == 90.0


def test_dsl_sees_applied_changes_and_time_since_fix():
    ctl = controller()
    inst = _inst(ctl)
    ops.execute(inst, "opsctl patch link/x-band --active=xb-secondary")  # nothing active: no fix
    assert ctl.get_applied()["link/x-band"]["active"] == "xb-secondary"
    assert ctl.get_since_fix() == {}
    ctl = phase(2, "after", since=42)
    assert 41 <= ctl.get_since_fix()[2] <= 44
    ctl.reset_platform_state()
    assert ctl.get_applied() == {} and ctl.get_since_fix() == {}


def test_profile_endpoint_serves_the_running_scenarios_vocabulary(monkeypatch):
    from fastapi.testclient import TestClient

    from app import main

    inst = _inst(controller())
    monkeypatch.setattr(main.registry, "get", {"dep-1": inst}.get)
    monkeypatch.setattr(main.registry, "all_instances", lambda: {"dep-1": inst})
    client = TestClient(main.app)
    view = client.get("/api/ops/profile").json()
    assert view["banner"].startswith("opsctl sends commands to the NOVA-7")
    assert "rollback" in {v["verb"] for v in view["verbs"]} and "rotate" not in {v["verb"] for v in view["verbs"]}
    assert "logs.otel.nova7" in view["guide"]
    assert view["namespaces"] is False and "-n <namespace>" not in view["usage"]
    actions = client.get("/api/ops/actions").json()
    assert "rollback" in {v["verb"] for v in actions["verbs"]}
    r = client.post("/api/ops/actions", json={"argv": ["rotate", "key/x"]})
    assert r.status_code == 400


def test_runbook_uses_the_mission_wording():
    from elastic_config.deployer_kb import _recovery_fact, _scope_fact

    assert _scope_fact({"workloads": ["fuel-system"]}, PROFILE).strip().endswith(
        "Other subsystems are nominal and make a useful comparison.")
    text = _recovery_fact(REG[2], PROFILE)
    assert "Mission Control command log" in text and "node group" not in text and "kubelet" not in text
    assert "metrics-generic.otel-default" in text and "logs.otel.nova7" in text


def test_ops_yaml_passes_check_45():
    import importlib.util

    spec = importlib.util.spec_from_file_location("verify", ROOT / "scripts" / "verify_yaml_scenarios.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for s in (SCN,):
        errors, _ = mod.check_ops_profile(s)
        assert errors == []
    bad = SimpleNamespace(_scenario_dir=None)
    assert mod.check_ops_profile(bad) == ([], [])


def test_since_fix_lets_a_value_converge():
    ctl = phase(2, "after", since=0)
    t0 = ctl.get_since_fix()[2]
    time.sleep(0.01)
    assert ctl.get_since_fix()[2] > t0
