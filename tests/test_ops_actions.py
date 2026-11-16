"""opsctl / platform operations API (app/ops.py, /api/ops/*)."""

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from app import ops
from app.chaos.controller import ACTIVE, STANDBY, ChaosController


def _channel(name, remediation_command, **extra):
    return {
        "name": name,
        "subsystem": "platform",
        "vehicle_section": "cluster",
        "error_type": "X",
        "sensor_type": "Y",
        "affected_services": ["ingress-gateway"],
        "cascade_services": [],
        "description": name,
        "remediation_command": remediation_command,
        **extra,
    }


REGISTRY = {
    1: _channel("dns", "opsctl rollback configmap/coredns -n kube-system --to-revision 42"),
    2: _channel(
        "cert",
        "opsctl restore iam-policy/cert-manager-route53 --version v3\n"
        "opsctl renew certificate --all -n tenant-edge --not-ready\n",
        remediation_effects={"state": {"certs_ready": True}, "transition_seconds": 60},
    ),
    3: _channel(
        "spot",
        ["opsctl patch nodegroup/batch-spot --instance-types c6i.4xlarge,c6a.4xlarge,m6i.4xlarge"],
    ),
    4: _channel("legacy", None),
}


class FakeInstance:
    def __init__(self, services=("kube-apiserver", "ingress-gateway")):
        self.chaos_controller = ChaosController(channel_registry=REGISTRY)
        self.ctx = SimpleNamespace(services={s: {} for s in services})
        self.deployment_id = "dep-1"

    def resolve_channel(self, channel, **kwargs):
        return self.chaos_controller.resolve(channel, **kwargs)

    def trigger_channel(self, channel, **kwargs):
        return self.chaos_controller.trigger(channel, **kwargs)


def _drain(inst, service="kube-apiserver"):
    ctl = inst.chaos_controller
    with ctl._lock:
        for e in ctl._pending_events:
            e["emit_at"] = 0
    return ctl.drain_events(service)


# ── Parsing ───────────────────────────────────────────────────────────────


def test_parse_normalizes_argv_and_string():
    a = ops.parse_command(["rollback", "cm/CoreDNS", "-n", "kube-system", "--to-revision", "42"])
    b = ops.parse_command("opsctl rollback configmap/coredns --namespace=kube-system --to-revision=42")
    for c in (a, b):
        assert (c.verb, c.target, c.namespace) == ("rollback", "configmap/coredns", "kube-system")
        assert c.flags == {"to-revision": "42"}
        assert not c.dry_run


def test_parse_switches_quoted_values_and_plan():
    c = ops.parse_command("renew certificate --all -n tenant-edge --not-ready")
    assert c.target == "certificate" and c.flags == {"all": True, "not-ready": True}
    c = ops.parse_command('patch harbor/gc-schedule --cron "0 0 3 * * *"')
    assert c.flags["cron"] == "0 0 3 * * *"
    c = ops.parse_command("plan drain node/ip-10-42-36-17.ec2.internal --ignore-daemonsets")
    assert c.dry_run and c.verb == "drain" and c.flags == {"ignore-daemonsets": True}
    c = ops.parse_command("delete ingresses -n tenant-previews -l preview-state=stale")
    assert c.target == "ingress" and c.flags == {"selector": "preview-state=stale"}


@pytest.mark.parametrize("bad", ["", "frobnicate x/y", "rollback", "rollback a/b c/d", "patch x -n"])
def test_parse_rejects_bad_input(bad):
    with pytest.raises(ops.OpsError):
        ops.parse_command(bad)


def test_expected_commands_handles_strings_lists_and_comments():
    cmds = ops.expected_commands("# fix\nopsctl run job/harbor-gc -n harbor\n\nopsctl expand pvc/x \\\n  -n harbor --size 2Ti")
    assert [c.display() for c in cmds] == ["run job/harbor-gc -n harbor", "expand pvc/x -n harbor --size=2Ti"]
    assert ops.expected_commands(None) == []


# ── Matching ──────────────────────────────────────────────────────────────


def _m(inv, exp):
    return ops.matches(ops.parse_command(inv), ops.parse_command(exp))


def test_match_exact_and_flag_subset():
    exp = "rollback configmap/coredns -n kube-system --to-revision 42"
    assert _m(exp, exp)
    assert _m(exp + " --reason incident", exp)  # extra flags allowed
    assert not _m("rollback configmap/coredns -n kube-system", exp)  # missing flag
    assert not _m("rollback configmap/coredns -n kube-system --to-revision 41", exp)


def test_match_previous_revision_is_lenient():
    assert _m("rollback configmap/coredns -n kube-system --to-revision previous",
              "rollback configmap/coredns -n kube-system --to-revision 42")
    assert _m("rollback configmap/coredns -n kube-system --to-revision 42",
              "rollback configmap/coredns -n kube-system --to-revision previous")


def test_match_lists_as_sets_and_case_insensitive_values():
    exp = "patch nodegroup/batch-spot --instance-types c6i.4xlarge,c6a.4xlarge,m6i.4xlarge"
    assert _m("patch nodegroup/batch-spot --instance-types=M6I.4xlarge, c6i.4xlarge,c6a.4xlarge", exp)
    assert not _m("patch nodegroup/batch-spot --instance-types c6i.4xlarge,c6a.4xlarge", exp)
    assert _m("patch deployment/x -n a --memory-limit 3gi", "patch deployment/x -n a --memory-limit 3Gi")


def test_match_namespace_verb_target_must_agree():
    exp = "rollback configmap/coredns -n kube-system --to-revision 42"
    assert not _m("rollback configmap/coredns -n default --to-revision 42", exp)
    assert not _m("rollback configmap/coredns --to-revision 42", exp)
    assert not _m("restart configmap/coredns -n kube-system --to-revision 42", exp)
    assert not _m("rollback configmap/kube-dns -n kube-system --to-revision 42", exp)


# ── Execution against a real controller ───────────────────────────────────


def test_correct_action_resolves_and_audits():
    inst = FakeInstance()
    inst.trigger_channel(1)
    r = ops.execute(inst, ["rollback", "configmap/coredns", "-n", "kube-system", "--to-revision", "42"])
    assert r["status"] == "applied"
    assert r["message"] == "configmap/coredns rolled back to revision 42"
    assert r["change_id"].startswith("chg-")
    assert "channel" not in r and "fixed" not in str(r).lower()
    assert inst.chaos_controller.get_channel_status(1)["state"] == STANDBY
    events = _drain(inst)
    assert len(events) == 1
    e = events[0]
    assert "verb=update" in e["body"] and "resource=configmaps" in e["body"]
    assert "name=coredns" in e["body"] and "namespace=kube-system" in e["body"]
    assert e["attributes"]["change.id"] == r["change_id"]
    assert e["attributes"]["user.name"] == "opsctl/claude-code"
    assert e["attributes"]["k8s.audit.resource"] == "configmaps"


def test_wrong_action_keeps_channel_active_and_audits_side_effects():
    inst = FakeInstance()
    inst.trigger_channel(1)
    r = ops.execute(inst, "restart deployment/coredns -n kube-system")
    assert r["status"] == "applied" and r["message"] == "deployment.apps/coredns restarted"
    assert inst.chaos_controller.get_channel_status(1)["state"] == ACTIVE
    bodies = [e["body"] for e in _drain(inst)]
    assert any("verb=patch" in b and "resource=deployments" in b for b in bodies)
    assert any("restartedAt" in b for b in bodies)


def test_two_line_command_requires_both_lines_in_any_order():
    inst = FakeInstance()
    inst.trigger_channel(2)
    ops.execute(inst, "renew certificate --all -n tenant-edge --not-ready")
    assert inst.chaos_controller.get_channel_status(2)["state"] == ACTIVE
    ops.execute(inst, "renew certificate --all -n tenant-edge --not-ready")  # repeat is not enough
    assert inst.chaos_controller.get_channel_status(2)["state"] == ACTIVE
    ops.execute(inst, "restore iam-policy/cert-manager-route53 --version v3")
    assert inst.chaos_controller.get_channel_status(2)["state"] == STANDBY
    # resolve applied the remediation_effects
    assert 2 in inst.chaos_controller.get_fixed_channels()
    bodies = [e["body"] for e in _drain(inst)]
    assert any(b.startswith("cloudtrail: ") and "eventName=SetDefaultPolicyVersion" in b
               and "policy/cert-manager-route53" in b for b in bodies)


def test_retrigger_resets_progress():
    inst = FakeInstance()
    ctl = inst.chaos_controller
    inst.trigger_channel(2)
    ops.execute(inst, "restore iam-policy/cert-manager-route53 --version v3")
    ctl.resolve(2, force=True)  # resolved some other way
    ctl.trigger(2)
    ctl._channels[2]["triggered_at"] = time.time() + 1  # distinct trigger time
    ops.execute(inst, "renew certificate --all -n tenant-edge --not-ready")
    assert ctl.get_channel_status(2)["state"] == ACTIVE  # earlier restore no longer counts
    ops.execute(inst, "restore iam-policy/cert-manager-route53 --version v3")
    assert ctl.get_channel_status(2)["state"] == STANDBY


def test_actions_before_trigger_do_not_count():
    inst = FakeInstance()
    ops.execute(inst, "patch nodegroup/batch-spot --instance-types c6i.4xlarge,c6a.4xlarge,m6i.4xlarge")
    inst.trigger_channel(3)
    assert inst.chaos_controller.get_channel_status(3)["state"] == ACTIVE
    ops.execute(inst, "patch nodegroup/batch-spot --instance-types m6i.4xlarge,c6i.4xlarge,c6a.4xlarge")
    assert inst.chaos_controller.get_channel_status(3)["state"] == STANDBY


def test_plan_is_a_dry_run():
    inst = FakeInstance()
    inst.trigger_channel(1)
    r = ops.execute(inst, "plan rollback configmap/coredns -n kube-system --to-revision 42")
    assert r["status"] == "planned" and r["changes"]
    assert "change_id" not in r
    assert inst.chaos_controller.get_channel_status(1)["state"] == ACTIVE
    assert _drain(inst) == []
    r = ops.execute(inst, "rollback configmap/coredns -n kube-system --to-revision 42", dry_run=True)
    assert r["status"] == "planned"
    assert inst.chaos_controller.get_channel_status(1)["state"] == ACTIVE


def test_audit_falls_back_to_first_service():
    inst = FakeInstance(services=("ingress-gateway", "payments"))
    ops.execute(inst, "run job/harbor-gc -n harbor")
    assert _drain(inst, "kube-apiserver") == []
    events = _drain(inst, "ingress-gateway")
    assert events and "verb=create" in events[0]["body"] and "code=201" in events[0]["body"]


def test_channels_without_remediation_command_are_never_resolved():
    inst = FakeInstance()
    inst.trigger_channel(4)
    ops.execute(inst, "restart deployment/anything -n default")
    assert inst.chaos_controller.get_channel_status(4)["state"] == ACTIVE


def test_list_actions_has_no_hints():
    actions = ops.list_actions()
    verbs = {v["verb"] for v in actions["verbs"]}
    assert {"rollback", "restore", "patch", "restart", "plan"} <= verbs
    text = str(actions).lower()
    for word in ("demo", "chaos", "channel", "scenario", "coredns"):
        assert word not in text


# ── Endpoints ─────────────────────────────────────────────────────────────


@pytest.fixture
def client(monkeypatch):
    from fastapi.testclient import TestClient

    from app import main

    inst = FakeInstance()
    monkeypatch.setattr(main.registry, "get", {"dep-1": inst}.get)
    monkeypatch.setattr(main.registry, "all_instances", lambda: {"dep-1": inst})
    return TestClient(main.app), inst


def test_endpoint_apply_argv_and_command(client):
    c, inst = client
    inst.trigger_channel(1)
    r = c.post("/api/ops/actions", json={"argv": ["restart", "deployment/coredns", "-n", "kube-system"]})
    assert r.status_code == 200 and r.json()["status"] == "applied"
    assert inst.chaos_controller.get_channel_status(1)["state"] == ACTIVE
    r = c.post("/api/ops/actions", json={
        "command": "rollback configmap/coredns -n kube-system --to-revision 42", "deployment_id": "dep-1"})
    assert r.status_code == 200 and r.json()["change_id"]
    assert inst.chaos_controller.get_channel_status(1)["state"] == STANDBY


def test_endpoint_dry_run_errors_and_listing(client):
    c, inst = client
    r = c.post("/api/ops/actions", json={"argv": ["patch", "deployment/x", "-n", "a", "--memory-limit", "3Gi"],
                                         "dry_run": True})
    assert r.json()["status"] == "planned"
    r = c.post("/api/ops/actions", json={"argv": ["frobnicate", "x"]})
    assert r.status_code == 400 and "unknown command" in r.json()["error"]
    r = c.post("/api/ops/actions", json={})
    assert r.status_code == 400
    r = c.post("/api/ops/actions", json={"argv": ["run", "job/x"], "deployment_id": "nope"})
    assert r.status_code == 404
    assert c.get("/api/ops/actions").json()["usage"].startswith("opsctl")


def test_endpoint_reset(client):
    c, inst = client
    inst.trigger_channel(2)
    ops.execute(inst, "restore iam-policy/cert-manager-route53 --version v3")
    ops.execute(inst, "renew certificate --all -n tenant-edge --not-ready")
    assert inst.chaos_controller.get_fixed_channels() == [2]
    r = c.post("/api/ops/reset")
    assert r.status_code == 200 and r.json()["status"] == "reset"
    assert inst.chaos_controller.get_fixed_channels() == []
