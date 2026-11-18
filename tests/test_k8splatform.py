"""Kubernetes Platform Operations (schema 1.3) — realism, scope, fixes and coherence.

Runs the scenario's nine services and the pinned-topology k8s generator against a real
ChaosController for every fault channel, and checks what an SRE (or Nightshift) would
cross-check: the error signature comes from the component at fault, inside the fault's
scope, with its root-cause attributes and no demo bookkeeping; the cause is logged before
the effect; a correct fix changes the telemetry and persists; related signals agree.
"""
from __future__ import annotations

import ipaddress
import re
import time
from pathlib import Path

import pytest

from app.chaos.controller import ChaosController
from app.context import ScenarioContext
from app.services.base_service import BaseService
from generator_engine.infra_topology import build_topology, get_resource_attrs
from generator_engine.platform_topology import (
    apply_state,
    effective_platform,
    get_platform,
    parse_bytes,
)
from scenario_engine.yaml_scenario import load_yaml_scenario
from tests.test_generator_contract import _RecordingOTLP

ROOT = Path(__file__).resolve().parent.parent
SCN = load_yaml_scenario(ROOT / "scenarios" / "k8splatform")
CTX = ScenarioContext.from_scenario(SCN)
REG = SCN.channel_registry
DEMO_TELLS = re.compile(r"chaos|fault channel|channel \d+|NOVA|mission", re.I)
UNRESOLVED = re.compile(r"(?<!\{)\{[a-z_]+\}(?!\})")


def _attrs(kvs) -> dict:
    return {a["key"]: list(a["value"].values())[0] for a in kvs or []}


def _controller(ch: int | None = None, elapsed: float = 900.0) -> ChaosController:
    c = ChaosController(channel_registry=REG)
    if ch:
        c.trigger(ch, user_email="sre@example.com")
        c._channels[ch]["triggered_at"] = time.time() - elapsed
    return c


def _service_records(controller, cycles: int = 3) -> list[dict]:
    client = _RecordingOTLP()
    BaseService.set_context(CTX)
    try:
        services = [cls(controller, client) for cls in SCN.get_service_classes()]
    finally:
        BaseService.clear_context()
    for _ in range(cycles):
        for svc in services:
            svc.generate_telemetry()
    out = []
    for name, args, _kw in client.payloads:
        if name != "send_logs":
            continue
        res = _attrs(args[0]["attributes"])
        for rec in args[1]:
            out.append({
                "service": res.get("service.name"),
                "severity": rec["severityText"],
                "body": rec["body"]["stringValue"],
                "attrs": _attrs(rec.get("attributes")),
                "res": res,
                "ts": int(rec["timeUnixNano"]) / 1e9,
            })
    return out


def _k8s(controller, ticks: int = 2) -> dict[str, list[tuple[dict, float]]]:
    from generators.kubernetes_otel.pinned import _Tick

    client = _RecordingOTLP()
    tick = _Tick(client, {"namespace": SCN.namespace, "k8s_topology": SCN.k8s_topology,
                          "channel_registry": REG}, controller)
    for _ in range(ticks):
        tick()
    metrics: dict[str, list[tuple[dict, float]]] = {}
    for name, args, _kw in client.payloads:
        if name != "_send":
            continue
        for rm in args[1].get("resourceMetrics", []):
            res = _attrs(rm["resource"]["attributes"])
            for sm in rm["scopeMetrics"]:
                for m in sm["metrics"]:
                    dp = (m.get("gauge") or m.get("sum"))["dataPoints"][0]
                    metrics.setdefault(m["name"], []).append(
                        (res, float(dp.get("asDouble", dp.get("asInt"))))
                    )
    return metrics


def _values(metrics, name, **where) -> list[float]:
    return [v for res, v in metrics.get(name, [])
            if all(res.get(k) == want for k, want in where.items())]


# ── Topology ───────────────────────────────────────────────────────────────────


def test_pinned_topology_is_consistent():
    platform = get_platform(SCN.k8s_topology, SCN.namespace)
    c = platform.default
    assert c.name == "prod-use1"
    assert len(c.nodes) == 41  # system 3 + edge 6 + general 24 + batch-spot 8
    for node in c.nodes:
        assert node.name == f"ip-{node.ip.replace('.', '-')}.ec2.internal"
        assert ipaddress.ip_address(node.ip) in ipaddress.ip_network(c.subnets[node.az])
    for pod in c.pods:
        node = c.node(pod.node)
        assert ipaddress.ip_address(pod.ip) in ipaddress.ip_network(c.subnets[node.az])
    ips = [n.ip for n in c.nodes] + [p.ip for p in c.pods]
    assert len(ips) == len(set(ips)), "node and pod IPs must be unique"
    # Deployments spread across zones.
    tg_azs = {c.node(p.node).az for p in c.pods_of("tenant-gateway")}
    assert tg_azs == {"us-east-1a", "us-east-1b", "us-east-1c"}


def test_hosts_reuse_scenario_identity_ids():
    c = get_platform(SCN.k8s_topology, SCN.namespace).default
    for host in SCN.hosts:
        node = c.node(host["host.name"])
        assert node is not None, host["host.name"]
        assert node.instance_id == host["host.id"]


def test_services_sit_on_real_pods():
    data = {"services": SCN.services, "namespace": SCN.namespace, "hosts": SCN.hosts,
            "k8s_clusters": SCN.k8s_clusters, "k8s_topology": SCN.k8s_topology}
    topo = build_topology(data)
    pods = {p.name for p in get_platform(SCN.k8s_topology, SCN.namespace).default.pods}
    for svc in ("tenant-gateway", "ingress-nginx", "coredns", "harbor-registry"):
        assert get_resource_attrs(topo, svc)["k8s.pod.name"] in pods


def test_legacy_scenarios_keep_generated_topology():
    aws = load_yaml_scenario(ROOT / "scenarios" / "aws")
    assert aws.k8s_topology is None
    data = {"services": aws.services, "namespace": aws.namespace, "hosts": aws.hosts,
            "k8s_clusters": aws.k8s_clusters, "k8s_topology": aws.k8s_topology}
    topo = build_topology(data)
    assert not any("_workload" in entry for entry in topo.values())


# ── Fault logs: signature, scope, realism ──────────────────────────────────────


def test_baseline_has_no_errors_or_tells():
    for rec in _service_records(_controller()):
        assert rec["severity"] != "ERROR", rec["body"]
        assert not DEMO_TELLS.search(rec["body"]), rec["body"]
        assert not UNRESOLVED.search(rec["body"]), rec["body"]


@pytest.mark.parametrize("ch", sorted(REG))
def test_channel_signature_comes_from_component_at_fault(ch):
    reg = REG[ch]
    recs = _service_records(_controller(ch))
    sig = [r for r in recs if r["severity"] == "ERROR" and reg["error_type"] in r["body"]]
    assert sig, f"ch{ch} emitted no signature lines"
    emitters = set(reg.get("error_services") or reg["affected_services"])
    cascade_sources = set(reg.get("cascade_services") or [])
    assert {r["service"] for r in sig} <= emitters | cascade_sources
    clue_keys = set((SCN._channels[ch].get("rca_clues") or {}).get(next(iter(emitters)), {}))
    for r in recs:
        assert not any(k.startswith(("chaos.", "ops.", "system.")) for k in r["attrs"]), r["attrs"]
        assert not DEMO_TELLS.search(r["body"]), r["body"]
        assert not UNRESOLVED.search(r["body"]), r["body"]
    primary = [r for r in sig if r["service"] in emitters]
    assert primary and clue_keys <= set(primary[0]["attrs"]), "rca_clues must ride on the log line"
    for other, oreg in REG.items():
        if other != ch:
            assert not any(oreg["error_type"] in r["body"] for r in sig), f"ch{ch} matches ch{other}"


def test_fault_scope_limits_the_emitting_nodes():
    c = get_platform(SCN.k8s_topology, SCN.namespace).default
    edge = {n.name for n in c.nodes_in_pool("edge-c6i")}
    recs = _service_records(_controller(2))
    nodes = {r["res"]["k8s.node.name"] for r in recs if REG[2]["error_type"] in r["body"]}
    assert nodes and nodes <= edge
    rb_nodes = {p.node for p in c.pods_of("report-builder")}
    recs = _service_records(_controller(12))
    nodes = {r["res"]["k8s.node.name"] for r in recs if REG[12]["error_type"] in r["body"]}
    assert nodes and nodes <= rb_nodes


def test_cause_is_logged_before_the_effect():
    c = _controller(1, elapsed=120)
    triggered = c.get_channel_triggered_at(1)
    recs = _service_records(c, cycles=1)
    change = [r for r in recs if "configmaps" in r["body"] and "coredns" in r["body"]]
    first_error = min(r["ts"] for r in recs if REG[1]["error_type"] in r["body"])
    assert change and change[0]["ts"] < triggered < first_error + 1


def test_no_symptoms_before_onset():
    recs = _service_records(_controller(1, elapsed=5), cycles=1)
    assert not [r for r in recs if REG[1]["error_type"] in r["body"]]
    assert [r for r in recs if "configmaps" in r["body"]], "the change event fires at trigger"


# ── Coherence (generator schema 1.1) ───────────────────────────────────────────


def test_hpa_at_max_means_hot_replicas():
    m = _k8s(_controller(17), ticks=4)
    cur = _values(m, "k8s.hpa.current_replicas", **{"k8s.hpa.name": "tenant-gateway", "k8s.cluster.name": "prod-use1"})
    assert cur[-1] == 20
    desired = _values(m, "k8s.deployment.desired", **{"k8s.deployment.name": "tenant-gateway", "k8s.cluster.name": "prod-use1"})
    assert desired[-1] == cur[-1]
    cpu = _values(m, "k8s.container.cpu_limit_utilization", **{"k8s.deployment.name": "tenant-gateway", "k8s.cluster.name": "prod-use1"})
    assert min(cpu[-20:]) > 0.9
    healthy = _values(_k8s(_controller()), "k8s.container.cpu_limit_utilization",
                      **{"k8s.deployment.name": "tenant-gateway", "k8s.cluster.name": "prod-use1"})
    assert max(healthy) < 0.8


def test_not_ready_node_stops_reporting():
    m = _k8s(_controller(4))
    assert set(_values(m, "k8s.node.condition_ready", **{"k8s.node.name": "ip-10-42-36-17.ec2.internal"})) == {0.0}
    assert not _values(m, "k8s.node.cpu.utilization", **{"k8s.node.name": "ip-10-42-36-17.ec2.internal"})


def test_daemonset_not_ready_on_new_arm_pool():
    m = _k8s(_controller(19))
    where = {"k8s.daemonset.name": "fluent-bit", "k8s.cluster.name": "prod-use1"}
    desired = _values(m, "k8s.daemonset.desired_scheduled_nodes", **where)[-1]
    ready = _values(m, "k8s.daemonset.ready_nodes", **where)[-1]
    assert (desired, ready) == (47, 41)


# ── Fixes: verifiable, persistent, repeatable ──────────────────────────────────


def _fixed(ch: int) -> ChaosController:
    c = _controller(ch)
    c.resolve(ch, force=True)
    for row in c._platform_rows:
        row["applied_at"] -= 10_000  # transition complete
    return c


def test_memory_limit_fix_shows_in_metrics():
    where = {"k8s.deployment.name": "ingress-nginx-controller", "k8s.cluster.name": "prod-use1"}
    before = set(_values(_k8s(_controller(13)), "k8s.container.memory_limit", **where))
    after = set(_values(_k8s(_fixed(13)), "k8s.container.memory_limit", **where))
    assert before == {float(parse_bytes("1Gi"))} and after == {float(parse_bytes("3Gi"))}


def test_spot_diversification_adds_new_instance_types():
    m = _k8s(_fixed(8), ticks=1)
    types = [res.get("host.type") for res, _ in m["k8s.node.condition_ready"]
             if res.get("k8s.node.label.eks.amazonaws.com/nodegroup") == "batch-spot"]
    assert len(types) == 14 and {"c6a.4xlarge", "m6i.4xlarge", "m7i.4xlarge"} <= set(types)


def test_node_replacement():
    names = {res.get("k8s.node.name") for res, _ in _k8s(_fixed(4), ticks=1)["k8s.node.condition_ready"]}
    assert "ip-10-42-36-17.ec2.internal" not in names and "ip-10-42-37-88.ec2.internal" in names


def test_fix_transition_plays_out_over_time():
    base = get_platform(SCN.k8s_topology, SCN.namespace)
    state = {"fixes": [{"channel": 8, "state": REG[8]["remediation_effects"]["state"],
                        "applied_at": 1000.0, "transition_seconds": 180}], "faults": []}
    count = lambda now: len(effective_platform(base, state, now).default.nodes_in_pool("batch-spot"))
    assert count(1000.0) == 8 and 8 < count(1090.0) < 14 and count(1200.0) == 14


def test_fix_logs_change_and_steady_attributes():
    c = _fixed(1)
    for e in c._pending_events:
        e["emit_at"] = 0
    recs = _service_records(c, cycles=1)
    assert any("Reloading complete" in r["body"] for r in recs)
    coredns = [r for r in recs if r["service"] == "coredns"]
    assert coredns and all(r["attrs"].get("dns.corp_upstream") == "10.20.0.53,10.20.1.53" for r in coredns)


def test_retrigger_resets_and_expiry_does_not_fix():
    c = _fixed(13)
    assert c.get_fixed_channels() == [13]
    c.trigger(13)
    assert c.get_fixed_channels() == []
    c2 = _controller(13)
    c2._channels[13]["triggered_at"] = time.time() - 10**6  # past CHANNEL_TIMEOUT
    c2.get_active_channels()
    c2.is_active(13)
    assert c2.get_fixed_channels() == []


def test_fix_state_survives_restart(tmp_path):
    from app.store import ChaosStore

    store = ChaosStore(db_path=str(tmp_path / "x.db"))
    c = ChaosController(channel_registry=REG, chaos_store=store, deployment_id="k8splatform")
    c.trigger(13)
    c.resolve(13, force=True)
    again = ChaosController(channel_registry=REG, chaos_store=store, deployment_id="k8splatform")
    assert again.get_fixed_channels() == [13]


def test_apply_state_image_rollout_renames_pods():
    base = get_platform(SCN.k8s_topology, SCN.namespace)
    after = apply_state(base, {"workloads": {"session-cache": {"image": "x/redis:7.4.1"}}})
    old = {p.name for p in base.default.pods_of("session-cache")}
    new = {p.name for p in after.default.pods_of("session-cache")}
    assert old.isdisjoint(new) and len(new) == 3


# ── Individual changes (regression: a lone memory patch used to change nothing) ──


class _Inst:
    def __init__(self, controller):
        self.chaos_controller = controller
        self.ctx = CTX
        self.namespace = CTX.namespace
        self.scenario = SCN
        self.scenario_id = SCN.scenario_id

    def resolve_channel(self, ch, **kw):
        return self.chaos_controller.resolve(ch, **kw)


def _limits_by_pod(controller) -> dict[str, float]:
    m = _k8s(controller, ticks=1)
    return {res["k8s.pod.name"]: v for res, v in m["k8s.container.memory_limit"]
            if res.get("k8s.deployment.name") == "ingress-nginx-controller"
            and res.get("k8s.cluster.name") == "prod-use1"}


def test_single_patch_rolls_out_without_completing_the_fix():
    from app import ops

    c = _controller(13)
    before = _limits_by_pod(c)
    ops.execute(_Inst(c), "patch deployment/ingress-nginx-controller -n ingress-nginx --memory-limit 3Gi")
    assert c.is_active(13), "half the runbook must not resolve the fault"
    rows = [r for r in c._platform_rows if r["channel"] < 0]
    for r in rows:
        r["applied_at"] -= 45  # mid-rollout
    mid = _limits_by_pod(c)
    old = {p: v for p, v in mid.items() if p in before}
    new = {p: v for p, v in mid.items() if p not in before}
    assert old and new and set(old.values()) == {float(parse_bytes("1Gi"))} \
        and set(new.values()) == {float(parse_bytes("3Gi"))}
    for r in rows:
        r["applied_at"] -= 600
    after = _limits_by_pod(c)
    assert len(after) == 6 and not (set(after) & set(before))
    assert set(after.values()) == {float(parse_bytes("3Gi"))}
    other = [v for res, v in _k8s(c, ticks=1)["k8s.container.memory_limit"]
             if res.get("k8s.deployment.name") == "ingress-nginx-controller"
             and res.get("k8s.cluster.name") != "prod-use1"]
    assert set(other) == {float(parse_bytes("1Gi"))}, "unfaulted clusters keep their limit"


def test_retrigger_drops_manual_changes_to_the_same_workload():
    from app import ops

    c = _controller(13)
    ops.execute(_Inst(c), "patch deployment/ingress-nginx-controller -n ingress-nginx --memory-limit 3Gi")
    ops.execute(_Inst(c), "delete ingresses -n tenant-previews --selector preview-state=stale")
    assert not c.is_active(13)
    c.trigger(13)
    for r in c._platform_rows:
        r["applied_at"] -= 600
    assert set(_limits_by_pod(c).values()) == {float(parse_bytes("1Gi"))}


def test_k8s_verification_signals_are_scoped_to_the_faulted_cluster():
    for ch, reg in REG.items():
        for v in (reg.get("remediation_effects") or {}).get("verification") or []:
            where = v.get("where", "")
            if "resource.attributes.k8s." in where:
                assert 'k8s.cluster.name == "prod-use1"' in where, (ch, where)


def test_log_clue_attributes_are_mapped_on_the_wired_stream():
    """The runbook Evidence query reads attributes.<clue>; the wired stream maps only declared fields."""
    from elastic_config.deployer_streams import _clue_wired_fields
    from scenario_engine import get_scenario, list_scenarios

    fields = _clue_wired_fields(get_scenario("k8splatform")._channels)
    assert fields["attributes.nginx.ingress_count"] == {"type": "double"}
    assert fields["attributes.k8s.container.memory_limit"] == {"type": "keyword"}
    for entry in list_scenarios():
        sid = entry["id"] if isinstance(entry, dict) else entry
        channels = getattr(get_scenario(sid), "_channels", {}) or {}
        fields = _clue_wired_fields(channels)
        for ch in channels.values():
            for clues in (ch.get("rca_clues") or {}).values():
                for key in clues or {}:
                    assert f"attributes.{key}" in fields, (sid, key)


def _ingress_run(controller, tick_gap: float, cycles: int = 20):
    client = _RecordingOTLP()
    BaseService.set_context(CTX)
    try:
        svc = next(k(controller, client) for k in SCN.get_service_classes()
                   if k.SERVICE_NAME == "ingress-nginx")
    finally:
        BaseService.clear_context()
    for _ in range(cycles):
        svc._state["_reload_tick"] = time.time() - tick_gap
        svc.generate_telemetry()
    bodies = [rec["body"]["stringValue"] for name, args, _ in client.payloads
              if name == "send_logs" for rec in args[1]]
    return bodies, client


def test_ch13_ingress_count_ramps_and_reload_rate_matches_clue():
    """The Ingress count climbs over the first 90s; reload lines match nginx.reloads_per_min (14-22/pod)."""
    def ingresses(client):
        text = str([a for n, a, _ in client.payloads if n == "send_metrics"])
        i = text.index("nginx_ingress_controller.ingresses")
        return float(re.search(r"'asDouble': ([\d.]+)", text[i:]).group(1))

    _, base = _ingress_run(_controller(), 6.0, cycles=1)
    _, early = _ingress_run(_controller(13, elapsed=45), 6.0, cycles=1)
    _, late = _ingress_run(_controller(13, elapsed=300), 6.0, cycles=1)
    assert ingresses(base) == 40 and ingresses(late) == 1840
    assert 40 < ingresses(early) < 1840

    bodies, _ = _ingress_run(_controller(13, elapsed=300), 6.0, cycles=20)  # 120s of ticks
    per_pod_per_min = sum("successfully reloaded" in b for b in bodies) / 6 / 2
    assert 12 <= per_pod_per_min <= 24, per_pod_per_min


def test_metrics_requests_carry_one_data_stream_each():
    """ES OTLP merges resources that differ only in data_stream.*; never mix them in one request."""
    from app.telemetry import _format_attributes, OTLPClient

    def rm(ds, node):
        return {"resource": {"attributes": _format_attributes(
            {"k8s.node.name": node, "data_stream.dataset": ds})}, "scopeMetrics": []}

    payload = {"resourceMetrics": [rm("kubeletstatsreceiver", "a"), rm("k8sclusterreceiver", "a"),
                                   rm("kubeletstatsreceiver", "b"), rm("k8sclusterreceiver", "b")]}
    parts = OTLPClient._split_by_data_stream(payload)
    assert len(parts) == 2
    for part in parts:
        dss = {a["value"]["stringValue"] for r in part["resourceMetrics"]
               for a in r["resource"]["attributes"] if a["key"] == "data_stream.dataset"}
        assert len(dss) == 1 and len(part["resourceMetrics"]) == 2
    single = {"resourceMetrics": [rm("kubeletstatsreceiver", "a")]}
    assert OTLPClient._split_by_data_stream(single) == [single]


def test_infrastructure_events_come_from_the_faulted_node():
    """ch04's containerd DeadlineExceeded line must come from the NotReady node."""
    recs = _service_records(_controller(4, elapsed=120), cycles=2)
    lines = [r for r in recs if "DeadlineExceeded" in r["body"]]
    assert lines, "ch04 emitted no containerd line"
    assert {r["res"].get("k8s.node.name") for r in lines} == {"ip-10-42-36-17.ec2.internal"}


def test_evidence_section_points_at_service_metrics_index():
    """Investigations looked for coredns metrics in the k8s indices and concluded they were missing."""
    from elastic_config.deployer_kb import KbMixin

    host = KbMixin()
    host.ns = "platform"
    host.scenario = SCN
    host._stream_name = "logs.otel.platform"
    text = host._evidence_section(1, REG[1])
    assert "`metrics-generic.otel-default`" in text
    assert "`metrics.coredns.dns.responses_per_s`" in text
    assert "coredns.dns.responses_per_s" in SCN.service_metric_names("coredns")


def test_ch02_audit_line_shows_the_changed_key():
    bodies = [e["body"] for e in REG[2]["infrastructure_events"] if e["service"] == "kube-apiserver"]
    assert any('upstream-keepalive-connections="0"' in b for b in bodies)


def test_ch03_fix_issues_every_affected_certificate():
    certs = {c[1] for c in SCN._service_specs["cert-manager"]["constants"]["CERTS"]}
    tenants = set(SCN._channels[3]["fault_params"]["tenant"]["choice"])
    events = REG[3]["remediation_effects"]["log_events"]
    issued = {re.search(r'name="([^"]+)"', e["body"]).group(1)
              for e in events if "Certificate issued" in e["body"]}
    loaded = {re.search(r'name="tenant-edge/([^"]+)"', e["body"]).group(1)
              for e in events if "Adding secret" in e["body"]}
    want = {f"{t}-custom-tls" for t in tenants}
    assert issued == loaded == want
    assert want <= certs  # every cert the fault names exists in the steady-state set
    assert "wildcard-api-tls" not in issued


def test_hpa_state_is_kept_per_cluster():
    """Same-named HPAs in different clusters must not share state: prod-use2's base of 4
    scaled prod-use1's tenant-gateway down from its pinned 8."""
    m = _k8s(_controller(), ticks=8)
    for cluster, want in (("prod-use1", 8), ("prod-use2", 4), ("prod-euw1", 4)):
        desired = _values(m, "k8s.deployment.desired",
                          **{"k8s.deployment.name": "tenant-gateway", "k8s.cluster.name": cluster})
        hpa = _values(m, "k8s.hpa.current_replicas",
                      **{"k8s.hpa.name": "tenant-gateway", "k8s.cluster.name": cluster})
        assert desired and set(desired) == {want}, (cluster, desired)
        assert set(hpa) == {want}, (cluster, hpa)


# ── Fixes from the 20-channel Nightshift + Claude Code run ─────────────────────


_VERIFY_INDICES = {
    "logs.otel.platform", "metrics-k8sclusterreceiver.otel-*", "metrics-kubeletstatsreceiver.otel-*",
    "metrics-generic.otel-default", "logs-k8seventsreceiver.otel-default",
}


def test_recovery_checks_render_runnable_queries():
    """Nightshift could not find k8s signals: every check names its index and valid fields."""
    from elastic_config.deployer_kb import recovery_query

    for ch, reg in REG.items():
        for v in reg["remediation_effects"]["verification"]:
            assert v.get("from") in _VERIFY_INDICES, (ch, v["signal"])
            q = recovery_query(v)
            assert q.startswith(f"FROM {v['from']} | WHERE @timestamp > NOW()"), (ch, q)
            bare = re.sub(r'"[^"]*"|`[^`]*`', " ", q)
            assert not re.search(r"[\w.]+/[\w./-]+", bare), (ch, q)            # '/' fields quoted
            assert not re.search(r"(?<![\w.])k8s\.[a-z_.]+\s*[<>=!]=?\s*\d", bare), (ch, q)  # metrics. prefix


def _ops_inst():
    from types import SimpleNamespace

    class _Inst:
        def __init__(self):
            self.chaos_controller = ChaosController(channel_registry=REG)
            self.ctx = SimpleNamespace(services={s: {} for s in ("kube-apiserver", "harbor-registry")},
                                       scenario=SCN)

        def resolve_channel(self, channel, **kw):
            return self.chaos_controller.resolve(channel, **kw)

    return _Inst()


def _drain_all(ctl):
    with ctl._lock:
        for e in ctl._pending_events:
            e["emit_at"] = 0
        evs, ctl._pending_events = list(ctl._pending_events), []
    return evs


def test_change_id_marks_only_the_fixers_records():
    # ch10: the PDB patch is the change; the node manager's eviction that follows is not
    from app.chaos.controller import AUTO_REMEDIATOR_ACTOR
    inst = _ops_inst()
    ctl = inst.chaos_controller
    ctl.trigger(10)
    ctl.resolve(10, force=True, actor=AUTO_REMEDIATOR_ACTOR)
    audits = [e["body"] for e in _drain_all(ctl) if e["body"].startswith("audit:")]
    fix = [a for a in audits if "poddisruptionbudgets" in a]
    follow = [a for a in audits if "eks:node-manager" in a]
    assert fix and follow, audits
    assert all("change_id=chg-" in a for a in fix)
    assert all("change_id=" not in a for a in follow)
    # ch17: the fix is the rate-limit service's own line, not an audit record
    ctl.trigger(17)
    ctl.resolve(17, force=True, actor=AUTO_REMEDIATOR_ACTOR)
    rls = [e["body"] for e in _drain_all(ctl) if e["body"].startswith("rls:")]
    assert rls and all("change_id=chg-" in b for b in rls), rls


def test_fix_audit_names_the_actor_once_per_action():
    from app import ops
    from app.chaos.controller import AUTO_REMEDIATOR_ACTOR

    # opsctl (Claude Code): one audit record, the channel's detailed one, naming claude-code
    inst = _ops_inst()
    inst.chaos_controller.trigger(5)
    r = ops.execute(inst, "opsctl rollback deployment/job-operator -n tenant-batch")
    assert inst.chaos_controller.get_channel_status(5)["state"] == "STANDBY"
    audits = [e for e in _drain_all(inst.chaos_controller) if e["body"].startswith("audit:")]
    assert len(audits) == 1, [a["body"] for a in audits]
    assert "user=opsctl/claude-code" in audits[0]["body"] and "rollback to revision 11" in audits[0]["body"]
    assert f"change_id={r['change_id']}" in audits[0]["body"]
    assert all("{actor}" not in a["body"] for a in audits)

    # the auto-remediate workflow: its change lines name the automation
    inst = _ops_inst()
    inst.chaos_controller.trigger(17)
    inst.resolve_channel(17, force=True, actor=AUTO_REMEDIATOR_ACTOR)
    bodies = [e["body"] for e in _drain_all(inst.chaos_controller)]
    assert any(AUTO_REMEDIATOR_ACTOR in b for b in bodies)
    assert not any("claude-code" in b for b in bodies)


def test_upgrade_fix_continues_from_the_fault_progress():
    """ch10: nodes already on v1.31 during the fault stay upgraded while the fix rolls on."""
    c = _controller(10, elapsed=300)
    c.resolve(10, force=True)
    row = c._platform_rows[-1]
    counts = []
    for age in (0, 120, 450, 950):
        row["applied_at"] = time.time() - age
        m = _k8s(c, ticks=1)
        counts.append(sum(
            1 for r, _ in m["k8s.node.condition_ready"]
            if r.get("k8s.cluster.name") == "prod-use1"
            and r.get("k8s.node.label.eks.amazonaws.com/nodegroup") == "general-c6i"
            and str(r.get("k8s.kubelet.version", "")).startswith("v1.31")
        ))
    assert counts[0] >= 6 and counts == sorted(counts) and counts[-1] == 24, counts


def _replicasets(m, wl):
    return {r["k8s.pod.name"].rsplit("-", 1)[0] for r, _ in m["k8s.pod.phase"]
            if r.get("k8s.deployment.name") == wl and r.get("k8s.cluster.name") == "prod-use1"}


def test_replicaset_follows_the_pod_template():
    # ch07: rotating a registry credential doesn't change the template; the stuck pods'
    # ReplicaSet is the one that finishes rolling out
    c = _controller(7, elapsed=300)
    during = _replicasets(_k8s(c, ticks=2), "session-cache")
    c.resolve(7, force=True)
    c._platform_rows[-1]["applied_at"] -= 10_000
    after = _replicasets(_k8s(c, ticks=2), "session-cache")
    assert len(after) == 1 and after <= during, (during, after)
    # ch16: the affinity patch changes the template, so a new ReplicaSet replaces the old
    c = _controller(16, elapsed=300)
    before = _replicasets(_k8s(c, ticks=1), "coredns")
    c.resolve(16, force=True)
    c._platform_rows[-1]["applied_at"] -= 10_000
    assert _replicasets(_k8s(c, ticks=1), "coredns").isdisjoint(before)


def test_rollout_never_creates_pods_in_the_old_replicaset():
    # ch16: the fix moves the m7i replica and rolls a new template over 600s. The old
    # ReplicaSet only loses pods; a pod's name, once gone, never comes back.
    from generator_engine.platform_topology import effective_platform, get_platform

    base = get_platform(SCN.k8s_topology, SCN.namespace)
    c = _controller(16, elapsed=300)
    c.resolve(16, force=True)
    row = c._platform_rows[-1]
    t0 = row["applied_at"]
    transition = row["transition_seconds"]

    def names(now):
        st = c.get_platform_state()
        m = effective_platform(base, st, now)
        return {p.name for p in m.cluster(None).pods_of("coredns")}

    at_fix = names(t0)
    old_hash = base.cluster(None).workloads["coredns"].replicaset_hash
    seen, gone = set(), set()
    for step in range(0, 11):
        now_names = names(t0 + transition * step / 10)
        assert not (now_names & gone), (step, now_names & gone)
        new_old = {n for n in now_names - at_fix if f"-{old_hash}-" in n}
        assert not new_old, (step, new_old)
        gone |= seen - now_names
        seen |= now_names
    assert all(f"-{old_hash}-" not in n for n in names(t0 + transition + 1))


def test_disk_fills_before_the_evictions_start():
    """ch12: DiskPressure is set when the kubelet's eviction lines begin (onset)."""
    from generators.kubernetes_otel.pinned import _Tick

    c = _controller(12, elapsed=0)
    client = _RecordingOTLP()
    tick = _Tick(client, {"namespace": SCN.namespace, "k8s_topology": SCN.k8s_topology,
                          "channel_registry": REG}, c)
    onset = float(REG[12]["onset_delay_seconds"])
    pressure_at = None
    for el in range(0, int(onset) + 61, 30):
        c._channels[12]["triggered_at"] = time.time() - el
        client.payloads.clear()
        tick()
        for name, args, _kw in client.payloads:
            if name != "_send":
                continue
            for rm in args[1].get("resourceMetrics", []):
                for sm in rm["scopeMetrics"]:
                    for mt in sm["metrics"]:
                        if mt["name"] == "k8s.node.condition_disk_pressure":
                            dp = mt["gauge"]["dataPoints"][0]
                            if int(dp.get("asInt", 0)) == 1 and pressure_at is None:
                                pressure_at = el
    assert pressure_at is not None and pressure_at <= onset, pressure_at


def test_volume_is_full_when_the_push_errors_start():
    """ch18: the registry volume is (nearly) full by onset, when pushes start failing."""
    from generators.kubernetes_otel.pinned import _Tick

    c = _controller(18, elapsed=0)
    client = _RecordingOTLP()
    tick = _Tick(client, {"namespace": SCN.namespace, "k8s_topology": SCN.k8s_topology,
                          "channel_registry": REG}, c)
    onset = float(REG[18]["onset_delay_seconds"])
    free_at_onset = None
    for el in [0, 10, 20, 25, onset, onset + 30]:
        c._channels[18]["triggered_at"] = time.time() - el
        client.payloads.clear()
        tick()
        vals = {}
        for name, args, _kw in client.payloads:
            if name != "_send":
                continue
            for rm in args[1].get("resourceMetrics", []):
                attrs = {a["key"]: a["value"].get("stringValue") for a in rm["resource"]["attributes"]}
                if attrs.get("k8s.persistentvolumeclaim.name") != "harbor-registry":
                    continue
                for sm in rm["scopeMetrics"]:
                    for mt in sm["metrics"]:
                        dp = mt["gauge"]["dataPoints"][0]
                        vals[mt["name"]] = int(dp.get("asInt", 0))
        if el == onset:
            free_at_onset = vals["k8s.volume.available"] / vals["k8s.volume.capacity"]
    assert free_at_onset is not None and free_at_onset < 0.03, free_at_onset


def test_fault_lines_taper_after_a_fix_but_not_after_expiry():
    c = _controller(9, elapsed=300)
    c.resolve(9, force=True)
    frac = c.taper_fraction(9)
    assert 0.9 < frac <= 1.0
    c._tapers[9] = (time.time() - REG[9]["remediation_effects"]["log_taper_seconds"] - 1,
                    REG[9]["remediation_effects"]["log_taper_seconds"])
    assert c.taper_fraction(9) == 0.0
    c = _controller(9, elapsed=300)
    c.resolve(9, apply_effects=False)  # auto-expiry
    assert c.taper_fraction(9) == 0.0


def test_fluent_bit_failure_is_seen_centrally_not_from_the_broken_nodes():
    """ch19: fluent-bit ships the nodes' logs, so its failure can't arrive in their kubelet logs."""
    recs = _service_records(_controller(19, elapsed=300), cycles=4)
    fault = [r for r in recs if "exec format error" in r["body"]]
    assert fault and {r["service"] for r in fault} == {"kube-apiserver"}
    assert all(UNRESOLVED.search(r["body"]) is None for r in fault)
    assert not [r for r in recs if r["service"] == "kubelet" and r["res"].get("host.arch") == "arm64"]
    assert {e["reason"] for e in REG[19]["k8s_events"]} >= {"Failed", "BackOff"}


def test_apf_saturation_rejects_the_other_workload_low_clients_too():
    # victims are sampled at random (p=0.55 per cycle, 1 of 4 clients), so use enough
    # cycles that seeing fewer than two distinct clients is vanishingly unlikely
    recs = _service_records(_controller(5, elapsed=300), cycles=60)
    # grouped on the mapped clue field, the way an investigation queries it
    others = {r["attrs"].get("k8s.audit.user") for r in recs if "code=429" in r["body"]} - {None}
    others.discard("system:serviceaccount:tenant-batch:job-operator")
    assert len(others) >= 2, others


def test_ci_pushes_succeed_in_steady_state_and_fail_while_the_volume_is_full():
    def pushes(recs):
        return [r for r in recs if r["service"] == "harbor-registry" and "method=PUT" in r["body"]]

    ok = [r for r in pushes(_service_records(_controller(), cycles=150)) if "status=201" in r["body"]]
    assert ok
    full = pushes(_service_records(_controller(18, elapsed=300), cycles=150))
    assert full and all("status=500" in r["body"] for r in full)


def test_green_deployment_is_reported_at_zero_replicas():
    m = _k8s(_controller(), ticks=1)
    assert _values(m, "k8s.deployment.desired",
                   **{"k8s.deployment.name": "tenant-gateway-green", "k8s.cluster.name": "prod-use1"}) == [0.0]


def test_klog_lines_declare_the_severity_their_prefix_says():
    """A klog 'E1114 ...' line logged at WARN hides it from ERROR-level searches (ch04)."""
    levels = {"E": "ERROR", "W": "WARN", "I": "INFO"}
    for ch, reg in REG.items():
        events = list(reg.get("infrastructure_events") or []) + \
            list((reg.get("remediation_effects") or {}).get("log_events") or [])
        for e in events:
            m = re.match(r"([EWI])(\d{4}|\{klog_time\})", str(e.get("body", "")))
            if m:
                assert e.get("severity", "INFO") == levels[m.group(1)], (ch, e["body"][:80])



def test_auto_remediation_queue_credits_the_automation():
    """ch16-20 are fixed by the workflow through the remediation queue, not by a person."""
    import threading
    from app.chaos.controller import AUTO_REMEDIATOR_ACTOR
    from app.chaos.remediation_poller import RemediationPoller

    def fixed_by(approval_mode):
        c = ChaosController(channel_registry=REG)
        c.trigger(16)
        c._channels[16]["triggered_at"] = time.time() - 300
        p = RemediationPoller(elastic_url="http://es", elastic_api_key="k", namespace="platform",
                              chaos_controller=c, stop_event=threading.Event())
        p._mark_processed = lambda *a, **k: None
        src = {"channel": 16, "dry_run": "false"}
        if approval_mode:
            src["approval_mode"] = approval_mode
        p._process_hit(None, {"_id": "d1", "_source": src}, set())
        return " ".join(e["body"] for e in _drain_all(c))

    assert AUTO_REMEDIATOR_ACTOR in fixed_by("skip")
    approved = fixed_by(None)
    assert AUTO_REMEDIATOR_ACTOR not in approved and "opsctl/oncall" in approved


def test_volume_space_is_freed_when_gc_finishes():
    """ch18: the expansion doubles capacity at once (~50% free); GC frees space later (~70%)."""
    from generators.kubernetes_otel.pinned import _Tick

    c = _controller(18, elapsed=300)
    client = _RecordingOTLP()
    tick = _Tick(client, {"namespace": SCN.namespace, "k8s_topology": SCN.k8s_topology,
                          "channel_registry": REG}, c)

    def free_frac():
        cap = avail = None
        for name, args, _kw in client.payloads:
            if name != "_send":
                continue
            for rm in args[1].get("resourceMetrics", []):
                if _attrs(rm["resource"]["attributes"]).get("k8s.persistentvolumeclaim.name") != "harbor-registry":
                    continue
                for sm in rm["scopeMetrics"]:
                    for m in sm["metrics"]:
                        dp = m["gauge"]["dataPoints"][0] if "gauge" in m else None
                        if dp and m["name"] == "k8s.volume.capacity":
                            cap = float(dp.get("asInt", dp.get("asDouble")))
                        if dp and m["name"] == "k8s.volume.available":
                            avail = float(dp.get("asInt", dp.get("asDouble")))
        client.payloads.clear()
        return avail / cap

    for _ in range(40):  # the fault fills the 1Ti volume to the brim
        tick()
    client.payloads.clear()
    c.resolve(18, force=True)
    row = c._platform_rows[-1]
    for age in (30, 120, 300):  # GC still running
        row["applied_at"] = time.time() - age
        tick()
        frac = free_frac()
        assert 0.4 < frac < 0.6, (age, frac)
    for age in range(560, 800, 30):  # GC done
        row["applied_at"] = time.time() - age
        tick()
    assert free_frac() > 0.65
