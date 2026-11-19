"""gcp hybrid connectivity channels (12, 13, 18, 19): before / during / after a fault.

Each signal sits in its baseline band before the fault, is out of band during it (matching
the channel's fault_params, status log says ANOMALY), and converges back after the opsctl
fix, with the changed configuration on the status log and one Cloud Audit line per command.
"""
from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from app import ops
from app.chaos.controller import ChaosController
from app.services.base_service import BaseService
from tests.test_generator_contract import _RecordingOTLP
from tests._gcp_harness import (
    CTX,
    OPS_ACTOR, PROFILE, REG, ROOT, SCN, attr_values, bodies, controller, phase, records, values,
)

IC, VPN = "cloud-interconnect", "cloud-vpn-gateway"


def _raw(ch: int) -> dict:
    import glob
    import yaml
    return yaml.safe_load(open(glob.glob(str(ROOT / "scenarios" / "gcp" / "channels" / f"{ch:02d}-*.yaml"))[0]))


def _fp(ch: int) -> dict:
    import glob
    import yaml
    path = glob.glob(str(ROOT / "scenarios" / "gcp" / "channels" / f"{ch:02d}-*.yaml"))[0]
    return yaml.safe_load(open(path))["fault_params"]


def _within(ch: int) -> float:
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _sig(ch, which, svc, metric, **kw):
    logs, metrics = records(phase(ch, which, **kw))
    vals = values(metrics, svc, metric)
    assert vals, f"{svc} emitted no {metric}"
    return logs, vals


def _status(logs, svc, contains, key):
    return {r["attrs"].get(key) for r in logs if r["service"] == svc and contains in r["body"]}


# -- ch12: optic degradation on link 1 -> circuit down -------------------------

def test_ch12_rx_light_and_link_phases():
    m = "interconnect.rx_light_dbm"
    sel = "optic="
    logs, v = _sig(12, "before", IC, m)
    assert all(-3.3 <= x <= -1.7 for x in v), v
    assert set(values(records(phase(12, "before"))[1], IC, "interconnect.link_up")) == {1.0}
    assert _status(logs, IC, sel, "interconnect.status") == {"NOMINAL"}

    logs, v = _sig(12, "during", IC, m)
    assert all(-22.0 <= x <= -16.0 for x in v), v
    assert set(values(records(phase(12, "during"))[1], IC, "interconnect.link_up")) == {0.0}
    assert _status(logs, IC, sel, "interconnect.status") == {"ANOMALY"}
    assert _status(logs, IC, sel, "interconnect.lacp_state") == {"DETACHED"}
    assert attr_values(logs, IC, "interconnect.optic_batch") == {"lr4-100g-batch-2026q2"}
    assert _fp(12)["interconnect_name"] == "gcpnet-interconnect-primary"

    logs, v = _sig(12, "after", IC, m, since=_within(12))
    assert all(-3.3 <= x <= -1.7 for x in v), v
    assert set(values(records(phase(12, "after", since=_within(12)))[1], IC, "interconnect.link_up")) == {1.0}
    assert _status(logs, IC, sel, "interconnect.status") == {"NOMINAL"}
    assert attr_values(logs, IC, "interconnect.optic_batch") == {"lr4-100g-batch-2026q4"}
    _assert_audit(logs, 12)


def _metric_by_attachment(ctl, name, attachment):
    _, metrics = records(ctl, cycles=12)
    return [m["value"] for m in metrics
            if m["service"] == IC and m["name"] == name and m["attrs"].get("interconnect.attachment") == attachment]


def test_ch12_ramps_in():
    _, early = _sig(12, "during", IC, "interconnect.rx_light_dbm", elapsed=5)
    assert min(early) > -10.0, early


def test_ch12_recovery_stages():
    """Light is back at the swap; link ~75 s, LACP ~90 s, BGP + routes ~165 s, then failback."""
    rx, up = "interconnect.rx_light_dbm", "interconnect.link_up"
    routes, util = "interconnect.bgp_advertised_routes", "interconnect.utilization_pct"
    prim, sec = "gcpnet-interconnect-primary", "gcpnet-interconnect-secondary"

    # t+10 s: new optic, light normal, but the link is still retraining and nothing is routed
    logs, metrics = records(phase(12, "after", since=10))
    assert all(-3.3 <= x <= -1.7 for x in values(metrics, IC, rx))
    assert set(values(metrics, IC, up)) == {0.0}
    assert set(values(metrics, IC, routes)) == {0.0}
    assert _status(logs, IC, "optic=", "interconnect.lacp_state") == {"DETACHED"}
    assert _status(logs, IC, "optic=", "interconnect.status") == {"ANOMALY"}
    assert _status(logs, IC, "flaps_10m=", "interconnect.bgp_state") == {"IDLE"}

    # t+80 s: link is up, LACP member not attached yet, BGP still down
    logs, metrics = records(phase(12, "after", since=80))
    assert set(values(metrics, IC, up)) == {1.0}
    assert set(values(metrics, IC, routes)) == {0.0}
    assert _status(logs, IC, "optic=", "interconnect.lacp_state") == {"DETACHED"}

    # t+120 s: LACP attached, BGP session not established, primary still carries nothing
    ctl = phase(12, "after", since=120)
    logs, metrics = records(ctl)
    assert _status(logs, IC, "optic=", "interconnect.lacp_state") == {"ATTACHED"}
    assert set(values(metrics, IC, routes)) == {0.0}
    assert _status(logs, IC, "flaps_10m=", "interconnect.bgp_state") == {"ACTIVE"}
    assert max(_metric_by_attachment(ctl, util, prim)) == 0.0
    assert min(_metric_by_attachment(ctl, util, sec)) > 55.0

    # t+300 s: session established, routes back, traffic failed back to the primary
    ctl = phase(12, "after", since=300)
    logs, metrics = records(ctl)
    assert set(values(metrics, IC, routes)) == {48.0}
    assert _status(logs, IC, "flaps_10m=", "interconnect.bgp_state") == {"ESTABLISHED"}
    assert min(_metric_by_attachment(ctl, util, prim)) > 30.0
    assert max(_metric_by_attachment(ctl, util, sec)) < 40.0


def test_ch12_verification_has_an_entry_per_stage():
    ver = REG[12]["remediation_effects"]["verification"]
    fields = {v["field"] for v in ver}
    assert {"metrics.interconnect.rx_light_dbm", "metrics.interconnect.link_up",
            "attributes.interconnect.lacp_state", "metrics.interconnect.bgp_advertised_routes"} <= fields
    assert _within(12) == 300
    by = {v["field"]: v["within_seconds"] for v in ver}
    assert by["metrics.interconnect.rx_light_dbm"] < by["metrics.interconnect.link_up"] \
        < by["metrics.interconnect.bgp_advertised_routes"]


def test_ch12_bgp_is_down_with_the_link_not_flapping():
    logs, metrics = records(phase(12, "during"))
    assert set(values(metrics, IC, "interconnect.bgp_advertised_routes")) == {0.0}
    assert set(values(metrics, IC, "bgp.session_flaps")) == {0.0}
    assert _status(logs, IC, "flaps_10m=", "interconnect.bgp_state") == {"IDLE"}


# -- ch13: peer BFD multiplier 2 at 1000 ms -> BGP flaps ---------------------------------------

def test_ch13_bgp_flap_phases():
    f, u = "bgp.session_flaps", "bgp.session_uptime_s"
    logs, v = _sig(13, "before", IC, f)
    assert set(v) == {0.0}
    _, up = _sig(13, "before", IC, u)
    assert min(up) > 100000
    assert _status(logs, IC, "flaps_10m=", "bgp.status") == {"NOMINAL"}
    assert attr_values(logs, IC, "bgp.peer_bfd_multiplier") == {"5"}
    assert attr_values(logs, IC, "bgp.bfd_min_rx_interval_ms") == {"1000"}

    logs, v = _sig(13, "during", IC, f)
    assert all(5 <= x <= 30 for x in v), v
    _, up = _sig(13, "during", IC, u)
    assert all(x < 90 for x in up), up
    assert _status(logs, IC, "flaps_10m=", "bgp.status") == {"ANOMALY"}
    assert attr_values(logs, IC, "bgp.peer_bfd_multiplier") == {"2"}
    assert attr_values(logs, IC, "bgp.bfd_min_rx_interval_ms") == {"1000"}
    assert attr_values(logs, IC, "interconnect.bgp_peer") == {_fp(13)["bgp_peer_ip"]}
    assert attr_values(logs, IC, "interconnect.bgp_state") == {_fp(13)["bgp_state"]}

    logs, v = _sig(13, "after", IC, f, since=_within(13))
    assert set(v) == {0.0}
    assert _status(logs, IC, "flaps_10m=", "bgp.status") == {"NOMINAL"}
    # the peer's own multiplier is unchanged; Cloud Router's negotiated interval is what moved
    assert attr_values(logs, IC, "bgp.peer_bfd_multiplier") == {"2"}
    assert attr_values(logs, IC, "bgp.bfd_min_rx_interval_ms") == {"5000"}
    _assert_audit(logs, 13)


def test_ch13_uptime_grows_after_fix_and_flaps_decay():
    _, up = _sig(13, "after", IC, "bgp.session_uptime_s", since=60)
    assert all(x >= 60 for x in up), up
    _, fl = _sig(13, "after", IC, "bgp.session_flaps", since=10)
    assert min(fl) > 5, fl


# -- ch18: stale PSK on tunnel-02 ---------------------------------------------

def test_ch18_tunnel_phases():
    est, fail = "vpn.tunnel_02_established", "vpn.tunnel_02_ike_auth_failures"
    sel = "tunnel=gcpnet-vpn-tunnel-02 gateway=gcpnet-vpn-gw-europe1 region"
    logs, v = _sig(18, "before", VPN, est)
    assert set(v) == {1.0}
    assert set(_sig(18, "before", VPN, fail)[1]) == {0.0}
    assert _status(logs, VPN, sel, "vpn.health") == {"NOMINAL"}
    assert _status(logs, VPN, sel, "vpn.psk_version") == {"psk-2026q3-b1"}

    logs, v = _sig(18, "during", VPN, est)
    assert set(v) == {0.0}
    assert all(6 <= x <= 14 for x in _sig(18, "during", VPN, fail)[1])
    assert _status(logs, VPN, sel, "vpn.health") == {"ANOMALY"}
    assert _status(logs, VPN, sel, "vpn.status") == {"AUTHORIZATION_ERROR"}
    assert _status(logs, VPN, sel, "vpn.psk_version") == {"psk-2026q3-b1"}
    assert _fp(18)["vpn_tunnel_name"] == "gcpnet-vpn-tunnel-02"

    logs, v = _sig(18, "after", VPN, est, since=_within(18))
    assert set(v) == {1.0}
    assert set(_sig(18, "after", VPN, fail, since=_within(18))[1]) == {0.0}
    assert _status(logs, VPN, sel, "vpn.health") == {"NOMINAL"}
    assert _status(logs, VPN, sel, "vpn.psk_version") == {"psk-2026q4-b2"}
    _assert_audit(logs, 18)


def test_ch18_reestablishes_not_instantly_and_other_tunnels_unaffected():
    _, v = _sig(18, "after", VPN, "vpn.tunnel_02_established", since=10)
    assert set(v) == {0.0}
    _, v = _sig(18, "during", VPN, "vpn.tunnel_03_established")
    assert set(v) == {1.0}


# -- ch19: peer dropped IKEv1, tunnel-03 is IKEv1 ---------------------------------

def test_ch19_ike_phases():
    est, fail = "vpn.tunnel_03_established", "vpn.tunnel_03_ike_negotiation_failures"
    sel = "tunnel=gcpnet-vpn-tunnel-03 gateway=gcpnet-vpn-gw-europe1 region"
    logs, v = _sig(19, "before", VPN, fail)
    assert set(v) == {0.0}
    assert _status(logs, VPN, sel, "vpn.health") == {"NOMINAL"}
    assert _status(logs, VPN, sel, "vpn.ike_version") == {"1"}

    logs, v = _sig(19, "during", VPN, fail)
    assert all(3 <= x <= 10 for x in v), v
    assert set(_sig(19, "during", VPN, est)[1]) == {0.0}
    assert _status(logs, VPN, sel, "vpn.health") == {"ANOMALY"}
    assert _status(logs, VPN, sel, "vpn.ike_version") == {"1"}
    assert _status(logs, VPN, sel, "vpn.status") == {"NEGOTIATION_FAILURE"}
    assert _fp(19)["vpn_tunnel_name"] == "gcpnet-vpn-tunnel-03"
    assert all(d.startswith("Negotiation failure") and "no response from peer" in d
               for d in _status(logs, VPN, sel, "vpn.detailed_status"))

    logs, v = _sig(19, "after", VPN, fail, since=_within(19))
    assert set(v) == {0.0}
    assert set(_sig(19, "after", VPN, est, since=_within(19))[1]) == {1.0}
    assert _status(logs, VPN, sel, "vpn.health") == {"NOMINAL"}
    assert _status(logs, VPN, sel, "vpn.ike_version") == {"2"}
    assert _status(logs, VPN, sel, "vpn.detailed_status") == {"Tunnel is up and running."}
    _assert_audit(logs, 19)
    audits = bodies(logs, "vpc-network-manager", "gcp-audit:")
    assert any("methodName=v1.compute.vpnTunnels.insert" in a and "vpnTunnels/gcpnet-vpn-tunnel-03" in a
               for a in audits), audits


def test_ch19_fix_is_a_tunnel_replace():
    cmd = _cmd_lines(19)
    assert cmd == ["opsctl replace vpn-tunnel/gcpnet-vpn-tunnel-03 --region europe-west1 --ike-version 2"]


def test_ch19_unanswered_ike_evidence_pins_the_cause():
    """The IKEv1 first message is dropped (no clean error); other evidence points at the version."""
    logs, _ = records(phase(19, "during"))
    retrans = [r for r in logs if r["service"] == VPN and r["attrs"].get("operation") == "ike_retransmit"]
    assert retrans and all("no response from peer" in r["body"] or "giving up" in r["body"] for r in retrans)
    assert all(r["severity"] in ("WARN", "ERROR") for r in retrans)
    everything = " ".join(r["body"] for r in logs) + str(REG[19]["error_message"]) + str(REG[19]["stack_trace"])
    assert "INVALID_MAJOR_VERSION" not in everything and "ike_version_mismatch" not in str(_raw(19)["rca_clues"])
    # same peer, IKEv2 tunnel, same PSK version: the secret and the path are fine
    sib = [r for r in logs if r["service"] == VPN and r["attrs"].get("vpn.tunnel") == "gcpnet-vpn-tunnel-04"
           and r["attrs"].get("operation") == "tunnel_status"]
    t3 = [r for r in logs if r["service"] == VPN and r["attrs"].get("vpn.tunnel") == "gcpnet-vpn-tunnel-03"
          and r["attrs"].get("operation") == "tunnel_status"]
    assert sib and {r["attrs"]["vpn.status"] for r in sib} == {"ESTABLISHED"}
    assert {r["attrs"]["vpn.ike_version"] for r in sib} == {"2"}
    assert {r["attrs"]["vpn.peer_ip"] for r in sib} == {r["attrs"]["vpn.peer_ip"] for r in t3}
    assert {r["attrs"]["vpn.psk_version"] for r in sib} == {r["attrs"]["vpn.psk_version"] for r in t3}
    ev = REG[19]["infrastructure_events"][0]
    assert ev["offset_seconds"] < 0 and "IKEv2 only" in ev["body"]


def test_ch19_converges_and_is_independent_of_ch18():
    _, v = _sig(19, "after", VPN, "vpn.tunnel_03_ike_negotiation_failures", since=5)
    assert min(v) >= 3, v
    ctl = controller()
    ctl.trigger(18, user_email="n@example.com")
    ctl.trigger(19, user_email="n@example.com")
    for ch in (18, 19):
        ctl._channels[ch]["triggered_at"] -= 600
    _, metrics = records(ctl)
    assert set(values(metrics, VPN, "vpn.tunnel_02_established")) == {0.0}
    assert set(values(metrics, VPN, "vpn.tunnel_03_established")) == {0.0}


# -- dependents lag the component at fault (service_lag_seconds) ------------------

def _services_logging(channel: int, elapsed: float) -> set[str]:
    """Services logging WARN/ERROR *elapsed* s after *channel* fired."""
    c = ChaosController(channel_registry=REG)
    c.trigger(channel, user_email="sre@example.com")
    c._channels[channel]["triggered_at"] = time.time() - elapsed
    client = _RecordingOTLP()
    BaseService.set_context(CTX)
    try:
        services = [cls(c, client) for cls in SCN.get_service_classes()]
    finally:
        BaseService.clear_context()
    seen: set[str] = set()
    for svc in services:
        before = len(client.payloads)
        for _ in range(4):
            svc.generate_telemetry()
        for name, args, _kw in client.payloads[before:]:
            if name == "send_logs":
                for rec in args[1]:
                    if rec["severityText"] in ("WARN", "ERROR") and rec["body"]["stringValue"].startswith(
                            ("cloud-vpn:", "network-intelligence:", "vpc-network-manager:", "cloud-nat:", "[")):
                        seen.add(svc.SERVICE_NAME)
    return seen


def test_lag_values_are_30_to_90_and_cover_the_cascade():
    for ch in (12, 13):
        lag = REG[ch]["service_lag_seconds"]
        assert all(30 <= v <= 90 for v in lag.values()), lag
        assert set(REG[ch]["cascade_services"]) <= set(lag), (ch, lag)
        assert set(lag) <= set(REG[ch]["affected_services"]) | set(REG[ch]["cascade_services"])
    assert REG[12]["service_lag_seconds"]["vpc-network-manager"] < REG[12]["service_lag_seconds"]["cloud-vpn-gateway"]
    assert REG[13]["service_lag_seconds"]["cloud-vpn-gateway"] < REG[13]["service_lag_seconds"]["cloud-nat-gateway"]


def test_ch12_dependents_follow_the_interconnect():
    early = _services_logging(12, elapsed=10)
    mid = _services_logging(12, elapsed=50)
    late = _services_logging(12, elapsed=100)
    assert IC in early
    assert not {"vpc-network-manager", "network-intelligence", VPN} & early
    assert "vpc-network-manager" in mid and VPN not in mid
    assert {"vpc-network-manager", "network-intelligence", VPN} <= late


def test_ch13_cascade_follows_cloud_router():
    early = _services_logging(13, elapsed=10)
    mid = _services_logging(13, elapsed=60)
    late = _services_logging(13, elapsed=120)
    assert IC in early
    assert not {VPN, "vpc-network-manager", "cloud-nat-gateway"} & early
    assert VPN in mid and "cloud-nat-gateway" not in mid
    assert {VPN, "vpc-network-manager", "cloud-nat-gateway"} <= late


# -- shared: audit lines and opsctl end to end ---------------------------------

CHANNELS = [12, 13, 18, 19]


def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _assert_audit(logs, ch):
    audits = bodies(logs, "vpc-network-manager", "gcp-audit:")
    assert len(audits) >= len(_cmd_lines(ch)), audits
    assert any(f"principalEmail={OPS_ACTOR}" in b for b in audits), audits


def _ops_inst():
    ctl = controller()
    return SimpleNamespace(
        chaos_controller=ctl,
        ctx=SimpleNamespace(services={s: {} for s in SCN.services}, scenario=SCN),
        resolve_channel=lambda ch, **kw: ctl.resolve(ch, **kw),
    )


@pytest.mark.parametrize("ch", CHANNELS)
def test_expected_commands_parse(ch):
    assert len(ops.expected_commands(REG[ch]["remediation_command"], PROFILE)) == len(_cmd_lines(ch))


@pytest.mark.parametrize("ch", CHANNELS)
def test_opsctl_resolves_channel_with_one_audit_per_command(ch):
    inst = _ops_inst()
    ctl = inst.chaos_controller
    ctl.trigger(ch, user_email="netops@example.com")
    lines = _cmd_lines(ch)
    results = [ops.execute(inst, line) for line in lines]
    assert ctl.get_channel_status(ch)["state"] == "STANDBY"
    with ctl._lock:
        for e in ctl._pending_events:
            e["emit_at"] = 0
        evs, ctl._pending_events = list(ctl._pending_events), []
    audits = [e["body"] for e in evs if e["body"].startswith("gcp-audit:")]
    assert len(audits) == len(lines), audits
    for r, a in zip(results, audits):
        assert f"principalEmail={OPS_ACTOR}" in a and f"change_id={r['change_id']}" in a
        assert "{actor}" not in a
