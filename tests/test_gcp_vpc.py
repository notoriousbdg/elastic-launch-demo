"""gcp VPC channels (01, 02, 03): before / during / after a fault.

Each channel's signal metric sits in its baseline band before the fault, deviates during it
(consistent with the channel's fault_params) while the vpc-network-manager status log says
ANOMALY, and converges back after the opsctl fix, with the changed configuration on the status
log and one gcp-audit line per remediation command.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from tests._gcp_harness import (
    OPS_ACTOR, PROFILE, REG, SCN, attr_values, bodies, controller, phase, records, values,
)

SVC = "vpc-network-manager"


def _within(ch: int) -> float:
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _sig(ch, which, metric, **kw):
    logs, metrics = records(phase(ch, which, **kw))
    vals = values(metrics, SVC, metric)
    assert vals, f"{SVC} emitted no {metric}"
    return logs, vals


def _lines(logs, contains, key):
    return {str(r["attrs"][key]) for r in logs
            if r["service"] == SVC and contains in r["body"] and key in r["attrs"]}


def _fault_lines(logs, code, *tokens):
    return [r for r in logs if code in r["body"] and all(tok in r["body"] for tok in tokens)]


# -- ch01: prod imports staging custom routes -> 250/250 imported routes -------------

def test_ch01_peering_route_phases():
    m, d = "vpc.peering_imported_routes", "vpc.peering_dropped_prefixes"
    sel, st, cfg = "peering=gcpnet-vpc-staging-peer", "vpc.peering.status", "vpc.peering.import_custom_routes"
    logs, v = _sig(1, "before", m)
    assert all(176 <= x <= 186 for x in v), v
    assert set(values(records(phase(1, "before"))[1], SVC, d)) == {0.0}
    assert _lines(logs, sel, st) == {"NOMINAL"} and _lines(logs, sel, cfg) == {"false"}

    logs, v = _sig(1, "during", m)
    assert all(x == 250 for x in v), v  # capped at the limit
    logs, dv = _sig(1, "during", d)
    assert max(dv) == 18 and set(dv) <= {17.0, 18.0}, dv
    assert _lines(logs, sel, st) == {"ANOMALY"} and _lines(logs, sel, cfg) == {"true"}
    assert _fault_lines(logs, "VPC-ROUTE-LIMIT-EXCEEDED", "peering=gcpnet-vpc-staging-peer", "network=gcpnet-vpc-prod", "routes=250/250")

    logs, v = _sig(1, "after", m, since=_within(1))
    assert all(176 <= x <= 186 for x in v), v
    logs, dv = _sig(1, "after", d, since=_within(1))
    assert set(dv) == {0.0}
    assert _lines(logs, sel, st) == {"NOMINAL"} and _lines(logs, sel, cfg) == {"false"}
    _assert_audit(logs, 1)


def test_ch01_ramps_in_and_converges():
    _, early = _sig(1, "during", "vpc.peering_imported_routes", elapsed=5)
    assert max(early) < 250, early
    _, mid = _sig(1, "after", "vpc.peering_imported_routes", since=10)
    assert min(mid) > 190, mid


# -- ch02: node pool exhausts gcpnet-subnet-central1 -> expanded to /19 ------------------

def test_ch02_subnet_utilization_phases():
    m, sel, cidr = "vpc.subnet_ip_utilization_pct", "subnet=gcpnet-subnet-central1", "vpc.subnet.cidr"
    logs, v = _sig(2, "before", m)
    assert all(66 <= x <= 74 for x in v), v
    assert _lines(logs, sel, "vpc.subnet.status") == {"NOMINAL"} and _lines(logs, sel, cidr) == {"10.128.0.0/20"}

    logs, v = _sig(2, "during", m)
    assert all(98.0 <= x <= 99.5 for x in v), v
    assert _lines(logs, sel, "vpc.subnet.status") == {"ANOMALY"} and _lines(logs, sel, cidr) == {"10.128.0.0/20"}
    assert _fault_lines(logs, "VPC-SUBNET-IP-EXHAUSTION", "subnet=gcpnet-subnet-central1", "cidr=10.128.0.0/20", "/4092")
    assert {str(x) for x in attr_values(logs, SVC, "vpc.total_ips")} == {"4092"}

    logs, v = _sig(2, "after", m, since=_within(2))
    assert all(47 <= x <= 51 for x in v), v
    assert _lines(logs, sel, "vpc.subnet.status") == {"NOMINAL"} and _lines(logs, sel, cidr) == {"10.128.0.0/19"}
    assert "8188" in {str(x) for x in attr_values(logs, SVC, "vpc.total_ips")}
    _assert_audit(logs, 2)


def test_ch02_other_subnets_stay_baseline():
    logs, _ = records(phase(2, "during"))
    others = [r for r in logs if r["service"] == SVC and "vpc-monitor: subnet=" in r["body"]
              and "gcpnet-subnet-central1" not in r["body"]]
    assert others and all(r["attrs"]["vpc.utilization_pct"] < 85 for r in others)


def test_ch02_ramps_in_and_converges():
    _, early = _sig(2, "during", "vpc.subnet_ip_utilization_pct", elapsed=5)
    assert max(early) < 90, early
    # Expanding a subnet is instant: the used addresses stay where they were and the
    # utilization drops because the capacity grew, not because anything was released.
    _, metrics = records(phase(2, "after", since=5))
    used = [m["value"] for m in metrics
            if m["name"] == "vpc.subnet.used_ips" and m["attrs"].get("vpc.subnet") == "gcpnet-subnet-central1"]
    assert used and min(used) >= 3900, used
    _, util = _sig(2, "after", "vpc.subnet_ip_utilization_pct", since=5)
    assert all(45 <= x <= 52 for x in util), util


# -- ch03: deny-all-ingress at 900 above allow-internal-all ------------------------------

def test_ch03_firewall_denied_phases():
    m, sel, st, pr = "vpc.firewall_denied_packets", "vpc-firewall:", "vpc.firewall.status", "vpc.firewall.deny_rule_priority"
    logs, v = _sig(3, "before", m)
    assert all(0 <= x <= 20 for x in v), v
    assert _lines(logs, sel, st) == {"NOMINAL"} and _lines(logs, sel, pr) == {"65534"}

    logs, v = _sig(3, "during", m)
    assert all(100 <= x <= 5000 for x in v), v
    assert _lines(logs, sel, st) == {"ANOMALY"} and _lines(logs, sel, pr) == {"900"}
    assert _fault_lines(logs, "VPC-FIREWALL-RULE-CONFLICT", "rule_b=deny-all-ingress", "priority_b=900", "network=gcpnet-vpc-prod")

    logs, v = _sig(3, "after", m, since=_within(3))
    assert all(0 <= x <= 20 for x in v), v
    assert _lines(logs, sel, st) == {"NOMINAL"} and _lines(logs, sel, pr) == {"65534"}
    _assert_audit(logs, 3)


def test_ch03_ramps_in_and_converges():
    _, early = _sig(3, "during", "vpc.firewall_denied_packets", elapsed=2)
    assert max(early) < 400, early
    _, mid = _sig(3, "after", "vpc.firewall_denied_packets", since=5)
    assert min(mid) > 100, mid


# -- shared: audit lines and opsctl end to end -------------------------------------------

CHANNELS = [1, 2, 3]


def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _assert_audit(logs, ch):
    audits = bodies(logs, SVC, "gcp-audit:")
    assert len(audits) == len(_cmd_lines(ch)), audits
    assert all(f"principalEmail={OPS_ACTOR}" in a for a in audits)


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
    assert len(audits) >= len(lines), audits
    for r in results:
        assert any(f"change_id={r['change_id']}" in a for a in audits), (r, audits)
    assert all("{actor}" not in a for a in audits)
