"""Cloud NAT channels (07, 08, 09): before / during / after a fault.

The faulted gateway's signal sits in its normal band before the fault, deviates during it
(consistent with fault_params) while the status log says ANOMALY, and converges back after
the opsctl fix with the changed configuration on the log line. Other gateways stay at baseline.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from tests._gcp_harness import (
    OPS_ACTOR, PROFILE, REG, SCN, bodies, controller, phase, records,
)

SVC = "cloud-nat-gateway"
C, E, U = "gcpnet-nat-central1", "gcpnet-nat-east1", "gcpnet-nat-europe1"


def _within(ch):
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _m(metrics, name, gw):
    v = [m["value"] for m in metrics
         if m["service"] == SVC and m["name"] == name and m["attrs"].get("gateway") == gw]
    assert v, (name, gw)
    return v


def _logs(logs, gw, op):
    return [r for r in logs if r["service"] == SVC and r["attrs"].get("nat.gateway") == gw
            and r["attrs"].get("operation") == op]


def _attr(logs, gw, op, key):
    return {r["attrs"][key] for r in _logs(logs, gw, op)}


def _run(ch, which, **kw):
    return records(phase(ch, which, **kw))


# ── ch07: min ports per VM 1024 -> 64 ──────────────────────────────────────

def test_ch07_port_utilization_phases():
    util, drop, tot = "nat.port_utilization_pct", "nat.dropped_connections", "nat.ports_total"
    logs, m = _run(7, "before")
    assert all(15.0 <= x <= 45.0 for x in _m(m, util, C))
    assert all(x <= 5 for x in _m(m, drop, C))
    assert set(_m(m, tot, C)) == {1024.0}
    assert _attr(logs, C, "port_allocation", "nat.status") == {"NOMINAL"}

    logs, m = _run(7, "during")
    assert all(93.8 <= x <= 99.2 for x in _m(m, util, C)), _m(m, util, C)
    assert all(50 <= x <= 500 for x in _m(m, drop, C))
    assert set(_m(m, tot, C)) == {64.0}
    assert all(60 <= x <= 64 for x in _m(m, "nat.ports_used", C))
    assert _attr(logs, C, "port_allocation", "nat.status") == {"ANOMALY"}
    assert {int(v) for v in _attr(logs, C, "port_allocation", "nat.min_ports_per_vm")} == {64}
    # other gateways stay at baseline
    assert all(x <= 45.0 for x in _m(m, util, E) + _m(m, util, U))

    logs, m = _run(7, "after", since=_within(7))
    assert all(15.0 <= x <= 45.0 for x in _m(m, util, C)), _m(m, util, C)
    assert all(x <= 5 for x in _m(m, drop, C))
    assert set(_m(m, tot, C)) == {1024.0}
    assert _attr(logs, C, "port_allocation", "nat.status") == {"NOMINAL"}
    assert {int(v) for v in _attr(logs, C, "port_allocation", "nat.min_ports_per_vm")} == {1024}
    _assert_audit(logs, 7)


def test_ch07_ramps_and_converges():
    _, m = _run(7, "during", elapsed=5)
    assert max(_m(m, "nat.port_utilization_pct", C)) < 60.0
    # the pool is bigger at once: usage starts low and climbs as the retrying clients reconnect
    _, m = _run(7, "after", since=10)
    assert max(_m(m, "nat.port_utilization_pct", C)) < 20.0
    assert set(_m(m, "nat.ports_total", C)) == {1024.0}


# ── ch08: endpoint-independent mapping on ──────────────────────────────────

def test_ch08_mapping_failures_phases():
    mf, drop = "nat.mapping_failures", "nat.dropped_connections"
    logs, m = _run(8, "before")
    assert all(x <= 1 for x in _m(m, mf, C))
    assert _attr(logs, C, "connection_stats", "nat.endpoint_independent_mapping") == {False}
    assert _attr(logs, C, "connection_stats", "nat.status") == {"NOMINAL"}

    logs, m = _run(8, "during")
    assert all(100 <= x <= 2001 for x in _m(m, mf, C)), _m(m, mf, C)
    assert all(50 <= x <= 500 for x in _m(m, drop, C))  # co-moves with the failures
    ut = _m(m, "nat.port_utilization_pct", C)  # elevated by EIM, nowhere near exhaustion
    assert all(30.0 <= x <= 55.0 for x in ut), ut
    assert set(_m(m, "nat.ports_total", C)) == {1024.0}
    assert all(x <= 1 for x in _m(m, mf, E))
    assert _attr(logs, C, "connection_stats", "nat.endpoint_independent_mapping") == {True}
    assert _attr(logs, C, "connection_stats", "nat.status") == {"ANOMALY"}

    logs, m = _run(8, "after", since=_within(8))
    assert all(x <= 1 for x in _m(m, mf, C))
    assert all(x <= 5 for x in _m(m, drop, C))
    assert all(15.0 <= x <= 45.0 for x in _m(m, "nat.port_utilization_pct", C))
    assert _attr(logs, C, "connection_stats", "nat.endpoint_independent_mapping") == {False}
    assert _attr(logs, C, "connection_stats", "nat.status") == {"NOMINAL"}
    _assert_audit(logs, 8)


def test_ch08_ramps_and_converges():
    _, m = _run(8, "during", elapsed=5)
    assert max(_m(m, "nat.mapping_failures", C)) < 600
    _, m = _run(8, "after", since=10)
    # 10 s into a 60 s decay: still far above the baseline of 0-1, whatever the fault level drawn
    assert min(_m(m, "nat.mapping_failures", C)) > 50


# ── ch09: NAT IP allocation blocked by STATIC_ADDRESSES quota ──────────────

def test_ch09_ip_allocation_phases():
    pend, quota, alloc = "nat.pending_ip_allocations", "nat.static_address_quota_used", "nat.allocated_ips"
    logs, m = _run(9, "before")
    assert set(_m(m, pend, E)) == {0.0}
    assert set(_m(m, quota, E)) == {6.0}
    assert _attr(logs, E, "ip_pool_status", "nat.status") == {"NOMINAL"}

    logs, m = _run(9, "during")
    assert set(_m(m, pend, E)) == {3.0}, _m(m, pend, E)
    assert set(_m(m, quota, E)) == {10.0}
    assert set(_m(m, alloc, E)) == {5.0}
    assert set(_m(m, pend, C)) == {0.0}  # other regions unaffected
    assert _attr(logs, E, "ip_pool_status", "nat.status") == {"ANOMALY"}

    logs, m = _run(9, "after", since=_within(9))
    assert set(_m(m, pend, E)) == {0.0}
    assert set(_m(m, quota, E)) == {9.0}
    assert set(_m(m, alloc, E)) == {8.0}
    assert _attr(logs, E, "ip_pool_status", "nat.status") == {"NOMINAL"}
    assert {int(v) for v in _attr(logs, E, "ip_pool_status", "nat.static_address_quota_used")} == {9}
    _assert_audit(logs, 9)


def test_ch09_pending_converges_not_instant():
    _, m = _run(9, "after", since=10)
    assert max(_m(m, "nat.pending_ip_allocations", E)) >= 1


# ── shared: audit lines and opsctl end to end ──────────────────────────────

CHANNELS = [7, 8, 9]


def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _assert_audit(logs, ch):
    method = {7: "v1.compute.routers.patch", 8: "v1.compute.routers.patch",
              9: "v1.compute.addresses.delete"}[ch]
    audits = bodies(logs, "vpc-network-manager", f"methodName={method}")
    assert any(f"principalEmail={OPS_ACTOR}" in a for a in audits), audits


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
    for line in lines:
        assert ctl.get_channel_status(ch)["state"] != "STANDBY"
        ops.execute(inst, line)
    assert ctl.get_channel_status(ch)["state"] == "STANDBY"
    with ctl._lock:
        for e in ctl._pending_events:
            e["emit_at"] = 0
        evs, ctl._pending_events = list(ctl._pending_events), []
    audits = [e["body"] for e in evs if e["body"].startswith("gcp-audit:")]
    assert len(audits) == len(lines), [e["body"] for e in evs]
