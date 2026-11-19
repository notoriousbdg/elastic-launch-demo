"""gcp DNS + Network Intelligence channels (10, 11, 20): before / during / after a fault.

Each channel's signal metric sits in its baseline band before the fault, deviates during it
(matching fault_params) while the status log says ANOMALY, and converges back after the
opsctl fix with the changed configuration on the status log and one Cloud Audit line per
remediation command.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from tests._gcp_harness import (
    OPS_ACTOR, PROFILE, REG, SCN, attr_values, bodies, controller, phase, records, values,
)

DNS = "cloud-dns-resolver"
NI = "network-intelligence"


def _within(ch: int) -> float:
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _signal(ch, which, svc, metric, **kw):
    logs, metrics = records(phase(ch, which, **kw))
    vals = values(metrics, svc, metric)
    assert vals, f"{svc} emitted no {metric}"
    return logs, vals


def _status(logs, svc, op, key):
    return {r["attrs"].get(key) for r in logs if r["service"] == svc and r["attrs"].get("operation") == op}


# -- ch10: stale DS after KSK rotation -----------------------------------------

def test_ch10_dnssec_failures_phases():
    m = "dns.dnssec_validation_failures"
    logs, v = _signal(10, "before", DNS, m)
    assert all(0 <= x <= 1 for x in v), v
    assert _status(logs, DNS, "dnssec_status", "dns.status") == {"NOMINAL"}
    assert attr_values(logs, DNS, "dns.active_ksk_id") == {"ksk-2026q2"}

    logs, v = _signal(10, "during", DNS, m)
    assert all(50 <= x <= 500 for x in v), v
    assert _status(logs, DNS, "dnssec_status", "dns.status") == {"ANOMALY"}
    assert attr_values(logs, DNS, "dns.active_ksk_id") == {"ksk-2026q4-rotation"}
    assert _status(logs, DNS, "dnssec_status", "dns.zone") == {"gcpnet-public-zone"}
    assert _status(logs, DNS, "dnssec_status", "dns.ds_matches_active_ksk") == {"false"}

    logs, v = _signal(10, "after", DNS, m, since=_within(10))
    assert all(x <= 2 for x in v), v
    assert _status(logs, DNS, "dnssec_status", "dns.status") == {"NOMINAL"}
    assert attr_values(logs, DNS, "dns.active_ksk_id") == {"ksk-2026q2"}
    _assert_audit(logs, 10)


def test_ch10_ramps_in_and_converges():
    _, early = _signal(10, "during", DNS, "dns.dnssec_validation_failures", elapsed=2)
    assert max(early) < 50, early
    _, mid = _signal(10, "after", DNS, "dns.dnssec_validation_failures", since=10)
    assert min(mid) > 150, mid


def test_ch10_cached_servfail_decays_over_about_four_minutes():
    """The KSK is right again at once; resolvers' cached SERVFAILs expire over 1-5 minutes."""
    m = "dns.dnssec_validation_failures"
    assert _within(10) == 300
    tail = []
    for since in (10, 60, 120, 180, 230, 300):
        logs, v = _signal(10, "after", DNS, m, since=since)
        tail.append(sum(v) / len(v))
        # the zone side is fixed from the first second after the rollback
        assert attr_values(logs, DNS, "dns.active_ksk_id") == {"ksk-2026q2"}
        assert _status(logs, DNS, "dnssec_status", "dns.ds_matches_active_ksk") == {"true"}
    # the last points sit in the noise around zero, so only the clearly decaying part is ordered
    assert tail[:4] == sorted(tail[:4], reverse=True), tail
    assert tail[0] > 200 and 30 < tail[2] < 120 and tail[4] < 10 and tail[5] <= 3.0, tail
    logs, _ = _signal(10, "after", DNS, m, since=60)
    assert _status(logs, DNS, "dnssec_status", "dns.status") == {"ANOMALY"}
    _, sf = _signal(10, "after", DNS, "dns.servfail_count", since=120)
    assert 20 < min(sf), sf
    _, sf = _signal(10, "after", DNS, "dns.servfail_count", since=300)
    assert max(sf) <= 2, sf


def test_ch10_degraded_resolution_stats_are_warn():
    logs, _ = records(phase(10, "during"))
    rs = [r for r in logs if r["service"] == DNS and r["attrs"].get("operation") == "resolution_stats"]
    assert rs and {r["severity"] for r in rs} == {"WARN"}
    logs, _ = records(phase(10, "before"))
    rs = [r for r in logs if r["service"] == DNS and r["attrs"].get("operation") == "resolution_stats"]
    assert {r["severity"] for r in rs} == {"INFO"}


# -- ch11: blocked record store -> late propagation ----------------------------

def test_ch11_propagation_phases():
    m = "dns.propagation_time_s"
    logs, v = _signal(11, "before", DNS, m)
    assert all(20.0 <= x <= 45.0 for x in v), v
    assert set(values(records(phase(11, "before"))[1], DNS, "dns.servers_updated")) == {4.0}
    assert _status(logs, DNS, "zone_health", "dns.status") == {"NOMINAL"}
    assert _status(logs, DNS, "zone_health", "dns.publisher_blocked") == {"false"}

    logs, metrics = records(phase(11, "during"))
    v = values(metrics, DNS, m)
    assert all(120.0 <= x <= 600.0 for x in v), v
    assert all(1.0 <= x <= 3.0 for x in values(metrics, DNS, "dns.servers_updated"))
    assert _status(logs, DNS, "zone_health", "dns.status") == {"ANOMALY"}
    assert _status(logs, DNS, "zone_health", "dns.publisher_blocked") == {"true"}
    assert _status(logs, DNS, "zone_health", "dns.zone") == {"gcpnet-prod-zone"}

    logs, metrics = records(phase(11, "after", since=_within(11)))
    assert all(x <= 45.0 for x in values(metrics, DNS, m)), values(metrics, DNS, m)
    assert set(values(metrics, DNS, "dns.servers_updated")) == {4.0}
    assert _status(logs, DNS, "zone_health", "dns.status") == {"NOMINAL"}
    assert _status(logs, DNS, "zone_health", "dns.publisher_blocked") == {"false"}
    _assert_audit(logs, 11)


def test_ch11_ramps_in_and_converges():
    _, early = _signal(11, "during", DNS, "dns.propagation_time_s", elapsed=3)
    assert max(early) < 120.0, early
    _, mid = _signal(11, "after", DNS, "dns.propagation_time_s", since=15)
    assert min(mid) > 60.0, mid


def test_ch11_blocking_transaction_ends_when_session_is_killed_and_backlog_drains():
    w, p = "dns.record_store_blocked_writers", "dns.pending_changes"
    assert set(values(records(phase(11, "before"))[1], DNS, w)) == {0.0}
    assert set(values(records(phase(11, "before"))[1], DNS, p)) == {0.0}
    metrics = records(phase(11, "during"))[1]
    assert all(5 <= x <= 20 for x in values(metrics, DNS, w))
    assert all(5 <= x <= 20 for x in values(metrics, DNS, p))
    # killed session: no writer is blocked any more, the queue is still publishing
    metrics = records(phase(11, "after", since=10))[1]
    assert set(values(metrics, DNS, w)) == {0.0}
    assert min(values(metrics, DNS, p)) >= 7
    mid = values(records(phase(11, "after", since=25))[1], DNS, p)
    assert 0 < max(mid) < min(values(metrics, DNS, p))
    assert set(values(records(phase(11, "after", since=60))[1], DNS, p)) == {0.0}


def test_ch11_fix_stops_job_kills_session_then_syncs():
    cmds = _cmd_lines(11)
    assert cmds == [
        "opsctl delete job/dns-ttl-bulk-update",
        "opsctl delete sql-session --selector job=dns-ttl-bulk-update --instance gcpnet-dns-sqlserver",
        "opsctl sync dns-zone/gcpnet-prod-zone",
    ]
    logs, _ = records(phase(11, "after", since=_within(11)))
    audits = bodies(logs, "vpc-network-manager", "gcp-audit:")
    sql = [a for a in audits if "methodName=google.cloud.sql.v1.SqlInstancesService.ExecuteSql" in a]
    assert len(sql) == 1 and "resourceName=projects/gcpnet-prod-001/instances/gcpnet-dns-sqlserver" in sql[0]
    fields = {v["field"] for v in REG[11]["remediation_effects"]["verification"]}
    assert {"metrics.dns.record_store_blocked_writers", "metrics.dns.pending_changes"} <= fields


# -- ch20: gemini-2.5-pro on shared quota -> throttled -------------------------

def test_ch20_throttle_phases():
    t, p = "ni.ai_summary_throttle_pct", "ni.pending_analyses"
    logs, metrics = records(phase(20, "before"))
    assert all(0.0 <= x <= 2.0 for x in values(metrics, NI, t))
    assert all(x <= 6 for x in values(metrics, NI, p))
    assert _status(logs, NI, "ai_summary", "ni.status") == {"NOMINAL"}
    assert attr_values(logs, NI, "ni.model_deployment") == {"gemini-2.5-flash-ni-summarizer-v1"}

    logs, metrics = records(phase(20, "during"))
    assert all(35.0 <= x <= 45.0 for x in values(metrics, NI, t)), values(metrics, NI, t)
    assert all(20 <= x <= 300 for x in values(metrics, NI, p)), values(metrics, NI, p)
    assert _status(logs, NI, "ai_summary", "ni.status") == {"ANOMALY"}
    assert attr_values(logs, NI, "ni.model_deployment") == {"gemini-2.5-pro-ni-summarizer-v2"}
    assert _status(logs, NI, "ai_summary", "ni.vertex_endpoint") == {"gcpnet-ni-assist-central1"}

    logs, metrics = records(phase(20, "after", since=_within(20)))
    assert all(x <= 2.0 for x in values(metrics, NI, t)), values(metrics, NI, t)
    assert all(x <= 10 for x in values(metrics, NI, p)), values(metrics, NI, p)
    assert _status(logs, NI, "ai_summary", "ni.status") == {"NOMINAL"}
    assert attr_values(logs, NI, "ni.model_deployment") == {"gemini-2.5-flash-ni-summarizer-v1"}
    _assert_audit(logs, 20)


def test_ch20_ramps_in_and_converges():
    _, early = _signal(20, "during", NI, "ni.ai_summary_throttle_pct", elapsed=3)
    assert max(early) < 35.0, early
    _, mid = _signal(20, "after", NI, "ni.ai_summary_throttle_pct", since=15)
    assert min(mid) > 10.0, mid
    _, pend = _signal(20, "after", NI, "ni.pending_analyses", since=15)
    assert min(pend) > 10, pend


# -- shared: audit lines and opsctl end to end ---------------------------------

CHANNELS = [10, 11, 20]


def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _assert_audit(logs, ch):
    audits = bodies(logs, "vpc-network-manager", "gcp-audit:")
    effects = REG[ch]["remediation_effects"]["log_events"]
    expected = [e for e in effects if e["body"].startswith("gcp-audit:")]
    assert len(expected) == len(_cmd_lines(ch))
    for e in expected:
        method = e["body"].split("methodName=")[1].split()[0]
        hits = [a for a in audits if f"methodName={method}" in a and f"principalEmail={OPS_ACTOR}" in a]
        assert hits, (method, audits)


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
    results = []
    for line in lines:
        assert ctl.get_channel_status(ch)["state"] != "STANDBY"
        results.append(ops.execute(inst, line))
    assert ctl.get_channel_status(ch)["state"] == "STANDBY"
    with ctl._lock:
        for e in ctl._pending_events:
            e["emit_at"] = 0
        evs, ctl._pending_events = list(ctl._pending_events), []
    audits = [e["body"] for e in evs if e["body"].startswith("gcp-audit:")]
    assert len(audits) == len(lines), audits
    for r in results:
        assert sum(f"change_id={r['change_id']}" in a for a in audits) == 1, (r, audits)
    assert all("{actor}" not in a for a in audits)
