"""GCP network edge channels (14, 15, 16, 17): before / during / after a fault.

Each channel's signal sits in its baseline band before the fault, deviates during it (matching
fault_params) while the status log says ANOMALY, and converges back after the opsctl fix with
the changed configuration on the status log and one gcp-audit line per remediation command.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import yaml

import pytest

from app import ops
from tests._gcp_harness import (
    OPS_ACTOR, PROFILE, REG, SCN, attr_values, bodies, controller, phase, records, values,
)

_CH_DIR = Path(__file__).resolve().parent.parent / "scenarios" / "gcp" / "channels"


def _fp(ch: int) -> dict:
    path = next(_CH_DIR.glob(f"{ch:02d}-*.yaml"))
    return yaml.safe_load(path.read_text())["fault_params"]


CDN = "cloud-cdn-service"
LB = "cloud-load-balancer"


def _within(ch: int) -> float:
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _sig(ch, which, svc, metric, **kw):
    logs, metrics = records(phase(ch, which, **kw))
    vals = values(metrics, svc, metric)
    assert vals, f"{svc} emitted no {metric}"
    return logs, vals


def _status(logs, svc, contains, key):
    return {r["attrs"].get(key) for r in logs if r["service"] == svc and contains in r["body"]}


# -- ch14: cache key policy -> CDN hit ratio collapse -----------------

def test_ch14_hit_ratio_phases():
    m, key = "cdn.backend_hit_ratio_pct", "cdn.cache_status"
    logs, v = _sig(14, "before", CDN, m)
    assert all(86.0 <= x <= 96.0 for x in v), v
    assert _status(logs, CDN, "cache_key backend=", key) == {"OK"}
    assert attr_values(logs, CDN, "cdn.cache_key_policy") == {"exclude-query-params"}
    assert all(x < 20000 for x in _sig(14, "before", CDN, "cdn.unique_cache_keys")[1])

    logs, v = _sig(14, "during", CDN, m)
    assert all(15.0 <= x <= 45.0 for x in v), v
    assert _status(logs, CDN, "cache_key backend=", key) == {"ANOMALY"}
    assert attr_values(logs, CDN, "cdn.cache_key_policy") == {"include-all-query-params"}
    assert all(x >= 50000 for x in _sig(14, "during", CDN, "cdn.unique_cache_keys")[1])
    assert _fp(14)["cdn_backend"] == "gcpnet-cdn-backend-01"

    logs, v = _sig(14, "after", CDN, m, since=_within(14))
    assert all(86.0 <= x <= 96.0 for x in v), v
    assert _status(logs, CDN, "cache_key backend=", key) == {"OK"}
    assert attr_values(logs, CDN, "cdn.cache_key_policy") == {"exclude-query-params"}
    assert all(x < 20000 for x in _sig(14, "after", CDN, "cdn.unique_cache_keys", since=_within(14))[1])
    _assert_audit(logs, 14)


def test_ch14_ramps_and_rewarms_gradually():
    _, early = _sig(14, "during", CDN, "cdn.backend_hit_ratio_pct", elapsed=5)
    assert min(early) > 60.0, early
    _, mid = _sig(14, "after", CDN, "cdn.backend_hit_ratio_pct", since=30)
    assert max(mid) < 80.0, mid


# -- ch15: bucket IAM -> origin 403, stale served ----------------------------

def test_ch15_origin_phases():
    err, stale = "cdn.origin_fetch_errors", "cdn.stale_served_pct"
    logs, v = _sig(15, "before", CDN, err)
    assert all(0 <= x <= 2 for x in v), v
    assert all(0.0 <= x <= 2.0 for x in _sig(15, "before", CDN, stale)[1])
    assert _status(logs, CDN, "backend_bucket=media-assets", "cdn.origin_health") == {"OK"}
    assert _status(logs, CDN, "backend_bucket=media-assets", "cdn.origin_status") == {"200"}

    logs, v = _sig(15, "during", CDN, err)
    assert all(50 <= x <= 500 for x in v), v
    assert all(20.0 <= x <= 80.0 for x in _sig(15, "during", CDN, stale)[1])
    assert _status(logs, CDN, "backend_bucket=media-assets", "cdn.origin_health") == {"ANOMALY"}
    assert _status(logs, CDN, "backend_bucket=media-assets", "cdn.origin_status") == {"403"}
    assert _fp(15)["cdn_origin_status"] == 403

    logs, v = _sig(15, "after", CDN, err, since=_within(15))
    assert all(0 <= x <= 2 for x in v), v
    assert all(x <= 2.0 for x in _sig(15, "after", CDN, stale, since=_within(15))[1])
    assert _status(logs, CDN, "backend_bucket=media-assets", "cdn.origin_health") == {"OK"}
    assert _status(logs, CDN, "backend_bucket=media-assets", "cdn.origin_status") == {"200"}
    _assert_audit(logs, 15)


def test_ch15_converges_rather_than_snaps():
    _, early = _sig(15, "after", CDN, "cdn.stale_served_pct", since=10)
    assert min(early) > 5.0, early
    _, ramp = _sig(15, "during", CDN, "cdn.origin_fetch_errors", elapsed=2)
    assert max(ramp) < 100, ramp


# -- ch16: web MIG template -> backends unhealthy ---------------------------

def test_ch16_backend_phases():
    m, key = "lb.healthy_backends", "lb.backend_status"
    logs, v = _sig(16, "before", LB, m)
    assert set(v) == {10.0}, v
    assert _status(logs, LB, "mig=gcpnet-web-mig", key) == {"OK"}
    assert attr_values(logs, LB, "lb.instance_template") == {"web-frontend-v3-7-4"}

    fp = _fp(16)
    logs, v = _sig(16, "during", LB, m)
    assert fp["lb_total_backends"] == 10 and fp["gcp_region"] == "us-central1"
    assert all(x == fp["lb_total_backends"] - fp["lb_unhealthy_count"] for x in v), v
    assert _status(logs, LB, "mig=gcpnet-web-mig", key) == {"ANOMALY"}
    assert "web-frontend-v3-8-0" in attr_values(logs, LB, "lb.instance_template")
    assert all(x > 90.0 for x in _sig(16, "during", LB, "lb.backend_cpu_max_pct")[1])
    assert all(x >= 5.0 for x in _sig(16, "during", LB, "lb.error_rate_5xx_pct")[1])

    logs, v = _sig(16, "after", LB, m, since=_within(16))
    assert set(v) == {10.0}, v
    assert _status(logs, LB, "mig=gcpnet-web-mig", key) == {"OK"}
    assert attr_values(logs, LB, "lb.instance_template") == {"web-frontend-v3-7-4"}
    assert all(x < 70.0 for x in _sig(16, "after", LB, "lb.backend_cpu_max_pct", since=_within(16))[1])
    _assert_audit(logs, 16)


def test_ch16_rolls_back_one_instance_at_a_time():
    _, v = _sig(16, "after", LB, "lb.healthy_backends", since=30)
    assert all(6.0 < x < 10.0 for x in v), v
    _, ramp = _sig(16, "during", LB, "lb.healthy_backends", elapsed=5)
    assert all(x > 6.0 for x in ramp), ramp


# -- ch17: domain cutover before the DNS move -> managed cert stuck PROVISIONING --

def test_ch17_cert_phases():
    a, f = "lb.ssl_cert_active", "lb.tls_handshake_failures"
    logs, v = _sig(17, "before", LB, a)
    assert set(v) == {1.0}, v
    assert all(0 <= x <= 2 for x in _sig(17, "before", LB, f)[1])
    assert attr_values(logs, LB, "lb.ssl_managed_status") == {"ACTIVE"}
    assert attr_values(logs, LB, "lb.ssl_domain_status") == {"ACTIVE"}
    assert attr_values(logs, LB, "lb.ssl_dns_a_record") == {"34.149.212.91"}
    assert _status(logs, LB, "ssl_cert=", "lb.cert_status") == {"OK"}

    fp = _fp(17)
    logs, v = _sig(17, "during", LB, a)
    assert set(v) == {0.0}, v
    assert all(300 <= x <= 1500 for x in _sig(17, "during", LB, f)[1])
    assert attr_values(logs, LB, "lb.ssl_managed_status") == {fp["ssl_managed_status"]}
    assert attr_values(logs, LB, "lb.ssl_domain_status") == {fp["ssl_domain_status"]}
    assert attr_values(logs, LB, "lb.ssl_dns_a_record") == {fp["ssl_old_ip"]}
    assert _status(logs, LB, "ssl_cert=", "lb.cert_status") == {"ANOMALY"}
    assert {r["severity"] for r in logs if r["service"] == LB and "ssl_cert=" in r["body"]} == {"WARN"}

    logs, v = _sig(17, "after", LB, a, since=_within(17))
    assert set(v) == {1.0}, v
    assert all(0 <= x <= 2 for x in _sig(17, "after", LB, f, since=_within(17))[1])
    assert attr_values(logs, LB, "lb.ssl_managed_status") == {"ACTIVE"}
    assert attr_values(logs, LB, "lb.ssl_dns_a_record") == {fp["ssl_new_ip"]}
    assert _status(logs, LB, "ssl_cert=", "lb.cert_status") == {"OK"}
    _assert_audit(logs, 17)


def test_ch17_recovery_is_not_instant():
    fp = _fp(17)
    # record moved, resolvers and Google's validator still see the old target
    logs, active = _sig(17, "after", LB, "lb.ssl_cert_active", since=10)
    assert set(active) == {0.0}
    assert _status(logs, LB, "ssl_cert=", "lb.ssl_domain_status") == {"FAILED_NOT_VISIBLE"}
    assert _status(logs, LB, "ssl_cert=", "lb.ssl_dns_a_record") == {fp["ssl_old_ip"]}
    # validation and issuance in progress: still no certificate, failures unchanged
    logs, active = _sig(17, "after", LB, "lb.ssl_cert_active", since=90)
    assert set(active) == {0.0}
    assert _status(logs, LB, "ssl_cert=", "lb.ssl_managed_status") == {"PROVISIONING"}
    assert _status(logs, LB, "ssl_cert=", "lb.ssl_domain_status") == {"PROVISIONING"}
    assert _status(logs, LB, "ssl_cert=", "lb.ssl_dns_a_record") == {fp["ssl_new_ip"]}
    assert all(x >= 300 for x in _sig(17, "after", LB, "lb.tls_handshake_failures", since=90)[1])
    # ACTIVE: failures gone
    _, fails = _sig(17, "after", LB, "lb.tls_handshake_failures", since=200)
    assert all(x <= 2 for x in fails), fails
    _, ramp = _sig(17, "during", LB, "lb.tls_handshake_failures", elapsed=4)
    # 4 s into a 20 s ramp: about a fifth of the fault level (up to 1500), far below its ~900 mean
    assert all(x < 450 for x in ramp), ramp


def test_ch17_single_dns_command_and_cause_before_effect():
    assert _cmd_lines(17) == [
        "opsctl patch dns-record/api.gcpnet.example.com --zone gcpnet-public-zone --type A "
        "--rrdata " + _fp(17)["ssl_new_ip"]]
    assert _within(17) == 300
    evs = REG[17]["infrastructure_events"]
    assert evs and all(e["offset_seconds"] < 0 for e in evs)
    assert any("still resolves to " + _fp(17)["ssl_old_ip"] in e["body"] for e in evs)


# -- ch16 service lag: dependents show symptoms after the load balancer ------

def _warn_services(ch: int, elapsed: float) -> set[str]:
    logs, _ = records(phase(ch, "during", elapsed=elapsed))
    # symptom lines about the unhealthy backends (the CDN's own cache-key / origin status lines are not)
    return {r["service"] for r in logs if r["severity"] in ("WARN", "ERROR")
            and ("unhealthy" in r["body"] or "traffic_management" in r["body"] or "LB-BACKEND" in r["body"])}


def test_ch16_service_lag_orders_dependents_after_lb():
    lag = REG[16]["service_lag_seconds"]
    assert set(lag) <= set(REG[16]["affected_services"]) | set(REG[16]["cascade_services"])
    assert LB not in lag
    assert 30 <= lag[CDN] <= 60 and 30 <= lag["cloud-armor-waf"] <= 90
    early, late = _warn_services(16, 5), _warn_services(16, 100)
    assert LB in early
    assert CDN not in early and "cloud-armor-waf" not in early
    assert {LB, CDN} <= late


# -- shared: audit lines and opsctl end to end ------------------------------

CHANNELS = [14, 15, 16, 17]


def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _methods(ch):
    return [ev["body"].split("methodName=")[1].split()[0]
            for ev in REG[ch]["remediation_effects"]["log_events"]
            if ev["body"].startswith("gcp-audit:")]


def _assert_audit(logs, ch):
    audits = bodies(logs, "vpc-network-manager", "gcp-audit:")
    for method in _methods(ch):
        hits = [b for b in audits if f"methodName={method}" in b]
        assert len(hits) == 1, (method, audits)
        assert f"principalEmail={OPS_ACTOR}" in hits[0]
    assert len(_methods(ch)) == len(_cmd_lines(ch))


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
    assert len(audits) >= len(lines), audits
    for r in results:
        assert any(f"change_id={r['change_id']}" in a for a in audits), (r, audits)
    for a in audits:
        assert "{actor}" not in a
