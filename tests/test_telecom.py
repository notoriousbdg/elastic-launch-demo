"""Meridian Telecom (telecom) channels before, during and after a fault.

Each channel's signal metric sits in its normal band before the fault, deviates during it,
and is back in band once the verification window after the opsctl fix has passed. The
configuration the fix changes is on the component's status lines (the channel's
correlation_attr while faulted, its steady_attributes value after), the error signature
stops after the fix, and every remediation command resolves through opsctl with exactly
one audit record.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from tests._telecom_harness import (
    OPS_ACTOR, PROFILE, REG, SCN, attr_values, bodies, controller, phase, records, values,
)

# ch: (service, metric, before band, during band, after band, seconds after the fix)
SIGNALS = {
    1: ("provisioning-service", "provisioning_service.hikari_pending_threads", (0, 0), (130, 240), (0, 0), 120),
    2: ("subscriber-db", "subscriber_db.query_latency_p99_ms", (15, 30), (4000, 5000), (15, 50), 180),
    3: ("identity-db", "identity_db.replica_lag_s", (0, 1.5), (100, 145), (0, 1.5), 240),
    4: ("network-core", "network_core.prb_utilization_pct", (50, 62), (92, 100), (50, 62), 150),
    5: ("network-core", "network_core.mme_cpu_pct", (37, 48), (88, 99), (37, 48), 240),
    6: ("network-core", "network_core.ue_ip_pool_utilization_pct", (59, 63), (99, 100), (59, 64), 60),
    7: ("auth-service", "auth_service.jwt_validation_failures_per_min", (0, 10), (1200, 1600), (0, 10), 60),
    8: ("auth-service", "auth_service.federation_failures_per_min", (0, 0), (120, 270), (0, 0), 120),
    9: ("auth-service", "auth_service.session_store_used_memory_pct", (60, 64), (98.5, 100), (60, 64), 150),
    10: ("provisioning-service", "provisioning_service.hlr_queue_depth", (0, 40), (1600, 1800), (0, 40), 300),
    11: ("provisioning-service", "provisioning_service.mnp_lookup_failures_per_min", (0, 0), (40, 120), (0, 0), 60),
    12: ("bss-billing", "bss_billing.order_rejects_per_min", (0, 0), (20, 70), (0, 0), 90),
    13: ("bss-billing", "bss_billing.cdr_consumer_lag", (0, 1500), (55000, 66000), (0, 1500), 600),
    14: ("bss-billing", "bss_billing.mediation_pods_ready", (3, 3), (2, 2), (3, 3), 90),
    15: ("activation-service", "activation_service.enterprise_activation_p95_s", (8, 16), (95, 135), (8, 16), 150),
    16: ("api-gateway", "api_gateway.http_429_per_sec", (0, 1), (160, 215), (0, 1), 60),
    17: ("api-gateway", "api_gateway.mtls_handshake_failures_per_min", (0, 0), (1800, 2600), (0, 0), 90),
    18: ("notification-service", "notification_service.smpp_binds_active", (8, 8), (0, 0), (8, 8), 60),
    19: ("notification-service", "notification_service.push_429_per_min", (0, 0), (1600, 2200), (0, 0), 120),
    20: ("notification-service", "notification_service.webhook_timeouts_per_min", (0, 0), (80, 170), (0, 0), 60),
}

CHANNELS = sorted(REG)


def _in(vals, band, label):
    lo, hi = band
    assert vals, label
    assert all(lo <= v <= hi for v in vals), (label, band, vals)


def _within(ch):
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def test_every_channel_has_a_signal_row():
    assert sorted(SIGNALS) == CHANNELS == list(range(1, 21))


@pytest.mark.parametrize("ch", CHANNELS)
def test_signal_before_during_after(ch):
    svc, name, before, during, after, since = SIGNALS[ch]
    _, m = records(phase(ch, "before"))
    _in(values(m, svc, name), before, f"ch{ch} before")
    _, m = records(phase(ch, "during"))
    _in(values(m, svc, name), during, f"ch{ch} during")
    _, m = records(phase(ch, "after", since=since))
    _in(values(m, svc, name), after, f"ch{ch} after {since}s")


# The value was already in place before the incident; what changed was around it (a new
# signing key, a newly published plan).
UNCHANGED_BEFORE = {7, 12}


@pytest.mark.parametrize("ch", CHANNELS)
def test_config_attribute_follows_fault_and_fix(ch):
    corr = SCN._channels[ch]["correlation_attr"]
    key, faulty = corr["key"], str(corr["value"])
    steady = REG[ch]["remediation_effects"]["steady_attributes"]
    (svc, fixed), = ((s, str(a[key])) for s, a in steady.items() if key in a)
    if ch not in UNCHANGED_BEFORE:
        logs, _ = records(phase(ch, "before"))
        assert faulty not in {str(v) for v in attr_values(logs, svc, key)}, (ch, key)
    logs, _ = records(phase(ch, "during"))
    assert faulty in {str(v) for v in attr_values(logs, svc, key)}, (ch, key, attr_values(logs, svc, key))
    logs, _ = records(phase(ch, "after", since=_within(ch)))
    assert {str(v) for v in attr_values(logs, svc, key)} == {fixed}, (ch, key, attr_values(logs, svc, key))


@pytest.mark.parametrize("ch", CHANNELS)
def test_status_anomaly_only_while_faulted(ch):
    svc = SIGNALS[ch][0]
    logs, _ = records(phase(ch, "before"))
    assert not bodies(logs, svc, "status=ANOMALY"), ch
    logs, _ = records(phase(ch, "during"))
    assert bodies(logs, svc, "status=ANOMALY"), ch
    logs, _ = records(phase(ch, "after", since=SIGNALS[ch][5]))
    assert not bodies(logs, svc, "status=ANOMALY"), (ch, bodies(logs, svc, "status=ANOMALY"))


@pytest.mark.parametrize("ch", CHANNELS)
def test_error_signature_from_error_services_and_stops_after_fix(ch):
    et = REG[ch]["error_type"]
    err_svcs = set(REG[ch].get("error_services") or REG[ch]["affected_services"])
    logs, _ = records(phase(ch, "during"))
    errs = [r for r in logs if r["severity"] == "ERROR" and r["attrs"].get("error.type") == et]
    assert errs and {r["service"] for r in errs} <= err_svcs, (ch, {r["service"] for r in errs})
    assert all(et in r["body"] for r in errs), ch
    logs, _ = records(phase(ch, "after", since=_within(ch)))
    assert not [r for r in logs if r["attrs"].get("error.type") == et], ch


@pytest.mark.parametrize("ch", CHANNELS)
def test_error_type_is_unique_to_its_channel(ch):
    et = REG[ch]["error_type"]
    for other in CHANNELS:
        if other != ch:
            assert et not in REG[other]["error_message"], (ch, other)


@pytest.mark.parametrize("ch", CHANNELS)
def test_alert_phrase_appears_only_while_its_channel_is_faulted(ch):
    # alert rules match error_type as a phrase in body.text, on any service and any line
    et = REG[ch]["error_type"]
    for which, kw in (("before", {}), ("after", {"since": _within(ch)})):
        logs, _ = records(phase(ch, which, **kw))
        assert not [r["body"] for r in logs if et in r["body"]], (ch, which)
    logs, _ = records(phase(ch, "during"))
    for other in CHANNELS:
        if other != ch:
            phrase = REG[other]["error_type"]
            assert not [r["body"] for r in logs if phrase in r["body"]], (ch, other)


@pytest.mark.parametrize("ch", CHANNELS)
def test_cause_event_precedes_symptoms(ch):
    events = REG[ch]["infrastructure_events"]
    assert any(float(e.get("offset_seconds", 0)) < 0 for e in events), ch


@pytest.mark.parametrize("ch", CHANNELS)
def test_backdated_cause_does_not_contradict_status_lines(ch):
    # status lines show the old configuration until the trigger, so an event further back than
    # a rollout or reload must not claim the new one (a pending change or an issuer switch may)
    key = SCN._channels[ch]["correlation_attr"]["key"]
    for e in REG[ch]["infrastructure_events"]:
        if float(e.get("offset_seconds", 0)) < -120:
            assert key not in (e.get("attributes") or {}), (ch, e["body"][:80])


# ch: (seconds after the trigger, service, metric, band, error phrase not yet logged)
BUILD_UP = {
    2: (10, "subscriber-db", "subscriber_db.query_latency_p99_ms", None, "canceling statement"),
    5: (60, "network-core", "network_core.mme_cpu_pct", (55, 80), "OVERLOAD START"),
    6: (45, "network-core", "network_core.ue_ip_pool_utilization_pct", (40, 60), "All dynamic addresses"),
    9: (60, "auth-service", "auth_service.session_store_used_memory_pct", (75, 85), "OOM command"),
    13: (30, "bss-billing", "bss_billing.cdr_consumer_lag", (2500, 5000), "lag increasing on"),
    14: (60, "bss-billing", "bss_billing.mediation_volume_used_pct", (83, 88), "No space left"),
}


@pytest.mark.parametrize("ch", sorted(BUILD_UP))
def test_cause_builds_up_before_the_first_error(ch):
    elapsed, svc, name, band, phrase = BUILD_UP[ch]
    logs, m = records(phase(ch, "during", elapsed=elapsed))
    if band is None:
        assert not values(m, svc, name), ch
    else:
        _in(values(m, svc, name), band, f"ch{ch} at {elapsed}s")
    assert not [r for r in logs if phrase in r["body"]], ch


@pytest.mark.parametrize("ch", CHANNELS)
def test_verification_metrics_are_emitted_after_fix(ch):
    _, m = records(phase(ch, "after", since=_within(ch)))
    names = {x["name"] for x in m}
    for v in REG[ch]["remediation_effects"]["verification"]:
        field = str(v.get("field") or "")
        if field.startswith("metrics."):
            assert field[len("metrics."):] in names, (ch, field)


# ── convergence, not snapping ──────────────────────────────────────────────

def test_ch01_rollback_drains_pod_by_pod():
    _, m = records(phase(1, "after", since=15))
    assert min(values(m, "provisioning-service", "provisioning_service.hikari_pending_threads")) > 60


def test_ch02_reboot_gap_then_cache_warmup():
    logs, m = records(phase(2, "after", since=10))
    assert not values(m, "subscriber-db", "subscriber_db.query_latency_p99_ms")
    assert bodies(logs, "subscriber-db", "status=rebooting")
    _, m = records(phase(2, "after", since=35))
    assert all(80 <= v <= 200 for v in values(m, "subscriber-db", "subscriber_db.query_latency_p99_ms"))
    _, m = records(phase(2, "after", since=35))
    assert all(v <= 4 for v in values(m, "subscriber-db", "subscriber_db.seq_scans_per_s"))


def test_ch03_before_is_the_d8_replica_in_sync():
    logs, m = records(phase(3, "before"))
    assert attr_values(logs, "identity-db", "db.replica_sku") == {"Standard_D8ds_v5"}
    assert all(v < 1.5 for v in values(m, "identity-db", "identity_db.replica_lag_s"))


def test_ch03_scale_down_restarts_the_replica_before_errors_start():
    logs, m = records(phase(3, "during", elapsed=30))
    assert attr_values(logs, "identity-db", "db.replica_state") == {"restarting"}
    assert attr_values(logs, "identity-db", "db.replica_sku") == {"Standard_D2ds_v5"}
    assert all(20 <= v <= 40 for v in values(m, "identity-db", "identity_db.replica_lag_s"))
    assert not bodies(logs, "auth-service", "session lookup returned no row")
    logs, m = records(phase(3, "during", elapsed=90))
    assert bodies(logs, "auth-service", "session lookup returned no row")
    assert all(v > 90 for v in values(m, "identity-db", "identity_db.replica_cpu_pct"))


def test_ch03_lag_keeps_growing_while_the_replica_restarts():
    _, m = records(phase(3, "after", since=60))
    assert min(values(m, "identity-db", "identity_db.replica_lag_s")) > 100


def test_ch05_tau_decays_over_minutes():
    _, m = records(phase(5, "after", since=60))
    tau = values(m, "network-core", "network_core.tau_per_min")
    assert all(20000 <= v <= 30000 for v in tau), tau
    assert all(v < 85 for v in values(m, "network-core", "network_core.mme_cpu_pct"))


def test_ch08_failures_continue_until_the_policy_applies():
    _, m = records(phase(8, "after", since=30))
    assert min(values(m, "auth-service", "auth_service.federation_failures_per_min")) > 100


def test_ch10_queue_grows_during_and_drains_after():
    _, early = records(phase(10, "during", elapsed=60))
    _, late = records(phase(10, "during", elapsed=300))
    assert max(values(early, "provisioning-service", "provisioning_service.hlr_queue_depth")) < \
        min(values(late, "provisioning-service", "provisioning_service.hlr_queue_depth"))
    _, m = records(phase(10, "after", since=60))
    q = values(m, "provisioning-service", "provisioning_service.hlr_queue_depth")
    assert all(1000 <= v <= 1300 for v in q), q
    assert set(values(m, "provisioning-service", "provisioning_service.hlr_sessions_in_use")) == {16.0}


def test_ch13_lag_falls_at_the_catch_up_rate():
    svc, name = "bss-billing", "bss_billing.cdr_consumer_lag"
    _, a = records(phase(13, "after", since=60))
    _, b = records(phase(13, "after", since=120))
    assert max(values(b, svc, name)) < min(values(a, svc, name)) - 15000
    rate = values(b, svc, "bss_billing.cdr_rating_rate_per_min")
    assert all(34000 <= v <= 38000 for v in rate), rate


def test_ch17_before_is_the_old_cert_at_its_renewal_point():
    logs, m = records(phase(17, "before"))
    assert set(values(m, "api-gateway", "api_gateway.client_cert_days_remaining")) == {10.0}
    assert attr_values(logs, "api-gateway", "tls.client_cert_serial") == {"4f:9a:1c:e2"}
    assert attr_values(logs, "api-gateway", "tls.client_cert_issuer_ca") == {"meridian-mtls-ca-2025"}


def test_ch17_renewed_cert_is_fresh_but_untrusted():
    logs, m = records(phase(17, "during"))
    assert set(values(m, "api-gateway", "api_gateway.client_cert_days_remaining")) == {30.0}
    assert attr_values(logs, "api-gateway", "tls.client_cert_serial") == {"9c:3e:51:a0"}


def test_ch17_cert_reissued_after_about_twenty_seconds():
    _, m = records(phase(17, "after", since=10))
    assert min(values(m, "api-gateway", "api_gateway.mtls_handshake_failures_per_min")) > 1500
    logs, m = records(phase(17, "after", since=60))
    assert set(values(m, "api-gateway", "api_gateway.mtls_handshake_failures_per_min")) == {0.0}
    assert attr_values(logs, "api-gateway", "tls.client_cert_serial") == {"7b:21:e0:5d"}


def test_ch18_queue_drains_at_contracted_throughput():
    _, m = records(phase(18, "after", since=75))
    q = values(m, "notification-service", "notification_service.sms_queue_depth")
    assert all(3800 <= v <= 4400 for v in q), q
    assert all(v >= 2800 for v in values(m, "notification-service", "notification_service.sms_delivered_per_min"))


def test_other_signals_stay_at_baseline_during_a_fault():
    # a fault on one component leaves the signals of unrelated channels in their normal band
    _, m = records(phase(7, "during"))
    for ch in (1, 4, 13, 16, 19):
        svc, name, before, *_ = SIGNALS[ch]
        _in(values(m, svc, name), before, f"ch{ch} during ch7")


# ── opsctl end to end ──────────────────────────────────────────────────────

AUDIT_PREFIXES = ("[NOC-CHG]", "cloudtrail:", "azure-activity:", "gcp-audit:")


def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _ops_inst():
    ctl = controller()
    return SimpleNamespace(
        chaos_controller=ctl,
        namespace=SCN.namespace,
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
    ctl.trigger(ch, user_email="noc@example.com")
    lines = _cmd_lines(ch)
    for line in lines:
        assert ctl.get_channel_status(ch)["state"] != "STANDBY"
        ops.execute(inst, line)
    assert ctl.get_channel_status(ch)["state"] == "STANDBY"
    with ctl._lock:
        for e in ctl._pending_events:
            e["emit_at"] = 0
        evs, ctl._pending_events = list(ctl._pending_events), []
    audits = [e["body"] for e in evs if e["body"].startswith(AUDIT_PREFIXES)]
    assert len(audits) == len(lines), (ch, [e["body"] for e in evs])
    assert all("{actor}" not in a for a in audits), audits


def test_actor_lands_on_fix_line():
    logs, _ = records(phase(9, "after", since=120))
    assert any(f"actor={OPS_ACTOR}" in b for b in bodies(logs, "auth-service", "[NOC-CHG]"))
