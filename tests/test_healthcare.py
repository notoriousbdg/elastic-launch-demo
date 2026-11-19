"""Healthcare channels 01-20: before / during / after a fault.

Each channel's signal metric sits in its baseline band before the fault, moves into the band
the channel's fault_params describe while it is active (and only that signal moves), and
converges back after the opsctl fix. The owning system's status line carries the faulty
configuration during the fault and the restored one after it, and the change log has one
line per remediation command.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from tests._healthcare_harness import (
    OPS_ACTOR, PROFILE, REG, SCN, attr_values, bodies, controller, phase, records, values,
)

# ch: (service, metric, before band, during band, after band, attribute, during value, after value)
SIGNALS = {
    1: ("ehr-system", "ehr.hl7_ack_ae_pct", (0.0, 0.5), (35.0, 45.0), (0.0, 0.5),
        "hl7.channel_revision", "r42", "r41"),
    2: ("patient-monitor", "patient_monitor.alerts_per_minute", (3, 13), (60, 120), (3, 13),
        "vitals.alarm_profile", "adult-icu-v7", "medsurg-default"),
    3: ("lab-integration", "lab.astm_results_per_min", (20.0, 40.0), (0.0, 1.5), (20.0, 40.0),
        "deployment.lis_interface_version", "lis2a2-bridge-v2.8.1-beta", "lis2a2-bridge-v2.8.0"),
    4: ("data-warehouse", "warehouse.audit_out_of_order_writes_per_min", (0, 0), (12, 24), (0, 0),
        "deployment.audit_chain_version", "sha256-chain-v2.1.0-rc3", "sha256-chain-v2.0.6"),
    5: ("pharmacy-system", "pharmacy.interaction_queue_depth", (2, 15), (240, 360), (2, 15),
        "deployment.cpoe_engine_config", "fdb-cache-aggressive-v2", "fdb-cache-standard-v1"),
    6: ("pharmacy-system", "pharmacy.erx_reject_pct", (0.2, 1.0), (48.0, 62.0), (0.0, 1.0),
        "infra.ncpdp_gateway_version", "surescripts-adapter-v6.0.3-rc1", "surescripts-adapter-v6.0.2"),
    7: ("ehr-system", "ehr.empi_auto_link_pct", (92.0, 97.0), (42.0, 58.0), (90.0, 97.0),
        "deployment.empi_algorithm_config", "jaro-winkler-tuned-v3.2", "jaro-winkler-v3.1"),
    8: ("scheduling-api", "scheduling.bed_board_adt_lag_seconds", (1.0, 5.0), (240.0, 360.0), (0.0, 10.0),
        "infra.bed_board_sync_driver", "adt-bridge-v1.9.4-unstable", "adt-bridge-v1.9.3"),
    9: ("scheduling-api", "scheduling.double_bookings_per_min", (0, 0), (2, 8), (0, 0),
        "deployment.scheduler_lock_mode", "optimistic-lock-v2.1-exp", "slot-row-lock-v2.0"),
    10: ("billing-processor", "billing.eligibility_p95_ms", (700, 1400), (5000, 5050), (700, 1500),
         "x12.clearinghouse", "availity", "trizetto"),
    11: ("billing-processor", "billing.claim_999_reject_pct", (1.0, 3.0), (30.0, 46.0), (1.0, 3.0),
         "deployment.claims_engine_build", "x12-837-processor-v5.1.2-beta", "x12-837-processor-v5.1.1"),
    12: ("scheduling-api", "scheduling.or_block_cache_age_seconds", (5, 60), (880, 3620), (0, 60),
         "infra.or_scheduler_config", "block-mgmt-v3.2.0-beta", "block-mgmt-v3.1.4"),
    13: ("clinical-alerts", "clinical_alerts.rule_eval_ms", (50, 500), (5500, 8500), (50, 500),
         "deployment.cds_rule_engine", "arden-mlm-v3.0.1-experimental", "arden-mlm-v2.4.0"),
    14: ("clinical-alerts", "clinical_alerts.nurse_call_undelivered", (0, 0), (28, 28), (0, 0),
         "ncs.bridge_socket_state", "CLOSE_WAIT", "ESTABLISHED"),
    15: ("lab-integration", "lab.bb_inventory_discrepancy_units", (0, 0), (7, 7), (0, 0),
         "bb.isbt_scanner_status", "OFFLINE", "ONLINE"),
    16: ("imaging-service", "imaging.pacs_storage_used_pct", (76.0, 80.0), (90.0, 95.0), (76.0, 85.0),
         "pacs.archive_job_state", "SUSPENDED", "SCHEDULED"),
    17: ("ehr-system", "ehr.adt_feed_lag_seconds", (0.5, 3.0), (300.0, 420.0), (0.0, 10.0),
         "adt.queue_threads", "1", "8"),
    18: ("data-warehouse", "warehouse.etl_rows_per_second", (5000, 25000), (0, 150), (5000, 25000),
         "infra.etl_db_pool_config", "hikari-pool-experimental-64conn", "hikari-pool-standard-64conn"),
    19: ("imaging-service", "imaging.cstore_failure_pct", (0.0, 0.4), (64.0, 76.0), (0.0, 1.0),
         "infra.pacs_gateway_build", "dcm4chee-5.31.1-patch3", "dcm4chee-5.31.1-patch2"),
    20: ("patient-monitor", "patient_monitor.telehealth_packet_loss_pct", (0.1, 0.8), (8.0, 16.0), (0.0, 1.0),
         "infra.turn_server_build", "coturn-4.6.2-high-concurrency", "coturn-4.6.2-default"),
}
CHANNELS = sorted(SIGNALS)


def _within(ch: int) -> float:
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _in(vals, band):
    lo, hi = band
    return bool(vals) and all(lo <= v <= hi for v in vals)


def test_every_channel_has_a_signal_row():
    assert CHANNELS == sorted(REG)


@pytest.mark.parametrize("ch", CHANNELS)
def test_before(ch):
    svc, metric, before, _, _, attr, bad, _ = SIGNALS[ch]
    logs, metrics = records(phase(ch, "before"))
    v = values(metrics, svc, metric)
    assert _in(v, before), v
    assert bad not in attr_values(logs, svc, attr) or ch == 10


@pytest.mark.parametrize("ch", CHANNELS)
def test_during_only_this_signal_moves(ch):
    svc, metric, _, during, _, attr, bad, _ = SIGNALS[ch]
    logs, metrics = records(phase(ch, "during"))
    v = values(metrics, svc, metric)
    assert _in(v, during), v
    assert bad in attr_values(logs, svc, attr)
    for other, (osvc, ometric, obefore, *_rest) in SIGNALS.items():
        if other != ch:
            ov = values(metrics, osvc, ometric)
            assert _in(ov, obefore), (other, ometric, ov)


@pytest.mark.parametrize("ch", CHANNELS)
def test_after_fix(ch):
    svc, metric, _, _, after, attr, bad, good = SIGNALS[ch]
    logs, metrics = records(phase(ch, "after", since=_within(ch)))
    v = values(metrics, svc, metric)
    assert _in(v, after), v
    seen = attr_values(logs, svc, attr)
    assert good in seen and bad not in seen, seen
    error_type = REG[ch]["error_type"]
    assert not bodies(logs, contains=error_type), error_type
    _assert_cmd_lines(logs, ch)


# ── ramps in and converges back rather than snapping ───────────────────────

# ch: (elapsed for an early "during" sample or None, since for an early "after" sample or None)
RAMPS = {
    1: (5, 10), 2: (5, 10), 3: (5, None), 4: (5, None), 5: (5, 15), 6: (5, 10), 7: (5, 10),
    8: (10, 20), 9: (None, 10), 10: (5, 10), 11: (5, 15), 13: (5, 15), 14: (5, 10), 15: (5, 10),
    16: (5, 10), 17: (10, 20), 18: (5, 15), 19: (3, 5), 20: (5, 10),
}


@pytest.mark.parametrize("ch", sorted(RAMPS))
def test_ramps_in_and_converges(ch):
    svc, metric, before, during, *_ = SIGNALS[ch]
    rising = during[0] > before[1]
    early_during, early_after = RAMPS[ch]
    if early_during is not None:
        _, m = records(phase(ch, "during", elapsed=early_during))
        v = values(m, svc, metric)
        assert v and (max(v) < during[0] if rising else min(v) > during[1]), v
    if early_after is not None:
        _, m = records(phase(ch, "after", since=early_after))
        v = values(m, svc, metric)
        assert v and (min(v) > before[1] if rising else max(v) < before[0]), v


def test_ch03_held_results_burst_after_the_rollback():
    _, m = records(phase(3, "after", since=30))
    v = values(m, "lab-integration", "lab.astm_results_per_min")
    assert v and min(v) > 40.0, v


def test_ch16_volume_drains_over_minutes():
    _, m = records(phase(16, "after", since=60))
    v = values(m, "imaging-service", "imaging.pacs_storage_used_pct")
    assert v and all(85.0 < x < 93.8 for x in v), v


# ── the healthy comparison stays healthy ───────────────────────────────────

def test_comparisons_stay_healthy_during_their_fault():
    logs, _ = records(phase(3, "during"))
    assert all("status=OK" in b for b in bodies(logs, "lab-integration", "instrument=SYSMEX-XN"))
    logs, _ = records(phase(2, "during"))
    assert all("status=OK" in b for b in bodies(logs, "patient-monitor", "central_station unit=MedSurg-4S"))
    logs, _ = records(phase(14, "during"))
    br2 = bodies(logs, "clinical-alerts", "bridge=EHR-NCS-BRIDGE-02 tower=West")
    assert br2 and all("socket=ESTABLISHED" in b for b in br2)
    logs, _ = records(phase(17, "during"))
    others = [b for b in bodies(logs, "ehr-system", "[EHR] adt_feed feed=") if "ADT-FEED-02" not in b]
    assert others and all("status=CURRENT" in b for b in others)
    logs, _ = records(phase(18, "during"))
    progress = bodies(logs, "data-warehouse", "etl_progress")
    assert progress and not any("ETL-CLINICAL-01" in b for b in progress)


def test_cause_precedes_symptoms():
    for ch in CHANNELS:
        events = REG[ch].get("infrastructure_events") or []
        assert any(float(e.get("offset_seconds", 0)) < 0 for e in events), ch


# ── shared: change-log lines and opsctl end to end ─────────────────────────

def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _assert_cmd_lines(logs, ch):
    chg = bodies(logs, None, "[CHG] CHANGE IMPLEMENTED")
    for line in _cmd_lines(ch):
        hits = [b for b in chg if line in b]
        assert len(hits) == 1, (line, chg)
        assert f"by {OPS_ACTOR}" in hits[0]


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
def test_opsctl_resolves_channel_with_one_change_line_per_command(ch):
    inst = _ops_inst()
    ctl = inst.chaos_controller
    ctl.trigger(ch, user_email="oncall@example.com")
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
    audits = [e for e in evs if e["body"].startswith("[CHG] CHANGE IMPLEMENTED")]
    assert len(audits) == len(lines), [e["body"] for e in audits]
    owner = REG[ch]["error_services"][0]
    for line, r in zip(lines, results):
        hit = [a for a in audits if line in a["body"]]
        assert len(hit) == 1, (line, [a["body"] for a in audits])
        body = hit[0]["body"]
        assert f"by {OPS_ACTOR}" in body and f"change_id={r['change_id']}" in body
        assert "{actor}" not in body
        assert hit[0]["service"] == owner
