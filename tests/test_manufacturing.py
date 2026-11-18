"""Manufacturing at schema 1.3: telemetry before, during and after each fix.

For every channel the verification metric sits in its normal band before the fault,
leaves it during the fault, and is back after the opsctl fix; the status line that
carries the configuration attribute shows the bad value during the fault and the good
value after it; and the owning service logs one plant change-log line per fix step.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from tests._manufacturing_harness import (
    PROFILE, REG, SCN, attr_values, bodies, controller, phase, records, values,
)

# ch: (service, metric, before band, during band, after band,
#      status-line marker, config attribute, value during, value after)
CASES = {
    1: ("scada-gateway", "scada.plc_comm_load_pct", (28, 38), (85, 99.5), (28, 38),
        "plc_status plc=PLC-LINE03-A", "scada.opcua_profile_rev", "15", "14"),
    2: ("mes-controller", "mes.deadlocks_per_min", (0, 0), (4, 10), (0, 0),
        "routing_status", "mes.routing_ruleset", "routing-v3.4.1", "routing-v3.4.0"),
    3: ("mes-controller", "mes.batch_traceability_complete_pct", (100, 100), (70, 86), (100, 100),
        "batch_genealogy", "mes.batch_close_grace_s", "60", "900"),
    4: ("scada-gateway", "scada.tag_update_rate_per_s", (1050, 1350), (28000, 41000), (1050, 1350),
        "tag_group group=conveyor-drives", "scada.tag_template_rev", "9", "8"),
    5: ("ot-historian", "historian.disk_await_ms", (2, 6), (57, 141), (2, 10),
        "storage volume=historian-data", "historian.volume_throughput_mibs", "125", "500"),
    6: ("scada-gateway", "scada.modbus_exceptions_per_min", (0, 0), (20, 60), (0, 0),
        "modbus_poll plc=PLC-PACK-01", "scada.regmap_version", "v3.0", "v3.1"),
    7: ("scada-gateway", "scada.connected_plcs", (5, 5), (1, 1), (5, 5),
        "certificate subject=", "scada.cert_serial", "4F2A91C3E07B", "7B10E5D2A946"),
    8: ("oee-analytics", "oee.availability_pct", (82, 96), (45, 74), (82, 96),
        "oee_window", "oee.shift_calendar_rev", "23", "22"),
    9: ("quality-inspector", "quality.cpk", (1.45, 1.75), (0.78, 0.95), (1.33, 1.75),
        "spc_sample", "quality.cmm_cal_rev", "r42", "r41"),
    10: ("quality-inspector", "quality.vision_confidence_avg", (0.93, 0.98), (0.55, 0.78), (0.93, 0.98),
         "vision_status station=VIS-02", "vision.model_version",
         "defect-yolov8s-v3.4.0", "defect-yolov8s-v3.3.2"),
    11: ("maintenance-scheduler", "maintenance.confirmations_rejected_per_min", (0, 0), (6, 14), (0, 0),
         "confirmation_sync", "cmms.sync_version", "4.0.0", "3.9.2"),
    12: ("maintenance-scheduler", "maintenance.vibration_rms_max_mm_s", (1.4, 2.4), (4.6, 7.0), (1.4, 2.4),
         "vibration_profile gateway=VIB-GW-02", "maintenance.sensor_profile_rev", "r7", "r6"),
    13: ("oee-analytics", "oee.container_memory_limit_mib", (1024, 1024), (512, 512), (1024, 1024),
         "pod_resources", "k8s.container.memory_limit", "512Mi", "1Gi"),
    14: ("erp-bridge", "erp.idoc_backlog", (0, 15), (80, 250), (0, 15),
         "service_status service=MfgIdocBridgeSvc", "erp.bridge_build", "2.8.0", "2.7.4"),
    15: ("inventory-sync", "inventory.alu_6061_on_hand_kg", (4150, 4250), (1880, 1930), (4150, 4250),
         "stock_status material=RM-ALU-6061", "inventory.base_uom", "LB", "KG"),
    16: ("shopfloor-portal", "portal.ready_replicas", (3, 3), (0, 0), (3, 3),
         "deployment_status", "container.image.tag", "1.18.4", "1.18.3"),
    17: ("inventory-sync", "inventory.rfc_errors_per_min", (0, 0), (3, 8), (0, 0),
         "rfc_pool pool=inventory-sync-sap", "rfc.pool_state", "stale", "healthy"),
    18: ("mes-controller", "mes.workorder_workers", (6, 6), (2, 2), (6, 6),
         "workorder_pool", "mes.workorder_workers", "2", "6"),
    19: ("scada-gateway", "scada.session_pool_used", (18, 26), (54, 64), (18, 26),
         "session_pool pool=client-sessions", "scada.session_idle_timeout_s", "0", "300"),
    20: ("oee-analytics", "oee.cache_hit_rate_pct", (94, 98), (35, 60), (90, 98),
         "cache_status cache=oee-results", "oee.redis_maxmemory", "2gb", "4gb"),
}
CHANNELS = sorted(CASES)
# The status attribute only changes at the fix: the regmap was v3.0 before the firmware
# upgrade (ch 6) and the expiring certificate had the same serial while still valid (ch 7).
SAME_BEFORE = {6, 7}
# Signals that depend on another channel's fault: the overloaded PLC (1) drops off the
# gateway (7's connected-PLC count) and, like rejected OPC UA sessions (7), cuts the tag
# rate (4's signal); a tag storm (4) loads the historian disk (5's signal). Every other
# channel's signal stays in its normal band.
DOWNSTREAM = {1: {4, 7}, 7: {4}, 4: {5}}


def _in_band(vals: list[float], band: tuple[float, float]) -> bool:
    lo, hi = band
    return bool(vals) and all(lo <= v <= hi for v in vals)


def _status_attr(logs: list[dict], svc: str, marker: str, key: str) -> set:
    return {str(r["attrs"].get(key)) for r in logs if r["service"] == svc and marker in r["body"]}


def _within_seconds(ch: int) -> float:
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _inst(ctl):
    return SimpleNamespace(
        deployment_id="dep-1",
        chaos_controller=ctl,
        ctx=SimpleNamespace(services={n: {} for n in SCN._service_specs}, scenario=SCN),
        resolve_channel=lambda ch, **kw: ctl.resolve(ch, **kw),
    )


def _drain(ctl) -> list[dict]:
    with ctl._lock:
        for e in ctl._pending_events:
            e["emit_at"] = 0
        out, ctl._pending_events = list(ctl._pending_events), []
    return out


def test_every_channel_is_covered_and_at_schema_13():
    assert SCN.schema_version == "1.3"
    assert set(CASES) == set(REG)


@pytest.mark.parametrize("ch", CHANNELS)
def test_before_normal(ch):
    svc, metric, before, _, _, marker, key, bad, good = CASES[ch]
    logs, metrics = records(phase(ch, "before"))
    assert _in_band(values(metrics, svc, metric), before), (metric, values(metrics, svc, metric))
    assert _status_attr(logs, svc, marker, key) == ({bad} if ch in SAME_BEFORE else {good})
    assert not bodies(logs, contains=REG[ch]["error_type"])


@pytest.mark.parametrize("ch", CHANNELS)
def test_during_fault(ch):
    svc, metric, _, during, _, marker, key, bad, _ = CASES[ch]
    logs, metrics = records(phase(ch, "during"))
    assert _in_band(values(metrics, svc, metric), during), (metric, values(metrics, svc, metric))
    assert _status_attr(logs, svc, marker, key) == {bad}
    assert bodies(logs, svc, REG[ch]["error_type"])


@pytest.mark.parametrize("ch", CHANNELS)
def test_during_only_this_signal_moves(ch):
    logs, metrics = records(phase(ch, "during"))
    for other, (osvc, ometric, obefore, _, _, omarker, okey, obad, ogood) in CASES.items():
        if other == ch:
            continue
        normal = obad if other in SAME_BEFORE else ogood
        assert _status_attr(logs, osvc, omarker, okey) == {normal}, (other, okey)
        if other not in DOWNSTREAM.get(ch, ()):
            ov = values(metrics, osvc, ometric)
            assert _in_band(ov, obefore), (other, ometric, ov)


@pytest.mark.parametrize("ch", sorted(DOWNSTREAM))
def test_downstream_signal_moves_with_the_fault_and_recovers_with_the_fix(ch):
    _, during_metrics = records(phase(ch, "during"))
    _, after_metrics = records(phase(ch, "after", since=_within_seconds(ch)))
    for other in DOWNSTREAM[ch]:
        osvc, ometric, obefore = CASES[other][:3]
        assert not _in_band(values(during_metrics, osvc, ometric), obefore), (other, ometric)
        ov = values(after_metrics, osvc, ometric)
        assert _in_band(ov, obefore), (other, ometric, ov)


@pytest.mark.parametrize("ch", CHANNELS)
def test_after_fix(ch):
    svc, metric, _, _, after, marker, key, _, good = CASES[ch]
    logs, metrics = records(phase(ch, "after", since=_within_seconds(ch)))
    assert _in_band(values(metrics, svc, metric), after), (metric, values(metrics, svc, metric))
    assert _status_attr(logs, svc, marker, key) == {good}
    assert not bodies(logs, contains=REG[ch]["error_type"])


def test_every_fix_leaves_one_change_log_line_per_step_on_the_owning_service():
    for ch, data in REG.items():
        steps = ops.expected_commands(data["remediation_command"], PROFILE)
        lines = [e for e in data["remediation_effects"]["log_events"] if "{actor}" in str(e["body"])]
        assert len(lines) == len(steps), (ch, [e["body"] for e in lines])
        for step, e in zip(steps, lines):
            assert e["service"] in data["affected_services"], (ch, e["service"])
            body = str(e["body"])
            assert body.startswith("[CHG] CHANGE APPLIED opsctl "), (ch, body)
            logged = body[len("[CHG] CHANGE APPLIED "):body.index(" by {actor}")]
            assert ops.matches(step, ops.parse_command(logged, PROFILE)), (ch, body)


@pytest.mark.parametrize("ch", CHANNELS)
def test_opsctl_runbook_commands_fix_the_channel(ch):
    ctl = controller()
    ctl.trigger(ch, user_email="plant@example.com")
    steps = ops.expected_commands(REG[ch]["remediation_command"], PROFILE)
    for step in steps:
        ops.execute(_inst(ctl), "opsctl " + step.display())
    assert ctl.get_channel_status(ch)["state"] == "STANDBY"
    changes = [e for e in _drain(ctl) if e["body"].startswith("[CHG] CHANGE APPLIED")]
    assert len(changes) == len(steps), [e["body"] for e in changes]
    assert all("opsctl/claude-code" in e["body"] for e in changes)


def test_a_partial_fix_leaves_the_channel_active():
    ctl = controller()
    ctl.trigger(15, user_email="plant@example.com")
    ops.execute(_inst(ctl), "opsctl rollback material-master/RM-ALU-6061 --to-revision previous")
    assert ctl.get_channel_status(15)["state"] == "ACTIVE"


def test_restarting_the_gateway_does_not_fix_the_cert():
    ctl = controller()
    ctl.trigger(7, user_email="plant@example.com")
    ops.execute(_inst(ctl), "opsctl restart deployment/scada-gateway")
    assert ctl.get_channel_status(7)["state"] == "ACTIVE"


def test_dependents_follow_the_gateway_after_their_lag():
    # PLC-LINE03-A drops at once; MES sees stale LINE-03 tags only after its 20 s lag.
    logs, metrics = records(phase(1, "during", elapsed=10))
    assert max(values(metrics, "mes-controller", "mes.upstream_tag_age_s")) < 2
    assert min(values(metrics, "scada-gateway", "scada.stale_tags")) > 0
    logs, metrics = records(phase(1, "during", elapsed=120))
    assert min(values(metrics, "mes-controller", "mes.upstream_tag_age_s")) > 60


def test_cert_expires_after_the_onset_delay():
    _, metrics = records(phase(7, "during", elapsed=20))
    assert set(values(metrics, "scada-gateway", "scada.cert_valid")) == {1.0}
    assert set(values(metrics, "scada-gateway", "scada.connected_plcs")) == {5.0}
    _, metrics = records(phase(7, "during", elapsed=120))
    assert set(values(metrics, "scada-gateway", "scada.cert_valid")) == {0.0}


def test_protocol_drift_fix_keeps_the_new_firmware():
    logs, _ = records(phase(6, "after", since=60))
    assert attr_values(logs, "scada-gateway", "scada.plc_firmware") == {"V3.1.0"}


def test_cause_is_logged_before_the_symptoms():
    for ch, data in REG.items():
        offsets = [e.get("offset_seconds", 0) for e in data.get("infrastructure_events") or []]
        assert offsets and min(offsets) < 0, ch
