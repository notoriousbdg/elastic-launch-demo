"""NOVA-7 GN&C, comms and range-safety faults: telemetry before, during and after the fix.

Channels 4, 5, 6 (navigation), 7, 15 (range-safety), 8, 9, 19 (comms-array). For each:
the signal metric sits in its nominal band before the fault, leaves it during the fault
(consistent with the channel's fault_params), the subsystem's periodic status line says
ANOMALY, and after the opsctl fix the metric is back in band, the new configuration is on
the status lines and Mission Control logged one command line per fix step.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from tests._space_harness import (
    OPS_ACTOR,
    PROFILE,
    REG,
    SCN,
    attr_values,
    bodies,
    controller,
    phase,
    records,
    values,
)

# ch: (service, metric, before-band, during-band, after-band, status-line marker,
#      config attr, value after the fix, cause-event marker)
CASES = {
    4: ("navigation", "navigation.position_uncertainty_m",
        (0.0, 5.0), (12.0, 50.0), (0.0, 5.0), "gps_fix",
        "nav.mode", "IMU_PRIMARY", "swing arm SA-2 retracted to position 2"),
    5: ("navigation", "navigation.imu.sync_drift_ms",
        (0.0, 1.0), (5.0, 25.0), (0.0, 3.0), "imu_timing",
        "gnc.pps_source", "GPS_1PPS_B", "GPS_1PPS_A amplitude 1.9V"),
    6: ("navigation", "navigation.star_tracker.alignment_arcsec",
        (0.0, 3.0), (10.0, 45.0), (0.0, 5.0), "star_tracker unit=ST-A",
        "deployment.image_tag", "star-tracker-v3.0.4", "star-tracker-v3.1.0-beta (was"),
    7: ("range-safety", "range_safety.fts_selftest_pass",
        (1.0, 1.0), (0.0, 0.0), (1.0, 1.0), "fts_selftest",
        "deployment.fts_firmware", "fts-controller-v5.1.4", "fts-controller-v5.2.0-rc1 (was"),
    8: ("comms-array", "comms.xband.packet_loss_pct",
        (0.0, 0.5), (5.0, 25.0), (0.0, 2.0), "antenna=X-Band",
        "comms.active_link", "XB-SECONDARY", "XB-PRIMARY LNA bias current"),
    9: ("comms-array", "comms.uhf.pointing_error_deg",
        (0.0, 0.2), (1.0, 5.0), (0.0, 0.3), "antenna=UHF",
        "deployment.servo_fw", "gimbal-ctrl-v1.9.6", "gimbal-ctrl-v2.0.1-rc3 (was"),
    15: ("range-safety", "range_safety.track_gap_ms",
         (0.0, 250.0), (500.0, 5000.0), (0.0, 250.0), "tracking radars",
         "infra.radar_firmware", "rdr-track-v8.0.9", "rdr-track-v8.1.3-beta (was"),
    19: ("comms-array", "comms.sband.snr_db",
         (15.0, 20.0), (3.0, 8.0), (12.0, 99.0), "antenna=S-Band",
         "comms.sband_feed", "BACKUP", "rain cell over ground station"),
}
CHANNELS = sorted(CASES)


def _within_seconds(ch: int) -> float:
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _lines(ch: int) -> list[str]:
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _in_band(vals: list[float], band: tuple[float, float]) -> bool:
    lo, hi = band
    return bool(vals) and all(lo <= v <= hi for v in vals)


@pytest.mark.parametrize("ch", CHANNELS)
def test_before_nominal(ch):
    svc, metric, before, _, _, marker, _, _, cause = CASES[ch]
    logs, metrics = records(phase(ch, "before"))
    vals = values(metrics, svc, metric)
    assert _in_band(vals, before), (metric, vals)
    status = bodies(logs, svc, marker)
    assert status and not any("ANOMALY" in b for b in status), status
    assert not bodies(logs, None, cause)


@pytest.mark.parametrize("ch", CHANNELS)
def test_during_fault(ch):
    svc, metric, before, during, _, marker, _, _, cause = CASES[ch]
    logs, metrics = records(phase(ch, "during", elapsed=600))
    vals = values(metrics, svc, metric)
    assert _in_band(vals, during), (metric, vals)
    assert not any(before[0] <= v <= before[1] for v in vals), (metric, vals)
    status = bodies(logs, svc, marker)
    assert status and all("ANOMALY" in b for b in status), status
    assert bodies(logs, None, cause), f"cause event {cause!r} not logged"
    assert bodies(logs, svc, REG[ch]["error_type"]), "error signature missing"


@pytest.mark.parametrize("ch", CHANNELS)
def test_after_fix(ch):
    svc, metric, _, _, after, marker, attr, new_value, _ = CASES[ch]
    logs, metrics = records(phase(ch, "after", since=_within_seconds(ch)))
    vals = values(metrics, svc, metric)
    assert _in_band(vals, after), (metric, vals)
    status = bodies(logs, svc, marker)
    assert status and not any("ANOMALY" in b for b in status), status
    assert attr_values(logs, svc, attr) == {new_value}
    assert not bodies(logs, svc, REG[ch]["error_type"])
    cmds = bodies(logs, "mission-control", "[MCC] CMD ACCEPTED")
    assert len(cmds) == len(_lines(ch)), cmds
    for line in _lines(ch):
        text = line.removeprefix("opsctl ")
        assert any(text in c and OPS_ACTOR in c for c in cmds), (text, cmds)


@pytest.mark.parametrize("ch", CHANNELS)
def test_converges_rather_than_snapping(ch):
    """Right after the fix the metric is still out of band (physical/rollout recovery)."""
    svc, metric, _, _, after, _, _, _, _ = CASES[ch]
    _, metrics = records(phase(ch, "after", since=1))
    vals = values(metrics, svc, metric)
    assert vals and not _in_band(vals, after), (metric, vals)


def test_gps_multipath_satellites_and_mask():
    _, metrics = records(phase(4, "during"))
    assert set(values(metrics, "navigation", "navigation.gps.satellites")) == {3.0}
    assert max(values(metrics, "navigation", "navigation.gps.confidence")) < 0.65
    logs, metrics = records(phase(4, "after", since=120))
    assert all(6 <= v <= 8 for v in values(metrics, "navigation", "navigation.gps.satellites"))
    assert attr_values(logs, "navigation", "gnc.sv_mask") == {"G04,G07,G15"}


def test_range_tracking_quality_drops():
    _, metrics = records(phase(15, "during"))
    assert max(values(metrics, "range-safety", "range_safety.track_quality")) < 0.65
    assert set(values(metrics, "range-safety", "range_safety.active_radars")) == {2.0}


def _ops_inst(ctl):
    services = {name: {} for name in SCN.services} if getattr(SCN, "services", None) else {"mission-control": {}}
    return SimpleNamespace(
        chaos_controller=ctl,
        ctx=SimpleNamespace(services=services, scenario=SCN),
        resolve_channel=lambda ch, **kw: ctl.resolve(ch, **kw),
    )


def _drain(ctl) -> list[dict]:
    with ctl._lock:
        for e in ctl._pending_events:
            e["emit_at"] = 0
        evs, ctl._pending_events = list(ctl._pending_events), []
    return evs


@pytest.mark.parametrize("ch", CHANNELS)
def test_opsctl_resolves_with_one_audit_line_per_command(ch):
    lines = _lines(ch)
    assert len(ops.expected_commands(REG[ch]["remediation_command"], PROFILE)) == len(lines)
    ctl = controller()
    inst = _ops_inst(ctl)
    ctl.trigger(ch, user_email="flight@example.com")
    change_ids = []
    for i, line in enumerate(lines):
        if i:
            assert ctl.get_channel_status(ch)["state"] != "STANDBY", "resolved before the last step"
        change_ids.append(ops.execute(inst, line)["change_id"])
    assert ctl.get_channel_status(ch)["state"] == "STANDBY"
    audits = [e["body"] for e in _drain(ctl) if e["body"].startswith("[MCC] CMD ACCEPTED")]
    assert len(audits) == len(lines), audits
    for line, cid, body in zip(lines, change_ids, audits):
        assert line.removeprefix("opsctl ") in body, (line, body)
        assert f"from {OPS_ACTOR}" in body and f"change_id={cid}" in body, body
        assert "{actor}" not in body
