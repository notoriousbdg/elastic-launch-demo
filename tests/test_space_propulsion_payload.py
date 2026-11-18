"""NOVA-7 propulsion + payload channels (01, 02, 03, 10, 11): before / during / after a fault.

Each channel's signal metric sits in its nominal band before the fault, deviates during it
(consistent with the channel's fault_params) while the subsystem's status log says ANOMALY,
and converges back after the opsctl fix, with the new configuration on the status log and
one Mission Control command line per remediation command.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from tests._space_harness import (
    OPS_ACTOR, PROFILE, REG, SCN, attr_values, bodies, controller, phase, records, values,
)


def _within(ch: int) -> float:
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _signal(ch, which, svc, metric, **kw):
    logs, metrics = records(phase(ch, which, **kw))
    vals = values(metrics, svc, metric)
    assert vals, f"{svc} emitted no {metric}"
    return logs, metrics, vals


def _status(logs, svc, contains, key="sensor.status"):
    return {r["attrs"].get(key) for r in logs if r["service"] == svc and contains in r["body"]}


# ── ch01: TCS calibration drift ────────────────────────────────────────────

def test_ch01_thermal_cal_offset_phases():
    m = "fuel_system.engine_thermal_cal_offset_k"
    logs, _, v = _signal(1, "before", "fuel-system", m)
    assert all(0.0 <= x <= 1.0 for x in v), v
    assert _status(logs, "fuel-system", "cal_offset=") == {"NOMINAL"}

    logs, _, v = _signal(1, "during", "fuel-system", m)
    assert all(3.0 <= x <= 12.0 for x in v), v
    assert _status(logs, "fuel-system", "cal_offset=") == {"ANOMALY"}
    assert "v2.3.1-canary" in attr_values(logs, "fuel-system", "deployment.config_version")

    logs, _, v = _signal(1, "after", "fuel-system", m, since=_within(1))
    assert all(x < 2.5 for x in v), v
    assert _status(logs, "fuel-system", "cal_offset=") == {"NOMINAL"}
    assert attr_values(logs, "fuel-system", "deployment.config_version") == {"v2.3.0"}
    _assert_cmd_lines(logs, 1)


def test_ch01_converges_rather_than_snaps():
    _, _, early = _signal(1, "after", "fuel-system", "fuel_system.engine_thermal_cal_offset_k", since=10)
    assert min(early) > 2.5, early


# ── ch02: REG-A firmware rc1 -> LOX-1 pressure low ─────────────────────────

def test_ch02_lox_pressure_phases():
    m = "fuel_system.lox_pressure"
    logs, _, v = _signal(2, "before", "fuel-system", m)
    assert all(240.0 <= x <= 290.0 for x in v), v
    assert _status(logs, "fuel-system", "sensor=lox_pressure") == {"NOMINAL"}

    logs, _, v = _signal(2, "during", "fuel-system", m)
    assert all(150.0 <= x <= 195.0 for x in v), v
    assert _status(logs, "fuel-system", "sensor=lox_pressure") == {"ANOMALY"}
    assert "fw-4.2.0-rc1" in attr_values(logs, "fuel-system", "infra.firmware_rev")

    logs, _, v = _signal(2, "after", "fuel-system", m, since=_within(2))
    assert all(240.0 <= x <= 290.0 for x in v), v
    assert _status(logs, "fuel-system", "sensor=lox_pressure") == {"NOMINAL"}
    assert attr_values(logs, "fuel-system", "infra.firmware_rev") == {"fw-4.1.7"}
    _assert_cmd_lines(logs, 2)


def test_ch02_ramps_in_and_converges():
    _, _, early = _signal(2, "during", "fuel-system", "fuel_system.lox_pressure", elapsed=5)
    assert min(early) > 200.0, early
    _, _, mid = _signal(2, "after", "fuel-system", "fuel_system.lox_pressure", since=15)
    assert max(mid) < 240.0, mid


# ── ch03: MOV-OX feedback offset -> LOX flow -6.5% ─────────────────────────

def _deltas(metrics):
    flow = values(metrics, "fuel-system", "fuel_system.oxidizer_flow_rate")
    cmd = values(metrics, "fuel-system", "fuel_system.oxidizer_flow_commanded")
    assert flow and len(flow) == len(cmd)
    return [100.0 * (f - c) / c for f, c in zip(flow, cmd)]


def test_ch03_oxidizer_flow_phases():
    logs, metrics = records(phase(3, "before"))
    assert all(abs(d) < 3.0 for d in _deltas(metrics))
    assert _status(logs, "fuel-system", "sensor=oxidizer_flow") == {"NOMINAL"}

    logs, metrics = records(phase(3, "during"))
    assert all(-7.5 <= d <= -5.5 for d in _deltas(metrics)), _deltas(metrics)
    assert _status(logs, "fuel-system", "sensor=oxidizer_flow") == {"ANOMALY"}

    logs, metrics = records(phase(3, "after", since=_within(3)))
    assert all(abs(d) < 3.0 for d in _deltas(metrics)), _deltas(metrics)
    assert _status(logs, "fuel-system", "sensor=oxidizer_flow") == {"NOMINAL"}
    trims = {r["attrs"]["propulsion.mov_trim_pct"] for r in logs
             if r["service"] == "fuel-system" and "sensor=oxidizer_flow" in r["body"]}
    assert trims == {6.5}, trims
    _assert_cmd_lines(logs, 3)


# ── ch10: HTR-A3 stuck ON -> payload zone A over temperature ───────────────

def test_ch10_bay_temperature_phases():
    m = "payload.bay_temperature"
    logs, _, v = _signal(10, "before", "payload-monitor", m)
    assert all(-5.0 <= x <= 35.0 for x in v), v
    assert _status(logs, "payload-monitor", "sensor=bay_temperature", "payload.status") == {"NOMINAL"}

    logs, _, v = _signal(10, "during", "payload-monitor", m)
    assert all(52.5 <= x <= 54.5 for x in v), v
    assert _status(logs, "payload-monitor", "sensor=bay_temperature", "payload.status") == {"ANOMALY"}
    assert "STUCK_ON" in attr_values(logs, "payload-monitor", "payload.heater_state")

    logs, _, v = _signal(10, "after", "payload-monitor", m, since=_within(10))
    assert all(x < 35.0 for x in v), v
    assert _status(logs, "payload-monitor", "sensor=bay_temperature", "payload.status") == {"NOMINAL"}
    assert attr_values(logs, "payload-monitor", "payload.heater_state") == {"OFF"}
    flows = {r["attrs"]["payload.coolant_flow_lpm"] for r in logs
             if r["service"] == "payload-monitor" and "sensor=bay_temperature" in r["body"]}
    assert all(3.9 <= f <= 4.1 for f in flows), flows
    _assert_cmd_lines(logs, 10)


def test_ch10_cools_over_minutes():
    _, _, one_min = _signal(10, "after", "payload-monitor", "payload.bay_temperature", since=60)
    assert all(35.0 < x < 45.0 for x in one_min), one_min


# ── ch11: ISO-X damper leak -> payload X-axis vibration ────────────────────

def test_ch11_vibration_x_phases():
    m = "payload.vibration_x"
    logs, _, v = _signal(11, "before", "payload-monitor", m)
    assert all(x < 1.0 for x in v), v
    assert _status(logs, "payload-monitor", "sensor=vibration_x", "payload.status") == {"NOMINAL"}

    logs, _, v = _signal(11, "during", "payload-monitor", m)
    assert all(2.0 <= x <= 8.0 for x in v), v
    assert _status(logs, "payload-monitor", "sensor=vibration_x", "payload.status") == {"ANOMALY"}
    psi = _status(logs, "payload-monitor", "sensor=vibration_x", "payload.damper_pressure_psi")
    assert psi and all(40.0 <= p <= 42.0 for p in psi), psi

    logs, _, v = _signal(11, "after", "payload-monitor", m, since=_within(11))
    assert all(x < 1.5 for x in v), v
    assert _status(logs, "payload-monitor", "sensor=vibration_x", "payload.status") == {"NOMINAL"}
    assert attr_values(logs, "payload-monitor", "payload.isolator_mode") == {"ACTIVE"}
    _assert_cmd_lines(logs, 11)


# ── shared: command audit lines and opsctl end to end ──────────────────────

CHANNELS = [1, 2, 3, 10, 11]


def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _assert_cmd_lines(logs, ch):
    mcc = bodies(logs, "mission-control", "[MCC] CMD ACCEPTED")
    for line in _cmd_lines(ch):
        hits = [b for b in mcc if line in b]
        assert len(hits) == 1, (line, mcc)
        assert f"from {OPS_ACTOR}" in hits[0]


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
    ctl.trigger(ch, user_email="flight@example.com")
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
    audits = [e["body"] for e in evs if e["body"].startswith("[MCC] CMD ACCEPTED")]
    assert len(audits) == len(lines), audits
    for line, r in zip(lines, results):
        hit = [a for a in audits if line.removeprefix("opsctl ") in a]
        assert len(hit) == 1, (line, audits)
        assert f"from {OPS_ACTOR}" in hit[0] and f"change_id={r['change_id']}" in hit[0]
        assert "{actor}" not in hit[0]
    # the subsystem's own confirmation line follows
    own = [e for e in evs if e["service"] != "mission-control"]
    assert own, [e["body"] for e in evs]
