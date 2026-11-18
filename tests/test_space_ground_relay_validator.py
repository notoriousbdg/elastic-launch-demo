"""NOVA-7 ground, relay and validation channels (12, 13, 14, 16, 17, 18, 20).

Each channel's signal metric is nominal before the fault, deviates during it, and
converges back after the opsctl fix, with the subsystem's status log, the new
configuration and the Mission Control command audit to match.
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

# ch: (service, metric, before band, during band, after band, status-log marker,
#      config attr, value after the fix, value before the fix)
CASES = {
    12: ("ground-systems", "ground.hydraulics.pressure_psi", (2950, 3050), (2400, 2750), (2800, 2960),
         "hydraulics_status", "ground.hydraulics.active_system", "HYD-B", "HYD-A"),
    13: ("telemetry-relay", "relay.corrupted_frames_pct", (0.0, 0.1), (2.0, 20.0), (0.0, 0.1),
         "link_status", "relay.transceiver_serial", "FTRJ1319P1BTL-B2204", "FTRJ1319P1BTL-A7731"),
    14: ("ground-systems", "ground.power.bus_voltage", (119.0, 121.0), (109.0, 110.5), (119.0, 121.0),
         "power_bus_status", "ground.power.transformer_tap", "+2", "0"),
    16: ("telemetry-relay", "relay.latency_ms", (40.0, 80.0), (500.0, 3000.0), (60.0, 200.0),
         "link_status", "relay.route_path", "backup", "primary"),
    17: ("sensor-validator", "sensor_validator.queue_depth", (0, 20), (500, 5000), (0, 20),
         "pipeline_status", "validation.heap_limit", "12g", "unset"),
    18: ("sensor-validator", "sensor_validator.epoch_skew_s", (0.0, 1.0), (3700.0, 86400.0), (0.0, 1.0),
         "epoch_check", "validation.calibration_table", "reference-1738100800", "reference-1738100800"),
    20: ("ground-systems", "ground.weather.data_age_s", (0.0, 15.0), (15.01, 180.0), (0.0, 15.0),
         "weather_check", "deployment.wx_station_fw", "davis-v3.1.8", "davis-v3.1.8"),
}
CHANNELS = sorted(CASES)


def _within(ch: int) -> int:
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _command_lines(ch: int) -> list[str]:
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _status_logs(logs, svc, marker):
    return [r for r in logs if r["service"] == svc and marker in r["body"] and "sensor.status" in r["attrs"]]


def _in(vals, band):
    lo, hi = band
    return vals and all(lo <= v <= hi for v in vals)


@pytest.mark.parametrize("ch", CHANNELS)
def test_before_nominal(ch):
    svc, metric, before, _d, _a, marker, attr, _new, old = CASES[ch]
    logs, metrics = records(phase(ch, "before"))
    vals = values(metrics, svc, metric)
    assert _in(vals, before), (ch, vals)
    status = _status_logs(logs, svc, marker)
    assert status and all(r["attrs"]["sensor.status"] == "NOMINAL" for r in status), [r["body"] for r in status]
    assert all("status=NOMINAL" in r["body"] for r in status)
    assert attr_values(logs, svc, attr) == {old}


@pytest.mark.parametrize("ch", CHANNELS)
def test_during_out_of_band(ch):
    svc, metric, _b, during, _a, marker, _attr, _new, _old = CASES[ch]
    logs, metrics = records(phase(ch, "during", elapsed=600))
    vals = values(metrics, svc, metric)
    assert _in(vals, during), (ch, vals)
    status = _status_logs(logs, svc, marker)
    assert status and all(r["attrs"]["sensor.status"] == "ANOMALY" for r in status), [r["body"] for r in status]
    assert all("status=ANOMALY" in r["body"] for r in status)
    # the channel's error signature is in the logs while the fault is active
    assert bodies(logs, contains=REG[ch]["error_type"])


@pytest.mark.parametrize("ch", CHANNELS)
def test_during_ramps_in(ch):
    """Shortly after the trigger the signal is still near nominal (a ramp, not a step)."""
    svc, metric, before, during, *_ = CASES[ch]
    _logs, metrics = records(phase(ch, "during", elapsed=1), cycles=1)
    vals = values(metrics, svc, metric)
    nominal, fault = sum(before) / 2, sum(during) / 2
    # not yet at the fault value, and closer to nominal than to the fault
    assert vals and not _in(vals, during), (ch, vals)
    assert all(abs(v - nominal) < abs(v - fault) for v in vals), (ch, vals)


@pytest.mark.parametrize("ch", CHANNELS)
def test_after_recovered(ch):
    svc, metric, _b, _d, after, marker, attr, new, _old = CASES[ch]
    logs, metrics = records(phase(ch, "after", since=_within(ch)))
    vals = values(metrics, svc, metric)
    assert _in(vals, after), (ch, vals)
    status = _status_logs(logs, svc, marker)
    assert status and all(r["attrs"]["sensor.status"] == "NOMINAL" for r in status), [r["body"] for r in status]
    assert new in attr_values(logs, svc, attr)
    mcc = bodies(logs, "mission-control", "[MCC] CMD ACCEPTED")
    for line in _command_lines(ch):
        cmd = line[len("opsctl "):]
        hits = [b for b in mcc if f"opsctl {cmd} from {OPS_ACTOR}" in b]
        assert len(hits) == 1, (ch, line, mcc)


@pytest.mark.parametrize("ch", CHANNELS)
def test_after_converges_not_snaps(ch):
    """Right after the fix the signal is still on its way back."""
    svc, metric, _b, _d, after, *_ = CASES[ch]
    _logs, metrics = records(phase(ch, "after", since=0), cycles=1)
    vals = values(metrics, svc, metric)
    assert vals and not _in(vals, after), (ch, vals)


def test_validation_rate_stalls_and_recovers():
    rate = "sensor_validator.validations_per_sec"
    _l, m = records(phase(17, "before"))
    assert _in(values(m, "sensor-validator", rate), (45.0, 65.0))
    _l, m = records(phase(17, "during"))
    assert _in(values(m, "sensor-validator", rate), (1.0, 10.0))
    _l, m = records(phase(17, "after", since=_within(17)))
    assert _in(values(m, "sensor-validator", rate), (45.0, 65.0))


def test_relay_hop_latency_matches_route_log():
    logs, metrics = records(phase(16, "during"))
    route = [r for r in logs if r["service"] == "telemetry-relay"
             and r["attrs"].get("relay.source_cloud") == "aws" and r["attrs"].get("relay.dest_cloud") == "gcp"]
    assert all(r["attrs"]["relay.latency_ms"] >= 500 for r in route)


def _ops_inst():
    ctl = controller()
    return SimpleNamespace(
        chaos_controller=ctl,
        ctx=SimpleNamespace(services={s: {} for s in ("mission-control", "ground-systems",
                                                          "telemetry-relay", "sensor-validator")},
                            scenario=SCN),
        resolve_channel=lambda ch, **kw: ctl.resolve(ch, **kw),
    )


def _drain(ctl):
    with ctl._lock:
        for e in ctl._pending_events:
            e["emit_at"] = 0
        evs, ctl._pending_events = list(ctl._pending_events), []
    return evs


@pytest.mark.parametrize("ch", CHANNELS)
def test_opsctl_resolves_channel(ch):
    lines = _command_lines(ch)
    assert len(ops.expected_commands(REG[ch]["remediation_command"], PROFILE)) == len(lines)
    inst = _ops_inst()
    ctl = inst.chaos_controller
    ctl.trigger(ch)
    for i, line in enumerate(lines):
        assert ctl.get_channel_status(ch)["state"] != "STANDBY", (ch, line)
        ops.execute(inst, line)
    assert ctl.get_channel_status(ch)["state"] == "STANDBY"
    events = [e["body"] for e in _drain(ctl)]
    accepted = [b for b in events if b.startswith("[MCC] CMD ACCEPTED")]
    assert len(accepted) == len(lines), accepted
    for line in lines:
        assert sum(1 for b in accepted if f"{line} from {OPS_ACTOR}" in b) == 1, (line, accepted)
    assert all("{actor}" not in b for b in events)
