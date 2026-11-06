"""iOS OTel generator: dashboard field coverage, crash grouping, impact keys."""

from __future__ import annotations

import random

from generator_engine import mobile_common as mc
from generator_engine.identity import ScenarioIdentity
from generators.otel_ios_dashboards import SPEC
from generators.otel_ios_dashboards.generator import IOSState, simulate_tick

NOW = 1_700_000_000 * 10**9


def _group_id(exc_type: str, stacktrace: str):
    """Python port of the dashboard's _stacktrace.group_id Painless script."""
    lines = stacktrace.split("\n")
    system: dict[str, bool] = {}
    in_images = False
    for line in lines:
        if line == "Binary Images:":
            in_images = True
        elif in_images:
            dash, uuid, path = line.find(" - "), line.find("  <"), line.find("> ")
            if dash < 0 or uuid < 0 or path < 0:
                continue
            image = line[dash + 3:uuid].strip()
            image = image[image.find(" ") + 1:].strip().lstrip("+")
            image = image[:image.rfind(" ")]
            image_path = line[path + 2:]
            system[image] = "/System/Library/" in image_path or "/usr/lib/" in image_path
    section = "Thread "
    for line in lines:
        if line == "Last Exception Backtrace:":
            section = line
    in_section = False
    for line in lines:
        if not in_section:
            in_section = (line.startswith("Thread ") and line.endswith(" Crashed:")) if section == "Thread " else line == section
            continue
        if not line:
            return None
        address, offset = line.find(" 0x"), line.rfind(" + ")
        if address < 0 or offset < 0:
            continue
        start = 0
        while start < address and line[start].isdigit():
            start += 1
        image = line[start:address].strip()
        if image in system and not system[image]:
            return f"{exc_type}|{image}+{line[offset + 3:]}"
    return None


def _run(ticks: int = 40, seed: int = 11, **impact):
    rng = random.Random(seed)
    state = IOSState(ScenarioIdentity("ios"), {}, rng)
    logs, spans = [], []
    for i in range(ticks):
        lb, sb = simulate_tick(state, rng, NOW + i * 10 * 10**9, **impact)
        for batch, sink in ((lb, logs), (sb, spans)):
            for resource, _scope, recs in batch.grouped():
                ra = mc.attr_map(resource)
                for r in recs:
                    sink.append((ra, r, mc.attr_map(r)))
    return logs, spans


def _events(logs, name):
    return [(ra, r, a) for ra, r, a in logs if r.get("eventName") == name]


def test_resources_carry_the_dashboard_fields():
    logs, spans = _run(ticks=5)
    for ra, _r, a in logs + spans:
        assert ra["os.name"] == "iOS"
        for key in ("os.version", "app.installation.id", "service.name", "service.version"):
            assert ra.get(key), key
        assert a.get("session.id")
    assert {ra["service.name"] for ra, _r, _a in spans} == {"ios-shopper-ios", "ios-courier-ios"}
    assert len({ra["os.version"] for ra, _r, _a in spans}) >= 4
    names = {s["name"] for _ra, s, _a in spans}
    assert "AppLaunch" in names and any(n.endswith(" - View appearing") for n in names)


def test_healthy_baseline_is_low_crash():
    logs, spans = _run()
    sessions = {a["session.id"] for _ra, _r, a in logs + spans}
    crashed = {a["session.id"] for _ra, _r, a in _events(logs, "app.crash")}
    assert 0 < len(crashed) / len(sessions) < 0.05
    assert len(_events(logs, "app.hang")) < 15


def test_crash_reports_group_by_first_app_frame():
    logs, _spans = _run(crash_rate=0.2)
    crashes = _events(logs, "app.crash")
    assert crashes
    groups = {_group_id(a["exception.type"], a["exception.stacktrace"]) for _ra, _r, a in crashes}
    assert None not in groups
    assert any(g.startswith("SIGTRAP|Shopper+") for g in groups)
    # one group per (signature, app binary), stable across ASLR load addresses
    assert len(groups) <= 8


def test_crash_rate_drives_crash_sessions_on_the_newest_release():
    logs, spans = _run(crash_rate=0.25)
    crashes = _events(logs, "app.crash")
    sessions = {a["session.id"] for _ra, _r, a in logs + spans}
    assert len({a["session.id"] for _ra, _r, a in crashes}) / len(sessions) > 0.3
    newest = [ra for ra, _r, _a in crashes if ra["service.version"] == "5.14.0"]
    assert len(newest) / len(crashes) > 0.8
    assert sum(a["exception.message"] == "Fatal error: Index out of range" for _ra, _r, a in crashes) / len(crashes) > 0.6


def test_hang_rate_drives_hang_events_and_slow_views():
    base_logs, _ = _run()
    logs, spans = _run(hang_rate=0.2)
    hangs = _events(logs, "app.hang")
    assert len(hangs) > 5 * max(1, len(_events(base_logs, "app.hang")))
    assert all(a["exception.stacktrace"] and a["exception.type"] == "AppHang" for _ra, _r, a in hangs)
    slow = [s for _ra, s, _a in spans if s["name"].endswith("View appearing")
            and int(s["endTimeUnixNano"]) - int(s["startTimeUnixNano"]) > 2.5e9]
    assert len(slow) > 20


def test_slow_start_stretches_app_launch_spans():
    def avg_launch(spans):
        d = [(int(s["endTimeUnixNano"]) - int(s["startTimeUnixNano"])) / 1e6
             for _ra, s, _a in spans if s["name"] == "AppLaunch"]
        return sum(d) / len(d)

    _l, healthy = _run()
    _l, slow = _run(slow_start=5.0)
    assert avg_launch(healthy) < 1300
    assert avg_launch(slow) > 4 * avg_launch(healthy)


def test_spec_shape():
    assert SPEC.wants_chaos and SPEC.chaos_mode == "explicit"
    assert set(SPEC.impact_keys) == {"crash_rate", "hang_rate", "slow_start"}
    assert SPEC.integrations == ("otel_ios_dashboards",)
