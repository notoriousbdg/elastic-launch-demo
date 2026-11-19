"""Android OTel generator: dashboard field coverage, healthy baseline, impact keys."""

from __future__ import annotations

import random
import re

from generator_engine import mobile_common as mc
from generator_engine.identity import ScenarioIdentity
from generators.otel_android_dashboards import SPEC
from generators.otel_android_dashboards.generator import AndroidState, simulate_tick

NOW = 1_700_000_000 * 10**9
# The dashboard's _stacktrace.group_id runtime field only groups traces of this shape.
JAVA_TRACE = re.compile(r"^[^\s].+((\n\sat.+)+)[\S\s]*")


def _run(ticks: int = 40, seed: int = 7, **impact):
    rng = random.Random(seed)
    state = AndroidState(ScenarioIdentity("droid"), {}, rng)
    logs, spans, res = [], [], {}
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


def _crash_session_share(logs, spans):
    sessions = {a["session.id"] for _ra, _r, a in logs + spans if "session.id" in a}
    crashed = {a["session.id"] for _ra, _r, a in _events(logs, "device.crash")}
    return len(crashed) / len(sessions)


def test_resources_carry_the_dashboard_fields():
    logs, spans = _run(ticks=5)
    for ra, _r, a in logs + spans:
        assert ra["os.name"] == "Android"
        for key in ("os.version", "device.manufacturer", "app.installation.id", "app.build_id",
                    "service.name", "service.version"):
            assert ra.get(key), key
        assert a.get("session.id")
    assert {ra["service.name"] for ra, _r, _a in spans} == {"droid-shopper-android", "droid-courier-android"}
    assert len({ra["device.manufacturer"] for ra, _r, _a in spans}) >= 4
    assert len({ra["os.version"] for ra, _r, _a in spans}) >= 3
    assert len({ra["service.version"] for ra, _r, _a in spans}) == 3


def test_healthy_baseline_is_low_crash_low_anr():
    logs, spans = _run()
    assert 0 < _crash_session_share(logs, spans) < 0.05
    assert len(_events(logs, "device.anr")) < 15
    failed = [s for _ra, s, _a in spans if s["status"]["code"] == mc.STATUS_ERROR]
    assert 0 < len(failed) / len(spans) < 0.03
    starts = [(int(s["endTimeUnixNano"]) - int(s["startTimeUnixNano"])) / 1e6
              for _ra, s, _a in spans if s["name"] == "AppStart"]
    assert starts and max(starts) < 1500


def test_crash_and_anr_events_have_groupable_java_traces():
    logs, _spans = _run(crash_rate=0.2, anr_rate=0.1)
    crashes = _events(logs, "device.crash")
    anrs = _events(logs, "device.anr")
    assert crashes and anrs
    for _ra, _r, a in crashes + anrs:
        assert a["exception.type"] and a["exception.message"]
        assert JAVA_TRACE.fullmatch(a["exception.stacktrace"]), a["exception.stacktrace"]


def test_crash_rate_drives_crash_sessions_on_the_newest_release():
    logs, spans = _run(crash_rate=0.25)
    assert _crash_session_share(logs, spans) > 0.3
    crashes = _events(logs, "device.crash")
    newest = [ra for ra, _r, _a in crashes if ra["service.version"] == "5.14.0"]
    assert len(newest) / len(crashes) > 0.8
    types = [a["exception.type"] for _ra, _r, a in crashes]
    assert types.count("java.lang.IndexOutOfBoundsException") / len(types) > 0.6


def test_anr_rate_drives_anr_events_and_long_spans():
    base_logs, base_spans = _run()
    logs, spans = _run(anr_rate=0.2)
    assert len(_events(logs, "device.anr")) > 5 * max(1, len(_events(base_logs, "device.anr")))
    long_resumed = [s for _ra, s, _a in spans if s["name"] == "Resumed"
                    and int(s["endTimeUnixNano"]) - int(s["startTimeUnixNano"]) > 5e9]
    assert len(long_resumed) > 20


def test_slow_start_stretches_app_start_spans():
    def avg_start(spans):
        d = [(int(s["endTimeUnixNano"]) - int(s["startTimeUnixNano"])) / 1e6
             for _ra, s, _a in spans if s["name"] == "AppStart"]
        return sum(d) / len(d)

    _l, healthy = _run()
    _l, slow = _run(slow_start=5.0)
    assert avg_start(slow) > 4 * avg_start(healthy)


def test_spec_shape():
    assert SPEC.wants_chaos and SPEC.chaos_mode == "explicit"
    assert set(SPEC.impact_keys) == {"crash_rate", "anr_rate", "slow_start"}
    assert SPEC.integrations == ("otel_android_dashboards",)
    assert SPEC.default_enabled is False and SPEC.core is False
