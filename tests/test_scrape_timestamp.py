"""One timestamp per scrape for OTLP metric data points.

Elasticsearch's OTLP intake stores metric points in one document only when the
resource, scope, data-point attributes, ``timeUnixNano``, ``startTimeUnixNano``,
unit and metric type are all equal. Points stamped even 1 ns apart land in
separate documents, and an ES|QL query that reads two metrics from one row comes
back empty. These tests pin the scrape clock and check that every OTLP metrics
generator stamps one cycle with one ``timeUnixNano``.
"""
import inspect
import sys
import threading
import time

import pytest

from generator_engine import db_common
from generator_engine.registry import GENERATORS, load_entrypoint
from generator_engine.scrape_clock import _SCRAPE_NS, scrape_clock, scrape_now_ns, scrape_start_ns
from generator_engine.tick import run_tick_loop
from tests.test_generator_contract import (
    _OTLP_SENDS,
    _freeze,
    _OneCycle,
    _RecordingOTLP,
    _run_cycle,
    _scenario_data,
)

_NS = 1_000_000_000
_METRIC_KINDS = ("gauge", "sum", "histogram", "exponentialHistogram", "summary")
METRIC_GENERATORS = sorted(
    gid for gid, spec in GENERATORS.items()
    if spec.client == "otlp" and "metrics" in spec.signals
)


# ── scrape_clock ──────────────────────────────────────────────────────────────

def test_scrape_now_falls_back_to_the_wall_clock_outside_a_scrape():
    first = scrape_now_ns()
    time.sleep(0.002)
    second = scrape_now_ns()
    assert first != second
    assert abs(int(second) - time.time_ns()) < _NS


def test_scrape_now_is_constant_inside_a_scrape():
    with scrape_clock() as pinned:
        first = scrape_now_ns()
        time.sleep(0.002)
        assert scrape_now_ns() == first == pinned
    time.sleep(0.002)
    assert scrape_now_ns() != pinned


def test_nested_scrape_clock_keeps_the_outer_time():
    with scrape_clock() as outer:
        time.sleep(0.002)
        with scrape_clock() as inner:
            assert inner == outer
            assert scrape_now_ns() == outer
        assert scrape_now_ns() == outer


def test_scrape_clock_is_isolated_per_thread():
    entered = threading.Event()
    release = threading.Event()
    seen: dict[str, str | None] = {}

    def _other():
        seen["before"] = None
        with scrape_clock() as mine:
            seen["mine"] = mine
            seen["inside"] = scrape_now_ns()
            entered.set()
            release.wait(5)

    with scrape_clock() as main_time:
        time.sleep(0.002)
        worker = threading.Thread(target=_other)
        worker.start()
        assert entered.wait(5)
        assert scrape_now_ns() == main_time
        release.set()
        worker.join(5)

    assert seen["mine"] != main_time
    assert seen["inside"] == seen["mine"]


def test_thread_outside_a_clock_does_not_see_another_threads_clock():
    seen: list[str] = []
    with scrape_clock() as main_time:
        time.sleep(0.002)
        worker = threading.Thread(target=lambda: seen.append(scrape_now_ns()))
        worker.start()
        worker.join(5)
    assert seen and seen[0] != main_time


def test_scrape_start_is_the_window_before_the_scrape():
    with scrape_clock() as now:
        assert int(now) - int(scrape_start_ns()) == 60 * _NS
        assert int(now) - int(scrape_start_ns(300)) == 300 * _NS
        assert int(now) - int(scrape_start_ns(window_s=10)) == 10 * _NS


# ── run_tick_loop ─────────────────────────────────────────────────────────────

class _Ticks(threading.Event):
    """Stops after ``n`` interval waits."""

    def __init__(self, n: int):
        super().__init__()
        self._left = n

    def wait(self, timeout=None):
        self._left -= 1
        if self._left <= 0:
            self.set()
        return self.is_set()


def test_each_tick_gets_one_scrape_time():
    ticks: list[list[str]] = []

    def _tick():
        calls = [scrape_now_ns()]
        time.sleep(0.002)
        calls.append(scrape_now_ns())
        calls.append(db_common.build_gauge("g", "1", 1.0)["gauge"]["dataPoints"][0]["timeUnixNano"])
        ticks.append(calls)

    run_tick_loop(_Ticks(2), _tick, interval=0)
    assert len(ticks) == 2
    for calls in ticks:
        assert len(set(calls)) == 1, calls
    assert ticks[0][0] != ticks[1][0]


# ── db_common builders ────────────────────────────────────────────────────────

def test_builders_share_the_scrape_time():
    with scrape_clock() as now:
        gauge = db_common.build_gauge("a", "1", 1.0)
        time.sleep(0.002)
        mono = db_common.build_cumulative_sum("b", "1", 2)
        updown = db_common.build_non_monotonic_sum("c", "1", 3)
    points = [
        gauge["gauge"]["dataPoints"][0],
        mono["sum"]["dataPoints"][0],
        updown["sum"]["dataPoints"][0],
    ]
    assert {p["timeUnixNano"] for p in points} == {now}


def test_default_sum_start_is_scrape_start():
    with scrape_clock():
        start = scrape_start_ns()
        mono = db_common.build_cumulative_sum("b", "1", 2)
        updown = db_common.build_non_monotonic_sum("c", "1", 3)
        explicit = db_common.build_cumulative_sum("d", "1", 4, start_time_ns="123")
    assert mono["sum"]["dataPoints"][0]["startTimeUnixNano"] == start
    assert updown["sum"]["dataPoints"][0]["startTimeUnixNano"] == start
    assert explicit["sum"]["dataPoints"][0]["startTimeUnixNano"] == "123"


# ── one cycle of every OTLP metrics generator ─────────────────────────────────

_BUILDER: list = []


def _resource_metrics(name: str, args: tuple, kwargs: dict) -> list[dict]:
    """Normalise a recorded send into the OTLP ``resourceMetrics`` it would POST.

    ``send_metrics*`` calls are replayed through a real ``OTLPClient`` whose
    ``_send`` only captures, so the default scope and the data-stream patch match
    what the intake receives.
    """
    if name == "_send":
        payload = args[1] if len(args) > 1 else kwargs.get("payload") or {}
        return list(payload.get("resourceMetrics") or [])
    if name not in ("send_metrics", "send_metrics_scoped"):
        return []
    if not _BUILDER:
        from app.telemetry import OTLPClient

        _BUILDER.append(OTLPClient(endpoint="http://127.0.0.1:9", api_key=""))
    builder = _BUILDER[0]
    captured: list[dict] = []
    builder._send = lambda _url, payload, _label: captured.append(payload)
    getattr(builder, name)(*args, **kwargs)
    return [rm for payload in captured for rm in payload.get("resourceMetrics") or []]


def metric_points(payloads: list) -> list[dict]:
    """Every metric data point in a recorded cycle, with its grouping context."""
    out: list[dict] = []
    for name, args, kwargs in payloads:
        for rm in _resource_metrics(name, args, kwargs):
            resource = rm.get("resource") or {}
            for sm in rm.get("scopeMetrics") or []:
                scope = sm.get("scope") or {}
                for metric in sm.get("metrics") or []:
                    for kind in _METRIC_KINDS:
                        body = metric.get(kind)
                        if not isinstance(body, dict):
                            continue
                        for dp in body.get("dataPoints") or []:
                            out.append({
                                "resource": resource,
                                "scope": scope,
                                "name": metric.get("name"),
                                "unit": metric.get("unit", ""),
                                "kind": kind,
                                "monotonic": bool(body.get("isMonotonic")),
                                "dp": dp,
                            })
    return out


def _ticking_clock(monkeypatch) -> None:
    """Advance the wall clock 1 ms on every read.

    The contract harness freezes ``time.time``, which would let a generator that
    stamps points from the wall clock pass by accident. A clock that moves on
    every read exposes it, while ``scrape_clock`` reads it once per scrape.
    """
    lock = threading.Lock()
    state = {"ns": 1_700_000_000 * _NS}

    def _time_ns() -> int:
        with lock:
            state["ns"] += 1_000_000
            return state["ns"]

    monkeypatch.setattr(time, "time_ns", _time_ns)
    monkeypatch.setattr(time, "time", lambda: _time_ns() / _NS)


class _Cycles(_OneCycle):
    """Lets a generator finish ``n`` loops; ``index`` counts finished loops."""

    def __init__(self, n: int):
        super().__init__()
        self._n = n
        self.index = 0

    def wait(self, timeout=None):
        self.index += 1
        if self.index >= self._n:
            self.set()
        return self.is_set()


class _CycleRecordingOTLP(_RecordingOTLP):
    """Tags every recorded send with its loop and the scrape clock it ran under."""

    def __init__(self, stop: _Cycles):
        super().__init__()
        self.stop = stop
        self.cycles: list[tuple[int, str | None]] = []

    def _tag(self) -> tuple[int, str | None]:
        return self.stop.index, _SCRAPE_NS.get()

    def __getattr__(self, name):
        target = super().__getattr__(name)
        if name not in _OTLP_SENDS:
            return target

        def _tagged(*args, **kwargs):
            self.cycles.append(self._tag())
            return target(*args, **kwargs)
        return _tagged

    def _send(self, url, payload, label):
        self.cycles.append(self._tag())
        super()._send(url, payload, label)


def run_cycles(gid: str, scenario, monkeypatch, cycles: int = 2) -> list[list]:
    """Run ``cycles`` chaos-off loops of ``gid``; return the OTLP payloads per scrape.

    Two loops by default: mysql_otel and postgresql_otel send metrics every other
    loop. A scrape is one loop, split further when the generator opens more than one
    ``scrape_clock`` per loop (vercel_otel seeds history in its own clock before its
    first loop). The one-time rule applies per scrape.
    """
    spec = GENERATORS[gid]
    for key in ("ELASTIC_URL", "ELASTICSEARCH_URL", "ELASTIC_API_KEY", "ELASTICSEARCH_API_KEY"):
        monkeypatch.delenv(key, raising=False)
    fn = load_entrypoint(spec)
    _freeze(inspect.getmodule(fn), monkeypatch)
    _ticking_clock(monkeypatch)
    data, _ = _scenario_data(scenario, spec, chaos=False)
    stop = _Cycles(cycles)
    client = _CycleRecordingOTLP(stop)
    monkeypatch.setattr(sys.modules[_run_cycle.__module__], "_OneCycle", lambda: stop)
    _run_cycle(fn, client, data, None)
    scrapes: dict[tuple[int, str | None], list] = {}
    for (index, clock), payload in zip(client.cycles, client.payloads):
        scrapes.setdefault((min(index, cycles - 1), clock), []).append(payload)
    return list(scrapes.values()) or [[]]


def run_one_cycle(gid: str, scenario, monkeypatch) -> list:
    """The OTLP payloads of the first scrape of ``gid`` that sent metric points."""
    per_cycle = run_cycles(gid, scenario, monkeypatch)
    for payloads in per_cycle:
        if metric_points(payloads):
            return payloads
    return per_cycle[0]


@pytest.fixture(scope="module")
def scenario():
    from scenario_engine import get_scenario

    return get_scenario("space")


# Points that keep their own time on purpose. vercel_otel's Web Vitals are
# per-page-load samples, not scrape metrics (see test_vercel_page_loads_* below).
_OWN_TIME = {
    "vercel_otel": {"LCP", "INP", "CLS", "TTFB"},
}


@pytest.mark.parametrize("gid", METRIC_GENERATORS)
def test_one_cycle_stamps_every_metric_point_with_one_time(gid, scenario, monkeypatch):
    own = _OWN_TIME.get(gid, set())
    per_cycle = [
        [p for p in metric_points(payloads) if p["name"] not in own]
        for payloads in run_cycles(gid, scenario, monkeypatch)
    ]
    if not any(per_cycle):
        # traces declares metrics for the APM rollup it bulk-writes to ES, not OTLP.
        pytest.skip(f"{gid} sends no OTLP metric points")
    stamps = []
    for scrape, points in enumerate(per_cycle):
        if not points:
            continue
        times: dict[str, set[str]] = {}
        for p in points:
            times.setdefault(str(p["dp"].get("timeUnixNano")), set()).add(p["name"])
        assert len(times) == 1, (
            f"{gid} scrape {scrape}: {len(points)} metric points carry {len(times)} distinct "
            f"timeUnixNano values; sample: "
            + "; ".join(f"{t}: {sorted(n)[:3]}" for t, n in list(times.items())[:4])
        )
        stamps.extend(times)
    assert len(stamps) == len(set(stamps)), f"{gid} reused one scrape time across scrapes"


@pytest.mark.parametrize("gid", METRIC_GENERATORS)
def test_one_cycle_monotonic_sums_share_few_start_times(gid, scenario, monkeypatch):
    """Soft check: counters built from CounterState share a start time per entity.

    More than one start per cycle is normal (one CounterState per entity, plus the
    builder default for per-scrape sums), so this only catches a start time drawn
    per point.
    """
    points = metric_points(run_one_cycle(gid, scenario, monkeypatch))
    starts = {
        str(p["dp"].get("startTimeUnixNano"))
        for p in points if p["kind"] == "sum" and p["monotonic"]
    }
    assert len(starts) <= 2, f"{gid}: {len(starts)} distinct startTimeUnixNano among monotonic sums"


def test_vercel_page_loads_each_keep_one_time(scenario, monkeypatch):
    """A page load's four vitals share one time (one document per unit); no two
    loads in a scrape share a time, so identical attributes never collide."""
    points = []
    for payloads in run_cycles("vercel_otel", scenario, monkeypatch):
        points += [p for p in metric_points(payloads) if p["name"] in _OWN_TIME["vercel_otel"]]
    assert points
    loads: dict[str, list[str]] = {}
    for p in points:
        loads.setdefault(str(p["dp"]["timeUnixNano"]), []).append(p["name"])
    assert all(sorted(names) == ["CLS", "INP", "LCP", "TTFB"] for names in loads.values()), loads


def test_scrape_clock_can_pin_a_past_scrape():
    with scrape_clock(at_ns=1_700_000_000_000_000_000) as pinned:
        assert pinned == scrape_now_ns() == "1700000000000000000"
        assert scrape_start_ns() == str(1_700_000_000_000_000_000 - 60 * _NS)
    assert scrape_now_ns() != "1700000000000000000"
