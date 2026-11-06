"""Profiling metrics generator: panel fields, healthy baseline, impact keys."""

from __future__ import annotations

import copy
import random

from generator_engine.identity import ScenarioIdentity
from generators.profilingmetrics_otel import SPEC
from generators.profilingmetrics_otel.generator import (
    build_metrics,
    build_processes,
    process_samples,
)

FRAME_TYPES = {"jvm", "cpython", "go", "v8js", "dotnet", "rust", "native"}


def _scenario():
    from scenario_engine import get_scenario

    s = get_scenario("space")
    return {"services": copy.deepcopy(s.services), "hosts": copy.deepcopy(s.hosts), "namespace": s.namespace}


def _totals(procs, rng, ticks=5, **impact):
    """Summed counts per (metric, sorted attrs) over all processes and ticks."""
    out: dict[tuple, int] = {}
    for _ in range(ticks):
        for proc in procs:
            for name, points in process_samples(proc, rng, interval_s=10, hz=20, **impact).items():
                for attrs, count in points:
                    key = (name, tuple(sorted(attrs.items())))
                    out[key] = out.get(key, 0) + count
    return out


def _sum(totals, metric, **match):
    return sum(c for (name, attrs), c in totals.items()
               if name == metric and all(dict(attrs).get(k) == v for k, v in match.items()))


def _procs(seed=1):
    sd = _scenario()
    return build_processes(sd, ScenarioIdentity.from_scenario(sd), {}, random.Random(seed))


def test_processes_follow_scenario_services_and_hosts():
    sd = _scenario()
    procs = _procs()
    services = {p.service for p in procs}
    assert set(sd["services"]) <= services
    hosts = {h["host.name"] for h in sd["hosts"]}
    for p in procs:
        attrs = {a["key"]: next(iter(a["value"].values())) for a in p.resource["attributes"]}
        assert attrs["host.name"] in hosts
        assert attrs["data_stream.dataset"] == "profilingmetricsconnector"
        assert attrs["process.executable.name"]
    assert sum(p.hotspot for p in procs) == 1
    assert {p.runtime for p in procs} >= {"jvm", "cpython", "go", "v8js", "native"}


def test_every_panel_metric_and_dimension_is_emitted():
    totals = _totals(_procs(), random.Random(2))
    names = {name for name, _a in totals}
    assert {"samples.user.count", "samples.kernel.count", "samples.native.count"} <= names
    assert {f"samples.{ft}.count" for ft in ("jvm", "cpython", "go", "v8js")} <= names
    for area in ("synchronization", "disk", "network", "memory", "ipc"):
        assert _sum(totals, "samples.kernel.count", kernel_area=area) > 0, area
    for proto in ("tcp", "udp", "other"):
        for io in ("read", "write"):
            assert _sum(totals, "samples.kernel.count", kernel_area="network", kernel_proto=proto, kernel_io=io) > 0
    uncategorised = sum(c for (n, a), c in totals.items() if n == "samples.kernel.count" and "kernel_area" not in dict(a))
    assert uncategorised > 0
    assert any("shlib_name" not in dict(a) for n, a in totals if n == "samples.native.count")
    assert any(dict(a).get("syscall_name") == "futex" for n, a in totals)


def test_user_samples_equal_the_frame_type_breakdown():
    rng = random.Random(3)
    for proc in _procs():
        samples = process_samples(proc, rng, interval_s=10, hz=20)
        frames = sum(c for name, pts in samples.items()
                     if name.split(".")[1] in FRAME_TYPES for _a, c in pts)
        assert samples["samples.user.count"][0][1] == frames


def test_data_points_are_integer_gauges():
    rng = random.Random(4)
    proc = _procs()[0]
    metrics = build_metrics(process_samples(proc, rng, interval_s=10, hz=20), 1_700_000_000 * 10**9, 10)
    for m in metrics:
        assert "gauge" in m
        for dp in m["gauge"]["dataPoints"]:
            assert "asInt" in dp and int(dp["asInt"]) >= 0


def test_healthy_baseline_kernel_share():
    totals = _totals(_procs(), random.Random(5))
    user, kernel = _sum(totals, "samples.user.count"), _sum(totals, "samples.kernel.count")
    assert 0.08 < kernel / (user + kernel) < 0.25


def test_cpu_hotspot_multiplier_inflates_the_hot_process():
    procs = _procs()
    hot = [p for p in procs if p.hotspot]
    base = _totals(hot, random.Random(6))
    spiked = _totals(hot, random.Random(6), cpu_hotspot_multiplier=5.0)
    assert _sum(spiked, "samples.user.count") > 4 * _sum(base, "samples.user.count")
    assert _sum(spiked, f"samples.{hot[0].runtime}.count") > 4 * _sum(base, f"samples.{hot[0].runtime}.count")


def test_kernel_time_spike_shifts_toward_synchronization_and_disk():
    procs = _procs()
    base = _totals(procs, random.Random(7))
    spiked = _totals(procs, random.Random(7), kernel_time_spike=8.0)
    assert _sum(spiked, "samples.kernel.count") > 2.5 * _sum(base, "samples.kernel.count")
    share = lambda t: _sum(t, "samples.kernel.count", kernel_area="synchronization") / _sum(t, "samples.kernel.count")  # noqa: E731
    assert share(spiked) > 1.5 * share(base)


def test_native_spike_grows_tls_libraries():
    procs = _procs()
    base = _totals(procs, random.Random(8))
    spiked = _totals(procs, random.Random(8), native_spike=6.0)
    assert _sum(spiked, "samples.native.count", shlib_name="libssl.so.3") > 4 * _sum(base, "samples.native.count", shlib_name="libssl.so.3")
    assert _sum(spiked, "samples.native.count", shlib_name="libc.so.6") < 1.3 * _sum(base, "samples.native.count", shlib_name="libc.so.6")


def test_fallback_without_scenario_uses_identity():
    procs = build_processes(None, ScenarioIdentity("solo"), {}, random.Random(1))
    names = {next(iter(a["value"].values())) for p in procs for a in p.resource["attributes"] if a["key"] == "host.name"}
    assert names and all(n.startswith("solo-") for n in names)


def test_spec_shape():
    assert SPEC.wants_chaos and SPEC.chaos_mode == "explicit"
    assert set(SPEC.impact_keys) == {"cpu_hotspot_multiplier", "kernel_time_spike", "native_spike"}
    assert SPEC.datasets == ("profilingmetricsconnector.otel",)
