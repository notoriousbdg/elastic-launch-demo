"""Ops-dashboard tile bands: affected services go red, cascade services go yellow.

The per-service tiles show ``error spans / all spans`` with green < 12 %,
yellow 12-20 %, red > 20 % (PALETTE_ERROR_RATE).  Simulates the trace generator for
the shipped ``space`` scenario's ch-01 and checks both bands.
"""
import collections
import os
import random

os.environ.setdefault("ACTIVE_SCENARIO", "space")

from generators.traces import generator as g  # noqa: E402
from generators.traces import SPEC  # noqa: E402
from scenario_engine import get_scenario  # noqa: E402


class _Client:
    def build_span(self, **kw):
        return {"status": kw.get("status_code")}

    def __getattr__(self, name):
        return lambda *a, **k: None


def _ratios(channel: int, traces: int = 3000) -> tuple[dict, set, set]:
    sc = get_scenario(os.environ["ACTIVE_SCENARIO"])
    reg = sc.channel_registry
    ch = reg[channel]
    affected = set(ch["affected_services"])
    cascade = set(ch["cascade_services"]) - affected
    rng = random.Random(7)
    res = {s: {} for s in g.SERVICES}
    tot, err = collections.Counter(), collections.Counter()
    for _ in range(traces):
        spans, _, _ = g._generate_trace(
            _Client(), res, rng, affected,
            error_rate=SPEC.impact_keys["error_rate"]["default"],
            scenario=sc, active_channels=[channel], channel_registry=reg,
            chaos_cascade=cascade or None,
        )
        for svc, ss in spans.items():
            for s in ss:
                tot[svc] += 1
                err[svc] += s["status"] == g.STATUS_ERROR
    return {s: err[s] / tot[s] for s in tot}, affected, cascade


def test_affected_red_cascade_yellow():
    ratios, affected, cascade = _ratios(1)
    for svc in affected:
        assert ratios[svc] > 0.20, (svc, ratios[svc])
    for svc in cascade:
        assert 0.12 <= ratios[svc] <= 0.25, (svc, ratios[svc])


class _AttrClient(_Client):
    def build_span(self, **kw):
        return {"status": kw.get("status_code"), "attrs": dict(kw.get("attributes") or {})}


def test_correlated_attribute_stays_on_the_affected_services():
    """A channel's correlation_attr (the changed firmware, config or image) is on the
    spans of its affected services only, never on cascade or unrelated services, so
    the change doesn't look like it reached every subsystem."""
    sc = get_scenario(os.environ["ACTIVE_SCENARIO"])
    reg = sc.channel_registry
    ch = reg[1]
    ca = sc._channels[1]["correlation_attr"]
    key, value = ca["key"], ca["value"]
    affected = set(ch["affected_services"])
    rng = random.Random(11)
    seen: collections.Counter = collections.Counter()
    total: collections.Counter = collections.Counter()
    for _ in range(1500):
        spans, _, _ = g._generate_trace(
            _AttrClient(), {s: {} for s in g.SERVICES}, rng, set(ch["affected_services"]),
            error_rate=SPEC.impact_keys["error_rate"]["default"],
            scenario=sc, active_channels=[1], channel_registry=reg,
            chaos_cascade=set(ch["cascade_services"]) or None,
        )
        for svc, ss in spans.items():
            for s in ss:
                total[svc] += 1
                seen[svc] += s["attrs"].get(key) == value
    assert any(seen[s] for s in affected), seen
    others = {s for s in total if s not in affected}
    assert others and not any(seen[s] for s in others), {s: seen[s] for s in others}
    cascade = set(ch["cascade_services"]) & set(total)
    assert cascade and not any(seen[s] for s in cascade), {s: seen[s] for s in cascade}
