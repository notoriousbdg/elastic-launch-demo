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
