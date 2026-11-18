"""generator_hold_seconds: a package stream keeps its fault impact for a while after the fix."""

from __future__ import annotations

from generator_engine.db_common import resolve_active_chaos

REGISTRY = {
    1: {"name": "a", "generator_impact": {"gen": {"k": 1.0}}, "generator_hold_seconds": {"gen": 60}},
    2: {"name": "b", "generator_impact": {"gen": {"k": 2.0}}},
    3: {"name": "c", "generator_impact": {"other": {"k": 3.0}}, "generator_hold_seconds": {"gen": 60}},
}


class _Ctl:
    def __init__(self, active=(), since=None):
        self._active = list(active)
        self._since = since or {}

    def get_active_channels(self):
        return self._active

    def get_since_fix(self):
        return self._since


def _resolve(ctl):
    return resolve_active_chaos(ctl, REGISTRY, "gen", chaos_mode="explicit")


def test_impact_is_held_until_the_hold_elapses():
    held = _resolve(_Ctl(since={1: 30.0}))
    assert held and held["channel_ids"] == [1]
    assert _resolve(_Ctl(since={1: 60.0})) is None
    assert _resolve(_Ctl(since={1: 600.0})) is None


def test_channel_without_a_hold_stops_at_the_fix():
    assert _resolve(_Ctl(since={2: 1.0})) is None


def test_hold_needs_the_generators_own_impact():
    # channel 3 names "gen" in the hold but only sets impact for another generator
    assert _resolve(_Ctl(since={3: 5.0})) is None


def test_active_channels_come_first_and_are_not_duplicated():
    res = _resolve(_Ctl(active=[2], since={1: 10.0, 2: 0.0}))
    assert res["channel_ids"] == [2, 1] and res["channel_id"] == 2


def test_controller_without_since_fix_is_unchanged():
    class _Old:
        def get_active_channels(self):
            return [2]
    assert _resolve(_Old())["channel_ids"] == [2]
