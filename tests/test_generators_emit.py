"""Shared generator entrypoint contract.

Every generator ``run`` must accept the kwargs ``ServiceManager`` passes.
Technology-specific emit checks stay in ``generators/<id>/test_generator.py``.
"""

from __future__ import annotations

import inspect
import threading

import pytest

from generator_engine.registry import GENERATORS, load_entrypoint
from generator_engine.tick import run_tick_loop


class _Client:
    """Accepts the setup calls some generators make before the stop check."""

    endpoint = "http://127.0.0.1:4318"
    elastic_url = ""
    api_key = ""
    configured = True

    def __getattr__(self, name: str):
        def _call(*_args, **_kwargs):
            return None

        return _call


class _Chaos:
    def is_active(self, channel: int) -> bool:
        return False

    def get_active_channels(self) -> list[int]:
        return []

    def get_infra_spikes(self) -> dict[str, float]:
        return {}

    def get_channel_metadata(self, channel: int) -> dict[str, str]:
        return {"callback_url": "", "user_email": ""}


@pytest.mark.parametrize("spec", list(GENERATORS.values()), ids=lambda spec: spec.id)
def test_run_accepts_manager_kwargs(spec):
    fn = load_entrypoint(spec)
    params = inspect.signature(fn).parameters
    assert list(params)[:2] == ["client", "stop_event"]
    for name in ("scenario_data", "chaos_controller"):
        assert params[name].kind is inspect.Parameter.KEYWORD_ONLY

    stop = threading.Event()
    stop.set()
    fn(_Client(), stop, scenario_data=None, chaos_controller=_Chaos())


def test_run_tick_loop_waits_between_ticks_and_stops():
    calls = {"n": 0}
    stop = threading.Event()

    def tick() -> None:
        calls["n"] += 1
        if calls["n"] == 2:
            stop.set()

    run_tick_loop(stop, tick, interval=0)
    assert calls["n"] == 2
    assert stop.is_set()
