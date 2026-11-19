"""Shared generator tick loop.

Generators supply one ``tick`` callable that emits a cycle of telemetry.
This module owns the stop check and the sleep between cycles.
"""

from __future__ import annotations

import threading
from collections.abc import Callable

from generator_engine.scrape_clock import scrape_clock


def run_tick_loop(
    stop_event: threading.Event,
    tick: Callable[[], None],
    *,
    interval: float | Callable[[], float],
) -> None:
    """Call ``tick`` until ``stop_event`` is set, then wait ``interval`` seconds.

    ``interval`` may be a number of seconds or a zero-argument callable.
    The callable is evaluated after each tick, so the delay can depend on
    state the tick just updated. ``stop_event`` is checked once per cycle,
    before ``tick``, matching the generators' previous loop. Each tick runs
    inside a :func:`scrape_clock`, so its metric points share one timestamp.
    """
    while not stop_event.is_set():
        with scrape_clock():
            tick()
        delay = interval() if callable(interval) else float(interval)
        stop_event.wait(delay)
