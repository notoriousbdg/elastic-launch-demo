"""One timestamp per scrape for OTLP metric data points.

A real collector stamps every data point of a scrape with the same time.
Elasticsearch's OTLP intake stores points that share resource, scope,
attributes, ``timeUnixNano``, ``startTimeUnixNano``, unit and metric type in
one document, so points stamped a few nanoseconds apart land in one document
per metric and queries that read two metrics from one row come back empty.

``run_tick_loop`` opens a :func:`scrape_clock` around every tick; the metric
builders read :func:`scrape_now_ns`. Each generator runs in its own thread, and
a ``ContextVar`` keeps one thread's scrape time out of another's.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Iterator
from contextvars import ContextVar

_SCRAPE_NS: ContextVar[str | None] = ContextVar("scrape_ns", default=None)


def scrape_now_ns() -> str:
    """The current scrape's timestamp, or the wall clock outside a scrape."""
    return _SCRAPE_NS.get() or str(time.time_ns())


def scrape_start_ns(window_s: int = 60) -> str:
    """Default ``startTimeUnixNano`` for a sum: ``window_s`` before the scrape."""
    return str(int(scrape_now_ns()) - window_s * 1_000_000_000)


@contextlib.contextmanager
def scrape_clock(at_ns: int | None = None) -> Iterator[str]:
    """Pin :func:`scrape_now_ns` to one timestamp for the duration of a scrape.

    ``at_ns`` pins a past scrape, for history a generator backfills at startup.
    A nested clock keeps the outer scrape's time, so a generator that opens its
    own clock inside ``run_tick_loop`` still stamps one time per scrape.
    """
    if _SCRAPE_NS.get() is not None:
        yield _SCRAPE_NS.get()  # type: ignore[misc]
        return
    token = _SCRAPE_NS.set(str(at_ns if at_ns is not None else time.time_ns()))
    try:
        yield _SCRAPE_NS.get()  # type: ignore[misc]
    finally:
        _SCRAPE_NS.reset(token)
