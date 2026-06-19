"""Thread-safe shared context store for log-trace correlation.

The trace generator writes (trace_id, span_id) per service after each trace batch.
Service log emitters read the latest context to correlate their logs with active traces.
Always returns the most recent trace context — no TTL expiry.

A parallel per-channel map captures the most recent *error* trace for each
(channel_id, service_name) pair, so fault logs can prefer a real error trace.id
over the generic last-seen-per-service one.
"""

from __future__ import annotations

import threading


class TraceContextStore:
    """Maps (namespace, service_name) -> (trace_id, span_id) plus a per-channel error-trace map.

    Namespace-scoped so that different scenarios sharing a service name (e.g.
    "analytics-pipeline" in ecommerce and gaming) do not overwrite each other's
    trace context. Pass namespace="" for standalone / single-scenario usage.
    """

    def __init__(self):
        self._store: dict[tuple[str, str], tuple[str, str]] = {}
        self._channel_store: dict[tuple[int, str, str], tuple[str, str]] = {}
        self._lock = threading.Lock()

    def set(self, service_name: str, trace_id: str, span_id: str, namespace: str = "") -> None:
        with self._lock:
            self._store[(namespace, service_name)] = (trace_id, span_id)

    def get(self, service_name: str, namespace: str = "") -> tuple[str | None, str | None]:
        with self._lock:
            entry = self._store.get((namespace, service_name))
            if entry is None:
                return None, None
            return entry

    def set_for_channel(
        self, channel_id: int, service_name: str, trace_id: str, span_id: str, namespace: str = ""
    ) -> None:
        """Record the most recent error trace for a (channel, namespace, service) triple."""
        with self._lock:
            self._channel_store[(channel_id, namespace, service_name)] = (trace_id, span_id)

    def get_for_channel(
        self, channel_id: int, service_name: str, namespace: str = ""
    ) -> tuple[str | None, str | None]:
        with self._lock:
            entry = self._channel_store.get((channel_id, namespace, service_name))
            if entry is None:
                return None, None
            return entry


# Module-level singleton — imported by trace_generator and base_service
_trace_context_store = TraceContextStore()
