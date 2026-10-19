"""ProgressStore — thread-safe deploy/teardown progress keyed by deployment id.

Worker threads (op_queue runners and their progress callbacks) write entries
while request handlers read them. Every read and write goes through a lock and
works on copies, so a handler never serializes a dict that a worker is
mutating.

An entry has the shape the selector UI polls::

    {"finished": bool, "error": str, "scenario_id": str, "scenario_name": str,
     "steps": [{"name", "status", "detail", "items_total", "items_done"}, ...]}
"""

from __future__ import annotations

import copy
import threading
from typing import Any

IDLE: dict[str, Any] = {"finished": True, "error": "", "steps": []}


def queued_step(position: int, total: int) -> dict:
    """Synthetic 'Queued' progress step that mirrors DeployStep.to_dict() shape."""
    return {
        "name": "Queued",
        "status": "running",
        "detail": f"Position {position} of {total} in queue",
        "items_total": 0,
        "items_done": 0,
    }


class ProgressStore:
    """Thread-safe map of deployment_id -> progress entry."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, dict[str, Any]] = {}

    def set(self, deployment_id: str, entry: dict[str, Any]) -> None:
        """Replace the entry for *deployment_id* (a copy is stored)."""
        snapshot = copy.deepcopy(entry)
        with self._lock:
            # Re-insert so latest() returns the most recently written entry.
            self._entries.pop(deployment_id, None)
            self._entries[deployment_id] = snapshot

    def get(self, deployment_id: str) -> dict[str, Any] | None:
        """Return a copy of the entry, or None."""
        with self._lock:
            entry = self._entries.get(deployment_id)
            return copy.deepcopy(entry) if entry is not None else None

    def latest(self) -> dict[str, Any] | None:
        """Return a copy of the most recently written entry, or None."""
        with self._lock:
            if not self._entries:
                return None
            return copy.deepcopy(next(reversed(self._entries.values())))

    def start_queued(self, deployment_id: str, scenario_id: str, scenario_name: str,
                     only_if_idle: bool = False) -> None:
        """Record a new operation as queued.

        With ``only_if_idle``, an unfinished entry is left as it is.
        """
        with self._lock:
            existing = self._entries.get(deployment_id)
            if only_if_idle and existing and not existing.get("finished"):
                return
            self._entries.pop(deployment_id, None)
            self._entries[deployment_id] = {
                "finished": False, "error": "",
                "scenario_id": scenario_id, "scenario_name": scenario_name,
                "steps": [queued_step(1, 1)],
            }

    def set_queue_position(self, deployment_id: str, position: int, total: int) -> None:
        """Update the synthetic 'Queued' step of an unfinished entry."""
        with self._lock:
            entry = self._entries.get(deployment_id)
            if not entry or entry.get("finished"):
                return
            steps = entry.get("steps") or []
            if steps and steps[0].get("name") == "Queued":
                steps[0] = queued_step(position, total)
            else:
                entry["steps"] = [queued_step(position, total)]

    def mark_finished(self, deployment_id: str, error: str = "") -> None:
        """Mark an existing entry finished (no-op if there is none)."""
        with self._lock:
            entry = self._entries.get(deployment_id)
            if entry:
                entry["finished"] = True
                entry["error"] = error

    def in_progress(self) -> dict[str, dict[str, Any]]:
        """Return copies of all unfinished entries, in insertion order."""
        with self._lock:
            return {
                d: copy.deepcopy(e)
                for d, e in self._entries.items()
                if not e.get("finished", True)
            }
