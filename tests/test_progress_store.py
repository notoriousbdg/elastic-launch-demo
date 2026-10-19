"""Tests for app/progress.py and the progress endpoints that read it.

Run with:
    python -m pytest tests/test_progress_store.py -v
"""

from __future__ import annotations

import json
import sys
import threading
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from app.progress import IDLE, ProgressStore, queued_step  # noqa: E402


def test_get_and_set_work_on_copies():
    store = ProgressStore()
    entry = {"finished": False, "error": "", "steps": [{"name": "a"}]}
    store.set("d1", entry)
    entry["steps"].append({"name": "mutated"})

    got = store.get("d1")
    assert got["steps"] == [{"name": "a"}]
    got["steps"].clear()
    assert store.get("d1")["steps"] == [{"name": "a"}]
    assert store.get("missing") is None


def test_latest_is_most_recently_written():
    store = ProgressStore()
    assert store.latest() is None
    store.set("a", {"finished": False, "steps": [], "n": 1})
    store.set("b", {"finished": False, "steps": [], "n": 2})
    store.set("a", {"finished": False, "steps": [], "n": 3})
    assert store.latest()["n"] == 3


def test_queue_position_lifecycle():
    store = ProgressStore()
    store.start_queued("d1", "space", "NOVA-7")
    assert store.get("d1")["steps"] == [queued_step(1, 1)]

    store.set_queue_position("d1", 2, 3)
    assert store.get("d1")["steps"] == [queued_step(2, 3)]

    # Once real steps replace the queued step, a position update resets to Queued.
    store.set("d1", {"finished": False, "error": "", "steps": [{"name": "Connect"}]})
    store.set_queue_position("d1", 1, 1)
    assert store.get("d1")["steps"] == [queued_step(1, 1)]

    store.mark_finished("d1", "boom")
    store.set_queue_position("d1", 5, 5)
    entry = store.get("d1")
    assert entry["finished"] is True and entry["error"] == "boom"
    assert entry["steps"] == [queued_step(1, 1)]


def test_start_queued_only_if_idle_keeps_running_entry():
    store = ProgressStore()
    store.set("d1", {"finished": False, "error": "", "steps": [{"name": "Running"}]})
    store.start_queued("d1", "space", "NOVA-7", only_if_idle=True)
    assert store.get("d1")["steps"] == [{"name": "Running"}]

    store.mark_finished("d1")
    store.start_queued("d1", "space", "NOVA-7", only_if_idle=True)
    assert store.get("d1")["steps"] == [queued_step(1, 1)]


def test_in_progress_filters_finished():
    store = ProgressStore()
    store.start_queued("a", "a", "A")
    store.start_queued("b", "b", "B")
    store.mark_finished("a")
    assert list(store.in_progress()) == ["b"]


def test_concurrent_writers_and_readers():
    """Readers serializing entries while writers update them must never fail."""
    store = ProgressStore()
    errors: list[BaseException] = []
    stop = threading.Event()

    def writer(dep: str) -> None:
        steps: list[dict] = []
        for i in range(2000):
            steps.append({"name": f"s{i}", "status": "done"})
            store.set(dep, {"finished": False, "error": "", "steps": steps})
            store.set_queue_position(dep, i % 3 + 1, 3)

    def reader() -> None:
        try:
            while not stop.is_set():
                for dep in ("d0", "d1", "d2"):
                    json.dumps(store.get(dep))
                json.dumps(store.in_progress())
                json.dumps(store.latest())
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    readers = [threading.Thread(target=reader) for _ in range(3)]
    writers = [threading.Thread(target=writer, args=(f"d{i}",)) for i in range(3)]
    for t in readers + writers:
        t.start()
    for t in writers:
        t.join()
    stop.set()
    for t in readers:
        t.join()
    assert errors == []


# ---------------------------------------------------------------------------
# Endpoints (lifespan is not started, so no auto-deploy or store restore runs)
# ---------------------------------------------------------------------------

@pytest.fixture()
def client(monkeypatch):
    from fastapi.testclient import TestClient

    import app.main as main

    monkeypatch.setattr(main, "_deploy_progress", ProgressStore())
    monkeypatch.setattr(main, "_teardown_progress", ProgressStore())
    return TestClient(main.app), main


def test_progress_endpoints(client):
    http, main = client
    assert http.get("/api/setup/progress").json() == IDLE

    main._deploy_progress.start_queued("space", "space", "NOVA-7")
    main._deploy_progress.start_queued("gaming", "gaming", "Gaming")
    main._deploy_progress.mark_finished("gaming")

    assert http.get("/api/setup/progress", params={"deployment_id": "space"}).json()["steps"] == [
        queued_step(1, 1)
    ]
    # Unknown id falls back to the most recently written entry.
    assert http.get("/api/setup/progress", params={"deployment_id": "nope"}).json()["finished"] is True

    active = http.get("/api/setup/active-deploys").json()["deployments"]
    assert active == [{"deployment_id": "space", "scenario_id": "space", "scenario_name": "NOVA-7"}]

    assert http.get("/api/setup/teardown-progress").json() == IDLE
    assert http.get("/api/setup/active-teardowns").json() == {"teardowns": []}
