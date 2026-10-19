"""Tests for the deploy/teardown operation queue.

Run with:
    python -m pytest tests/test_op_queue.py -v
"""

from __future__ import annotations

import threading
import time

from app.op_queue import OperationQueue


class Gate:
    """A runner that blocks until released, recording when it started."""

    def __init__(self, name: str, log: list[str]) -> None:
        self.name, self.log = name, log
        self.started = threading.Event()
        self.release = threading.Event()

    def __call__(self) -> None:
        self.log.append(self.name)
        self.started.set()
        self.release.wait(10)


def _wait_until(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not reached"
        time.sleep(0.01)


def test_runs_at_most_max_concurrent_and_prioritises_deploys():
    q = OperationQueue(max_concurrent=2)
    log: list[str] = []
    gates = {n: Gate(n, log) for n in ("d1", "d2", "t1", "d3")}

    assert q.submit_deploy("a", gates["d1"])
    assert q.submit_deploy("b", gates["d2"])
    gates["d1"].started.wait(5)
    gates["d2"].started.wait(5)
    assert q.submit_teardown("c", gates["t1"])
    assert q.submit_deploy("d", gates["d3"])

    time.sleep(0.1)
    assert sorted(q.snapshot()["active"]) == ["a", "b"]
    assert q.list_ordered("deploy") == ["a", "b", "d"]
    assert q.list_ordered("teardown") == ["c"]

    gates["d1"].release.set()
    # The queued deploy runs before the earlier-queued teardown.
    assert gates["d3"].started.wait(5)
    assert not gates["t1"].started.is_set()

    gates["d2"].release.set()
    assert gates["t1"].started.wait(5)
    for g in gates.values():
        g.release.set()
    _wait_until(lambda: q.snapshot()["active"] == [])
    assert log == ["d1", "d2", "d3", "t1"]


def test_rejects_duplicate_deployment_ids():
    q = OperationQueue(max_concurrent=1)
    log: list[str] = []
    running, queued = Gate("run", log), Gate("queued", log)
    assert q.submit_deploy("a", running)
    running.started.wait(5)
    assert not q.submit_deploy("a", Gate("dup", log))      # active
    assert not q.submit_teardown("a", Gate("dup", log))    # active, other kind
    assert q.submit_deploy("b", queued)
    assert not q.submit_teardown("b", Gate("dup", log))    # queued
    running.release.set()
    queued.release.set()


def test_cancel_only_removes_queued_deploys():
    q = OperationQueue(max_concurrent=1)
    log: list[str] = []
    running, queued = Gate("run", log), Gate("queued", log)
    q.submit_deploy("a", running)
    running.started.wait(5)
    q.submit_deploy("b", queued)

    assert not q.cancel_deploy("a")   # already running
    assert q.cancel_deploy("b")
    assert q.snapshot()["deploy_queue"] == []
    running.release.set()
    time.sleep(0.1)
    assert not queued.started.is_set()


def test_position_callbacks_and_runner_errors():
    q = OperationQueue(max_concurrent=1)
    log: list[str] = []
    blocker = Gate("block", log)
    positions: dict[str, list] = {"b": [], "c": []}

    def boom() -> None:
        raise RuntimeError("runner failed")

    q.submit_deploy("a", blocker)
    blocker.started.wait(5)
    q.submit_deploy("b", boom, lambda p, t: positions["b"].append((p, t)))
    q.submit_teardown("c", Gate("c", log).release.set, lambda p, t: positions["c"].append((p, t)))

    assert positions["b"][-1] == (1, 2) and positions["c"][-1] == (2, 2)
    blocker.release.set()
    # A raising runner is logged and does not wedge the queue.
    _wait_until(lambda: q.snapshot() == {"active": [], "deploy_queue": [], "teardown_queue": []})
