"""Contracts for ChaosController and OperationQueue.

These lock the behaviors routes and generators share: trigger, resolve,
session ownership, auto-expiry, SQLite restore, and deploy-before-teardown
dispatch.
"""
import threading
import time

from app.chaos.controller import ACTIVE, STANDBY, ChaosController
from app.op_queue import OperationQueue
from app.store import ChaosStore


def _registry():
    def channel(name, impact=None):
        return {
            "name": name,
            "subsystem": "propulsion",
            "affected_services": ["engine"],
            "cascade_services": [],
            "error_type": "Fault",
            "sensor_type": "pressure",
            "vehicle_section": "booster",
            "description": name,
            "infra_impact": impact or {},
        }

    return {
        1: channel("fuel", {"latency_multiplier": 5.0, "cpu_pct": 80}),
        2: channel("oxidizer"),
    }


def _store(tmp_path):
    return ChaosStore(str(tmp_path / "chaos.db"))


def test_trigger_and_resolve_honor_session_and_spikes(tmp_path):
    resolved = threading.Event()
    controller = ChaosController(
        _registry(),
        chaos_store=_store(tmp_path),
        deployment_id="dep",
        on_resolve=resolved.set,
    )

    assert controller.trigger(9)["error"].startswith("Unknown channel")

    result = controller.trigger(1, session_id="sess", user_email="a@b.c")
    assert result["status"] == "triggered"
    assert controller.get_channel_status(1)["state"] == ACTIVE
    assert controller.get_infra_spikes()["latency_multiplier"] == 5.0
    assert controller.get_infra_spikes()["cpu_pct"] == 80
    assert controller.trigger(1)["status"] == "already_active"

    mismatch = controller.resolve(1, session_id="other")
    assert mismatch["error"] == "session_mismatch"
    assert controller.is_active(1)

    resolved_result = controller.resolve(1, session_id="sess")
    assert resolved_result["status"] == "resolved"
    assert controller.get_channel_status(1)["state"] == STANDBY
    assert controller.get_infra_spikes()["latency_multiplier"] == 1.0
    assert controller.get_infra_spikes()["cpu_pct"] == 0
    assert resolved.wait(2)


def test_store_restores_active_channel_and_spikes(tmp_path):
    store = _store(tmp_path)
    first = ChaosController(_registry(), chaos_store=store, deployment_id="dep")
    first.trigger(1, session_id="sess")

    second = ChaosController(_registry(), chaos_store=store, deployment_id="dep")
    assert second.is_active(1)
    assert second.get_active_channels() == [1]
    assert second.get_infra_spikes()["latency_multiplier"] == 5.0
    assert second.validate_session("sess") == [1]
    assert second.validate_session("nope") == []


def test_stale_channel_expires_and_clears_spikes(tmp_path, monkeypatch):
    monkeypatch.setattr("app.chaos.controller.MAX_FAULT_DURATION", 0)
    resolved = threading.Event()
    controller = ChaosController(
        _registry(),
        chaos_store=_store(tmp_path),
        deployment_id="dep",
        on_resolve=resolved.set,
    )
    controller.trigger(1)
    status = controller.get_status()
    assert status[1]["state"] == STANDBY
    assert controller.get_infra_spikes()["cpu_pct"] == 0
    assert resolved.wait(2)

    restored = ChaosController(_registry(), chaos_store=controller._store, deployment_id="dep")
    assert restored.get_channel_status(1)["state"] == STANDBY


def test_operation_queue_prefers_deploy_and_rejects_duplicates():
    queue = OperationQueue(max_concurrent=1)
    started = threading.Event()
    release = threading.Event()
    order = []

    def hold():
        order.append("hold")
        started.set()
        release.wait(2)

    def record(name):
        def _run():
            order.append(name)
        return _run

    assert queue.submit_deploy("hold", hold)
    assert started.wait(2)
    assert queue.submit_teardown("tear", record("tear"))
    assert queue.submit_deploy("next", record("next"))
    assert queue.submit_deploy("next", record("next-again")) is False
    assert queue.list_ordered("deploy") == ["hold", "next"]

    positions = []
    assert queue.submit_teardown("tear-2", record("tear-2"), on_position=lambda pos, total: positions.append((pos, total)))
    assert positions[-1][0] >= 1

    release.set()
    deadline = time.time() + 2
    while time.time() < deadline and order != ["hold", "next", "tear", "tear-2"]:
        time.sleep(0.01)
    assert order == ["hold", "next", "tear", "tear-2"]


def test_cancel_deploy_removes_queued_work_only():
    queue = OperationQueue(max_concurrent=1)
    started = threading.Event()
    release = threading.Event()
    ran = []

    def hold():
        started.set()
        release.wait(2)

    assert queue.submit_deploy("active", hold)
    assert started.wait(2)
    assert queue.submit_deploy("queued", lambda: ran.append("queued"))
    assert queue.cancel_deploy("queued") is True
    assert queue.cancel_deploy("active") is False
    assert queue.list_ordered("deploy") == ["active"]
    release.set()
    time.sleep(0.05)
    assert ran == []
