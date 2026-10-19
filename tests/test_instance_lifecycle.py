"""Tests for instance start/stop/restart and registry locking.

Run with:
    python -m pytest tests/test_instance_lifecycle.py -v
"""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from app.chaos.remediation_poller import RemediationPoller  # noqa: E402
from app.context import ScenarioContext  # noqa: E402
from app.instance import ScenarioInstance  # noqa: E402
from app.registry import InstanceRegistry  # noqa: E402
from scenario_engine import get_scenario  # noqa: E402


def _alive_generators(manager) -> set[str]:
    return {t.name for t in manager._generator_threads if t.is_alive()}


@pytest.fixture()
def instance(monkeypatch):
    # No endpoints configured: clients drop sends instead of making HTTP calls.
    import app.telemetry as telemetry

    monkeypatch.setattr(telemetry, "OTLP_ENDPOINT", "")
    ctx = ScenarioContext.from_scenario(get_scenario("space"))
    inst = ScenarioInstance(ctx)
    yield inst
    inst.stop()


def test_instance_can_restart_after_stop(instance):
    manager = instance.service_manager

    instance.start()
    assert instance.running
    time.sleep(0.2)
    # gen-raw-access exits on its own when ES is not configured.
    first_run = _alive_generators(manager)
    assert len(first_run) >= 8

    instance.stop()
    assert not instance.running
    assert manager._stop_event.is_set()
    assert instance.otlp.client.is_closed

    instance.start()
    assert instance.running
    assert not manager._stop_event.is_set()
    assert not instance.otlp.client.is_closed
    assert not instance.service_manager.es_bulk.client.is_closed
    time.sleep(0.2)
    assert len(manager._generator_threads) == 9
    assert _alive_generators(manager) == first_run
    assert all(svc._thread and svc._thread.is_alive() for svc in manager.services.values())


class _IdleChaos:
    def get_active_channels(self) -> list[int]:
        return []


def test_remediation_poller_stops_and_restarts(monkeypatch):
    monkeypatch.setattr(RemediationPoller, "_flush_stale_pending", lambda self: None)
    poller = RemediationPoller(
        elastic_url="https://es.example",
        elastic_api_key="k",
        namespace="ns",
        chaos_controller=_IdleChaos(),
    )

    poller.start()
    first = poller._thread
    assert first.is_alive()

    poller.stop()
    assert not first.is_alive()

    poller.start()
    assert poller._thread is not first and poller._thread.is_alive()
    poller.stop()


class _SlowStopInstance:
    """Minimal stand-in whose stop() blocks until released."""

    def __init__(self) -> None:
        self.deployment_id = ""
        self.release = threading.Event()
        self.stopping = threading.Event()

    def stop(self) -> None:
        self.stopping.set()
        self.release.wait(timeout=10)


def test_registry_does_not_hold_lock_while_stopping_old_instance():
    registry = InstanceRegistry()
    old, new = _SlowStopInstance(), _SlowStopInstance()
    registry.register("dep", old)

    worker = threading.Thread(target=registry.register, args=("dep", new))
    worker.start()
    assert old.stopping.wait(timeout=5)

    # While the old instance is still stopping, the registry stays usable.
    started = time.monotonic()
    assert registry.get("dep") is new
    assert len(registry) == 1
    assert time.monotonic() - started < 1

    old.release.set()
    worker.join(timeout=5)
    assert not worker.is_alive()


def test_registry_stop_all_releases_lock_and_clears():
    registry = InstanceRegistry()
    inst = _SlowStopInstance()
    registry.register("dep", inst)

    worker = threading.Thread(target=registry.stop_all)
    worker.start()
    assert inst.stopping.wait(timeout=5)
    assert registry.get("dep") is None

    inst.release.set()
    worker.join(timeout=5)
