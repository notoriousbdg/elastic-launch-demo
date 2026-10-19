"""Tests for the chaos controller state machine and its SQLite persistence.

Run with:
    python -m pytest tests/test_chaos_controller.py -v
"""

from __future__ import annotations

import threading
import time

import pytest

import app.chaos.controller as controller_mod
from app.chaos.controller import ACTIVE, STANDBY, ChaosController
from app.store import ChaosStore
from scenario_engine import get_scenario

BASELINE = {"cpu_pct": 0, "memory_pct": 0, "k8s_oom_intensity": 0, "latency_multiplier": 1.0}


def _registry(**impact_by_channel):
    registry = {ch: dict(entry) for ch, entry in get_scenario("space").channel_registry.items()}
    for ch, impact in impact_by_channel.items():
        registry[int(ch.lstrip("ch"))]["infra_impact"] = impact
    return registry


@pytest.fixture()
def store(tmp_path):
    return ChaosStore(str(tmp_path / "chaos.db"))


def test_trigger_and_resolve_round_trip():
    c = ChaosController(channel_registry=_registry())
    assert c.trigger(3, mode="demo", session_id="s1", user_email="a@x")["status"] == "triggered"
    assert c.is_active(3) and c.get_active_channels() == [3]
    assert c.trigger(3)["status"] == "already_active"
    assert c.get_channel_metadata(3)["user_email"] == "a@x"

    assert c.resolve(3, session_id="s1")["status"] == "resolved"
    assert not c.is_active(3)
    assert c.resolve(3)["status"] == "already_standby"


def test_unknown_channel():
    c = ChaosController(channel_registry=_registry())
    assert "error" in c.trigger(99)
    assert "error" in c.resolve(99)


@pytest.mark.parametrize("kwargs,ok", [
    ({"session_id": "s1"}, True),
    ({"user_email": "owner@x"}, True),
    ({"session_id": "other"}, False),
    ({"user_email": "someone@x"}, False),
    ({}, False),
    ({"force": True}, True),
])
def test_resolve_requires_owner_unless_forced(kwargs, ok):
    c = ChaosController(channel_registry=_registry())
    c.trigger(5, session_id="s1", user_email="owner@x")
    result = c.resolve(5, **kwargs)
    assert (result.get("status") == "resolved") is ok
    assert c.is_active(5) is (not ok)


def test_validate_session_lists_owned_channels():
    c = ChaosController(channel_registry=_registry())
    c.trigger(1, session_id="s1")
    c.trigger(2, session_id="s2")
    c.trigger(4, session_id="s1")
    assert sorted(c.validate_session("s1")) == [1, 4]


def test_infra_impact_spikes_combine_and_reset():
    c = ChaosController(channel_registry=_registry(ch2={"cpu_pct": 80}, ch3={"cpu_pct": 40, "memory_pct": 70}))
    c.trigger(2)
    c.trigger(3)
    assert c.get_infra_spikes() == {**BASELINE, "cpu_pct": 80, "memory_pct": 70}
    c.resolve(2, force=True)
    assert c.get_infra_spikes()["cpu_pct"] == 80  # only the last resolve resets
    c.resolve(3, force=True)
    assert c.get_infra_spikes() == BASELINE


def test_expiry_resolves_stale_channels(monkeypatch):
    monkeypatch.setattr(controller_mod, "MAX_FAULT_DURATION", 60)
    c = ChaosController(channel_registry=_registry())
    c.trigger(7)
    c._channels[7]["triggered_at"] = time.time() - 61
    assert not c.is_active(7)
    assert c.get_status()[7]["state"] == STANDBY


def test_on_resolve_fires_when_last_channel_resolves():
    fired = threading.Event()
    c = ChaosController(channel_registry=_registry(), on_resolve=fired.set)
    c.trigger(1)
    c.trigger(2)
    c.resolve(1, force=True)
    assert not fired.wait(0.2)
    c.resolve(2, force=True)
    assert fired.wait(2)


def test_state_survives_restart_via_store(store):
    registry = _registry(ch6={"latency_multiplier": 4.0})
    c = ChaosController(channel_registry=registry, chaos_store=store, deployment_id="dep")
    c.trigger(6, session_id="s1", user_email="a@x", callback_url="http://cb")
    c.trigger(8, session_id="s1")
    c.resolve(8, session_id="s1")

    restored = ChaosController(channel_registry=registry, chaos_store=store, deployment_id="dep")
    assert restored.get_active_channels() == [6]
    assert restored.get_channel_metadata(6)["callback_url"] == "http://cb"
    assert restored.get_infra_spikes()["latency_multiplier"] == 4.0

    other = ChaosController(channel_registry=registry, chaos_store=store, deployment_id="other")
    assert other.get_active_channels() == []


def test_status_shape():
    c = ChaosController(channel_registry=_registry())
    c.trigger(1, mode="calibration")
    status = c.get_status()
    assert set(status) == set(range(1, 21))
    assert status[1]["state"] == ACTIVE and status[1]["mode"] == "calibration"
    assert {"name", "subsystem", "state", "affected_services"} <= set(status[1])
