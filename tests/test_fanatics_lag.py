"""Fanatics: dependents log their symptoms after the component at fault (service_lag_seconds)."""

from __future__ import annotations

import time
from pathlib import Path

from app.chaos.controller import ChaosController
from app.context import ScenarioContext
from app.services.base_service import BaseService
from scenario_engine.yaml_scenario import load_yaml_scenario
from tests.test_generator_contract import _RecordingOTLP

ROOT = Path(__file__).resolve().parent.parent
SCN = load_yaml_scenario(ROOT / "scenarios" / "fanatics")
CTX = ScenarioContext.from_scenario(SCN)
REG = SCN.channel_registry


def _services_logging(channel: int, elapsed: float) -> set[str]:
    """Services logging WARN/ERROR (fault or cascade lines) *elapsed* s after *channel* fired."""
    c = ChaosController(channel_registry=REG)
    c.trigger(channel, user_email="sre@example.com")
    c._channels[channel]["triggered_at"] = time.time() - elapsed
    client = _RecordingOTLP()
    BaseService.set_context(CTX)
    try:
        services = [cls(c, client) for cls in SCN.get_service_classes()]
    finally:
        BaseService.clear_context()
    seen: set[str] = set()
    for svc in services:
        before = len(client.payloads)
        for _ in range(3):
            svc.generate_telemetry()
        for name, args, _kw in client.payloads[before:]:
            if name != "send_logs":
                continue
            for rec in args[1]:
                if rec["severityText"] in ("WARN", "ERROR"):
                    seen.add(svc.SERVICE_NAME)
    return seen


def test_firewall_cpu_dependents_follow_in_order():
    """ch5: firewall-gateway first, then network-controller / marketplace, then DNS."""
    early = _services_logging(5, elapsed=5)
    mid = _services_logging(5, elapsed=25)
    late = _services_logging(5, elapsed=60)
    assert "firewall-gateway" in early
    assert not {"network-controller", "digital-marketplace", "dns-dhcp-service"} & early
    assert {"network-controller", "digital-marketplace"} <= mid
    assert "dns-dhcp-service" not in mid
    assert "dns-dhcp-service" in late


def test_every_lagged_service_is_affected_or_cascade():
    for n, ch in REG.items():
        known = set(ch["affected_services"]) | set(ch.get("cascade_services") or [])
        assert set(ch.get("service_lag_seconds") or {}) <= known, n
