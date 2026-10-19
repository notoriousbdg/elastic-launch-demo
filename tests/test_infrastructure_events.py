"""Tests for one-shot infrastructure events emitted when a fault channel fires.

Run with:
    python -m pytest tests/test_infrastructure_events.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from app.context import ScenarioContext  # noqa: E402
from app.services.base_service import BaseService  # noqa: E402
from app.telemetry import OTLPClient  # noqa: E402
from app.trace_context import _trace_context_store  # noqa: E402
from scenario_engine import get_scenario  # noqa: E402

SCENARIO = "telecom"
CHANNEL = 6
SERVICE = "network-core"
EVENT_NAME = "gcp.monitoring.GTPTunnelSetupFailure"


class RecordingOTLP:
    """Stands in for OTLPClient; keeps every sent log record."""

    def __init__(self) -> None:
        self._real = OTLPClient.__new__(OTLPClient)
        self.logs: list[dict] = []

    def send_logs(self, resource, records):
        self.logs.extend(records)

    def send_metrics(self, resource, metrics):
        pass

    def send_traces(self, resource, spans):
        pass

    def __getattr__(self, name):
        return getattr(self._real, name)


class FakeChaos:
    def __init__(self) -> None:
        self.active: set[int] = set()

    def is_active(self, ch: int) -> bool:
        return ch in self.active

    def get_channel_metadata(self, ch: int) -> dict:
        return {}


@pytest.fixture()
def service():
    scenario = get_scenario(SCENARIO)
    ch = scenario.channel_registry[CHANNEL]
    assert any(e.get("event_name") == EVENT_NAME for e in ch["infrastructure_events"])

    ctx = ScenarioContext.from_scenario(scenario)
    otlp, chaos = RecordingOTLP(), FakeChaos()
    cls = next(c for c in scenario.get_service_classes() if c.SERVICE_NAME == SERVICE)
    BaseService.set_context(ctx)
    try:
        svc = cls(chaos, otlp)
    finally:
        BaseService.clear_context()
    return svc, otlp, chaos


def _infra_events(otlp: RecordingOTLP) -> list[dict]:
    return [r for r in otlp.logs if r.get("eventName") == EVENT_NAME]


def test_infra_event_uses_channel_error_trace(service):
    svc, otlp, chaos = service
    _trace_context_store.set(SERVICE, "a" * 32, "b" * 16, namespace=svc._namespace)
    _trace_context_store.set_for_channel(CHANNEL, SERVICE, "c" * 32, "d" * 16, namespace=svc._namespace)

    chaos.active = {CHANNEL}
    svc.generate_telemetry()

    events = _infra_events(otlp)
    assert len(events) == 1
    assert events[0]["traceId"] == "c" * 32


def test_infra_event_fires_once_per_activation(service):
    svc, otlp, chaos = service

    chaos.active = {CHANNEL}
    svc.generate_telemetry()
    svc.generate_telemetry()
    assert len(_infra_events(otlp)) == 1

    chaos.active = set()
    svc.generate_telemetry()

    chaos.active = {CHANNEL}
    svc.generate_telemetry()
    assert len(_infra_events(otlp)) == 2


def test_only_one_emit_infrastructure_events_definition():
    source = (REPO_ROOT / "app/services/base_service.py").read_text()
    assert source.count("def emit_infrastructure_events(") == 1
    assert source.count("def reset_infrastructure_events(") == 1
