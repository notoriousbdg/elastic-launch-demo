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
from app.trace_context import _trace_context_store  # noqa: E402
from scenario_engine import get_scenario  # noqa: E402

SCENARIO = "telecom"
CHANNEL = 6
SERVICE = "network-core"
EVENT_NAME = "gcp.monitoring.GTPTunnelSetupFailure"


@pytest.fixture()
def service(recording_otlp, fake_chaos):
    scenario = get_scenario(SCENARIO)
    ch = scenario.channel_registry[CHANNEL]
    assert any(e.get("event_name") == EVENT_NAME for e in ch["infrastructure_events"])

    ctx = ScenarioContext.from_scenario(scenario)
    otlp, chaos = recording_otlp(), fake_chaos()
    cls = next(c for c in scenario.get_service_classes() if c.SERVICE_NAME == SERVICE)
    svc = cls(chaos, otlp, ctx)
    return svc, otlp, chaos


def _infra_events(otlp) -> list[dict]:
    return [r for r in otlp.log_records() if r.get("eventName") == EVENT_NAME]


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
