"""Run every service of every scenario and check the telemetry it produces.

For each scenario: 50 seeded ticks with no faults, then 5 ticks with each of
the 20 channels active on its own. Every tick must complete without raising,
nothing may be logged at ERROR, and every payload must convert to the OTLP
protobuf request it will be sent as. A broken ``expr:`` or template shows up
here instead of as a runtime log line.

Run with:
    python -m pytest tests/test_scenarios_smoke.py -v
"""

from __future__ import annotations

import logging
import random
from pathlib import Path

import pytest
from google.protobuf.json_format import ParseDict
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from app.context import ScenarioContext
from conftest import FakeChaos, RecordingOTLP
from scenario_engine import get_scenario

REPO_ROOT = Path(__file__).resolve().parent.parent
SCENARIO_IDS = sorted(p.parent.name for p in (REPO_ROOT / "scenarios").glob("*/scenario.yaml"))
NOMINAL_TICKS = 50
FAULT_TICKS = 5
FROZEN_NOW = 1_800_000_000.0

_REQUEST_FOR = {
    "resourceLogs": ExportLogsServiceRequest,
    "resourceMetrics": ExportMetricsServiceRequest,
    "resourceSpans": ExportTraceServiceRequest,
}


def _build_services(scenario, chaos, otlp, seed: int):
    ctx = ScenarioContext.from_scenario(scenario)
    clock_state = {"now": FROZEN_NOW}

    def clock() -> float:
        return clock_state["now"]

    services = [
        cls(chaos, otlp, ctx, rng=random.Random(f"{seed}:{cls.SERVICE_NAME}"), clock=clock)
        for cls in scenario.get_service_classes()
    ]
    return services, clock_state


def _tick(services, clock_state, n: int) -> None:
    for _ in range(n):
        # Advance 2 s per tick so `every:` blocks fire.
        clock_state["now"] += 2.0
        for svc in services:
            svc.generate_telemetry()


def _assert_valid_payloads(otlp: RecordingOTLP) -> None:
    assert otlp.sent, "no telemetry was produced"
    for signal, payload in otlp.sent:
        (key,) = [k for k in _REQUEST_FOR if k in payload]
        ParseDict(payload, _REQUEST_FOR[key]())  # raises on any malformed field
    for rec in otlp.log_records():
        assert rec.get("severityText"), rec
        assert rec.get("body", {}).get("stringValue"), rec


@pytest.fixture()
def error_logs(caplog):
    caplog.set_level(logging.WARNING)
    yield lambda: [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.mark.parametrize("scenario_id", SCENARIO_IDS)
def test_services_nominal(scenario_id, error_logs):
    scenario = get_scenario(scenario_id)
    assert len(scenario.get_service_classes()) == 9

    otlp = RecordingOTLP()
    services, clock_state = _build_services(scenario, FakeChaos(), otlp, seed=1)
    _tick(services, clock_state, NOMINAL_TICKS)

    _assert_valid_payloads(otlp)
    assert error_logs() == []


@pytest.mark.parametrize("scenario_id", SCENARIO_IDS)
def test_services_with_each_channel_active(scenario_id, error_logs):
    scenario = get_scenario(scenario_id)
    for channel in sorted(scenario.channel_registry):
        otlp, chaos = RecordingOTLP(), FakeChaos(active=[channel])
        services, clock_state = _build_services(scenario, chaos, otlp, seed=channel)
        _tick(services, clock_state, FAULT_TICKS)

        _assert_valid_payloads(otlp)
        error_type = scenario.channel_registry[channel]["error_type"]
        fault_logs = [
            r for r in otlp.log_records()
            if r.get("severityText") == "ERROR" and error_type in r["body"]["stringValue"]
        ]
        assert fault_logs, f"channel {channel}: no ERROR log mentions {error_type}"
    assert error_logs() == []
