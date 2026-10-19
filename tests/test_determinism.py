"""Tests that injected rng/clock make service and generator output reproducible.

Run with:
    python -m pytest tests/test_determinism.py -v
"""

from __future__ import annotations

import random
import sys
import threading
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from app.context import ScenarioContext  # noqa: E402
from app.telemetry import OTLPClient  # noqa: E402
from scenario_engine import get_scenario  # noqa: E402

FROZEN_NOW = 1_800_000_000.0
VOLATILE = {"timeUnixNano", "observedTimeUnixNano", "startTimeUnixNano", "traceId", "spanId"}


def _normalize(obj):
    """Drop timestamps and trace/span ids, which come from the wall clock / secrets."""
    if isinstance(obj, dict):
        return {k: _normalize(v) for k, v in obj.items() if k not in VOLATILE}
    if isinstance(obj, list):
        return [_normalize(v) for v in obj]
    return obj


class RecordingOTLP(OTLPClient):
    def __init__(self) -> None:
        super().__init__(endpoint="http://otlp.invalid", api_key="k", elastic_url="http://es.invalid")
        self.payloads: list = []

    def _send(self, url, payload, signal_name):
        self.payloads.append(_normalize(payload))


class FakeChaos:
    def __init__(self, active=()):
        self.active = set(active)

    def is_active(self, ch):
        return ch in self.active

    def get_active_channels(self):
        return sorted(self.active)

    def get_channel_metadata(self, ch):
        return {}

    def get_infra_spikes(self):
        return {"cpu_pct": 0, "memory_pct": 0, "k8s_oom_intensity": 0, "latency_multiplier": 1.0}


def _run_services(scenario_id: str, seed: int, active=(), ticks: int = 3) -> list:
    scenario = get_scenario(scenario_id)
    ctx = ScenarioContext.from_scenario(scenario)
    otlp, chaos = RecordingOTLP(), FakeChaos(active)
    clock = lambda: FROZEN_NOW  # noqa: E731
    services = [
        cls(chaos, otlp, ctx, rng=random.Random(f"{seed}:{cls.SERVICE_NAME}"), clock=clock)
        for cls in scenario.get_service_classes()
    ]
    for _ in range(ticks):
        for svc in services:
            svc.generate_telemetry()
    otlp.client.close()
    return otlp.payloads


@pytest.mark.parametrize("scenario_id", ["space", "financial", "telecom"])
@pytest.mark.parametrize("active", [(), (1, 7, 16)], ids=["nominal", "faults"])
def test_services_are_reproducible_with_seeded_rng(scenario_id, active):
    first = _run_services(scenario_id, seed=42, active=active)
    second = _run_services(scenario_id, seed=42, active=active)
    assert first and first == second


def test_different_seeds_differ():
    assert _run_services("space", seed=1) != _run_services("space", seed=2)


def test_injected_clock_drives_expr_time_and_state():
    scenario = get_scenario("space")
    ctx = ScenarioContext.from_scenario(scenario)
    cls = scenario.get_service_classes()[0]
    svc = cls(FakeChaos(), RecordingOTLP(), ctx, rng=random.Random(0), clock=lambda: FROZEN_NOW)
    assert svc._safe_time.time() == FROZEN_NOW
    assert svc._last_status_change == FROZEN_NOW


@pytest.mark.parametrize("module", [
    "vpc_flow_generator", "nginx_metrics_generator", "host_metrics_generator",
    "jvm_metrics_generator", "mysql_log_generator",
])
def test_generators_are_reproducible_with_seeded_rng(module):
    import importlib

    run = importlib.import_module(f"log_generators.{module}").run
    data = ScenarioContext.from_scenario(get_scenario("space")).generator_data()

    def one_cycle(seed: int) -> list:
        stop = threading.Event()
        client = RecordingOTLP()
        original = client._send

        def _send(url, payload, signal_name):
            original(url, payload, signal_name)
            stop.set()  # finish after the first cycle

        client._send = _send
        kwargs = {"scenario_data": data, "rng": random.Random(seed)}
        if "chaos_controller" in run.__code__.co_varnames:
            kwargs["chaos_controller"] = FakeChaos()
        run(client, stop, **kwargs)
        client.client.close()
        return client.payloads

    first, second = one_cycle(7), one_cycle(7)
    assert first and first == second
