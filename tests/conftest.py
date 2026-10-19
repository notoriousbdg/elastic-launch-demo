"""Shared test fixtures: a recording OTLP client and a fake chaos controller.

Both stand in for the real collaborators at the interfaces services and
generators use, so telemetry can be produced without a network or a store.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Callable, Iterable

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from app.telemetry import OTLPClient  # noqa: E402

# Fields that come from the wall clock or secrets rather than the rng.
VOLATILE_FIELDS = frozenset(
    {"timeUnixNano", "observedTimeUnixNano", "startTimeUnixNano", "traceId", "spanId"}
)


def normalize_payload(obj: Any) -> Any:
    """Drop timestamps and trace/span ids from an OTLP payload."""
    if isinstance(obj, dict):
        return {k: normalize_payload(v) for k, v in obj.items() if k not in VOLATILE_FIELDS}
    if isinstance(obj, list):
        return [normalize_payload(v) for v in obj]
    return obj


class RecordingOTLP(OTLPClient):
    """A real OTLPClient (same payload building and routing) that records instead of POSTing."""

    def __init__(self, normalize: bool = False) -> None:
        super().__init__(endpoint="http://otlp.invalid", api_key="k", elastic_url="http://es.invalid")
        self.client.close()  # never used
        self._normalize = normalize
        self.sent: list[tuple[str, dict]] = []  # (signal, payload)

    def _send(self, url: str, payload: dict, signal_name: str) -> None:
        payload = self.prepare_payload(payload)
        self.sent.append((signal_name, normalize_payload(payload) if self._normalize else payload))

    @property
    def payloads(self) -> list[dict]:
        return [p for _, p in self.sent]

    def log_records(self) -> list[dict]:
        return [
            rec
            for signal, p in self.sent if "resourceLogs" in p
            for rl in p["resourceLogs"] for sl in rl["scopeLogs"] for rec in sl["logRecords"]
        ]


class FakeChaos:
    """The parts of ChaosController that services and generators call."""

    def __init__(self, active: Iterable[int] = (), spikes: dict | None = None) -> None:
        self.active = set(active)
        self.spikes = spikes or {
            "cpu_pct": 0, "memory_pct": 0, "k8s_oom_intensity": 0, "latency_multiplier": 1.0,
        }

    def is_active(self, ch: int) -> bool:
        return ch in self.active

    def get_active_channels(self) -> list[int]:
        return sorted(self.active)

    def get_channel_metadata(self, ch: int) -> dict:
        return {}

    def get_infra_spikes(self) -> dict:
        return dict(self.spikes)


@pytest.fixture()
def recording_otlp() -> Callable[..., RecordingOTLP]:
    """Factory: ``recording_otlp(normalize=False)``."""
    return RecordingOTLP


@pytest.fixture()
def fake_chaos() -> Callable[..., FakeChaos]:
    """Factory: ``fake_chaos(active=(), spikes=None)``."""
    return FakeChaos
