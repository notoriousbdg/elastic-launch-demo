"""Technology-specific tests for the ``raw_access`` generator."""

from __future__ import annotations

import random
import re

from generators.raw_access import SPEC
from generators.raw_access.generator import generate_record


_PROFILE = {
    "methods": ["GET", "POST"],
    "paths": ["/api/health", "/api/data"],
    "change_point_path": "/api/checkout",
    "user_id_prefix": "user-",
    "country_weights": {"USA": 50, "DEU": 30, "JPN": 20},
    "tier_values": [("gold", 40), ("silver", 60)],
    "tier_field": "tier",
    "service_name": "demo-api",
}


def _status_from_message(message: str) -> int:
    match = re.search(r'"(\w+) [^"]+" (\d{3}) ', message)
    assert match, message
    return int(match.group(2))


def test_healthy_baseline_no_chaos():
    rng = random.Random(1)
    records = [generate_record(_PROFILE, "demo", rng) for _ in range(200)]
    errors = sum(1 for r in records if _status_from_message(r["message"]) >= 500)
    assert errors / len(records) < 0.15


def test_error_fraction_chaos_increases_5xx():
    rng = random.Random(1)
    records = [
        generate_record(_PROFILE, "demo", rng, error_fraction=0.9)
        for _ in range(200)
    ]
    errors = sum(1 for r in records if _status_from_message(r["message"]) >= 500)
    assert errors / len(records) >= 0.35


def test_spike_active_uses_change_point_path():
    rng = random.Random(1)
    records = [
        generate_record(_PROFILE, "demo", rng, spike_active=True, spike_country="JPN")
        for _ in range(100)
    ]
    checkout_hits = sum(1 for r in records if "/api/checkout" in r["message"])
    assert checkout_hits >= 30


def test_spec_wants_chaos_and_impact_keys():
    assert SPEC.wants_chaos is True
    assert sorted(SPEC.impact_keys) == ["error_fraction", "volume_multiplier"]
