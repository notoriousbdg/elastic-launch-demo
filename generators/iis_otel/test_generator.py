"""IIS-specific tests for impact_keys coverage."""

from __future__ import annotations

import random
import threading
from unittest.mock import MagicMock

from generators.iis_otel import SPEC
from generators.iis_otel.generator import PoolState, SiteState, run


def test_healthy_tick_does_not_increase_rejected_or_blocked():
    rng = random.Random(0)
    site = SiteState(rng)
    pool = PoolState(rng)
    blocked0 = site.blocked_bytes
    rejected0 = pool.rejected
    site.tick(rng, connection_error_rate=0.0)
    pool.tick(rng, connection_error_rate=0.0)
    assert site.blocked_bytes == blocked0
    assert pool.rejected == rejected0


def test_connection_error_rate_increases_rejected_and_blocked():
    rng = random.Random(0)
    site = SiteState(rng)
    pool = PoolState(rng)
    blocked0 = site.blocked_bytes
    rejected0 = pool.rejected
    site.tick(rng, connection_error_rate=0.8)
    pool.tick(rng, connection_error_rate=0.8)
    assert site.blocked_bytes > blocked0
    assert pool.rejected > rejected0


def test_run_emits_metrics():
    client = MagicMock()
    stop = threading.Event()
    sends = {"n": 0}

    def _after(*_a, **_k):
        sends["n"] += 1
        if sends["n"] >= 3:
            stop.set()

    client.send_metrics_scoped.side_effect = _after
    run(client, stop, scenario_data={
        "namespace": "testns",
        "generator_config": {},
        "channel_registry": {},
    })
    assert client.send_metrics_scoped.call_count >= 3

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.iis_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['application_pool_not_running', 'bandwidth_throttling_detected', 'request_queue_age_elevated', 'request_queue_depth_elevated', 'request_rejections_detected']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
