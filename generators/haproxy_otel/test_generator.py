"""HAProxy-specific tests for the haproxy generator."""
# All invariant tests are covered by tests/test_generator_registry.py.

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.haproxy_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['backend_has_no_active_servers', 'connection_errors_spike', 'connection_retries_or_redispatches_spike', 'high_backend_connection_time', 'high_backend_queue_time', 'high_backend_response_time', 'request_errors_spike', 'requests_queued_at_backend', 'response_errors_spike', 'server_health_check_failures', 'session_saturation_approaching_limit']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


# --- per-alert behaviour: each key alone breaches only its own alert ------------------
# Drives run() for several cycles and evaluates the package alert ES|QL (v0.3.1) per
# entity (proxy, service, addr) over the last WINDOW cycles.

import random  # noqa: E402
from unittest.mock import MagicMock, patch  # noqa: E402

import pytest  # noqa: E402

from generators.haproxy_otel import generator as mod  # noqa: E402

DEFAULTS = {key: meta["default"] for key, meta in _SPEC_FOR_ALERT_KEYS.impact_keys.items()}
SEEDS = range(6)
WINDOW = 6


class _SeqChaos:
    """Channel ch1 is active for the first ``faulted`` cycles, then resolves."""

    def __init__(self, faulted):
        self._faulted = faulted
        self._calls = 0

    def get_active_channels(self):
        self._calls += 1
        return ["ch1"] if self._calls <= self._faulted else []


def _simulate(impact, seed, faulted, cycles):
    client = MagicMock()
    stop = MagicMock()
    stop.is_set.side_effect = [False] * cycles + [True]
    stop.wait.return_value = None
    bulk = MagicMock()
    bulk.configured = False
    real_random = random.Random
    with patch.object(mod, "ESBulkClient", return_value=bulk), \
            patch.object(mod.random, "Random", lambda *a: real_random(seed)):
        mod.run(
            client, stop,
            scenario_data={
                "namespace": "testns",
                "generator_config": {"chaos": "explicit"},
                "channel_registry": {"ch1": {"generator_impact": {"haproxy_otel": impact}}},
            },
            chaos_controller=_SeqChaos(faulted),
        )
    series: dict[tuple, list[float]] = {}
    for call in client.send_metrics_scoped.call_args_list:
        resource, _scope, metrics = call.args
        entity = tuple(
            mod._resource_attr_map(resource).get(k)
            for k in ("haproxy.proxy_name", "haproxy.service_name", "haproxy.addr")
        )
        for m in metrics:
            dp = (m.get("gauge") or m["sum"])["dataPoints"][0]
            if dp.get("attributes"):  # requests.total by status_code: not alerted
                continue
            value = float(dp["asInt"] if "asInt" in dp else dp["asDouble"])
            series.setdefault((m["name"], entity), []).append(value)
    return {k: v[-WINDOW:] for k, v in series.items()}


def _entities(series, metric):
    return {entity: w for (name, entity), w in series.items() if name == metric}


def _increase(window):
    return window[-1] - window[0]


def _breached(series):
    def inc(metric, threshold, *, skip=()):
        return any(
            _increase(w) > threshold
            for entity, w in _entities(series, metric).items() if entity[1] not in skip
        )

    def peak(metric, threshold):
        return any(max(w) > threshold for w in _entities(series, metric).values())

    def instability():
        retries = _entities(series, "haproxy.connections.retries")
        redispatched = _entities(series, "haproxy.requests.redispatched")
        return any(
            _increase(retries.get(e, [0, 0])) + _increase(redispatched.get(e, [0, 0])) > 5
            for e in set(retries) | set(redispatched)
        )

    sessions = _entities(series, "haproxy.sessions.count")
    limits = _entities(series, "haproxy.sessions.limit")
    return {
        "backend_has_no_active_servers": any(
            min(w) < 1 for e, w in _entities(series, "haproxy.active").items() if e[1] == "BACKEND"
        ),
        "connection_errors_spike": inc("haproxy.connections.errors", 10),
        "server_health_check_failures": inc(
            "haproxy.failed_checks", 0, skip=("FRONTEND", "BACKEND")),
        "high_backend_connection_time": peak("haproxy.connections.average_time", 100),
        "high_backend_queue_time": peak("haproxy.requests.average_time", 10),
        "high_backend_response_time": peak("haproxy.responses.average_time", 500),
        "request_errors_spike": inc("haproxy.requests.errors", 10),
        "requests_queued_at_backend": any(
            w[-1] > 0 for w in _entities(series, "haproxy.requests.queued").values()),
        "response_errors_spike": inc("haproxy.responses.errors", 10),
        "connection_retries_or_redispatches_spike": instability(),
        "session_saturation_approaching_limit": any(
            max(sessions[e]) * 100.0 / max(limits[e]) > 80 for e in sessions if e in limits),
    }


# GT channel values.
FAULTS = {
    "backend_has_no_active_servers": 0.0,
    "connection_errors_spike": 75.0,
    "server_health_check_failures": 50.0,
    "high_backend_connection_time": 750.0,
    "high_backend_queue_time": 75.0,
    "high_backend_response_time": 3750.0,
    "request_errors_spike": 75.0,
    "requests_queued_at_backend": 75.0,
    "response_errors_spike": 75.0,
    "connection_retries_or_redispatches_spike": 75.0,
    "session_saturation_approaching_limit": 85.0,
}


def _hits(breached):
    return sorted(k for k, hit in breached.items() if hit)


def test_fault_table_covers_every_impact_key():
    assert sorted(FAULTS) == sorted(_SPEC_FOR_ALERT_KEYS.impact_keys)


@pytest.mark.parametrize("key", sorted(FAULTS))
def test_key_alone_breaches_only_its_own_alert(key):
    for seed in SEEDS:
        series = _simulate({**DEFAULTS, key: FAULTS[key]}, seed, faulted=99, cycles=WINDOW + 2)
        assert _hits(_breached(series)) == [key], f"seed {seed}: {key} fired {_hits(_breached(series))}"


def test_healthy_run_breaches_nothing():
    for seed in SEEDS:
        series = _simulate(dict(DEFAULTS), seed, faulted=0, cycles=WINDOW + 2)
        assert not _hits(_breached(series)), f"seed {seed}: {_hits(_breached(series))}"


@pytest.mark.parametrize("key", sorted(FAULTS))
def test_alert_clears_after_the_fault_resolves(key):
    for seed in SEEDS:
        series = _simulate({**DEFAULTS, key: FAULTS[key]}, seed, faulted=6, cycles=6 + WINDOW + 2)
        assert not _hits(_breached(series)), (
            f"seed {seed}: {_hits(_breached(series))} still firing after {key} resolved"
        )


@pytest.mark.parametrize("key,breaching,healthy", [
    ("server_health_check_failures", 1.0, 0.0),
    ("requests_queued_at_backend", 1.0, 0.0),
    ("connection_errors_spike", 3.0, 0.0),
    ("session_saturation_approaching_limit", 85.0, 50.0),
    ("high_backend_queue_time", 11.0, 0.0),
])
def test_key_units_match_the_documented_thresholds(key, breaching, healthy):
    for seed in SEEDS:
        hit = _breached(_simulate({**DEFAULTS, key: breaching}, seed, 99, WINDOW + 2))[key]
        miss = _breached(_simulate({**DEFAULTS, key: healthy}, seed, 99, WINDOW + 2))[key]
        assert hit and not miss, f"seed {seed}: {key} breaching={hit} healthy={miss}"


# --- healthy request success rate (package SLO "Request success rate 99.5%") ---------

def _success_ratio(resp_errors: int, seed: int) -> float:
    rng = random.Random(seed)
    state = mod.EntityState(rng)
    before = dict(state.requests_by_status)
    for _ in range(10):
        state.tick(rng, resp_errors=resp_errors)
    inc = {code: state.requests_by_status[code] - before[code] for code in mod.STATUS_CODES}
    total = sum(inc.values())
    return (total - inc["5xx"]) / total


@pytest.mark.parametrize("seed", SEEDS)
def test_healthy_baseline_meets_request_success_rate_slo(seed):
    """Non-5xx share of requests stays >= 99.5% when no fault is active."""
    assert _success_ratio(0, seed) >= 0.995


@pytest.mark.parametrize("seed", SEEDS)
def test_response_errors_spike_breaks_request_success_rate(seed):
    """The response-errors fault still pushes the 5xx share well past the SLO budget."""
    assert _success_ratio(10, seed) < 0.995
