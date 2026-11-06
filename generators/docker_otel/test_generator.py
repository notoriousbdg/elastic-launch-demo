"""Docker-specific tests for the docker_otel generator."""
from __future__ import annotations

import random

from generators.docker_otel import SPEC
from generators.docker_otel.generator import ContainerState, _generate_container_metrics


def test_cpu_utilization_does_not_decay_to_floor():
    """business_load must scale emit util, not compound into state each tick.

    The old ``util *= load`` walk collapsed to 0.001 within a few ticks and made
    Overview / Container Stats CPU charts look like flat zeros.
    """
    rng = random.Random(1)
    state = ContainerState(rng)
    for _ in range(40):
        state.tick(rng, None)
    assert state._cpu_util >= 0.01
    # Under any realistic load curve the walk should stay visibly above the old floor.
    assert state._cpu_util > 0.005


def test_cpu_utilization_emitted_in_visible_range():
    rng = random.Random(2)
    state = ContainerState(rng)
    resource = {"attributes": []}
    metrics = _generate_container_metrics(
        "abc", "web", "nginx:latest", state, resource, rng, None,
    )
    util = next(m for m in metrics if m["name"] == "container.cpu.utilization")
    value = util["gauge"]["dataPoints"][0]["asDouble"]
    assert 0.01 <= value <= 0.99


def test_cpu_counters_advance_with_utilization():
    rng = random.Random(3)
    state = ContainerState(rng)
    before = (state.cpu_kernel, state.cpu_user)
    state.tick(rng, None)
    assert state.cpu_kernel > before[0]
    assert state.cpu_user > before[1]
    # Meaningful RATE: at least ~1ms of CPU time per tick at healthy util.
    assert (state.cpu_kernel - before[0]) + (state.cpu_user - before[1]) > 1_000_000


def test_cpu_throttle_rate_key():
    rng = random.Random(4)
    state = ContainerState(rng)
    for _ in range(10):
        state.tick(rng, {"cpu_throttle_rate": 0.5})
    assert state.total_periods > 0
    assert state.throttled_periods / state.total_periods > 0.2


def test_spec_wants_chaos():
    assert SPEC.wants_chaos is True
    assert "cpu_utilization_multiplier" in SPEC.impact_keys


# --- one timestamp per scrape cycle ---------------------------------------------------
# The package dashboards take max(counter) across every container in each time bucket and
# differentiate it. If one scrape cycle straddles a bucket boundary (containers and metrics
# each stamped with their own "now"), a bucket holds a single container's much lower counter
# and the next bucket's rate spikes by 13-28x. A scrape must land in one bucket or the other.

from unittest.mock import MagicMock  # noqa: E402

from generators.docker_otel import generator as _docker_gen  # noqa: E402


def _cycles(n_cycles=3):
    client = MagicMock()
    stop = MagicMock()
    stop.is_set.side_effect = [False] * n_cycles + [True]
    stop.wait.return_value = None
    _docker_gen.run(
        client, stop,
        scenario_data={"namespace": "t", "generator_config": {"chaos": "explicit"}, "channel_registry": {}},
    )
    containers = 4
    calls = client.send_metrics_scoped.call_args_list
    assert len(calls) == n_cycles * containers
    for i in range(n_cycles):
        yield [c.args[2] for c in calls[i * containers:(i + 1) * containers]]


def test_every_datapoint_in_a_scrape_cycle_shares_one_timestamp():
    for cycle in _cycles():
        stamps = {
            dp["timeUnixNano"]
            for metrics in cycle
            for m in metrics
            for dp in (m.get("gauge") or m["sum"])["dataPoints"]
        }
        assert len(stamps) == 1, f"{len(stamps)} distinct timestamps in one scrape cycle"


def test_consecutive_cycles_get_increasing_timestamps():
    firsts = [
        int((cycle[0][0].get("gauge") or cycle[0][0]["sum"])["dataPoints"][0]["timeUnixNano"])
        for cycle in _cycles(4)
    ]
    assert firsts == sorted(firsts) and len(set(firsts)) == len(firsts)
