"""Traefik tests for impact key apply + healthy baseline."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from generators.traefik_otel import SPEC
from generators.traefik_otel import generator as mod


class _FakeChaos:
    def get_active_channels(self):
        return ["ch1"]


class _RecordingBulk:
    """Stands in for ESBulkClient: records co-located bulk docs, no network."""

    def __init__(self):
        self.configured = True
        self.docs: list[dict] = []

    def send_bulk(self, _stream, docs, **_kwargs):
        self.docs.extend(docs)

    def close(self):
        return None


def _run_full(impact: dict | None = None) -> tuple[list[dict], list[dict]]:
    """Run one cycle; return (OTLP metrics, bulk docs)."""
    client = MagicMock()
    stop = MagicMock()
    stop.is_set.side_effect = [False, True]
    stop.wait.return_value = None
    registry = {}
    controller = None
    if impact is not None:
        controller = _FakeChaos()
        registry = {"ch1": {"generator_impact": {"traefik_otel": impact}}}
    with patch.object(mod, "_reset_counter_long_mapping"):
        bulk = _RecordingBulk()
        with patch.object(mod, "ESBulkClient", return_value=bulk):
            mod.run(client, stop, scenario_data={
                "namespace": "testns",
                "generator_config": {"chaos": "explicit"},
                "channel_registry": registry,
            }, chaos_controller=controller)
    metrics: list[dict] = []
    for call in client.send_metrics_scoped.call_args_list:
        metrics.extend(call.args[2])
    return metrics, bulk.docs


def _run(impact: dict | None = None) -> list[dict]:
    return _run_full(impact)[0]


def _fd_ratio(impact: dict | None = None) -> float:
    """open/max FDs from the co-located bulk doc (the pair is never sent over OTLP)."""
    _metrics, docs = _run_full(impact)
    fds = docs[-1]["metrics"]
    return fds["process_open_fds"] / fds["process_max_fds"]


def _gauge(metrics, name) -> float:
    vals = []
    for m in metrics:
        if m["name"] == name and "gauge" in m:
            dp = m["gauge"]["dataPoints"][0]
            vals.append(float(dp.get("asDouble", dp.get("asInt"))))
    if not vals:
        raise AssertionError(f"missing {name}")
    return max(vals)


def test_healthy_baseline_no_chaos():
    m = _run(None)
    assert _gauge(m, "go_goroutines") <= 2000
    assert _gauge(m, "process_resident_memory_bytes") <= 2 * 1024**3
    assert _fd_ratio(None) <= 0.8
    assert _gauge(m, "traefik_open_connections") <= 1000


def test_fd_pair_is_bulk_only_never_otlp():
    """OTLP would coerce whole-number floats to gauge_long and break the CASE() panel."""
    metrics, docs = _run_full(None)
    names = {m["name"] for m in metrics}
    assert not {"process_open_fds", "process_max_fds"} & names
    assert docs and {"process_open_fds", "process_max_fds"} <= set(docs[-1]["metrics"])


def test_fd_goroutine_memory_conns():
    assert _fd_ratio({"file_descriptor_pressure": 90}) > 0.8
    assert _gauge(_run({"high_goroutine_count": 3000}), "go_goroutines") > 2000
    assert _gauge(_run({"high_memory_usage": 3}), "process_resident_memory_bytes") > 2 * 1024**3
    assert _gauge(_run({"high_open_connections": 2500}), "traefik_open_connections") > 1000


def test_cert_expiry_and_error_rate_applied():
    import time
    m = _run({"tls_certificate_expiry_imminent": 5})
    not_after = _gauge(m, "traefik_tls_certs_not_after")
    days = (not_after - time.time()) / 86400
    assert days < 30
    m = _run({"high_5xx_error_rate_by_service": 50})
    assert any(x["name"] == "traefik_service_requests_total" for x in m)

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.traefik_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['file_descriptor_pressure', 'high_5xx_error_rate_by_entrypoint', 'high_5xx_error_rate_by_router', 'high_5xx_error_rate_by_service', 'high_goroutine_count', 'high_memory_usage', 'high_open_connections', 'stale_config_reload_success', 'tls_certificate_expiry_imminent']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


# --- package SLO baselines -----------------------------------------------------------
import random  # noqa: E402

import pytest  # noqa: E402


@pytest.mark.parametrize("seed", range(6))
def test_healthy_service_availability_meets_slo(seed):
    """[Traefik OTel] Service availability 99.5%: non-5xx share of service requests."""
    rng = random.Random(seed)
    state = mod._TraefikState(rng)
    before = {k: c.value for k, c in state.service_reqs.items()}
    for _ in range(6):  # 90 s: even a short window holds the objective
        state.tick(rng, load=1.0, error_rate=0.0)
    inc = {k: c.value - before[k] for k, c in state.service_reqs.items()}
    total = sum(inc.values())
    bad = sum(v for (_svc, code), v in inc.items() if code.startswith("5"))
    assert total > 0 and (total - bad) / total >= 0.995


def test_5xx_fault_still_breaks_service_availability():
    m = _run({"high_5xx_error_rate_by_service": 500})
    svc = [x for x in m if x["name"] == "traefik_service_requests_total"]
    codes = {a["value"]["stringValue"] for x in svc
             for a in x["gauge"]["dataPoints"][0]["attributes"] if a["key"] == "code"}
    assert codes and all(c.startswith("5") for c in codes)


def _duration_points(metrics):
    return [dp for m in metrics if m["name"] == "traefik_service_request_duration_seconds"
            for dp in m["histogram"]["dataPoints"]]


def test_service_duration_histogram_per_service():
    """The latency SLO reads traefik_service_request_duration_seconds as a histogram."""
    points = _duration_points(_run(None))
    services = {a["value"]["stringValue"] for dp in points
                for a in dp["attributes"] if a["key"] == "service"}
    assert services == set(mod._SERVICES)
    for dp in points:
        assert dp["explicitBounds"] == mod.DURATION_BOUNDS
        assert len(dp["bucketCounts"]) == len(dp["explicitBounds"]) + 1
        assert sum(dp["bucketCounts"]) == dp["count"] > 0


@pytest.mark.parametrize("seed", range(6))
def test_healthy_latency_meets_200ms_slo(seed):
    """[Traefik OTel] Service latency under 200 ms 99.5%: share of requests <= 0.2 s."""
    samples = mod.latency_samples(random.Random(seed), 5_000)
    hist = mod.build_duration_histogram(samples, {"service": "x"}, "0")
    dp = hist["histogram"]["dataPoints"][0]
    good_buckets = mod.DURATION_BOUNDS.index(0.2) + 1  # buckets with upper bound <= 0.2
    assert sum(dp["bucketCounts"][:good_buckets]) / dp["count"] >= 0.995
