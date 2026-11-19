"""Technology-specific tests for the ``host_metrics`` generator.

Standard emit/registry tests live in tests/test_generators_emit.py
(parametrized over GENERATORS) and run for this generator automatically.
Add tests here only for behaviour that is special to host_metrics.
"""

from __future__ import annotations

import random

from generators.host_metrics.generator import (
    _generate_host_metrics,
    _with_infra_hosts,
    build_hostmetrics_bulk_docs,
)


def _fake_resource() -> dict:
    return {
        "attributes": [
            {"key": "host.name", "value": {"stringValue": "xborder-aws-host-01"}},
            {"key": "host.id", "value": {"stringValue": "i-test"}},
            {"key": "data_stream.dataset", "value": {"stringValue": "hostmetricsreceiver"}},
        ]
    }


def test_with_infra_hosts_adds_service_hostnames():
    base = [{"host.name": "xborder-aws-host-01", "host.id": "i-1", "cloud.instance.id": "i-1"}]
    out = _with_infra_hosts(base, "xborder")
    names = [h["host.name"] for h in out]
    assert names[0] == "xborder-aws-host-01"
    assert "xborder-kafka-broker" in names
    assert "xborder-redis-host" in names
    assert "xborder-nginx-01" in names
    assert "xborder-nginx-02" in names
    # Idempotent when already present
    assert len(_with_infra_hosts(out, "xborder")) == len(out)


def test_bulk_docs_cover_host_details_fields():
    rng = random.Random(1)
    from generators.host_metrics.generator import HostMetricState

    state = HostMetricState(2, 8_000_000_000, 100_000_000_000, rng)
    metrics_by_scope, _use = _generate_host_metrics(state, rng)
    docs = build_hostmetrics_bulk_docs(_fake_resource(), metrics_by_scope, timestamp_ms=1_700_000_000_000)

    metric_names = set()
    for doc in docs:
        metric_names.update(doc["metrics"])
        assert doc["data_stream"]["dataset"] == "hostmetricsreceiver.otel"
        assert doc["resource"]["attributes"]["host.name"] == "xborder-aws-host-01"
        assert "data_stream.dataset" not in doc["resource"]["attributes"]

    for required in (
        "system.cpu.utilization",
        "system.cpu.load_average.1m",
        "system.cpu.load_average.5m",
        "system.cpu.load_average.15m",
        "system.cpu.logical.count",
        "system.memory.utilization",
        "system.memory.usage",
        "system.filesystem.usage",
        "system.filesystem.utilization",
        "system.disk.operations",
        "system.disk.io",
        "system.disk.io_time",
        "system.network.io",
        "system.network.packets",
        "system.network.dropped",
        "system.network.errors",
        "system.network.connections",
        "system.processes.count",
        "system.processes.created",
    ):
        assert required in metric_names, f"missing {required}"

    # Attribute-bearing series keep filters Lens needs.
    cpu_states = {
        d["attributes"].get("state")
        for d in docs
        if "system.cpu.utilization" in d["metrics"]
    }
    assert "idle" in cpu_states and "user" in cpu_states

    mem_states = {
        d["attributes"].get("state")
        for d in docs
        if "system.memory.utilization" in d["metrics"]
    }
    assert "used" in mem_states

    disk_dirs = {
        d["attributes"].get("direction")
        for d in docs
        if "system.disk.operations" in d["metrics"]
    }
    assert disk_dirs == {"read", "write"}


# ── Chaos UI node disk fill (mirrors the Kubernetes node view) ────────────────

import threading  # noqa: E402

from generators.host_metrics.generator import HOSTS, run as _host_run  # noqa: E402


class _NScrapes(threading.Event):
    def __init__(self, n):
        super().__init__()
        self.n, self.done = n, 0

    def wait(self, timeout=None):
        self.done += 1
        if self.done >= self.n:
            self.set()
        return self.is_set()


class _HostRecorder:
    endpoint = "http://127.0.0.1:9"
    configured = False

    def __init__(self):
        self.fill: dict[str, float] = {}

    def _send(self, url, payload, label):
        for rm in payload["resourceMetrics"]:
            host = next(a["value"]["stringValue"] for a in rm["resource"]["attributes"] if a["key"] == "host.name")
            for sc in rm["scopeMetrics"]:
                for m in sc["metrics"]:
                    if m["name"] == "system.filesystem.utilization":
                        for dp in m["gauge"]["dataPoints"]:
                            self.fill[host] = max(self.fill.get(host, 0.0), float(dp["asDouble"]))

    def __getattr__(self, name):
        return lambda *args, **kwargs: None


class _HostChaos:
    def __init__(self, spikes):
        self._spikes = spikes

    def get_active_channels(self):
        return [1]

    def get_infra_spikes(self):
        return self._spikes


def _disk_fill_by_host(spikes, scrapes=45):
    aws = next(h for h in HOSTS if h.get("cloud.provider") == "aws")
    rec = _HostRecorder()
    _host_run(
        rec,  # type: ignore[arg-type]
        _NScrapes(scrapes),
        scenario_data={
            "hosts": [h for h in HOSTS],
            "k8s_clusters": [],
            "namespace": "nova7",
            "services": {"fuel-system": {"cloud_provider": "aws"}},
            "channel_registry": {1: {"name": "x", "affected_services": ["fuel-system"]}},
        },
        chaos_controller=_HostChaos(spikes),
    )
    return aws["host.name"], rec.fill


def test_node_disk_fill_slider_fills_only_the_channel_clouds_hosts():
    aws_host, fill = _disk_fill_by_host({"node_disk_fill_pct": 95})
    assert fill[aws_host] >= 0.9
    # hosts of the clouds the channel does not touch stay healthy
    # (kafka-broker / nginx-02 are cloned from the aws host, so they fill with it)
    assert fill["nova7-gcp-host-01"] < 0.85
    assert fill["nova7-azure-host-01"] < 0.85


def test_host_disk_stays_healthy_without_the_slider():
    _, fill = _disk_fill_by_host({})
    assert max(fill.values()) < 0.85
