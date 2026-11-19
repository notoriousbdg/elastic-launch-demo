"""Technology-specific tests for the ``kubernetes_otel`` generator."""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "tests"))

from generators.kubernetes_otel import SPEC
from generators.kubernetes_otel.generator import (  # noqa: E402
    _build_container_resource,
    build_container_status_doc,
    build_pod_util_doc,
    _generate_daemonset_metrics,
    _generate_deployment_metrics,
    _generate_hpa_metrics,
    _generate_job_metrics,
    _generate_node_metrics_cluster,
    _generate_node_metrics_kubelet,
    _generate_pod_metrics,
    _init_pod_data,
    CLUSTERS,
    K8sState,
)
import random


def _attr_map(resource: dict) -> dict:
    return {a["key"]: a["value"].get("stringValue") for a in resource["attributes"]}


def test_container_resource_includes_deployment_for_restart_panels():
    cluster = CLUSTERS[0]
    pod_data = _init_pod_data(cluster, namespace="nova7")
    svc = cluster["services"][0]
    attrs = _attr_map(_build_container_resource(svc, pod_data, cluster, "nova7"))

    assert attrs["k8s.deployment.name"] == pod_data["pods"][svc]["deployment_name"]
    assert attrs["k8s.replicaset.name"] == pod_data["pods"][svc]["replicaset_name"]
    assert attrs["k8s.container.name"] == f"{svc}-container"
    assert attrs["k8s.pod.uid"] == pod_data["pods"][svc]["pod_uid"]


def test_container_status_bulk_doc_carries_oomkilled_reason():
    """OOTB OOM alert filters k8sclusterreceiver on last_terminated_reason."""
    cluster = CLUSTERS[0]
    pod_data = _init_pod_data(cluster, namespace="xborder")
    svc = cluster["services"][0]
    res = _build_container_resource(
        svc, pod_data, cluster, "xborder", last_terminated_reason="OOMKilled",
    )
    doc = build_container_status_doc(res, restarts=3, timestamp_ms=1_700_000_000_000)
    assert doc["data_stream"]["dataset"] == "k8sclusterreceiver.otel"
    assert doc["metrics"]["k8s.container.restarts"] == 3
    assert (
        doc["resource"]["attributes"]["k8s.container.status.last_terminated_reason"]
        == "OOMKilled"
    )


def test_pod_spike_50_pct_stays_under_container_alert_threshold():
    """Live OOTB container CPU/mem alerts fire at avg_util > 0.9 — 50% must stay quiet."""
    rng = random.Random(0)
    cluster = CLUSTERS[0]
    state = K8sState(rng, cluster["services"], [])
    metrics = _generate_pod_metrics(
        cluster["services"][0], state, rng,
        mem_util_override=0.50, cpu_util_override=0.50,
    )
    assert _metric_val(metrics, "k8s.container.cpu_limit_utilization") == pytest.approx(0.50)
    assert _metric_val(metrics, "k8s.container.memory_limit_utilization") == pytest.approx(0.50)
    assert _metric_val(metrics, "k8s.pod.cpu.node.utilization") == pytest.approx(0.50)
    assert _metric_val(metrics, "k8s.container.cpu_limit_utilization") < 0.9
    assert _metric_val(metrics, "k8s.container.memory_limit_utilization") < 0.9


def test_pod_spike_91_pct_breaches_container_alert_threshold():
    """Slider at/above 0.9 must breach Container CPU throttling / memory near limit."""
    rng = random.Random(1)
    cluster = CLUSTERS[0]
    state = K8sState(rng, cluster["services"], [])
    metrics = _generate_pod_metrics(
        cluster["services"][0], state, rng,
        mem_util_override=0.91, cpu_util_override=0.91,
    )
    assert _metric_val(metrics, "k8s.container.cpu_limit_utilization") > 0.9
    assert _metric_val(metrics, "k8s.container.memory_limit_utilization") > 0.9


def test_host_spike_50_pct_node_stays_under_saturation():
    """Node CPU >0.9 and node mem >0.85 — 50% host spike stays under both."""
    rng = random.Random(2)
    metrics = _generate_node_metrics_kubelet(
        rng, node_name="node-1",
        profiles={"node": {"node-1": {"allocatable_cpu": 4.0, "allocatable_memory": 16_000_000_000}}},
        cpu_util_override=0.50, mem_util_override=0.50,
    )
    util_cpu = _metric_val(metrics, "k8s.node.cpu.utilization")
    util_mem = _metric_val(metrics, "k8s.node.memory.utilization")
    assert util_cpu == pytest.approx(0.50)
    assert util_mem == pytest.approx(0.50)
    assert util_cpu < 0.9
    assert util_mem < 0.85
    # Ratio alert uses usage/allocatable — usage cores = alloc * util
    assert _metric_val(metrics, "k8s.node.cpu.usage") / 4.0 < 0.9


def test_host_spike_crosses_node_cpu_and_mem_thresholds():
    """91% CPU and 86% mem cross live Node saturation floors (0.9 / 0.85)."""
    rng = random.Random(3)
    metrics = _generate_node_metrics_kubelet(
        rng, node_name="node-1",
        profiles={"node": {"node-1": {"allocatable_cpu": 4.0, "allocatable_memory": 16_000_000_000}}},
        cpu_util_override=0.91, mem_util_override=0.86,
    )
    assert _metric_val(metrics, "k8s.node.cpu.utilization") > 0.9
    assert _metric_val(metrics, "k8s.node.memory.utilization") > 0.85
    ws = _metric_val(metrics, "k8s.node.memory.working_set")
    assert ws / 16_000_000_000 > 0.85


def test_pod_util_bulk_doc_for_ml_fields():
    cluster = CLUSTERS[0]
    pod_data = _init_pod_data(cluster, namespace="xborder")
    svc = cluster["services"][0]
    from generators.kubernetes_otel.generator import _build_pod_resource
    res = _build_pod_resource(svc, pod_data, cluster, "xborder")
    doc = build_pod_util_doc(
        res, cpu_node_util=0.5, mem_working_set=1000,
        cpu_limit_util=0.5, mem_limit_util=0.5, timestamp_ms=1,
    )
    assert doc["metrics"]["k8s.pod.cpu.node.utilization"] == 0.5
    assert doc["metrics"]["k8s.pod.memory.working_set"] == 1000


def _metric_val(metrics, name):
    for m in metrics:
        if m["name"] == name and "gauge" in m:
            dp = m["gauge"]["dataPoints"][0]
            return float(dp.get("asDouble", dp.get("asInt")))
    raise AssertionError(f"missing {name}")


def test_node_cpu_saturation_alert_stays_quiet_without_chaos():
    """OOTB 'Node CPU saturation' alert fires on avg_cpu (cores) > 3.6.

    allocatable_cpu varies 2.0-8.0 cores per node, so a naive utilization
    ratio can push large nodes' absolute core usage over the threshold.
    """
    for alloc_cpu in (2.0, 4.0, 6.0, 8.0):
        for seed in range(10):
            rng = random.Random(seed)
            metrics = _generate_node_metrics_kubelet(
                rng, node_name="node-1",
                profiles={"node": {"node-1": {"allocatable_cpu": alloc_cpu}}},
            )
            cpu_usage_ns = _metric_val(metrics, "k8s.node.cpu.usage")
            avg_cpu_cores = cpu_usage_ns / 1_000_000_000
            assert avg_cpu_cores < 3.6, f"alloc={alloc_cpu} seed={seed} avg_cpu={avg_cpu_cores}"


def test_node_memory_saturation_alert_stays_quiet_without_chaos():
    """OOTB 'Node memory saturation' alert fires on avg_memory (bytes) > 13743895347."""
    for alloc_mem in (8_000_000_000, 16_000_000_000, 32_000_000_000):
        for seed in range(10):
            rng = random.Random(seed)
            metrics = _generate_node_metrics_kubelet(
                rng, node_name="node-1",
                profiles={"node": {"node-1": {"allocatable_memory": alloc_mem}}},
            )
            mem_usage = _metric_val(metrics, "k8s.node.memory.usage")
            assert mem_usage < 13_743_895_347, f"alloc={alloc_mem} seed={seed} usage={mem_usage}"


def test_healthy_node_conditions_are_clear():
    metrics = _generate_node_metrics_cluster(random.Random(0))
    by_name = {m["name"]: m for m in metrics}
    assert by_name["k8s.node.condition_ready"]["gauge"]["dataPoints"][0]["asInt"] == "1"
    assert by_name["k8s.node.condition_memory_pressure"]["gauge"]["dataPoints"][0]["asInt"] == "0"
    assert by_name["k8s.node.condition_disk_pressure"]["gauge"]["dataPoints"][0]["asInt"] == "0"


def test_job_failed_only_when_chaos_key_set():
    healthy = _generate_job_metrics(random.Random(0), failed_pods=0)
    assert {m["name"]: m for m in healthy}["k8s.job.failed_pods"]["gauge"]["dataPoints"][0]["asInt"] == "0"
    failed = _generate_job_metrics(random.Random(0), failed_pods=2)
    assert {m["name"]: m for m in failed}["k8s.job.failed_pods"]["gauge"]["dataPoints"][0]["asInt"] == "2"


def test_hpa_at_max_key():
    metrics = _generate_hpa_metrics(random.Random(1), at_max=True)
    by_name = {m["name"]: int(m["gauge"]["dataPoints"][0]["asInt"]) for m in metrics}
    assert by_name["k8s.hpa.desired_replicas"] >= by_name["k8s.hpa.max_replicas"]


def test_pod_metrics_emit_container_limit_utilization():
    rng = random.Random(0)
    cluster = CLUSTERS[0]
    state = K8sState(rng, cluster["services"], [])
    metrics = _generate_pod_metrics(
        cluster["services"][0], state, rng,
        mem_util_override=0.95, cpu_util_override=0.96,
    )
    names = {m["name"] for m in metrics}
    assert "k8s.container.cpu_limit_utilization" in names
    assert "k8s.container.memory_limit_utilization" in names
    by_name = {m["name"]: m for m in metrics}
    assert by_name["k8s.container.cpu_limit_utilization"]["gauge"]["dataPoints"][0]["asDouble"] == pytest.approx(0.96)
    assert by_name["k8s.container.memory_limit_utilization"]["gauge"]["dataPoints"][0]["asDouble"] == pytest.approx(0.95)


def test_daemonset_and_deployment_respect_ready_fraction():
    ds = _generate_daemonset_metrics(
        random.Random(0), 5, ready_fraction=0.0, misscheduled=2,
    )
    by_name = {m["name"]: int(m["gauge"]["dataPoints"][0]["asInt"]) for m in ds}
    assert by_name["k8s.daemonset.misscheduled_nodes"] == 2
    assert by_name["k8s.daemonset.ready_nodes"] < by_name["k8s.daemonset.desired_scheduled_nodes"]

    dep = _generate_deployment_metrics(random.Random(0), ready_fraction=0.0)
    by_name = {m["name"]: int(m["gauge"]["dataPoints"][0]["asInt"]) for m in dep}
    assert by_name["k8s.deployment.available"] < by_name["k8s.deployment.desired"]


def test_cluster_logs_event_record_has_body_text_pod_and_node():
    """Cluster Detail Cluster logs KEEP body.*, k8s.pod.name, k8s.node.name."""
    from generators.kubernetes_otel.generator import (
        _build_k8s_event_log_record,
        _k8s_event_resource_attrs,
    )

    record, _ = _build_k8s_event_log_record(
        pod_name="api-abc123-xyz",
        node_name="ip-10-0-1-5.compute.internal",
        namespace="inttest",
        reason="FailedScheduling",
        message="0/3 nodes are available: 3 Insufficient memory.",
    )
    assert record["body"]["stringValue"]
    attrs = {a["key"]: a["value"].get("stringValue") for a in record["attributes"]}
    assert attrs["k8s.pod.name"] == "api-abc123-xyz"
    assert attrs["k8s.node.name"] == "ip-10-0-1-5.compute.internal"

    res = _k8s_event_resource_attrs(
        {"name": "c1", "provider": "gcp", "platform": "gcp_gke", "region": "us-central1"},
        "inttest",
        pod_name="api-abc123-xyz",
        node_name="ip-10-0-1-5.compute.internal",
    )
    assert res["k8s.pod.name"] == "api-abc123-xyz"
    assert res["k8s.node.name"] == "ip-10-0-1-5.compute.internal"
    assert res["k8s.cluster.name"] == "c1"


# --- 1:1 alert impact_keys coverage (auto) ---
from generators.kubernetes_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['container_cpu_throttling', 'container_memory_near_limit', 'daemonset_mis_scheduled_or_not_ready', 'deployment_unavailable_replicas', 'hpa_at_max_replicas', 'job_failures', 'node_cpu_saturation', 'node_disk_pressure', 'node_filesystem_saturation', 'node_memory_pressure', 'node_memory_saturation', 'node_not_ready', 'oomkilled_containers', 'persistent_volume_space_low', 'pod_crashloopbackoff', 'pods_in_failed_phase', 'pods_stuck_in_pending_phase', 'statefulset_replicas_not_ready']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


def test_succeeded_job_pod_phase_is_non_running():
    """Overview non-running panel needs phase != 2 without Pending/Failed."""
    from generators.kubernetes_otel.generator import (
        _build_job_pod_phase_resource,
        _generate_succeeded_pod_phase_metric,
    )
    cluster = {
        "name": "demo-aws-k8s",
        "provider": "aws",
        "platform": "eks",
        "region": "us-east-1",
        "services": ["api"],
        "os_description": "linux",
    }
    res = _build_job_pod_phase_resource("banking-db-backup", cluster, "banking")
    keys = {a["key"] for a in res["attributes"]}
    assert "k8s.pod.name" in keys
    assert "k8s.pod.uid" in keys
    assert "k8s.cluster.name" in keys
    metrics = _generate_succeeded_pod_phase_metric()
    assert metrics[0]["name"] == "k8s.pod.phase"
    assert metrics[0]["gauge"]["dataPoints"][0]["asInt"] == "3"  # Succeeded


# --- metrics the package dashboards read (kubernetes_otel@2.6.1) -----------------------
# Pod Detail reads container.cpu.usage / container.memory.working_set / k8s.container.ready;
# Node Detail reads k8s.node.memory.rss and k8s.node.network.io (per direction). A missing
# column makes the whole panel fail with "Unknown column".

from generators.kubernetes_otel.generator import _generate_container_metrics  # noqa: E402


def _dp(metric):
    return (metric.get("gauge") or metric["sum"])["dataPoints"][0]


def _dp_attrs(metric):
    return {a["key"]: next(iter(a["value"].values())) for a in _dp(metric).get("attributes", [])}


def test_container_kubelet_metrics_use_the_names_the_package_reads():
    rng = random.Random(0)
    cluster = CLUSTERS[0]
    state = K8sState(rng, cluster["services"], [])
    metrics = _generate_pod_metrics(cluster["services"][0], state, rng)
    by_name = {m["name"]: m for m in metrics}
    for name in ("container.cpu.usage", "container.memory.working_set"):
        assert name in by_name, f"Pod Detail panels read {name}"
        assert "k8s.container.name" in _dp_attrs(by_name[name])
    # The package never reads the k8s.-prefixed spellings the generator used to emit.
    assert "k8s.container.cpu.usage" not in by_name
    assert "k8s.container.memory.working_set" not in by_name


def test_node_kubelet_emits_rss_and_per_direction_network_io():
    rng = random.Random(0)
    cluster = CLUSTERS[0]
    node = "node-a"
    state = K8sState(rng, cluster["services"], [node])
    first = _generate_node_metrics_kubelet(rng, node, None, state)
    names = [m["name"] for m in first]
    assert "k8s.node.network.rx" not in names and "k8s.node.network.tx" not in names

    assert _metric_val(first, "k8s.node.memory.rss") <= _metric_val(first, "k8s.node.memory.working_set")

    def io(metrics):
        series = [m for m in metrics if m["name"] == "k8s.node.network.io"]
        assert all("sum" in m and m["sum"]["isMonotonic"] for m in series)
        return {_dp_attrs(m)["direction"]: int(_dp(m)["asInt"]) for m in series}

    before = io(first)
    assert set(before) == {"receive", "transmit"}
    state.tick()
    after = io(_generate_node_metrics_kubelet(rng, node, None, state))
    for direction in before:
        assert after[direction] > before[direction], f"{direction} counter must only grow (RATE)"


def test_container_ready_gauge_is_one_when_healthy_and_zero_when_not():
    rng = random.Random(0)
    cluster = CLUSTERS[0]
    state = K8sState(rng, cluster["services"], [])
    svc = cluster["services"][0]
    healthy = {m["name"]: m for m in _generate_container_metrics(svc, state, rng)}
    assert int(_dp(healthy["k8s.container.ready"])["asInt"]) == 1
    down = {m["name"]: m for m in _generate_container_metrics(svc, state, rng, ready=False)}
    assert int(_dp(down["k8s.container.ready"])["asInt"]) == 0


# ── Channel generator_impact scope ────────────────────────────────────────────
# A channel that names kubernetes_otel gets every unset key filled with its 0.0
# default. Those defaults must stay healthy, and the keys that are set must only
# touch the channel's affected services; the infra sliders keep driving memory.

import threading  # noqa: E402

from generators.kubernetes_otel.generator import run as _k8s_run  # noqa: E402


class _OneScrape(threading.Event):
    def wait(self, timeout=None):
        self.set()
        return True


class _Recorder:
    endpoint = "http://127.0.0.1:9"

    def __init__(self):
        self.resource_metrics: list[dict] = []

    def _send(self, url, payload, label):
        self.resource_metrics.extend(payload.get("resourceMetrics", []))

    def __getattr__(self, name):
        return lambda *args, **kwargs: None


class _ActiveChannel:
    def __init__(self, spikes=None):
        self._spikes = spikes or {}

    def get_active_channels(self):
        return [1]

    def get_infra_spikes(self):
        return self._spikes


def _pod_memory_util(impact: dict, spikes: dict | None = None, *, target: str = "fuel-system"):
    """Run one scrape with channel 1 active; return {service: container memory limit util}."""
    services = [s for c in CLUSTERS for s in c["services"]]
    registry = {1: {
        "name": "k8s scope test",
        "affected_services": [target],
        "generator_impact": {"kubernetes_otel": impact},
    }}
    client = _Recorder()
    _k8s_run(
        client,  # type: ignore[arg-type]
        _OneScrape(),
        scenario_data={
            "k8s_clusters": CLUSTERS,
            "namespace": "k8sscope",
            "services": {s: {} for s in services},
            "channel_registry": registry,
        },
        chaos_controller=_ActiveChannel(spikes),
    )
    util: dict[str, float] = {}
    for rm in client.resource_metrics:
        deployment = _attr_map(rm["resource"]).get("k8s.deployment.name") or ""
        for scope in rm["scopeMetrics"]:
            for m in scope["metrics"]:
                if m["name"] == "k8s.container.memory_limit_utilization":
                    svc = next((s for s in services if deployment.startswith(s)), None)
                    if svc:
                        util[svc] = float(_dp(m)["asDouble"])
    return util


def test_channel_without_memory_key_leaves_pod_memory_healthy():
    util = _pod_memory_util({"pod_crashloopbackoff": 0.6})
    assert util, "no pod memory utilization emitted"
    assert max(util.values()) < 0.9


def test_channel_memory_key_only_touches_affected_services():
    util = _pod_memory_util({"container_memory_near_limit": 0.97}, target="fuel-system")
    assert util["fuel-system"] > 0.9
    others = {s: v for s, v in util.items() if s != "fuel-system"}
    assert others and max(others.values()) < 0.9


def test_small_memory_key_still_breaches_the_strict_threshold():
    """OOTB is AVG(util) > 0.9; a key at or under 0.9 used to clamp to exactly 0.9."""
    util = _pod_memory_util({"container_memory_near_limit": 0.5}, target="fuel-system")
    assert util["fuel-system"] > 0.9


def test_memory_slider_still_drives_pod_memory_under_a_k8s_channel():
    util = _pod_memory_util(
        {"pod_crashloopbackoff": 0.6}, {"k8s_pod_memory_intensity": 95}, target="fuel-system",
    )
    assert util["fuel-system"] > 0.9


# ── Chaos UI channel augmentations ───────────────────────────────────────────

import re  # noqa: E402
import types  # noqa: E402

import generators.kubernetes_otel.generator as _k8s_gen  # noqa: E402
from generators.kubernetes_otel.generator import _not_ready_stage  # noqa: E402

_AUG_TARGET = "fuel-system"


class _Scrapes(threading.Event):
    """Runs ``n`` scrapes; each wait() advances the fake clock by ``step`` seconds."""

    def __init__(self, n: int, step: float, clock: list):
        super().__init__()
        self.n, self.step, self.clock, self.done = n, step, clock, 0

    def wait(self, timeout=None):
        self.done += 1
        self.clock[0] += self.step
        if self.done >= self.n:
            self.set()
        return self.is_set()


class _AugRecorder:
    endpoint = "http://127.0.0.1:9"

    def __init__(self):
        self.scrapes: list[dict] = []
        self.event = None

    def _scrape(self) -> dict:
        idx = self.event.done
        while len(self.scrapes) <= idx:
            self.scrapes.append({"metrics": [], "events": []})
        return self.scrapes[idx]

    def _send(self, url, payload, label):
        scrape = self._scrape()
        scrape["metrics"].extend(payload.get("resourceMetrics", []))
        for rl in payload.get("resourceLogs", []):
            for sl in rl["scopeLogs"]:
                for rec in sl["logRecords"]:
                    scrape["events"].append({
                        a["key"]: next(iter(a["value"].values())) for a in rec["attributes"]
                    })

    def __getattr__(self, name):
        return lambda *args, **kwargs: None


class _Channels:
    def __init__(self, spikes, active=(1,)):
        self._spikes, self._active = spikes, list(active)

    def get_active_channels(self):
        return list(self._active)

    def get_infra_spikes(self):
        return self._spikes


def _aug_run(spikes, *, scrapes=1, step=15.0, active=(1,), target=_AUG_TARGET, monkeypatch=None):
    services = [s for c in CLUSTERS for s in c["services"]]
    clock = [1_000_000.0]
    if monkeypatch is not None:
        monkeypatch.setattr(_k8s_gen, "time", types.SimpleNamespace(time=lambda: clock[0]))
    rec = _AugRecorder()
    rec.event = _Scrapes(scrapes, step, clock)
    _k8s_run(
        rec,  # type: ignore[arg-type]
        rec.event,
        scenario_data={
            "k8s_clusters": CLUSTERS,
            "namespace": "k8saug",
            "services": {s: {} for s in services},
            "channel_registry": {1: {"name": "aug", "affected_services": [target]}},
        },
        chaos_controller=_Channels(spikes, active),
    )
    return rec


def _gauge_by_svc(scrape, metric_name, services):
    """{svc: value} for k8sclusterreceiver resources named <svc>-deployment.

    The newest datapoint per service is the live value.
    """
    newest: dict[str, tuple[int, float]] = {}
    for rm in scrape["metrics"]:
        dep = _attr_map(rm["resource"]).get("k8s.deployment.name") or ""
        svc = dep[: -len("-deployment")] if dep.endswith("-deployment") else None
        if svc not in services:
            continue
        for scope in rm["scopeMetrics"]:
            for m in scope["metrics"]:
                if m["name"] == metric_name:
                    dp = _dp(m)
                    ts = int(dp["timeUnixNano"])
                    if svc not in newest or ts >= newest[svc][0]:
                        newest[svc] = (ts, float(dp.get("asDouble", dp.get("asInt"))))
    return {svc: v for svc, (_, v) in newest.items()}


_ALL_SVCS = {s for c in CLUSTERS for s in c["services"]}


def _events(scrape, reason, pod_prefix=_AUG_TARGET):
    return [
        e for e in scrape["events"]
        if e.get("k8s.event.reason") == reason
        and re.match(rf"^{re.escape(pod_prefix)}-[0-9a-f]{{8}}-", e.get("k8s.pod.name", ""))
    ]


def test_not_ready_stage_cascade():
    assert _not_ready_stage(None, False) == "ready"
    assert _not_ready_stage(0, False) == "down"
    assert _not_ready_stage(39, False) == "down"
    assert _not_ready_stage(40, False) == "unknown"
    assert _not_ready_stage(299, False) == "unknown"
    assert _not_ready_stage(300, False) == "rescheduled"
    assert _not_ready_stage(300, True) == "pending"


def test_workload_counts_are_stable_without_faults(monkeypatch):
    rec = _aug_run({}, scrapes=4, monkeypatch=monkeypatch)
    desired = [_gauge_by_svc(s, "k8s.deployment.desired", _ALL_SVCS) for s in rec.scrapes]
    assert desired[0] and all(d == desired[0] for d in desired)
    avail = _gauge_by_svc(rec.scrapes[-1], "k8s.deployment.available", _ALL_SVCS)
    assert avail == desired[0]


def test_crashloop_restarts_on_backoff_schedule_for_channel_services_only(monkeypatch):
    rec = _aug_run({"k8s_crashloop": 1}, scrapes=6, step=15.0, monkeypatch=monkeypatch)
    last = rec.scrapes[-1]
    restarts = _gauge_by_svc(last, "k8s.container.restarts", _ALL_SVCS)
    assert restarts[_AUG_TARGET] >= 2
    # 10s, 20s, 40s back-off: 6 scrapes 15s apart cannot restart on every scrape
    assert restarts[_AUG_TARGET] < 6
    assert all(v == 0 for s, v in restarts.items() if s != _AUG_TARGET)
    assert _gauge_by_svc(last, "k8s.container.ready", _ALL_SVCS)[_AUG_TARGET] == 0
    desired = _gauge_by_svc(last, "k8s.deployment.desired", _ALL_SVCS)
    avail = _gauge_by_svc(last, "k8s.deployment.available", _ALL_SVCS)
    assert avail[_AUG_TARGET] < desired[_AUG_TARGET]
    assert _events(rec.scrapes[0], "BackOff"), "BackOff event must land on the crash-looping pod"


def test_crashloop_clears_when_toggled_off(monkeypatch):
    spikes = {"k8s_crashloop": 1}
    rec = _aug_run(spikes, scrapes=1, monkeypatch=monkeypatch)
    spikes["k8s_crashloop"] = 0
    off = _aug_run({}, scrapes=1, monkeypatch=monkeypatch)
    assert _gauge_by_svc(off.scrapes[0], "k8s.container.restarts", _ALL_SVCS)[_AUG_TARGET] == 0
    assert _gauge_by_svc(off.scrapes[0], "k8s.container.ready", _ALL_SVCS)[_AUG_TARGET] == 1
    assert _gauge_by_svc(rec.scrapes[0], "k8s.container.ready", _ALL_SVCS)[_AUG_TARGET] == 0


def test_readiness_failure_drops_ready_without_restarts(monkeypatch):
    rec = _aug_run({"k8s_readiness_fail": 1}, scrapes=3, monkeypatch=monkeypatch)
    last = rec.scrapes[-1]
    assert _gauge_by_svc(last, "k8s.container.restarts", _ALL_SVCS)[_AUG_TARGET] == 0
    assert _gauge_by_svc(last, "k8s.container.ready", _ALL_SVCS)[_AUG_TARGET] == 0
    desired = _gauge_by_svc(last, "k8s.deployment.desired", _ALL_SVCS)
    avail = _gauge_by_svc(last, "k8s.deployment.available", _ALL_SVCS)
    assert avail[_AUG_TARGET] < desired[_AUG_TARGET]
    unhealthy = _events(rec.scrapes[0], "Unhealthy")
    assert any("Readiness probe failed" in e["k8s.event.message"] for e in unhealthy)


def test_liveness_failure_restarts_and_logs_probe_event(monkeypatch):
    rec = _aug_run({"k8s_liveness_fail": 1}, scrapes=3, monkeypatch=monkeypatch)
    assert _gauge_by_svc(rec.scrapes[-1], "k8s.container.restarts", _ALL_SVCS)[_AUG_TARGET] >= 1
    unhealthy = _events(rec.scrapes[0], "Unhealthy")
    assert any("Liveness probe failed" in e["k8s.event.message"] for e in unhealthy)


def test_augmentations_do_nothing_without_an_active_channel(monkeypatch):
    spikes = {
        "k8s_crashloop": 1, "k8s_readiness_fail": 1, "k8s_node_not_ready": 1,
        "k8s_hpa_capped": 1, "node_disk_fill_pct": 95, "k8s_pvc_fill_pct": 95,
    }
    rec = _aug_run(spikes, scrapes=3, active=(), monkeypatch=monkeypatch)
    last = rec.scrapes[-1]
    assert all(v == 0 for v in _gauge_by_svc(last, "k8s.container.restarts", _ALL_SVCS).values())
    assert all(v == 1 for v in _gauge_by_svc(last, "k8s.container.ready", _ALL_SVCS).values())
    assert set(_node_conditions(last).values()) == {1}
    assert set(_pod_phases(last).values()) <= {2, 3}  # Running, plus Succeeded job pods
    assert _kubelet_nodes(last) == set(_node_conditions(last))
    desired = _gauge_by_svc(rec.scrapes[0], "k8s.deployment.desired", _ALL_SVCS)
    assert _gauge_by_svc(last, "k8s.deployment.desired", _ALL_SVCS) == desired  # HPA never scaled out


def _node_conditions(scrape):
    """{node: condition_ready} from the k8sclusterreceiver node resources."""
    out = {}
    for rm in scrape["metrics"]:
        node = _attr_map(rm["resource"]).get("k8s.node.name")
        for scope in rm["scopeMetrics"]:
            for m in scope["metrics"]:
                if m["name"] == "k8s.node.condition_ready" and node:
                    out[node] = int(_dp(m)["asInt"])
    return out


def _kubelet_nodes(scrape):
    return {
        _attr_map(rm["resource"]).get("k8s.node.name")
        for rm in scrape["metrics"]
        for scope in rm["scopeMetrics"] for m in scope["metrics"]
        if m["name"] == "k8s.node.filesystem.utilization"
    }


def _pod_phases(scrape):
    out = {}
    for rm in scrape["metrics"]:
        pod = _attr_map(rm["resource"]).get("k8s.pod.name") or ""
        for scope in rm["scopeMetrics"]:
            for m in scope["metrics"]:
                if m["name"] == "k8s.pod.phase":
                    out[pod] = int(_dp(m)["asInt"])
    return out


def test_node_not_ready_takes_down_exactly_one_node(monkeypatch):
    rec = _aug_run({"k8s_node_not_ready": 1}, scrapes=1, monkeypatch=monkeypatch)
    conditions = _node_conditions(rec.scrapes[0])
    down = [n for n, c in conditions.items() if c == 0]
    assert len(down) == 1 and len(conditions) > 1
    assert down[0] not in _kubelet_nodes(rec.scrapes[0]), "the NotReady node's kubelet stats stop"
    assert _kubelet_nodes(rec.scrapes[0]) == set(conditions) - set(down)
    assert [e for e in rec.scrapes[0]["events"] if e.get("k8s.event.reason") == "NodeNotReady"
            and e.get("k8s.object.name") == down[0]]


def test_node_not_ready_cascade_unknown_then_evicted(monkeypatch):
    rec = _aug_run({"k8s_node_not_ready": 1}, scrapes=4, step=100.0, monkeypatch=monkeypatch)
    assert 5 not in _pod_phases(rec.scrapes[0]).values()           # t=0: node down, pods still run
    assert 5 in _pod_phases(rec.scrapes[1]).values()               # t=100s: pods Unknown
    assert 5 not in _pod_phases(rec.scrapes[3]).values()           # t=300s: evicted
    assert any(e.get("k8s.event.reason") == "TaintManagerEviction" for e in rec.scrapes[3]["events"])


def test_node_not_ready_replacements_stay_pending_when_nodes_are_full(monkeypatch):
    rec = _aug_run(
        {"k8s_node_not_ready": 1, "memory_pct": 85}, scrapes=4, step=100.0, monkeypatch=monkeypatch,
    )
    assert 1 in _pod_phases(rec.scrapes[3]).values()
    assert any(e.get("k8s.event.reason") == "FailedScheduling" for e in rec.scrapes[2]["events"] + rec.scrapes[3]["events"])


def test_node_disk_fill_reaches_pressure_and_evicts_one_pod(monkeypatch):
    rec = _aug_run({"node_disk_fill_pct": 95}, scrapes=40, monkeypatch=monkeypatch)
    last = rec.scrapes[-1]
    fills = {}
    for rm in last["metrics"]:
        node = _attr_map(rm["resource"]).get("k8s.node.name")
        for scope in rm["scopeMetrics"]:
            for m in scope["metrics"]:
                if m["name"] == "k8s.node.filesystem.utilization":
                    fills[node] = float(_dp(m)["asDouble"])
    assert max(fills.values()) >= 0.9
    assert sorted(fills.values())[0] < 0.85, "nodes without the channel's pods keep a healthy fill"
    pressured = [n for n, c in _node_conditions(last).items() if c == 1]
    assert pressured
    phases = list(_pod_phases(last).values())
    assert phases.count(4) == 1, "exactly one pod is evicted"
    reasons = {e.get("k8s.event.reason") for s in rec.scrapes[-4:] for e in s["events"]}
    assert {"NodeHasDiskPressure", "Evicted"} <= reasons


def test_pvc_fill_ramps_only_the_channel_services_volume(monkeypatch):
    rec = _aug_run({"k8s_pvc_fill_pct": 97}, scrapes=45, monkeypatch=monkeypatch)
    avail_pct = {}
    for rm in rec.scrapes[-1]["metrics"]:
        vol = _attr_map(rm["resource"]).get("k8s.volume.name") or ""
        if not vol.endswith("-data-pvc"):
            continue
        vals = {m["name"]: float(_dp(m)["asInt"]) for sc in rm["scopeMetrics"] for m in sc["metrics"]}
        avail_pct[vol[: -len("-data-pvc")]] = 100 * vals["k8s.volume.available"] / vals["k8s.volume.capacity"]
    assert avail_pct[_AUG_TARGET] < 20, "breaches the Persistent volume space low alert"
    assert all(v > 20 for s, v in avail_pct.items() if s != _AUG_TARGET)
    assert any(e.get("k8s.event.reason") == "VolumeResizeFailed" for s in rec.scrapes[-6:] for e in s["events"])


def test_hpa_capped_scales_out_to_max_then_saturates_replicas(monkeypatch):
    rec = _aug_run({"k8s_hpa_capped": 1}, scrapes=10, monkeypatch=monkeypatch)

    def hpa(scrape):
        out = {}
        for rm in scrape["metrics"]:
            name = _attr_map(rm["resource"]).get("k8s.hpa.name") or ""
            if name:
                out[name[: -len("-hpa")]] = {
                    m["name"]: int(_dp(m)["asInt"]) for sc in rm["scopeMetrics"] for m in sc["metrics"]
                }
        return out

    first, last = hpa(rec.scrapes[0])[_AUG_TARGET], hpa(rec.scrapes[-1])[_AUG_TARGET]
    assert first["k8s.hpa.current_replicas"] < last["k8s.hpa.current_replicas"]
    assert last["k8s.hpa.current_replicas"] == last["k8s.hpa.desired_replicas"] == last["k8s.hpa.max_replicas"]
    others = {s: v for s, v in hpa(rec.scrapes[-1]).items() if s != _AUG_TARGET}
    assert all(v["k8s.hpa.current_replicas"] < v["k8s.hpa.max_replicas"] for v in others.values())
    # Deployment follows the HPA; capped replicas run hot (past the 0.9 throttling alert)
    assert _gauge_by_svc(rec.scrapes[-1], "k8s.deployment.desired", _ALL_SVCS)[_AUG_TARGET] == last["k8s.hpa.max_replicas"]
    cpu = {}
    for rm in rec.scrapes[-1]["metrics"]:
        pod = _attr_map(rm["resource"]).get("k8s.pod.name") or ""
        for sc in rm["scopeMetrics"]:
            for m in sc["metrics"]:
                if m["name"] == "k8s.container.cpu_limit_utilization":
                    cpu[pod] = float(_dp(m)["asDouble"])
    target_cpu = [v for pod, v in cpu.items() if re.match(rf"^{_AUG_TARGET}-[0-9a-f]{{8}}-", pod)]
    assert target_cpu and min(target_cpu) >= 0.95
    assert max(v for pod, v in cpu.items() if not pod.startswith(_AUG_TARGET)) < 0.9
    assert any(e.get("k8s.event.reason") == "SuccessfulRescale" for e in rec.scrapes[0]["events"])


def test_daemonset_misscheduled_count_follows_slider(monkeypatch):
    rec = _aug_run({"k8s_daemonset_misscheduled": 2}, scrapes=1, monkeypatch=monkeypatch)
    vals = [
        int(_dp(m)["asInt"])
        for rm in rec.scrapes[0]["metrics"] for sc in rm["scopeMetrics"] for m in sc["metrics"]
        if m["name"] == "k8s.daemonset.misscheduled_nodes"
    ]
    assert vals and all(v == 2 for v in vals)
    assert any(e.get("k8s.event.reason") == "FailedDaemonPod" for e in rec.scrapes[0]["events"])


# ── StatefulSet / DaemonSet workload pods and healthy replica baseline ──────────


def _workload_resources(scrape, kind):
    """kubeletstats pod resources owned by a StatefulSet / DaemonSet."""
    out = []
    for rm in scrape["metrics"]:
        attrs = _attr_map(rm["resource"])
        if attrs.get(f"k8s.{kind}.name") and attrs.get("k8s.pod.name"):
            names = {m["name"] for sc in rm["scopeMetrics"] for m in sc["metrics"]}
            if "k8s.pod.memory.working_set" in names:
                out.append((attrs, rm))
    return out


def _owner_gauges(scrape, kind, metric_names):
    """{owner: {metric: value}} for k8sclusterreceiver StatefulSet / DaemonSet objects."""
    out: dict[str, dict[str, float]] = {}
    for rm in scrape["metrics"]:
        attrs = _attr_map(rm["resource"])
        owner = attrs.get(f"k8s.{kind}.name")
        if not owner or attrs.get("k8s.pod.name"):
            continue
        for sc in rm["scopeMetrics"]:
            for m in sc["metrics"]:
                if m["name"] in metric_names:
                    dp = _dp(m)
                    out.setdefault(owner, {})[m["name"]] = float(dp.get("asDouble", dp.get("asInt")))
    return out


def test_healthy_baseline_has_every_deployment_replica_available(monkeypatch):
    """Package SLO "Deployment Replica Availability" needs available == desired on
    every datapoint of the chaos-off baseline (no back-dated lagging replica)."""
    rec = _aug_run({}, scrapes=3, active=(), monkeypatch=monkeypatch)
    for scrape in rec.scrapes:
        seen = 0
        for rm in scrape["metrics"]:
            vals = {}
            for sc in rm["scopeMetrics"]:
                for m in sc["metrics"]:
                    if m["name"] in ("k8s.deployment.desired", "k8s.deployment.available"):
                        vals.setdefault(m["name"], []).append(int(_dp(m)["asInt"]))
            if vals:
                seen += 1
                assert vals["k8s.deployment.available"] == vals["k8s.deployment.desired"]
        assert seen == len(_ALL_SVCS)


def test_statefulset_and_daemonset_pods_emit_pod_memory(monkeypatch):
    """ML job k8s_workload_memory_anomaly splits k8s.pod.memory.working_set by
    statefulset and daemonset name, so those workloads need kubelet pod metrics."""
    rec = _aug_run({}, scrapes=1, active=(), monkeypatch=monkeypatch)
    scrape = rec.scrapes[0]
    ss_pods = _workload_resources(scrape, "statefulset")
    ds_pods = _workload_resources(scrape, "daemonset")
    assert ss_pods and ds_pods
    for attrs, _rm in ss_pods + ds_pods:
        assert attrs["k8s.namespace.name"] == "k8saug"
        assert attrs["k8s.pod.name"].startswith("k8saug-")
        assert not attrs.get("k8s.deployment.name")
    # StatefulSet pods are <name>-<ordinal>, one per desired replica.
    ss_desired = _owner_gauges(scrape, "statefulset", {"k8s.statefulset.desired_pods"})
    for cluster in CLUSTERS:
        pods = [a for a, _ in ss_pods if a["k8s.cluster.name"] == cluster["name"]]
        for owner, g in ss_desired.items():
            names = sorted(a["k8s.pod.name"] for a in pods if a["k8s.statefulset.name"] == owner)
            assert names == [f"{owner}-{i}" for i in range(int(g["k8s.statefulset.desired_pods"]))]
        # DaemonSet pods: one per node.
        nodes = {a["k8s.node.name"] for a in pods} | {
            a["k8s.node.name"] for a, _ in ds_pods if a["k8s.cluster.name"] == cluster["name"]
        }
        for owner in {a["k8s.daemonset.name"] for a, _ in ds_pods}:
            on = [a["k8s.node.name"] for a, _ in ds_pods
                  if a["k8s.daemonset.name"] == owner and a["k8s.cluster.name"] == cluster["name"]]
            assert sorted(on) == sorted(set(on)) and set(on) == nodes


def test_statefulset_and_daemonset_replicas_are_healthy_and_stable(monkeypatch):
    rec = _aug_run({}, scrapes=4, active=(), monkeypatch=monkeypatch)
    ss_names = {"k8s.statefulset.desired_pods", "k8s.statefulset.ready_pods"}
    ds_names = {"k8s.daemonset.desired_scheduled_nodes", "k8s.daemonset.ready_nodes"}
    first_ss = _owner_gauges(rec.scrapes[0], "statefulset", ss_names)
    assert len(first_ss) == 2
    for scrape in rec.scrapes:
        ss = _owner_gauges(scrape, "statefulset", ss_names)
        assert ss == first_ss
        for g in ss.values():
            assert g["k8s.statefulset.ready_pods"] == g["k8s.statefulset.desired_pods"] > 0
        ds = _owner_gauges(scrape, "daemonset", ds_names)
        assert len(ds) == 2
        for g in ds.values():
            assert g["k8s.daemonset.ready_nodes"] == g["k8s.daemonset.desired_scheduled_nodes"] > 0


def test_daemonset_fault_marks_daemon_pods_not_ready(monkeypatch):
    rec = _aug_run({"k8s_daemonset_misscheduled": 1}, scrapes=1, monkeypatch=monkeypatch)
    scrape = rec.scrapes[0]
    ds = _owner_gauges(scrape, "daemonset", {"k8s.daemonset.desired_scheduled_nodes", "k8s.daemonset.ready_nodes"})
    assert all(g["k8s.daemonset.ready_nodes"] < g["k8s.daemonset.desired_scheduled_nodes"] for g in ds.values())
    ready = []
    for rm in scrape["metrics"]:
        attrs = _attr_map(rm["resource"])
        if attrs.get("k8s.daemonset.name") and attrs.get("k8s.container.name"):
            for sc in rm["scopeMetrics"]:
                for m in sc["metrics"]:
                    if m["name"] == "k8s.container.ready":
                        ready.append(int(_dp(m)["asInt"]))
    assert 0 in ready and 1 in ready
