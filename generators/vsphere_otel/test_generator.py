"""vSphere impact_keys coverage and co-located bulk doc tests."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

from generator_engine.db_common import build_signal_resource, otel_dataset
from generators.vsphere_otel import SPEC
from generators.vsphere_otel.generator import (
    _CLUSTER_HEADROOM_METRICS,
    _GAUGE_DYNAMIC_TEMPLATES,
    _GAUGE_DOUBLE_FIELDS,
    _GAUGE_LONG_FIELDS,
    _HOST_UTILIZATION_METRICS,
    _SIBLING_GAUGE_FIELDS,
    _sibling_gauges_need_mapping_reset,
    build_cluster_headroom_doc,
    build_host_utilization_doc,
    build_vm_disk_latency_doc,
    build_vm_memory_table_doc,
    run,
)

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.vsphere_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def _sample_resource():
    return build_signal_resource(
        ns="test",
        signal_type="metrics",
        dataset=otel_dataset("vcenterreceiver"),
        service_name="vsphere",
        service_version="8.0.0",
        service_instance_id="vsphere-001",
        host_name="vcenter.test.example.com",
        cloud={
            "provider": "gcp",
            "platform": "gcp_compute_engine",
            "region": "us-central1",
            "availability_zone": "us-central1-b",
        },
        extra_attrs={"vcenter.datacenter.name": "test-dc"},
    )


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['datastore_utilization_high', 'entity_status_unhealthy', 'host_cpu_utilization_high', 'host_disk_latency_high', 'host_memory_utilization_high', 'host_power_state_unhealthy', 'vm_disk_latency_high', 'vm_memory_ballooning', 'vm_memory_swapping']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


def test_cluster_headroom_doc_colocates_cpu_and_memory_pairs():
    resource = _sample_resource()
    doc = build_cluster_headroom_doc(
        resource,
        cluster_name="test-cluster",
        datacenter_name="test-dc",
        cpu_effective=68_000,
        cpu_limit=80_000,
        memory_effective=218_103_808_000,
        memory_limit=256_592_715_776,
        timestamp_ms=1_700_000_000_000,
    )
    assert doc["data_stream"]["dataset"] == "vcenterreceiver.otel"
    assert doc["attributes"]["vcenter.cluster.name"] == "test-cluster"
    assert doc["attributes"]["vcenter.datacenter.name"] == "test-dc"
    metrics = doc["metrics"]
    for name in _CLUSTER_HEADROOM_METRICS:
        assert name in metrics
    assert metrics["vcenter.cluster.cpu.effective"] == 68_000
    assert metrics["vcenter.cluster.cpu.limit"] == 80_000
    assert metrics["vcenter.cluster.memory.effective"] == 218_103_808_000
    assert metrics["vcenter.cluster.memory.limit"] == 256_592_715_776
    assert "data_stream.dataset" not in doc["resource"]["attributes"]


def test_host_utilization_doc_colocates_cpu_memory_siblings():
    resource = _sample_resource()
    doc = build_host_utilization_doc(
        resource,
        host_name="test-esxi-01",
        cluster_name="test-cluster",
        datacenter_name="test-dc",
        cpu_utilization=42.5,
        cpu_usage=17_000,
        cpu_capacity=40_000,
        memory_utilization=55.0,
        memory_usage=72_089,
    )
    metrics = doc["metrics"]
    for name in _HOST_UTILIZATION_METRICS:
        assert name in metrics
    assert metrics["vcenter.host.cpu.utilization"] == 42.5
    assert metrics["vcenter.host.cpu.usage"] == 17_000
    assert metrics["vcenter.host.cpu.capacity"] == 40_000
    assert metrics["vcenter.host.memory.utilization"] == 55.0
    assert metrics["vcenter.host.memory.usage"] == 72_089
    assert doc["attributes"]["vcenter.host.name"] == "test-esxi-01"
    assert "object" not in doc["attributes"]


def test_vm_memory_table_doc_colocates_usage_balloon_swap_disk():
    resource = _sample_resource()
    doc = build_vm_memory_table_doc(
        resource,
        vm_name="test-vm-01",
        host_name="test-esxi-01",
        cluster_name="test-cluster",
        datacenter_name="test-dc",
        memory_usage=8_192,
        ballooned=0,
        swapped=0,
        disk_utilization=61.25,
    )
    metrics = doc["metrics"]
    assert set(metrics) == {
        "vcenter.vm.memory.usage",
        "vcenter.vm.memory.ballooned",
        "vcenter.vm.memory.swapped",
        "vcenter.vm.disk.utilization",
    }
    assert metrics["vcenter.vm.memory.ballooned"] == 0
    assert metrics["vcenter.vm.memory.swapped"] == 0
    assert metrics["vcenter.vm.disk.utilization"] == 61.25
    assert doc["attributes"]["vcenter.vm.name"] == "test-vm-01"
    assert "object" not in doc["attributes"]


def test_vm_disk_latency_doc_colocates_latency_and_utilization():
    resource = _sample_resource()
    doc = build_vm_disk_latency_doc(
        resource,
        vm_name="test-vm-01",
        host_name="test-esxi-01",
        cluster_name="test-cluster",
        datacenter_name="test-dc",
        disk_latency_max=8,
        disk_utilization=61.25,
    )
    assert doc["metrics"]["vcenter.vm.disk.latency.max"] == 8
    assert doc["metrics"]["vcenter.vm.disk.utilization"] == 61.25
    assert doc["attributes"]["object"] == "scsi0:0"
    assert doc["attributes"]["vcenter.vm.name"] == "test-vm-01"


def test_gauge_dynamic_templates_use_long_and_double():
    for name in _GAUGE_LONG_FIELDS:
        assert _GAUGE_DYNAMIC_TEMPLATES[f"metrics.{name}"] == "gauge_long"
    for name in _GAUGE_DOUBLE_FIELDS:
        assert _GAUGE_DYNAMIC_TEMPLATES[f"metrics.{name}"] == "gauge_double"


def test_cluster_and_host_headroom_fields_are_bulk_only_constants():
    """SUM panels must not dual-emit these via OTLP — tracked as bulk-only sets."""
    assert "vcenter.cluster.cpu.effective" in _CLUSTER_HEADROOM_METRICS
    assert "vcenter.cluster.cpu.limit" in _CLUSTER_HEADROOM_METRICS
    assert "vcenter.cluster.memory.effective" in _CLUSTER_HEADROOM_METRICS
    assert "vcenter.cluster.memory.limit" in _CLUSTER_HEADROOM_METRICS
    assert "vcenter.host.cpu.utilization" in _HOST_UTILIZATION_METRICS
    assert "vcenter.host.memory.usage" in _HOST_UTILIZATION_METRICS


def test_sibling_gauge_reset_detects_plain_long_without_gauge():
    class _Resp:
        status_code = 200

        def json(self):
            return {
                "fields": {
                    "metrics.vcenter.cluster.cpu.effective": {
                        "long": {"type": "long"},
                    },
                    "metrics.vcenter.host.cpu.utilization": {
                        "double": {"type": "double", "time_series_metric": "gauge"},
                    },
                }
            }

    with patch("httpx.get", return_value=_Resp()) as get:
        assert _sibling_gauges_need_mapping_reset("https://es.example", "key") is True
        params = get.call_args.kwargs.get("params") or {}
        assert "metrics.vcenter.cluster.cpu.effective" in params.get("fields", "")


def test_sibling_gauge_reset_ok_when_all_gauges():
    class _Resp:
        status_code = 200

        def json(self):
            return {
                "fields": {
                    f"metrics.{name}": {
                        ("double" if name in _GAUGE_DOUBLE_FIELDS else "long"): {
                            "type": "double" if name in _GAUGE_DOUBLE_FIELDS else "long",
                            "time_series_metric": "gauge",
                        },
                    }
                    for name in _SIBLING_GAUGE_FIELDS
                }
            }

    with patch("httpx.get", return_value=_Resp()):
        assert _sibling_gauges_need_mapping_reset("https://es.example", "key") is False


def test_spec_datasets_unchanged():
    assert SPEC.datasets == ("vcenterreceiver.otel",)


def test_run_does_not_dual_emit_cluster_or_host_bulk_only_fields():
    """Cluster headroom + host utilization are bulk-only (SUM panels)."""
    stop = threading.Event()
    otlp_names: list[str] = []
    bulk_metric_keys: list[set[str]] = []

    client = MagicMock()

    def _capture_otlp(_resource, _scope, metrics):
        otlp_names.extend(m["name"] for m in metrics)
        stop.set()

    client.send_metrics_scoped.side_effect = _capture_otlp

    es_bulk = MagicMock()
    es_bulk.configured = True
    es_bulk.consecutive_failures = 0
    es_bulk.max_failures_before_backoff = 5

    def _capture_bulk(_stream, docs, dynamic_templates=None):
        for doc in docs:
            bulk_metric_keys.append(set(doc["metrics"]))
        return len(docs)

    es_bulk.send_bulk.side_effect = _capture_bulk

    with patch("generators.vsphere_otel.generator.ESBulkClient", return_value=es_bulk), \
         patch("generators.vsphere_otel.generator._reset_poisoned_sibling_gauge_mapping"):
        run(
            client,
            stop,
            scenario_data={
                "namespace": "test",
                "generator_config": {
                    "vm_names": ["test-vm-01"],
                    "host_names": ["test-esxi-01"],
                    "datastore_names": ["test-ds-01"],
                },
                "channel_registry": {},
            },
        )

    for name in _CLUSTER_HEADROOM_METRICS:
        assert name not in otlp_names
    for name in _HOST_UTILIZATION_METRICS:
        assert name not in otlp_names

    assert any(set(_CLUSTER_HEADROOM_METRICS) <= keys for keys in bulk_metric_keys)
    assert any(set(_HOST_UTILIZATION_METRICS) <= keys for keys in bulk_metric_keys)
    assert es_bulk.send_bulk.call_args.kwargs["dynamic_templates"] == _GAUGE_DYNAMIC_TEMPLATES


def _run_one_tick_capture(host_cpu_key: float = 0.0):
    """Run one tick; return [(resource attrs dict, [metric, ...]), ...] per OTLP send."""
    from generators.vsphere_otel import generator as gen

    stop = threading.Event()
    sends: list[tuple[dict, list[dict]]] = []
    client = MagicMock()

    def _capture(resource, _scope, metrics):
        attrs = {a["key"]: next(iter(a["value"].values())) for a in resource["attributes"]}
        sends.append((attrs, metrics))

    client.send_metrics_scoped.side_effect = _capture
    es_bulk = MagicMock()
    es_bulk.configured = False

    def _one_tick(stop_event, emit, interval):
        emit()

    chaos = {"host_cpu_utilization_high": host_cpu_key} if host_cpu_key else None
    with patch.object(gen, "ESBulkClient", return_value=es_bulk), \
         patch.object(gen, "_reset_poisoned_sibling_gauge_mapping"), \
         patch.object(gen, "run_tick_loop", _one_tick), \
         patch.object(gen, "resolve_active_chaos", return_value=chaos):
        run(client, stop, scenario_data={
            "namespace": "test",
            "generator_config": {
                "vm_names": ["test-vm-01", "test-vm-02"],
                "host_names": ["test-esxi-01"],
                "datastore_names": ["test-ds-01"],
            },
            "channel_registry": {},
        })
    return sends


def _points(sends, name):
    out = []
    for res_attrs, metrics in sends:
        for m in metrics:
            if m["name"] == name:
                body = m.get("gauge") or m.get("sum")
                for dp in body["dataPoints"]:
                    out.append((res_attrs, dp))
    return out


def test_host_disk_latency_carries_host_name_as_resource_attribute():
    """SLO groups by resource.attributes.vcenter.host.name (vcenter receiver shape)."""
    points = _points(_run_one_tick_capture(), "vcenter.host.disk.latency.max")
    assert points
    for res_attrs, dp in points:
        assert res_attrs["vcenter.host.name"] == "test-esxi-01"
        # Bare-name alerts / dashboards still see the data-point attribute.
        dp_attrs = {a["key"]: next(iter(a["value"].values())) for a in dp["attributes"]}
        assert dp_attrs["vcenter.host.name"] == "test-esxi-01"
        assert int(dp["asInt"]) < 15


def test_vm_cpu_readiness_per_vm_resource_healthy_below_slo():
    points = _points(_run_one_tick_capture(), "vcenter.vm.cpu.readiness")
    assert {r["vcenter.vm.name"] for r, _ in points} == {"test-vm-01", "test-vm-02"}
    for _, dp in points:
        assert 0 <= int(dp["asInt"]) < 5


def test_vm_cpu_readiness_rises_under_host_cpu_contention():
    points = _points(_run_one_tick_capture(host_cpu_key=95.0), "vcenter.vm.cpu.readiness")
    assert points and all(int(dp["asInt"]) >= 5 for _, dp in points)
