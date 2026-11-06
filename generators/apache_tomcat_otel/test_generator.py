"""Tomcat tests for impact key apply + healthy baseline."""
from __future__ import annotations

import random
import time
from unittest.mock import MagicMock, patch

from generators.apache_tomcat_otel import SPEC
from generators.apache_tomcat_otel import generator as mod


class _FakeChaos:
    def get_active_channels(self):
        return ["ch1"]


def _run(impact: dict | None = None, ticks: int = 1) -> list[dict]:
    client = MagicMock()
    stop = MagicMock()
    stop.is_set.side_effect = [False] * ticks + [True]
    stop.wait.return_value = None
    registry = {}
    controller = None
    if impact is not None:
        controller = _FakeChaos()
        registry = {"ch1": {"generator_impact": {"apache_tomcat_otel": impact}}}
    with patch.object(mod, "_reset_sticky_long_mapping"):
        with patch.object(mod, "ESBulkClient", side_effect=Exception("skip")):
            mod.run(client, stop, scenario_data={
                "namespace": "testns",
                "generator_config": {"chaos": "explicit"},
                "channel_registry": registry,
            }, chaos_controller=controller)
    metrics: list[dict] = []
    for call in client.send_metrics_scoped.call_args_list:
        metrics.extend(call.args[2])
    return metrics


def _gauge(metrics, name) -> float:
    for m in metrics:
        if m["name"] == name and "gauge" in m:
            dp = m["gauge"]["dataPoints"][0]
            assert "asDouble" in dp, f"{name} must be asDouble, got {dp.keys()}"
            return float(dp["asDouble"])
    raise AssertionError(f"missing {name}")


def test_healthy_baseline_no_chaos():
    m = _run(None)
    threads = _gauge(m, "Catalina_ThreadPool_currentThreadsBusy")
    max_t = _gauge(m, "Catalina_ThreadPool_maxThreads")
    assert threads / max_t <= 0.8
    open_fds = _gauge(m, "java_lang_OperatingSystem_OpenFileDescriptorCount")
    max_fds = _gauge(m, "java_lang_OperatingSystem_MaxFileDescriptorCount")
    assert open_fds / max_fds <= 0.8
    heap_used = _gauge(m, "java_lang_Memory_HeapMemoryUsage_used")
    heap_max = _gauge(m, "java_lang_Memory_HeapMemoryUsage_max")
    assert heap_used / heap_max <= 0.85
    assert _gauge(m, "java_lang_OperatingSystem_ProcessCpuLoad") <= 0.9
    # OOTB "Session rejection" / "Old generation GC activity" alerts fire on
    # DELTA/INCREASE > 0 — healthy baseline must start and stay at 0.
    assert _gauge(m, "Catalina_Manager_rejectedSessions") == 0
    assert _gauge(m, "java_lang_G1_Old_Generation_CollectionCount") == 0


def test_old_gc_and_session_rejections_stay_zero_across_ticks():
    state = mod.TomcatMetricState(random.Random(4))
    assert state.old_gc == 0
    assert state.rejected_sessions == 0
    for _ in range(15):
        state.tick(random.Random(4), load=1.0, err_mult=1.0, thread_pool_exhausted=False, gc_activity=0.0)
    assert state.old_gc == 0
    assert state.rejected_sessions == 0


def test_thread_pool_and_connection_and_fd():
    m = _run({"thread_pool_saturation": 0.9})
    assert _gauge(m, "Catalina_ThreadPool_currentThreadsBusy") / _gauge(m, "Catalina_ThreadPool_maxThreads") > 0.8
    m = _run({"connection_pool_saturation": 0.9})
    assert _gauge(m, "Catalina_ThreadPool_connectionCount") / _gauge(m, "Catalina_ThreadPool_maxConnections") > 0.8
    m = _run({"file_descriptor_exhaustion": 0.9})
    assert _gauge(m, "java_lang_OperatingSystem_OpenFileDescriptorCount") / _gauge(m, "java_lang_OperatingSystem_MaxFileDescriptorCount") > 0.8


def test_cpu_heap_gc_latency_sessions():
    assert _gauge(_run({"high_jvm_process_cpu_usage": 0.95}), "java_lang_OperatingSystem_ProcessCpuLoad") > 0.9
    m = _run({"jvm_heap_memory_pressure": 0.92})
    assert _gauge(m, "java_lang_Memory_HeapMemoryUsage_used") / _gauge(m, "java_lang_Memory_HeapMemoryUsage_max") > 0.85
    assert _gauge(_run({"old_generation_gc_activity": 3}), "java_lang_G1_Old_Generation_CollectionCount") >= 3
    assert _gauge(_run({"session_rejection": 5}), "Catalina_Manager_rejectedSessions") >= 5


def test_session_rejections_keep_accruing_while_active():
    """OOTB Session rejection alerts on MAX - MIN in the window: a flat counter recovers."""
    metrics = _run({"session_rejection": 5}, ticks=3)
    values = [
        m["gauge"]["dataPoints"][0]["asDouble"]
        for m in metrics if m["name"] == "Catalina_Manager_rejectedSessions"
    ]
    assert len(values) == 3
    assert values[0] < values[1] < values[2]


def test_delta_gauges_seeded_from_wall_clock():
    """Restarts must not emit lower cumulatives (negative DELTA panels)."""
    before = int(time.time())
    state = mod.TomcatMetricState(random.Random(0))
    assert state.realm_failures >= before
    assert state.session_counter >= before
    assert min(state.request_count.values()) >= before
    assert state.young_gc >= before
    assert state.cache_lookups >= before


def test_fd_fields_emitted_as_double_with_fraction():
    """Open/Max must stay float so Open/Max ratio is not integer-divided to 0."""
    m = _run(None)
    open_fds = _gauge(m, "java_lang_OperatingSystem_OpenFileDescriptorCount")
    max_fds = _gauge(m, "java_lang_OperatingSystem_MaxFileDescriptorCount")
    # +0.1 nudge keeps a fractional component even when pressure is integral.
    assert open_fds != int(open_fds)
    assert max_fds == 65536.0
    assert 0 < open_fds / max_fds <= 0.8


def test_bulk_docs_include_colocated_fd_pair():
    """FD util panel needs Open+Max on one doc; builder must emit that pair."""
    from generator_engine.db_common import build_signal_resource

    resource = build_signal_resource(
        ns="testns",
        signal_type="metrics",
        dataset="tomcat",
        service_name="apache-tomcat",
        service_version="10.1.18",
        service_instance_id="apache-tomcat-001",
        host_name="testns-tomcat",
        cloud={"provider": "aws", "platform": "aws_ec2", "region": "us-east-1",
               "availability_zone": "us-east-1a"},
    )
    state = mod.TomcatMetricState(random.Random(1))
    _metrics, bulk_docs = mod._build_metrics_and_docs(
        state, random.Random(1), load=1.0, err_mult=1.0,
        thread_pool_exhausted=False, max_threads=200, resource=resource,
        timestamp_ms=1_700_000_000_000,
    )
    fd_docs = [
        d for d in bulk_docs
        if "java_lang_OperatingSystem_OpenFileDescriptorCount" in d["metrics"]
        and "java_lang_OperatingSystem_MaxFileDescriptorCount" in d["metrics"]
    ]
    assert fd_docs, "missing co-located Open+Max FD bulk doc"
    # Same-ms timestamps across empty-attr docs collide in TSDS — run() staggers.
    assert all(isinstance(d["@timestamp"], int) for d in bulk_docs)


def test_fd_mapping_reset_treats_gauge_long_as_poison():
    class _Resp:
        status_code = 200

        def json(self):
            return {
                "fields": {
                    "metrics.java_lang_OperatingSystem_OpenFileDescriptorCount": {
                        "long": {
                            "type": "gauge_long",
                            "time_series_metric": "gauge",
                        },
                    },
                    "metrics.java_lang_OperatingSystem_MaxFileDescriptorCount": {
                        "long": {
                            "type": "gauge_long",
                            "time_series_metric": "gauge",
                        },
                    },
                }
            }

    with patch("httpx.get", return_value=_Resp()):
        assert mod._fields_need_mapping_reset("https://es.example", "key") is True


def test_fd_mapping_ok_when_gauge_double():
    class _Resp:
        status_code = 200

        def json(self):
            return {
                "fields": {
                    "metrics.java_lang_OperatingSystem_OpenFileDescriptorCount": {
                        "double": {
                            "type": "gauge_double",
                            "time_series_metric": "gauge",
                        },
                    },
                    "metrics.java_lang_OperatingSystem_MaxFileDescriptorCount": {
                        "double": {
                            "type": "double",
                            "time_series_metric": "gauge",
                        },
                    },
                }
            }

    with patch("httpx.get", return_value=_Resp()):
        assert mod._fields_need_mapping_reset("https://es.example", "key") is False


# --- 1:1 alert impact_keys coverage (auto) ---
from generators.apache_tomcat_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['connection_pool_saturation', 'file_descriptor_exhaustion', 'high_jvm_process_cpu_usage', 'high_request_error_rate', 'jvm_heap_memory_pressure', 'old_generation_gc_activity', 'request_latency_spike', 'session_rejection', 'thread_pool_saturation']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
