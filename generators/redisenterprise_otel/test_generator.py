"""Redis Enterprise tests for impact key apply + healthy baseline."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from generators.redisenterprise_otel import SPEC
from generators.redisenterprise_otel import generator as mod


class _FakeChaos:
    def __init__(self, active=("ch1",)):
        self._active = list(active)

    def get_active_channels(self):
        return self._active


def _run(impact: dict | None) -> tuple[list[dict], list[dict]]:
    client = MagicMock()
    stop = MagicMock()
    stop.is_set.side_effect = [False, True]
    stop.wait.return_value = None
    registry = {}
    controller = None
    if impact is not None:
        controller = _FakeChaos()
        registry = {
            "ch1": {"generator_impact": {"redisenterprise_otel": impact}},
        }
    bulk = MagicMock()
    bulk.configured = True
    bulk.consecutive_failures = 0
    bulk.max_failures_before_backoff = 5
    bulk.send_bulk.return_value = 1
    with patch.object(mod, "_reset_sticky_long_mapping"):
        with patch.object(mod, "ESBulkClient", return_value=bulk):
            mod.run(
                client, stop,
                scenario_data={
                    "namespace": "testns",
                    "generator_config": {"chaos": "explicit"},
                    "channel_registry": registry,
                },
                chaos_controller=controller,
            )
    metrics: list[dict] = []
    for call in client.send_metrics_scoped.call_args_list:
        metrics.extend(call.args[2])
    bulk_docs: list[dict] = []
    for call in bulk.send_bulk.call_args_list:
        bulk_docs.extend(call.args[1])
    return metrics, bulk_docs


def _gauge(metrics, name) -> float:
    for m in metrics:
        if m["name"] == name and "gauge" in m:
            dp = m["gauge"]["dataPoints"][0]
            assert "asDouble" in dp, f"{name} must be asDouble, got {dp.keys()}"
            return float(dp["asDouble"])
    raise AssertionError(f"missing gauge {name}")


def _attrs(metrics, name) -> dict:
    for m in metrics:
        if m["name"] == name and "gauge" in m:
            dp = m["gauge"]["dataPoints"][0]
            out = {}
            for attr in dp.get("attributes", []):
                val = attr["value"]
                out[attr["key"]] = (
                    val.get("stringValue")
                    or val.get("intValue")
                    or val.get("doubleValue")
                )
            return out
    raise AssertionError(f"missing gauge {name}")


def test_healthy_baseline_no_chaos():
    metrics, _ = _run(None)
    assert _gauge(metrics, "bdb_up") == 1
    assert _gauge(metrics, "node_up") == 1
    assert _gauge(metrics, "redis_up") == 1
    assert _gauge(metrics, "node_cert_expiration_seconds") >= 30 * 86400
    assert _gauge(metrics, "bdb_avg_latency") * 1000 <= 5
    assert _gauge(metrics, "listener_auth_errors") == 0
    used = _gauge(metrics, "bdb_used_memory")
    limit = _gauge(metrics, "bdb_memory_limit")
    assert used / limit <= 0.90


def test_bdb_down_and_node_down_and_shard_down():
    m, _ = _run({"database_down": 0})
    assert _gauge(m, "bdb_up") == 0
    m, _ = _run({"node_down": True})
    assert _gauge(m, "node_up") == 0
    m, _ = _run({"shard_down": True})
    assert _gauge(m, "redis_up") == 0


def test_cert_expiry_and_latency_and_memory():
    m, _ = _run({"tls_certificate_expiring_soon": 10})
    assert _gauge(m, "node_cert_expiration_seconds") < 30 * 86400
    m, _ = _run({"database_high_latency": 100})
    assert _gauge(m, "bdb_avg_latency") * 1000 > 5
    m, _ = _run({"database_memory_utilization_critical": 95})
    used = _gauge(m, "bdb_used_memory")
    limit = _gauge(m, "bdb_memory_limit")
    assert used / limit > 0.90


def test_eviction_auth_persist_fd_conn_frag():
    m, _ = _run({"database_unexpected_key_evictions": 5})
    assert any(x["name"] == "bdb_evicted_objects" for x in m)
    assert "sum" not in next(x for x in m if x["name"] == "bdb_evicted_objects")
    m, _ = _run({"authentication_failures_detected": 3})
    assert _gauge(m, "listener_auth_errors") >= 3
    m, _ = _run({"shard_persistence_failures": 2})
    assert _gauge(m, "redis_rdb_saves_consecutive_failures") >= 2
    m, _ = _run({"proxy_file_descriptor_saturation": 90})
    open_fds = _gauge(m, "dmcproxy_process_open_fds")
    max_fds = _gauge(m, "dmcproxy_process_max_fds")
    assert open_fds / max_fds > 0.8
    m, _ = _run({"shard_connection_saturation": 90})
    assert _gauge(m, "redis_connected_clients") / _gauge(m, "redis_maxclients") > 0.8
    m, _ = _run({"shard_high_memory_fragmentation": 2.0})
    assert _gauge(m, "redis_mem_fragmentation_ratio") > 1.5


def test_dashboard_fields_are_gauges_with_attrs():
    metrics, bulk_docs = _run(None)

    for name in (
        "bdb_evicted_objects",
        "bdb_read_hits",
        "bdb_read_misses",
        "bdb_read_req",
        "bdb_write_req",
        "bdb_other_req",
        "bdb_ingress_bytes",
        "bdb_egress_bytes",
        "bdb_expired_objects",
        "bdb_avg_read_latency_max",
        "bdb_avg_write_latency_max",
        "bdb_avg_other_latency_max",
        "bdb_no_of_expires",
        "bdb_shard_cpu_user",
        "bdb_shard_cpu_system",
        "dmcproxy_process_resident_memory_bytes",
        "dmcproxy_process_virtual_memory_bytes",
        "listener_max_connections_exceeded",
        "listener_acc_read_latency",
        "listener_acc_write_latency",
        "listener_read_req",
        "listener_write_req",
        "redis_total_error_replies",
        "redis_process_cpu_usage_percent",
        "redis_current_cow_size",
        "redis_current_fork_perc",
        "redis_rdb_changes_since_last_save",
        "redis_eventloop_duration_sum",
    ):
        _gauge(metrics, name)

    cert_attrs = _attrs(metrics, "node_cert_expiration_seconds")
    assert "path" in cert_attrs and cert_attrs["path"]

    listener_attrs = _attrs(metrics, "listener_total_req")
    assert listener_attrs.get("endpoint")
    assert listener_attrs.get("port")
    assert listener_attrs.get("node")
    assert listener_attrs.get("bdb")

    shard_attrs = _attrs(metrics, "redis_up")
    assert shard_attrs.get("node")
    assert shard_attrs.get("redis")
    assert shard_attrs.get("role")

    assert bulk_docs, "expected co-located bulk docs"
    bdb_doc = next(d for d in bulk_docs if "bdb_used_memory" in d["metrics"])
    assert "bdb_memory_limit" in bdb_doc["metrics"]
    assert "bdb_up" in bdb_doc["metrics"]
    assert "bdb_avg_read_latency_max" in bdb_doc["metrics"]
    assert "bdb_shard_cpu_user" in bdb_doc["metrics"]
    node_doc = next(d for d in bulk_docs if "node_cpu_user" in d["metrics"])
    assert "node_cpu_system" in node_doc["metrics"]
    assert "path" not in node_doc["attributes"]
    cert_doc = next(d for d in bulk_docs if "node_cert_expiration_seconds" in d["metrics"])
    assert "path" in cert_doc["attributes"]
    listener_doc = next(d for d in bulk_docs if "listener_read_req" in d["metrics"])
    assert listener_doc["attributes"].get("endpoint")
    assert listener_doc["attributes"].get("port")
    assert listener_doc["attributes"].get("node")
    shard_doc = next(d for d in bulk_docs if "redis_total_error_replies" in d["metrics"])
    assert "redis_process_cpu_usage_percent" in shard_doc["metrics"]
    assert "redis_aof_rewrite_in_progress" in shard_doc["metrics"]
    assert "redis_rdb_bgsave_in_progress" in shard_doc["metrics"]
    assert shard_doc["attributes"].get("node")
    proxy_doc = next(
        d for d in bulk_docs if "dmcproxy_process_resident_memory_bytes" in d["metrics"]
    )
    assert "dmcproxy_process_virtual_memory_bytes" in proxy_doc["metrics"]


def test_mapping_reset_ignores_healthy_gauge_long():
    class _Resp:
        status_code = 200

        def json(self):
            return {
                "fields": {
                    "metrics.bdb_up": {
                        "long": {"type": "long", "time_series_metric": "gauge"},
                    },
                }
            }

    with patch("httpx.get", return_value=_Resp()):
        assert mod._fields_need_mapping_reset("https://es.example", "key") is False


def test_mapping_reset_detects_counter_long_and_plain_long():
    class _CounterResp:
        status_code = 200

        def json(self):
            return {
                "fields": {
                    "metrics.bdb_read_hits": {
                        "counter_long": {
                            "type": "counter_long",
                            "time_series_metric": "counter",
                        },
                    },
                }
            }

    class _PlainLongResp:
        status_code = 200

        def json(self):
            return {
                "fields": {
                    "metrics.bdb_read_hits": {
                        "long": {"type": "long"},
                    },
                }
            }

    with patch("httpx.get", return_value=_CounterResp()):
        assert mod._fields_need_mapping_reset("https://es.example", "key") is True
    with patch("httpx.get", return_value=_PlainLongResp()):
        assert mod._fields_need_mapping_reset("https://es.example", "key") is True


def test_dimensions_need_widen_when_listener_attrs_missing():
    class _Resp:
        status_code = 200

        def json(self):
            return {
                "fields": {
                    "metrics.listener_total_req": {
                        "double": {"type": "double", "time_series_metric": "gauge"},
                    },
                    # attributes.endpoint/port/node absent → widen required
                }
            }

    with patch("httpx.get", return_value=_Resp()):
        assert mod._dimensions_need_widen("https://es.example", "key") is True


def test_dimensions_ok_when_listener_and_shard_attrs_present():
    class _Resp:
        status_code = 200

        def json(self):
            return {
                "fields": {
                    "metrics.listener_total_req": {
                        "double": {"type": "double", "time_series_metric": "gauge"},
                    },
                    "metrics.redis_up": {
                        "double": {"type": "double", "time_series_metric": "gauge"},
                    },
                    "attributes.endpoint": {"keyword": {"type": "keyword"}},
                    "attributes.port": {"keyword": {"type": "keyword"}},
                    "attributes.node": {"keyword": {"type": "keyword"}},
                }
            }

    with patch("httpx.get", return_value=_Resp()):
        assert mod._dimensions_need_widen("https://es.example", "key") is False


# --- 1:1 alert impact_keys coverage (auto) ---
from generators.redisenterprise_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['authentication_failures_detected', 'database_down', 'database_high_latency', 'database_memory_utilization_critical', 'database_unexpected_key_evictions', 'node_cpu_saturation', 'node_down', 'node_free_memory_low', 'node_storage_running_low', 'proxy_file_descriptor_saturation', 'shard_connection_saturation', 'shard_down', 'shard_high_memory_fragmentation', 'shard_persistence_failures', 'tls_certificate_expiring_soon']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


# --- per-alert behaviour: each key alone breaches only its own alert ------------------
# Drives run() for several cycles and evaluates the package alert ES|QL (v0.3.2) per
# entity (bdb / node / shard / proxy) over the last WINDOW cycles.

import random  # noqa: E402

import pytest  # noqa: E402

DEFAULTS = {key: meta["default"] for key, meta in SPEC.impact_keys.items()}
SEEDS = range(8)
WINDOW = 6
GIB = 1024 ** 3


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
    bulk.configured = True
    bulk.consecutive_failures = 0
    bulk.max_failures_before_backoff = 5
    bulk.send_bulk.return_value = 1
    real_random = random.Random
    with patch.object(mod, "_reset_sticky_long_mapping"), \
            patch.object(mod, "ESBulkClient", return_value=bulk), \
            patch.object(mod.random, "Random", lambda *a: real_random(seed)):
        mod.run(
            client, stop,
            scenario_data={
                "namespace": "testns",
                "generator_config": {"chaos": "explicit"},
                "channel_registry": {"ch1": {"generator_impact": {"redisenterprise_otel": impact}}},
            },
            chaos_controller=_SeqChaos(faulted),
        )
    series: dict[tuple, list[float]] = {}
    for call in client.send_metrics_scoped.call_args_list:
        for m in call.args[2]:
            dp = (m.get("gauge") or m["sum"])["dataPoints"][0]
            value = float(dp["asInt"] if "asInt" in dp else dp["asDouble"])
            attrs = tuple(sorted(
                (a["key"], next(iter(a["value"].values()))) for a in dp.get("attributes", [])
            ))
            series.setdefault((m["name"], attrs), []).append(value)
    return {k: v[-WINDOW:] for k, v in series.items()}


def _per_entity(series, *names):
    """{attrs: {name: window}} for entities that have every metric in ``names``."""
    out: dict[tuple, dict] = {}
    for (name, attrs), window in series.items():
        if name in names:
            out.setdefault(attrs, {})[name] = window
    return [e for e in out.values() if len(e) == len(names)]


def _avg(values):
    return sum(values) / len(values)


def _breached(series):
    def any_entity(names, test):
        return any(test(*(e[n] for n in names)) for e in _per_entity(series, *names))

    return {
        "tls_certificate_expiring_soon": any_entity(
            ["node_cert_expiration_seconds"], lambda s: min(s) / 86400 < 30),
        "database_down": any_entity(["bdb_up"], lambda s: min(s) < 1),
        "database_high_latency": any_entity(
            ["bdb_avg_latency"], lambda s: _avg(s) * 1000 > 5),
        "database_memory_utilization_critical": any_entity(
            ["bdb_used_memory", "bdb_memory_limit"], lambda u, m: _avg(u) / _avg(m) * 100 > 90),
        "database_unexpected_key_evictions": any_entity(
            ["bdb_evicted_objects"], lambda s: max(s) > 0),
        "authentication_failures_detected": any_entity(
            ["listener_auth_errors"], lambda s: max(s) > 0),
        "node_down": any_entity(["node_up"], lambda s: min(s) < 1),
        "node_cpu_saturation": any_entity(
            ["node_cpu_user", "node_cpu_system", "node_cpu_idle"],
            lambda u, s, i: (_avg(u) + _avg(s)) / (_avg(u) + _avg(s) + _avg(i)) * 100 > 85),
        "node_free_memory_low": any_entity(
            ["node_free_memory"], lambda s: _avg(s) / GIB < 1),
        "node_storage_running_low": any_entity(
            ["node_persistent_storage_avail"], lambda s: _avg(s) / GIB < 5
        ) or any_entity(["node_ephemeral_storage_avail"], lambda s: _avg(s) / GIB < 5),
        "proxy_file_descriptor_saturation": any_entity(
            ["dmcproxy_process_open_fds", "dmcproxy_process_max_fds"],
            lambda o, m: _avg(o) / _avg(m) * 100 > 80),
        "shard_connection_saturation": any_entity(
            ["redis_connected_clients", "redis_maxclients"],
            lambda c, m: _avg(c) / _avg(m) * 100 > 80),
        "shard_down": any_entity(["redis_up"], lambda s: min(s) < 1),
        "shard_high_memory_fragmentation": any_entity(
            ["redis_mem_fragmentation_ratio", "redis_used_memory"],
            lambda f, u: _avg(u) > 10 * 1024 * 1024 and _avg(f) > 1.5),
        "shard_persistence_failures": any_entity(
            ["redis_rdb_saves_consecutive_failures"], lambda s: max(s) > 0
        ) or any_entity(["redis_aof_rewrites_consecutive_failures"], lambda s: max(s) > 0),
    }


# Fault values at or beyond the GT channels (cpu probes 95, above its 85% line).
FAULTS = {
    "tls_certificate_expiring_soon": 10.0,
    "database_down": 0.0,
    "database_high_latency": 75.0,
    "database_memory_utilization_critical": 675.0,
    "database_unexpected_key_evictions": 50.0,
    "authentication_failures_detected": 50.0,
    "node_down": True,
    "node_cpu_saturation": 95.0,
    "node_free_memory_low": 10.0,
    "node_storage_running_low": 10.0,
    "proxy_file_descriptor_saturation": 600.0,
    "shard_connection_saturation": 600.0,
    "shard_down": True,
    "shard_high_memory_fragmentation": 75.0,
    "shard_persistence_failures": 50.0,
}


def _hits(breached):
    return sorted(k for k, hit in breached.items() if hit)


def test_fault_table_covers_every_impact_key():
    assert sorted(FAULTS) == sorted(SPEC.impact_keys)


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
        series = _simulate(
            {**DEFAULTS, key: FAULTS[key]}, seed, faulted=6, cycles=6 + WINDOW + 2)
        assert not _hits(_breached(series)), (
            f"seed {seed}: {_hits(_breached(series))} still firing after {key} resolved"
        )


@pytest.mark.parametrize("key,breaching,healthy", [
    ("database_high_latency", 6.0, 0.0),
    ("node_cpu_saturation", 90.0, 0.0),
    ("database_unexpected_key_evictions", 1.0, 0.0),
])
def test_key_units_match_the_documented_thresholds(key, breaching, healthy):
    for seed in SEEDS:
        hit = _breached(_simulate({**DEFAULTS, key: breaching}, seed, 99, WINDOW + 2))[key]
        miss = _breached(_simulate({**DEFAULTS, key: healthy}, seed, 99, WINDOW + 2))[key]
        assert hit and not miss, f"seed {seed}: {key} breaching={hit} healthy={miss}"
