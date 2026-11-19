"""Supabase-specific tests for the supabase_otel generator."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

from generators.supabase_otel.generator import (
    CPU_MODES,
    METRIC_NAMES,
    _vmstat_mapped_as_counter,
    run,
)


def _metric_names(calls) -> set[str]:
    names: set[str] = set()
    for call in calls:
        metrics = call.args[2] if len(call.args) >= 3 else call.kwargs.get("metrics", [])
        for m in metrics:
            names.add(m["name"])
    return names


def _datapoint_attrs(metric: dict) -> dict:
    dps = metric.get("sum", metric.get("gauge", {})).get("dataPoints", [])
    if not dps:
        return {}
    attrs = {}
    for a in dps[0].get("attributes", []):
        attrs[a["key"]] = next(iter(a["value"].values()))
    return attrs


def test_uses_send_metrics_scoped_not_send_metrics():
    """OTLPClient.send_metrics(resource, metrics) rejects a scope arg (TypeError)."""
    client = MagicMock()
    stop = threading.Event()
    client.send_metrics_scoped.side_effect = lambda *_a, **_k: stop.set()

    with patch(
        "generators.supabase_otel.generator._reset_vmstat_counter_mapping",
    ):
        run(client, stop, scenario_data={
            "namespace": "testns",
            "channel_registry": {},
            "generator_config": {},
        })

    assert client.send_metrics_scoped.called
    assert not client.send_metrics.called


def test_emits_all_dashboard_metrics():
    client = MagicMock()
    stop = threading.Event()
    client.send_metrics_scoped.side_effect = lambda *_a, **_k: stop.set()

    with patch(
        "generators.supabase_otel.generator._reset_vmstat_counter_mapping",
    ):
        run(client, stop, scenario_data={
            "namespace": "testns",
            "channel_registry": {},
            "generator_config": {"project_ref": "demo-project"},
        })

    names = _metric_names(client.send_metrics_scoped.call_args_list)
    missing = set(METRIC_NAMES) - names
    assert not missing, f"missing metrics: {sorted(missing)}"


def test_cpu_and_filesystem_attributes():
    client = MagicMock()
    stop = threading.Event()
    client.send_metrics_scoped.side_effect = lambda *_a, **_k: stop.set()

    with patch(
        "generators.supabase_otel.generator._reset_vmstat_counter_mapping",
    ):
        run(client, stop, scenario_data={
            "namespace": "testns",
            "channel_registry": {},
            "generator_config": {},
        })

    metrics = client.send_metrics_scoped.call_args.args[2]
    cpu_points = [m for m in metrics if m["name"] == "node_cpu_seconds_total"]
    assert cpu_points
    modes = {_datapoint_attrs(m).get("mode") for m in cpu_points}
    assert set(CPU_MODES).issubset(modes)
    assert all("cpu" in _datapoint_attrs(m) for m in cpu_points)

    fs_points = [m for m in metrics if m["name"] == "node_filesystem_free_bytes"]
    assert fs_points
    for m in fs_points:
        attrs = _datapoint_attrs(m)
        assert "device" in attrs and "mountpoint" in attrs

    db_points = [m for m in metrics if m["name"] == "pg_stat_database_num_backends"]
    assert db_points
    assert "datname" in _datapoint_attrs(db_points[0])


def test_supabase_project_ref_on_every_datapoint():
    """Dashboard Options List looks up bare field supabase_project_ref."""
    client = MagicMock()
    stop = threading.Event()
    client.send_metrics_scoped.side_effect = lambda *_a, **_k: stop.set()

    with patch(
        "generators.supabase_otel.generator._reset_vmstat_counter_mapping",
    ):
        run(client, stop, scenario_data={
            "namespace": "testns",
            "channel_registry": {},
            "generator_config": {"project_ref": "abcxyzproject"},
        })

    metrics = client.send_metrics_scoped.call_args.args[2]
    assert metrics
    for m in metrics:
        attrs = _datapoint_attrs(m)
        assert attrs.get("supabase_project_ref") == "abcxyzproject", m["name"]
        assert attrs.get("service_type") in {"db", "gotrue"}, m["name"]

    resource = client.send_metrics_scoped.call_args.args[0]
    res_attrs = {
        a["key"]: next(iter(a["value"].values()))
        for a in resource["attributes"]
    }
    # The package alerts group BY resource.attributes.supabase_project_ref; the dashboards
    # read the bare field, which resolves from the datapoint attribute.
    assert res_attrs["supabase_project_ref"] == "abcxyzproject"
    assert "supabase.project.ref" not in res_attrs


def test_vmstat_emitted_as_gauge_not_counter():
    """AVG_OVER_TIME rejects counter_long; Prometheus untyped → gauge."""
    client = MagicMock()
    stop = threading.Event()
    client.send_metrics_scoped.side_effect = lambda *_a, **_k: stop.set()

    with patch(
        "generators.supabase_otel.generator._reset_vmstat_counter_mapping",
    ):
        run(client, stop, scenario_data={
            "namespace": "testns",
            "channel_registry": {},
            "generator_config": {},
        })

    metrics = client.send_metrics_scoped.call_args.args[2]
    vmstat_names = {
        "node_vmstat_pgpgin",
        "node_vmstat_pgpgout",
        "node_vmstat_pgfault",
        "node_vmstat_pgmajfault",
        "node_vmstat_oom_kill",
    }
    found = {m["name"] for m in metrics if m["name"] in vmstat_names}
    assert found == vmstat_names
    for m in metrics:
        if m["name"] in vmstat_names:
            assert "gauge" in m, m["name"]
            assert "sum" not in m, m["name"]


def test_cpu_idle_rate_stays_below_one():
    """Util panel clamps to 0% when RATE(idle) >= 1."""
    from generators.supabase_otel.generator import EMIT_INTERVAL_S, _SupabaseState
    import random

    rng = random.Random(0)
    state = _SupabaseState(rng)
    before = {cpu: state.cpu[(cpu, "idle")] for cpu in ("0", "1")}
    state.tick(rng, load=1.0, faults={})
    for cpu, prev in before.items():
        delta = state.cpu[(cpu, "idle")] - prev
        assert delta / EMIT_INTERVAL_S < 1.0, (cpu, delta)


def test_cpu_idle_rate_respects_warmup_wall():
    """Warmup ticks must advance CPU by the short wait, not EMIT_INTERVAL_S."""
    from generators.supabase_otel.generator import (
        _WARMUP_INTERVAL_S,
        _SupabaseState,
    )
    import random

    rng = random.Random(1)
    state = _SupabaseState(rng)
    before = {cpu: state.cpu[(cpu, "idle")] for cpu in ("0", "1")}
    state.tick(rng, load=1.0, faults={}, wall=_WARMUP_INTERVAL_S)
    for cpu, prev in before.items():
        delta = state.cpu[(cpu, "idle")] - prev
        assert delta / _WARMUP_INTERVAL_S < 1.0, (cpu, delta)
        assert delta < 1.0, (cpu, delta)


def test_dataset_is_bare_supabase_metrics():
    """OTLP intake appends .otel — must not pass supabase.metrics.otel."""
    client = MagicMock()
    stop = threading.Event()
    client.send_metrics_scoped.side_effect = lambda *_a, **_k: stop.set()

    with patch(
        "generators.supabase_otel.generator._reset_vmstat_counter_mapping",
    ):
        run(client, stop, scenario_data={
            "namespace": "testns",
            "channel_registry": {},
            "generator_config": {},
        })

    resource = client.send_metrics_scoped.call_args.args[0]
    attrs = {
        a["key"]: next(iter(a["value"].values()))
        for a in resource["attributes"]
    }
    assert attrs["data_stream.dataset"] == "supabase.metrics"


def test_vmstat_mapped_as_counter_detects_counter_meta():
    class _Resp:
        status_code = 200

        @staticmethod
        def json():
            return {
                "fields": {
                    "metrics.node_vmstat_pgpgin": {
                        "long": {"time_series_metric": "counter"},
                    },
                },
            }

    with patch("httpx.get", return_value=_Resp()):
        assert _vmstat_mapped_as_counter("https://es.example", "key") is True



# ═════════════════════════════════════════════════════════════════════════════
# Package alerts (supabase_otel@0.4.0), evaluated on a simulated run()
# ═════════════════════════════════════════════════════════════════════════════
# run() is driven on a fake clock (wait() advances it). Each alert mirrors its TS query:
# RATE / INCREASE / DELTA / LAST_OVER_TIME are computed per series over the alert window.

import random
import time
from collections import defaultdict

import pytest

_REAL_RANDOM = random.Random
MINUTES = 35


def _simulate(monkeypatch, *, fault=None, seed=7, minutes=MINUTES):
    from generators.supabase_otel import generator as mod

    start = 1_700_000_000.0
    clock = {"t": start}
    end = start + minutes * 60

    monkeypatch.setattr(mod, "_reset_vmstat_counter_mapping", lambda: False)
    monkeypatch.setattr(mod, "business_load", lambda: 1.0)
    monkeypatch.setattr(time, "time", lambda: clock["t"])
    # Metric points take the scrape clock, which reads time.time_ns().
    monkeypatch.setattr(time, "time_ns", lambda: int(clock["t"] * 1e9))
    monkeypatch.setattr(mod.random, "Random", lambda *a: _REAL_RANDOM(seed))

    class _Stop:
        def is_set(self):
            return clock["t"] >= end

        def wait(self, timeout=None):
            clock["t"] += timeout

    class _Chaos:
        def get_active_channels(self):
            return ["ch1"] if fault else []

    series = defaultdict(list)   # (name, attrs-tuple) -> [(ts, value)]
    resources = []

    class _Client:
        def send_metrics_scoped(self, resource, scope, metrics):
            resources.append(resource)
            for m in metrics:
                kind = "sum" if "sum" in m else "gauge"
                for dp in m[kind]["dataPoints"]:
                    attrs = tuple(sorted(
                        (a["key"], next(iter(a["value"].values()))) for a in dp.get("attributes", [])
                    ))
                    value = float(dp["asDouble"] if "asDouble" in dp else dp["asInt"])
                    series[(m["name"], attrs)].append((int(dp["timeUnixNano"]) / 1e9, value))

        def send_logs_scoped(self, *_a, **_k):
            pass

        def build_log_record(self, sev, body, attrs=None, **_k):
            return {"body": body}

        def close(self):
            pass

    mod.run(_Client(), _Stop(), scenario_data={
        "namespace": "test", "channel_registry": {"ch1": {"generator_impact": {"supabase_otel": fault or {}}}},
        "generator_config": {"project_ref": "demo-project"},
    }, chaos_controller=_Chaos())
    return {"series": series, "now": end, "resources": resources}


def _window(sim, name, minutes):
    """series key attrs -> points inside the alert window."""
    lo = sim["now"] - minutes * 60
    out = {}
    for (n, attrs), pts in sim["series"].items():
        if n == name:
            inside = [(t, v) for t, v in pts if t >= lo]
            if inside:
                out[dict(attrs) and attrs] = inside
    return out


def _rate(pts):
    return (pts[-1][1] - pts[0][1]) / (pts[-1][0] - pts[0][0]) if len(pts) > 1 else 0.0


def _increase(pts):
    return max(0.0, pts[-1][1] - pts[0][1]) if len(pts) > 1 else 0.0


def _attr(attrs, key):
    return dict(attrs).get(key)


def _by(win, key):
    out = defaultdict(list)
    for attrs, pts in win.items():
        out[_attr(attrs, key)].append(pts)
    return out


def _last(pts):
    return pts[-1][1]


def _a_auth_errors(sim):
    win = _window(sim, "http_status_codes_total", 10)
    total = sum(_increase(p) for p in win.values())
    server = sum(_increase(p) for a, p in win.items() if str(_attr(a, "code")).startswith("5"))
    return total >= 20 and server / total * 100 >= 5


def _a_auth_down(sim):
    return any(_last(p) == 0 for p in _window(sim, "gotrue_running", 5).values())


def _a_disk_io(sim):
    for _dev, group in _by(_window(sim, "node_disk_io_time_seconds_total", 15), "device").items():
        if min(1.0, max(0.0, sum(_rate(p) for p in group))) * 100 >= 90:
            return True
    return False


def _a_fs_full(sim):
    avail = _window(sim, "node_filesystem_avail_bytes", 10)
    size = _window(sim, "node_filesystem_size_bytes", 10)
    for attrs, pts in avail.items():
        if attrs in size and _last(size[attrs]) > 0 and (1 - _last(pts) / _last(size[attrs])) * 100 >= 85:
            return True
    return False


def _a_fs_readonly(sim):
    return any(_last(p) == 1 for p in _window(sim, "node_filesystem_readonly", 5).values())


def _a_cpu(sim):
    idle = [p for a, p in _window(sim, "node_cpu_seconds_total", 15).items() if _attr(a, "mode") == "idle"]
    if not idle:
        return False
    rate = sum(_rate(p) for p in idle) / len(idle)
    util = 0.0 if rate >= 1 else 100.0 if rate <= 0 else (1 - rate) * 100
    return util >= 90


def _a_memory(sim):
    avail = [_last(p) for p in _window(sim, "node_memory_MemAvailable_bytes", 15).values()]
    total = [_last(p) for p in _window(sim, "node_memory_MemTotal_bytes", 15).values()]
    return bool(avail and total) and max(total) > 0 and min(avail) / max(total) * 100 < 10


def _a_oom(sim):
    return sum(max(0.0, p[-1][1] - p[0][1]) for p in _window(sim, "node_vmstat_oom_kill", 10).values()) > 0


def _a_long_txn(sim):
    return any(max(v for _t, v in p) > 900 for p in _window(sim, "pg_stat_activity_xact_runtime", 10).values())


def _a_collection(sim):
    for name, bad in (("up", lambda v: v == 0), ("pg_scrape_collector_success", lambda v: v == 0),
                      ("node_scrape_collector_success", lambda v: v == 0),
                      ("pg_exporter_last_scrape_error", lambda v: v > 0)):
        if any(bad(_last(p)) for p in _window(sim, name, 10).values()):
            return True
    return False


def _a_pgbouncer_queue(sim):
    groups = defaultdict(list)
    for attrs, pts in _window(sim, "pgbouncer_pools_client_maxwait_seconds", 10).items():
        groups[(_attr(attrs, "database"), _attr(attrs, "user"))].append(sum(v for _t, v in pts) / len(pts))
    return any(sum(v) / len(v) > 1 for v in groups.values())


def _a_pgbouncer_down(sim):
    return any(_last(p) == 0 for p in _window(sim, "pgbouncer_up", 5).values())


def _a_conn_budget(sim):
    backends = sum(_last(p) for p in _window(sim, "pg_stat_database_num_backends", 10).values())
    mx = max((_last(p) for p in _window(sim, "max_connections_connection_count", 10).values()), default=0)
    return mx > 0 and backends / mx * 100 >= 90


def _a_deadlocks(sim):
    return sum(_increase(p) for p in _window(sim, "pg_stat_database_deadlocks_total", 15).values()) > 0


def _a_pg_down(sim):
    return any(_last(p) == 0 for p in _window(sim, "pg_up", 5).values())


def _a_restarted(sim):
    return sum(_increase(p) for p in _window(sim, "postgresql_restarts_total", 10).values()) > 0


def _a_pool_timeouts(sim):
    return sum(_increase(p) for p in _window(sim, "pgrst_db_pool_timeouts_total", 10).values()) > 0


def _a_repl_lag(sim):
    phys = [_last(p) for p in _window(sim, "physical_replication_lag_physical_replication_lag_seconds", 10).values()]
    replay = [_last(p) for p in _window(sim, "pg_stat_replication_replay_lag", 10).values()]
    return max([0.0, *phys, *replay]) > 300


def _a_slot_wal(sim):
    return any(_last(p) / 1024 ** 3 > 10 for p in _window(sim, "replication_slots_max_lag_bytes", 30).values())


def _a_supavisor(sim):
    out = defaultdict(lambda: [0.0, 0.0])
    for attrs, pts in _window(sim, "supavisor_pool_connections_checked_out", 15).items():
        out[(_attr(attrs, "tenant"), _attr(attrs, "mode"))][0] += _last(pts)
    for attrs, pts in _window(sim, "supavisor_pool_connections_idle", 15).items():
        out[(_attr(attrs, "tenant"), _attr(attrs, "mode"))][1] += _last(pts)
    return any(co + idle > 0 and co / (co + idle) * 100 >= 95 for co, idle in out.values())


ALERTS = {
    "auth_api_server_errors": _a_auth_errors,
    "auth_service_is_down": _a_auth_down,
    "disk_io_saturation": _a_disk_io,
    "filesystem_nearly_full": _a_fs_full,
    "filesystem_is_read_only": _a_fs_readonly,
    "host_cpu_saturation": _a_cpu,
    "host_memory_pressure": _a_memory,
    "host_oom_kill_detected": _a_oom,
    "long_running_transaction": _a_long_txn,
    "metrics_collection_is_failing": _a_collection,
    "pgbouncer_clients_queueing_for_connections": _a_pgbouncer_queue,
    "pgbouncer_is_down": _a_pgbouncer_down,
    "postgresql_connection_budget_nearly_exhausted": _a_conn_budget,
    "postgresql_deadlocks_detected": _a_deadlocks,
    "postgresql_is_down": _a_pg_down,
    "postgresql_restarted": _a_restarted,
    "postgrest_database_pool_timeouts": _a_pool_timeouts,
    "replication_lag_is_high": _a_repl_lag,
    "replication_slot_retaining_wal": _a_slot_wal,
    "supavisor_pool_exhausted": _a_supavisor,
}

# Magnitudes used by the gt-supabase channels.
GT_VALUES = {
    "auth_api_server_errors": 0.12,
    "auth_service_is_down": 1.0,
    "disk_io_saturation": 0.97,
    "filesystem_nearly_full": 0.93,
    "filesystem_is_read_only": 1.0,
    "host_cpu_saturation": 0.96,
    "host_memory_pressure": 0.96,
    "host_oom_kill_detected": 1.0,
    "long_running_transaction": 1500.0,
    "metrics_collection_is_failing": 1.0,
    "pgbouncer_clients_queueing_for_connections": 20.0,
    "pgbouncer_is_down": 1.0,
    "postgresql_connection_budget_nearly_exhausted": 0.95,
    "postgresql_deadlocks_detected": 2.0,
    "postgresql_is_down": 1.0,
    "postgresql_restarted": 1.0,
    "postgrest_database_pool_timeouts": 3.0,
    "replication_lag_is_high": 600.0,
    "replication_slot_retaining_wal": 15.0,
    "supavisor_pool_exhausted": 1.0,
}


def _breached(sim) -> set[str]:
    return {key for key, fires in ALERTS.items() if fires(sim)}


def test_alert_table_covers_every_impact_key():
    from generators.supabase_otel import SPEC

    assert set(ALERTS) == set(SPEC.impact_keys) == set(GT_VALUES)


@pytest.mark.parametrize("seed", [3, 7, 11])
def test_healthy_run_breaches_no_alert(monkeypatch, seed):
    assert _breached(_simulate(monkeypatch, seed=seed)) == set()


@pytest.mark.parametrize("key", sorted(GT_VALUES))
def test_fault_alone_breaches_only_its_own_alert(monkeypatch, key):
    assert _breached(_simulate(monkeypatch, fault={key: GT_VALUES[key]})) == {key}


# Metrics and labels the alert queries read. A missing column is a verification_exception.
ALERT_METRICS = {
    "gotrue_running": None, "node_disk_io_time_weighted_seconds_total": "device",
    "node_filesystem_avail_bytes": "mountpoint", "node_filesystem_size_bytes": "mountpoint",
    "node_filesystem_device_error": "mountpoint", "node_memory_MemAvailable_bytes": None,
    "node_memory_SwapTotal_bytes": None, "node_memory_SwapFree_bytes": None,
    "pg_stat_activity_xact_runtime": None, "up": None, "pg_scrape_collector_success": None,
    "node_scrape_collector_success": None, "pg_exporter_last_scrape_error": None,
    "pgbouncer_up": None, "max_connections_connection_count": None,
    "connection_stats_connection_count": None, "pg_stat_database_deadlocks_total": "datname",
    "pg_up": None, "pgrst_db_pool_waiting": None, "pg_stat_replication_replay_lag": None,
    "physical_replication_lag_is_connected_to_primary": None,
    "replication_slots_max_lag_bytes": None, "supavisor_pool_connections_checked_out": "tenant",
    "supavisor_pool_connections_idle": "tenant", "supavisor_connections_active": "tenant",
    "pg_stat_database_xact_commit_total": "datname", "http_status_codes_total": "code",
    "pgbouncer_pools_client_maxwait_seconds": "database",
    "pgbouncer_pools_client_waiting_connections": "user",
}


@pytest.mark.parametrize("name", sorted(ALERT_METRICS))
def test_every_metric_the_alerts_read_is_emitted_and_declared(monkeypatch, name):
    from generators.supabase_otel.generator import METRIC_NAMES

    assert name in METRIC_NAMES
    sim = _simulate(monkeypatch, minutes=2)
    keys = [attrs for (n, attrs) in sim["series"] if n == name]
    assert keys, f"{name} is not emitted"
    label = ALERT_METRICS[name]
    if label:
        assert all(label in dict(a) for a in keys), f"{name} lacks the {label} label"


def test_alerts_see_the_project_ref_as_a_resource_attribute(monkeypatch):
    sim = _simulate(monkeypatch, minutes=1)
    for res in sim["resources"]:
        attrs = {a["key"]: next(iter(a["value"].values())) for a in res["attributes"]}
        assert attrs["supabase_project_ref"] == "demo-project"


def test_counters_the_alerts_increase_stay_flat_when_healthy(monkeypatch):
    """INCREASE(...) > 0 fires these alerts, so a healthy run must not move them."""
    sim = _simulate(monkeypatch)
    for name in ("pg_stat_database_deadlocks_total", "postgresql_restarts_total",
                 "pgrst_db_pool_timeouts_total"):
        pts = [p for (n, _a), p in sim["series"].items() if n == name]
        assert pts and all(p[-1][1] == p[0][1] for p in pts), name


# --- log baseline: error panels are not empty on a healthy project --------------------
# "Gateway 4xx Errors" and "Top 15 SQLSTATE Error Codes" read edge_logs / postgres_logs.
# No package alert reads the log streams (they are all TS metric alerts), so a small
# ambient error rate is safe; 5xx and pool errors stay fault-only.

import random as _random  # noqa: E402

from generators.supabase_otel.generator import _generate_service_logs  # noqa: E402


class _LogClient:
    def build_log_record(self, severity, body, attributes=None):
        return {"severity": severity, "body": body, "attrs": dict(attributes or {})}


def _logs(ticks, seed, *, pool_exhaustion=False, slow_rate=0.0):
    rng = _random.Random(seed)
    out = []
    for _ in range(ticks):
        out += _generate_service_logs(rng, "ref", _LogClient(), pool_exhaustion, slow_rate, 1.0)
    return out


def _service(records, name):
    return [r for r in records if r["attrs"].get("supabase.service") == name]


def test_healthy_edge_logs_include_some_4xx_but_no_5xx():
    for seed in range(5):
        edge = _service(_logs(300, seed), "edge_logs")
        codes = [r["attrs"]["http.response.status_code"] for r in edge]
        four = [c for c in codes if 400 <= c < 500]
        assert four, f"seed {seed}: no 4xx, the Gateway 4xx panel would be empty"
        assert 0.01 <= len(four) / len(codes) <= 0.15, f"seed {seed}: 4xx share {len(four) / len(codes):.3f}"
        assert not [c for c in codes if c >= 500], f"seed {seed}: 5xx must stay fault-only"


def test_healthy_postgres_logs_include_some_sqlstate_errors():
    for seed in range(5):
        pg = _service(_logs(300, seed), "postgres_logs")
        errors = [r for r in pg if r["attrs"]["db.response.status_code"] != "00000"]
        assert errors, f"seed {seed}: no SQLSTATE errors, the Top 15 SQLSTATE panel would be empty"
        assert 0.01 <= len(errors) / len(pg) <= 0.12, f"seed {seed}: error share {len(errors) / len(pg):.3f}"
        assert all(r["severity"] == "ERROR" for r in errors)


def test_pool_exhaustion_still_raises_5xx_and_sql_errors():
    records = _logs(100, 1, pool_exhaustion=True)
    assert [r for r in _service(records, "edge_logs") if r["attrs"]["http.response.status_code"] >= 500]
    pg = _service(records, "postgres_logs")
    healthy = _service(_logs(100, 1), "postgres_logs")
    share = lambda rs: sum(r["attrs"]["db.response.status_code"] != "00000" for r in rs) / len(rs)  # noqa: E731
    assert share(pg) > share(healthy)


def test_realtime_billing_metrics_match_connection_panels():
    import fnmatch

    rt = _service(_logs(200, 3), "realtime_logs")
    conns = [r for r in rt if "supabase.realtime.connected" in r["attrs"]]
    assert conns
    for r in conns:
        a = r["attrs"]
        assert r["body"].startswith("Billing metrics:")
        assert 0 < a["supabase.realtime.connected"] <= a["supabase.realtime.connected_cluster"]
        # Peak Utilisation gauge = connected_cluster / limit must stay a healthy fraction.
        assert a["supabase.realtime.connected_cluster"] / a["supabase.realtime.limit"] <= 0.7
    billing = [r["body"] for r in rt if r["body"].startswith("Billing metrics:")]
    assert any(fnmatch.fnmatch(b, "*presence_events]*") for b in billing)
    assert any(fnmatch.fnmatch(b, "*:events]*") for b in billing)
    assert all(r["attrs"]["supabase.realtime.external_id"] for r in rt)
