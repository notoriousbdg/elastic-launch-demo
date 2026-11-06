"""Technology-specific tests for the ``microsoft_sqlserver_otel`` generator.

Standard emit/registry tests live in tests/test_generators_emit.py
(parametrized over GENERATORS) and run for this generator automatically.
"""
from __future__ import annotations

import random

import importlib
import os
import sys
import threading

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "tests"))

try:
    import dotenv  # noqa: F401
    _HAS_DOTENV = True
except ImportError:
    _HAS_DOTENV = False

pytestmark = pytest.mark.skipif(
    not _HAS_DOTENV,
    reason="Generator modules require dotenv via app.telemetry; run inside Docker",
)


class _StubClient:
    def __init__(self):
        self.metrics: list[dict] = []
        self.logs: list[dict] = []
        self.metric_resources: list[dict] = []

    def send_metrics_scoped(self, resource: dict, scope: dict, metrics: list[dict]) -> None:
        self.metrics.extend(metrics)
        self.metric_resources.append(resource)

    def send_logs_scoped(self, resource: dict, scope: dict, records: list[dict]) -> None:
        self.logs.extend(records)

    def build_log_record(self, severity, body, attributes=None, event_name=None, **kwargs):
        from app.telemetry import OTLPClient
        return OTLPClient.build_log_record(
            self, severity=severity, body=body, attributes=attributes, event_name=event_name,
        )

    def close(self):
        pass


def _run() -> _StubClient:
    mod = importlib.import_module("generators.microsoft_sqlserver_otel.generator")
    client = _StubClient()
    stop_event = threading.Event()
    scenario_data = {
        "namespace": "mfg",
        "channel_registry": {},
        "generator_config": {},
    }
    threading.Timer(0.2, stop_event.set).start()
    mod.run(client, stop_event, scenario_data=scenario_data)
    return client


def _resource_attr(resource: dict, key: str):
    for attr in resource.get("attributes", []):
        if attr.get("key") == key:
            val = attr.get("value", {})
            return val.get("stringValue") or val.get("intValue") or val.get("doubleValue")
    return None


def _dp_attr_keys(metric: dict) -> set[str]:
    keys = set()
    for kind in ("gauge", "sum"):
        for dp in (metric.get(kind) or {}).get("dataPoints", []):
            for attr in dp.get("attributes") or []:
                keys.add(attr.get("key"))
    return keys


class TestSQLServerGeneratorSpecific:
    def test_logs_use_otlp_camelcase(self):
        client = _run()
        assert client.logs, "expected query_sample / top_query logs"
        for rec in client.logs:
            assert "timeUnixNano" in rec
            assert "eventName" in rec
            assert rec["eventName"] in {"db.server.query_sample", "db.server.top_query"}
            assert isinstance(rec.get("attributes"), list)
            assert rec.get("body", {}).get("stringValue")

    def test_cpu_time_is_seconds_double(self):
        client = _run()
        cpu_values = []
        for rec in client.logs:
            for attr in rec.get("attributes") or []:
                if attr.get("key") == "sqlserver.cpu_time":
                    assert "doubleValue" in attr["value"]
                    cpu_values.append(attr["value"]["doubleValue"])
        assert cpu_values
        # Seconds, not milliseconds — dashboard multiplies by 1000.
        assert all(v < 500 for v in cpu_values)

    def test_database_name_on_resource(self):
        client = _run()
        db_names = {
            _resource_attr(r, "sqlserver.database.name")
            for r in client.metric_resources
            if _resource_attr(r, "sqlserver.database.name")
        }
        assert db_names, "per-database metrics must set resource sqlserver.database.name"
        assert any(n.startswith("mfg_") for n in db_names)

    def test_io_metrics_use_file_attrs_not_db_datapoint(self):
        client = _run()
        io_metrics = [m for m in client.metrics if m.get("name") == "sqlserver.database.io"]
        assert io_metrics
        for m in io_metrics:
            keys = _dp_attr_keys(m)
            assert "file_type" in keys
            assert "direction" in keys
            assert "logical_filename" in keys
            assert "physical_filename" in keys
            assert "sqlserver.database.name" not in keys

    def test_latency_emitted_as_double(self):
        client = _run()
        latency = [m for m in client.metrics if m.get("name") == "sqlserver.database.latency"]
        assert latency
        for m in latency:
            for dp in m["sum"]["dataPoints"]:
                assert "asDouble" in dp, "latency must be asDouble (metadata value_type: double)"
                assert dp["asDouble"] > 0

    def test_emits_blocked_query_samples(self):
        """Query Performance 'Recent query samples with blocking' filters > 0."""
        # Run several cycles — baseline block rate is probabilistic.
        blocked = []
        for _ in range(8):
            client = _run()
            for rec in client.logs:
                if rec.get("eventName") != "db.server.query_sample":
                    continue
                for attr in rec.get("attributes") or []:
                    if attr.get("key") == "sqlserver.blocking_session_id":
                        val = attr["value"].get("intValue")
                        if val is not None and int(val) > 0:
                            blocked.append(int(val))
        assert blocked, "expected some query_sample logs with blocking_session_id > 0"


from generators.microsoft_sqlserver_otel import SPEC
from generators.microsoft_sqlserver_otel.generator import (
    SQLServerMetricState,
    _generate_instance_metrics,
)


def _gauge(m):
    dps = m.get("gauge", {}).get("dataPoints") or []
    return float(dps[0].get("asDouble", dps[0].get("asInt", 0)))


def test_healthy_baseline_no_chaos():
    rng = random.Random(0)
    state = SQLServerMetricState(rng, ["app"])
    metrics = _generate_instance_metrics(state, rng, None)
    by = {m["name"]: m for m in metrics}
    assert _gauge(by["sqlserver.deadlock.rate"]) < 1.0
    assert _gauge(by["sqlserver.page.life_expectancy"]) >= 300
    assert _gauge(by["sqlserver.page.buffer_cache.hit_ratio"]) >= 95
    assert _gauge(by["sqlserver.processes.blocked"]) <= 1
    assert _gauge(by["sqlserver.lock.wait.rate"]) <= 10
    assert _gauge(by["sqlserver.login.rate"]) <= 50


def test_page_life_key_alone():
    rng = random.Random(1)
    state = SQLServerMetricState(rng, ["app"])
    metrics = _generate_instance_metrics(state, rng, {"page_life_expectancy_low": 100})
    ple = next(m for m in metrics if m["name"] == "sqlserver.page.life_expectancy")
    assert _gauge(ple) == 100


def test_deadlock_rate_key_alone():
    rng = random.Random(2)
    state = SQLServerMetricState(rng, ["app"])
    metrics = _generate_instance_metrics(state, rng, {"deadlocks_detected": 3.5})
    d = next(m for m in metrics if m["name"] == "sqlserver.deadlock.rate")
    assert _gauge(d) == 3.5

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.microsoft_sqlserver_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['blocked_processes', 'buffer_cache_hit_ratio_low', 'database_i_o_latency_high', 'deadlocks_detected', 'execution_errors', 'free_list_stalls', 'lock_timeouts', 'lock_wait_rate_high', 'login_rate_spike', 'memory_grants_pending', 'os_wait_duration_high', 'page_life_expectancy_low', 'tempdb_space_low', 'tempdb_version_store_large']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


# --- per-alert behaviour: each key alone breaches only its own alert ------------------
# Calls the per-cycle generators directly and evaluates the package alert ES|QL
# (v0.3.1) over the last WINDOW cycles.

from generators.microsoft_sqlserver_otel.generator import _generate_db_metrics  # noqa: E402

DEFAULTS = {key: meta["default"] for key, meta in _SPEC_FOR_ALERT_KEYS.impact_keys.items()}
DATABASES = ["app", "reporting"]
SEEDS = range(8)
WINDOW = 6


def _simulate(chaos_by_cycle, seed):
    rng = random.Random(seed)
    state = SQLServerMetricState(rng, DATABASES)
    series: dict[tuple, list[float]] = {}

    def record(metric, scope=""):
        dp = (metric.get("gauge") or metric["sum"])["dataPoints"][0]
        value = float(dp.get("asDouble", dp.get("asInt", 0)))
        attrs = tuple(sorted(
            (a["key"], next(iter(a["value"].values()))) for a in dp.get("attributes", [])
        ))
        series.setdefault((metric["name"], scope, attrs), []).append(value)

    for chaos in chaos_by_cycle:
        state.tick(rng, DATABASES, chaos)
        for m in _generate_instance_metrics(state, rng, chaos):
            record(m)
        for db in DATABASES:
            for m in _generate_db_metrics(state, db, rng, chaos):
                record(m, db)
    return {k: v[-WINDOW:] for k, v in series.items()}


def _all(series, name, **attr):
    return [
        w for (n, _scope, attrs), w in series.items()
        if n == name and all((k, v) in attrs for k, v in attr.items())
    ]


def _breached(series):
    def one(name, **attr):
        (window,) = _all(series, name, **attr)
        return window

    io_latency = False
    for (name, scope, attrs), lat in series.items():
        if name != "sqlserver.database.latency":
            continue
        ops = series[("sqlserver.database.operations", scope, attrs)]
        d_ops = ops[-1] - ops[0]
        if d_ops > 0 and (lat[-1] - lat[0]) / d_ops > 0.1:
            io_latency = True
    os_wait = any(
        w[-1] - w[0] > 60.0 for w in _all(series, "sqlserver.os.wait.duration")
    )
    errors = sum(sum(w) for w in _all(series, "sqlserver.database.execution.errors"))
    return {
        "blocked_processes": max(one("sqlserver.processes.blocked")) > 0,
        "buffer_cache_hit_ratio_low": min(one("sqlserver.page.buffer_cache.hit_ratio")) < 95.0,
        "database_i_o_latency_high": io_latency,
        "deadlocks_detected": max(one("sqlserver.deadlock.rate")) > 0,
        "execution_errors": errors > 0,
        "free_list_stalls": max(one("sqlserver.page.buffer_cache.free_list.stalls.rate")) > 0,
        "lock_timeouts": max(one("sqlserver.lock.timeout.rate")) > 0,
        "lock_wait_rate_high": sum(one("sqlserver.lock.wait.rate")) / WINDOW > 10.0,
        "login_rate_spike": sum(one("sqlserver.login.rate")) / WINDOW > 50.0,
        "memory_grants_pending": max(one("sqlserver.memory.grants.pending.count")) > 0,
        "os_wait_duration_high": os_wait,
        "page_life_expectancy_low": min(one("sqlserver.page.life_expectancy")) < 300,
        "tempdb_space_low": min(
            one("sqlserver.database.tempdb.space", **{"tempdb.state": "free"})) < 102400,
        "tempdb_version_store_large": max(
            one("sqlserver.database.tempdb.version_store.size")) > 1048576,
    }


# GT channel values.
FAULTS = {
    "blocked_processes": 50.0,
    "buffer_cache_hit_ratio_low": 200.0,
    "database_i_o_latency_high": 75.0,
    "deadlocks_detected": 50.0,
    "execution_errors": 50.0,
    "free_list_stalls": 50.0,
    "lock_timeouts": 50.0,
    "lock_wait_rate_high": 75.0,
    "login_rate_spike": 375.0,
    "memory_grants_pending": 500,
    "os_wait_duration_high": 450.0,
    "page_life_expectancy_low": 300.0,
    "tempdb_space_low": 102400.0,
    "tempdb_version_store_large": 7864320.0,
}


def _hits(breached):
    return sorted(k for k, hit in breached.items() if hit)


def _faulted(key, value, cycles=WINDOW + 2):
    return [{**DEFAULTS, key: value}] * cycles


def test_fault_table_covers_every_impact_key():
    assert sorted(FAULTS) == sorted(_SPEC_FOR_ALERT_KEYS.impact_keys)


@pytest.mark.parametrize("key", sorted(FAULTS))
def test_key_alone_breaches_only_its_own_alert(key):
    for seed in SEEDS:
        breached = _breached(_simulate(_faulted(key, FAULTS[key]), seed))
        assert _hits(breached) == [key], f"seed {seed}: {key} fired {_hits(breached)}"


def test_healthy_run_breaches_nothing():
    for seed in SEEDS:
        for chaos in (dict(DEFAULTS), None):
            breached = _breached(_simulate([chaos] * (WINDOW + 2), seed))
            assert not _hits(breached), f"seed {seed} chaos={chaos is not None}: {_hits(breached)}"


@pytest.mark.parametrize("key", sorted(FAULTS))
def test_alert_clears_after_the_fault_resolves(key):
    cycles = _faulted(key, FAULTS[key], 6) + [dict(DEFAULTS)] * (WINDOW + 2)
    for seed in SEEDS:
        breached = _breached(_simulate(cycles, seed))
        assert not _hits(breached), f"seed {seed}: {_hits(breached)} still firing after {key} resolved"


@pytest.mark.parametrize("key,breaching,healthy", [
    ("page_life_expectancy_low", 0.0, 3000.0),      # 0, the worst PLE, must breach
    ("tempdb_space_low", 0.0, 1024000.0),            # 0 kB free must breach
    ("database_i_o_latency_high", 1.0, 0.0),         # documented: any value above 0
    ("os_wait_duration_high", 61.0, 0.0),
    ("buffer_cache_hit_ratio_low", 50.0, 0.0),
])
def test_key_units_match_the_documented_thresholds(key, breaching, healthy):
    for seed in SEEDS:
        hit = _breached(_simulate(_faulted(key, breaching), seed))[key]
        miss = _breached(_simulate(_faulted(key, healthy), seed))[key]
        assert hit and not miss, f"seed {seed}: {key} breaching={hit} healthy={miss}"
