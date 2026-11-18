"""Azure conversation-store and prompt-guard channels (07, 08, 09, 12, 19).

Each signal metric sits in its baseline band before the fault, deviates during it while the
status log says ANOMALY (with the bad configuration visible), and converges back after the
opsctl fix with the corrected configuration. Channels 07, 12 and 19 share conversation-store
metrics: each metric moves only for the channel(s) that cause it.
"""
from __future__ import annotations

import pytest

from app import ops
from tests._azure_harness import (
    OPS_ACTOR, PROFILE, REG, SCN, phase, records, values,
)

CS = "conversation-store"
PG = "prompt-guard"
CHANNELS = [7, 8, 9, 12, 19]


def _within(ch: int) -> float:
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _read(ch, which, **kw):
    return records(phase(ch, which, **kw))


def _vals(metrics, svc, name):
    v = values(metrics, svc, name)
    assert v, f"{svc} emitted no {name}"
    return v


def _status(logs, svc, op, key="db.status"):
    return {r["attrs"][key] for r in logs
            if r["service"] == svc and r["attrs"].get("operation") == op and key in r["attrs"]}


def _attr(logs, svc, op, key):
    """Values of *key* on the service's status log *op* (cause and fix lines excluded)."""
    got = {r["attrs"][key] for r in logs
           if r["service"] == svc and r["attrs"].get("operation") == op and key in r["attrs"]}
    return {str(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v for v in got}


def _fp(ch):
    return SCN._channels[ch]["fault_params"]


def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _assert_cmd_lines(logs, ch):
    for line in _cmd_lines(ch):
        cmd = ops.parse_command(line, PROFILE)
        hits = [r["body"] for r in logs
                if cmd.name in r["body"] and f"={OPS_ACTOR}" in r["body"]
                and r["body"].startswith(("azure-activity:", "config-service:"))]
        assert len(hits) == 1, (line, hits)


def _in(vals, lo, hi):
    return all(lo <= x <= hi for x in vals)


CS_BASELINE = {
    "conversation_store.normalized_ru_consumption_pct": (35.0, 60.0),
    "conversation_store.request_units_consumed": (3500, 6000),
    "conversation_store.hot_partition_pct": (12.0, 18.0),
    "conversation_store.throttled_requests_429": (0, 2),
    "conversation_store.replication_lag_ms": (40, 400),
    "conversation_store.write_conflicts_per_min": (0, 0),
}


def _baseline_except(metrics, moving):
    for name, (lo, hi) in CS_BASELINE.items():
        if name in moving:
            continue
        assert _in(_vals(metrics, CS, name), lo, hi), (name, values(metrics, CS, name))


# -- ch07: tenant-4417 hot partition ------------------------------------------

def test_ch07_hot_partition_phases():
    fp = _fp(7)
    logs, metrics = _read(7, "before")
    _baseline_except(metrics, ())
    assert _status(logs, CS, "partition_heat") == {"NOMINAL"}
    assert _attr(logs, CS, "partition_heat", "ratelimit.tenant_rpm") == {"12000"}

    logs, metrics = _read(7, "during")
    assert _in(_vals(metrics, CS, "conversation_store.hot_partition_pct"), 55.0, 92.0)
    assert min(_vals(metrics, CS, "conversation_store.throttled_requests_429")) >= 300
    # total consumption stays about half of provisioned: not the autoscale ceiling
    assert _in(_vals(metrics, CS, "conversation_store.normalized_ru_consumption_pct"), 35.0, 60.0)
    assert _in(_vals(metrics, CS, "conversation_store.request_units_consumed"),
               fp["consumed_ru"]["randint"][0] - 1300, fp["provisioned_ru"] * 0.6)
    _baseline_except(metrics, {"conversation_store.hot_partition_pct",
                               "conversation_store.throttled_requests_429"})
    assert _status(logs, CS, "partition_heat") == {"ANOMALY"}
    assert _attr(logs, CS, "partition_heat", "ratelimit.tenant_rpm") == {"12000"}
    assert _attr(logs, CS, "partition_heat", "db.hottest_partition_key") == {fp["hot_partition"]}

    logs, metrics = _read(7, "after", since=_within(7))
    _baseline_except(metrics, ())
    assert _status(logs, CS, "partition_heat") == {"NOMINAL"}
    assert _attr(logs, CS, "partition_heat", "ratelimit.tenant_rpm") == {"600"}
    _assert_cmd_lines(logs, 7)


def test_ch07_converges_rather_than_snaps():
    _, metrics = _read(7, "during", elapsed=5)
    assert max(_vals(metrics, CS, "conversation_store.hot_partition_pct")) < 40.0
    _, metrics = _read(7, "after", since=10)
    assert min(_vals(metrics, CS, "conversation_store.hot_partition_pct")) > 30.0
    assert min(_vals(metrics, CS, "conversation_store.throttled_requests_429")) > 2


# -- ch19: autoscale ceiling ----------------------------------------------------

def test_ch19_autoscale_ceiling_phases():
    fp = _fp(19)
    logs, metrics = _read(19, "before")
    _baseline_except(metrics, ())
    assert _status(logs, CS, "ru_stats") == {"NOMINAL"}
    assert _attr(logs, CS, "ru_stats", "cosmos.autoscale_max_ru") == {str(fp["max_rus"])}

    logs, metrics = _read(19, "during")
    assert _in(_vals(metrics, CS, "conversation_store.normalized_ru_consumption_pct"), 98.0, 100.0)
    assert _in(_vals(metrics, CS, "conversation_store.request_units_consumed"),
               fp["consumed_rus"]["randint"][0], fp["max_rus"])
    assert min(_vals(metrics, CS, "conversation_store.throttled_requests_429")) >= 150
    # all partitions evenly high: no hot partition (differs from ch07)
    _baseline_except(metrics, {"conversation_store.normalized_ru_consumption_pct",
                               "conversation_store.request_units_consumed",
                               "conversation_store.throttled_requests_429"})
    assert _status(logs, CS, "ru_stats") == {"ANOMALY"}
    assert _attr(logs, CS, "ru_stats", "cosmos.autoscale_max_ru") == {str(fp["max_rus"])}

    logs, metrics = _read(19, "after", since=_within(19))
    norm = _vals(metrics, CS, "conversation_store.normalized_ru_consumption_pct")
    assert _in(norm, 50.0, 60.0), norm
    assert _in(_vals(metrics, CS, "conversation_store.throttled_requests_429"), 0, 0)
    assert _status(logs, CS, "ru_stats") == {"NOMINAL"}
    assert _attr(logs, CS, "ru_stats", "cosmos.autoscale_max_ru") == {str(fp["new_max_rus"])}
    _assert_cmd_lines(logs, 19)


def test_ch19_converges_rather_than_snaps():
    _, metrics = _read(19, "after", since=10)
    assert min(_vals(metrics, CS, "conversation_store.normalized_ru_consumption_pct")) > 70.0
    assert min(_vals(metrics, CS, "conversation_store.throttled_requests_429")) > 0


# -- ch12: multi-region write conflicts ----------------------------------------

def test_ch12_write_conflicts_phases():
    logs, metrics = _read(12, "before")
    _baseline_except(metrics, ())
    assert _status(logs, CS, "replication_health") == {"NOMINAL"}
    assert _attr(logs, CS, "replication_health", "cosmos.write_mode") == {"single-region"}

    logs, metrics = _read(12, "during")
    fp = _fp(12)
    assert _in(_vals(metrics, CS, "conversation_store.write_conflicts_per_min"),
               fp["conflict_count"]["randint"][0], fp["conflict_count"]["randint"][1])
    assert _in(_vals(metrics, CS, "conversation_store.replication_lag_ms"),
               fp["replication_lag_ms"]["randint"][0], fp["replication_lag_ms"]["randint"][1])
    _baseline_except(metrics, {"conversation_store.write_conflicts_per_min",
                               "conversation_store.replication_lag_ms"})
    assert _status(logs, CS, "replication_health") == {"ANOMALY"}
    assert _attr(logs, CS, "replication_health", "cosmos.write_mode") == {"multi-region"}
    assert _attr(logs, CS, "replication_health", "cosmos.multi_region_writes") == {True}
    assert _attr(logs, CS, "replication_health", "cosmos.history_reconcile") == {"pending"}

    logs, metrics = _read(12, "after", since=_within(12))
    _baseline_except(metrics, ())
    assert _status(logs, CS, "replication_health") == {"NOMINAL"}
    assert _attr(logs, CS, "replication_health", "cosmos.write_mode") == {"single-region"}
    assert _attr(logs, CS, "replication_health", "cosmos.history_reconcile") == {"completed"}
    _assert_cmd_lines(logs, 12)


def test_ch12_converges_rather_than_snaps():
    _, metrics = _read(12, "after", since=10)
    assert min(_vals(metrics, CS, "conversation_store.write_conflicts_per_min")) > 0
    assert min(_vals(metrics, CS, "conversation_store.replication_lag_ms")) > 400


def test_ch12_write_mode_follows_the_account_patch():
    ctl = phase(12, "during")
    ctl.record_applied("cosmos-account/azureai-cosmos", "patch", {"multi-region-writes": "false"})
    logs, metrics = records(ctl)
    assert _attr(logs, CS, "replication_health", "cosmos.write_mode") == {"single-region"}
    # conflicts and the reconcile state only converge once the job has also run
    assert _attr(logs, CS, "replication_health", "cosmos.history_reconcile") == {"pending"}
    assert min(_vals(metrics, CS, "conversation_store.write_conflicts_per_min")) >= 40


# -- ch08: content filter threshold --------------------------------------------

def test_ch08_block_rate_phases():
    fp = _fp(8)
    m = "prompt_guard.block_rate_pct"
    logs, metrics = _read(8, "before")
    assert _in(_vals(metrics, PG, m), 0.0, 3.0)
    assert _status(logs, PG, "window_totals", "guard.status") == {"NOMINAL"}
    assert _attr(logs, PG, "window_totals", "contentfilter.severity_threshold") == {str(fp["old_threshold"])}

    logs, metrics = _read(8, "during")
    assert _in(_vals(metrics, PG, m), 17.0, 46.0), values(metrics, PG, m)
    assert _status(logs, PG, "window_totals", "guard.status") == {"ANOMALY"}
    assert _attr(logs, PG, "window_totals", "contentfilter.severity_threshold") == {str(fp["new_threshold"])}
    assert _attr(logs, PG, "window_totals", "contentfilter.policy") == {fp["policy_name"]}
    # a threshold change, not an attack
    assert max(_vals(metrics, PG, "prompt_guard.injection_attempts_per_min")) < 5.0
    assert max(_vals(metrics, PG, "prompt_guard.injection_score")) < 0.25

    logs, metrics = _read(8, "after", since=_within(8))
    assert _in(_vals(metrics, PG, m), 0.0, 3.0)
    assert _status(logs, PG, "window_totals", "guard.status") == {"NOMINAL"}
    assert _attr(logs, PG, "window_totals", "contentfilter.severity_threshold") == {str(fp["old_threshold"])}
    _assert_cmd_lines(logs, 8)


def test_ch08_converges_rather_than_snaps():
    _, metrics = _read(8, "after", since=10)
    assert min(_vals(metrics, PG, "prompt_guard.block_rate_pct")) > 5.0


# -- ch09: prompt injection burst ----------------------------------------------

def test_ch09_injection_burst_phases():
    fp = _fp(9)
    ip = fp["top_client_ip"]
    per_min = (fp["injection_attempts"]["randint"][0] // fp["window_minutes"],
               fp["injection_attempts"]["randint"][1] // fp["window_minutes"])
    m = "prompt_guard.injection_attempts_per_min"
    logs, metrics = _read(9, "before")
    assert _in(_vals(metrics, PG, m), 0.0, 3.0)
    assert _in(_vals(metrics, PG, "prompt_guard.injection_score"), 0.0, 0.25)
    assert _status(logs, PG, "injection_shield", "guard.status") == {"NOMINAL"}
    assert _attr(logs, PG, "injection_shield", "waf.custom_rule") == {"none"}

    logs, metrics = _read(9, "during")
    assert _in(_vals(metrics, PG, m), *per_min), values(metrics, PG, m)
    assert _in(_vals(metrics, PG, "prompt_guard.injection_score"), 0.62, 0.97)
    # content filter untouched: the block rate stays at baseline
    assert _in(_vals(metrics, PG, "prompt_guard.block_rate_pct"), 0.0, 3.0)
    assert _status(logs, PG, "injection_shield", "guard.status") == {"ANOMALY"}
    assert _attr(logs, PG, "injection_shield", "security.top_client_ip") == {ip}
    assert _attr(logs, PG, "injection_shield", "waf.custom_rule") == {"none"}
    assert _attr(logs, PG, "injection_shield", "security.blocked_ip") == {"none"}

    logs, metrics = _read(9, "after", since=_within(9))
    assert _in(_vals(metrics, PG, m), 0.0, 3.0)
    assert _in(_vals(metrics, PG, "prompt_guard.injection_score"), 0.0, 0.25)
    assert _status(logs, PG, "injection_shield", "guard.status") == {"NOMINAL"}
    assert _attr(logs, PG, "injection_shield", "waf.custom_rule") == {f"block-{ip}"}
    assert _attr(logs, PG, "injection_shield", "security.blocked_ip") == {ip}
    _assert_cmd_lines(logs, 9)


def test_ch09_converges_rather_than_snaps():
    _, metrics = _read(9, "after", since=10)
    assert min(_vals(metrics, PG, "prompt_guard.injection_attempts_per_min")) > 100.0


# -- shared: cause events, commands, scope -------------------------------------

@pytest.mark.parametrize("ch", CHANNELS)
def test_cause_event_precedes_symptoms(ch):
    events = REG[ch]["infrastructure_events"]
    assert len(events) == 1 and events[0]["offset_seconds"] < 0
    assert events[0]["service"] in SCN.services


@pytest.mark.parametrize("ch", CHANNELS)
def test_commands_parse_with_the_scenario_profile(ch):
    lines = _cmd_lines(ch)
    assert lines
    for line in lines:
        assert ops.parse_command(line, PROFILE).verb in ops.ops_profile.verbs(PROFILE)
    assert len(ops.expected_commands(REG[ch]["remediation_command"], PROFILE)) == len(lines)


@pytest.mark.parametrize("ch", CHANNELS)
def test_fault_scope_names_services(ch):
    workloads = REG[ch]["fault_scope"]["workloads"]
    assert workloads and all(w in SCN.services for w in workloads)
