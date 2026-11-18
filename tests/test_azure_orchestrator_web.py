"""Azure channels 04, 15, 16, 18 (assistant-web, chat-orchestrator) plus the secondary effects.

Each channel's signal metric sits in its baseline band before the fault, leaves it during the
fault (status ANOMALY, bad configuration on the status log) and converges back after the opsctl
fix (corrected configuration, NOMINAL), not instantly.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app import ops
from tests._azure_harness import (
    OPS_ACTOR, PROFILE, REG, SCN, attr_values, bodies, controller, phase, records, values,
)

WEB, ORCH = "assistant-web", "chat-orchestrator"


def _within(ch: int) -> float:
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _sig(ch, which, svc, metric, *, cycles=4, **kw):
    logs, metrics = records(phase(ch, which, **kw), cycles=cycles)
    vals = values(metrics, svc, metric)
    assert vals, f"{svc} emitted no {metric}"
    return logs, metrics, vals


def _status(logs, svc, contains, key):
    return {r["attrs"].get(key) for r in logs if r["service"] == svc and contains in r["body"]}


def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _assert_fix_line(logs, ch, svc, target):
    hits = [b for b in bodies(logs, svc, target) if f"{OPS_ACTOR}" in b or "caller=" + OPS_ACTOR in b]
    assert len(hits) == len(_cmd_lines(ch)), (target, hits)
    assert "{actor}" not in hits[0]


def _cause_precedes(ch):
    evs = REG[ch]["infrastructure_events"]
    assert evs and evs[0]["offset_seconds"] < 0


# ── ch04: WAF policy v14 -> blocked share ───────────────────────────────────

def test_ch04_waf_blocked_phases():
    m = "assistant_web.waf_blocked_pct"
    logs, _, v = _sig(4, "before", WEB, m)
    assert all(0.1 <= x <= 1.2 for x in v), v
    assert _status(logs, WEB, "appgw.waf_summary", "appgw.status") == {"NOMINAL"}

    lo, hi = 38.0, 62.0
    logs, _, v = _sig(4, "during", WEB, m)
    assert all(lo <= x <= hi for x in v), v
    assert _status(logs, WEB, "appgw.waf_summary", "appgw.status") == {"ANOMALY"}
    assert attr_values(logs, WEB, "waf.policy_version") == {"v14"}
    assert attr_values(logs, WEB, "waf.rule_id") == {"942100"}

    logs, _, v = _sig(4, "after", WEB, m, since=_within(4))
    assert all(x < 1.2 for x in v), v
    assert _status(logs, WEB, "appgw.waf_summary", "appgw.status") == {"NOMINAL"}
    assert attr_values(logs, WEB, "waf.policy_version") == {"v13"}
    _assert_fix_line(logs, 4, WEB, "waf-policy-azureai-prod")
    _cause_precedes(4)


def test_ch04_converges_rather_than_snaps():
    _, _, early = _sig(4, "after", WEB, "assistant_web.waf_blocked_pct", since=10)
    assert min(early) > 5.0, early


def test_ch04_pinned_resource_matches_params():
    assert SCN.get_fault_params(4)["waf_policy"] == "waf-policy-azureai-prod"
    assert SCN.get_fault_params(4)["rule_id"] == "942100"


# ── ch15: agent-config v42 -> tool loop ─────────────────────────────────────

def test_ch15_tool_calls_phases():
    m = "chat.tool_calls_per_turn"
    logs, _, v = _sig(15, "before", ORCH, m)
    assert all(0 <= x <= 3 for x in v), v
    assert _status(logs, ORCH, "chat.agent_status", "chat.status") == {"NOMINAL"}

    p = SCN.get_fault_params(15)
    logs, _, v = _sig(15, "during", ORCH, m)
    assert all(25 <= x <= 120 for x in v), v
    assert _status(logs, ORCH, "chat.agent_status", "chat.status") == {"ANOMALY"}
    assert attr_values(logs, ORCH, "agent.config_version") == {p["agent_config"]}

    logs, _, v = _sig(15, "after", ORCH, m, since=_within(15))
    assert all(x <= p["normal_max_iterations"] for x in v), v
    assert _status(logs, ORCH, "chat.agent_status", "chat.status") == {"NOMINAL"}
    assert attr_values(logs, ORCH, "agent.config_version") == {"agent-config-v41"}
    _assert_fix_line(logs, 15, ORCH, "agent-config/chat-orchestrator")
    _cause_precedes(15)


def test_ch15_converges_and_token_cost_follows():
    _, _, early = _sig(15, "after", ORCH, "chat.tool_calls_per_turn", since=10)
    assert min(early) > 5, early
    _, _, base = _sig(15, "before", ORCH, "chat.token_cost_usd_per_conversation")
    _, _, dur = _sig(15, "during", ORCH, "chat.token_cost_usd_per_conversation")
    _, _, aft = _sig(15, "after", ORCH, "chat.token_cost_usd_per_conversation", since=_within(15))
    assert min(dur) > 2 * max(base) and max(aft) <= 1.1, (base, dur, aft)


# ── ch16: Azure OpenAI capacity 450K -> quota headroom ──────────────────────

def test_ch16_quota_headroom_phases():
    m = "chat.openai_quota_headroom_pct"
    logs, _, v = _sig(16, "before", ORCH, m)
    assert all(20.0 <= x <= 30.0 for x in v), v
    assert _status(logs, ORCH, "chat.quota_status", "chat.status") == {"NOMINAL"}
    assert attr_values(logs, ORCH, "openai.capacity_tpm") == {"600000"}

    p = SCN.get_fault_params(16)
    logs, _, v = _sig(16, "during", ORCH, m)
    assert all(0.0 <= x <= 2.0 for x in v), v
    assert _status(logs, ORCH, "chat.quota_status", "chat.status") == {"ANOMALY"}
    assert attr_values(logs, ORCH, "openai.capacity_tpm") == {str(p["tpm_quota"])}
    assert {r["attrs"]["gen_ai.deployment"] for r in logs
            if "chat.quota_status" in r["body"]} == {p["deployment_name"]}

    logs, _, v = _sig(16, "after", ORCH, m, since=_within(16))
    assert all(20.0 <= x <= 30.0 for x in v), v
    assert _status(logs, ORCH, "chat.quota_status", "chat.status") == {"NOMINAL"}
    assert attr_values(logs, ORCH, "openai.capacity_tpm") == {"600000"}
    _assert_fix_line(logs, 16, ORCH, p["deployment_name"])
    _cause_precedes(16)


def test_ch16_converges_and_latency_follows():
    _, _, early = _sig(16, "after", ORCH, "chat.openai_quota_headroom_pct", since=10)
    assert max(early) < 10.0, early
    _, _, base = _sig(16, "before", ORCH, "chat.turn_latency_ms")
    _, _, dur = _sig(16, "during", ORCH, "chat.turn_latency_ms")
    _, _, aft = _sig(16, "after", ORCH, "chat.turn_latency_ms", since=_within(16))
    assert min(dur) > 2 * max(base) and max(aft) <= 2800.0, (base, dur, aft)


# ── ch18: NSG rule blocks probes -> healthy backend hosts ───────────────────

def test_ch18_healthy_hosts_phases():
    m = "assistant_web.appgw_healthy_backend_hosts"
    logs, _, v = _sig(18, "before", WEB, m)
    assert set(v) == {6}
    assert _status(logs, WEB, "appgw.backend_health", "appgw.status") == {"NOMINAL"}

    p = SCN.get_fault_params(18)
    logs, _, v = _sig(18, "during", WEB, m)
    assert set(v) == {0}
    assert _status(logs, WEB, "appgw.backend_health", "appgw.status") == {"ANOMALY"}
    assert attr_values(logs, WEB, "nsg.rule_set_version") == {"nsg-assistant-web-ruleset-v14"}
    assert attr_values(logs, WEB, "appgw.unhealthy_hosts") == {str(p["unhealthy_hosts"])}
    assert attr_values(logs, WEB, "appgw.pool_size") == {str(p["pool_size"])}
    assert attr_values(logs, WEB, f"nsg.rule.{p['nsg_rule']}") == {"present"}

    logs, _, v = _sig(18, "after", WEB, m, since=_within(18))
    assert set(v) == {6}
    assert _status(logs, WEB, "appgw.backend_health", "appgw.status") == {"NOMINAL"}
    assert attr_values(logs, WEB, "nsg.rule_set_version") == {"nsg-assistant-web-ruleset-v13"}
    assert attr_values(logs, WEB, f"nsg.rule.{p['nsg_rule']}") == {"removed"}
    _assert_fix_line(logs, 18, WEB, p["nsg_name"])
    _cause_precedes(18)


def test_ch18_converges_as_probes_pass():
    _, _, early = _sig(18, "after", WEB, "assistant_web.appgw_healthy_backend_hosts", since=10)
    assert max(early) < 6, early


# ── secondary effects on the two services ───────────────────────────────────

@pytest.mark.parametrize("ch,svc,metric", [
    (1, ORCH, "chat.turn_latency_ms"), (7, ORCH, "chat.turn_latency_ms"),
    (11, ORCH, "chat.turn_latency_ms"), (19, ORCH, "chat.turn_latency_ms"),
    (1, WEB, "assistant_web.first_token_ms"), (11, WEB, "assistant_web.first_token_ms"),
    (10, WEB, "assistant_web.degraded_answer_pct"), (17, WEB, "assistant_web.degraded_answer_pct"),
    (8, ORCH, "chat.degraded_turn_pct"), (5, ORCH, "chat.degraded_turn_pct"),
])
def test_secondary_effect_rises_and_recovers(ch, svc, metric):
    _, _, base = _sig(ch, "before", svc, metric)
    _, _, dur = _sig(ch, "during", svc, metric)
    _, _, aft = _sig(ch, "after", svc, metric, since=120)
    assert min(dur) > max(base), (base, dur)
    assert max(aft) <= max(base) * 1.05 + 1e-9 or max(aft) < min(dur), (base, aft)
    assert max(aft) < 2800.0 if "latency" in metric else True


def test_ch02_token_metrics_rise_and_recover():
    for m in ("chat.prompt_tokens", "chat.completion_tokens", "chat.token_cost_usd_per_conversation"):
        base = [{"chat.prompt_tokens": 3050.0, "chat.completion_tokens": 625.0,
                 "chat.token_cost_usd_per_conversation": 0.725}[m]]
        # the metrics are uniform random draws; average enough samples that the mean
        # test is not flaky (4 samples failed ~4% of runs for completion_tokens)
        _, _, dur = _sig(2, "during", ORCH, m, cycles=20)
        _, _, aft = _sig(2, "after", ORCH, m, since=60)
        assert sum(dur) / len(dur) > 2.5 * (sum(base) / len(base)), (m, base, dur)
        assert max(aft) <= {"chat.prompt_tokens": 5200, "chat.completion_tokens": 1100, "chat.token_cost_usd_per_conversation": 1.1}[m], (m, aft)
    _, _, early = _sig(2, "after", ORCH, "chat.token_cost_usd_per_conversation", since=10)
    assert min(early) > 1.1, early


def test_unrelated_channel_leaves_services_at_baseline():
    # channel 6 lists neither service
    _, _, v = _sig(6, "during", ORCH, "chat.turn_latency_ms")
    assert max(v) <= 2800.0
    _, _, v = _sig(6, "during", WEB, "assistant_web.first_token_ms")
    assert max(v) <= 900.0


# ── command parsing and opsctl end to end ───────────────────────────────────

CHANNELS = [4, 15, 16, 18]


@pytest.mark.parametrize("ch", CHANNELS)
def test_expected_commands_parse(ch):
    assert len(ops.expected_commands(REG[ch]["remediation_command"], PROFILE)) == len(_cmd_lines(ch))
    for line in _cmd_lines(ch):
        ops.parse_command(line, PROFILE)


@pytest.mark.parametrize("ch", CHANNELS)
def test_opsctl_resolves_channel(ch):
    ctl = controller()
    inst = SimpleNamespace(
        chaos_controller=ctl,
        ctx=SimpleNamespace(services={s: {} for s in SCN.services}, scenario=SCN),
        resolve_channel=lambda c, **kw: ctl.resolve(c, **kw),
    )
    ctl.trigger(ch, user_email="sre@example.com")
    for line in _cmd_lines(ch):
        ops.execute(inst, line)
    assert ctl.get_channel_status(ch)["state"] == "STANDBY"
