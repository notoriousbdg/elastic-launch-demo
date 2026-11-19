"""aws_bedrock_agentcore field-shape and chaos tests."""

from __future__ import annotations

import random

from generators.aws_bedrock_agentcore import SPEC
from generators.aws_bedrock_agentcore.generator import (
    DOC_FIELDS,
    SOURCE_INTEGRATION,
    build_log_docs,
    build_metric_docs,
)

_PROFILE = {
    "aws_account_id": "121212121212",
    "aws_account_name": "DemoAccount",
    "aws_region": "us-east-1",
}
_CFG = {
    "agent_name": "customersupport",
    "endpoint_name": "DEFAULT",
    "gateway_resource": "gw-1",
    "aws_region": "us-east-1",
}


def _runtime(docs):
    return next(
        d for d in docs if d["aws"]["dimensions"].get("Operation") == "InvokeAgentRuntime"
    )


def _m(docs, *path):
    cur = _runtime(docs)["aws"]["bedrock_agentcore"]["metrics"]
    for p in path:
        cur = cur[p]
    return cur


def test_source_citation():
    assert "aws_bedrock_agentcore@1.1.0" in SOURCE_INTEGRATION


def test_runtime_name_uses_agent_endpoint_separator():
    docs = build_metric_docs(1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(1), None)
    runtime = _runtime(docs)
    assert runtime["aws"]["dimensions"]["Name"] == "customersupport::DEFAULT"
    assert "::" in runtime["aws"]["dimensions"]["Name"]
    assert "agent_name" not in runtime["aws"]["bedrock_agentcore"]
    assert "endpoint_name" not in runtime["aws"]["bedrock_agentcore"]
    assert runtime["event"].get("module") == "aws"
    metrics = runtime["aws"]["bedrock_agentcore"]["metrics"]
    assert "Invocations" in metrics and "sum" in metrics["Invocations"]
    assert "Latency" in metrics and "avg" in metrics["Latency"]
    assert DOC_FIELDS


def test_healthy_baseline_no_chaos():
    docs = build_metric_docs(1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(1), None)
    assert _m(docs, "Latency", "avg") <= 10000
    # OOTB error/throttle alerts fire on SUM > 0, so a healthy emit must be exactly 0
    assert _m(docs, "SystemErrors", "sum") == 0
    assert _m(docs, "UserErrors", "sum") == 0
    assert _m(docs, "Throttles", "sum") == 0
    identity = next(d for d in docs if "WorkloadIdentity" in d["aws"]["dimensions"])
    im = identity["aws"]["bedrock_agentcore"]["metrics"]
    assert im["WorkloadAccessTokenFetchFailures"]["sum"] == 0
    assert im["ResourceAccessTokenFetchFailures"]["sum"] == 0
    assert im["WorkloadAccessTokenFetchThrottles"]["sum"] == 0


def test_gateway_emits_multiple_dimension_series():
    docs = build_metric_docs(1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(1), None)
    gateway = [
        d for d in docs if d["aws"]["dimensions"].get("Operation") == "InvokeGateway"
        or d["aws"]["dimensions"].get("Method") == "tools/call"
    ]
    assert len(gateway) >= 5
    dim_sets = [frozenset(d["aws"]["dimensions"]) for d in gateway]
    assert frozenset({"Operation", "Protocol"}) in dim_sets
    assert frozenset({"Operation", "Resource", "Protocol"}) in dim_sets
    # Method-only series must include Operation (global InvokeGateway filter)
    assert frozenset({"Operation", "Method"}) in dim_sets
    method_only = [
        d for d in docs
        if d["aws"]["dimensions"].get("Operation") == "InvokeGateway"
        and d["aws"]["dimensions"].get("Method") == "tools/call"
        and "Resource" not in d["aws"]["dimensions"]
        and "Name" not in d["aws"]["dimensions"]
    ]
    assert method_only
    m = method_only[0]["aws"]["bedrock_agentcore"]["metrics"]
    assert m["TargetType_LAMBDA"]["sum"] > 0
    assert m["TargetType_MCP"]["sum"] > 0
    methods = {
        d["aws"]["dimensions"]["Method"]
        for d in docs
        if d["aws"]["dimensions"].get("Operation") == "InvokeGateway"
        and "Method" in d["aws"]["dimensions"]
        and "Resource" not in d["aws"]["dimensions"]
    }
    assert methods >= {"tools/call", "tools/list"}


def test_identity_has_workload_dimensions():
    docs = build_metric_docs(1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(1), None)
    identity = [d for d in docs if "WorkloadIdentity" in d["aws"]["dimensions"]]
    assert identity
    dims = identity[0]["aws"]["dimensions"]
    assert dims["WorkloadIdentityDirectory"]
    assert dims["ProviderName"]
    assert dims["Type"]


def test_browser_and_code_ops_present():
    docs = build_metric_docs(1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(1), None)
    ops = {d["aws"]["dimensions"].get("Operation") for d in docs}
    assert "BrowserUserTakeOver" in ops
    assert "BrowserUserTakeOverRelease" in ops
    assert "CodeInterpreterSession" in ops
    assert "StopCodeInterpreterSession" in ops


def test_latency_multiplier_alone():
    docs = build_metric_docs(
        1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(2),
        {"agent_runtime_high_latency": 35000.0},
    )
    assert _m(docs, "Latency", "avg") > 10000
    assert _m(docs, "SystemErrors", "sum") == 0
    gw = next(d for d in docs if d["aws"]["dimensions"].get("Operation") == "InvokeGateway")
    assert gw["aws"]["bedrock_agentcore"]["metrics"]["Latency"]["avg"] > 5000
    code = next(
        d for d in docs if d["aws"]["dimensions"].get("Operation") == "InvokeCodeInterpreter"
    )
    assert code["aws"]["bedrock_agentcore"]["metrics"]["Duration"]["avg"] > 30000
    mem = next(
        d for d in docs if d["aws"]["dimensions"].get("Operation") == "RetrieveMemoryRecords"
    )
    assert mem["aws"]["bedrock_agentcore"]["metrics"]["Latency"]["avg"] > 3000


def test_system_error_count_alone():
    docs = build_metric_docs(
        1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(3),
        {"agent_runtime_system_errors": 6.0},
    )
    assert _m(docs, "SystemErrors", "sum") > 0
    assert _m(docs, "Latency", "avg") <= 10000


def test_user_error_count_alone():
    docs = build_metric_docs(
        1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(4),
        {"agent_runtime_user_errors": 11.0},
    )
    assert _m(docs, "UserErrors", "sum") > 0
    assert _m(docs, "SystemErrors", "sum") == 0


def test_throttle_count_alone():
    docs = build_metric_docs(
        1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(5),
        {"gateway_throttles": 12.0, "identity_throttles": 12.0, "code_interpreter_throttles": 12.0},
    )
    gw = next(d for d in docs if d["aws"]["dimensions"].get("Operation") == "InvokeGateway")
    assert gw["aws"]["bedrock_agentcore"]["metrics"]["Throttles"]["sum"] > 0
    identity = next(d for d in docs if "WorkloadIdentity" in d["aws"]["dimensions"])
    assert identity["aws"]["bedrock_agentcore"]["metrics"]["ApiKeyFetchThrottles"]["sum"] > 0
    code = next(
        d for d in docs if d["aws"]["dimensions"].get("Operation") == "InvokeCodeInterpreter"
    )
    assert code["aws"]["bedrock_agentcore"]["metrics"]["Throttles"]["sum"] > 0


def test_identity_failure_count_alone():
    docs = build_metric_docs(
        1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(6),
        {"identity_token_fetch_failures": 6.0},
    )
    identity = next(d for d in docs if "WorkloadIdentity" in d["aws"]["dimensions"])
    im = identity["aws"]["bedrock_agentcore"]["metrics"]
    assert im["WorkloadAccessTokenFetchFailures"]["sum"] > 0
    assert im["ResourceAccessTokenFetchFailures"]["sum"] > 0
    assert _m(docs, "SystemErrors", "sum") == 0


def test_log_docs_cover_three_streams():
    logs = build_log_docs(1_700_000_000.0, "demo", _PROFILE, _CFG, random.Random(3), None)
    assert logs["runtime"][0]["data_stream"]["dataset"].endswith("runtime_application_logs")
    assert logs["gateway"][0]["aws"]["bedrock_agentcore"]["operation"] == "InvokeGateway"
    assert logs["memory"][0]["aws"]["bedrock_agentcore"]["operation"] == "CreateEvent"

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.aws_bedrock_agentcore import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['agent_runtime_high_latency', 'agent_runtime_system_errors', 'agent_runtime_user_errors', 'browser_errors', 'browser_session_throttles', 'code_interpreter_errors', 'code_interpreter_high_latency', 'code_interpreter_throttles', 'gateway_errors', 'gateway_high_latency', 'gateway_throttles', 'identity_throttles', 'identity_token_fetch_failures', 'memory_errors', 'memory_high_latency']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


def test_emit_namespaces_includes_ootb_default():
    from generator_engine.es_doc_common import emit_namespaces

    assert emit_namespaces("default") == ["default"]
    assert emit_namespaces("xborder") == ["xborder", "default"]
