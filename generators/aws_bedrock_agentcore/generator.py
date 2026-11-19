"""AWS Bedrock AgentCore telemetry generator.

Source: elastic/integrations packages/aws_bedrock_agentcore@1.1.0

Writes ``metrics-aws_bedrock_agentcore.metrics-<ns>`` plus application log
streams. Package dashboards hardcode ES|QL ``FROM …-default``; when the
scenario namespace differs, metrics and logs are dual-written there.

Documents use ``pipeline=_none``. Emit post-pipeline shapes:

- Runtime ``aws.dimensions.Name`` must be ``{agent}::{endpoint}`` (dashboard
  global filter ``Name : *::*``).
- Emit **multiple** CloudWatch dimension series per tick (presence/absence of
  Protocol/Method/Resource/Name is intentional in panels).
- Gateway dashboard global filter requires ``Operation:InvokeGateway`` on every
  gateway series panels read (including Method-only Lambda/MCP KPI series).
- Identity series need WorkloadIdentity / WorkloadIdentityDirectory / ProviderName.
- Lens ``emptyAsNull=True`` on sum metrics maps literal ``0`` → null; healthy
  baseline emits small non-zero error/throttle counts under alert thresholds.
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/aws_bedrock_agentcore@1.1.0"

import logging
import math
import os
import random
import threading
import time
import uuid
from typing import Any

from app.telemetry import ESBulkClient
from generator_engine.identity import ScenarioIdentity
from generator_engine.es_doc_common import (
    build_base_doc,
    business_load,
    emit_namespaces,
    resolve_active_chaos,
    send_dual_bulk,
)

logger = logging.getLogger(__name__)

DATA_STREAMS = (
    "metrics-aws_bedrock_agentcore.metrics",
    "logs-aws_bedrock_agentcore.runtime_application_logs",
    "logs-aws_bedrock_agentcore.gateway_application_logs",
    "logs-aws_bedrock_agentcore.memory_application_logs",
)

DOC_FIELDS = (
    "aws.bedrock_agentcore.metrics.Invocations.sum",
    "aws.bedrock_agentcore.metrics.Sessions.sum",
    "aws.bedrock_agentcore.metrics.Latency.avg",
    "aws.bedrock_agentcore.metrics.Duration.avg",
    "aws.bedrock_agentcore.metrics.SystemErrors.sum",
    "aws.bedrock_agentcore.metrics.UserErrors.sum",
    "aws.bedrock_agentcore.metrics.Throttles.sum",
    "aws.dimensions.Operation",
    "aws.dimensions.Name",
    "aws.dimensions.Resource",
    "aws.dimensions.Protocol",
    "aws.dimensions.Method",
    "aws.dimensions.WorkloadIdentity",
    "aws.dimensions.WorkloadIdentityDirectory",
    "aws.dimensions.ProviderName",
)

_BULK_PARAMS = {"pipeline": "_none"}


def _account_cloud(profile: dict, cfg: dict) -> dict[str, str]:
    return {
        "provider": "aws",
        "region": cfg.get("aws_region") or profile.get("aws_region", "us-east-1"),
        "account_id": str(profile.get("aws_account_id") or ""),
        "account_name": profile.get("aws_account_name", "DemoAccount"),
    }


def _metric_doc(
    ts: float,
    ns: str,
    cloud: dict[str, str],
    metrics: dict[str, Any],
    dimensions: dict[str, str],
) -> dict[str, Any]:
    """Build one AgentCore metrics document.

    Metrics data stream is TSDS. Do not set ``agent_name`` / ``endpoint_name``
    on metrics docs (not dimensions/metrics). Dashboards filter
    ``aws.dimensions.Name`` as ``{agent}::{endpoint}``.
    """
    doc = build_base_doc(
        timestamp=ts,
        dataset="aws_bedrock_agentcore.metrics",
        namespace=ns,
        doc_type="metrics",
        agent_type="metricbeat",
        cloud=cloud,
    )
    shaped: dict[str, Any] = {}
    for key, value in metrics.items():
        if isinstance(value, float):
            shaped[key] = {"avg": float(value)}
        else:
            shaped[key] = {"sum": int(value)}
    doc["aws"] = {
        "bedrock_agentcore": {"metrics": shaped},
        "cloudwatch": {"namespace": "AWS/Bedrock-AgentCore"},
        "dimensions": dimensions,
    }
    doc["event"]["module"] = "aws"
    doc["metricset"] = {"name": "cloudwatch", "period": 300000}
    doc["service"] = {"type": "aws"}
    # Unique agent.id per dimension series helps TSDS routing.
    dim_key = "-".join(f"{k}{v}" for k, v in sorted(dimensions.items()))[:48] or "none"
    doc["agent"]["id"] = f"agentcore-{cloud['account_id']}-{dim_key}"
    return doc


def _chaos_val(chaos: dict | None, key: str, default: float = 0.0) -> float:
    return float((chaos or {}).get(key, default) or default)


# OOTB error/throttle alerts use `SUM(...) > 0`. Healthy emit must be 0 — a
# non-zero "emptyAsNull" dashboard baseline false-positives every rule.
# Prefer cold series / literal 0 over under-threshold noise when threshold is 0.
_BASELINE_ERR = 0
_BASELINE_THROTTLE = 0


def _chaos_or_baseline(chaos: dict | None, key: str, *, baseline: int) -> int:
    """Return chaos value when a fault dict is active; else healthy baseline."""
    if chaos is None:
        return baseline
    return int(_chaos_val(chaos, key, 0.0))


def _counts(rng: random.Random, load: float, chaos: dict | None) -> dict[str, Any]:
    inv = max(1, int(rng.randint(8, 40) * load))
    latency = float(max(50.0, min(2500.0, math.exp(rng.gauss(6.5, 0.4)))))
    runtime_lat = _chaos_val(chaos, "agent_runtime_high_latency")
    if runtime_lat > 0:
        latency = max(latency, runtime_lat, 10001.0)
    return {
        "inv": inv,
        "latency": latency,
        "runtime_sys": _chaos_or_baseline(
            chaos, "agent_runtime_system_errors", baseline=_BASELINE_ERR
        ),
        "runtime_user": _chaos_or_baseline(
            chaos, "agent_runtime_user_errors", baseline=_BASELINE_ERR
        ),
        "gateway_sys": _chaos_or_baseline(chaos, "gateway_errors", baseline=_BASELINE_ERR),
        "gateway_user": _chaos_or_baseline(chaos, "gateway_errors", baseline=_BASELINE_ERR),
        "gateway_throttle": _chaos_or_baseline(
            chaos, "gateway_throttles", baseline=_BASELINE_THROTTLE
        ),
        "gateway_latency": (
            max(latency * 0.6, _chaos_val(chaos, "gateway_high_latency"), 5001.0)
            if _chaos_val(chaos, "gateway_high_latency") > 0
            else latency * 0.6
        ),
        "memory_sys": _chaos_or_baseline(chaos, "memory_errors", baseline=_BASELINE_ERR),
        "memory_latency": (
            max(latency * 0.4, _chaos_val(chaos, "memory_high_latency"), 3001.0)
            if _chaos_val(chaos, "memory_high_latency") > 0
            else latency * 0.4
        ),
        "browser_sys": _chaos_or_baseline(chaos, "browser_errors", baseline=_BASELINE_ERR),
        "browser_user": _chaos_or_baseline(chaos, "browser_errors", baseline=_BASELINE_ERR),
        "browser_throttle": _chaos_or_baseline(
            chaos, "browser_session_throttles", baseline=_BASELINE_THROTTLE
        ),
        "code_sys": _chaos_or_baseline(
            chaos, "code_interpreter_errors", baseline=_BASELINE_ERR
        ),
        "code_user": _chaos_or_baseline(
            chaos, "code_interpreter_errors", baseline=_BASELINE_ERR
        ),
        "code_throttle": _chaos_or_baseline(
            chaos, "code_interpreter_throttles", baseline=_BASELINE_THROTTLE
        ),
        "code_duration": (
            max(latency, _chaos_val(chaos, "code_interpreter_high_latency"), 30001.0)
            if _chaos_val(chaos, "code_interpreter_high_latency") > 0
            else latency
        ),
        "identity_fail": int(_chaos_val(chaos, "identity_token_fetch_failures")),
        "identity_throttle": _chaos_or_baseline(
            chaos, "identity_throttles", baseline=_BASELINE_THROTTLE
        ),
    }


def build_metric_docs(
    ts: float,
    ns: str,
    profile: dict,
    cfg: dict,
    rng: random.Random,
    chaos: dict | None,
) -> list[dict[str, Any]]:
    cloud = _account_cloud(profile, cfg)
    agent_name = cfg.get("agent_name", "customersupport")
    endpoint_name = cfg.get("endpoint_name", "DEFAULT")
    # Dashboard global filter: aws.dimensions.Name : *::*
    name_dim = f"{agent_name}::{endpoint_name}"
    gateway_resource = cfg.get("gateway_resource", "agentcore-gateway-prod")
    memory_resource = cfg.get("memory_resource", f"memory-{agent_name}")
    browser_resource = cfg.get("browser_resource", f"browser-{agent_name}")
    code_resource = cfg.get("code_resource", f"code-{agent_name}")
    load = business_load()
    c = _counts(rng, load, chaos)
    id_fail = c["identity_fail"]
    docs: list[dict[str, Any]] = []

    # ── Runtime ──────────────────────────────────────────────────────────
    docs.append(
        _metric_doc(
            ts,
            ns,
            cloud,
            {
                "Sessions": max(1, c["inv"] // 3),
                "Invocations": c["inv"],
                "Errors": c["runtime_sys"] + c["runtime_user"],
                "SystemErrors": c["runtime_sys"],
                "UserErrors": c["runtime_user"],
                "Latency": c["latency"],
                "Duration": c["latency"] * rng.uniform(0.8, 1.1),
                "TokenCount": c["inv"] * rng.randint(40, 200),
                # No runtime-throttle impact key; keep a demo baseline for emptyAsNull panels.
                "Throttles": _BASELINE_THROTTLE,
            },
            {"Operation": "InvokeAgentRuntime", "Name": name_dim},
        )
    )

    # ── Gateway — multiple dimension series ──────────────────────────────
    # Stock panels filter on presence/absence of Protocol/Method/Resource/Name.
    # Gateway dashboard global filter requires Operation:InvokeGateway on every
    # series those panels read (including Lambda/MCP Method-only KPIs).
    protocol = "MCP"
    gw_latency = c["gateway_latency"]
    gw_base = {
        "Invocations": c["inv"],
        "Latency": gw_latency,
        "Duration": gw_latency * 0.9,
        "TargetExecutionTime": gw_latency * 0.7,
        "TargetType_LAMBDA": max(1, c["inv"] // 2),
        "TargetType_MCP": max(1, c["inv"] // 3),
        "Errors": c["gateway_sys"] + c["gateway_user"],
        "SystemErrors": c["gateway_sys"],
        "UserErrors": c["gateway_user"],
        "Throttles": c["gateway_throttle"],
    }
    gateway_series = [
        # Protocol only — "by method protocol", gateway KPI strips
        {"Operation": "InvokeGateway", "Protocol": protocol},
        # Resource+Protocol — "by resource"
        {"Operation": "InvokeGateway", "Resource": gateway_resource, "Protocol": protocol},
        # Method only (no Resource/Name) — Lambda/MCP KPIs + "by method type"
        {"Operation": "InvokeGateway", "Method": "tools/call"},
        {"Operation": "InvokeGateway", "Method": "tools/list"},
        # Method+Resource+Protocol (no Name) — method / tool vs non-tool panels
        {
            "Operation": "InvokeGateway",
            "Method": "tools/list",
            "Resource": gateway_resource,
            "Protocol": protocol,
        },
        {
            "Operation": "InvokeGateway",
            "Method": "tools/call",
            "Resource": gateway_resource,
            "Protocol": protocol,
        },
        # Resource+Name — Total errors / top summary by resource and target
        {
            "Operation": "InvokeGateway",
            "Resource": gateway_resource,
            "Name": name_dim,
        },
        # tools/call + Name — tool-call errors / performance by resource and target
        {
            "Operation": "InvokeGateway",
            "Method": "tools/call",
            "Resource": gateway_resource,
            "Protocol": protocol,
            "Name": name_dim,
        },
        # tools/list + Name — non-tool panels that also split on target
        {
            "Operation": "InvokeGateway",
            "Method": "tools/list",
            "Resource": gateway_resource,
            "Protocol": protocol,
            "Name": name_dim,
        },
    ]
    for dims in gateway_series:
        docs.append(_metric_doc(ts, ns, cloud, dict(gw_base), dims))

    # ── Memory ───────────────────────────────────────────────────────────
    for op in ("CreateEvent", "RetrieveMemoryRecords", "ListEvents", "ListMemory"):
        mem_latency = c["memory_latency"]
        mem = {
            "Invocations": max(1, c["inv"] // 2),
            "CreationCount": max(1, c["inv"] // 2) if op == "CreateEvent" else 0,
            "Latency": mem_latency,
            "Errors": c["memory_sys"],
            "SystemErrors": c["memory_sys"],
            "UserErrors": 0,
            "Throttles": 0,
        }
        docs.append(
            _metric_doc(
                ts,
                ns,
                cloud,
                mem,
                {"Operation": op, "Resource": memory_resource},
            )
        )
    for op in ("Extraction", "Consolidation", "LongTermMemoryProcessing"):
        docs.append(
            _metric_doc(
                ts,
                ns,
                cloud,
                {
                    "Invocations": max(1, c["inv"] // 3),
                    "Latency": c["memory_latency"] * 0.5,
                    "Errors": c["memory_sys"],
                    "SystemErrors": c["memory_sys"],
                    "UserErrors": 0,
                    "Throttles": 0,
                },
                {
                    "Operation": op,
                    "Resource": memory_resource,
                    "StrategyId": "strategy-semantic-1",
                    "StrategyType": "semantic",
                },
            )
        )
    docs.append(
        _metric_doc(
            ts,
            ns,
            cloud,
            {
                "Invocations": max(1, c["inv"] // 4),
                "CreationCount": max(1, c["inv"] // 4),
                "Latency": c["latency"] * 0.3,
                "Errors": 0,
                "SystemErrors": 0,
                "UserErrors": 0,
                "Throttles": 0,
            },
            {"ItemType": "MemoryRecordsExtracted", "Resource": memory_resource},
        )
    )

    # ── Identity ─────────────────────────────────────────────────────────
    for provider_type in ("OAUTH2", "API_KEY"):
        docs.append(
            _metric_doc(
                ts,
                ns,
                cloud,
                {
                    "WorkloadAccessTokenFetchSuccess": max(1, int(10 * load)),
                    "WorkloadAccessTokenFetchFailures": id_fail,
                    "WorkloadAccessTokenFetchThrottles": c["identity_throttle"],
                    "ResourceAccessTokenFetchSuccess": max(1, int(8 * load)),
                    "ResourceAccessTokenFetchFailures": id_fail,
                    "ResourceAccessTokenFetchThrottles": c["identity_throttle"],
                    "ApiKeyFetchSuccess": max(1, int(5 * load)),
                    "ApiKeyFetchFailures": id_fail,
                    "ApiKeyFetchThrottles": c["identity_throttle"],
                    "InboundAuthorizationSuccess": max(1, int(12 * load)),
                    "InboundAuthorizationFailure": id_fail,
                    "CallCount": max(1, int(15 * load)),
                    "ThrottleCount": c["identity_throttle"],
                },
                {
                    "WorkloadIdentity": f"wi-{agent_name}",
                    "WorkloadIdentityDirectory": f"wid-{agent_name}",
                    "ProviderName": f"provider-{provider_type.lower()}",
                    "Type": provider_type,
                },
            )
        )

    # ── Browser ──────────────────────────────────────────────────────────
    browser_ops = (
        "StartBrowserSession",
        "StopBrowserSession",
        "BrowserSession",
        "ConnectBrowserAutomationStream",
        "BrowserUserTakeOver",
        "BrowserUserTakeOverRelease",
    )
    for op in browser_ops:
        docs.append(
            _metric_doc(
                ts,
                ns,
                cloud,
                {
                    "Sessions": max(1, c["inv"] // 4),
                    "Invocations": max(1, c["inv"] // 3),
                    "Duration": c["latency"] * 0.5,
                    "Errors": c["browser_sys"] + c["browser_user"],
                    "SystemErrors": c["browser_sys"],
                    "UserErrors": c["browser_user"],
                    "Throttles": c["browser_throttle"],
                    "TakeOverCount": max(1, c["inv"] // 5)
                    if "TakeOver" in op and "Release" not in op
                    else 0,
                    "TakeOverReleaseCount": max(1, c["inv"] // 5)
                    if op == "BrowserUserTakeOverRelease"
                    else 0,
                    "TakerOverDuration": c["latency"] * 0.2,
                    "CallCount": max(1, c["inv"] // 3),
                },
                {"Operation": op, "Resource": browser_resource},
            )
        )

    # ── Code interpreter ─────────────────────────────────────────────────
    code_ops = (
        "InvokeCodeInterpreter",
        "StartCodeInterpreterSession",
        "StopCodeInterpreterSession",
        "CodeInterpreterSession",
        "ListCodeInterpreterSessions",
    )
    for op in code_ops:
        duration = c["code_duration"]
        docs.append(
            _metric_doc(
                ts,
                ns,
                cloud,
                {
                    "Invocations": max(1, c["inv"] // 3),
                    "Sessions": max(1, c["inv"] // 4),
                    "Duration": duration,
                    "Errors": c["code_sys"] + c["code_user"],
                    "SystemErrors": c["code_sys"],
                    "UserErrors": c["code_user"],
                    "Throttles": c["code_throttle"],
                },
                {"Operation": op, "Resource": code_resource},
            )
        )

    return docs


def _log_doc(
    ts: float,
    ns: str,
    dataset: str,
    cloud: dict[str, str],
    body: dict[str, Any],
    *,
    level: str = "INFO",
    outcome: str = "success",
) -> dict[str, Any]:
    doc = build_base_doc(
        timestamp=ts,
        dataset=dataset,
        namespace=ns,
        doc_type="logs",
        agent_type="filebeat",
        cloud=cloud,
    )
    doc["log"] = {"level": level}
    doc["event"]["outcome"] = outcome
    doc["event"]["id"] = str(uuid.uuid4())
    doc["aws"] = {"bedrock_agentcore": body}
    return doc


def build_log_docs(
    ts: float,
    ns: str,
    profile: dict,
    cfg: dict,
    rng: random.Random,
    chaos: dict | None,
) -> dict[str, list[dict[str, Any]]]:
    cloud = _account_cloud(profile, cfg)
    agent_name = cfg.get("agent_name", "customersupport")
    endpoint_name = cfg.get("endpoint_name", "DEFAULT")
    account = cloud["account_id"]
    region = cloud["region"]
    sys_extra = int(_chaos_val(chaos, "agent_runtime_system_errors"))
    outcome = "failure" if sys_extra > 0 and rng.random() < 0.5 else "success"
    level = "ERROR" if outcome == "failure" else "INFO"

    return {
        "runtime": [
            _log_doc(
                ts,
                ns,
                "aws_bedrock_agentcore.runtime_application_logs",
                cloud,
                {
                    "conversation_id": str(uuid.uuid4()),
                    "operation_name": "invoke_agent",
                    "provider_name": "aws_bedrock_agentcore",
                    "agent_name": agent_name,
                    "endpoint_name": endpoint_name,
                    "operation": "InvokeAgentRuntime",
                    "request_id": str(uuid.uuid4()),
                    "resource_arn": (
                        f"arn:aws:bedrock-agentcore:{region}:{account}:runtime/{agent_name}"
                    ),
                    "service_name": "AgentCoreCodeRuntime",
                    "session_id": str(uuid.uuid4()),
                    "prompt_number": rng.randint(1, 20),
                    "request_payload": {
                        "actor_id": "DEFAULT",
                        "prompt": "Summarize my previous conversation",
                    },
                },
                level=level,
                outcome=outcome,
            )
        ],
        "gateway": [
            _log_doc(
                ts,
                ns,
                "aws_bedrock_agentcore.gateway_application_logs",
                cloud,
                {
                    "operation": "InvokeGateway",
                    "operation_name": "invoke_gateway",
                    "provider_name": "aws_bedrock_agentcore",
                    "request_id": str(uuid.uuid4()),
                    "resource_arn": (
                        f"arn:aws:bedrock-agentcore:{region}:{account}:gateway/"
                        f"{cfg.get('gateway_resource', 'agentcore-gateway-prod')}"
                    ),
                    "service_name": "AgentCoreGateway",
                    "target_type": "LAMBDA" if rng.random() < 0.6 else "MCP",
                },
                level=level,
                outcome=outcome,
            )
        ],
        "memory": [
            _log_doc(
                ts,
                ns,
                "aws_bedrock_agentcore.memory_application_logs",
                cloud,
                {
                    "operation": "CreateEvent",
                    "operation_name": "create_event",
                    "provider_name": "aws_bedrock_agentcore",
                    "request_id": str(uuid.uuid4()),
                    "agent_name": agent_name,
                    "service_name": "AgentCoreMemory",
                },
                level=level,
                outcome=outcome,
            )
        ],
    }


def _send_metrics(client: ESBulkClient, ns: str, docs: list[dict[str, Any]]) -> None:
    send_dual_bulk(client, "metrics-aws_bedrock_agentcore.metrics", ns, docs)



from generators.aws_bedrock_agentcore import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop

def run(
    client: ESBulkClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    if scenario_data and "scenario" in scenario_data:
        scenario = scenario_data["scenario"]
        ns = scenario_data["namespace"]
    else:
        from app.config import ACTIVE_SCENARIO
        from scenario_engine import get_scenario

        scenario = get_scenario(ACTIVE_SCENARIO)
        ns = scenario.namespace

    profile = getattr(scenario, "llm_profile", None) or {
        "aws_account_id": ScenarioIdentity(ns).aws_account_id,
        "aws_account_name": ScenarioIdentity(ns).label("aws-account"),
        "aws_region": "us-east-1",
    }
    cfg = {}
    if hasattr(scenario, "get_generator_config"):
        cfg = scenario.get_generator_config("aws_bedrock_agentcore") or {}
    _channel_registry = scenario.channel_registry if hasattr(scenario, "channel_registry") else {}

    if not client.configured:
        logger.warning("ESBulkClient not configured; aws_bedrock_agentcore will not emit.")
        return

    rng = random.Random()
    logger.info(
        "aws_bedrock_agentcore generator started (namespace=%s, emit=%s) [%s]",
        ns,
        ",".join(emit_namespaces(ns)),
        SOURCE_INTEGRATION,
    )

    def _emit_tick() -> None:
        chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="aws_bedrock_agentcore",
            chaos_mode="explicit",
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        ts = time.time()
        _send_metrics(client, ns, build_metric_docs(ts, ns, profile, cfg, rng, chaos))
        logs = build_log_docs(ts, ns, profile, cfg, rng, chaos)
        send_dual_bulk(
            client,
            "logs-aws_bedrock_agentcore.runtime_application_logs",
            ns,
            logs["runtime"],
        )
        send_dual_bulk(
            client,
            "logs-aws_bedrock_agentcore.gateway_application_logs",
            ns,
            logs["gateway"],
        )
        send_dual_bulk(
            client,
            "logs-aws_bedrock_agentcore.memory_application_logs",
            ns,
            logs["memory"],
        )
    run_tick_loop(stop_event, _emit_tick, interval=30)


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "60"))
    client = ESBulkClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
