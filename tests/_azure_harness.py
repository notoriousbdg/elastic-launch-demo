"""Run the Azure GenAI Assistant services before, during and after a fault.

Shared by the tests/test_azure*.py files. A test builds a controller in one of three
phases and reads back the log records and metric points the services emit:

    ctl = phase(2, "before")          # nothing active, nothing fixed
    ctl = phase(2, "during")          # channel 2 active for `elapsed` seconds
    ctl = phase(2, "after", since=60) # channel 2 fixed by opsctl 60 s ago
    logs, metrics = records(ctl)
    values(metrics, "model-router", "model_router.route_latency_ms")
"""
from __future__ import annotations

import time
from pathlib import Path

from app.chaos.controller import ChaosController
from app.context import ScenarioContext
from app.services.base_service import BaseService
from scenario_engine.yaml_scenario import load_yaml_scenario
from tests.test_generator_contract import _RecordingOTLP

ROOT = Path(__file__).resolve().parent.parent
SCN = load_yaml_scenario(ROOT / "scenarios" / "azure")
CTX = ScenarioContext.from_scenario(SCN)
REG = SCN.channel_registry
PROFILE = SCN.ops_profile
OPS_ACTOR = "opsctl/claude-code"


def _attrs(kvs) -> dict:
    return {a["key"]: list(a["value"].values())[0] for a in kvs or []}


def controller() -> ChaosController:
    return ChaosController(channel_registry=REG, actors=PROFILE.get("actors"))


def phase(ch: int, which: str, *, elapsed: float = 600.0, since: float = 300.0,
          actor: str = OPS_ACTOR) -> ChaosController:
    """A controller with channel *ch* in phase ``before`` / ``during`` / ``after``.

    ``during``: triggered *elapsed* seconds ago. ``after``: triggered, then resolved with
    its remediation_effects applied by *actor* *since* seconds ago (pending fix log
    lines are released immediately).
    """
    ctl = controller()
    if which == "before":
        return ctl
    ctl.trigger(ch, user_email="sre@example.com")
    ctl._channels[ch]["triggered_at"] = time.time() - elapsed
    if which == "during":
        return ctl
    assert which == "after", which
    ctl.resolve(ch, force=True, actor=actor)
    with ctl._lock:
        for row in ctl._platform_rows:
            if row["channel"] == ch:
                row["applied_at"] = time.time() - since
        for evt in ctl._pending_events:
            evt["emit_at"] = 0
        if ch in ctl._tapers:
            _, span = ctl._tapers[ch]
            ctl._tapers[ch] = (time.time() - since, span)
    return ctl


def records(ctl: ChaosController, cycles: int = 4) -> tuple[list[dict], list[dict]]:
    """(logs, metrics) the scenario's services emit over *cycles* telemetry cycles."""
    client = _RecordingOTLP()
    BaseService.set_context(CTX)
    try:
        services = [cls(ctl, client) for cls in SCN.get_service_classes()]
    finally:
        BaseService.clear_context()
    for _ in range(cycles):
        for svc in services:
            svc.generate_telemetry()
    logs: list[dict] = []
    metrics: list[dict] = []
    for name, args, _kw in client.payloads:
        if name == "send_logs":
            res = _attrs(args[0]["attributes"])
            for rec in args[1]:
                logs.append({
                    "service": res.get("service.name"),
                    "severity": rec["severityText"],
                    "body": rec["body"]["stringValue"],
                    "attrs": _attrs(rec.get("attributes")),
                    "res": res,
                })
        elif name == "send_metrics":
            res = _attrs(args[0]["attributes"])
            for m in args[1]:
                for dp in (m.get("gauge") or m.get("sum") or {}).get("dataPoints", []):
                    metrics.append({
                        "service": res.get("service.name"),
                        "name": m["name"],
                        "value": float(dp.get("asDouble", dp.get("asInt", 0))),
                        "attrs": _attrs(dp.get("attributes")),
                    })
    return logs, metrics


def values(metrics: list[dict], service: str, name: str) -> list[float]:
    return [m["value"] for m in metrics if m["service"] == service and m["name"] == name]


def bodies(logs: list[dict], service: str | None = None, contains: str = "") -> list[str]:
    return [r["body"] for r in logs
            if (service is None or r["service"] == service) and contains in r["body"]]


def attr_values(logs: list[dict], service: str, key: str) -> set:
    return {r["attrs"][key] for r in logs if r["service"] == service and key in r["attrs"]}
