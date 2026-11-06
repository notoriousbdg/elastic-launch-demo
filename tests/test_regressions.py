"""Regression tests for bugs found in the architecture review.

Each test fails on the pre-fix code.
"""
import ast
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent


# ── metrics ML backfill used to raise UnboundLocalError (swallowed as non-fatal) ──

class _FakeIndexer:
    """Stands in for ConcurrentBulkIndexer: counts docs, no network."""

    def __init__(self, *args, **kwargs):
        self.count = 0

    def add(self, doc):
        self.count += 1

    def flush(self):
        return self.count

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_metrics_ml_backfill_writes_documents(monkeypatch, caplog):
    from elastic_config import deployer_backfill as mod

    monkeypatch.setattr(mod, "ConcurrentBulkIndexer", _FakeIndexer)
    fake_self = SimpleNamespace(
        scenario=SimpleNamespace(namespace="demo", services={"api": {}}),
        elastic_url="https://es.example",
        api_key="k",
        _metrics_ml_backfill_min_ts=None,
    )

    sent = mod.BackfillMixin._deploy_metrics_ml_backfill(fake_self)

    assert sent > 0, f"backfill wrote nothing; log: {caplog.text}"
    assert fake_self._metrics_ml_backfill_min_ts is not None
    assert "failed (non-fatal)" not in caplog.text


# ── duplicate method definitions silently override each other ──

def _duplicate_methods(path: Path):
    tree = ast.parse(path.read_text())
    dups = []
    for cls in (n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)):
        seen = {}
        for item in cls.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                # @property/@x.setter pairs legitimately share a name
                if any(isinstance(d, ast.Attribute) for d in item.decorator_list):
                    continue
                if item.name in seen:
                    dups.append(f"{path.name}:{cls.name}.{item.name} (lines {seen[item.name]}, {item.lineno})")
                seen[item.name] = item.lineno
    return dups


def test_no_duplicate_method_definitions():
    dups = []
    for sub in ("app", "elastic_config", "scenario_engine", "generator_engine"):
        for path in (ROOT / sub).rglob("*.py"):
            dups += _duplicate_methods(path)
    assert not dups, "duplicate method definitions:\n" + "\n".join(dups)


def test_reset_infrastructure_events_only_forgets_inactive_channels():
    from app.services.base_service import BaseService

    class _Svc(BaseService):
        def generate_telemetry(self):  # abstract in the base
            pass

    svc = object.__new__(_Svc)  # skip __init__: only the reset logic is under test
    svc._infra_events_emitted = {(1, 0), (1, 1), (2, 0)}

    svc.reset_infrastructure_events(active=[1])
    assert svc._infra_events_emitted == {(1, 0), (1, 1)}

    svc.reset_infrastructure_events(active=[])
    assert svc._infra_events_emitted == set()

    svc._infra_events_emitted = {(3, 0)}
    svc.reset_infrastructure_events()
    assert svc._infra_events_emitted == set()


# ── chaos routed to the wrong deployment ──

def _fake_instance(dep_id):
    return SimpleNamespace(deployment_id=dep_id)


def test_get_instance_does_not_fall_back_for_unknown_id(monkeypatch):
    import app.main as main

    only = _fake_instance("only")
    monkeypatch.setattr(main.registry, "get", lambda _id: None)
    monkeypatch.setattr(main.registry, "all_instances", lambda: {"only": only})

    assert main._get_instance("stale-id") is None
    assert main._get_instance(None) is only


def test_get_instance_refuses_to_guess_between_deployments(monkeypatch):
    import pytest

    import app.main as main

    insts = {"telecom": _fake_instance("telecom"), "banking": _fake_instance("banking")}
    monkeypatch.setattr(main.registry, "get", insts.get)
    monkeypatch.setattr(main.registry, "all_instances", lambda: dict(insts))

    with pytest.raises(main.AmbiguousDeployment) as exc:
        main._get_instance(None)
    assert exc.value.deployments == ["banking", "telecom"]
    assert main._get_instance("telecom") is insts["telecom"]


# ── OTLP metrics routed to the env cluster; reconfigure reset auth type ──

def test_metrics_url_uses_the_instances_own_elasticsearch(monkeypatch):
    import app.telemetry as telemetry

    monkeypatch.setattr(telemetry, "ELASTIC_URL", "https://env-cluster.example")
    client = telemetry.OTLPClient(
        endpoint="https://x.ingest.example", api_key="k",
        elastic_url="https://deployment.es.example/",
    )
    assert client._metrics_otlp_url() == "https://deployment.es.example/_otlp/v1/metrics"
    client.close()


def test_metrics_url_falls_back_to_environment(monkeypatch):
    import app.telemetry as telemetry

    monkeypatch.setattr(telemetry, "ELASTIC_URL", "https://env-cluster.example")
    client = telemetry.OTLPClient(endpoint="https://x.ingest.example", api_key="k")
    assert client._metrics_otlp_url() == "https://env-cluster.example/_otlp/v1/metrics"
    client.close()


def test_reconfigure_keeps_configured_auth_type():
    import app.telemetry as telemetry

    client = telemetry.OTLPClient(
        endpoint="https://a.example", api_key="k", auth_type="Bearer"
    )
    client.reconfigure("https://b.example", "k2")
    assert client.auth_type == "Bearer"
    assert client.client.headers["Authorization"] == "Bearer k2"
    client.reconfigure("https://b.example", "k3", auth_type="ApiKey")
    assert client.client.headers["Authorization"] == "ApiKey k3"
    client.close()


# ── progress endpoints returned another deployment's progress for unknown ids ──

def test_progress_endpoints_do_not_fall_back_for_unknown_id(monkeypatch):
    import asyncio

    import app.main as main

    other = {"finished": False, "error": "", "steps": [{"name": "other"}]}
    monkeypatch.setattr(main, "_deploy_progress", {"other-dep": other})
    monkeypatch.setattr(main, "_teardown_progress", {"other-dep": other})

    for handler in (main.setup_progress, main.teardown_progress):
        unknown = asyncio.run(handler("stale-id"))
        assert unknown is not other
        assert unknown["finished"] is True and unknown["error"]
        assert asyncio.run(handler("other-dep")) is other
        assert asyncio.run(handler(None)) is other


# ── teardown left the dashboard tag behind ──

def test_dashboard_teardown_deletes_namespace_tag():
    from elastic_config.dashboards.generate_exec_dashboard import _tag_saved_object
    from elastic_config.deployer import _dashboard_objects_to_delete

    import json

    tag = json.loads(_tag_saved_object("demo"))
    assert {"type": tag["type"], "id": tag["id"]} in _dashboard_objects_to_delete("demo")


# ── teardown_with_progress deleted streams before stopping ML datafeeds ──

def test_teardown_with_progress_stops_ml_before_deleting_streams(monkeypatch):
    import httpx

    import elastic_config.deployer as deployer_mod

    calls: list[str] = []

    def record(name, result=None):
        def method(*_args, **_kwargs):
            calls.append(name)
            return result
        return method

    real_client = httpx.Client
    monkeypatch.setattr(
        deployer_mod.httpx, "Client",
        lambda **kw: real_client(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={}))),
    )
    dep = deployer_mod.ScenarioDeployer(
        SimpleNamespace(namespace="demo"), "https://es.example", "https://kb.example", "key",
    )
    for name, result in {
        "_cleanup_workflows": 0, "_cleanup_alerts": (0, 0), "_cleanup_agent": 0,
        "_cleanup_apm_ml": None, "_cleanup_apm_rollup": None, "_cleanup_aiops_ml": None,
        "_delete_stream": True, "_delete_ecs_stream": True,
        "_cleanup_slos": 0, "_cleanup_cases": 0, "_cleanup_data_views": (0, 0),
    }.items():
        monkeypatch.setattr(dep, name, record(name, result))

    progress = dep.teardown_with_progress()

    assert calls.index("_cleanup_aiops_ml") < calls.index("_delete_stream")
    assert calls.index("_cleanup_apm_ml") < calls.index("_delete_stream")
    assert all(s.status == "ok" for s in progress.steps), [(s.name, s.status, s.detail) for s in progress.steps]


# ── blocking network I/O ran on the event loop, stalling every request ──

def test_blocking_setup_handlers_run_off_the_event_loop():
    import inspect

    import app.main as main

    for handler in (main.test_connection, main.launch_setup, main.detect_existing, main.teardown_setup):
        # Plain `def` routes run on FastAPI's threadpool; `async def` would block the loop.
        assert not inspect.iscoroutinefunction(handler), handler.__name__


def test_smtp_send_runs_in_a_worker_thread(monkeypatch):
    import asyncio
    import threading

    import app.notify.email_handler as email_handler

    threads: list[str] = []
    monkeypatch.setattr(email_handler, "SMTP_HOST", "smtp.example")
    monkeypatch.setattr(email_handler, "_smtp_send", lambda *a: threads.append(threading.current_thread().name))

    result = asyncio.run(email_handler.send_email("a@example.com", "s", "b"))

    assert result["sent"] is True
    assert threads and threads[0] != threading.main_thread().name


# ── routes without deployment_id silently acted on the first deployment ──

class _RouteInstance:
    """Just enough of ScenarioInstance for the chaos/status routes."""

    def __init__(self, dep_id):
        self.deployment_id = dep_id
        self.elastic_url = ""
        self.elastic_api_key = ""
        self.scenario = SimpleNamespace(namespace=dep_id)
        self.triggered: list[int] = []

    def chaos_status(self):
        return {"1": {"name": f"{self.deployment_id}-ch1"}}

    def channel_status(self, channel):
        return {"channel": channel, "owner": self.deployment_id}

    def trigger_channel(self, channel, **_kw):
        self.triggered.append(channel)
        return {"status": "triggered", "channel": channel}


def _client_with(monkeypatch, *dep_ids):
    from fastapi.testclient import TestClient

    import app.main as main

    insts = {d: _RouteInstance(d) for d in dep_ids}
    monkeypatch.setattr(main.registry, "get", insts.get)
    monkeypatch.setattr(main.registry, "all_instances", lambda: dict(insts))
    # No lifespan: TestClient outside a `with` block skips startup auto-deploy.
    return TestClient(main.app), insts


def test_routes_without_id_are_rejected_when_several_deployments_run(monkeypatch):
    client, insts = _client_with(monkeypatch, "banking", "telecom")

    for method, url, kw in [
        ("get", "/api/chaos/status", {}),
        ("get", "/api/chaos/status/1", {}),
        ("get", "/api/status", {}),
        ("post", "/api/chaos/trigger", {"json": {"channel": 1}}),
        ("post", "/api/remediate/1", {}),
    ]:
        resp = getattr(client, method)(url, **kw)
        assert resp.status_code == 400, url
        assert resp.json()["deployments"] == ["banking", "telecom"], url
    assert insts["banking"].triggered == [] and insts["telecom"].triggered == []

    resp = client.get("/chaos", follow_redirects=False)
    assert resp.status_code == 307 and resp.headers["location"] == "/"


def test_routes_report_the_deployment_that_answered(monkeypatch):
    import app.main as main

    client, insts = _client_with(monkeypatch, "banking", "telecom")

    resp = client.get("/api/chaos/status/1", params={"deployment_id": "telecom"})
    assert resp.json()["owner"] == "telecom"
    assert resp.headers["X-Deployment-Id"] == "telecom"

    resp = client.post("/api/chaos/trigger", json={"channel": 2, "deployment_id": "banking"})
    assert resp.headers["X-Deployment-Id"] == "banking"
    assert insts["banking"].triggered == [2] and insts["telecom"].triggered == []

    # Threadpool (plain def) routes set the header too.
    monkeypatch.setattr(main, "_get_default_creds", lambda: ("", "", ""))
    resp = client.get("/api/setup/detect", params={"deployment_id": "telecom"})
    assert resp.headers["X-Deployment-Id"] == "telecom"


def test_single_deployment_still_works_without_id(monkeypatch):
    client, _ = _client_with(monkeypatch, "banking")

    resp = client.get("/api/chaos/status")
    assert resp.status_code == 200
    assert resp.json() == {"1": {"name": "banking-ch1"}}  # body shape unchanged
    assert resp.headers["X-Deployment-Id"] == "banking"



def test_chaos_page_pins_the_resolved_deployment(monkeypatch):
    from scenario_engine import get_scenario

    client, insts = _client_with(monkeypatch, "banking")
    inst = insts["banking"]
    inst.scenario = get_scenario("banking")
    inst.mission_id = "BANKING"
    inst.kibana_url = "https://kb.example"

    html = client.get("/chaos").text  # no deployment_id in the URL

    assert "window.DEPLOYMENT_ID = 'banking';" in html
