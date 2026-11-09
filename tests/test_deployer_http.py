"""Deployer HTTP goes through the shared helpers, not inline clients."""

import json
import threading

import httpx

from elastic_config.deployer_kb import KbMixin
from elastic_config.deployer_skills import SkillsMixin


class _Step:
    def __init__(self):
        self.status = ""
        self.detail = ""
        self.items_done = 0
        self.items_total = 0


class _Scenario:
    namespace = "demo"
    scenario_name = "Demo Corp"
    builtin_skill_names: list[str] = []
    skill_definitions = [
        {"id": "demo-skill", "name": "Demo", "description": "d", "content": "c"},
    ]
    tool_definitions: list[dict] = []
    channel_registry = {
        1: {
            "name": "Latency",
            "error_type": "Timeout",
            "subsystem": "edge",
            "affected_services": ["api"],
            "cascade_services": [],
            "description": "slow",
            "remediation_action": "restart_edge",
        },
    }

    def prefixed_tool_id(self, name: str) -> str:
        return f"demo.{name}"

    def fault_param_ranges(self, channel: int) -> dict[str, str]:
        return {}

    def rca_clue_ranges(self, channel: int, service_name: str) -> dict[str, str]:
        return {"edge.queue_depth": "40–60"} if service_name == "api" else {}

    def get_correlation_attribute(self, channel: int, is_error: bool, rng) -> dict:
        return {"runtime.pool": "small"} if rng.random() < 0.9 else {}


def _handler(seen: list[httpx.Request], lock: threading.Lock):
    def handle(request: httpx.Request) -> httpx.Response:
        with lock:
            seen.append(request)
            same = [
                item for item in seen
                if item.method == request.method and item.url.path == request.url.path
            ]
            nth = len(same)
        if request.url.path == "/api/agent_builder/skills" and request.method == "POST" and nth == 1:
            return httpx.Response(503, text="starting")
        if request.url.path == "/_bulk" and nth == 1:
            return httpx.Response(503, text="busy")
        if request.url.path == "/_bulk":
            return httpx.Response(
                200,
                json={"errors": False, "items": [{"index": {"status": 201}}]},
            )
        if request.url.path == "/api/agent_builder/skills" and request.method == "GET":
            return httpx.Response(200, json={"data": []})
        return httpx.Response(200, json={})

    return handle


def test_skill_create_retries_with_kibana_headers(monkeypatch):
    seen: list[httpx.Request] = []
    monkeypatch.setattr("elastic_config.deployer_base.time.sleep", lambda *_a, **_k: None)
    host = SkillsMixin()
    host.api_key = "skill-key"
    host.kibana_url = "http://kibana.test"
    host.scenario = _Scenario()
    host.progress = object()
    host._created_skill_ids = []
    host._created_tool_ids = set()
    host._step = lambda _idx: _Step()

    transport = httpx.MockTransport(_handler(seen, threading.Lock()))
    with httpx.Client(transport=transport) as client:
        host._deploy_skills(client, lambda _progress: None)

    posts = [
        req for req in seen
        if req.method == "POST" and req.url.path == "/api/agent_builder/skills"
    ]
    assert len(posts) == 2
    for req in posts:
        assert req.headers["authorization"] == "ApiKey skill-key"
        assert req.headers["kbn-xsrf"] == "true"
        assert req.headers["content-type"].startswith("application/json")
    assert host._created_skill_ids == ["demo-skill"]


def test_knowledge_base_bulk_retries_with_es_headers(monkeypatch):
    seen: list[httpx.Request] = []
    lock = threading.Lock()
    handle = _handler(seen, lock)
    monkeypatch.setattr("elastic_config.deployer_base.time.sleep", lambda *_a, **_k: None)

    real_client = httpx.Client

    def client_factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handle)
        return real_client(*args, **kwargs)

    monkeypatch.setattr("elastic_config.deployer_base.httpx.Client", client_factory)

    step = _Step()
    host = KbMixin()
    host.ns = "demo"
    host.elastic_url = "http://es.test"
    host.api_key = "kb-key"
    host.scenario = _Scenario()
    host.progress = object()
    host._step = lambda _idx: step
    host._is_serverless = lambda: False

    with real_client(transport=httpx.MockTransport(handle)) as client:
        host._deploy_knowledge_base(client, lambda _progress: None)

    puts = [req for req in seen if req.method == "PUT"]
    bulks = [req for req in seen if req.url.path == "/_bulk"]
    assert puts and puts[0].headers["authorization"] == "ApiKey kb-key"
    assert puts[0].headers["content-type"].startswith("application/json")
    assert len(bulks) == 2
    assert bulks[0].url.params["refresh"] == "true"
    assert bulks[-1].headers["authorization"] == "ApiKey kb-key"
    assert bulks[-1].headers["content-type"].startswith("application/x-ndjson")
    assert b'"_id": "ch01-timeout"' in bulks[-1].content
    assert step.status == "ok"
    assert step.items_done == 1


_CORTEX = "/internal/nightshift/cortex/pages"
_REAL_CLIENT = httpx.Client


def _cortex_host(monkeypatch, seen, existing_version=None, write_status=200, serverless=True):
    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/_bulk":
            return httpx.Response(
                200, json={"errors": False, "items": [{"index": {"status": 201}}]},
            )
        if request.url.path.startswith(f"{_CORTEX}/") and request.method == "GET":
            if existing_version is None:
                return httpx.Response(404, json={})
            return httpx.Response(200, json={"page": {"version": existing_version}})
        if request.url.path == _CORTEX:
            return httpx.Response(write_status, json={})
        return httpx.Response(200, json={})

    monkeypatch.setattr("elastic_config.deployer_base.time.sleep", lambda *_a, **_k: None)

    def client_factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handle)
        return _REAL_CLIENT(*args, **kwargs)

    monkeypatch.setattr("elastic_config.deployer_base.httpx.Client", client_factory)

    step = _Step()
    host = KbMixin()
    host.ns = "demo"
    host.elastic_url = "http://es.test"
    host.kibana_url = "http://kibana.test"
    host.api_key = "kb-key"
    host.scenario = _Scenario()
    host.progress = object()
    host._stream_name = "logs.otel.demo"
    host._step = lambda _idx: step
    host._is_serverless = lambda: serverless
    return host, step, httpx.MockTransport(handle)


def test_serverless_kb_also_writes_cortex_runbook(monkeypatch):
    seen: list[httpx.Request] = []
    host, step, transport = _cortex_host(monkeypatch, seen)
    with _REAL_CLIENT(transport=transport) as client:
        host._deploy_knowledge_base(client, lambda _progress: None)

    gets = [r for r in seen if r.method == "GET" and r.url.path.startswith(_CORTEX)]
    posts = [r for r in seen if r.method == "POST" and r.url.path == _CORTEX]
    assert gets[0].url.path == f"{_CORTEX}/cortex_runbook_demo-ch01-timeout"
    assert len(posts) == 1
    assert posts[0].headers["x-elastic-internal-origin"] == "kibana"
    body = json.loads(posts[0].content)
    assert body["entity_type"] == "runbook"
    assert body["slug"] == "demo-ch01-timeout"
    assert body["status"] == "established"
    assert "version" not in body
    assert "`Timeout`" in body["content"] and "restart_edge" in body["content"]
    assert "demo." not in body["content"] and "remediation-guide" not in body["content"]
    assert "`attributes.edge.queue_depth` (40–60)" in body["content"]
    assert "`attributes.runtime.pool` = `small`" in body["content"]
    assert step.status == "ok"
    assert "1/1 Cortex runbooks" in step.detail


def test_cortex_runbook_updates_existing_page_with_version(monkeypatch):
    seen: list[httpx.Request] = []
    host, _step, transport = _cortex_host(monkeypatch, seen, existing_version="WzEsMV0=")
    with _REAL_CLIENT(transport=transport) as client:
        host._deploy_knowledge_base(client, lambda _progress: None)

    puts = [r for r in seen if r.method == "PUT" and r.url.path == _CORTEX]
    assert len(puts) == 1
    assert json.loads(puts[0].content)["version"] == "WzEsMV0="
    assert not [r for r in seen if r.method == "POST" and r.url.path == _CORTEX]


def test_cortex_failure_keeps_kb_step_ok(monkeypatch):
    seen: list[httpx.Request] = []
    host, step, transport = _cortex_host(monkeypatch, seen, write_status=500)
    with _REAL_CLIENT(transport=transport) as client:
        host._deploy_knowledge_base(client, lambda _progress: None)

    assert step.status == "ok"
    assert "0/1 Cortex runbooks" in step.detail


def test_non_serverless_kb_skips_cortex(monkeypatch):
    seen: list[httpx.Request] = []
    host, step, transport = _cortex_host(monkeypatch, seen, serverless=False)
    with _REAL_CLIENT(transport=transport) as client:
        host._deploy_knowledge_base(client, lambda _progress: None)

    assert not [r for r in seen if r.url.path.startswith(_CORTEX)]
    assert "Cortex" not in step.detail


def test_cortex_cleanup_deletes_each_channel_page(monkeypatch):
    seen: list[httpx.Request] = []
    host, _step, transport = _cortex_host(monkeypatch, seen, existing_version="7:1")
    with _REAL_CLIENT(transport=transport) as client:
        assert host._cleanup_cortex_runbooks(client) == 1

    deletes = [r for r in seen if r.method == "DELETE"]
    assert [r.url.path for r in deletes] == [f"{_CORTEX}/cortex_runbook_demo-ch01-timeout"]
    assert deletes[0].url.params["version"] == "7:1"


def test_cortex_cleanup_skips_missing_pages(monkeypatch):
    seen: list[httpx.Request] = []
    host, _step, transport = _cortex_host(monkeypatch, seen)
    with _REAL_CLIENT(transport=transport) as client:
        assert host._cleanup_cortex_runbooks(client) == 0

    assert not [r for r in seen if r.method == "DELETE"]


# ── [serverless] Nightshift sandbox connector + custom context ───────────────

from elastic_config.deployer_platform import PlatformMixin  # noqa: E402


def _platform_host(seen, *, exists=False, listed=True, types_status=200, assigned=()):
    connector = f"/api/actions/connector/{PlatformMixin._SANDBOX_CONNECTOR_ID}"
    agent = f"/api/agent_builder/agents/{PlatformMixin._NIGHTSHIFT_AGENT_ID}"

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        path = request.url.path
        if path == PlatformMixin._SANDBOX_CONNECTOR_TYPES_PATH:
            return httpx.Response(types_status, json={"connector_types": [{"id": ".elasticsearch"}]})
        if path == connector and request.method == "GET":
            return httpx.Response(200 if exists else 404, json={})
        if path == PlatformMixin._SANDBOX_CONNECTORS_PATH:
            ids = [PlatformMixin._SANDBOX_CONNECTOR_ID] if listed else []
            return httpx.Response(200, json={"connectors": [{"id": i} for i in ids]})
        if path == agent and request.method == "GET":
            return httpx.Response(200, json={"configuration": {"connector_ids": list(assigned)}})
        return httpx.Response(200, json={})

    host = PlatformMixin()
    host.kibana_url = "http://kibana.test"
    host.elastic_url = "https://es.test"
    host.api_key = "plat-key"
    return host, httpx.MockTransport(handle), connector


def test_sandbox_connector_created_and_listed(monkeypatch):
    monkeypatch.setattr("elastic_config.deployer_base.time.sleep", lambda *_a, **_k: None)
    seen: list[httpx.Request] = []
    host, transport, connector = _platform_host(seen)
    configured: list[str] = []
    errors: list[str] = []
    with httpx.Client(transport=transport) as client:
        host._configure_sandbox_connector_serverless(client, configured, errors)

    posts = [r for r in seen if r.method == "POST" and r.url.path == connector]
    assert len(posts) == 1
    body = json.loads(posts[0].content)
    assert body["connector_type_id"] == ".elasticsearch"
    assert body["config"]["url"] == "https://es.test"
    assert body["secrets"]["Authorization"] == "ApiKey plat-key"
    agent_puts = [r for r in seen if r.method == "PUT" and r.url.path.startswith("/api/agent_builder/")]
    assert [json.loads(r.content) for r in agent_puts] == [
        {"configuration": {"connector_ids": [PlatformMixin._SANDBOX_CONNECTOR_ID]}}
    ]
    assert configured == ["Nightshift sandbox connector"] and errors == []


def test_sandbox_connector_assignment_keeps_other_connectors(monkeypatch):
    monkeypatch.setattr("elastic_config.deployer_base.time.sleep", lambda *_a, **_k: None)
    seen: list[httpx.Request] = []
    host, transport, _connector = _platform_host(seen, assigned=("github-1",))
    with httpx.Client(transport=transport) as client:
        host._configure_sandbox_connector_serverless(client, [], [])
    [put] = [r for r in seen if r.method == "PUT" and r.url.path.startswith("/api/agent_builder/")]
    assert json.loads(put.content)["configuration"]["connector_ids"] == [
        "github-1", PlatformMixin._SANDBOX_CONNECTOR_ID,
    ]


def test_sandbox_connector_already_assigned_is_not_rewritten(monkeypatch):
    monkeypatch.setattr("elastic_config.deployer_base.time.sleep", lambda *_a, **_k: None)
    seen: list[httpx.Request] = []
    host, transport, _connector = _platform_host(
        seen, exists=True, assigned=(PlatformMixin._SANDBOX_CONNECTOR_ID,),
    )
    configured: list[str] = []
    with httpx.Client(transport=transport) as client:
        host._configure_sandbox_connector_serverless(client, configured, [])
    assert not [r for r in seen if r.method == "PUT" and r.url.path.startswith("/api/agent_builder/")]
    assert configured == ["Nightshift sandbox connector"]


def test_sandbox_connector_updated_when_present(monkeypatch):
    monkeypatch.setattr("elastic_config.deployer_base.time.sleep", lambda *_a, **_k: None)
    seen: list[httpx.Request] = []
    host, transport, connector = _platform_host(seen, exists=True)
    with httpx.Client(transport=transport) as client:
        host._configure_sandbox_connector_serverless(client, [], [])

    puts = [r for r in seen if r.method == "PUT" and r.url.path == connector]
    assert len(puts) == 1
    assert "connector_type_id" not in json.loads(puts[0].content)


def test_sandbox_connector_not_listed_is_an_error(monkeypatch):
    monkeypatch.setattr("elastic_config.deployer_base.time.sleep", lambda *_a, **_k: None)
    host, transport, _connector = _platform_host([], listed=False)
    errors: list[str] = []
    with httpx.Client(transport=transport) as client:
        host._configure_sandbox_connector_serverless(client, [], errors)
    assert errors and "not offered" in errors[0]


def test_sandbox_connector_skipped_on_builds_without_route():
    seen: list[httpx.Request] = []
    host, transport, connector = _platform_host(seen, types_status=404)
    errors: list[str] = []
    with httpx.Client(transport=transport) as client:
        host._configure_sandbox_connector_serverless(client, [], errors)
    assert errors == [] and not [r for r in seen if r.url.path == connector]


def _context_host(monkeypatch, snippets):
    state = {"snippets": list(snippets)}
    puts: list[dict] = []

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/internal/nightshift/custom_context":
            if request.method == "PUT":
                body = json.loads(request.content)
                puts.append(body)
                state["snippets"] = body["snippets"]
            return httpx.Response(200, json={"snippets": state["snippets"]})
        return httpx.Response(200, json={})

    monkeypatch.setattr("elastic_config.deployer_base.time.sleep", lambda *_a, **_k: None)
    host = KbMixin()
    host.ns = "demo"
    host.kibana_url = "http://kibana.test"
    host.api_key = "kb-key"
    host.scenario = _Scenario()
    host._stream_name = "logs.otel.demo"
    return host, httpx.MockTransport(handle), state, puts


def test_nightshift_context_keeps_other_snippets_and_replaces_ours(monkeypatch):
    other = [{"text": "Hand-written note"}, {"text": "[launch-next:other] Other scenario"}]
    host, transport, state, _puts = _context_host(
        monkeypatch, other + [{"text": "[launch-next:demo] old"}],
    )
    with _REAL_CLIENT(transport=transport) as client:
        assert host._set_nightshift_context(client, host._nightshift_context_text())
        assert host._set_nightshift_context(client, host._nightshift_context_text())

    texts = [s["text"] for s in state["snippets"]]
    assert texts[:2] == ["Hand-written note", "[launch-next:other] Other scenario"]
    ours = [t for t in texts if t.startswith("[launch-next:demo]")]
    assert len(ours) == 1
    assert "logs.otel.demo" in ours[0] and "Timeout" in ours[0]


def test_nightshift_context_teardown_removes_only_ours(monkeypatch):
    host, transport, state, _puts = _context_host(
        monkeypatch, [{"text": "[launch-next:other] x"}, {"text": "[launch-next:demo] y"}],
    )
    with _REAL_CLIENT(transport=transport) as client:
        assert host._set_nightshift_context(client, None)
    assert state["snippets"] == [{"text": "[launch-next:other] x"}]


def _enable_host(seen, *, status=None, accounts=()):
    status = status or {"serviceAccountsEnabled": True, "installed": True, "enabled": False, "bound": False}

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        path = request.url.path
        if path == PlatformMixin._NIGHTSHIFT_SA_STATUS_PATH:
            return httpx.Response(200, json=status)
        if path == PlatformMixin._SERVICE_ACCOUNT_PATH and request.method == "GET":
            return httpx.Response(200, json={"serviceAccounts": list(accounts)})
        if path == PlatformMixin._SERVICE_ACCOUNT_PATH and request.method == "POST":
            name = json.loads(request.content)["name"]
            return httpx.Response(200, json={"id": f"id-{name}"})
        return httpx.Response(200, json={})

    host = PlatformMixin()
    host.kibana_url = "http://kibana.test"
    host.api_key = "plat-key"
    return host, httpx.MockTransport(handle)


def test_enable_nightshift_creates_accounts_and_binds_them():
    seen: list[httpx.Request] = []
    host, transport = _enable_host(seen)
    configured: list[str] = []
    errors: list[str] = []
    with httpx.Client(transport=transport) as client:
        host._enable_nightshift_serverless(client, configured, errors)

    created = [json.loads(r.content) for r in seen
               if r.method == "POST" and r.url.path == PlatformMixin._SERVICE_ACCOUNT_PATH]
    assert [(c["name"], c["roles"]) for c in created] == [
        ("nightshift_agent", ["_nightshift_agent"]),
        ("nightshift_automations", ["_nightshift_automations"]),
    ]
    [enable] = [r for r in seen if r.url.path.endswith("/service_account/_enable")]
    assert json.loads(enable.content) == {"serviceAccountIds": {
        "agent": "id-nightshift_agent", "automations": "id-nightshift_automations",
    }}
    assert [r for r in seen if r.url.path == "/internal/nightshift/onboarding/enable"]
    assert configured == ["Nightshift enabled"] and errors == []


def test_enable_nightshift_reuses_existing_accounts():
    seen: list[httpx.Request] = []
    existing = [{"id": "sa-agent", "name": "nightshift_agent", "roles": ["_nightshift_agent"],
                 "enabled": True, "assumable": True}]
    host, transport = _enable_host(seen, accounts=existing)
    with httpx.Client(transport=transport) as client:
        host._enable_nightshift_serverless(client, [], [])
    created = [json.loads(r.content)["name"] for r in seen
               if r.method == "POST" and r.url.path == PlatformMixin._SERVICE_ACCOUNT_PATH]
    assert created == ["nightshift_automations"]
    [enable] = [r for r in seen if r.url.path.endswith("/service_account/_enable")]
    assert json.loads(enable.content)["serviceAccountIds"]["agent"] == "sa-agent"


def test_enable_nightshift_skips_when_already_enabled():
    seen: list[httpx.Request] = []
    host, transport = _enable_host(seen, status={
        "serviceAccountsEnabled": True, "installed": True, "enabled": True, "bound": True,
    })
    configured: list[str] = []
    with httpx.Client(transport=transport) as client:
        host._enable_nightshift_serverless(client, configured, [])
    assert [r.url.path for r in seen] == [PlatformMixin._NIGHTSHIFT_SA_STATUS_PATH]
    assert configured == []


def test_enable_nightshift_uiam_refusal_is_pending_not_an_error():
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == PlatformMixin._NIGHTSHIFT_SA_STATUS_PATH:
            return httpx.Response(200, json={"serviceAccountsEnabled": True, "installed": False})
        if request.url.path == PlatformMixin._SERVICE_ACCOUNT_PATH and request.method == "GET":
            return httpx.Response(200, json={"serviceAccounts": []})
        if request.url.path == PlatformMixin._SERVICE_ACCOUNT_PATH:
            return httpx.Response(400, json={"message": "Provided credential is not compatible with UIAM"})
        return httpx.Response(200, json={})

    host = PlatformMixin()
    host.kibana_url = "http://kibana.test"
    host.api_key = "plat-key"
    errors: list[str] = []
    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        host._enable_nightshift_serverless(client, [], errors)
    assert errors == [] and host._nightshift_enable_pending is True
    assert not [r for r in seen if r.url.path.endswith("/_enable")]


def test_pending_enable_starts_watcher_instead_of_failing(monkeypatch):
    monkeypatch.setattr("elastic_config.deployer_base.time.sleep", lambda *_a, **_k: None)
    seen: list[httpx.Request] = []
    host, transport, _connector = _platform_host(seen)
    host._nightshift_enable_pending = True
    started: list[bool] = []
    host._watch_nightshift_enable = lambda: started.append(True)
    configured: list[str] = []
    errors: list[str] = []
    with httpx.Client(transport=transport) as client:
        host._configure_sandbox_connector_serverless(client, configured, errors)
    assert started == [True] and errors == []
    assert configured == ["Nightshift sandbox connector (assigned once Nightshift is enabled)"]
    assert not [r for r in seen if r.url.path.startswith("/api/agent_builder/")]


def test_watcher_assigns_connector_once_enabled(monkeypatch):
    seen: list[httpx.Request] = []
    host, transport, _connector = _platform_host(seen)
    states = iter([False, True])
    host._nightshift_enabled = lambda _client: next(states)
    monkeypatch.setattr("elastic_config.deployer_platform.time.sleep", lambda *_a: None)
    monkeypatch.setattr(
        "elastic_config.deployer_platform.httpx.Client",
        lambda **kw: _REAL_CLIENT(transport=transport),
    )

    class _Inline:
        def __init__(self, target, **_kw):
            self._target = target

        def start(self):
            self._target()

    monkeypatch.setattr("elastic_config.deployer_platform.threading.Thread", _Inline)
    host._watch_nightshift_enable()
    puts = [r for r in seen if r.method == "PUT" and r.url.path.startswith("/api/agent_builder/")]
    assert [json.loads(r.content)["configuration"]["connector_ids"] for r in puts] == [
        [PlatformMixin._SANDBOX_CONNECTOR_ID]
    ]


# ── [serverless] Nightshift onboarding: the Add data "Proceed" ────────────────

def _onboarding_host(seen, *, state, status=200):
    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == PlatformMixin._NIGHTSHIFT_ONBOARDING_PATH:
            return httpx.Response(status, json=state)
        return httpx.Response(200, json={"execution_id": "exec-1"})

    host = PlatformMixin()
    host.kibana_url = "http://kibana.test"
    host.api_key = "plat-key"
    return host, httpx.MockTransport(handle)


def _suggestion_posts(seen):
    return [r for r in seen
            if r.method == "POST" and r.url.path == "/internal/nightshift/onboarding/suggestions"]


def test_onboarding_starts_suggestions_once_enabled():
    seen: list[httpx.Request] = []
    host, transport = _onboarding_host(seen, state={"agent_installed": True})
    with httpx.Client(transport=transport) as client:
        host._start_nightshift_onboarding(client)
    [post] = _suggestion_posts(seen)
    assert post.headers["x-elastic-internal-origin"] == "kibana"


def test_onboarding_never_starts_a_second_run():
    seen: list[httpx.Request] = []
    host, transport = _onboarding_host(
        seen, state={"agent_installed": True, "execution": {"execution_id": "e", "status": "running"}},
    )
    with httpx.Client(transport=transport) as client:
        host._start_nightshift_onboarding(client)
    assert _suggestion_posts(seen) == []


def test_onboarding_waits_for_nightshift_to_be_enabled():
    seen: list[httpx.Request] = []
    host, transport = _onboarding_host(seen, state={"agent_installed": False})
    with httpx.Client(transport=transport) as client:
        host._start_nightshift_onboarding(client)
    assert _suggestion_posts(seen) == []


def test_onboarding_skips_builds_without_it():
    seen: list[httpx.Request] = []
    host, transport = _onboarding_host(seen, state={}, status=404)
    with httpx.Client(transport=transport) as client:
        host._start_nightshift_onboarding(client)
    assert _suggestion_posts(seen) == []
