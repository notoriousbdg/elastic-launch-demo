"""Stack-generation split: 9.5, 9.6 and serverless take different deploy paths.

Each path is a separate method behind a dispatcher (see ``_stack_generation`` in
``deployer.py``).  These tests pin which Kibana endpoints each generation calls so
the ``[9.5]`` code can be deleted, and the ``[9.6]`` code changed, without the
other paths drifting.
"""
from __future__ import annotations

import json
import logging

import httpx
import pytest

from elastic_config.deployer import ScenarioDeployer
from elastic_config import deployer_platform
from elastic_config.deployer_platform import (
    PlatformMixin,
    _PLAN_LINES_95,
    _PLAN_LINES_96,
    _PLAN_LINES_COMMON,
)
from elastic_config.deployer_streams import StreamsMixin


class _Step:
    def __init__(self):
        self.status = ""
        self.detail = ""
        self.items_done = 0
        self.items_total = 0


class _Scenario:
    scenario_name = "Demo"
    channel_registry = {
        1: {"name": "Latency", "error_type": "Timeout", "subsystem": "edge"},
        2: {"name": "Errors", "error_type": "Boom", "subsystem": "core"},
    }


class _Deployer(PlatformMixin, StreamsMixin):
    kibana_url = "https://kb.example"
    elastic_url = "https://es.example"
    api_key = "k"
    ns = "demo"
    scenario = _Scenario()
    _stream_name = "logs.otel.demo"

    # Reuse the real version/flavour helpers.
    _stack_generation = ScenarioDeployer._stack_generation
    _is_serverless = ScenarioDeployer._is_serverless
    _kibana_version_ge = ScenarioDeployer._kibana_version_ge

    def __init__(self, flavor: str, version: str):
        self._kibana_env = (flavor, version)
        self._step_obj = _Step()
        self.progress = None

    def _step(self, _idx):
        return self._step_obj


GENERATIONS = {
    "9.5": ("traditional", "9.5.4"),
    "9.6": ("traditional", "9.6.0-SNAPSHOT"),
    "serverless": ("serverless", "9.7.0"),
}


def _client(seen: list[tuple[str, str, bytes]], status_for=lambda m, p: 200):
    def handle(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path, request.content))
        return httpx.Response(status_for(request.method, request.url.path), json={})

    return httpx.Client(transport=httpx.MockTransport(handle))


def _paths(seen):
    return [p for _, p, _ in seen]


@pytest.mark.parametrize("gen,expected", [
    (("traditional", "9.5.4"), "9.5"),
    (("traditional", "9.6.0-SNAPSHOT"), "9.6"),
    (("traditional", "10.0.0"), "9.6"),
    (("serverless", "9.7.0"), "serverless"),
    (("", ""), "9.5"),  # version unknown -> the proven 9.5 path
])
def test_stack_generation(gen, expected):
    assert _Deployer(*gen)._stack_generation() == expected


def test_gates_95_set_both_ui_settings_and_no_feature_flags():
    seen: list = []
    d = _Deployer(*GENERATIONS["9.5"])
    configured: list[str] = []
    d._enable_stack_gates(_client(seen), configured, [])
    assert "/internal/core/_settings" not in _paths(seen)
    bodies = b"".join(c for _, p, c in seen if p == "/api/kibana/settings")
    assert b"streamsEnableSignificantEvents" in bodies
    assert b"streamsEnableQueryStreams" in bodies
    assert configured == ["significant events", "query streams"]


def test_gates_96_use_feature_flags_only():
    seen: list = []
    _Deployer(*GENERATIONS["9.6"])._enable_stack_gates(_client(seen), [], [])
    assert _paths(seen) == ["/internal/core/_settings"]
    assert b"nightshift.enabled" in seen[0][2]


def test_gates_serverless_never_call_core_settings():
    seen: list = []
    d = _Deployer(*GENERATIONS["serverless"])
    d._enable_stack_gates(_client(seen, lambda m, p: 403), [], [])
    assert "/internal/core/_settings" not in _paths(seen)


def test_gate_readonly_setting_counts_as_configured():
    d = _Deployer(*GENERATIONS["9.5"])
    configured: list[str] = []
    d._enable_stack_gates(_client([], lambda m, p: 400), configured, [])
    assert configured == ["significant events (plan)", "query streams (plan)"]


def test_plan_lines_by_generation():
    # 9.5 plan: uiSetting only; the 9.6-only keys would fail Kibana config validation.
    text95 = "\n".join(_PLAN_LINES_COMMON + _PLAN_LINES_95)
    assert "streamsEnableSignificantEvents" in text95
    assert "feature_flags" not in text95 and "xpack." not in text95
    assert "coreApp" not in text95
    # 9.6 plan: feature flags, no removed uiSetting.
    text96 = "\n".join(_PLAN_LINES_COMMON + _PLAN_LINES_96)
    assert "nightshift.enabled" in text96 and "coreApp.allowDynamicConfigOverrides" in text96
    assert "streamsEnableSignificantEvents" not in text96
    # Indented 9.5 line must sit inside the uiSettings.overrides block.
    assert _PLAN_LINES_95[0].startswith("  ")
    assert _PLAN_LINES_96[0] and not _PLAN_LINES_96[0].startswith(" ")


def test_significant_events_95_use_bulk_queries():
    seen: list = []
    d = _Deployer(*GENERATIONS["9.5"])
    d._deploy_significant_events(_client(seen), lambda _p: None)
    assert _paths(seen) == ["/api/streams/logs.otel.demo/queries/_bulk"]
    body = seen[0][2]
    assert b"FROM logs.otel.demo,logs.otel.demo.*" in body and b"$." not in body
    assert d._step_obj.status == "ok" and d._step_obj.items_done == 2


def test_significant_events_96_use_query_streams(monkeypatch):
    seen: list = []
    d = _Deployer(*GENERATIONS["9.6"])
    monkeypatch.setattr(_Deployer, "_ensure_dollar_stream_alias", lambda self, alias: None)
    d._deploy_significant_events(_client(seen), lambda _p: None)
    assert _paths(seen) == [
        "/api/streams/logs.otel.demo.demo-se-ch01/_query",
        "/api/streams/logs.otel.demo.demo-se-ch02/_query",
    ]
    assert all(b"FROM $.logs.otel.demo" in c for _, _, c in seen)
    assert d._step_obj.status == "ok" and d._step_obj.items_done == 2


def test_se_unavailable_hint_names_the_right_gate():
    assert "uiSetting" in _Deployer(*GENERATIONS["9.5"])._SE_UNAVAILABLE_DETAIL
    assert "nightshift.enabled" in _Deployer(*GENERATIONS["9.6"])._SE_UNAVAILABLE_DETAIL
    assert "managed by Elastic" in _Deployer(*GENERATIONS["serverless"])._SE_UNAVAILABLE_DETAIL


# ── [serverless] Nightshift sources ───────────────────────────────────────────

SOURCE = {
    "id": "src-1", "title": "logs.otel.demo", "enabled": True,
    "view_name": "$.nightshift.sources.default.logs-otel-demo",
}


def _source_client(seen, *, existing=None, busy_first=None, status_for=None):
    """Mock of the serverless Kibana source APIs."""
    state = {"busy": busy_first}

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path, request.content))
        path, method = request.url.path, request.method
        if status_for and status_for(method, path):
            return httpx.Response(status_for(method, path), json={})
        if path == "/internal/nightshift/sources" and method == "GET":
            return httpx.Response(200, json={"sources": [existing] if existing else [], "total": 1 if existing else 0})
        if path == "/internal/nightshift/sources" and method == "POST":
            return httpx.Response(200, json={"source": SOURCE})
        if path.endswith("/onboarding/_status"):
            return httpx.Response(200, json={"status": "in_progress"})
        if state["busy"] == (method, path):
            state["busy"] = None
            return httpx.Response(409, json={})
        return httpx.Response(200, json={"acknowledged": True})

    return httpx.Client(transport=httpx.MockTransport(handle))


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr("elastic_config.deployer_streams.time.sleep", lambda _s: None)


class _KIScenario(_Scenario):
    services: list = []
    hosts: list = []


def _serverless_deployer():
    d = _Deployer(*GENERATIONS["serverless"])
    d.scenario = _KIScenario()
    d._build_knowledge_indicators = lambda: [
        {"id": "a", "type": "schema", "stream_name": "logs.otel.demo"},
    ]
    d._significant_events_available = lambda client: True
    return d


def test_serverless_ki_use_the_source_id(no_sleep):
    seen: list = []
    d = _serverless_deployer()
    d._deploy_knowledge_indicators(_source_client(seen), lambda _p: None)
    calls = [(m, p) for m, p, _ in seen]
    assert ("POST", "/internal/nightshift/sources") in calls
    assert ("POST", "/internal/streams/src-1/onboarding/_execute") in calls
    assert calls[-1] == ("POST", "/internal/streams/src-1/features/_bulk")
    assert b"stream_name" not in seen[-1][2]
    assert d._step_obj.status == "ok" and d._step_obj.items_done == 1


def test_serverless_source_is_reused_not_recreated(no_sleep):
    seen: list = []
    d = _serverless_deployer()
    client = _source_client(seen, existing=SOURCE)
    d._deploy_knowledge_indicators(client, lambda _p: None)
    posts = [p for m, p, _ in seen if m == "POST" and p == "/internal/nightshift/sources"]
    assert posts == []


def test_serverless_creates_no_significant_event_queries(no_sleep):
    seen: list = []
    d = _serverless_deployer()
    d._deploy_significant_events(_source_client(seen, existing=SOURCE), lambda _p: None)
    assert seen == []
    assert d._step_obj.status == "skipped" and "Universal alert rules" in d._step_obj.detail


def test_serverless_source_lease_409_is_retried(no_sleep):
    seen: list = []
    d = _serverless_deployer()
    bulk = ("POST", "/internal/streams/src-1/features/_bulk")
    d._deploy_knowledge_indicators(_source_client(seen, existing=SOURCE, busy_first=bulk), lambda _p: None)
    assert [(m, p) for m, p, _ in seen].count(bulk) == 2
    assert d._step_obj.status == "ok" and d._step_obj.items_done == 1


def test_serverless_source_create_failure_fails_the_step(no_sleep, monkeypatch):
    monkeypatch.setattr("elastic_config.deployer_streams._SOURCE_CREATE_WAIT", 0.0)
    seen: list = []
    d = _serverless_deployer()
    client = _source_client(seen, status_for=lambda m, p: 400 if (m, p) == ("POST", "/internal/nightshift/sources") else 0)
    d._deploy_knowledge_indicators(client, lambda _p: None)
    assert d._step_obj.status == "failed"
    assert not [1 for m, p, _ in seen if p.endswith("/features/_bulk")]


def test_serverless_stream_delete_removes_the_source_first(no_sleep):
    seen: list = []
    d = _serverless_deployer()
    assert d._delete_stream(_source_client(seen, existing=SOURCE)) is True
    assert [(m, p) for m, p, _ in seen if m == "DELETE"] == [
        ("DELETE", "/internal/nightshift/sources/src-1"),
        ("DELETE", "/api/streams/logs.otel.demo"),
    ]


def test_95_and_96_stream_delete_do_not_touch_sources():
    for gen in ("9.5", "9.6"):
        seen: list = []
        _Deployer(*GENERATIONS[gen])._delete_stream(_client(seen))
        assert not any("nightshift" in p for _, p, _ in seen)


def test_95_knowledge_indicators_still_post_by_stream_name():
    seen: list = []
    d = _Deployer(*GENERATIONS["9.5"])
    d.scenario = _KIScenario()
    d._build_knowledge_indicators = lambda: [{"id": "a", "type": "schema"}]
    d._significant_events_available = lambda client: True
    d._deploy_knowledge_indicators(_client(seen), lambda _p: None)
    assert _paths(seen) == ["/internal/streams/logs.otel.demo/features/_bulk"]


# ── [serverless] Nightshift sandbox secrets ───────────────────────────────────

SECRETS = "/internal/nightshift/sandbox_secrets"


def _secrets_client(
    seen, *, keys=(), version="v1", can_encrypt=True, put_statuses=(200,), get_status=200,
    connector_types_status=404,
):
    puts = list(put_statuses)

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path, request.content))
        if request.url.path == "/internal/nightshift/investigations/service_account":
            return httpx.Response(404, json={})
        if request.url.path == "/internal/nightshift/sandbox_connector_types":
            # 404: a build with neither sandbox secrets nor sandbox connectors.
            return httpx.Response(connector_types_status, json={"connector_types": []})
        if request.url.path != SECRETS:
            return httpx.Response(200, json={})
        if request.method == "GET":
            body = {"keys": list(keys), "canEncrypt": can_encrypt}
            if version is not None:
                body["version"] = version
            return httpx.Response(get_status, json=body)
        return httpx.Response(puts.pop(0) if len(puts) > 1 else puts[0], json={})

    return httpx.Client(transport=httpx.MockTransport(handle))


def _secrets_deployer(gen="serverless"):
    d = _Deployer(*GENERATIONS[gen])
    d.elastic_url = "https://es.example:443"
    d.api_key = "api-key-value"
    return d


def _put_bodies(seen):
    return [json.loads(c) for m, p, c in seen if m == "PUT" and p == SECRETS]


def test_serverless_sets_the_project_credentials_as_sandbox_secrets():
    seen: list = []
    configured: list = []
    errors: list = []
    _secrets_deployer()._enable_stack_gates(_secrets_client(seen), configured, errors)
    [body] = _put_bodies(seen)
    assert body == {
        "entries": [
            {"key": "ELASTICSEARCH_URL", "value": "https://es.example:443"},
            {"key": "ELASTICSEARCH_API_KEY", "value": "api-key-value"},
        ],
        "version": "v1",
    }
    assert "Nightshift sandbox secrets" in configured and not errors


def test_serverless_sandbox_secrets_keep_other_keys_without_a_value():
    seen: list = []
    _secrets_deployer()._enable_stack_gates(
        _secrets_client(seen, keys=["GITHUB_TOKEN", "ELASTICSEARCH_URL"]), [], []
    )
    [body] = _put_bodies(seen)
    assert body["entries"][0] == {"key": "GITHUB_TOKEN"}
    assert [e["key"] for e in body["entries"]].count("ELASTICSEARCH_URL") == 1


def test_serverless_sandbox_secrets_omit_version_when_none_is_stored():
    seen: list = []
    _secrets_deployer()._enable_stack_gates(_secrets_client(seen, version=None), [], [])
    assert "version" not in _put_bodies(seen)[0]


def test_serverless_sandbox_secrets_retry_on_version_conflict():
    seen: list = []
    configured: list = []
    _secrets_deployer()._enable_stack_gates(
        _secrets_client(seen, put_statuses=(409, 200)), configured, []
    )
    assert [m for m, p, _ in seen if p == SECRETS] == ["GET", "PUT", "GET", "PUT"]
    assert "Nightshift sandbox secrets" in configured


def test_serverless_sandbox_secrets_report_a_missing_encryption_key():
    seen: list = []
    errors: list = []
    _secrets_deployer()._enable_stack_gates(_secrets_client(seen, can_encrypt=False), [], errors)
    assert not _put_bodies(seen)
    assert any("encryptionKey" in e for e in errors)


def test_serverless_sandbox_secrets_skip_when_the_feature_is_absent():
    seen: list = []
    configured: list = []
    errors: list = []
    _secrets_deployer()._enable_stack_gates(_secrets_client(seen, get_status=404), configured, errors)
    assert not _put_bodies(seen)
    assert "Nightshift sandbox secrets" not in configured and not errors


def test_serverless_sandbox_secrets_report_a_failed_save_without_the_values():
    seen: list = []
    errors: list = []
    _secrets_deployer()._enable_stack_gates(_secrets_client(seen, put_statuses=(500,)), [], errors)
    assert errors and "api-key-value" not in " ".join(errors)


@pytest.mark.parametrize("gen", ["9.5", "9.6"])
def test_95_and_96_never_touch_sandbox_secrets(gen):
    seen: list = []
    _secrets_deployer(gen)._enable_stack_gates(_secrets_client(seen), [], [])
    assert not any(p == SECRETS for _, p, _ in seen)


def test_serverless_sandbox_secrets_log_the_names_and_never_the_values(caplog):
    with caplog.at_level(logging.INFO, logger="deployer-platform"):
        _secrets_deployer()._enable_stack_gates(_secrets_client([]), [], [])
    assert "Nightshift sandbox secrets saved (ELASTICSEARCH_URL, ELASTICSEARCH_API_KEY)" in caplog.text
    assert "api-key-value" not in caplog.text
