"""Who triggered a fault: the ``{ns}-notify-context`` document and the workflow lookup.

Each channel can be triggered by a different person, so the notification workflows
look the email up by the alert's fault code (error_type), not "newest document".
The document carries no channel number or deployment id: Nightshift can query any
index, and those would show the audience the fault was injected.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import httpx
import yaml

from app.chaos.controller import ChaosController
from app.instance import ScenarioInstance
from elastic_config.deployer_base import (
    LEGACY_WORKFLOW_INDEX_SUFFIXES,
    WORKFLOW_INDEX_MAPPINGS,
    WORKFLOW_INDEX_SUFFIXES,
)
from scenario_engine.yaml_scenario import load_yaml_scenario

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = ROOT / "elastic_config" / "workflows"
SCN = load_yaml_scenario(ROOT / "scenarios" / "space")


def _record(monkeypatch, channel: int) -> list[tuple[str, dict]]:
    posts: list[tuple[str, dict]] = []

    class _Client:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def post(self, url, headers=None, json=None):
            posts.append((url, json))
            return httpx.Response(201)

    class _SyncThread:
        def __init__(self, target, name=None, daemon=None):
            self._target = target

        def start(self):
            self._target()

    monkeypatch.setattr(httpx, "Client", _Client)
    monkeypatch.setattr("threading.Thread", _SyncThread)
    fake = SimpleNamespace(
        ctx=SimpleNamespace(elastic_url="https://es.example/", elastic_api_key="k"),
        chaos_controller=ChaosController(channel_registry=SCN.channel_registry),
        namespace="nova7",
        scenario_id="space",
    )
    ScenarioInstance._record_notify_context(fake, channel, "alice@example.com", "https://cb.example")
    return posts


def test_document_is_keyed_by_fault_code_with_no_channel(monkeypatch):
    posts = _record(monkeypatch, 2)
    assert len(posts) == 1
    url, doc = posts[0]
    assert url == "https://es.example/nova7-notify-context/_doc"
    assert doc["error_type"] == SCN.channel_registry[2]["error_type"]
    assert doc["user_email"] == "alice@example.com"
    assert set(doc) == {"@timestamp", "error_type", "user_email", "callback_url"}


def test_index_is_created_with_the_lookup_fields_and_old_one_is_cleaned_up():
    assert "notify-context" in WORKFLOW_INDEX_SUFFIXES
    assert "chaos-context" not in WORKFLOW_INDEX_SUFFIXES
    assert "chaos-context" in LEGACY_WORKFLOW_INDEX_SUFFIXES
    props = WORKFLOW_INDEX_MAPPINGS["notify-context"]["properties"]
    assert "channel" not in props and "deployment_id" not in props


def test_every_notification_workflow_looks_up_the_email_by_its_alerts_fault_code():
    expected = {
        "significant_event_notification.yaml": "event.rule.tags[1]",
        "significant_event_notification_auto_remediate.yaml": "event.rule.tags[1]",
        "significant_event_notification_serverless.yaml": "inputs.payload.alerts[0].data.error_type",
        "significant_event_notification_auto_remediate_serverless.yaml":
            "inputs.payload.alerts[0].data.error_type",
    }
    for name, var in expected.items():
        wf = yaml.safe_load((WORKFLOWS / name).read_text())
        step = next(s for s in wf["steps"] if s["name"] == "enrich_context")
        query = " ".join(step["with"]["query"].split())
        assert query.startswith("FROM __NS__-notify-context"), (name, query)
        assert f'WHERE error_type == "{{{{ {var} }}}}"' in query, (name, query)
        assert "chaos-context" not in (WORKFLOWS / name).read_text(), name
