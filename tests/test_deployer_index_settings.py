"""Shard and replica counts are ECH-only: serverless rejects them with HTTP 400.

Both index creates (the workflow audit/queue indices and the knowledge-base
index) send them on ECH and send no settings on serverless.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import httpx
import pytest

from elastic_config.deployer_base import WORKFLOW_INDEX_SUFFIXES
from elastic_config.deployer_kb import KbMixin
from elastic_config.deployer_workflows import WorkflowsMixin

SETTINGS = {"number_of_shards": 1, "number_of_replicas": 1}


class _Workflows(WorkflowsMixin):
    def __init__(self, serverless: bool):
        self.ns = "awsorders"
        self.elastic_url = "https://es.example"
        self.api_key = "k"
        self._serverless = serverless

    def _is_serverless(self) -> bool:
        return self._serverless


class _Kb(KbMixin):
    def __init__(self, serverless: bool):
        self.ns = "awsorders"
        self.elastic_url = "https://es.example"
        self.api_key = "k"
        self.scenario = SimpleNamespace(channel_registry={})
        self.progress = None
        self._serverless = serverless
        self._kb_step = SimpleNamespace(status="", detail="", items_done=0, items_total=0)

    def _is_serverless(self) -> bool:
        return self._serverless

    def _step(self, _idx):
        return self._kb_step


def _client(puts: list, put_status: int = 200) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            return httpx.Response(404)
        if request.method == "PUT":
            puts.append((request.url.path, json.loads(request.content or b"null")))
            return httpx.Response(put_status, json={"error": "x"} if put_status >= 300 else {})
        return httpx.Response(200, json={})

    return httpx.Client(transport=httpx.MockTransport(handler))


class TestWorkflowIndices:
    def test_ech_keeps_the_settings(self):
        puts: list = []
        created = _Workflows(serverless=False)._ensure_workflow_indices(_client(puts))
        assert created == len(WORKFLOW_INDEX_SUFFIXES)
        assert len(puts) == len(WORKFLOW_INDEX_SUFFIXES)
        for _path, body in puts:
            assert body == {"settings": SETTINGS}

    def test_serverless_sends_no_settings(self):
        puts: list = []
        created = _Workflows(serverless=True)._ensure_workflow_indices(_client(puts))
        assert created == len(WORKFLOW_INDEX_SUFFIXES)
        for _path, body in puts:
            assert "settings" not in body
            assert "number_of_shards" not in json.dumps(body)
            assert "number_of_replicas" not in json.dumps(body)

    def test_failures_are_counted(self):
        d = _Workflows(serverless=True)
        created = d._ensure_workflow_indices(_client([], put_status=400))
        assert created == 0
        assert d._workflow_index_failures == len(WORKFLOW_INDEX_SUFFIXES)


class TestKnowledgeBaseIndex:
    def _kb_put(self, serverless: bool, put_status: int = 200):
        puts: list = []
        _Kb(serverless)._deploy_knowledge_base(_client(puts, put_status), lambda _p: None)
        assert len(puts) == 1 and puts[0][0] == "/awsorders-knowledge-base"
        return puts[0][1]

    def test_ech_keeps_the_settings(self):
        body = self._kb_put(serverless=False)
        assert body["settings"] == SETTINGS
        assert "mappings" in body

    def test_serverless_sends_no_settings_but_keeps_the_mappings(self):
        body = self._kb_put(serverless=True)
        assert "settings" not in body
        assert body["mappings"]["properties"]["error_type"] == {"type": "keyword"}

    def test_a_failed_create_is_logged(self, caplog):
        with caplog.at_level("WARNING", logger="deployer"):
            self._kb_put(serverless=True, put_status=400)
        assert "Failed to create knowledge base index awsorders-knowledge-base: HTTP 400" in caplog.text
