"""Tests for logs-otel / logs@custom Log Rate Analysis heals."""

from __future__ import annotations

import generator_engine.logs_otel_error_heal as mod


class _Resp:
    def __init__(self, status_code: int, payload=None, text: str = ""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text or str(payload)

    def json(self):
        return self._payload


def test_put_pipeline_and_merge_preserve_mappings(monkeypatch):
    calls = []

    def fake_get(url, headers=None, timeout=None):
        calls.append(("GET", url))
        if "/_mapping/field/" in url:
            # Pretend stubs already present so reset is a no-op in this test.
            field = url.rsplit("/", 1)[-1]
            return _Resp(
                200,
                {".ds-probe": {"mappings": {field: {"mapping": {"type": "keyword"}}}}},
            )
        if mod._LOGS_CUSTOM in url:
            return _Resp(404)
        return _Resp(
            200,
            {
                "component_templates": [
                    {
                        "component_template": {
                            "template": {
                                "mappings": {
                                    "properties": {
                                        "body": {
                                            "type": "object",
                                            "properties": {
                                                "structured": {"type": "object"},
                                            },
                                        }
                                    }
                                },
                                "settings": {"index": {"number_of_replicas": "0"}},
                            }
                        }
                    }
                ]
            },
        )

    def fake_put(url, headers=None, json=None, timeout=None):
        calls.append(("PUT", url, json))
        return _Resp(200, {"acknowledged": True})

    monkeypatch.setattr(mod.httpx, "get", fake_get)
    monkeypatch.setattr(mod.httpx, "put", fake_put)
    monkeypatch.setattr(mod.httpx, "post", lambda *a, **k: _Resp(200, {"fields": {}}))
    monkeypatch.setattr(mod.httpx, "delete", lambda *a, **k: _Resp(404))

    mod.ensure_error_object_mapping("https://es.example", "key", reset_streams=True)

    put_calls = [c for c in calls if c[0] == "PUT"]
    assert any(mod._PIPELINE_NAME in c[1] for c in put_calls)
    custom = next(c for c in put_calls if mod._LOGS_OTEL_CUSTOM in c[1])
    body = custom[2]
    assert body["template"]["settings"]["index"]["default_pipeline"] == mod._PIPELINE_NAME
    assert (
        body["template"]["mappings"]["properties"]["body"]["properties"]["structured"][
            "type"
        ]
        == "object"
    )
    logs_custom = next(c for c in put_calls if mod._LOGS_CUSTOM in c[1])
    stubs = logs_custom[2]["template"]["mappings"]["properties"]
    assert stubs["error"]["properties"]["type"]["type"] == "keyword"
    assert stubs["event"]["properties"]["type"]["type"] == "keyword"
    assert stubs["source"]["properties"]["geo"]["properties"]["region_name"]["type"] == "keyword"


def test_reset_streams_on_keyword_object_conflict(monkeypatch):
    deleted = []

    monkeypatch.setattr(mod, "_put_pipeline", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_merge_default_pipeline_setting", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_merge_logs_custom_ecs_stubs", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_error_field_types", lambda *a, **k: {"keyword", "object"})
    monkeypatch.setattr(
        mod, "_list_generic_otel_streams", lambda *a, **k: ["logs-generic.otel-default"]
    )
    monkeypatch.setattr(mod, "_reset_streams_missing_ecs_stubs", lambda *a, **k: None)
    monkeypatch.setattr(
        mod, "_delete_data_stream", lambda _u, _k, name: deleted.append(name)
    )

    mod.ensure_error_object_mapping("https://es.example", "key", reset_streams=True)
    assert deleted == ["logs-generic.otel-default"]


def test_no_reset_when_only_object(monkeypatch):
    deleted = []
    monkeypatch.setattr(mod, "_put_pipeline", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_merge_default_pipeline_setting", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_merge_logs_custom_ecs_stubs", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_error_field_types", lambda *a, **k: {"object"})
    monkeypatch.setattr(
        mod, "_list_generic_otel_streams", lambda *a, **k: ["logs-generic.otel-default"]
    )
    monkeypatch.setattr(mod, "_reset_streams_missing_ecs_stubs", lambda *a, **k: None)
    monkeypatch.setattr(
        mod, "_delete_data_stream", lambda _u, _k, name: deleted.append(name)
    )

    mod.ensure_error_object_mapping("https://es.example", "key", reset_streams=True)
    assert deleted == []


def test_reset_azure_when_error_type_unmapped(monkeypatch):
    deleted = []
    monkeypatch.setattr(
        mod,
        "_field_mapped_on_stream",
        lambda _u, _k, stream, field: not (
            stream.startswith("logs-azure_openai") and field == "error.type"
        ),
    )
    monkeypatch.setattr(
        mod, "_delete_data_stream", lambda _u, _k, name: deleted.append(name)
    )
    mod._reset_streams_missing_ecs_stubs("https://es.example", "key")
    assert "logs-azure_openai.logs-default" in deleted
    assert "logs-azure_openai.logs-xborder" in deleted
