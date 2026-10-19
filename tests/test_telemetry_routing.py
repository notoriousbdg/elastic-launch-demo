"""Tests that OTLPClient routes each signal to the right per-deployment URL.

Run with:
    python -m pytest tests/test_telemetry_routing.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import app.telemetry as telemetry  # noqa: E402
from app.telemetry import OTLPClient  # noqa: E402


def _recording_client(otlp: OTLPClient) -> list[str]:
    """Swap *otlp*'s HTTP client for one that records request URLs."""
    urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        urls.append(str(request.url))
        return httpx.Response(200)

    otlp.client = httpx.Client(transport=httpx.MockTransport(handler))
    return urls


def _send_one_of_each(otlp: OTLPClient) -> None:
    from scenario_engine import get_scenario

    name, cfg = next(iter(get_scenario("space").services.items()))
    resource = OTLPClient.build_resource(name, cfg, namespace="ns")
    otlp.send_metrics(resource, [otlp.build_gauge("m", 1.0)])
    otlp.send_logs(resource, [otlp.build_log_record("INFO", "hello")])


@pytest.fixture()
def global_es_url(monkeypatch):
    """A process-wide ES URL that must NOT be used when a tenant URL is given."""
    monkeypatch.setattr(telemetry, "ELASTIC_URL", "https://global-es.example")
    monkeypatch.setenv("ELASTICSEARCH_URL", "https://global-es-env.example")


def test_metrics_use_tenant_es_url(global_es_url):
    otlp = OTLPClient(
        endpoint="https://ingest.example", api_key="k", elastic_url="https://tenant-es.example/"
    )
    urls = _recording_client(otlp)
    _send_one_of_each(otlp)

    assert urls == [
        "https://tenant-es.example/_otlp/v1/metrics",
        "https://ingest.example/v1/logs",
    ]


def test_reconfigure_keeps_tenant_url_unless_given(global_es_url):
    otlp = OTLPClient(endpoint="https://a.example", api_key="k", elastic_url="https://t1.example")
    otlp.reconfigure("https://b.example", "k")
    assert otlp.elastic_url == "https://t1.example"

    otlp.reconfigure("https://b.example", "k", elastic_url="https://t2.example/")
    urls = _recording_client(otlp)
    _send_one_of_each(otlp)
    assert urls[0] == "https://t2.example/_otlp/v1/metrics"


def test_falls_back_to_global_url_without_tenant_url(global_es_url):
    otlp = OTLPClient(endpoint="https://ingest.example", api_key="k")
    assert otlp._metrics_otlp_url() == "https://global-es.example/_otlp/v1/metrics"


def test_metrics_fall_back_to_ingest_without_any_es_url(monkeypatch):
    monkeypatch.setattr(telemetry, "ELASTIC_URL", "")
    monkeypatch.delenv("ELASTICSEARCH_URL", raising=False)
    otlp = OTLPClient(endpoint="https://ingest.example", api_key="k")
    urls = _recording_client(otlp)
    _send_one_of_each(otlp)
    assert urls[0] == "https://ingest.example/v1/metrics"


def test_scenario_instance_passes_context_es_url():
    from app.context import ScenarioContext
    from app.instance import ScenarioInstance
    from scenario_engine import get_scenario

    ctx = ScenarioContext.from_scenario(
        get_scenario("space"),
        otlp_endpoint="https://ingest.example",
        otlp_api_key="k",
        elastic_url="https://tenant-es.example",
        elastic_api_key="k",
    )
    inst = ScenarioInstance(ctx)
    try:
        assert inst.otlp._metrics_otlp_url() == "https://tenant-es.example/_otlp/v1/metrics"
    finally:
        inst.otlp.client.close()


def test_prepare_payload_hoists_schema_url_without_mutating():
    resource = {"attributes": [{"key": "service.name", "value": {"stringValue": "s"}}],
                "schemaUrl": "https://opentelemetry.io/schemas/1.35.0"}
    payload = {"resourceLogs": [{"resource": resource, "scopeLogs": []}]}

    out = OTLPClient.prepare_payload(payload)

    entry = out["resourceLogs"][0]
    assert entry["schemaUrl"] == "https://opentelemetry.io/schemas/1.35.0"
    assert "schemaUrl" not in entry["resource"]
    # The caller's (reused) resource dict is untouched.
    assert resource["schemaUrl"] == "https://opentelemetry.io/schemas/1.35.0"
    assert payload["resourceLogs"][0]["resource"] is resource


def test_sent_logs_carry_schema_url_on_the_envelope():
    import json

    otlp = OTLPClient(endpoint="https://ingest.example", api_key="k", elastic_url="https://es.example")
    bodies: list[bytes] = []
    otlp.client = httpx.Client(transport=httpx.MockTransport(
        lambda req: (bodies.append(req.content), httpx.Response(200))[1]
    ))
    _send_one_of_each(otlp)
    sent = json.loads(bodies[1])  # [0] is protobuf metrics, [1] JSON logs
    assert "schemaUrl" in sent["resourceLogs"][0]
    assert "schemaUrl" not in sent["resourceLogs"][0]["resource"]
