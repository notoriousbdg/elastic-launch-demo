"""Technology-specific tests for the ``aws_elb_otel`` generator.

Standard emit/registry tests live in tests/test_generators_emit.py
(parametrized over GENERATORS) and run for this generator automatically.
Add tests here only for behaviour that is special to aws_elb_otel.
"""
from __future__ import annotations

import importlib
import threading

import pytest

try:
    import dotenv  # noqa: F401
    _HAS_DOTENV = True
except ImportError:
    _HAS_DOTENV = False

pytestmark = pytest.mark.skipif(
    not _HAS_DOTENV,
    reason="Generator modules require dotenv via app.telemetry; run inside Docker",
)


class _StubClient:
    def __init__(self):
        self.logs: list[dict] = []

    def send_logs_scoped(self, resource, scope, records):
        self.logs.extend(records)

    def close(self):
        pass


def _run() -> _StubClient:
    mod = importlib.import_module("generators.aws_elb_otel.generator")
    client = _StubClient()
    stop = threading.Event()
    threading.Timer(0.15, stop.set).start()
    mod.run(client, stop, scenario_data={
        "namespace": "test",
        "channel_registry": {},
        "generator_config": {"records_per_cycle": 5},
    })
    return client


def test_otlp_camelcase_records():
    client = _run()
    assert client.logs
    for r in client.logs:
        assert "timeUnixNano" in r
        assert "observedTimeUnixNano" in r
        assert "eventName" in r
        assert isinstance(r["attributes"], list)


def test_status_code_is_int():
    client = _run()
    status_vals = []
    for r in client.logs:
        for a in r["attributes"]:
            if a["key"] == "aws.elb.status.code":
                assert "intValue" in a["value"]
                status_vals.append(int(a["value"]["intValue"]))
    assert status_vals
    assert all(100 <= s < 600 for s in status_vals)


def test_dashboard_fields_present():
    required = {
        "aws.elb.status.code",
        "client.address",
        "cloud.resource_id",
        "http.request.size",
        "http.response.size",
        "network.protocol.name",
        "url.full",
    }
    client = _run()
    keys = {a["key"] for a in client.logs[0]["attributes"]}
    assert required <= keys
