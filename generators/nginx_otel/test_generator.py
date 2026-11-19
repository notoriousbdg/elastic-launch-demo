"""Nginx impact_keys + resolve_active_chaos wiring tests."""

from __future__ import annotations

import random
import re
import threading
from unittest.mock import MagicMock, patch

import pytest

from app.telemetry import OTLPClient

from generators.nginx_otel import SPEC
from generators.nginx_otel.generator import (
    nginx_error_line,
    HEALTHY_ERROR_MESSAGES,
    _generate_access_log,
    _generate_error_log,
    build_error_log_bulk_doc,
    run,
)


def test_error_log_bulk_doc_has_request_health_fields():
    doc = build_error_log_bulk_doc(
        "xborder-nginx-01",
        log_level="error",
        error_msg="upstream timed out",
        process_pid=12345,
        body="[error] upstream timed out",
        timestamp_iso="2026-08-20T12:00:00.000Z",
    )
    assert doc["data_stream"]["dataset"] == "nginx.error.otel"
    assert doc["severity_text"] == "error"
    assert doc["attributes"]["log.level"] == "error"
    assert doc["attributes"]["process.pid"] == 12345
    assert doc["resource"]["attributes"]["host.name"] == "xborder-nginx-01"


def test_run_uses_resolve_active_chaos_and_emits_metrics():
    client = MagicMock()
    stop = threading.Event()
    sends = {"logs": 0}

    def _after_logs(*_a, **_k):
        sends["logs"] += 1
        if sends["logs"] >= 2:
            stop.set()

    client.send_logs.side_effect = _after_logs
    client.send_metrics_scoped = MagicMock()
    client.send_traces = MagicMock()

    with patch(
        "generators.nginx_otel.generator.resolve_active_chaos",
        return_value=None,
    ) as mocked:
        run(client, stop, scenario_data={
            "namespace": "testns",
            "generator_config": {},
            "channel_registry": {},
        })
        assert mocked.called
        assert client.send_metrics_scoped.called
        metrics = client.send_metrics_scoped.call_args.args[2]
        names = {m["name"] for m in metrics}
        assert "nginx.connections_accepted" in names
        assert "nginx.connections_handled" in names
        assert "nginx.connections_current" in names
        assert "nginx.requests" in names

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.nginx_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['dropped_connections_detected', 'error_log_spike', 'high_4xx_error_rate', 'high_5xx_error_rate', 'high_active_connections', 'high_error_rate_by_service']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True


def test_healthy_error_messages_are_non_severe():
    from generators.nginx_otel.generator import (
        HEALTHY_ERROR_MESSAGES,
        SEVERE_ERROR_MESSAGES,
        _SEVERE_LEVELS,
    )
    assert HEALTHY_ERROR_MESSAGES
    assert SEVERE_ERROR_MESSAGES
    assert all(lvl not in _SEVERE_LEVELS for lvl, _ in HEALTHY_ERROR_MESSAGES)
    assert all(lvl in _SEVERE_LEVELS for lvl, _ in SEVERE_ERROR_MESSAGES)


# ── nginx_otel@0.7.0 field shapes ──────────────────────────────────────────────
# 0.7.0 moved the dashboards to root ``log.level`` (an alias of severity_text) and
# ``attributes.network.protocol.version`` (was ``attributes.http.version``).

_PATHS = ["/", "/api/v1/items", "/healthz"]
_SERVERS = ["demo-nginx-01"]


@pytest.fixture
def otlp():
    client = OTLPClient(endpoint="http://127.0.0.1:9", api_key="")
    yield client
    client.close()


def _attrs(record: dict) -> dict:
    return {a["key"]: next(iter(a["value"].values())) for a in record.get("attributes", [])}


def test_access_log_uses_network_protocol_version_not_http_version(otlp):
    record, _span = _generate_access_log(
        otlp, random.Random(1), endpoints=_PATHS, server_names=_SERVERS, namespace="demo",
    )
    attrs = _attrs(record)
    assert attrs["network.protocol.version"] == "1.1"
    assert "http.version" not in attrs


@pytest.mark.parametrize("chaos,message", [(None, ""), ({"channel_id": 1}, "upstream timed out")])
def test_error_log_severity_text_carries_the_lowercase_nginx_level(otlp, chaos, message):
    """log.level is an alias of severity_text, so that is where the dashboards read it."""
    for seed in range(20):
        record = _generate_error_log(
            otlp, random.Random(seed), endpoints=_PATHS, server_names=_SERVERS,
            active_chaos=chaos, namespace="demo", error_message=message,
        )
        level = record["severityText"]
        assert level == level.lower() and level in {"warn", "notice", "error", "crit", "alert", "emerg"}
        if chaos is None:
            assert level in {lvl for lvl, _msg in HEALTHY_ERROR_MESSAGES}


def test_log_level_is_never_written_as_a_root_field(otlp):
    """A concrete root log.level collides with the alias and sends the document to the failure store."""
    record = _generate_error_log(
        otlp, random.Random(1), endpoints=_PATHS, server_names=_SERVERS, namespace="demo",
    )
    assert "log.level" not in record and "log" not in record
    doc = build_error_log_bulk_doc(
        "host", log_level="warn", error_msg="m", process_pid=1, body="b",
        timestamp_iso="2026-08-20T12:00:00.000Z",
    )
    assert "log.level" not in doc and "log" not in doc
    assert doc["severity_text"] == "warn"


# 0.9.0: latency, body-size and upstream-timing panels on the access log; error panels
# GROK nginx's own error_log line format to pull out the message.
_GROK = re.compile(r"\[(?P<level>\w+)\] \d+#\d+: (\*\d+ )?(?P<message>.*)")


def test_access_log_carries_the_0_9_0_request_fields(otlp):
    for seed in range(20):
        record, _span = _generate_access_log(
            otlp, random.Random(seed), endpoints=_PATHS, server_names=_SERVERS, namespace="demo",
        )
        a = _attrs(record)
        assert a["url.path"] in _PATHS
        assert int(a["http.response.body.size"]) == int(a["http.response.body.bytes"])
        total = float(a["http.server.request.duration"])
        assert total == float(a["nginx.request_time"])
        connect, header, response = (float(a[f"nginx.upstream.{k}.duration"]) for k in ("connect", "header", "response"))
        assert 0 < connect <= header <= response <= total


def test_error_lines_match_the_dashboard_grok(otlp):
    for seed in range(10):
        record = _generate_error_log(
            otlp, random.Random(seed), endpoints=_PATHS, server_names=_SERVERS, namespace="demo",
        )
        line = record["body"]["stringValue"]
        match = _GROK.search(line)
        assert match and match["level"] == record["severityText"]
        assert match["message"] and not match["message"].startswith("*")
    line = nginx_error_line(random.Random(1), "warn", 4321, "m", "10.0.0.1", "s", "/x")
    assert _GROK.search(line)["message"].startswith("m, client: 10.0.0.1")
