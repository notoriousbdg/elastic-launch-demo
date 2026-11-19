"""Azure Application Gateway-specific tests for the azure_gateway generator.

Standard emit/registry tests live in tests/test_generators_emit.py.
These cover dashboard field contracts that those shared tests do not.
"""
from __future__ import annotations

import importlib
import random
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
        self.resources: list[dict] = []
        self.scopes: list[dict] = []

    def send_logs_scoped(self, resource, scope, records):
        self.resources.append(resource)
        self.scopes.append(scope)
        self.logs.extend(records)

    def close(self):
        pass


def _attr_map(record: dict) -> dict:
    out = {}
    for a in record.get("attributes", []):
        val = a["value"]
        if "stringValue" in val:
            out[a["key"]] = val["stringValue"]
        elif "intValue" in val:
            out[a["key"]] = int(val["intValue"])
        elif "doubleValue" in val:
            out[a["key"]] = float(val["doubleValue"])
        elif "boolValue" in val:
            out[a["key"]] = val["boolValue"]
    return out


def _resource_attr_map(resource: dict) -> dict:
    out = {}
    for a in resource.get("attributes", []):
        val = a["value"]
        if "stringValue" in val:
            out[a["key"]] = val["stringValue"]
        elif "intValue" in val:
            out[a["key"]] = int(val["intValue"])
    return out


def _run(records_per_cycle: int = 40) -> _StubClient:
    mod = importlib.import_module("generators.azure_gateway_otel.generator")
    client = _StubClient()
    stop = threading.Event()
    threading.Timer(0.2, stop.set).start()
    mod.run(client, stop, scenario_data={
        "namespace": "test",
        "channel_registry": {},
        "generator_config": {"records_per_cycle": records_per_cycle},
    })
    return client


def test_access_dashboard_fields_present():
    mod = importlib.import_module("generators.azure_gateway_otel.generator")
    record = mod._build_access_record(
        gateway_host="api.test.contoso.com",
        backends=["10.0.0.1:8080"],
        instance_id="appgw_0",
        rng=random.Random(1),
        error_rate=0.0,
        jitter_ns=0,
    )
    attrs = _attr_map(record)
    # Force a non-terminated sample for pool/setting assertions.
    for _ in range(50):
        record = mod._build_access_record(
            gateway_host="api.test.contoso.com",
            backends=["10.0.0.1:8080"],
            instance_id="appgw_0",
            rng=random.Random(),
            error_rate=0.0,
            jitter_ns=0,
        )
        attrs = _attr_map(record)
        if "azure.agw.backend.status_code" in attrs:
            break
    assert attrs["azure.category"] == "ApplicationGatewayAccessLog"
    assert isinstance(attrs["http.response.status_code"], int)
    assert isinstance(attrs["azure.request.duration"], float)
    assert "azure.agw.listener.name" in attrs
    assert "azure.agw.backend.pool.name" in attrs
    assert "azure.agw.backend.setting.name" in attrs
    assert "azure.agw.backend.latency" in attrs
    assert "client.address" in attrs
    assert "host.name" in attrs
    assert "url.path" in attrs


def test_gateway_terminated_omits_backend_status():
    mod = importlib.import_module("generators.azure_gateway_otel.generator")
    terminated = []
    for seed in range(200):
        attrs = _attr_map(mod._build_access_record(
            gateway_host="api.test.contoso.com",
            backends=["10.0.0.1:8080"],
            instance_id="appgw_0",
            rng=random.Random(seed),
            error_rate=0.0,
            jitter_ns=0,
        ))
        if "azure.agw.backend.status_code" not in attrs:
            terminated.append(attrs)
    assert len(terminated) >= 10, f"expected gateway-terminated samples, got {len(terminated)}"
    for attrs in terminated:
        assert "error.type" in attrs
        assert "azure.agw.backend.latency" not in attrs
        assert "azure.agw.backend.pool.name" not in attrs
        assert "azure.agw.backend.setting.name" not in attrs


def test_firewall_dashboard_fields_and_actions():
    mod = importlib.import_module("generators.azure_gateway_otel.generator")
    actions = set()
    for seed in range(40):
        record = mod._build_firewall_record(
            gateway_host="api.test.contoso.com",
            subscription_id="11111111-1111-1111-1111-111111111111",
            resource_group="test-rg",
            gateway_name="test-agw",
            instance_id="appgw_0",
            rng=random.Random(seed),
            jitter_ns=0,
        )
        attrs = _attr_map(record)
        assert attrs["azure.category"] == "ApplicationGatewayFirewallLog"
        assert attrs["security_rule.action"] in {"Blocked", "Detected", "Matched"}
        actions.add(attrs["security_rule.action"])
        assert "security_rule.ruleset.name" in attrs
        assert "azure.firewall.policy.scope.type" in attrs
        assert "azure.firewall.policy.object.name" in attrs
        assert "client.address" in attrs
        assert "host.name" in attrs
        assert "url.original" in attrs
        assert record.get("body", {}).get("stringValue")
    assert "Blocked" in actions
    assert "Detected" in actions


def test_resource_has_cloud_resource_id_and_instances():
    client = _run(records_per_cycle=20)
    assert client.logs
    assert client.resources
    instance_ids = set()
    for resource in client.resources:
        attrs = _resource_attr_map(resource)
        assert "cloud.resource_id" in attrs
        assert attrs["cloud.resource_id"].startswith("/SUBSCRIPTIONS/")
        assert "APPLICATIONGATEWAYS" in attrs["cloud.resource_id"]
        assert "service.instance.id" in attrs
        instance_ids.add(attrs["service.instance.id"])
    assert instance_ids
    assert instance_ids <= {"test-agw-01", "test-agw-02", "test-agw-03"}


def test_scope_encoding_format():
    client = _run(records_per_cycle=5)
    assert client.scopes
    scope = client.scopes[0]
    assert "azureencodingextension" in scope["name"]
    scope_attrs = {a["key"]: a["value"].get("stringValue") for a in scope.get("attributes", [])}
    assert scope_attrs.get("encoding.format") == "azure.application_gateway"


def test_duration_is_double_not_string():
    mod = importlib.import_module("generators.azure_gateway_otel.generator")
    record = mod._build_access_record(
        gateway_host="api.test.contoso.com",
        backends=["10.0.0.1:8080"],
        instance_id="appgw_0",
        rng=random.Random(42),
        error_rate=0.0,
        jitter_ns=0,
    )
    for a in record["attributes"]:
        if a["key"] == "azure.request.duration":
            assert "doubleValue" in a["value"]
            assert isinstance(a["value"]["doubleValue"], float)
            return
    raise AssertionError("azure.request.duration missing")


def test_clients_hosts_and_policies_vary_like_a_real_gateway():
    client = _run(records_per_cycle=200)
    access, waf = [], []
    for record in client.logs:
        attrs = _attr_map(record)
        (waf if attrs["azure.category"] == "ApplicationGatewayFirewallLog" else access).append(attrs)
    assert access and waf
    assert len({a["host.name"] for a in access}) > 1
    assert all(a["http.response.size"] > 0 for a in access)
    # Returning clients: the top client accounts for more than one request.
    counts: dict = {}
    for a in access:
        counts[a["client.address"]] = counts.get(a["client.address"], 0) + 1
    assert max(counts.values()) > 1
    assert len({a["client.address"] for a in waf}) <= 12
    assert all(a["azure.firewall.policy.id"] and a["security_rule.uuid"] for a in waf)
