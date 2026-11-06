"""ES|QL-backed dashboard controls get the data views Kibana looks them up by.

Kibana 9.6 resolves an ES|QL options list control through an ad-hoc data view it
never saves, so package dashboards with such controls (the GCP OTel packs) show
"Could not locate that data view (id: …)". The deployer saves a data view with
that id after installing packages.
"""
from __future__ import annotations

import json

import httpx

from elastic_config.deployer_integrations import (
    IntegrationsMixin,
    esql_control_data_view_id,
    esql_control_patterns,
)

# Ids Kibana reported as missing on a staging-3 demo, per control index pattern.
REPORTED = {
    "metrics-gcp.cloudrun.otel-default":
        "b2d7fccc435fede66c7ef58945bf7ae7e908f3713b2501f1b170fb037b2c296c",
    "metrics-gcp.cloudsql_mysql.otel-default":
        "b4d3dcbf970afb3ca37d31b39fbb5868a8cef776641c7676ef57c13fb13118b1",
    "metrics-gcp.cloudsql_sqlserver.otel-*":
        "c96f91cac7e63c7602fa446be4ecd38aa8e4ba7b6f51a2d9a547e0810099318c",
    "metrics-gcp.cloudsql_postgresql.otel-*":
        "b354a757ecc5ca4e33714ef491bb3eaf50545fcc7cb4148149f9111e7e91f6ce",
}


def _control(query: str, source: str = "esql", kind: str = "options_list_control") -> dict:
    return {"type": kind, "config": {"values_source": source, "esql_query": query}}


def _dashboard(*controls: dict) -> dict:
    return {"pinned_panels": {"panels": {str(i): c for i, c in enumerate(controls)}}}


def test_data_view_id_matches_what_kibana_looks_up():
    for pattern, view_id in REPORTED.items():
        assert esql_control_data_view_id(pattern) == view_id


def test_patterns_come_from_esql_options_list_controls_only():
    attrs = _dashboard(
        _control("FROM metrics-gcp.cloudrun.otel-default | STATS BY `service_name`"),
        _control("TS metrics-gcp.cloudsql_sqlserver.otel-*\n| STATS BY database_id"),
        _control("FROM metrics-ignored-*", source="field"),
        _control("FROM metrics-ignored-2-*", kind="esql_control"),
    )
    assert esql_control_patterns(attrs) == {
        "metrics-gcp.cloudrun.otel-default", "metrics-gcp.cloudsql_sqlserver.otel-*"}


def test_patterns_accept_a_json_string_and_dashboards_without_controls():
    attrs = _dashboard(_control("FROM a-*, b-* | STATS BY x"))
    assert esql_control_patterns({"pinned_panels": json.dumps(attrs["pinned_panels"])}) == {"a-*,b-*"}
    assert esql_control_patterns({}) == set()


class _Deployer(IntegrationsMixin):
    kibana_url = "http://kibana.test"


def test_deploy_step_saves_one_data_view_per_control_pattern():
    dashboard = {"type": "dashboard", "id": "d1", "attributes": _dashboard(
        _control("FROM metrics-gcp.cloudrun.otel-default | STATS BY service_name"),
        _control("FROM metrics-gcp.cloudrun.otel-default | STATS BY region"),
    )}
    saved: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/api/fleet/epm/packages/gcp_cloudrun_otel":
            return httpx.Response(200, json={"item": {"installationInfo": {"installed_kibana": [
                {"type": "dashboard", "id": "d1"}, {"type": "tag", "id": "t"}]}}})
        if path == "/api/saved_objects/_export":
            return httpx.Response(200, text=json.dumps(dashboard) + "\n"
                                  + json.dumps({"exportedCount": 1}) + "\n")
        if path == "/api/data_views/data_view":
            saved.append(json.loads(request.content))
            return httpx.Response(200, json={"data_view": {}})
        return httpx.Response(404, json={})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    count = _Deployer()._ensure_esql_control_data_views(client, {}, ["gcp_cloudrun_otel"])
    assert count == 1
    assert saved == [{"data_view": {
        "id": REPORTED["metrics-gcp.cloudrun.otel-default"],
        "title": "metrics-gcp.cloudrun.otel-default",
        "name": "metrics-gcp.cloudrun.otel-default",
        "timeFieldName": "@timestamp"}, "override": True}]


def test_deploy_step_skips_packages_without_dashboards():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/api/fleet/epm/packages/"):
            return httpx.Response(200, json={"item": {"installationInfo": {"installed_kibana": []}}})
        raise AssertionError(f"unexpected call {request.url.path}")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    assert _Deployer()._ensure_esql_control_data_views(client, {}, ["redis_otel"]) == 0
