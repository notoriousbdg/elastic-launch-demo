"""Step 21 turns package SLO templates and ML modules into live, cluster-shared objects.

SLOs and ML jobs keep template-derived ids, carry no scenario namespace and are
switched off (not deleted) when their package is not part of the active scenario.
"""
from __future__ import annotations

import json
import re

import httpx
import pytest

from pathlib import Path

from elastic_config.deployer_integrations import IntegrationsMixin
from elastic_config.deployer_workflow_templates import WorkflowTemplatesMixin
from elastic_config.deployer_workflows import WorkflowsMixin
from generator_engine.registry import GENERATORS
from elastic_config.deployer_package_templates import (
    ML_PACKAGE_SETTING,
    OOTB_PACKAGE_TAG_PREFIX,
    OOTB_TEMPLATE_TAG,
    PackageTemplatesMixin,
    slo_body_from_template,
    slo_id_for_template,
)

KIBANA = "http://kibana.test"
ES = "http://es.test"

SLO_TEMPLATE = {
    "id": "aws_rds_otel-avg-read-latency-99.5-Rolling30Days",
    "type": "slo_template",
    "attributes": {
        "name": "[AWS RDS OTel] Average read latency 99.5% rolling 30 days",
        "description": "read latency",
        "indicator": {"type": "sli.metric.timeslice", "params": {"index": "metrics-*"}},
        "budgetingMethod": "timeslices",
        "timeWindow": {"duration": "30d", "type": "rolling"},
        "objective": {"target": 0.995, "timesliceTarget": 0.95, "timesliceWindow": "1m"},
        "tags": ["aws_rds", "otel"],
        "groupBy": ["cloud.region", "DBInstanceIdentifier"],
        "artifacts": {"dashboards": [{"id": "dash-1"}]},
    },
}

ML_MODULE = {
    "id": "aws_rds_otel-metrics-ml",
    "type": "ml-module",
    "attributes": {
        "id": "aws_rds_otel-metrics-ml",
        "defaultIndexPattern": "metrics-aws.rds.otel-*",
        "query": {"bool": {"filter": [{"term": {"Namespace": "AWS/RDS"}}]}},
        "jobs": [{"id": "aws_rds_instance_resource_anomaly", "config": {
            "groups": ["aws"], "custom_settings": {"created_by": "ml-module-aws-rds"},
            "analysis_config": {"bucket_span": "15m", "detectors": []},
            "data_description": {"time_field": "@timestamp"},
        }}],
        "datafeeds": [{
            "id": "datafeed-aws_rds_instance_resource_anomaly",
            "job_id": "aws_rds_instance_resource_anomaly",
            "config": {"job_id": "aws_rds_instance_resource_anomaly", "indices": ["INDEX_PATTERN_NAME"]},
        }],
    },
}


class _Step:
    def __init__(self):
        self.status = ""
        self.detail = ""
        self.items_done = 0
        self.items_total = 0


class _Host(IntegrationsMixin, PackageTemplatesMixin):
    def __init__(self, packages: list[str]):
        self.kibana_url = KIBANA
        self.elastic_url = ES
        self.api_key = "key"
        self.progress = object()
        self.step = _Step()
        self._packages = packages

    def _step(self, _idx):
        return self.step

    def _packages_to_install(self):
        return list(self._packages)


class _Api:
    """Route table for httpx.MockTransport; records every request."""

    def __init__(self):
        self.seen: list[httpx.Request] = []
        self.routes: list[tuple[str, re.Pattern, object]] = []

    def on(self, method: str, path: str, reply):
        self.routes.append((method, re.compile(f"^{path}$"), reply))
        return self

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.seen.append(request)
        for method, pattern, reply in self.routes:
            if method == request.method and pattern.match(request.url.path):
                return reply(request) if callable(reply) else reply
        return httpx.Response(404, json={"message": "not found"})

    def calls(self, method: str, path: str) -> list[httpx.Request]:
        return [r for r in self.seen if r.method == method and re.match(f"^{path}$", r.url.path)]


def _json(request: httpx.Request) -> dict:
    return json.loads(request.content or b"{}")


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("elastic_config.deployer_base.time.sleep", lambda *_a, **_k: None)


def _run(api: _Api, fn):
    with httpx.Client(transport=httpx.MockTransport(api)) as client:
        return fn(client)


# ── SLO id and body ──

@pytest.mark.parametrize("tid", [
    "aws_rds_otel-avg-read-latency-99.5-Rolling30Days",
    "kafka_otel-consumer-lag-99-Rolling7Days-with-a-very-long-template-name-indeed",
    "abc",
])
def test_slo_id_is_valid_and_stable(tid):
    sid = slo_id_for_template(tid)
    assert re.fullmatch(r"[a-z0-9_-]{8,48}", sid)
    assert slo_id_for_template(tid) == sid


# Template ids whose SLO ids Kibana rejected on the serverless aws deploy (HTTP 400
# invalid_format, pattern /^[a-z0-9-_]+$/): the ``-Rolling30Days`` suffix kept an
# uppercase letter, whether the id was used as is or cut to ``...-Ro-<hash>``.
@pytest.mark.parametrize("tid", [
    "nginx_otel-request-availability-99-Rolling30Days",
    "nginx_otel-connection-drop-rate-99.5-Rolling30Days",
    "aws_sqs_otel-oldest-message-age-99.5-Rolling30Days",
    "aws_sqs_otel-dlq-empty-99.5-Rolling30Days",
    "aws_rds_otel-avg-read-latency-99.5-Rolling30Days",
    "aws_lambda_otel-avg-duration-99.5-Rolling30Days",
])
def test_slo_id_has_no_uppercase_for_the_ids_kibana_rejected(tid):
    sid = slo_id_for_template(tid)
    assert re.fullmatch(r"[a-z0-9_-]{8,48}", sid), sid


def test_every_package_slo_template_gets_a_valid_unique_id():
    templates = json.loads((Path(__file__).parent / "fixtures" / "slo_templates.json").read_text())
    ids = {tid: slo_id_for_template(tid) for tid in templates}
    bad = {tid: sid for tid, sid in ids.items() if not re.fullmatch(r"[a-z0-9_-]{8,48}", sid)}
    assert not bad
    assert len(set(ids.values())) == len(ids), "two templates share an SLO id"


def _slo_id_before_the_lowercase_fix(template_id: str) -> str:
    """The id algorithm as it was: sanitized without lowercasing."""
    import hashlib
    clean = re.sub(r"[^A-Za-z0-9_-]", "_", template_id)
    if 8 <= len(clean) <= 48:
        return clean
    digest = hashlib.sha1(template_id.encode()).hexdigest()[:8]
    return f"{clean[:48 - len(digest) - 1]}-{digest}"


def test_slo_ids_kibana_already_accepted_do_not_move():
    """An SLO that exists on a cluster keeps its id: only ids with an uppercase
    letter (which Kibana rejected, so no such SLO exists) change."""
    templates = json.loads((Path(__file__).parent / "fixtures" / "slo_templates.json").read_text())
    accepted = {tid: _slo_id_before_the_lowercase_fix(tid) for tid in templates
                if re.fullmatch(r"[a-z0-9_-]{8,48}", _slo_id_before_the_lowercase_fix(tid))}
    assert accepted, "fixture should hold SLOs whose old id was valid"
    moved = {tid: (old, slo_id_for_template(tid)) for tid, old in accepted.items()
             if slo_id_for_template(tid) != old}
    assert not moved


def test_slo_id_digest_is_taken_from_the_original_case():
    """Only the readable part is lowercased; two ids that differ only by case still diverge."""
    long_a = "kafka_otel-" + "x" * 60 + "-Rolling30Days"
    long_b = "kafka_otel-" + "x" * 60 + "-rolling30days"
    assert slo_id_for_template(long_a) != slo_id_for_template(long_b)


def test_long_slo_ids_stay_distinct():
    base = "kafka_otel-" + "x" * 60
    assert slo_id_for_template(base + "-a") != slo_id_for_template(base + "-b")


def test_slo_body_keeps_template_fields_and_adds_package_tags():
    body = slo_body_from_template(SLO_TEMPLATE, "aws_rds_otel")
    assert "id" not in body and "type" not in body
    assert body["groupBy"] == ["cloud.region", "DBInstanceIdentifier"]
    assert body["artifacts"] == {"dashboards": [{"id": "dash-1"}]}
    assert body["tags"] == ["aws_rds", "otel", OOTB_TEMPLATE_TAG, f"{OOTB_PACKAGE_TAG_PREFIX}aws_rds_otel"]


def test_slo_body_rejects_incomplete_templates():
    assert slo_body_from_template({"attributes": {"name": "x"}}, "p") is None


# ── SLO upsert ──

def test_new_slo_is_created_with_its_stable_id():
    api = _Api().on("POST", "/api/observability/slos", httpx.Response(200, json={"id": "x"}))
    host = _Host(["aws_rds_otel"])
    assert _run(api, lambda c: host._upsert_ootb_slo(c, "aws_rds_otel", SLO_TEMPLATE["id"], SLO_TEMPLATE, {}))
    sent = _json(api.calls("POST", "/api/observability/slos")[0])
    assert sent["id"] == slo_id_for_template(SLO_TEMPLATE["id"])
    assert sent["name"].startswith("[AWS RDS OTel]")


def test_existing_disabled_slo_is_updated_and_enabled():
    sid = slo_id_for_template(SLO_TEMPLATE["id"])
    api = (
        _Api()
        .on("GET", f"/api/observability/slos/{sid}", httpx.Response(200, json={"id": sid, "enabled": False}))
        .on("PUT", f"/api/observability/slos/{sid}", httpx.Response(200, json={}))
        .on("POST", f"/api/observability/slos/{sid}/enable", httpx.Response(204))
    )
    host = _Host(["aws_rds_otel"])
    assert _run(api, lambda c: host._upsert_ootb_slo(c, "aws_rds_otel", SLO_TEMPLATE["id"], SLO_TEMPLATE, {}))
    assert "id" not in _json(api.calls("PUT", f"/api/observability/slos/{sid}")[0])
    assert api.calls("POST", f"/api/observability/slos/{sid}/enable")
    assert not api.calls("POST", "/api/observability/slos")


def test_slo_create_retries_without_artifacts_when_rejected():
    def create(request):
        if "artifacts" in _json(request):
            return httpx.Response(400, json={"message": "Excess keys are not allowed: artifacts"})
        return httpx.Response(200, json={})

    api = _Api().on("POST", "/api/observability/slos", create)
    host = _Host(["aws_rds_otel"])
    assert _run(api, lambda c: host._upsert_ootb_slo(c, "aws_rds_otel", SLO_TEMPLATE["id"], SLO_TEMPLATE, {}))
    assert len(api.calls("POST", "/api/observability/slos")) == 2


def test_slo_create_does_not_retry_a_400_that_is_not_about_artifacts():
    """A bad id fails the same way twice; retrying only doubles the call and hides the first reason."""
    api = _Api().on(
        "POST", "/api/observability/slos",
        httpx.Response(400, json={"message": "invalid_format: id must match /^[a-z0-9-_]+$/"}),
    )
    host = _Host(["aws_rds_otel"])
    assert not _run(api, lambda c: host._upsert_ootb_slo(c, "aws_rds_otel", SLO_TEMPLATE["id"], SLO_TEMPLATE, {}))
    assert len(api.calls("POST", "/api/observability/slos")) == 1


def test_slo_failure_log_keeps_the_error_body(caplog):
    long_reason = "invalid_format " + "x" * 600 + " path-marker"
    api = _Api().on("POST", "/api/observability/slos", httpx.Response(400, json={"message": long_reason}))
    host = _Host(["aws_rds_otel"])
    with caplog.at_level("WARNING", logger="deployer"):
        _run(api, lambda c: host._upsert_ootb_slo(c, "aws_rds_otel", SLO_TEMPLATE["id"], SLO_TEMPLATE, {}))
    assert "path-marker" in caplog.text


def test_only_inactive_package_slos_are_disabled():
    slos = [
        {"id": "active-slo-1", "enabled": True, "tags": [OOTB_TEMPLATE_TAG, f"{OOTB_PACKAGE_TAG_PREFIX}aws_rds_otel"]},
        {"id": "orphan-slo-1", "enabled": True, "tags": [OOTB_TEMPLATE_TAG, f"{OOTB_PACKAGE_TAG_PREFIX}kafka_otel"]},
        # Same SLO again (one row per groupBy instance) — disabled once.
        {"id": "orphan-slo-1", "enabled": True, "tags": [OOTB_TEMPLATE_TAG, f"{OOTB_PACKAGE_TAG_PREFIX}kafka_otel"]},
        {"id": "already-off", "enabled": False, "tags": [OOTB_TEMPLATE_TAG, f"{OOTB_PACKAGE_TAG_PREFIX}redis_otel"]},
        {"id": "scenario-slo", "enabled": True, "tags": ["auto-created", "awsorders"]},
    ]
    api = (
        _Api()
        .on("GET", "/api/observability/slos", httpx.Response(200, json={"results": slos, "total": 5, "perPage": 500}))
        .on("POST", r"/api/observability/slos/[^/]+/disable", httpx.Response(204))
    )
    host = _Host(["aws_rds_otel"])
    assert _run(api, lambda c: host._disable_orphaned_ootb_slos(c, {}, ["aws_rds_otel"])) == 1
    assert [r.url.path for r in api.calls("POST", r"/api/observability/slos/[^/]+/disable")] == [
        "/api/observability/slos/orphan-slo-1/disable"
    ]


# ── ML modules ──

def test_ml_module_setup_keeps_job_ids_and_tags_the_package():
    api = _Api().on("POST", "/internal/ml/modules/setup/aws_rds_otel-metrics-ml", httpx.Response(200, json={
        "jobs": [{"id": "aws_rds_instance_resource_anomaly", "success": True}],
        "datafeeds": [{"id": "datafeed-aws_rds_instance_resource_anomaly", "success": True, "started": True}],
    }))
    host = _Host(["aws_rds_otel"])
    assert _run(api, lambda c: host._setup_ootb_ml_module(c, "aws_rds_otel", ML_MODULE, {})) == (1, 0)
    request = api.calls("POST", "/internal/ml/modules/setup/aws_rds_otel-metrics-ml")[0]
    body = _json(request)
    assert request.headers["elastic-api-version"] == "1"
    assert body["prefix"] == ""
    assert body["startDatafeed"] is True
    assert body["indexPatternName"] == "metrics-aws.rds.otel-*"
    assert body["jobOverrides"] == {"custom_settings": {ML_PACKAGE_SETTING: "aws_rds_otel"}}


def test_existing_ml_job_is_opened_and_its_datafeed_started():
    api = (
        _Api()
        .on("POST", "/internal/ml/modules/setup/aws_rds_otel-metrics-ml", httpx.Response(200, json={
            "jobs": [{"id": "aws_rds_instance_resource_anomaly", "success": False,
                      "error": {"error": {"type": "resource_already_exists_exception"}}}],
            "datafeeds": [{"id": "datafeed-aws_rds_instance_resource_anomaly", "success": False,
                           "error": "resource_already_exists_exception"}],
        }))
        .on("POST", "/_ml/anomaly_detectors/aws_rds_instance_resource_anomaly/_open", httpx.Response(200, json={}))
        .on("POST", "/_ml/datafeeds/datafeed-aws_rds_instance_resource_anomaly/_start",
            httpx.Response(409, json={"error": "already started"}))
    )
    host = _Host(["aws_rds_otel"])
    assert _run(api, lambda c: host._setup_ootb_ml_module(c, "aws_rds_otel", ML_MODULE, {})) == (1, 0)


def test_unknown_module_falls_back_to_the_es_ml_api():
    api = (
        _Api()
        .on("POST", "/internal/ml/modules/setup/aws_rds_otel-metrics-ml", httpx.Response(404, json={}))
        .on("PUT", "/_ml/anomaly_detectors/aws_rds_instance_resource_anomaly", httpx.Response(200, json={}))
        .on("PUT", "/_ml/datafeeds/datafeed-aws_rds_instance_resource_anomaly", httpx.Response(200, json={}))
        .on("POST", "/_ml/anomaly_detectors/aws_rds_instance_resource_anomaly/_open", httpx.Response(200, json={}))
        .on("POST", "/_ml/datafeeds/datafeed-aws_rds_instance_resource_anomaly/_start", httpx.Response(200, json={}))
    )
    host = _Host(["aws_rds_otel"])
    assert _run(api, lambda c: host._setup_ootb_ml_module(c, "aws_rds_otel", ML_MODULE, {})) == (1, 0)
    job = _json(api.calls("PUT", "/_ml/anomaly_detectors/aws_rds_instance_resource_anomaly")[0])
    assert job["custom_settings"] == {"created_by": "ml-module-aws-rds", ML_PACKAGE_SETTING: "aws_rds_otel"}
    feed = _json(api.calls("PUT", "/_ml/datafeeds/datafeed-aws_rds_instance_resource_anomaly")[0])
    assert feed["indices"] == ["metrics-aws.rds.otel-*"]
    assert feed["query"] == ML_MODULE["attributes"]["query"]


def test_only_started_datafeeds_of_inactive_packages_are_stopped():
    api = (
        _Api()
        .on("GET", "/_ml/anomaly_detectors", httpx.Response(200, json={"jobs": [
            {"job_id": "rds_job", "custom_settings": {ML_PACKAGE_SETTING: "aws_rds_otel"}},
            {"job_id": "k8s_job", "custom_settings": {ML_PACKAGE_SETTING: "kubernetes_otel"}},
            {"job_id": "k8s_idle", "custom_settings": {ML_PACKAGE_SETTING: "kubernetes_otel"}},
            {"job_id": "apm-awsorders-x", "custom_settings": {}},
        ]}))
        .on("GET", "/_ml/datafeeds/_stats", httpx.Response(200, json={"datafeeds": [
            {"datafeed_id": "datafeed-rds_job", "state": "started"},
            {"datafeed_id": "datafeed-k8s_job", "state": "started"},
            {"datafeed_id": "datafeed-k8s_idle", "state": "stopped"},
            {"datafeed_id": "datafeed-apm-awsorders-x", "state": "started"},
        ]}))
        .on("GET", "/_ml/datafeeds", httpx.Response(200, json={"datafeeds": [
            {"datafeed_id": f"datafeed-{j}", "job_id": j}
            for j in ("rds_job", "k8s_job", "k8s_idle", "apm-awsorders-x")
        ]}))
        .on("POST", r"/_ml/datafeeds/[^/]+/_stop", httpx.Response(200, json={"stopped": True}))
    )
    host = _Host(["aws_rds_otel"])
    assert _run(api, lambda c: host._stop_orphaned_ootb_ml_jobs(c, ["aws_rds_otel"])) == 1
    assert [r.url.path for r in api.calls("POST", r"/_ml/datafeeds/[^/]+/_stop")] == [
        "/_ml/datafeeds/datafeed-k8s_job/_stop"
    ]


# ── package asset listing ──

def test_idle_templates_do_not_block_the_package_file_fallback():
    item = {
        "installationInfo": {"installed_kibana": [
            {"type": "alerting_rule_template", "id": "fleet-aws_rds_otel-inactivity-monitoring"},
        ]},
        "assets": {"kibana": {"alerting_rule_template": [
            {"path": "aws_rds_otel-1.0.0/kibana/alerting_rule_template/aws_rds_otel-db-connections.json"},
        ]}},
    }
    ids = IntegrationsMixin._asset_ids(
        item, "alerting_rule_template",
        keep=lambda tid: not IntegrationsMixin._is_idle_template_id(tid),
    )
    assert ids == ["aws_rds_otel-db-connections"]


# ── the whole step ──

def test_step_21_upserts_rules_slos_and_ml_jobs_for_each_package():
    info = {"item": {
        "status": "installed",
        "version": "0.12.0",
        "assets": {"kibana": {
            "alerting_rule_template": [{"path": "p/kibana/alerting_rule_template/aws_rds_otel-db-connections.json"}],
            "slo_template": [{"path": f"p/kibana/slo_template/{SLO_TEMPLATE['id']}.json"}],
            "ml_module": [{"path": "p/kibana/ml_module/aws_rds_otel-metrics-ml.json"}],
        }},
    }}
    rule = {"id": "aws_rds_otel-db-connections", "attributes": {
        "name": "[AWS RDS OTel] DB connections high", "ruleTypeId": ".es-query",
        "params": {"searchType": "esqlQuery"}, "schedule": {"interval": "1m"},
    }}
    api = (
        _Api()
        .on("GET", "/api/fleet/epm/packages/aws_rds_otel", httpx.Response(200, json=info))
        .on("GET", "/api/fleet/epm/packages/aws_rds_otel/0.12.0", httpx.Response(200, json=info))
        .on("GET", "/api/fleet/epm/packages/aws_rds_otel/0.12.0/kibana/alerting_rule_template/aws_rds_otel-db-connections.json",
            httpx.Response(200, json=rule))
        .on("GET", f"/api/fleet/epm/packages/aws_rds_otel/0.12.0/kibana/slo_template/{SLO_TEMPLATE['id']}.json",
            httpx.Response(200, json=SLO_TEMPLATE))
        .on("GET", "/api/fleet/epm/packages/aws_rds_otel/0.12.0/kibana/ml_module/aws_rds_otel-metrics-ml.json",
            httpx.Response(200, json=ML_MODULE))
        .on("POST", "/api/alerting/rule/aws_rds_otel-db-connections", httpx.Response(200, json={}))
        .on("POST", "/api/observability/slos", httpx.Response(200, json={}))
        .on("GET", "/api/observability/slos", httpx.Response(200, json={"results": [], "total": 0}))
        .on("POST", "/internal/ml/modules/setup/aws_rds_otel-metrics-ml", httpx.Response(200, json={
            "jobs": [{"id": "aws_rds_instance_resource_anomaly", "success": True}],
            "datafeeds": [{"id": "datafeed-aws_rds_instance_resource_anomaly", "success": True, "started": True}],
        }))
        .on("GET", "/api/alerting/rules/_find", httpx.Response(200, json={"data": [], "total": 0}))
        .on("GET", "/_ml/anomaly_detectors", httpx.Response(200, json={"jobs": []}))
    )
    host = _Host(["aws_rds_otel"])
    _run(api, lambda c: host._create_ootb_package_templates(c, lambda _p: None))
    assert host.step.status == "ok", host.step.detail
    assert host.step.items_total == 3 and host.step.items_done == 3
    assert host.step.detail == "1 alert rules, 1 SLOs, 1 ML jobs"


def test_ml_index_follows_the_package_datasets_when_the_module_points_elsewhere():
    from elastic_config.deployer_package_templates import ml_index_pattern, package_datasets

    datasets = package_datasets({"discovery": {"datasets": [
        {"name": "aws.elb.otel"}, {"name": "aws.elb_network.otel"},
    ]}})
    assert ml_index_pattern({"defaultIndexPattern": "metrics-aws.applicationelb.otel-*"}, datasets) == (
        "metrics-aws.elb.otel-*,metrics-aws.elb_network.otel-*"
    )
    assert ml_index_pattern({"defaultIndexPattern": "metrics-aws.elb.otel-*"}, datasets) == "metrics-aws.elb.otel-*"
    assert ml_index_pattern({"defaultIndexPattern": "metrics-x-*"}, []) == "metrics-x-*"


# ── library investigation workflows (steps 6 and 21) ──

class _Scenario:
    namespace = "awsorders"
    scenario_name = "AWS Orders Platform"
    workflow_template_inputs = {
        "service_name": "orders-gateway",
        "aws-rds-connection-exhaustion-investigation-otel": {"service_name": "orders-service"},
    }

    def get_generator_config(self, _gid):
        return {}


class _WfHost(IntegrationsMixin, PackageTemplatesMixin, WorkflowTemplatesMixin, WorkflowsMixin):
    def __init__(self, gids):
        self.kibana_url = KIBANA
        self.elastic_url = ES
        self.api_key = "key"
        self.scenario = _Scenario()
        self._kibana_env = ("traditional", "9.5.4")
        self._specs = [GENERATORS[g] for g in gids]

    def _enabled_generator_specs(self):
        return self._specs


AWS = ["aws_rds_otel", "aws_elb_metrics_otel", "aws_lambda_otel", "aws_sqs_otel"]


def test_library_workflows_are_created_enabled_or_updated_by_name():
    created = []

    def create(request):
        created.append(json.loads(request.content)["yaml"])
        return httpx.Response(200, json={"id": f"wf-{len(created)}"})

    api = (
        _Api()
        .on("GET", "/api/workflows", httpx.Response(200, json={"results": [
            {"id": "existing-rds", "name": "AWS RDS Connection Exhaustion Investigation (OTel)"},
        ]}))
        .on("POST", "/api/workflows/workflow", create)
        .on("PUT", "/api/workflows/workflow/existing-rds", httpx.Response(200, json={}))
    )
    host = _WfHost(AWS)
    assert _run(api, host._deploy_ootb_workflows) == (4, 0)
    assert len(created) == 3
    update = json.loads(api.calls("PUT", "/api/workflows/workflow/existing-rds")[0].content)
    assert update["enabled"] is True
    assert "service_name | default: 'orders-service'" in update["yaml"]
    assert all(y.startswith("tags: [ootb-workflow,") and "\nenabled: true\n" in y for y in created)
    assert host._ootb_workflow_ids["aws-rds-connection-exhaustion-investigation-otel"] == "existing-rds"


def test_rule_body_carries_the_workflow_action_only_for_matching_rules():
    host = _WfHost(AWS)
    api = (
        _Api()
        .on("GET", "/api/workflows", httpx.Response(200, json={"results": []}))
        .on("POST", "/api/workflows/workflow", lambda r: httpx.Response(200, json={"id": "wf-x"}))
    )
    _run(api, host._deploy_ootb_workflows)
    rules = json.loads((Path(__file__).parent / "fixtures" / "alert_rules.json").read_text())

    def body(rule_id):
        rule = rules[rule_id]
        return host._rule_body_from_template({
            "id": rule_id, "name": rule["name"], "rule_type_id": ".es-query",
            "params": {"searchType": "esqlQuery", "esqlQuery": {"esql": rule["esql"]}},
            "schedule": {"interval": "1m"},
        })

    connections = next(k for k, r in rules.items() if r["name"] == "[AWS RDS OTel] Database connections high")
    cpu = next(k for k, r in rules.items() if r["name"] == "[AWS RDS OTel] CPU utilization high")
    action = body(connections)["actions"]
    assert action == [{
        "group": "query matched",
        "id": "system-connector-.workflows",
        "params": {"subAction": "run", "subActionParams": {"workflowId": "wf-x", "summaryMode": False}},
    }]
    assert body(cpu)["actions"] == []


def test_no_library_workflow_without_matching_generators():
    host = _WfHost(["redis_otel"])
    api = _Api().on("GET", "/api/workflows", httpx.Response(200, json={"results": []}))
    assert _run(api, host._deploy_ootb_workflows) == (0, 0)
    assert host._ootb_rule_actions({"params": {"esqlQuery": {"esql": "FROM metrics-aws.rds.otel-*"}}}) == []


def test_a_failing_workflow_api_does_not_abort_step_6(monkeypatch):
    host = _WfHost(AWS)
    host._workflow_ids = {}
    host.progress = object()
    host.step = _Step()
    host._step = lambda _idx: host.step
    host.ns = "awsorders"
    monkeypatch.setattr(host, "_cleanup_workflows", lambda _c: 0)
    monkeypatch.setattr(host, "_ensure_workflow_indices", lambda _c: 0)
    monkeypatch.setattr(host, "_resolve_llm_connector_ids", lambda _c: ("s", "h"))
    monkeypatch.setattr(host, "_resolve_email_connector_id", lambda _c: "e")
    monkeypatch.setattr(host, "_generate_workflow_yamls", lambda **_k: {"one": "name: one"})

    def boom(_client):
        raise httpx.ConnectError("workflows API down")

    monkeypatch.setattr(host, "_wf_search", boom)
    api = _Api().on("POST", "/api/workflows/workflow", httpx.Response(200, json={"id": "wf-1"}))
    _run(api, lambda c: host._deploy_workflows(c, lambda _p: None))
    assert host.step.status == "ok"
    assert "1 library failed" in host.step.detail


# ── step 20 installs the generator's package version ──

def _install_api(installed_version: str | None, latest: str, *, pin_available: bool = True):
    item = {"name": "nginx_otel", "latestVersion": latest, "version": installed_version or latest,
            "status": "installed" if installed_version else "not_installed"}
    api = (
        _Api()
        .on("GET", "/api/fleet/epm/packages/nginx_otel", httpx.Response(200, json={"item": item}))
        .on("POST", "/api/fleet/epm/packages/nginx_otel/0.9.0",
            httpx.Response(200, json={}) if pin_available else httpx.Response(400, json={}))
        .on("POST", "/api/fleet/epm/packages/nginx_otel", httpx.Response(200, json={}))
    )
    return api


@pytest.mark.parametrize("installed", [None, "1.0.0"], ids=["fresh", "newer-installed"])
def test_step_20_installs_the_pinned_version(installed, monkeypatch):
    monkeypatch.setattr("generator_engine.registry.package_versions", lambda *_a, **_k: {"nginx_otel": "0.9.0"})
    host = _Host(["nginx_otel"])
    monkeypatch.setattr(host, "_wait_for_fleet_ready", lambda *_a: None, raising=False)
    monkeypatch.setattr(host, "_ensure_fleet_setup", lambda *_a: None, raising=False)
    monkeypatch.setattr(host, "_ensure_esql_control_data_views", lambda *_a: 0, raising=False)
    api = _install_api(installed, latest="1.0.0")
    _run(api, lambda c: host._install_integrations(c, lambda _p: None))
    assert api.calls("POST", "/api/fleet/epm/packages/nginx_otel/0.9.0")
    assert not api.calls("POST", "/api/fleet/epm/packages/nginx_otel")
    assert "nginx_otel@0.9.0" in host.step.detail and host.step.status == "ok"


def test_step_20_reports_a_pin_it_could_not_install(monkeypatch):
    monkeypatch.setattr("generator_engine.registry.package_versions", lambda *_a, **_k: {"nginx_otel": "0.9.0"})
    host = _Host(["nginx_otel"])
    monkeypatch.setattr(host, "_wait_for_fleet_ready", lambda *_a: None, raising=False)
    monkeypatch.setattr(host, "_ensure_fleet_setup", lambda *_a: None, raising=False)
    monkeypatch.setattr(host, "_ensure_esql_control_data_views", lambda *_a: 0, raising=False)
    api = _install_api(None, latest="1.0.0", pin_available=False)
    _run(api, lambda c: host._install_integrations(c, lambda _p: None))
    assert "NOT at the generator's version: nginx_otel (pinned 0.9.0 unavailable)" in host.step.detail
