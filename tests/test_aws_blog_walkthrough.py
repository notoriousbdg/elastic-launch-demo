"""The AWS scenario reproduces the blog's RDS connection-exhaustion walkthrough.

"Automated root cause analysis for AWS" injects a connection leak into an RDS
instance; the [AWS RDS OTel] Database connections high alert starts the Template Library
investigation, which finds the plateau in the RDS metrics, the refusals in the Postgres
log, the 5xx in the gateway's access log and the target 5xx on the fronting ALB.

Here the aws scenario's generators run with channel 01 (RDS Connection Leak) active,
and every data step of that workflow is evaluated against their documents with the
inputs the deployer fills in (``WorkflowTemplatesMixin._workflow_template_input_values``),
so the metric, log and load-balancer steps name the same instance and load balancer.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from elastic_config.deployer_workflow_templates import WorkflowTemplatesMixin
from elastic_config.workflow_templates import load_library, pattern_matches_datasets
from generator_engine.registry import enabled_specs
from scenario_engine import get_scenario
from tests._emit_harness import cached_run, field, index_matches
from tests._template_eval import flat_docs, number
from tests._workflow_eval import step_problems

ROOT = Path(__file__).resolve().parent.parent
SCENARIO_DIR = ROOT / "scenarios" / "aws"
RDS_LEAK = 1
WORKFLOW = "aws-rds-connection-exhaustion-investigation-otel"


class _Deployer(WorkflowTemplatesMixin):
    def __init__(self):
        self.scenario = get_scenario("aws")


@pytest.fixture(scope="module")
def workflow():
    return next(wf for wf in load_library() if wf.slug == WORKFLOW)


@pytest.fixture(scope="module")
def inputs(workflow):
    return _Deployer()._workflow_template_input_values(workflow)


def _docs(step, monkeypatch, channel=RDS_LEAK):
    specs = [s for s in enabled_specs(get_scenario("aws"))
             if any(pattern_matches_datasets(p, s.datasets) for p in step.from_patterns)]
    docs = []
    for spec in specs:
        docs += [doc for _t, index, doc in flat_docs(cached_run(spec.id, channel, monkeypatch, scenario_dir=SCENARIO_DIR))
                 if index_matches(index, list(step.from_patterns))]
    return specs, docs


def test_deployer_fills_the_blog_inputs(inputs):
    assert inputs["db_instance_identifier"] == "awsorders-orders-db"
    assert inputs["service_name"] == "orders-service"
    assert inputs["load_balancer"].startswith("app/awsorders-")


@pytest.mark.parametrize("step_name", [
    "connections_incident", "connections_baseline", "db_resource_metrics", "db_resource_baseline",
    "alb_cascade", "db_connection_refusals", "service_5xx_logs",
])
def test_every_evidence_step_finds_data_during_the_leak(step_name, workflow, inputs, monkeypatch):
    step = next(s for s in workflow.steps if s.name == step_name)
    specs, docs = _docs(step, monkeypatch)
    assert specs, f"no aws scenario generator writes {step.from_patterns}"
    rows, problems = step_problems(step.query, inputs, docs)
    assert rows and not problems, f"{step_name}: {len(rows)} rows, missing {problems}"


def test_the_leak_shows_up_as_the_blog_describes(workflow, inputs, monkeypatch):
    steps = {s.name: s for s in workflow.steps}
    _specs, metrics = _docs(steps["connections_incident"], monkeypatch)
    connections = [number(field(d, "metrics.amazonaws.com/AWS/RDS/DatabaseConnections")) for d in metrics
                   if field(d, "attributes.stat") == "Average"
                   and field(d, "attributes.MetricName") == "DatabaseConnections"
                   and field(d, "attributes.DBInstanceIdentifier") == inputs["db_instance_identifier"]]
    assert connections and min(connections) == max(connections) == 117  # plateau above the alert's 100

    _specs, rds_log = _docs(steps["db_connection_refusals"], monkeypatch)
    refusals = [d for d in rds_log if "FATAL" in str(field(d, "message"))]
    assert refusals and {field(d, "aws.cloudwatch.log_group") for d in refusals} == {
        f"/aws/rds/instance/{inputs['db_instance_identifier']}/postgresql"
    }

    _specs, access = _docs(steps["service_5xx_logs"], monkeypatch)
    gateway = [d for d in access if field(d, "aws.cloudwatch.log_group") == "/ecs/awsorders-orders-gateway"]
    errors = [d for d in gateway if '" 5' in str(field(d, "message"))]
    assert gateway and 0.25 < len(errors) / len(gateway) < 0.55


def test_the_healthy_baseline_has_no_refusals_and_a_low_connection_count(workflow, monkeypatch):
    steps = {s.name: s for s in workflow.steps}
    _specs, rds_log = _docs(steps["db_connection_refusals"], monkeypatch, channel=None)
    assert rds_log and not [d for d in rds_log if "FATAL" in str(field(d, "message"))]
    _specs, metrics = _docs(steps["connections_baseline"], monkeypatch, channel=None)
    connections = [number(field(d, "metrics.amazonaws.com/AWS/RDS/DatabaseConnections")) for d in metrics
                   if field(d, "attributes.stat") == "Average"
                   and field(d, "attributes.MetricName") == "DatabaseConnections"]
    assert connections and max(connections) <= 2
