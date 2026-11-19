"""Library investigation workflows are selected and wired from the data they read.

``elastic_config/workflow_templates.py`` deploys a vendored Template Library workflow
when every required step reads a dataset an enabled generator writes, and wires it to
the OOTB package rules that read the evidence step's data, group by its alert
dimension and read one of its metrics. These offline checks pin that behaviour for the
vendored library (``scripts/snapshot_workflow_templates.py``) and the package rule
queries (``tests/fixtures/alert_rules.json``).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from elastic_config.workflow_templates import (
    LIBRARY_DIR,
    OOTB_WORKFLOW_TAG,
    deployable_yaml,
    eligibility,
    load_library,
    rule_wires_workflow,
)
from generator_engine.registry import GENERATORS, enabled_specs
from scenario_engine import get_scenario

ROOT = Path(__file__).resolve().parent.parent
RULES = json.loads((ROOT / "tests" / "fixtures" / "alert_rules.json").read_text())
LIBRARY = load_library()
ALL_DATASETS = {ds for spec in GENERATORS.values() for ds in spec.datasets}
SELECTED = [wf for wf in LIBRARY if eligibility(wf, ALL_DATASETS, "9.5.4")[0]]


def _datasets(scenario_id: str) -> set[str]:
    return {ds for spec in enabled_specs(get_scenario(scenario_id)) for ds in spec.datasets}


def _wired(wf, datasets) -> list[str]:
    return sorted(r["name"] for r in RULES.values() if rule_wires_workflow(r["esql"], wf, datasets))


def test_library_is_vendored_with_its_source():
    source = json.loads((LIBRARY_DIR / "SOURCE.json").read_text())
    assert sorted(source["workflows"]) == sorted(wf.slug for wf in LIBRARY)
    assert len(source["commit"]) == 40


def test_every_library_workflow_has_a_verdict():
    for wf in LIBRARY:
        ok, reason = eligibility(wf, ALL_DATASETS, "9.5.4")
        assert reason, wf.slug
        if not ok:
            assert wf not in SELECTED


def test_the_aws_investigations_are_the_integration_workflows_today():
    # A new integration workflow in the library shows up here after a snapshot refresh:
    # check it is wired and has data (test_workflow_template_emit.py), then add it.
    assert sorted(wf.slug for wf in SELECTED) == [
        "aws-alb-5xx-investigation-otel",
        "aws-lambda-errors-investigation-otel",
        "aws-rds-connection-exhaustion-investigation-otel",
        "aws-sqs-consumer-lag-investigation-otel",
    ]


def test_aws_scenario_deploys_all_four_and_others_none():
    aws = _datasets("aws")
    assert sorted(wf.slug for wf in LIBRARY if eligibility(wf, aws, "9.5.4")[0]) == sorted(
        wf.slug for wf in SELECTED
    )
    space = _datasets("space")
    assert not [wf.slug for wf in LIBRARY if eligibility(wf, space, "9.5.4")[0]]


def test_generic_and_form_driven_workflows_are_not_deployed():
    by_slug = {wf.slug: wf for wf in LIBRARY}
    for slug in ("root-cause-analysis", "disk-capacity-forecast-alert"):
        ok, reason = eligibility(by_slug[slug], ALL_DATASETS, "9.5.4")
        assert not ok and "install-form" in reason


def test_older_stacks_get_no_9_5_templates():
    for wf in SELECTED:
        assert not eligibility(wf, ALL_DATASETS, "9.4.3")[0]
        assert eligibility(wf, ALL_DATASETS, None)[0]  # serverless


@pytest.mark.parametrize("wf", SELECTED, ids=lambda wf: wf.slug)
def test_selected_workflow_is_wired_to_package_rules(wf):
    assert "alert" in wf.triggers and wf.grouping and wf.evidence is not None
    assert _wired(wf, ALL_DATASETS), f"{wf.slug} starts from no OOTB rule"


def test_rds_workflow_starts_only_from_the_connections_rule():
    by_slug = {wf.slug: wf for wf in SELECTED}
    assert _wired(by_slug["aws-rds-connection-exhaustion-investigation-otel"], ALL_DATASETS) == [
        "[AWS RDS OTel] Database connections high"
    ]


def test_each_rule_starts_at_most_one_workflow():
    for rule in RULES.values():
        hits = [wf.slug for wf in SELECTED if rule_wires_workflow(rule["esql"], wf, ALL_DATASETS)]
        assert len(hits) <= 1, f"{rule['name']} would start {hits}"


@pytest.mark.parametrize("wf", SELECTED, ids=lambda wf: wf.slug)
def test_deployable_yaml_is_enabled_tagged_and_keeps_alert_derived_inputs(wf):
    values = {name: f"value-for-{name}" for name in wf.manual_inputs}
    text = deployable_yaml(wf, values)
    doc = yaml.safe_load(text)
    assert doc["enabled"] is True
    assert OOTB_WORKFLOW_TAG in doc["tags"]
    assert "template-metadata" not in doc
    manual = next(t for t in doc["triggers"] if t["type"] == "manual")
    assert {i["name"]: i.get("default") for i in manual["inputs"]} == values
    for dim in wf.grouping:
        assert f"kibana.alert.grouping.{dim}" in text, "alert-derived resource must stay"


@pytest.mark.parametrize("gid", sorted(g for g, s in GENERATORS.items() if s.id.startswith("aws_")))
def test_generator_workflow_inputs_name_real_workflow_inputs(gid):
    import importlib

    module = importlib.import_module(GENERATORS[gid].module)
    provide = getattr(module, "workflow_input_values", None)
    if provide is None:
        pytest.skip("generator provides no workflow inputs")
    names = {n for wf in SELECTED for n in wf.manual_inputs}
    values = provide({"namespace": "demo", "generator_config": {}})
    assert values and set(values) <= names
