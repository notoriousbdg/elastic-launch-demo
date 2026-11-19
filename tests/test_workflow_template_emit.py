"""Library investigation workflows find their evidence in the generators' documents.

For each library workflow the deployer selects (``elastic_config/workflow_templates.py``)
and each of its ES|QL steps that reads a dataset some generator writes, run that
generator and evaluate the step against the documents Elasticsearch would index:

* ``{{ steps.resolve.output.<input> }}`` becomes the generator's own value for that
  input (``workflow_input_values``) — the value an alert on its data would carry;
* time-window terms are dropped (the documents are all "now");
* every remaining ``WHERE`` term must be **true** on some document — a missing field
  fails, so ``attributes.DBInstanceIdentifier`` must be a data-point attribute. A step
  that looks for fault evidence (``message LIKE "*FATAL*"``) is checked with each of
  the generator's gt fault channels active when the healthy baseline has none;
* every ``STATS`` aggregate with a field (``MAX(metric) WHERE stat == "Maximum"``) must
  have a document with that field numeric under its own condition.

Required steps (no ``on-failure: continue``) gate the workflow's verdict, so they must
pass. Optional steps degrade to "not available" in the report; a generator gap there is
tracked in ``tests/fixtures/template_emit_pending.json`` like other template gaps.
"""
from __future__ import annotations

import importlib

import pytest

from elastic_config.workflow_templates import _data_patterns, eligibility, load_library, pattern_matches_datasets
from generator_engine.registry import GENERATORS
from scenario_engine.yaml_scenario import load_yaml_scenario
from tests._emit_harness import cached_run, field, gt_channels, gt_scenario, index_matches
from tests._template_eval import flat_docs, mark_pending, skip_reason
from tests._workflow_eval import step_problems

ALL_DATASETS = {ds for spec in GENERATORS.values() for ds in spec.datasets}
SELECTED = [wf for wf in load_library() if eligibility(wf, ALL_DATASETS, "9.5.4")[0]]


def _integration_patterns(patterns) -> list[str]:
    """Patterns naming an integration's dataset (``metrics-aws.rds.otel-*``).

    A pattern whose dataset starts with a wildcard (``logs-*.otel-*``, the ALB
    workflow's generic application-log step) reads whatever the environment logs
    and belongs to no one generator, so it is not a per-generator case.
    """
    out = []
    for pattern in _data_patterns(patterns):
        dataset = pattern.split("-", 1)[1].rsplit("-", 1)[0] if pattern.count("-") >= 2 else ""
        if dataset and not dataset.startswith("*"):
            out.append(pattern)
    return out


def _cases():
    out = []
    for wf in SELECTED:
        for step in wf.steps:
            patterns = _integration_patterns(step.from_patterns)
            if not patterns:
                continue
            for gid, spec in sorted(GENERATORS.items()):
                if any(pattern_matches_datasets(p, spec.datasets) for p in patterns):
                    out.append((wf, step, gid))
    return out


CASES = _cases()


def _input_values(gid: str) -> dict[str, str]:
    """Every generator's ``workflow_input_values`` under ``gid``'s gt scenario identity."""
    scenario = load_yaml_scenario(gt_scenario(gid))
    values: dict[str, str] = {}
    for spec in GENERATORS.values():
        provide = getattr(importlib.import_module(spec.module), "workflow_input_values", None)
        if callable(provide):
            values.update(provide({"namespace": scenario.namespace,
                                   "generator_config": scenario.get_generator_config(spec.id)}))
    return values


@pytest.mark.parametrize(
    "wf,step,gid", CASES, ids=[f"{wf.slug}.{step.name}:{gid}" for wf, step, gid in CASES]
)
def test_workflow_step_finds_evidence_in_generator_documents(wf, step, gid, monkeypatch, request):
    mark_pending(request)
    reason = skip_reason("workflow", f"{wf.slug}.{step.name}", gid)
    if reason:
        pytest.skip(reason)
    values = _input_values(gid)
    kind = "required" if step.required else "optional"
    faults = sorted({channel for (g, _key), (_dir, channel) in gt_channels().items() if g == gid})
    docs: list[dict] = []
    rows: list[dict] = []
    problems: list[str] = []
    for channel in [None, *faults]:
        docs = [doc for _t, index, doc in flat_docs(cached_run(gid, channel, monkeypatch))
                if index_matches(index, list(step.from_patterns))]
        try:
            rows, problems = step_problems(step.query, values, docs)
        except LookupError as missing:
            pytest.fail(f"{wf.slug}.{step.name}: no generator's workflow_input_values provides {missing}, "
                        "so this step cannot be checked (or filled for alert runs)")
        if rows:
            break
    assert docs, f"{wf.slug}.{step.name} ({kind}): {gid} writes nothing to {step.from_patterns}"
    assert rows, f"{wf.slug}.{step.name} ({kind}): no {gid} document satisfies its WHERE, healthy or faulted"
    assert not problems, f"{wf.slug}.{step.name} ({kind}): {gid} has no data for {problems}"
