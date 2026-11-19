"""Every package SLO template gets data from the generator that installs its package.

Step 21 creates an SLO from each ``slo_template`` of every installed package. For each
(generator, template) pair in ``tests/fixtures/slo_templates.json``
(``scripts/snapshot_package_templates.py``), run the generator on its healthy
baseline and check, against the documents Elasticsearch would index:

1. some document is in the SLO ``index`` and passes its KQL ``filter``;
2. every metric the SLI reads has documents (per-metric filter + numeric field);
3. some of those documents carry every ``groupBy`` field, so each instance gets a slice;
4. the healthy baseline meets the objective, so a fresh deploy burns no budget.

A pair that cannot be met by a generator (an upstream template bug) needs an entry
with a reason in ``tests/fixtures/template_emit_skips.json``.
"""
from __future__ import annotations

import pytest

from tests._emit_harness import cached_run, field
from tests._template_eval import (
    fixture,
    flat_docs,
    mark_pending,
    metric_documents,
    skip_reason,
    slo_documents,
    slo_health,
)

SLOS = fixture("slo_templates.json")
CASES = sorted((gid, tid) for tid, entry in SLOS.items() for gid in entry["generators"])


def _docs(gid: str, slo: dict, monkeypatch):
    return slo_documents(slo, flat_docs(cached_run(gid, None, monkeypatch)))


def _skip(gid: str, tid: str) -> None:
    reason = skip_reason("slo", tid, gid)
    if reason:
        pytest.skip(reason)


@pytest.mark.parametrize("gid,tid", CASES, ids=[f"{g}:{t}" for g, t in CASES])
def test_slo_template_filter_matches_generator_documents(gid, tid, monkeypatch, request):
    mark_pending(request)
    _skip(gid, tid)
    slo = SLOS[tid]
    docs = _docs(gid, slo, monkeypatch)
    assert docs, (
        f"{gid}: no document in {slo['index']!r} passes the {slo['name']!r} filter "
        f"{slo['filter']!r}; emit {[t for t in slo['required_terms']]}"
    )


@pytest.mark.parametrize("gid,tid", CASES, ids=[f"{g}:{t}" for g, t in CASES])
def test_slo_template_metrics_and_groups_are_emitted(gid, tid, monkeypatch, request):
    mark_pending(request)
    _skip(gid, tid)
    slo = SLOS[tid]
    docs = _docs(gid, slo, monkeypatch)
    if not docs:
        pytest.skip("covered by test_slo_template_filter_matches_generator_documents")
    problems = []
    for metric in slo["metrics"]:
        feeding = metric_documents(metric, docs)
        if not feeding:
            problems.append(
                f"metric {metric['part']}.{metric['name']} ({metric['aggregation']} of "
                f"{metric['field']!r}, filter {metric['filter']!r}) has no document"
            )
            continue
        # Kibana groups SLI data by the groupBy fields and ignores documents that
        # lack one (e.g. load-balancer-level copies of a per-target-group metric), so
        # some of the feeding documents must carry all of them.
        if slo["group_by"] and not any(
            all(field(doc, group) is not None for group in slo["group_by"]) for _tick, doc in feeding
        ):
            problems.append(
                f"no document feeding {metric['field']!r} carries all groupBy fields {slo['group_by']}"
            )
    assert not problems, f"{gid} / {slo['name']}: " + "; ".join(dict.fromkeys(problems))


@pytest.mark.parametrize("gid,tid", CASES, ids=[f"{g}:{t}" for g, t in CASES])
def test_slo_template_is_healthy_on_the_baseline(gid, tid, monkeypatch, request):
    mark_pending(request)
    _skip(gid, tid)
    slo = SLOS[tid]
    docs = _docs(gid, slo, monkeypatch)
    good, determined, notes = slo_health(slo, docs)
    if not determined:
        pytest.skip("no determinable SLI slice (presence is checked by the other tests)")
    target = float(slo["objective"].get("timesliceTarget") or slo["objective"].get("target") or 0)
    assert good / determined >= target, (
        f"{gid} / {slo['name']}: healthy baseline is good in {good}/{determined} slices "
        f"(< {target}); e.g. {notes[:3]}"
    )
