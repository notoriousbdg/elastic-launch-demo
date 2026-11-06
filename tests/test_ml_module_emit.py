"""Every package ML module gets data from the generator that installs its package.

Step 21 sets up each ``ml_module`` of every installed package. For each
(generator, module) pair in ``tests/fixtures/ml_modules.json``
(``scripts/snapshot_package_templates.py``), run the generator on its healthy
baseline and check, against the documents Elasticsearch would index:

1. documents in the index the deployer points the datafeeds at
   (``ml_index_pattern``) match the module's recognizer ``query``;
2. each datafeed's ``query`` matches documents, and those carry every field its
   composite aggregation groups by (a missing source field drops the document);
3. each detector's ``field`` is numeric on some datafeed document that also carries
   its ``by`` / ``over`` / ``partition`` fields, and every influencer is emitted.

A pair that cannot be met by a generator needs an entry with a reason in
``tests/fixtures/template_emit_skips.json``.
"""
from __future__ import annotations

import pytest

from elastic_config.deployer_package_templates import ml_index_pattern
from tests._emit_harness import cached_run
from tests._template_eval import (
    field,
    fixture,
    flat_docs,
    mark_pending,
    ml_documents,
    number,
    present,
    skip_reason,
)

MODULES = fixture("ml_modules.json")
CASES = sorted((gid, mid) for mid, entry in MODULES.items() for gid in entry["generators"])


def _index(module: dict) -> str:
    return ml_index_pattern({"defaultIndexPattern": module["default_index"]}, module["discovery_datasets"])


def _skip(gid: str, mid: str) -> None:
    reason = skip_reason("ml", mid, gid)
    if reason:
        pytest.skip(reason)


def test_index_mismatches_are_redirected_to_the_package_datasets():
    for module in MODULES.values():
        if module["index_mismatch"]:
            assert _index(module) != module["default_index"]
            assert all(ds in _index(module) for ds in module["discovery_datasets"])


@pytest.mark.parametrize("gid,mid", CASES, ids=[f"{g}:{m}" for g, m in CASES])
def test_ml_module_is_recognized_on_generator_documents(gid, mid, monkeypatch, request):
    mark_pending(request)
    _skip(gid, mid)
    module = MODULES[mid]
    docs = flat_docs(cached_run(gid, None, monkeypatch))
    matched = ml_documents(_index(module), module["query"], docs)
    assert matched, f"{gid}: no document in {_index(module)} matches module {mid} query {module['query']}"


@pytest.mark.parametrize("gid,mid", CASES, ids=[f"{g}:{m}" for g, m in CASES])
def test_ml_datafeeds_and_detectors_have_data(gid, mid, monkeypatch, request):
    mark_pending(request)
    _skip(gid, mid)
    module = MODULES[mid]
    docs = flat_docs(cached_run(gid, None, monkeypatch))
    jobs = {job["id"]: job for job in module["jobs"]}
    problems = []
    for feed in module["datafeeds"]:
        feed_docs = ml_documents(_index(module), feed["query"], docs)
        if not feed_docs:
            problems.append(f"datafeed {feed['id']}: no document matches {feed['query']}")
            continue
        if feed["aggregated"]:
            for name in feed["fields"]:
                if any(not present(doc, name) for doc in feed_docs):
                    problems.append(f"datafeed {feed['id']}: some documents lack aggregation field {name!r}")
        job = jobs.get(feed["job_id"]) or {}
        for det in job.get("detectors") or []:
            groups = [det["by"], det["over"], det["partition"]]
            fed = [
                doc for doc in feed_docs
                if (det["field"] is None or number(field(doc, det["field"])) is not None)
                and all(present(doc, g) for g in groups)
            ]
            if not fed:
                problems.append(
                    f"job {job['id']}: detector {det['function']}({det['field']}) "
                    f"by {det['by']} partition {det['partition']} has no document"
                )
        for name in job.get("influencers") or []:
            if not any(present(doc, name) for doc in feed_docs):
                problems.append(f"job {job.get('id')}: influencer {name!r} never emitted")
    assert not problems, f"{gid} / {mid}: " + "; ".join(dict.fromkeys(problems))
