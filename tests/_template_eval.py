"""Evaluate package SLO templates and ML modules against a generator's documents.

The KQL, query-DSL and SLI arithmetic come from the add-telemetry-generator skill's
``extract_package_templates.py`` (one implementation for the skill gate and these
tests). Documents come from ``tests._emit_harness`` and are viewed the way
Elasticsearch would see them (``data_stream.*`` from the index, passthrough field
resolution).
"""
from __future__ import annotations

import importlib.util
import json

import pytest
from pathlib import Path
from typing import Any

from tests._emit_harness import es_view, field, index_matches

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / ".claude" / "skills" / "add-telemetry-generator" / "scripts"
FIXTURES = ROOT / "tests" / "fixtures"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


X = _load("extract_package_templates")


def fixture(name: str) -> dict:
    path = FIXTURES / name
    return json.loads(path.read_text()) if path.exists() else {}


SKIPS = fixture("template_emit_skips.json")
# Known generator gaps still to be fixed: strict xfail, so a fix forces the entry out.
PENDING = fixture("template_emit_pending.json")


def mark_pending(request) -> None:
    """Strict-xfail this test when ``template_emit_pending.json`` lists its node id."""
    name = request.node.name
    reason = PENDING.get(name)
    if reason:
        request.applymarker(pytest.mark.xfail(strict=True, reason=reason))


def skip_reason(kind: str, template_id: str, gid: str) -> str | None:
    """A recorded, reasoned exemption for ``(kind, template, generator)``, if any."""
    entry = (SKIPS.get(kind) or {}).get(template_id)
    if not entry:
        return None
    gens = entry.get("generators")
    return entry["reason"] if not gens or gid in gens else None


def getter(doc: dict):
    return lambda name: field(doc, name)


def number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def flat_docs(per_tick: list[list[tuple[str, dict]]]) -> list[tuple[int, str, dict]]:
    return [(tick, index, es_view(index, doc)) for tick, docs in enumerate(per_tick) for index, doc in docs]


# ── SLO templates ───────────────────────────────────────────────────────────

def slo_documents(slo: dict, docs: list[tuple[int, str, dict]]) -> list[tuple[int, dict]]:
    node = X.parse_kql(slo["filter"])
    return [(tick, doc) for tick, index, doc in docs
            if index_matches(index, slo["index"] or "*") and X.kql_matches(node, getter(doc))]


def _metric_value(metric: dict, doc: dict) -> Any:
    return field(doc, metric["field"]) if metric.get("field") else None


def metric_documents(metric: dict, docs: list[tuple[int, dict]]) -> list[tuple[int, dict]]:
    """Documents that feed one SLO metric (its own filter, and its field present)."""
    node = X.parse_kql(metric.get("filter") or "")
    out = []
    for tick, doc in docs:
        if not X.kql_matches(node, getter(doc)):
            continue
        if metric.get("field") is None:
            out.append((tick, doc))
            continue
        value = _metric_value(metric, doc)
        if isinstance(value, dict) and "counts" in value:
            out.append((tick, doc))
        elif number(value) is not None:
            out.append((tick, doc))
    return out


def instance_key(slo: dict, doc: dict) -> tuple:
    return tuple(field(doc, g) for g in slo["group_by"])


def _part_value(slo: dict, part: str, docs: list[tuple[int, dict]]) -> float | None:
    metrics = [m for m in slo["metrics"] if m["part"] == part]
    if slo["indicator"] == "sli.histogram.custom":
        metric = metrics[0]
        total = good = 0
        for _tick, doc in metric_documents(metric, docs):
            hist = _metric_value(metric, doc)
            if not isinstance(hist, dict):
                continue
            for value, count in zip(hist["values"], hist["counts"]):
                total += count
                low, high = metric.get("from", float("-inf")), metric.get("to", float("inf"))
                if low <= value < high:
                    good += count
        if metric["aggregation"] == "range":
            return float(good)
        return float(total)
    values = {}
    for metric in metrics:
        series = [number(_metric_value(metric, d)) for _t, d in metric_documents(metric, docs)]
        series = [v for v in series if v is not None] if metric.get("field") else series
        values[metric["name"]] = X.aggregate(metric["aggregation"], series,
                                             percentile=metric.get("percentile"))
    equation = slo["equations"].get(part, "")
    return X.evaluate_equation(equation, values) if len(metrics) else None


def slo_health(slo: dict, docs: list[tuple[int, dict]]) -> tuple[int, int, list[str]]:
    """``(good, determined, notes)`` for the healthy baseline.

    Timeslice SLOs: one slice per tick and instance (falls back to one whole-window
    slice when every per-tick slice is undefined, e.g. a counter delta from a single
    sample). Occurrence SLOs: good/total over the whole window per instance.
    """
    by_instance: dict[tuple, list[tuple[int, dict]]] = {}
    for tick, doc in docs:
        key = instance_key(slo, doc)
        if None in key:  # Kibana drops documents missing a groupBy field
            continue
        by_instance.setdefault(key, []).append((tick, doc))
    good = determined = 0
    notes: list[str] = []
    for instance, rows in sorted(by_instance.items(), key=str):
        if slo["budgeting"] == "timeslices" and slo["indicator"] == "sli.metric.timeslice":
            ticks = sorted({t for t, _d in rows})
            windows = [[r for r in rows if r[0] == t] for t in ticks]
            results = [_part_value(slo, "metric", w) for w in windows]
            if all(r is None for r in results):
                results = [_part_value(slo, "metric", rows)]
            for value in results:
                if value is None:
                    continue
                determined += 1
                ok = X.compare(value, slo["comparator"], float(slo["threshold"]))
                good += ok
                if not ok:
                    notes.append(f"{instance}: {value:g} not {slo['comparator']} {slo['threshold']}")
        else:
            g, t = _part_value(slo, "good", rows), _part_value(slo, "total", rows)
            if g is None or t is None or t <= 0:
                continue
            determined += 1
            ratio = g / t
            ok = ratio >= float(slo["objective"].get("target", 0))
            good += ok
            if not ok:
                notes.append(f"{instance}: good/total {ratio:.4f} < {slo['objective'].get('target')}")
    return good, determined, notes


# ── ML modules ──────────────────────────────────────────────────────────────

def ml_documents(index_patterns: str, query: dict | None, docs) -> list[dict]:
    return [doc for _tick, index, doc in docs
            if index_matches(index, index_patterns) and X.dsl_matches(query, getter(doc))]


def present(doc: dict, name: str | None) -> bool:
    return name is None or field(doc, name) is not None
