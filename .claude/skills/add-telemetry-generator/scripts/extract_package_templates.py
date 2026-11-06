#!/usr/bin/env python3
"""Parse a package's SLO templates and ML modules into the fields a generator must emit.

Integration packages ship ``kibana/slo_template/*.json`` and ``kibana/ml_module/*.json``
next to their alerting rule templates. The demo deploys all three (step 21), so a
generator is only complete when its documents feed every SLO and ML job too.

Writes two inventories into ``--out-dir``:

``slo_required_fields.json``
    one entry per SLO template: index, KQL ``filter`` and the ``field: value`` terms it
    requires, every metric (``part``, ``aggregation``, ``field``, per-metric ``filter``),
    the equations, comparator/threshold, ``group_by`` and the flat ``fields`` list.

``ml_required_fields.json``
    one entry per ML module: ``default_index`` (and ``index_mismatch`` when it matches none
    of the package's ``discovery.datasets``), the recognizer ``query``, each datafeed's
    ``query`` and composite-source ``fields``, each job's detectors
    (``field``/``by``/``over``/``partition``), influencers, and the flat ``fields`` list.

The KQL, query-DSL and SLI helpers here are imported by the repo tests that run the
generators and evaluate these templates against the emitted documents.

Usage::

    python3 extract_package_templates.py --package-dir .cache/integrations/packages/<pkg> \\
        --out-dir /tmp/<pkg>

Exit codes: 0 ok, 2 usage / I/O error.
"""
from __future__ import annotations

import argparse
import fnmatch
import json
import math
import re
import sys
from pathlib import Path
from typing import Any, Callable, Iterable

Getter = Callable[[str], Any]

# ── KQL ──────────────────────────────────────────────────────────────────────

_KQL_TOKEN = re.compile(
    r"""\s*(?:(?P<lp>\()|(?P<rp>\))|(?P<colon>:)|(?P<op>>=|<=|>|<)"""
    r"""|(?P<str>"(?:[^"\\]|\\.)*")|(?P<word>[^\s():<>"]+))"""
)
_KEYWORDS = {"and", "or", "not"}


def _kql_tokens(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    pos = 0
    while pos < len(text):
        match = _KQL_TOKEN.match(text, pos)
        if not match or match.end() == pos:
            if text[pos:].strip():
                raise ValueError(f"unreadable KQL at {pos}: {text!r}")
            break
        pos = match.end()
        kind = match.lastgroup
        value = match.group(kind)
        if kind == "word" and value.lower() in _KEYWORDS:
            kind, value = value.lower(), value.lower()
        out.append((kind, value))
    return out


class _KqlParser:
    """Recursive-descent KQL parser for the subset package templates use.

    Nodes: ``("and", [..])``, ``("or", [..])``, ``("not", node)``, ``("all",)`` and
    ``("term", field, test)`` where ``test`` is ``("exists",)``, ``("eq", value, quoted)``,
    ``("any", [(value, quoted), ..])`` or ``("range", op, number)``.
    """

    def __init__(self, text: str):
        self.tokens = _kql_tokens(text)
        self.pos = 0

    def _peek(self) -> tuple[str, str] | None:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def _take(self, kind: str | None = None) -> tuple[str, str]:
        tok = self._peek()
        if tok is None or (kind and tok[0] != kind):
            raise ValueError(f"expected {kind or 'token'} at {self.pos}, got {tok}")
        self.pos += 1
        return tok

    def parse(self):
        if not self.tokens:
            return ("all",)
        node = self._or()
        if self._peek() is not None:
            raise ValueError(f"trailing KQL tokens: {self.tokens[self.pos:]}")
        return node

    def _or(self):
        parts = [self._and()]
        while self._peek() and self._peek()[0] == "or":
            self._take()
            parts.append(self._and())
        return parts[0] if len(parts) == 1 else ("or", parts)

    def _and(self):
        parts = [self._not()]
        while self._peek() and self._peek()[0] == "and":
            self._take()
            parts.append(self._not())
        return parts[0] if len(parts) == 1 else ("and", parts)

    def _not(self):
        if self._peek() and self._peek()[0] == "not":
            self._take()
            return ("not", self._not())
        return self._primary()

    def _primary(self):
        tok = self._peek()
        if tok and tok[0] == "lp":
            self._take("lp")
            node = self._or()
            self._take("rp")
            return node
        field = self._take("word")[1]
        nxt = self._take()
        if nxt[0] == "op":
            return ("term", field, ("range", nxt[1], float(self._take("word")[1])))
        if nxt[0] != "colon":
            raise ValueError(f"expected ':' after {field}")
        return ("term", field, self._value())

    def _value(self):
        tok = self._take()
        if tok[0] == "lp":
            values = [self._scalar(self._take())]
            while self._peek() and self._peek()[0] == "or":
                self._take()
                values.append(self._scalar(self._take()))
            self._take("rp")
            return ("any", values)
        if tok == ("word", "*"):
            return ("exists",)
        value, quoted = self._scalar(tok)
        return ("eq", value, quoted)

    @staticmethod
    def _scalar(tok: tuple[str, str]) -> tuple[str, bool]:
        if tok[0] == "str":
            return json.loads(tok[1]), True
        if tok[0] == "word":
            return tok[1], False
        raise ValueError(f"expected a value, got {tok}")


def parse_kql(text: str | None):
    return _KqlParser(text or "").parse()


def _scalar_matches(actual: Any, value: str, quoted: bool) -> bool:
    candidates = actual if isinstance(actual, list) else [actual]
    for item in candidates:
        if item is None:
            continue
        if not quoted and ("*" in value or "?" in value):
            if fnmatch.fnmatchcase(str(item), value):
                return True
            continue
        if str(item) == value:
            return True
        try:
            if float(item) == float(value):
                return True
        except (TypeError, ValueError):
            pass
    return False


def kql_matches(node, get: Getter) -> bool:
    kind = node[0]
    if kind == "all":
        return True
    if kind == "and":
        return all(kql_matches(n, get) for n in node[1])
    if kind == "or":
        return any(kql_matches(n, get) for n in node[1])
    if kind == "not":
        return not kql_matches(node[1], get)
    _, field, test = node
    actual = get(field)
    if test[0] == "exists":
        return actual is not None
    if actual is None:
        return False
    if test[0] == "eq":
        return _scalar_matches(actual, test[1], test[2])
    if test[0] == "any":
        return any(_scalar_matches(actual, v, q) for v, q in test[1])
    try:
        number = float(actual)
    except (TypeError, ValueError):
        return False
    op, bound = test[1], test[2]
    return {">": number > bound, ">=": number >= bound, "<": number < bound, "<=": number <= bound}[op]


def kql_fields(node) -> list[str]:
    out: list[str] = []

    def walk(n):
        if n[0] in ("and", "or"):
            for child in n[1]:
                walk(child)
        elif n[0] == "not":
            walk(n[1])
        elif n[0] == "term":
            out.append(n[1])

    walk(node)
    return list(dict.fromkeys(out))


def kql_required_terms(node) -> list[dict[str, Any]]:
    """Positive terms every matching document must satisfy (top-level AND only)."""
    parts = node[1] if node[0] == "and" else [node]
    terms = []
    for part in parts:
        if part[0] != "term":
            continue
        _, field, test = part
        if test[0] == "exists":
            terms.append({"field": field, "exists": True})
        elif test[0] == "eq":
            terms.append({"field": field, "value": test[1]})
        elif test[0] == "any":
            terms.append({"field": field, "any": [v for v, _q in test[1]]})
        elif test[0] == "range":
            terms.append({"field": field, "range": [test[1], test[2]]})
    return terms


# ── Elasticsearch query DSL ─────────────────────────────────────────────────

def _as_list(value) -> list:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def dsl_matches(query: dict | None, get: Getter) -> bool:
    """Evaluate the query-DSL subset ML modules use against one document."""
    if not query:
        return True
    (kind, body), = query.items()
    if kind == "match_all":
        return True
    if kind == "bool":
        must = _as_list(body.get("filter")) + _as_list(body.get("must"))
        if not all(dsl_matches(q, get) for q in must):
            return False
        if any(dsl_matches(q, get) for q in _as_list(body.get("must_not"))):
            return False
        should = _as_list(body.get("should"))
        if should:
            minimum = body.get("minimum_should_match", 0 if must else 1)
            if sum(dsl_matches(q, get) for q in should) < int(minimum):
                return False
        return True
    if kind == "exists":
        return get(body["field"]) is not None
    (field, spec), = body.items()
    actual = get(field)
    if kind == "term":
        value = spec.get("value") if isinstance(spec, dict) else spec
        return actual is not None and _scalar_matches(actual, str(value), True)
    if kind == "terms":
        return actual is not None and any(_scalar_matches(actual, str(v), True) for v in spec)
    if kind in ("match", "match_phrase"):
        value = spec.get("query") if isinstance(spec, dict) else spec
        return actual is not None and str(value) in str(actual)
    if kind == "range":
        if actual is None:
            return False
        try:
            number = float(actual)
        except (TypeError, ValueError):
            return False
        checks = {"gt": number.__gt__, "gte": number.__ge__, "lt": number.__lt__, "lte": number.__le__}
        return all(checks[op](float(v)) for op, v in spec.items() if op in checks)
    raise ValueError(f"unsupported query clause {kind!r}")


def dsl_fields(query: dict | None) -> list[str]:
    out: list[str] = []

    def walk(q):
        if not isinstance(q, dict):
            return
        for kind, body in q.items():
            if kind == "bool":
                for key in ("filter", "must", "must_not", "should"):
                    for child in _as_list(body.get(key)):
                        walk(child)
            elif kind == "exists":
                out.append(body["field"])
            elif isinstance(body, dict):
                out.extend(body.keys())

    walk(query)
    return [f for f in dict.fromkeys(out) if not f.startswith("_")]


# ── SLO templates ───────────────────────────────────────────────────────────

def _group_by(value) -> list[str]:
    return [g for g in _as_list(value) if g and g != "*"]


def slo_inventory(doc: dict[str, Any]) -> dict[str, Any] | None:
    attrs = doc.get("attributes") or doc
    indicator = attrs.get("indicator") or {}
    params = indicator.get("params") or {}
    kind = indicator.get("type", "")
    metrics: list[dict[str, Any]] = []
    equations: dict[str, str] = {}
    entry: dict[str, Any] = {}

    if kind in ("sli.metric.timeslice", "sli.metric.custom"):
        parts = ("metric",) if kind == "sli.metric.timeslice" else ("good", "total")
        for part in parts:
            block = params.get(part) or {}
            equations[part] = block.get("equation", "")
            for metric in block.get("metrics") or []:
                metrics.append({
                    "part": part,
                    "name": metric.get("name"),
                    "aggregation": metric.get("aggregation"),
                    "field": metric.get("field"),
                    "filter": metric.get("filter") or "",
                    **({"percentile": metric["percentile"]} if "percentile" in metric else {}),
                })
            if part == "metric":
                entry["comparator"] = block.get("comparator")
                entry["threshold"] = block.get("threshold")
    elif kind == "sli.histogram.custom":
        for part in ("good", "total"):
            block = params.get(part) or {}
            metrics.append({
                "part": part,
                "name": part,
                "aggregation": block.get("aggregation"),
                "field": block.get("field"),
                "filter": block.get("filter") or "",
                **{k: block[k] for k in ("from", "to") if k in block},
            })
    elif kind == "sli.kql.custom":
        for part in ("good", "total"):
            metrics.append({"part": part, "name": part, "aggregation": "doc_count",
                            "field": None, "filter": params.get(part) or ""})
    else:
        return None

    query = params.get("filter") or ""
    node = parse_kql(query)
    group_by = _group_by(attrs.get("groupBy"))
    fields = kql_fields(node) + group_by
    for metric in metrics:
        if metric["field"]:
            fields.append(metric["field"])
        fields.extend(kql_fields(parse_kql(metric["filter"])))
    return {
        "id": doc.get("id") or attrs.get("id"),
        "name": attrs.get("name"),
        "indicator": kind,
        "index": params.get("index") or "",
        "filter": query,
        "required_terms": kql_required_terms(node),
        "metrics": metrics,
        "equations": equations,
        "budgeting": attrs.get("budgetingMethod"),
        "objective": attrs.get("objective") or {},
        "group_by": group_by,
        "fields": [f for f in dict.fromkeys(fields) if f != "@timestamp"],
        **entry,
    }


# Equation evaluation: letters are metric names, everything else arithmetic.
_EQUATION = re.compile(r"^[A-Z0-9_+\-*/(). ]+$")


def evaluate_equation(equation: str, values: dict[str, float | None]) -> float | None:
    """Evaluate an SLO equation (``(A-B)/(C-D)``); None when undefined (missing / ÷0)."""
    if not equation or not _EQUATION.match(equation):
        return None
    names = set(re.findall(r"\b[A-Z]\b", equation))
    if any(values.get(n) is None for n in names):
        return None
    expr = re.sub(r"\b([A-Z])\b", lambda m: repr(float(values[m.group(1)])), equation)
    try:
        result = eval(expr, {"__builtins__": {}}, {})  # noqa: S307 - arithmetic only (regex-checked)
    except ZeroDivisionError:
        return None
    return result if isinstance(result, (int, float)) and math.isfinite(result) else None


def aggregate(aggregation: str, values: list[float], *, percentile: float | None = None) -> float | None:
    """One SLO metric aggregation over a slice's values (in emit order)."""
    if aggregation == "doc_count":
        return float(len(values))
    if not values:
        return None
    if aggregation in ("sum", "value_count"):
        return float(sum(values)) if aggregation == "sum" else float(len(values))
    if aggregation == "avg":
        return sum(values) / len(values)
    if aggregation == "max":
        return max(values)
    if aggregation == "min":
        return min(values)
    if aggregation == "last_value":
        return values[-1]
    if aggregation == "percentile":
        ordered = sorted(values)
        rank = max(0, math.ceil((percentile or 50) / 100 * len(ordered)) - 1)
        return ordered[rank]
    return None


def compare(value: float, comparator: str, threshold: float) -> bool:
    return {"LT": value < threshold, "LTE": value <= threshold,
            "GT": value > threshold, "GTE": value >= threshold}[comparator]


# ── ML modules ──────────────────────────────────────────────────────────────

def index_matches_datasets(index: str, datasets: Iterable[str]) -> bool:
    datasets = list(datasets)
    if not datasets:
        return True
    return any(
        fnmatch.fnmatchcase(f"{kind}-{ds}-default", index)
        for ds in datasets
        for kind in ("metrics", "logs", "traces")
    )


def _composite_fields(aggregations: dict | None) -> list[str]:
    """Fields a datafeed's aggregation groups by (composite sources, terms buckets).

    A document missing one of these never lands in a bucket. Metric sub-aggregations
    (``avg`` …) are not listed: each document only needs the metrics it carries, and
    the detectors name those.
    """
    out: list[str] = []

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "composite":
                    for source in value.get("sources") or []:
                        for spec in source.values():
                            for agg in spec.values():
                                if isinstance(agg, dict) and agg.get("field"):
                                    out.append(agg["field"])
                    walk(value.get("aggregations") or value.get("aggs"))
                elif key == "terms" and isinstance(value, dict):
                    if value.get("field"):
                        out.append(value["field"])
                else:
                    walk(value)

    walk(aggregations)
    return [f for f in dict.fromkeys(out) if f != "@timestamp"]


def ml_inventory(doc: dict[str, Any], discovery_datasets: list[str] | None = None) -> dict[str, Any]:
    attrs = doc.get("attributes") or doc
    default_index = attrs.get("defaultIndexPattern") or ""
    datafeeds = []
    for feed in attrs.get("datafeeds") or []:
        config = feed.get("config") or {}
        datafeeds.append({
            "id": feed.get("id"),
            "job_id": feed.get("job_id") or config.get("job_id"),
            "query": config.get("query"),
            "fields": list(dict.fromkeys(dsl_fields(config.get("query"))
                                         + _composite_fields(config.get("aggregations")))),
            "aggregated": bool(config.get("aggregations")),
        })
    jobs = []
    fields = dsl_fields(attrs.get("query"))
    for job in attrs.get("jobs") or []:
        analysis = (job.get("config") or {}).get("analysis_config") or {}
        detectors = [{
            "function": d.get("function"),
            "field": d.get("field_name"),
            "by": d.get("by_field_name"),
            "over": d.get("over_field_name"),
            "partition": d.get("partition_field_name"),
        } for d in analysis.get("detectors") or []]
        jobs.append({
            "id": job.get("id"),
            "bucket_span": analysis.get("bucket_span"),
            "summary_count_field_name": analysis.get("summary_count_field_name"),
            "detectors": detectors,
            "influencers": analysis.get("influencers") or [],
        })
        for d in detectors:
            fields.extend(v for k, v in d.items() if k != "function" and v)
        fields.extend(analysis.get("influencers") or [])
    for feed in datafeeds:
        fields.extend(feed["fields"])
    return {
        "id": attrs.get("id") or doc.get("id"),
        "default_index": default_index,
        "index_mismatch": not index_matches_datasets(default_index, discovery_datasets or []),
        "discovery_datasets": list(discovery_datasets or []),
        "query": attrs.get("query"),
        "datafeeds": datafeeds,
        "jobs": jobs,
        "fields": [f for f in dict.fromkeys(fields) if f and not f.startswith("_")],
    }


# ── package reading ─────────────────────────────────────────────────────────

def discovery_datasets(manifest_text: str) -> list[str]:
    """``discovery.datasets[].name`` from a package manifest.yml (no YAML dependency)."""
    block = re.search(r"^discovery:\s*\n((?:[ \t]+.*\n?)*)", manifest_text, re.M)
    if not block:
        return []
    datasets = re.search(r"datasets:\s*\n((?:[ \t]+-?.*\n?)*)", block.group(1))
    return re.findall(r"name:\s*['\"]?([\w.\-]+)", datasets.group(1)) if datasets else []


def package_inventories(
    files: dict[str, bytes], manifest_text: str = "",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """``(slo inventories, ml inventories)`` from ``{path: bytes}`` of a package."""
    datasets = discovery_datasets(manifest_text)
    slos, mls = [], []
    for name in sorted(files):
        if not name.endswith(".json"):
            continue
        if "/slo_template/" in name:
            entry = slo_inventory(json.loads(files[name]))
            if entry:
                slos.append(entry)
        elif "/ml_module/" in name:
            mls.append(ml_inventory(json.loads(files[name]), datasets))
    return slos, mls


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--package-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    args = ap.parse_args(argv)
    if not args.package_dir.is_dir():
        print(f"error: package dir not found: {args.package_dir}", file=sys.stderr)
        return 2
    files = {
        str(p.relative_to(args.package_dir)): p.read_bytes()
        for p in args.package_dir.rglob("*.json")
        if "/kibana/" in f"/{p.relative_to(args.package_dir)}"
    }
    manifest = args.package_dir / "manifest.yml"
    slos, mls = package_inventories(files, manifest.read_text() if manifest.is_file() else "")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "slo_required_fields.json").write_text(json.dumps(slos, indent=1) + "\n")
    (args.out_dir / "ml_required_fields.json").write_text(json.dumps(mls, indent=1) + "\n")
    print(f"slo_templates={len(slos)} ml_modules={len(mls)} -> {args.out_dir}")
    for ml in mls:
        if ml["index_mismatch"]:
            print(f"warn: ML module {ml['id']} reads {ml['default_index']}, which matches none of "
                  f"the package datasets {ml['discovery_datasets']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
