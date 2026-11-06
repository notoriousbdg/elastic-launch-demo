#!/usr/bin/env python3
"""Audit package ES|QL that needs two metrics in one document.

Elasticsearch's OTLP intake stores metric points in one document only when the
resource, scope, data-point attributes, ``timeUnixNano``, ``startTimeUnixNano``,
unit and metric type are all equal. A query that reads two metrics from one row
before its first ``STATS`` returns nothing unless both land in one document.

For every generator that cites a package (``SOURCE_INTEGRATION``), download the
latest package zip from the public Elastic Package Registry (in memory), pull every
ES|QL string out of its dashboards, alert rule templates and SLO templates
(JSON-in-JSON decoded recursively, Vega specs and ``esql_query`` panels included)
and flag queries that, before the first ``STATS``:

  * WHERE           AND two or more metrics in one condition (``a > 0 AND b > 0``,
                    ``a > b``); ``(a IS NOT NULL OR b IS NOT NULL)`` is fine
  * EVAL            compute one value from two or more metrics (a COALESCE of
                    metrics is fine, one metric under CASE/COALESCE is fine)
  * KEEP            keep two or more metrics
  * WHERE->STATS    filter on metric A, then aggregate metric B (implicit co-location)

``logs-*`` queries, attribute fields and math after ``STATS`` are not flagged.

Each flagged pair is then checked against one real generator cycle (the harness
from ``tests/test_generator_contract.py``, two loops, a wall clock that moves on
every read, and the generator's own ``run_tick_loop`` scrape clock). Emitted points
are grouped the way the intake groups them, and the pair is reported as:

  * MERGED          both metrics share a document (same resource, scope, data-point
                    attributes, timeUnixNano, startTimeUnixNano, unit and type)
  * BULK            not merged, but a companion bulk document (``send_bulk``,
                    ``*_bulk_doc``, ``build_*_doc``) names both metrics
  * NOT_EMITTED     the generator does not emit one of the metrics at all
                    (``audit_package_coverage.py`` reports those)
  * UNCOVERED       emitted, never in one document; the reason lists what differs

Needs network (EPR), like ``audit_package_coverage.py``.

Usage::

    python3 scripts/audit_colocation.py                    # all generators
    python3 scripts/audit_colocation.py kubernetes_otel    # one or more
    python3 scripts/audit_colocation.py --json             # machine-readable

Exit status is 1 when any pair is UNCOVERED.
"""
from __future__ import annotations

import argparse
import inspect
import itertools
import json
import logging
import re
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from audit_package_coverage import (  # noqa: E402
    FIELD,
    PACKAGE_VERSION,
    SOURCE_CITATION,
    generator_ids,
    latest_version,
    package_zip,
)

KIBANA_DIRS = ("/kibana/dashboard/", "/kibana/alerting_rule_template/", "/kibana/slo")
QUERY_START = re.compile(r"^\s*(?:FROM|TS)\s+\S", re.I)
EMBEDDED = re.compile(r"""(?:'''|"|')\s*((?:FROM|TS)\s+[^"']*?)\s*(?:'''|"|')""")
TOKEN = re.compile(FIELD)
KEYWORDS = {"and", "or", "not", "is", "null", "in", "like", "rlike", "by", "as", "true",
            "false", "asc", "desc", "nulls", "first", "last", "metadata", "on", "with"}
BULK_MARK = re.compile(r"send_bulk|_bulk_doc|build_\w+_doc")
PAIR_KINDS = ("WHERE", "EVAL", "KEEP", "WHERE->STATS")


# ── ES|QL extraction ──────────────────────────────────────────────────────────

def _decode(node, out: list[str]) -> None:
    """Collect every string, decoding JSON-in-JSON (visState, Vega spec) on the way."""
    if isinstance(node, dict):
        for value in node.values():
            _decode(value, out)
    elif isinstance(node, list):
        for value in node:
            _decode(value, out)
    elif isinstance(node, str):
        text = node.strip()
        if text[:1] in "{[":
            try:
                _decode(json.loads(text), out)
                return
            except ValueError:
                pass  # HJSON Vega spec: fall back to the embedded-query scan
        out.append(node)


def esql_queries(zf: zipfile.ZipFile) -> list[tuple[str, str]]:
    """Return (package path, ES|QL query) for dashboards, alert and SLO templates."""
    found: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for name in zf.namelist():
        if not name.endswith(".json") or not any(d in name for d in KIBANA_DIRS):
            continue
        texts: list[str] = []
        _decode(json.loads(zf.read(name)), texts)
        for text in texts:
            queries = [text] if QUERY_START.match(text) else EMBEDDED.findall(text)
            for query in queries:
                if "|" not in query:
                    continue
                short = name.split("/kibana/", 1)[-1]
                if (short, query) not in seen:
                    seen.add((short, query))
                    found.append((short, query))
    return found


# ── ES|QL analysis ────────────────────────────────────────────────────────────

def _strip_comments(query: str) -> str:
    return re.sub(r"(?m)//[^\n\"]*$", "", query)


def _split_top(text: str, sep: str) -> list[str]:
    """Split on ``sep`` outside quotes and parentheses (``sep`` is a char or a word)."""
    parts, depth, quote, start, i = [], 0, "", 0, 0
    word = sep.isalpha()
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == quote:
                quote = ""
        elif ch in "\"'":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0:
            if word:
                if (text[i:i + len(sep)].upper() == sep
                        and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] in "_.`"))
                        and (i + len(sep) == len(text)
                             or not (text[i + len(sep)].isalnum() or text[i + len(sep)] in "_.`"))):
                    parts.append(text[start:i])
                    start = i + len(sep)
                    i = start
                    continue
            elif ch == sep:
                parts.append(text[start:i])
                start = i + 1
        i += 1
    parts.append(text[start:])
    return [p.strip() for p in parts]


def _unwrap(expr: str) -> str:
    expr = expr.strip()
    while expr.startswith("(") and expr.endswith(")"):
        depth = 0
        for i, ch in enumerate(expr):
            depth += ch == "("
            depth -= ch == ")"
            if depth == 0 and i < len(expr) - 1:
                return expr
        expr = expr[1:-1].strip()
    return expr


def _fields(text: str, is_metric) -> set[str]:
    """Metric fields named in ``text`` (string literals and function names dropped)."""
    text = re.sub(r'"(?:[^"\\]|\\.)*"', '""', text)
    out: set[str] = set()
    for m in TOKEN.finditer(text):
        raw = m.group(1)
        rest = text[m.end():].lstrip()
        if rest.startswith("(") or raw.lower() in KEYWORDS or raw.startswith("?"):
            continue
        name = normalise(raw)
        if is_metric(raw, name):
            out.add(name)
    return out


def normalise(raw: str) -> str:
    name = raw.replace("`", "").split("::", 1)[0]  # drop an ES|QL ``::type`` cast
    return name[len("metrics."):] if name.startswith("metrics.") else name


def _dnf(expr: str, is_metric) -> list[frozenset[str]]:
    """Alternative metric sets a WHERE condition needs together in one row."""
    expr = _unwrap(expr)
    ors = _split_top(expr, "OR")
    if len(ors) > 1:
        return [alt for part in ors for alt in _dnf(part, is_metric)]
    ands = _split_top(expr, "AND")
    if len(ands) > 1:
        alts = [frozenset()]
        for part in ands:
            alts = [a | b for a in alts for b in _dnf(part, is_metric)]
        return alts
    if re.match(r"(?i)^NOT\s*\(", expr):
        return _dnf(expr[3:], is_metric)
    return [frozenset(_fields(expr, is_metric))]


def _is_coalesce(expr: str) -> bool:
    """True when the whole expression is one ``COALESCE(...)`` call."""
    expr = _unwrap(expr)
    if not re.match(r"(?i)^COALESCE\s*\(", expr):
        return False
    depth = 0
    for i, ch in enumerate(expr):
        depth += ch == "("
        depth -= ch == ")"
        if depth == 0 and ch == ")":
            return i == len(expr) - 1
    return False


NULL_SAFE = re.compile(r"(?i)COALESCE\s*\(\s*" + FIELD + r"\s*,\s*[^(),]+\)")


def analyse(query: str, is_metric) -> list[tuple[str, str, tuple[str, ...]]]:
    """Return (rule, metric, partners): ``metric`` must share a document with a partner.

    WHERE, EVAL and KEEP pairs have one partner. WHERE->STATS lists every metric
    the filter reads; the aggregated metric is fine when it shares a document with
    any of them (``WHERE a IS NOT NULL OR b IS NOT NULL | STATS AVG(c)`` needs c
    next to a or b).
    """
    segments = _split_top(_strip_comments(query), "|")
    source = re.match(r"(?is)^\s*(?:FROM|TS)\s+(.*?)(?:\s+METADATA\b.*)?$", segments[0])
    if not source:
        return []
    indices = [i.strip().strip('"') for i in source.group(1).split(",") if i.strip()]
    if indices and all(i.split(":")[-1].startswith("logs") for i in indices):
        return []

    needs: set[tuple[str, str, tuple[str, ...]]] = set()

    def _pairs(rule: str, names: set[str]) -> None:
        for a, b in itertools.combinations(sorted(names), 2):
            needs.add((rule, a, (b,)))

    where_alts = [frozenset()]
    has_metric_where = False
    stats_fields: set[str] = set()
    for segment in segments[1:]:
        head, body = (re.split(r"\s+", segment, maxsplit=1) + [""])[:2]
        head = head.upper()
        if head in ("STATS", "INLINESTATS", "INLINE") or head.startswith("STATS"):
            agg = re.split(r"(?i)\bBY\b", body, maxsplit=1)[0]
            stats_fields = _fields(agg, is_metric)
            break
        if head == "WHERE":
            alts = _dnf(body, is_metric)
            for alt in alts:
                _pairs("WHERE", set(alt))
            has_metric_where = has_metric_where or any(alts)
            where_alts = [x | y for x in where_alts for y in alts]
        elif head == "EVAL":
            for assignment in _split_top(body, ","):
                expr = assignment.split("=", 1)[1] if re.match(r"^[\w.`]+\s*=[^=]", assignment) else assignment
                if _is_coalesce(expr):
                    continue
                # COALESCE(metric, 0) turns a missing metric into a constant: no empty row.
                _pairs("EVAL", _fields(NULL_SAFE.sub("0", expr), is_metric))
        elif head == "KEEP":
            _pairs("KEEP", _fields(body.replace("*", ""), is_metric))

    if has_metric_where and all(where_alts):
        filtered = tuple(sorted(set().union(*where_alts)))
        for metric in sorted(stats_fields):
            if not any(metric in alt for alt in where_alts):
                needs.add(("WHERE->STATS", metric, filtered))
    return sorted(needs)


# ── generator cycle ───────────────────────────────────────────────────────────

def _scenario():
    from scenario_engine import get_scenario

    return get_scenario("space")


def otlp_groups(gid: str, scenario) -> dict[tuple, set[str]]:
    """Group one generator's emitted points the way the OTLP intake builds documents."""
    from _pytest.monkeypatch import MonkeyPatch
    from tests.test_scrape_timestamp import metric_points, run_cycles

    patch = MonkeyPatch()
    try:
        per_cycle = run_cycles(gid, scenario, patch)
    finally:
        patch.undo()
    groups: dict[tuple, set[str]] = {}
    for payloads in per_cycle:
        for p in metric_points(payloads):
            dp = p["dp"]
            key = (
                json.dumps(p["resource"], sort_keys=True),
                json.dumps(p["scope"], sort_keys=True),
                json.dumps(sorted((a.get("key"), json.dumps(a.get("value"), sort_keys=True))
                                  for a in dp.get("attributes") or [])),
                str(dp.get("timeUnixNano")),
                str(dp.get("startTimeUnixNano", "")),
                p["unit"],
                p["kind"],
            )
            groups.setdefault(key, set()).add(p["name"])
    return groups


def bulk_docs(gid: str, scenario) -> list[set[str]]:
    """Flattened field names of every document one es_bulk generator cycle sends."""
    from _pytest.monkeypatch import MonkeyPatch
    from generator_engine.registry import GENERATORS, load_entrypoint
    from tests.test_generator_contract import _freeze, _RecordingES, _run_cycle, _scenario_data

    spec = GENERATORS[gid]
    patch = MonkeyPatch()
    try:
        fn = load_entrypoint(spec)
        _freeze(inspect.getmodule(fn), patch)
        data, _ = _scenario_data(scenario, spec, chaos=False)
        client = _RecordingES()
        _run_cycle(fn, client, data, None)
    finally:
        patch.undo()

    def _flat(node, prefix: str, out: set[str]) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                _flat(value, f"{prefix}{key}.", out)
        elif isinstance(node, (int, float)) and not isinstance(node, bool):
            out.add(prefix[:-1])

    docs = []
    for _name, (_stream, batch), _kw in client.payloads:
        for doc in batch or []:
            fields: set[str] = set()
            _flat(doc, "", fields)
            docs.append(fields)
    return docs


def bulk_companion(source: str, a: str, b: str) -> bool:
    """True when one function that writes a bulk doc names both metrics."""
    blocks = re.split(r"(?m)^\s*def ", source)
    return any(BULK_MARK.search(block) and f'"{a}"' in block and f'"{b}"' in block
               for block in blocks)


def _differences(groups: dict[tuple, set[str]], a: str, b: str) -> str:
    labels = ("resource", "scope", "attributes", "timeUnixNano", "startTimeUnixNano", "unit", "type")
    keys_a = [k for k, names in groups.items() if a in names]
    keys_b = [k for k, names in groups.items() if b in names]
    differ = [label for i, label in enumerate(labels)
              if {k[i] for k in keys_a}.isdisjoint({k[i] for k in keys_b})]
    detail = []
    for label, i in (("unit", 5), ("type", 6)):
        if label in differ:
            detail.append(f"{label} {sorted({k[i] for k in keys_a})} vs {sorted({k[i] for k in keys_b})}")
    others = [d for d in differ if d not in ("unit", "type")]
    if others:
        detail.append("differ: " + ", ".join(others))
    return "; ".join(detail) or "never in one group"


# ── audit ─────────────────────────────────────────────────────────────────────

def audit(generator_id: str, scenario) -> dict:
    from generator_engine.registry import GENERATORS

    source = (ROOT / "generators" / generator_id / "generator.py").read_text()
    citation = SOURCE_CITATION.search(source)
    found = PACKAGE_VERSION.search(citation.group(1)) if citation else None
    if not found:
        return {"generator": generator_id, "skipped": "no package citation"}
    package, cited = found.groups()
    latest = latest_version(package)
    queries = esql_queries(package_zip(package, latest))

    spec = GENERATORS.get(generator_id)
    bulk = spec is not None and spec.client == "es_bulk"
    groups: dict[tuple, set[str]] = {}
    docs: list[set[str]] = []
    if bulk:
        docs = bulk_docs(generator_id, scenario)
        emitted = set().union(*docs) if docs else set()
    else:
        groups = otlp_groups(generator_id, scenario)
        emitted = set().union(*groups.values()) if groups else set()

    def is_metric(raw: str, name: str) -> bool:
        return name in emitted or normalise(raw) != raw.replace("`", "").split("::", 1)[0]

    def pair_status(a: str, b: str) -> tuple[str, str]:
        missing = sorted(m for m in (a, b) if m not in emitted)
        if missing:
            return "NOT_EMITTED", "not emitted: " + ", ".join(missing)
        if bulk:
            if any(a in d and b in d for d in docs):
                return "MERGED", ""
            return "UNCOVERED", "never in one bulk document"
        if any(a in names and b in names for names in groups.values()):
            return "MERGED", ""
        if bulk_companion(source, a, b):
            return "BULK", _differences(groups, a, b)
        return "UNCOVERED", _differences(groups, a, b)

    needs: dict[tuple[str, tuple[str, ...]], dict] = {}
    for where, query in queries:
        for rule, metric, partners in analyse(query, is_metric):
            if len(partners) == 1:  # one pair, whichever side it was found on
                metric, partners = sorted((metric, partners[0]))[0], (max(metric, partners[0]),)
            entry = needs.setdefault((metric, partners), {"rules": set(), "sources": set()})
            entry["rules"].add(rule)
            entry["sources"].add(where)

    rank = {"MERGED": 0, "BULK": 1, "UNCOVERED": 2, "NOT_EMITTED": 3}
    findings = []
    for (metric, partners), entry in sorted(needs.items()):
        # Satisfied when the metric shares a document with any one partner.
        results = sorted(((pair_status(metric, p), p) for p in partners),
                         key=lambda item: rank[item[0][0]])
        (status, reason), partner = results[0]
        if status == "UNCOVERED" and len(partners) > 1:
            reason = "; ".join(f"vs {p}: {r}" for (_s, r), p in results if _s == "UNCOVERED")
        findings.append({
            "metrics": [metric, partner] if status != "UNCOVERED" else [metric, *partners],
            "rules": sorted(entry["rules"], key=PAIR_KINDS.index),
            "sources": sorted(entry["sources"]),
            "status": status,
            "reason": reason,
        })
    return {
        "generator": generator_id,
        "package": package,
        "cited": cited,
        "latest": latest,
        "queries": len(queries),
        "pairs": findings,
        "uncovered": sum(f["status"] == "UNCOVERED" for f in findings),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("generators", nargs="*", help="generator ids (default: all)")
    parser.add_argument("--json", action="store_true", help="emit JSON")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)  # generator chatter would bury the report

    scenario = _scenario()
    results = []
    for gid in args.generators or generator_ids():
        try:
            results.append(audit(gid, scenario))
        except Exception as exc:  # one network or generator failure must not hide the rest
            results.append({"generator": gid, "error": f"{type(exc).__name__}: {exc}", "uncovered": 1})

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        for r in results:
            if r.get("skipped") or not (r.get("pairs") or r.get("error")):
                continue
            print(f"\n{r['generator']}  [{r.get('package', '?')}@{r.get('latest', '?')}, "
                  f"{r.get('queries', 0)} ES|QL queries]")
            if r.get("error"):
                print(f"   ERROR {r['error']}")
            for f in r.get("pairs", []):
                reason = f"  ({f['reason']})" if f["reason"] else ""
                print(f"   {f['status']:<11} {f['metrics'][0]} with {' | '.join(f['metrics'][1:])}  "
                      f"[{', '.join(f['rules'])}] {len(f['sources'])} source(s){reason}")
        checked = [r for r in results if not r.get("skipped")]
        counts: dict[str, int] = {}
        for r in checked:
            for f in r.get("pairs", []):
                counts[f["status"]] = counts.get(f["status"], 0) + 1
        summary = ", ".join(f"{counts.get(s, 0)} {s}" for s in
                            ("MERGED", "BULK", "NOT_EMITTED", "UNCOVERED"))
        errors = sum(1 for r in checked if r.get("error"))
        print(f"\n{len(checked)} generators checked ({len(results) - len(checked)} without a "
              f"package citation, {errors} errors): {summary}")
    return 1 if any(r.get("uncovered") for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
