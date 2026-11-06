#!/usr/bin/env python3
"""Extract emit requirements from package OOTB alerting rule templates.

Reads ``kibana/alerting_rule_template/*.json`` (or a directory of template
JSON files) and writes ``alert_required_fields.json`` summarizing every
ES|QL identifier generators must emit for rules to verify.

Usage::

    python3 extract_alert_esql_fields.py \\
        --package-dir .cache/integrations/packages/aws_lambda_otel \\
        --out /tmp/aws_lambda_otel/alert_required_fields.json

    python3 extract_alert_esql_fields.py \\
        --templates-dir /path/to/alerting_rule_template \\
        --out ./alert_required_fields.json

Exit 0 always when templates parse; exit 2 on I/O / arg errors.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

# Strip line comments that appear in package ES|QL templates.
_COMMENT_RE = re.compile(r"(?m)^\s*//.*$")
# FROM index / data stream patterns (first clause only).
_FROM_RE = re.compile(r"(?i)\bFROM\s+([^\n|]+)")
# INCREASE(field) / RATE(field) — need counter_long.
_INCREASE_RE = re.compile(r"(?i)\b(?:INCREASE|RATE)\s*\(\s*`?([A-Za-z0-9_.*\-]+)`?\s*\)")
# COALESCE(a, b, ...) — every named operand must exist as a column.
_COALESCE_RE = re.compile(r"(?i)\bCOALESCE\s*\(([^)]*)\)")
# ``name = expr`` inside STATS / EVAL defines a column, not an emitted field. The lookbehind
# keeps ==, !=, <= and >= comparisons from looking like assignments.
_ALIAS_RE = re.compile(r"(?<![<>!=])\b([A-Za-z_]\w*)\s*=(?!=)")
_STRING_LITERAL_RE = re.compile(r'"(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\'')
# Backticked metric / field refs.
_BACKTICK_RE = re.compile(r"`([^`]+)`")
# Bare attributes.* / resource.attributes.* / metrics.* references.
_ATTR_RE = re.compile(
    r"\b((?:resource\.)?attributes\.[A-Za-z0-9_.*\-]+"
    r"|metrics\.[A-Za-z0-9_.*\-]+"
    r"|[A-Za-z][A-Za-z0-9_.*\-]*(?:\.[A-Za-z0-9_.*\-]+)+)\b"
)
# CloudWatch / common equality filters.
_EQ_STRING_RE = re.compile(
    r"""\b(Namespace|MetricName|stat|data_stream\.dataset)\s*==\s*["']([^"']+)["']"""
)
# VALUES(field) / BY field lists — keep column names.
_VALUES_RE = re.compile(r"(?i)\bVALUES\s*\(\s*`?([A-Za-z0-9_.*\-]+)`?\s*\)")
# SUM/AVG/MAX/MIN(field) — prefer gauges when not also INCREASE/RATE.
_AGG_RE = re.compile(
    r"(?i)\b(?:SUM|AVG|MAX|MIN)\s*\(\s*`?([A-Za-z0-9_.*\-]+)`?\s*\)"
)

_ESQL_KEYWORDS = frozenset({
    "from", "where", "stats", "eval", "sort", "limit", "keep", "drop",
    "rename", "dissect", "grok", "enrich", "mv_expand", "null", "true",
    "false", "and", "or", "not", "by", "in", "asc", "desc", "like", "rlike",
    "is", "as", "case", "when", "then", "else", "end", "coalesce", "concat",
    "round", "to_double", "to_long", "to_string", "to_datetime", "date_diff",
    "now", "count", "count_distinct", "values", "sum", "avg", "max", "min",
    "increase", "rate", "last_over_time", "sum_over_time", "avg_over_time",
    "max_over_time", "min_over_time", "bucket", "date_trunc",
})


def _strip_comments(esql: str) -> str:
    return _COMMENT_RE.sub("", esql)


def _split_coalesce_args(inner: str) -> list[str]:
    """Split COALESCE args on top-level commas (no nested paren support beyond 1)."""
    parts: list[str] = []
    depth = 0
    cur: list[str] = []
    for ch in inner:
        if ch == "(":
            depth += 1
            cur.append(ch)
        elif ch == ")":
            depth = max(0, depth - 1)
            cur.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if cur:
        parts.append("".join(cur).strip())
    return parts


def _normalize_field(token: str) -> str | None:
    t = token.strip().strip("`").strip()
    if not t or t.startswith('"') or t.startswith("'"):
        return None
    # Skip numeric / boolean / cast literals like 0::long
    if re.fullmatch(r"[0-9.]+(::\w+)?", t):
        return None
    if t.lower() in ("null", "true", "false", "unknown"):
        return None
    # Strip metrics. prefix for a secondary bare name, but keep original too.
    return t


def _is_identifier_field(name: str) -> bool:
    base = name.split(".")[0].lower()
    if name.lower() in _ESQL_KEYWORDS or base in _ESQL_KEYWORDS:
        return False
    # Must look like a field (has a dot, or CloudWatch / prometheus style).
    if "." in name or "_" in name or name in ("Namespace", "MetricName", "stat"):
        return True
    return False


def parse_esql(esql: str) -> dict[str, Any]:
    """Return structured emit requirements for one ES|QL query."""
    cleaned = _strip_comments(esql)
    aliases = set(_ALIAS_RE.findall(_STRING_LITERAL_RE.sub('""', cleaned)))
    from_streams: list[str] = []
    for m in _FROM_RE.finditer(cleaned):
        raw = m.group(1).strip()
        # Drop trailing pipes already handled; take first token cluster.
        for part in re.split(r"\s*,\s*", raw):
            part = part.strip().strip("`")
            if part:
                from_streams.append(part)

    counter_required: set[str] = set()
    for m in _INCREASE_RE.finditer(cleaned):
        f = _normalize_field(m.group(1))
        if f:
            counter_required.add(f)

    coalesce_operands: set[str] = set()
    for m in _COALESCE_RE.finditer(cleaned):
        for arg in _split_coalesce_args(m.group(1)):
            # Ignore string literals used as defaults.
            if arg.startswith('"') or arg.startswith("'"):
                continue
            # MAX(COALESCE(...)) etc. — take bare identifiers only.
            ident = re.match(r"`?([A-Za-z0-9_.*\-]+)`?", arg.strip())
            if not ident:
                continue
            f = _normalize_field(ident.group(1))
            if f and _is_identifier_field(f):
                coalesce_operands.add(f)

    fields: set[str] = set()
    for m in _BACKTICK_RE.finditer(cleaned):
        f = _normalize_field(m.group(1))
        if f and _is_identifier_field(f):
            fields.add(f)
    for m in _ATTR_RE.finditer(cleaned):
        f = _normalize_field(m.group(1))
        if f and _is_identifier_field(f):
            fields.add(f)
    for m in _VALUES_RE.finditer(cleaned):
        f = _normalize_field(m.group(1))
        if f:
            fields.add(f)

    gauge_preferred: set[str] = set()
    for m in _AGG_RE.finditer(cleaned):
        f = _normalize_field(m.group(1))
        if f and f not in counter_required:
            gauge_preferred.add(f)

    eq_filters: dict[str, set[str]] = defaultdict(set)
    for m in _EQ_STRING_RE.finditer(cleaned):
        eq_filters[m.group(1)].add(m.group(2))

    # CloudWatch attrs are always required when filtered.
    for key in ("Namespace", "MetricName", "stat"):
        if key in eq_filters:
            fields.add(key)

    fields |= counter_required | coalesce_operands | gauge_preferred
    # Columns the query itself defines are not fields a generator emits.
    coalesce_operands -= aliases
    fields -= aliases

    # FROM streams are index patterns, not emit fields — drop accidental matches.
    stream_tokens: set[str] = set()
    for s in from_streams:
        stream_tokens.add(s)
        stream_tokens.add(s.rstrip("*").rstrip("-"))
        # metrics-foo.otel-* → metrics-foo.otel
        if s.startswith("metrics-") or s.startswith("logs-") or s.startswith("traces-"):
            stream_tokens.add(s.split("*", 1)[0].rstrip("-"))
    fields -= stream_tokens
    # Incomplete path fragments from backticks around amazonaws.com/... metrics.
    fields = {
        f for f in fields
        if f not in ("metrics.amazonaws.com", "amazonaws.com")
        and not f.endswith(".otel")
    }

    return {
        "from_streams": sorted(set(from_streams)),
        "fields": sorted(fields),
        "counter_required": sorted(counter_required),
        "gauge_preferred": sorted(gauge_preferred),
        "coalesce_operands": sorted(coalesce_operands),
        "eq_filters": {k: sorted(v) for k, v in sorted(eq_filters.items())},
    }


def _split_top_level(text: str, sep: str) -> list[str]:
    """Split on ``sep`` outside parentheses and string literals."""
    parts: list[str] = []
    cur: list[str] = []
    depth = 0
    quote = ""
    for ch in text:
        if quote:
            cur.append(ch)
            if ch == quote:
                quote = ""
            continue
        if ch in "\"'":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        elif ch == sep and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
            continue
        cur.append(ch)
    if "".join(cur).strip():
        parts.append("".join(cur).strip())
    return parts


def _column_source(expr: str, aliases: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Where a grouping column's value comes from: the first non-null of ``fields``, else ``default``."""
    expr = expr.strip().strip("`")
    if expr in aliases:
        return aliases[expr]
    coalesce = re.fullmatch(r"(?is)COALESCE\s*\((.*)\)", expr)
    if coalesce:
        fields: list[str] = []
        default = None
        for arg in _split_top_level(coalesce.group(1), ","):
            if arg[:1] in "\"'":
                default = arg.strip("\"'")
                break
            fields.extend(_column_source(arg, aliases).get("fields", []))
        return {"fields": fields, "default": default}
    if re.fullmatch(r"[A-Za-z_@][\w.\-]*", expr):
        return {"fields": [expr], "default": None}
    return {"fields": [], "default": None, "unresolved": expr}


def alert_identity(esql: str) -> dict[str, Any]:
    """How Kibana groups an ES|QL rule's rows into alert instances.

    A rule with ``groupBy: row`` builds each alert id from its last ``STATS ... BY``
    columns, or from every ``KEEP`` column when the query has no ``STATS``. ``where``
    holds the row-level filters that run before that ``STATS`` (all filters for a
    raw-row rule), so a caller can tell which emitted rows reach the grouping.
    ``min_count`` is the smallest ``COUNT`` a group needs to pass a threshold after
    the ``STATS`` (1 when there is none).
    """
    commands = _split_top_level(_strip_comments(esql), "|")
    aliases: dict[str, dict[str, Any]] = {}
    where: list[str] = []
    identity: list[dict[str, Any]] = []
    mode = "row"
    keep: list[str] | None = None
    counts: set[str] = set()
    min_count = 1
    for position, command in enumerate(commands[1:], start=1):
        verb, _, body = command.strip().partition(" ")
        verb, _, rest = verb.partition("\n")
        body = f"{rest} {body}" if rest else body
        verb = verb.upper()
        if verb == "EVAL":
            for assignment in _split_top_level(body, ","):
                name, eq, expr = assignment.partition("=")
                if eq:
                    aliases[name.strip().strip("`")] = _column_source(expr, aliases)
        elif verb == "WHERE":
            where.append(body.strip())
        elif verb == "STATS":
            by = re.split(r"(?i)\bBY\b", body, maxsplit=1)
            identity = []
            mode = "by"
            for column in _split_top_level(by[1], ",") if len(by) == 2 else []:
                name, eq, expr = column.partition("=")
                source = _column_source(expr if eq else name, aliases)
                identity.append({"name": name.strip().strip("`"), **source})
            for aggregate in _split_top_level(by[0], ","):
                name, eq, expr = aggregate.partition("=")
                if eq and re.match(r"(?i)\s*COUNT\s*\(", expr):
                    counts.add(name.strip().strip("`"))
            for after in commands[position + 1:]:
                threshold = re.match(r"(?is)\s*WHERE\s+`?(\w+)`?\s*(>=|>)\s*(\d+)\s*$", after)
                if threshold and threshold.group(1) in counts:
                    floor = int(threshold.group(3)) + (threshold.group(2) == ">")
                    min_count = max(min_count, floor)
            break
        elif verb == "KEEP":
            keep = [c.strip().strip("`") for c in _split_top_level(body, ",")]
    if mode == "row":
        identity = [{"name": c, **_column_source(c, aliases)} for c in keep or []]
    return {"mode": mode, "identity": identity, "where": where, "min_count": min_count}


def _load_template(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"warn: skip {path}: {exc}", file=sys.stderr)
        return None
    attrs = data.get("attributes") or data
    params = attrs.get("params") or {}
    esql = None
    if isinstance(params.get("esqlQuery"), dict):
        esql = params["esqlQuery"].get("esql")
    elif isinstance(params.get("esql"), str):
        esql = params["esql"]
    if not esql:
        return None
    return {
        "id": data.get("id") or path.stem,
        "name": attrs.get("name") or path.stem,
        "esql": esql,
        "parsed": parse_esql(esql),
    }


def collect_templates(package_dir: Path | None, templates_dir: Path | None) -> list[Path]:
    paths: list[Path] = []
    if templates_dir:
        paths.extend(sorted(templates_dir.glob("*.json")))
    if package_dir:
        candidate = package_dir / "kibana" / "alerting_rule_template"
        if candidate.is_dir():
            paths.extend(sorted(candidate.glob("*.json")))
        # Some caches flatten templates next to the package.
        alt = package_dir / "alerting_rule_template"
        if alt.is_dir():
            paths.extend(sorted(alt.glob("*.json")))
    # De-dupe by path resolve.
    seen: set[Path] = set()
    out: list[Path] = []
    for p in paths:
        r = p.resolve()
        if r not in seen:
            seen.add(r)
            out.append(p)
    return out


def merge_inventory(templates: list[dict[str, Any]]) -> dict[str, Any]:
    all_fields: set[str] = set()
    counter_required: set[str] = set()
    gauge_preferred: set[str] = set()
    coalesce_operands: set[str] = set()
    from_streams: set[str] = set()
    eq_filters: dict[str, set[str]] = defaultdict(set)
    cloudwatch = False

    for t in templates:
        p = t["parsed"]
        all_fields.update(p["fields"])
        counter_required.update(p["counter_required"])
        gauge_preferred.update(p["gauge_preferred"])
        coalesce_operands.update(p["coalesce_operands"])
        from_streams.update(p["from_streams"])
        for k, vals in p["eq_filters"].items():
            eq_filters[k].update(vals)
        if "Namespace" in p["fields"] or "MetricName" in p["fields"]:
            cloudwatch = True
        if any(s.startswith("metrics-aws.") for s in p["from_streams"]):
            cloudwatch = True

    # Fields that appear in both INCREASE and SUM/AVG: counter wins for alerts.
    gauge_preferred -= counter_required

    return {
        "schema_version": 1,
        "alert_count": len(templates),
        "from_streams": sorted(from_streams),
        "fields": sorted(all_fields),
        "counter_required": sorted(counter_required),
        "gauge_preferred": sorted(gauge_preferred),
        "coalesce_operands": sorted(coalesce_operands),
        "eq_filters": {k: sorted(v) for k, v in sorted(eq_filters.items())},
        "requires_cloudwatch_namespace_metricname": cloudwatch,
        "alerts": [
            {
                "id": t["id"],
                "name": t["name"],
                **t["parsed"],
            }
            for t in templates
        ],
        "notes": [
            "Emit every field in fields[] / coalesce_operands[] or ES|QL fails with Unknown column.",
            "counter_required[] must map as counter_long (INCREASE/RATE); reset sticky gauges.",
            "gauge_preferred[] are SUM/AVG/MAX/MIN targets not also used with INCREASE — prefer gauges.",
            "When the same field is in both counter_required and dashboard SUM panels, prefer counter "
            "for the alert and accept dashboard SUM may need period-delta siblings or panel ops that "
            "tolerate counters — do not leave INCREASE on a gauge.",
            "If from_streams is outside SPEC.datasets / generator-owned streams, emit a companion "
            "dataset or flag a package template mismatch (do not edit OOTB rules).",
        ],
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--package-dir", type=Path, help="elastic/integrations package root")
    ap.add_argument("--templates-dir", type=Path, help="Directory of alerting_rule_template JSON")
    ap.add_argument("--out", type=Path, required=True, help="Output alert_required_fields.json")
    args = ap.parse_args(argv)

    if not args.package_dir and not args.templates_dir:
        print("error: pass --package-dir and/or --templates-dir", file=sys.stderr)
        return 2

    paths = collect_templates(args.package_dir, args.templates_dir)
    if not paths:
        print("warn: no alerting_rule_template JSON found", file=sys.stderr)
        inventory = merge_inventory([])
    else:
        templates: list[dict[str, Any]] = []
        for p in paths:
            loaded = _load_template(p)
            if loaded:
                templates.append(loaded)
        inventory = merge_inventory(templates)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(inventory, indent=2) + "\n", encoding="utf-8")
    print(
        f"Wrote {args.out} ({inventory['alert_count']} alerts, "
        f"{len(inventory['fields'])} fields, "
        f"{len(inventory['counter_required'])} counter_required, "
        f"{len(inventory['coalesce_operands'])} coalesce_operands)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
