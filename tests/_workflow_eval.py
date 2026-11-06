"""Evaluate a library workflow's ES|QL step against rebuilt generator documents.

Shared by ``test_workflow_template_emit.py`` (every generator, generically) and
``test_aws_blog_walkthrough.py`` (the AWS scenario's RDS story end to end).
"""
from __future__ import annotations

import re

from tests._emit_harness import field
from tests._esql_filter import Filter
from tests._template_eval import number

RESOLVED = re.compile(r"\{\{\s*steps\.resolve\.output\.([A-Za-z_]\w*)\s*\}\}")
_ROW_COMMANDS = ("EVAL", "DISSECT", "GROK", "STATS", "KEEP", "SORT", "LIMIT", "RENAME", "DROP", "MV_EXPAND")


def split_top(text: str, sep: str) -> list[str]:
    """Split on ``sep`` outside quotes and parentheses (``sep`` matched case-insensitively)."""
    parts, depth, quote, start, i = [], 0, None, 0, 0
    upper = text.upper()
    while i < len(text):
        ch = text[i]
        if quote:
            quote = None if ch == quote else quote
        elif ch in "\"'":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and upper.startswith(sep, i):
            parts.append(text[start:i])
            i += len(sep)
            start = i
            continue
        i += 1
    parts.append(text[start:])
    return [p.strip() for p in parts if p.strip()]


def usable(term: str) -> bool:
    """Drop the time-window terms; everything else is evaluated (or reported)."""
    return not any(x in term for x in ("@timestamp", "window_start"))


_NUMERIC_AGGS = {"SUM", "AVG", "MEDIAN", "PERCENTILE", "STD_DEV", "RATE", "INCREASE"}


def query_parts(query: str) -> tuple[list[str], list[tuple[str, str, str]], set[str]]:
    """``(row WHERE terms, [(function, field, condition)], computed column names)``."""
    commands = split_top(re.sub(r"//[^\n]*", "", query), "|")[1:]
    where: list[str] = []
    aggs: list[tuple[str, str, str]] = []
    computed: set[str] = set()
    raw_rows = True
    for command in commands:
        verb = command.split(None, 1)[0].upper()
        body = command[len(verb):].strip()
        if verb == "WHERE" and raw_rows:
            where.extend(t for t in split_top(body, " AND ") if usable(t))
        elif verb in ("EVAL", "DISSECT", "GROK"):
            raw_rows = False
            computed.update(re.findall(r"%\{(\w+)\}", body))
            computed.update(m.group(1) for m in re.finditer(r"(?:^|,)\s*(\w+)\s*=", body))
        elif verb == "STATS":
            raw_rows = False
            body = re.split(r"\sBY\s", body, maxsplit=1, flags=re.I)[0]
            for agg in split_top(body, ","):
                match = re.match(r"(?:\w+\s*=\s*)?(\w+)\(\s*(`[^`]+`|[\w.@/]+|\*)\s*\)\s*(?:WHERE\s+(.*))?$", agg, re.S)
                if match and match.group(2) not in ("*", "@timestamp"):
                    aggs.append((match.group(1).upper(), match.group(2).strip("`"), (match.group(3) or "").strip()))
        elif verb in _ROW_COMMANDS:
            raw_rows = False
    return where, aggs, computed


def strict(terms: list[str], doc: dict) -> bool:
    return all(Filter(term).evaluate(doc) is True for term in terms)




def step_problems(query: str, values: dict[str, str], docs: list[dict]) -> tuple[list[dict], list[str]]:
    """``(rows matching the step's WHERE, aggregates with no data)`` for one step.

    ``values`` fill ``{{ steps.resolve.output.<input> }}``; a term still holding a
    placeholder raises ``LookupError`` naming the inputs nothing provided.
    """
    query = RESOLVED.sub(lambda m: values.get(m.group(1), m.group(0)), query)
    where, aggs, computed = query_parts(query)
    unresolved = sorted({name for term in where for name in RESOLVED.findall(term)})
    if unresolved:
        raise LookupError(unresolved)
    rows = [doc for doc in docs if strict(where, doc)]
    problems = []
    for func, name, condition in aggs:
        if any(c in condition for c in computed):
            continue  # reads a column the query computes (EVAL / DISSECT), not a field
        terms = [t for t in split_top(condition, " AND ") if usable(t)] if condition else []
        has_value = (lambda v: number(v) is not None) if func in _NUMERIC_AGGS else (lambda v: v is not None)
        if not any(has_value(field(doc, name)) and strict(terms, doc) for doc in rows):
            problems.append(f"{func}({name})" + (f" WHERE {condition}" if condition else ""))
    return rows, problems
