"""Package alerts keep the same alert ids for as long as their fault is active.

Kibana's ES|QL rules (``groupBy: row``) build each alert id from the rule's
``STATS ... BY`` columns, or from every ``KEEP`` column when there is no ``STATS``.
A generator that draws one of those values at random per sample (a pid, a UUID, a
caller) hands the rule new ids on every run: the alert opens and recovers each run,
and never fires at all when the rule's ``alertDelay`` is above 1.

For every gt-* channel whose impact key drives a package alert, run the generator
for several ticks with that channel active, rebuild the documents the intake would
index, keep the rows the rule's pre-grouping filters let through, and fail when
fresh alert ids keep turning up that are never seen again. The rule shapes come from
``tests/fixtures/alert_identity.json`` (``scripts/snapshot_alert_identity.py``).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from generator_engine.registry import GENERATORS
from tests._esql_filter import Filter as _Filter
from tests._emit_harness import (
    TICKS,
    field as _field,
    gt_channels as _gt_channels,
    matches_from as _matches_from,
    run_ticks as _run_ticks,
)

ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT = json.loads((ROOT / "tests" / "fixtures" / "alert_identity.json").read_text())
ALERT_DOC = re.compile(r'Drive alert "([^"]+)"')
WARMUP = 1


def _cases() -> list[tuple[str, str, str]]:
    cases = []
    for (gid, key) in sorted(_gt_channels()):
        meta = GENERATORS[gid].impact_keys.get(key, {}) if gid in GENERATORS else {}
        for alert in ALERT_DOC.findall(meta.get("doc", "")):
            rule = SNAPSHOT.get(alert)
            if rule and _id_columns(rule):
                cases.append((gid, key, alert))
    return cases


def _passes(doc: dict, filters: list[_Filter]) -> bool:
    """Keep a row unless a filter is definitely false (unreadable filters keep it)."""
    return all(f.evaluate(doc) is not False for f in filters)


def _id_columns(rule: dict) -> list[dict]:
    """Identity columns a generator controls; a time bucket changes every window by design."""
    return [c for c in rule["identity"] if c["fields"] and "@timestamp" not in c["fields"]]


def _identity(doc: dict, columns: list[dict]):
    values = []
    for column in columns:
        value = next((_field(doc, f) for f in column["fields"] if _field(doc, f) is not None),
                     column.get("default"))
        if value is None:
            return None
        values.append(value)
    return tuple(values)


# ── tests ──

CASES = _cases()
GT = _gt_channels()


def test_every_alert_named_by_an_impact_key_is_in_the_snapshot():
    missing = sorted(
        (gid, key, alert)
        for gid, spec in GENERATORS.items()
        for key, meta in spec.impact_keys.items()
        for alert in ALERT_DOC.findall(meta.get("doc", ""))
        if alert not in SNAPSHOT
    )
    assert not missing, (
        f"refresh tests/fixtures/alert_identity.json (scripts/snapshot_alert_identity.py): {missing}"
    )


@pytest.mark.parametrize("gid,key,alert", CASES, ids=[f"{g}:{k}" for g, k, _ in CASES])
def test_alert_ids_stay_stable_while_the_fault_is_active(gid, key, alert, monkeypatch):
    rule = SNAPSHOT[alert]
    scenario_dir, channel = GT[(gid, key)]
    filters = [_Filter(text) for text in rule["where"]]
    per_tick = _run_ticks(gid, scenario_dir, channel, monkeypatch)

    columns = _id_columns(rule)
    ticks_seen: dict[tuple, set[int]] = {}
    rows: dict[tuple, int] = {}
    for tick, docs in enumerate(per_tick):
        for index, doc in docs:
            if _matches_from(index, rule["from"]) and _passes(doc, filters):
                ident = _identity(doc, columns)
                if ident is not None:
                    ticks_seen.setdefault(ident, set()).add(tick)
                    rows[ident] = rows.get(ident, 0) + 1
    # An id first seen before the last tick must come back: one that shows up in a
    # single tick is an alert that opens and recovers within one rule run. A lone
    # stray can be sampling within one rule window; fresh ids tick after tick cannot.
    one_shot = sorted(
        (ident for ident, seen in ticks_seen.items()
         if len(seen) == 1 and WARMUP <= min(seen) < TICKS - 1 and rows[ident] >= rule["min_count"]),
        key=str,
    )
    if len({min(ticks_seen[ident]) for ident in one_shot}) < 2:
        one_shot = []
    assert not one_shot, (
        f"{gid}.{key} gives {alert!r} (alertDelay {rule['alert_delay']}) {len(one_shot)} alert ids "
        f"that appear in a single tick, e.g. {one_shot[:3]}; keep {[c['name'] for c in columns]} "
        "fixed while the fault is active"
    )
