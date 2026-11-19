#!/usr/bin/env python3
"""Make every fault channel light up the dashboards in proportion to its scope.

For each scenario/channel this enforces what ``verify_yaml_scenarios.py`` checks:

* Check 31 - ``affected_services`` contains a trace *entry* service (the trace
  generator only raises the error rate for entry services).  If not, the nearest
  entry service (cascade member, else a caller of the affected service, else the
  first entry service) is moved into ``affected_services``.
* Check 32 - ``business_impact`` yields enough red/yellow executive KPI tiles for
  the channel's scope tier (``SCOPE_TIERS``).  Existing entries are kept where
  they already qualify; deficits are fixed by rewriting entries as absolute
  ``value:`` overrides that sit in the middle of the target palette band (the
  band edges come from ``kpi_band_edges``, the same code that colours the tiles),
  and by adding the most relevant extra KPIs when a channel has too few entries.

Only the ``business_impact`` / ``affected_services`` / ``cascade_services`` blocks
of ``channels/*.yaml`` are rewritten (as text, so the rest of the file is untouched).

Usage (from elastic-launch-demo/)::

    python3 scripts/retune_business_impact.py              # dry run, all scenarios
    python3 scripts/retune_business_impact.py space --apply
"""

from __future__ import annotations

import argparse
import importlib.util
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from elastic_config.dashboards.generate_exec_dashboard import (  # noqa: E402
    _kpi_nominal_range,
    _kpi_is_lower_better,
    _kpi_nominal_range,
    kpi_band_edges,
    kpi_band_usable,
)

_spec = importlib.util.spec_from_file_location("verify_yaml_scenarios", ROOT / "scripts" / "verify_yaml_scenarios.py")
verify = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(verify)

SCENARIOS = ROOT / "scenarios"  # overridden by --scenarios-dir (e.g. a downloaded copy)
_RANK = {"green": 0, "yellow": 1, "red": 2}


# ── text block helpers ───────────────────────────────────────────────────────

def _block_span(lines: list[str], key: str) -> tuple[int, int] | None:
    """Return (start, end) line indexes of a top-level ``key:`` block."""
    for i, line in enumerate(lines):
        if re.match(rf"^{re.escape(key)}:(\s|$)", line):
            j = i + 1
            while j < len(lines) and (not lines[j].strip() or lines[j][0] in " \t-"):
                j += 1
            while j > i + 1 and not lines[j - 1].strip():  # keep trailing blank lines outside
                j -= 1
            return i, j
    return None


def _replace_block(text: str, key: str, new_lines: list[str], *, insert_after: str | None = None) -> str:
    lines = text.split("\n")
    span = _block_span(lines, key)
    if span:
        lines[span[0]:span[1]] = new_lines
    else:
        anchor = _block_span(lines, insert_after) if insert_after else None
        at = anchor[1] if anchor else len(lines)
        lines[at:at] = new_lines
    return "\n".join(lines)


# ── scenario model ───────────────────────────────────────────────────────────

def _load(sd: Path):
    sc = yaml.safe_load((sd / "scenario.yaml").read_text())
    svcs = {}
    for f in (sd / "services").glob("*.yaml"):
        sp = yaml.safe_load(f.read_text())
        svcs[sp["service"]] = sp
    emissions = {e["name"]: e for e in sc.get("executive_kpi_emissions", [])}
    return sc, svcs, emissions


def _entry_services(svcs: dict) -> list[str]:
    no_trace = {n for n, s in svcs.items() if s.get("generates_traces") is False or s.get("subsystem") == "database"}
    ordered = sorted((n for n in svcs if n not in no_trace), key=lambda n: svcs[n].get("sort_order", 99))
    entry = set(ordered[:1]) | {n for n in ordered if svcs[n].get("entry_endpoints")}
    return [n for n in ordered if n in entry]


def _pick_entry(ch: dict, svcs: dict, entry: list[str]) -> str:
    affected = ch.get("affected_services") or []
    cascade = ch.get("cascade_services") or []
    for n in cascade:
        if n in entry:
            return n
    for n in entry:
        callees = {t[0] for t in (svcs[n].get("topology") or [])}
        if callees & set(affected):
            return n
    return entry[0]


def _round_for(spec) -> int | None:
    return spec.get("round") if isinstance(spec, dict) else None


def _fmt(x: float) -> str:
    return repr(round(x, 2)) if abs(x) < 1000 else repr(float(round(x)))


def _target_value(kpi: str, em: dict, band: str) -> float:
    """A value in the middle of the requested band for this KPI."""
    spec = em["value"]
    lower, yellow, red = kpi_band_edges(kpi, spec, em.get("better"))
    lo, hi = _kpi_nominal_range(spec)
    span = hi - lo
    if lower:
        return (yellow + red) / 2 if band == "yellow" else red + 0.35 * span
    if band == "yellow":
        return (yellow + red) / 2
    return red * 0.8 if red > 0 else lo - 0.6 * span


def _impact_entry(kpi: str, em: dict, band: str) -> dict:
    spec = em["value"]
    t = _target_value(kpi, em, band)
    entry = {"kpi": kpi, "value": {"uniform": [round(t * 0.93, 2), round(t * 1.07, 2)]}}
    if _round_for(spec) is not None:
        entry["value"]["round"] = _round_for(spec)
    return entry


def _tokens(text: str) -> set[str]:
    return {t for t in re.split(r"[^a-z0-9]+", text.lower()) if len(t) > 2}


def _relevance(kpi: str, ch: dict) -> int:
    blob = " ".join(
        [str(ch.get(k, "")) for k in ("name", "subsystem", "error_type", "description")]
        + list(ch.get("affected_services") or []) + list(ch.get("cascade_services") or [])
    )
    return len(_tokens(kpi.replace("business.", "")) & _tokens(blob))


def retune_channel(ch: dict, emissions: dict) -> list[dict]:
    """Return a new business_impact list meeting the channel's scope tier."""
    tier, scope, need_non_green, need_red = verify.scope_tier(ch)
    entries = [dict(e) for e in (ch.get("business_impact") or []) if isinstance(e, dict) and e.get("kpi") in emissions]

    def band_of(e) -> str:
        c = verify.impact_band_counts([e], emissions)
        return next((b for b, n in c.items() if n), "green")

    def usable(kpi: str) -> bool:
        return kpi_band_usable(kpi, emissions[kpi]["value"], emissions[kpi].get("better"))

    entries = [e for e in entries if usable(e["kpi"])]
    # add relevant KPIs until there are enough entries to reach the tier
    used = {e["kpi"] for e in entries}
    pool = sorted(
        (k for k in emissions if k not in used and usable(k)),
        key=lambda k: (-_relevance(k, ch), list(emissions).index(k)),
    )
    while len(entries) < need_non_green and pool:
        k = pool.pop(0)
        entries.append(_impact_entry(k, emissions[k], "yellow"))

    bands = [band_of(e) for e in entries]
    tried: set[tuple[int, str]] = set()

    def upgrade(i: int, band: str) -> None:
        tried.add((i, band))
        entries[i] = _impact_entry(entries[i]["kpi"], emissions[entries[i]["kpi"]], band)
        bands[i] = band_of(entries[i])

    # raise entries to red until the red quota is met (most severe first)
    while bands.count("red") < need_red:
        cand = [i for i, b in enumerate(bands) if b != "red" and (i, "red") not in tried]
        if not cand:
            break
        upgrade(max(cand, key=lambda i: _RANK[bands[i]]), "red")
    # raise green entries to yellow until the non-green quota is met
    while sum(b != "green" for b in bands) < need_non_green:
        cand = [i for i, b in enumerate(bands) if b == "green" and (i, "yellow") not in tried]
        if not cand:
            break
        upgrade(cand[0], "yellow")
    return entries


def _entry_lines(entries: list[dict]) -> list[str]:
    out = ["business_impact:"]
    for e in entries:
        out.append(f"  - kpi: {e['kpi']}")
        if "factor" in e:
            u = e["factor"]["uniform"]
            out.append(f"    factor: {{uniform: [{_fmt(u[0])}, {_fmt(u[1])}]}}")
        else:
            v = e["value"]
            u = v["uniform"]
            tail = f", round: {v['round']}" if "round" in v else ""
            out.append(f"    value: {{uniform: [{_fmt(u[0])}, {_fmt(u[1])}]{tail}}}")
    return out


def _infer_better(sd: Path, emissions: dict) -> dict[str, str]:
    """Infer ``better: lower|higher`` for KPIs a channel degrades, from the author's
    own direction: factor > 1 / value above nominal = the KPI rises when broken."""
    votes: dict[str, list[str]] = {}
    for f in sorted((sd / "channels").glob("*.yaml")):
        for imp in yaml.safe_load(f.read_text()).get("business_impact") or []:
            if not isinstance(imp, dict) or imp.get("kpi") not in emissions:
                continue
            nominal = verify._uniform_mid(emissions[imp["kpi"]].get("value"))
            if "factor" in imp:
                fm = verify._uniform_mid(imp["factor"])
                if fm is None or fm == 1.0:
                    continue
                rises = fm > 1.0
            else:
                vm = verify._uniform_mid(imp.get("value"))
                if vm is None or nominal is None or vm == nominal:
                    continue
                rises = vm > nominal
            votes.setdefault(imp["kpi"], []).append("lower" if rises else "higher")
    out = {}
    for kpi, v in votes.items():
        if emissions[kpi].get("better") in ("lower", "higher"):
            continue
        out[kpi] = max(("lower", "higher"), key=lambda d: (v.count(d), d == ("lower" if _kpi_is_lower_better(kpi) else "higher")))
    return out


def _write_better(text: str, better: dict[str, str]) -> str:
    lines = text.split("\n")
    out = []
    for line in lines:
        out.append(line)
        m = re.match(r"^(\s*)- name:\s*(\S+)\s*$", line)
        if m and m.group(2) in better:
            out.append(f"{m.group(1)}  better: {better[m.group(2)]}")
    return "\n".join(out)


def process(scenario_id: str, apply: bool) -> tuple[int, int]:
    sd = SCENARIOS / scenario_id
    sc, svcs, emissions = _load(sd)
    better = _infer_better(sd, emissions)
    if better:
        for k, d in better.items():
            emissions[k]["better"] = d
        if apply:
            p = sd / "scenario.yaml"
            p.write_text(_write_better(p.read_text(), better))
    entry = _entry_services(svcs)
    n_entry = n_impact = 0
    for f in sorted((sd / "channels").glob("*.yaml")):
        text = f.read_text()
        ch = yaml.safe_load(text)
        new_text = text

        if not set(ch.get("affected_services") or []) & set(entry):
            pick = _pick_entry(ch, svcs, entry)
            ch["affected_services"] = list(ch.get("affected_services") or []) + [pick]
            ch["cascade_services"] = [n for n in (ch.get("cascade_services") or []) if n != pick]
            new_text = _replace_block(new_text, "affected_services", [f"affected_services: [{', '.join(ch['affected_services'])}]"])
            new_text = _replace_block(
                new_text, "cascade_services",
                [f"cascade_services: [{', '.join(ch['cascade_services'])}]" if ch["cascade_services"] else "cascade_services: []"],
            )
            n_entry += 1

        if sc.get("executive_kpi_emitter_service_name") and emissions:
            _, _, need_non_green, need_red = verify.scope_tier(ch)
            c = verify.impact_band_counts(ch.get("business_impact") or [], emissions)
            if c["yellow"] + c["red"] < need_non_green or c["red"] < need_red:
                entries = retune_channel(ch, emissions)
                new_text = _replace_block(new_text, "business_impact", _entry_lines(entries), insert_after="rca_clues")
                n_impact += 1

        if new_text != text and apply:
            f.write_text(new_text)
    # KPIs the retune pulled in (or never voted on) get an explicit direction too,
    # from the same keyword heuristic the dashboard would otherwise use.
    used = {
        i["kpi"]
        for f in (sd / "channels").glob("*.yaml")
        for i in (yaml.safe_load(f.read_text()).get("business_impact") or [])
        if isinstance(i, dict) and i.get("kpi") in emissions
    }
    fill = {
        k: ("lower" if _kpi_is_lower_better(k) else "higher")
        for k in sorted(used) if emissions[k].get("better") is None
    }
    if fill and apply:
        p = sd / "scenario.yaml"
        p.write_text(_write_better(p.read_text(), fill))
    return n_entry, n_impact


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenarios", nargs="*", help="scenario ids (default: all under scenarios/)")
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    ap.add_argument("--scenarios-dir", type=Path, help="directory holding <id>/scenario.yaml (default: scenarios/)")
    args = ap.parse_args()
    if args.scenarios_dir:
        global SCENARIOS
        SCENARIOS = args.scenarios_dir.resolve()
    ids = args.scenarios or sorted(p.parent.name for p in SCENARIOS.glob("*/scenario.yaml"))
    tot_e = tot_i = 0
    for sid in ids:
        e, i = process(sid, args.apply)
        tot_e += e
        tot_i += i
        print(f"{sid:14} entry-service fixes: {e:2}  business_impact retunes: {i:2}")
    print(f"TOTAL entry fixes {tot_e}, impact retunes {tot_i} ({'applied' if args.apply else 'dry run'})")


if __name__ == "__main__":
    main()
