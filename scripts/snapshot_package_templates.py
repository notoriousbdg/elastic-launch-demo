#!/usr/bin/env python3
"""Snapshot the SLO templates and ML modules of every package a generator installs.

Step 21 of the deployer turns each installed package's ``slo_template`` and
``ml_module`` assets into live SLOs and anomaly detection jobs. For every package in
a generator's ``SPEC.integrations``, download the version the deployer would install
(the version its generator cites in ``SOURCE_INTEGRATION``) from the Elastic Package Registry and record what each
template reads, via the add-telemetry-generator skill's ``extract_package_templates``.

Writes ``tests/fixtures/slo_templates.json`` and ``tests/fixtures/ml_modules.json``,
keyed by template / module id, and ``tests/fixtures/alert_rules.json`` (each alert rule
template's ES|QL, which decides the library workflows a rule starts).
``tests/test_slo_template_emit.py``, ``tests/test_ml_module_emit.py`` and
``tests/test_workflow_template_eligibility.py`` read them offline. Re-run after a package release or
a change to ``SPEC.integrations``. Needs network.

Usage::

    python3 scripts/snapshot_package_templates.py            # all generators
    python3 scripts/snapshot_package_templates.py redis_otel # refresh one or more
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from audit_package_coverage import EPR, SKILL_SCRIPTS, _get, package_zip  # noqa: E402

FIXTURES = ROOT / "tests" / "fixtures"
SLO_OUT = FIXTURES / "slo_templates.json"
ML_OUT = FIXTURES / "ml_modules.json"
RULES_OUT = FIXTURES / "alert_rules.json"


def extractor():
    spec = importlib.util.spec_from_file_location(
        "extract_package_templates", SKILL_SCRIPTS / "extract_package_templates.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def deployed_version(package: str) -> str | None:
    """The version step 20 installs: the generator's pin, else the newest release."""
    from generator_engine.registry import package_versions

    pinned = package_versions().get(package)
    if pinned:
        return pinned
    found = json.loads(_get(f"{EPR}/search?package={package}&prerelease=true"))
    return found[0]["version"] if found else None


def _rules(files: dict[str, bytes]) -> list[dict]:
    rules = []
    for name, raw in sorted(files.items()):
        if "/alerting_rule_template/" not in name:
            continue
        doc = json.loads(raw)
        attrs = doc.get("attributes") or doc
        esql = ((attrs.get("params") or {}).get("esqlQuery") or {}).get("esql")
        if esql:
            rules.append({"id": doc.get("id") or Path(name).stem, "name": attrs.get("name"), "esql": esql})
    return rules


def package_templates(package: str, extract) -> tuple[str, list[dict], list[dict], list[dict]] | None:
    version = deployed_version(package)
    if not version:
        return None
    archive = package_zip(package, version)
    files = {n: archive.read(n) for n in archive.namelist() if "/kibana/" in n and n.endswith(".json")}
    manifest = next((n for n in archive.namelist() if n.endswith("/manifest.yml") and n.count("/") == 1), None)
    slos, mls = extract.package_inventories(files, archive.read(manifest).decode() if manifest else "")
    return version, slos, mls, _rules(files)


def _merge(snapshot: dict, entries: list[dict], package: str, version: str, gid: str) -> None:
    for entry in entries:
        gids = snapshot.get(entry["id"], {}).get("generators", [])
        snapshot[entry["id"]] = {
            "generators": sorted({*gids, gid}),
            "package": package,
            "version": version,
            **entry,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("generators", nargs="*", help="generator ids (default: all)")
    args = parser.parse_args()

    from generator_engine.registry import GENERATORS

    extract = extractor()
    partial = bool(args.generators)
    slo_snap = json.loads(SLO_OUT.read_text()) if SLO_OUT.exists() and partial else {}
    ml_snap = json.loads(ML_OUT.read_text()) if ML_OUT.exists() and partial else {}
    rule_snap = json.loads(RULES_OUT.read_text()) if RULES_OUT.exists() and partial else {}
    for snap in (slo_snap, ml_snap, rule_snap):
        for entry in snap.values():
            entry["generators"] = [g for g in entry["generators"] if g not in args.generators]

    cache: dict[str, tuple[str, list[dict], list[dict], list[dict]] | None] = {}
    for gid in sorted(args.generators or GENERATORS):
        for package in GENERATORS[gid].integrations:
            if package not in cache:
                cache[package] = package_templates(package, extract)
            found = cache[package]
            if not found:
                print(f"{gid}: {package} not in the registry")
                continue
            version, slos, mls, rules = found
            _merge(slo_snap, slos, package, version, gid)
            _merge(ml_snap, mls, package, version, gid)
            _merge(rule_snap, rules, package, version, gid)
            if slos or mls:
                print(f"{gid}: {package}@{version} — {len(slos)} SLO templates, {len(mls)} ML modules")

    FIXTURES.mkdir(parents=True, exist_ok=True)
    for path, snap in ((SLO_OUT, slo_snap), (ML_OUT, ml_snap), (RULES_OUT, rule_snap)):
        snap = {k: v for k, v in sorted(snap.items()) if v["generators"]}
        path.write_text(json.dumps(snap, indent=1) + "\n")
        print(f"Wrote {path.relative_to(ROOT)} ({len(snap)} entries)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
