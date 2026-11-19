#!/usr/bin/env python3
"""Snapshot how every generator's package alert rules group rows into alerts.

For each generator that cites a package (``SOURCE_INTEGRATION``), read the cited
(installed) package's ``alerting_rule_template`` ES|QL and record, per alert name, the identity
columns Kibana builds alert ids from, the row-level filters ahead of the grouping,
and the rule's ``alertDelay``. ``tests/test_alert_identity_stable.py`` reads the
snapshot offline. Re-run after bumping a ``SOURCE_INTEGRATION``. Needs network.

Usage::

    python3 scripts/snapshot_alert_identity.py            # all generators
    python3 scripts/snapshot_alert_identity.py redis_otel # refresh one or more
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

from audit_package_coverage import (  # noqa: E402
    PACKAGE_VERSION,
    SKILL_SCRIPTS,
    SOURCE_CITATION,
    latest_version,
    package_zip,
)

OUT = ROOT / "tests" / "fixtures" / "alert_identity.json"


def _extractor():
    spec = importlib.util.spec_from_file_location(
        "extract_alert_esql_fields", SKILL_SCRIPTS / "extract_alert_esql_fields.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def cited_package(generator_id: str) -> str | None:
    source = (ROOT / "generators" / generator_id / "generator.py").read_text()
    citation = SOURCE_CITATION.search(source)
    found = PACKAGE_VERSION.search(citation.group(1)) if citation else None
    return found.group(1) if found else None


def package_alerts(package: str, extract) -> dict[str, dict]:
    from generator_engine.registry import package_versions

    version = package_versions().get(package) or latest_version(package)
    archive = package_zip(package, version)
    alerts: dict[str, dict] = {}
    for name in sorted(archive.namelist()):
        if "/alerting_rule_template/" not in name or not name.endswith(".json"):
            continue
        attrs = json.loads(archive.read(name)).get("attributes", {})
        esql = (attrs.get("params", {}).get("esqlQuery") or {}).get("esql")
        if not esql:
            continue
        alerts[attrs["name"]] = {
            "package": f"{package}@{version}",
            "from": extract.parse_esql(esql)["from_streams"],
            "interval": attrs.get("schedule", {}).get("interval"),
            "alert_delay": (attrs.get("alertDelay") or {}).get("active", 1),
            **extract.alert_identity(esql),
        }
    return alerts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("generators", nargs="*", help="generator ids (default: all)")
    args = parser.parse_args()

    from generator_engine.registry import GENERATORS

    extract = _extractor()
    snapshot = json.loads(OUT.read_text()) if OUT.exists() and args.generators else {}
    by_package: dict[str, dict[str, dict]] = {}
    for gid in sorted(args.generators or GENERATORS):
        package = cited_package(gid)
        if not package:
            continue
        if package not in by_package:
            by_package[package] = package_alerts(package, extract)
        for entry in snapshot.values():
            if gid in entry["generators"]:
                entry["generators"].remove(gid)
        for alert, entry in by_package[package].items():
            gids = snapshot.get(alert, {}).get("generators", [])
            snapshot[alert] = {"generators": sorted({*gids, gid}), **entry}
        print(f"{gid}: {len(by_package[package])} alerts from {package}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    snapshot = {k: v for k, v in sorted(snapshot.items()) if v["generators"]}
    OUT.write_text(json.dumps(snapshot, indent=1) + "\n")
    print(f"Wrote {OUT.relative_to(ROOT)} ({len(snapshot)} alerts)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
