#!/usr/bin/env python3
"""Static check: generator sources mention what package SLO templates and ML modules read.

Loads ``slo_required_fields.json`` / ``ml_required_fields.json`` (from
``extract_package_templates.py``) and scans ``generators/<id>/**/*.py`` for the
fields, CloudWatch selector values (``MetricName``, ``stat``, ``Namespace``) and
groupBy / detector split fields. Same token rules as ``verify_alert_emit.py``.

It is a cheap pre-run gate. The proof is the runtime tests, which run the generator
and evaluate every template against its documents::

    python3 -m pytest -q tests/test_slo_template_emit.py tests/test_ml_module_emit.py -k <id>

Usage::

    python3 verify_template_emit.py --inventory-dir /tmp/<pkg> --generator-dir generators/<id>

Exit codes: 0 all tokens found (or nothing to check), 1 missing coverage, 2 usage error.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

# Selector fields whose *values* name what to emit (a MetricName must be emitted with
# exactly that name). data_stream.* values are routing, checked by the runtime tests.
_VALUE_FIELDS = {"MetricName", "stat", "Namespace", "attributes.percentile", "percentile"}


def _verify_alert_emit():
    spec = importlib.util.spec_from_file_location("verify_alert_emit", HERE / "verify_alert_emit.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def required_tokens(slos: list[dict], mls: list[dict]) -> dict[str, list[str]]:
    """``token -> [template ids]`` for every field / selector value the templates need."""
    need: dict[str, list[str]] = {}

    def add(token: str | None, owner: str) -> None:
        if token and not token.startswith(("data_stream.", "_")) and token != "@timestamp":
            need.setdefault(token, [])
            if owner not in need[token]:
                need[token].append(owner)

    for slo in slos:
        owner = f"SLO {slo['id']}"
        for name in slo.get("fields") or []:
            add(name, owner)
        for term in slo.get("required_terms") or []:
            if term.get("field") in _VALUE_FIELDS and "value" in term and "*" not in str(term["value"]):
                add(str(term["value"]), owner)
    for ml in mls:
        owner = f"ML {ml['id']}"
        for name in ml.get("fields") or []:
            add(name, owner)
    return need


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--inventory-dir", type=Path, required=True,
                    help="directory holding slo_required_fields.json / ml_required_fields.json")
    ap.add_argument("--generator-dir", type=Path, required=True)
    args = ap.parse_args(argv)
    if not args.generator_dir.is_dir():
        print(f"error: generator dir not found: {args.generator_dir}", file=sys.stderr)
        return 2

    def load(name: str) -> list[dict]:
        path = args.inventory_dir / name
        return json.loads(path.read_text()) if path.is_file() else []

    slos, mls = load("slo_required_fields.json"), load("ml_required_fields.json")
    vae = _verify_alert_emit()
    source = vae._read_sources(args.generator_dir)
    need = required_tokens(slos, mls)
    print(f"slo_templates={len(slos)} ml_modules={len(mls)} required_tokens={len(need)}")
    if not need:
        print("ok: no SLO / ML tokens to check")
        return 0
    missing = {token: owners for token, owners in need.items() if not vae._token_present(source, token)}
    # OTLP logs carry the level as the record's severityText (indexed as severity_text).
    if "severity_text" in missing and ("severityText" in source or "severity_text" in source):
        missing.pop("severity_text")
    # build_cloudwatch_metric() sets the Namespace / MetricName / stat columns centrally.
    if "build_cloudwatch_metric" in source:
        for token in ("Namespace", "MetricName", "stat"):
            missing.pop(token, None)
    for ml in mls:
        if ml.get("index_mismatch"):
            print(f"note: ML module {ml['id']} reads {ml['default_index']}, outside the package datasets "
                  f"{ml['discovery_datasets']}; the deployer points it at those datasets instead")
    if missing:
        print("MISSING SLO / ML emit coverage:")
        for token, owners in missing.items():
            print(f"  - {token}  ({', '.join(owners[:3])}{' …' if len(owners) > 3 else ''})")
        print("\nEmit these fields / values (do not edit package templates). Then run the runtime "
              "tests: tests/test_slo_template_emit.py and tests/test_ml_module_emit.py.")
        return 1
    print(f"ok: all {len(need)} SLO / ML tokens found in {args.generator_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
