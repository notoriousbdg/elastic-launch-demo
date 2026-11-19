#!/usr/bin/env python3
"""Drop entries from tests/fixtures/template_emit_pending.json whose gap is fixed.

Pending template-emit gaps run as strict xfail, so a fixed generator turns its test
into an XPASS failure. This re-runs every pending case (or those matching ``-k``) with
``--runxfail`` and removes the entries that now pass.

Usage::

    python3 scripts/prune_template_pending.py              # all pending cases
    python3 scripts/prune_template_pending.py -k aws_rds   # only matching node names
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PENDING = ROOT / "tests" / "fixtures" / "template_emit_pending.json"
TESTS = (
    "tests/test_slo_template_emit.py",
    "tests/test_ml_module_emit.py",
    "tests/test_workflow_template_emit.py",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("-k", default="", help="only prune node names containing this text")
    args = parser.parse_args()

    pending = json.loads(PENDING.read_text())
    names = [n for n in pending if args.k in n]
    if not names:
        print("nothing pending")
        return 0
    with tempfile.TemporaryDirectory() as tmp:
        report = Path(tmp) / "report.xml"
        node_ids = [f"{test}::{name}" for test in TESTS for name in names
                    if name.split("[", 1)[0] in (ROOT / test).read_text()]
        subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-W", "ignore",
             "--runxfail", f"--junitxml={report}", *node_ids],
            cwd=ROOT, capture_output=True, text=True,
        )
        passed = {
            case.get("name") for case in ET.parse(report).iter("testcase")
            if case.find("failure") is None and case.find("error") is None and case.find("skipped") is None
        }
    fixed = sorted(n for n in names if n in passed)
    for name in fixed:
        del pending[name]
    PENDING.write_text(json.dumps(dict(sorted(pending.items())), indent=1) + "\n")
    print(f"removed {len(fixed)} fixed, {len(pending)} still pending")
    for name in fixed:
        print(f"  fixed: {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
