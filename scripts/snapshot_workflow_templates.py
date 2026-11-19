#!/usr/bin/env python3
"""Vendor the Elastic Workflow Template Library into elastic_config/workflows/library/.

The deployer installs library workflows that investigate an integration's data (today
the four AWS ``*-investigation-otel`` templates) whenever a scenario enables generators
that write the datasets the workflow's evidence step reads, and wires them to the
matching OOTB package alert rules (``elastic_config/workflow_templates.py``). It deploys
from this vendored copy, so a deploy never depends on GitHub and every template is
tested (``tests/test_workflow_template_eligibility.py``,
``tests/test_workflow_template_emit.py``) before it reaches a demo.

Re-run to pick up new or changed library workflows; ``SOURCE.json`` records the
upstream commit. Needs network.

Usage::

    python3 scripts/snapshot_workflow_templates.py            # main
    python3 scripts/snapshot_workflow_templates.py --ref <sha>
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "elastic_config" / "workflows" / "library"
REPO = "elastic/workflows"
PREFIX = "library/workflows/"


def _get(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "snapshot-workflow-templates"})
    return urllib.request.urlopen(request, timeout=60).read()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--ref", default="main", help="branch or commit of elastic/workflows")
    args = parser.parse_args()

    commit = json.loads(_get(f"https://api.github.com/repos/{REPO}/commits/{args.ref}"))["sha"]
    tree = json.loads(_get(f"https://api.github.com/repos/{REPO}/git/trees/{commit}?recursive=1"))
    paths = sorted(
        item["path"] for item in tree.get("tree", [])
        if item["path"].startswith(PREFIX) and item["path"].endswith((".yaml", ".yml"))
        and item["path"].count("/") == 3
    )
    if not paths:
        print("no library workflows found", file=sys.stderr)
        return 1

    OUT.mkdir(parents=True, exist_ok=True)
    for old in OUT.glob("*.y*ml"):
        old.unlink()
    slugs = []
    for path in paths:
        slug = path.split("/")[2]
        text = _get(f"https://raw.githubusercontent.com/{REPO}/{commit}/{path}")
        (OUT / f"{slug}.yaml").write_bytes(text)
        slugs.append(slug)
    (OUT / "SOURCE.json").write_text(json.dumps({
        "repo": f"https://github.com/{REPO}",
        "commit": commit,
        "fetched": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "workflows": slugs,
    }, indent=1) + "\n")
    print(f"Vendored {len(slugs)} workflows from {REPO}@{commit[:12]} into {OUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
