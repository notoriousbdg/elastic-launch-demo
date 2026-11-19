"""The SLO / ML template fixtures cover every package a generator installs.

``scripts/snapshot_package_templates.py`` records each package's ``slo_template`` and
``ml_module`` assets for the generators that install it. A generator whose package is
missing from the fixtures (new generator, new package, renamed id) would silently get no
SLO / ML emit tests, so it fails here until the snapshot is refreshed. Pending gaps and
reasoned skips must name real test cases.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from generator_engine.registry import GENERATORS

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures"
SLOS = json.loads((FIXTURES / "slo_templates.json").read_text())
MODULES = json.loads((FIXTURES / "ml_modules.json").read_text())
PENDING = json.loads((FIXTURES / "template_emit_pending.json").read_text())
SKIPS = json.loads((FIXTURES / "template_emit_skips.json").read_text())


def test_fixture_generators_exist_and_install_the_package():
    for kind, fixture in (("slo", SLOS), ("ml", MODULES)):
        for tid, entry in fixture.items():
            for gid in entry["generators"]:
                assert gid in GENERATORS, f"{kind} {tid}: unknown generator {gid}"
                assert entry["package"] in GENERATORS[gid].integrations, (
                    f"{kind} {tid}: {gid} no longer installs {entry['package']}; "
                    "run scripts/snapshot_package_templates.py"
                )


def test_every_installed_package_with_templates_is_snapshotted():
    snapshotted = {e["package"] for e in [*SLOS.values(), *MODULES.values()]}
    covered = {(g, e["package"]) for e in [*SLOS.values(), *MODULES.values()] for g in e["generators"]}
    missing = sorted(
        (gid, pkg) for gid, spec in GENERATORS.items() for pkg in spec.integrations
        if pkg in snapshotted and (gid, pkg) not in covered
    )
    assert not missing, f"refresh with scripts/snapshot_package_templates.py: {missing}"


def test_pending_and_skip_entries_name_real_cases():
    from elastic_config.workflow_templates import load_library

    library = {wf.slug: {s.name for s in wf.steps} for wf in load_library()}
    case = re.compile(r"^test_(slo_template|ml_\w+)\w*\[(?P<gid>\w+):(?P<tid>[^\]]+)\]$")
    workflow_case = re.compile(
        r"^test_workflow_step_finds_evidence_in_generator_documents\[(?P<slug>[\w-]+)\.(?P<step>\w+):(?P<gid>\w+)\]$"
    )
    for name in PENDING:
        wf = workflow_case.match(name)
        if wf:
            assert wf["step"] in library.get(wf["slug"], set()), f"pending entry names no workflow step: {name}"
            assert wf["gid"] in GENERATORS, f"pending entry names no generator: {name}"
            continue
        match = case.match(name)
        assert match, f"pending entry is not a template-emit test id: {name}"
        entry = SLOS.get(match["tid"]) or MODULES.get(match["tid"])
        assert entry and match["gid"] in entry["generators"], f"pending entry names no case: {name}"
    for kind, entries in SKIPS.items():
        if kind == "workflow":
            for key, entry in entries.items():
                slug, _, step = key.partition(".")
                assert step in library.get(slug, set()), f"skip for unknown workflow step {key}"
                assert len(entry.get("reason", "")) > 20, f"skip for {key} needs a real reason"
            continue
        fixture = SLOS if kind == "slo" else MODULES
        for tid, entry in entries.items():
            assert tid in fixture, f"skip for unknown {kind} template {tid}"
            assert len(entry.get("reason", "")) > 20, f"skip for {tid} needs a real reason"
