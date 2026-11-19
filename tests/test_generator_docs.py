"""The generators/faults reference in the docs site matches the generator SPECs.

``docker/docs/docs/create-scenario.md`` carries a generated block (between
``<!-- generators:start -->`` and ``<!-- generators:end -->``).  Adding a generator,
or changing an ``impact_keys`` entry, without regenerating it fails here.  Fix with:

    python scripts/gen_generator_docs.py
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "gen_generator_docs.py"
DOCS = ROOT.parent / "docs" / "docs" / "create-scenario.md"


def _load():
    spec = importlib.util.spec_from_file_location("gen_generator_docs", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.skipif(not DOCS.is_file(), reason="docs site not in this checkout")
def test_generator_docs_are_current():
    mod = _load()
    assert mod.build(DOCS.read_text()) == DOCS.read_text(), (
        "create-scenario.md is out of date; run: python scripts/gen_generator_docs.py"
    )


def test_every_generator_family_is_listed():
    """A new family would otherwise be dropped from the docs page."""
    from generator_engine.registry import GENERATORS

    mod = _load()
    known = {key for key, _, _ in mod.FAMILIES}
    assert {s.family for s in GENERATORS.values()} <= known


def test_alert_names_are_found_for_alert_docs():
    mod = _load()
    assert mod.alert_for('Drive alert "[Redis OTel] High eviction rate": set above 100.') == "[Redis OTel] High eviction rate"
    assert mod.alert_for("When >0, node not ready. Alert: [Kubernetes OTel] Node not ready.") == "[Kubernetes OTel] Node not ready"
    assert mod.alert_for("Only moves a dashboard.") == ""


def test_every_generator_has_a_readable_name():
    """The docs show NAMES[id]; a missing entry would show the raw id."""
    from generator_engine.registry import GENERATORS

    mod = _load()
    assert sorted(set(GENERATORS) - set(mod.NAMES)) == []
    assert sorted(set(mod.NAMES) - set(GENERATORS)) == []
