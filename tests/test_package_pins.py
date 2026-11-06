"""Installed integration packages stay in sync with the generators that feed them.

Step 20 installs each package at the version its generator cites in
``SOURCE_INTEGRATION`` (``generator_engine.registry.package_versions``), not the newest
release, so the dashboards, alerts, SLOs and ML jobs a demo gets are the ones the
generator was built and tested against. Upgrading a package is therefore a generator
change: bump the citation, re-ground the generator (upgrade-telemetry-generator skill),
and refresh the fixtures — which this test enforces.
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

import pytest

from generator_engine.registry import GENERATORS, cited_package_versions, package_versions

FIXTURES = Path(__file__).resolve().parent / "fixtures"
PINS = package_versions()


def test_every_installed_package_is_pinned_by_its_generator():
    missing = sorted(
        (gid, pkg) for gid, spec in GENERATORS.items() for pkg in spec.integrations
        if pkg not in cited_package_versions(spec)
    )
    assert not missing, (
        f"generators install packages their SOURCE_INTEGRATION does not cite: {missing}; "
        'add "elastic/integrations packages/<pkg>@<version>" to the citation'
    )


def test_generators_agree_on_each_package_version():
    cited: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for gid, spec in GENERATORS.items():
        for pkg, version in cited_package_versions(spec).items():
            if pkg in spec.integrations:
                cited[pkg].add((version, gid))
    conflicts = {pkg: sorted(v) for pkg, v in cited.items() if len({ver for ver, _ in v}) > 1}
    assert not conflicts, f"one package, several cited versions: {conflicts}"


def _fixture_versions() -> list[tuple[str, str, str, str]]:
    """``(fixture, entry, package, version)`` for every snapshotted template."""
    out = []
    for name in ("slo_templates.json", "ml_modules.json", "alert_rules.json"):
        for key, entry in json.loads((FIXTURES / name).read_text()).items():
            out.append((name, key, entry["package"], entry["version"]))
    for key, entry in json.loads((FIXTURES / "alert_identity.json").read_text()).items():
        pkg, _, version = entry["package"].partition("@")
        out.append(("alert_identity.json", key, pkg, version))
    return out


@pytest.mark.parametrize("fixture", ["slo_templates.json", "ml_modules.json", "alert_rules.json", "alert_identity.json"])
def test_fixtures_were_snapshotted_from_the_pinned_version(fixture):
    stale = sorted(
        f"{pkg} {version} (pinned {PINS.get(pkg)}): {key}"
        for name, key, pkg, version in _fixture_versions()
        if name == fixture and pkg in PINS and version != PINS[pkg]
    )
    assert not stale, (
        f"{fixture} is from a different package version than the generator pins; run "
        "scripts/snapshot_package_templates.py and scripts/snapshot_alert_identity.py: "
        f"{stale[:5]}"
    )
