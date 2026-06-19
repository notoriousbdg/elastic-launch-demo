"""Scenario registry — discovers and serves scenario implementations."""

from __future__ import annotations

import io
import logging
import os
import re
import shutil
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from scenario_engine.yaml_scenario import load_yaml_scenario

if TYPE_CHECKING:
    from scenario_engine.base import BaseScenario

logger = logging.getLogger("scenarios")

# Registry: scenario_id -> BaseScenario instance
_registry: dict[str, BaseScenario] = {}
_loaded = False

# Scenarios data directory — pure YAML, mounted as a PVC at /app/scenarios in k8s.
# An env override (SCENARIOS_DIR) allows tests and dev scripts to point elsewhere.
_SCENARIOS_DIR = Path(
    os.environ.get("SCENARIOS_DIR")
    or Path(__file__).resolve().parent.parent / "scenarios"
)


def _discover_impl() -> list[dict[str, str]]:
    """Scan scenarios/*/scenario.yaml and populate _registry. Returns load errors."""
    errors: list[dict[str, str]] = []

    for scenario_file in sorted(_SCENARIOS_DIR.glob("*/scenario.yaml")):
        pkg = scenario_file.parent.name
        try:
            scenario = load_yaml_scenario(scenario_file.parent)
            _registry[scenario.scenario_id] = scenario
            logger.debug("Registered scenario: %s", scenario.scenario_id)
        except (OSError, KeyError, yaml.YAMLError) as e:
            msg = str(e)
            logger.warning("Scenario %s not available: %s", pkg, msg)
            errors.append({"package": pkg, "error": msg})

    return errors


def _discover(force: bool = False) -> list[dict[str, str]]:
    """Auto-discover all scenarios under scenarios/*/scenario.yaml."""
    global _loaded
    if _loaded and not force:
        return []

    if force:
        _registry.clear()

    errors = _discover_impl()
    _loaded = True
    return errors


def reload_registry() -> dict[str, Any]:
    """Re-scan the filesystem and reload all scenarios from disk."""
    global _loaded

    before_ids = set(_registry.keys())
    _registry.clear()
    _loaded = False

    errors = _discover(force=True)
    after_ids = set(_registry.keys())

    return {
        "scenarios": list_scenarios(),
        "added": sorted(after_ids - before_ids),
        "removed": sorted(before_ids - after_ids),
        "errors": errors,
        "count": len(_registry),
    }


def get_scenario(scenario_id: str) -> BaseScenario:
    """Get a scenario by ID. Raises KeyError if not found."""
    _discover()
    if scenario_id not in _registry:
        available = ", ".join(_registry.keys()) or "(none)"
        raise KeyError(f"Unknown scenario '{scenario_id}'. Available: {available}")
    return _registry[scenario_id]


def list_scenarios() -> list[dict[str, str]]:
    """Return list of available scenarios with metadata for the selector UI."""
    _discover()
    return [
        {
            "id": s.scenario_id,
            "name": s.scenario_name,
            "description": s.scenario_description,
            "namespace": s.namespace,
            "icon": s.scenario_icon,
        }
        for s in sorted(_registry.values(), key=lambda s: s.sort_order)
    ]


_SAFE_ID = re.compile(r"^[a-z0-9_-]+$")


def export_scenario_zip(scenario_id: str) -> bytes:
    """Return a zip archive (bytes) of the scenario folder.

    The zip has a single top-level directory named after the scenario id so
    that it is self-describing and can be re-imported cleanly.
    """
    if not _SAFE_ID.fullmatch(scenario_id):
        raise ValueError(f"Invalid scenario id: {scenario_id!r}")

    folder = _SCENARIOS_DIR / scenario_id
    if not folder.is_dir():
        raise FileNotFoundError(f"Scenario directory not found: {folder}")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(folder.rglob("*")):
            # Skip byte-compiled artefacts
            if "__pycache__" in path.parts or path.suffix == ".pyc":
                continue
            if path.is_file():
                rel = path.relative_to(folder)
                zf.write(path, arcname=f"{scenario_id}/{rel}")

    return buf.getvalue()


def import_scenario_zip(data: bytes) -> dict[str, Any]:
    """Unpack a scenario zip into the scenarios directory and reload.

    The zip must have a single top-level directory whose name is a valid
    scenario id, and that directory must contain ``scenario.yaml``.  If a
    scenario with the same id already exists it is replaced.

    Returns the ``reload_registry()`` result augmented with an ``id`` key
    naming the imported scenario.
    """
    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        raise ValueError("Uploaded file is not a valid zip archive")

    with zf:
        names = zf.namelist()
        if not names:
            raise ValueError("Zip archive is empty")

        # Derive scenario id from the common top-level directory.
        top_dirs = {n.split("/")[0] for n in names if "/" in n}
        # Also handle files at root (no slash) — treat their dir as "."
        root_files = [n for n in names if "/" not in n]
        if root_files or len(top_dirs) != 1:
            raise ValueError(
                "Zip must contain exactly one top-level directory named after the scenario"
            )

        scenario_id = next(iter(top_dirs))
        if not _SAFE_ID.fullmatch(scenario_id):
            raise ValueError(f"Invalid scenario id in zip: {scenario_id!r}")

        # Require scenario.yaml
        if f"{scenario_id}/scenario.yaml" not in names:
            raise ValueError(
                f"Zip does not contain {scenario_id}/scenario.yaml — not a valid scenario archive"
            )

        # Zip-slip guard: every entry must resolve inside _SCENARIOS_DIR.
        resolved_base = _SCENARIOS_DIR.resolve()
        for entry in names:
            target = (_SCENARIOS_DIR / entry).resolve()
            if not str(target).startswith(str(resolved_base) + "/"):
                raise ValueError(f"Zip entry would escape scenarios directory: {entry!r}")

        # Overwrite existing scenario folder if present.
        dest = _SCENARIOS_DIR / scenario_id
        if dest.exists():
            shutil.rmtree(dest)

        zf.extractall(_SCENARIOS_DIR)

    result = reload_registry()
    result["id"] = scenario_id
    return result
