"""Scenario registry — discovers and serves scenario implementations."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from scenarios.yaml_scenario import load_yaml_scenario

if TYPE_CHECKING:
    from scenarios.base import BaseScenario

logger = logging.getLogger("scenarios")

# Registry: scenario_id -> BaseScenario instance
_registry: dict[str, BaseScenario] = {}
_loaded = False

_SCENARIOS_DIR = Path(__file__).parent


def _evict_scenario_modules() -> None:
    """Remove cached scenario package modules so re-import picks up disk changes."""
    for name in list(sys.modules):
        if not name.startswith("scenarios."):
            continue
        parts = name.split(".")
        if len(parts) < 2:
            continue
        pkg = parts[1]
        if pkg and pkg != "base":
            del sys.modules[name]


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
    _evict_scenario_modules()
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
