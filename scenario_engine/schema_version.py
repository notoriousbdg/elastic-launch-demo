"""Scenario schema-version helpers.

Loads ``scenario_engine/schema.yaml`` and exposes utilities for comparing a
scenario's version to the current engine version, computing its status, and
listing which migrations are pending.

Status values
-------------
up_to_date          Scenario is at the current schema version.
outdated_compatible Same MAJOR as current but older MINOR — runs fine, just
                    missing additive features added in later minor bumps.
upgrade_required    Scenario's MAJOR is older than the engine's MAJOR — the
                    scenario contract has changed in a breaking way and the
                    scenario must be migrated before it can be launched.
ahead               Scenario's MAJOR is *newer* than the engine's — the engine
                    is too old to load this scenario reliably.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

# ── Load schema registry ──────────────────────────────────────────────────────

_SCHEMA_FILE = Path(__file__).resolve().parent / "schema.yaml"

with _SCHEMA_FILE.open() as _f:
    _SCHEMA: dict[str, Any] = yaml.safe_load(_f)

#: The schema version all bundled scenarios are expected to be at.
CURRENT_SCHEMA_VERSION: str = _SCHEMA["current"]

#: Version assigned to scenarios that have no ``schema_version`` key.
_DEFAULT_UNVERSIONED: str = _SCHEMA.get("default_unversioned", "1.0")

#: Ordered list of migration entries (oldest → newest).
_MIGRATIONS: list[dict[str, Any]] = _SCHEMA.get("migrations", [])


# ── Version parsing ───────────────────────────────────────────────────────────


def parse_version(version_str: str) -> tuple[int, int]:
    """Parse a ``"MAJOR.MINOR"`` string into ``(major, minor)`` ints.

    Raises ``ValueError`` if the string is not in the expected format.
    """
    parts = str(version_str).split(".")
    if len(parts) != 2:
        raise ValueError(
            f"Schema version must be 'MAJOR.MINOR', got {version_str!r}"
        )
    try:
        return int(parts[0]), int(parts[1])
    except ValueError:
        raise ValueError(
            f"Schema version components must be integers, got {version_str!r}"
        )


# ── Effective version (handles None / missing) ────────────────────────────────


def effective_version(scenario_version: str | None) -> str:
    """Return the version string to use for comparisons.

    A scenario with no ``schema_version`` key (``None``) is treated as
    ``default_unversioned`` (currently ``"1.0"``), placing it in the same
    MAJOR series as the current engine.  This ensures un-tagged legacy scenarios
    are ``outdated_compatible``, not ``upgrade_required``.
    """
    return scenario_version if scenario_version is not None else _DEFAULT_UNVERSIONED


# ── Status computation ────────────────────────────────────────────────────────


def version_status(scenario_version: str | None) -> str:
    """Return the version status for a scenario.

    Parameters
    ----------
    scenario_version:
        The raw value of ``schema_version`` from ``scenario.yaml``, or
        ``None`` if the key is absent.

    Returns
    -------
    One of: ``"up_to_date"``, ``"outdated_compatible"``,
    ``"upgrade_required"``, ``"ahead"``.
    """
    eff = effective_version(scenario_version)
    s_major, s_minor = parse_version(eff)
    c_major, c_minor = parse_version(CURRENT_SCHEMA_VERSION)

    if s_major > c_major:
        return "ahead"
    if s_major < c_major:
        return "upgrade_required"
    # Same MAJOR
    if s_minor >= c_minor:
        return "up_to_date"
    return "outdated_compatible"


# ── Pending migrations ────────────────────────────────────────────────────────


def pending_migrations(scenario_version: str | None) -> list[dict[str, Any]]:
    """Return migrations that have not yet been applied to *scenario_version*.

    Migrations whose ``to`` version is strictly newer than the scenario's
    effective version are returned in order (oldest → newest), so the upgrade
    skill can apply them sequentially.
    """
    eff = effective_version(scenario_version)
    try:
        s_tuple = parse_version(eff)
    except ValueError:
        # Unparseable — return all migrations so the skill can fix it.
        return list(_MIGRATIONS)

    pending = []
    for entry in _MIGRATIONS:
        try:
            to_tuple = parse_version(entry["to"])
        except (KeyError, ValueError):
            continue
        if to_tuple > s_tuple:
            pending.append(entry)
    return pending
