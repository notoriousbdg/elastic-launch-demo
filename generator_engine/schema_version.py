"""Generator schema-version helpers.

Loads ``generator_engine/schema.yaml`` and exposes utilities for comparing a
generator's version to the current engine version, computing its status, and
listing which migrations are pending.

Status values match scenario_engine.schema_version:
up_to_date | outdated_compatible | upgrade_required | ahead
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

_SCHEMA_FILE = Path(__file__).resolve().parent / "schema.yaml"

with _SCHEMA_FILE.open() as _f:
    _SCHEMA: dict[str, Any] = yaml.safe_load(_f)

CURRENT_SCHEMA_VERSION: str = _SCHEMA["current"]
_DEFAULT_UNVERSIONED: str = _SCHEMA.get("default_unversioned", "1.0")
_MIGRATIONS: list[dict[str, Any]] = _SCHEMA.get("migrations", [])


def parse_version(version_str: str) -> tuple[int, int]:
    """Parse a ``"MAJOR.MINOR"`` string into ``(major, minor)`` ints."""
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


def effective_version(generator_version: str | None) -> str:
    """Return the version string to use for comparisons."""
    return generator_version if generator_version is not None else _DEFAULT_UNVERSIONED


def version_status(generator_version: str | None) -> str:
    """Return version status for a generator SPEC schema_version."""
    eff = effective_version(generator_version)
    s_major, s_minor = parse_version(eff)
    c_major, c_minor = parse_version(CURRENT_SCHEMA_VERSION)

    if s_major > c_major:
        return "ahead"
    if s_major < c_major:
        return "upgrade_required"
    if s_minor >= c_minor:
        return "up_to_date"
    return "outdated_compatible"


def pending_migrations(generator_version: str | None) -> list[dict[str, Any]]:
    """Return migrations not yet applied to *generator_version*."""
    eff = effective_version(generator_version)
    try:
        s_tuple = parse_version(eff)
    except ValueError:
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
