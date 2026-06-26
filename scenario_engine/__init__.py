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

import httpx
import yaml

from scenario_engine.schema_version import (
    CURRENT_SCHEMA_VERSION,
    effective_version,
    version_status,
)
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


def list_scenarios() -> list[dict[str, Any]]:
    """Return list of available scenarios with metadata for the selector UI."""
    _discover()
    return [
        {
            "id": s.scenario_id,
            "name": s.scenario_name,
            "description": s.scenario_description,
            "namespace": s.namespace,
            "icon": s.scenario_icon,
            "schema_version": s.schema_version,
            "effective_schema_version": effective_version(s.schema_version),
            "current_schema_version": CURRENT_SCHEMA_VERSION,
            "version_status": version_status(s.schema_version),
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

    # Annotate the result with version information for the caller / UI.
    imported = _registry.get(scenario_id)
    sv = imported.schema_version if imported else None
    status = version_status(sv)
    result["schema_version"] = sv
    result["current_schema_version"] = CURRENT_SCHEMA_VERSION
    result["version_status"] = status

    if status == "upgrade_required":
        result["warning"] = (
            f"Scenario '{scenario_id}' has schema version {sv!r} which is "
            f"incompatible with the current engine schema {CURRENT_SCHEMA_VERSION!r}. "
            "Run the upgrade-scenario skill to migrate it before launching."
        )
    elif status == "outdated_compatible":
        sv_label = sv if sv is not None else "<unversioned>"
        result["warning"] = (
            f"Scenario '{scenario_id}' is at schema version {sv_label!r} "
            f"(current: {CURRENT_SCHEMA_VERSION!r}). It will still launch, but "
            "consider running the upgrade-scenario skill to bring it up to date."
        )

    return result


# ---------------------------------------------------------------------------
# GitHub repo import
# ---------------------------------------------------------------------------

_GITHUB_REPO_RE = re.compile(
    r"https?://github\.com/([^/]+)/([^/]+?)(?:\.git)?(?:/tree/([^/\s]+))?/?$"
)


def fetch_github_zipball(repo_url: str, token: str | None = None) -> bytes:
    """Download a GitHub repository as a zip archive (in memory).

    Accepts URLs of the form:
        https://github.com/<owner>/<repo>
        https://github.com/<owner>/<repo>.git
        https://github.com/<owner>/<repo>/tree/<branch>

    If *token* is provided it is sent as ``Authorization: Bearer <token>`` and
    enables access to private repositories.  The token is used only for this
    request and is never stored.
    """
    m = _GITHUB_REPO_RE.match(repo_url.strip())
    if not m:
        raise ValueError(
            "Invalid GitHub URL. Expected: https://github.com/<owner>/<repo>"
        )
    owner, repo, ref = m.group(1), m.group(2), m.group(3) or ""

    api_url = f"https://api.github.com/repos/{owner}/{repo}/zipball/{ref}"
    headers: dict[str, str] = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "elastic-launch-demo",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    logger.debug("Fetching GitHub zipball: %s", api_url)
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        resp = client.get(api_url, headers=headers)

    if resp.status_code == 404:
        raise ValueError(
            "Repo not found or private. "
            "If it's a private repo, supply a GitHub token."
        )
    if resp.status_code in (401, 403):
        raise ValueError("GitHub access denied — check the token.")
    if resp.status_code != 200:
        raise ValueError(f"GitHub returned HTTP {resp.status_code}")

    return resp.content


def import_scenarios_from_archive(data: bytes) -> dict[str, Any]:
    """Import every scenario under the repo's top-level ``scenarios/`` folder.

    Expects a GitHub zipball layout where the entire tree is wrapped under a
    single ``<repo>-<sha>/`` prefix.  The ``scenarios/`` directory immediately
    under that prefix is scanned; each immediate subfolder that contains a
    ``scenario.yaml`` is imported.

    The repo itself stores plain, unversioned YAML files — no ``.zip`` inside
    the repo.  The zipball is only the in-memory download transport.

    Returns the ``reload_registry()`` result augmented with:
    - ``imported_ids``: list of scenario ids that were imported
    - ``imported``: count of imported scenarios
    """
    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        raise ValueError("Downloaded archive is not a valid zip file")

    with zf:
        names = zf.namelist()
        if not names:
            raise ValueError("Downloaded archive is empty")

        # Determine the single wrapper prefix, e.g. "myrepo-abc123/".
        top_dirs = {n.split("/")[0] for n in names if "/" in n}
        if len(top_dirs) != 1:
            raise ValueError(
                "Unexpected archive layout: expected a single top-level directory"
            )
        prefix = next(iter(top_dirs)) + "/"  # e.g. "myrepo-abc123/"
        scenarios_prefix = prefix + "scenarios/"  # e.g. "myrepo-abc123/scenarios/"

        # Find all immediate subfolders of scenarios/ that have a scenario.yaml.
        # Build a map: scenario_id -> [archive entry names for that scenario]
        scenario_entries: dict[str, list[str]] = {}
        for name in names:
            if not name.startswith(scenarios_prefix):
                continue
            rel = name[len(scenarios_prefix):]  # e.g. "gaming/scenario.yaml"
            if "/" not in rel:
                continue  # stray file directly under scenarios/
            scenario_id = rel.split("/")[0]
            if not scenario_id:
                continue
            scenario_entries.setdefault(scenario_id, []).append(name)

        if not scenario_entries:
            raise ValueError(
                "Repo has no top-level scenarios/ folder. "
                "Expected layout: scenarios/<scenario_id>/scenario.yaml"
            )

        resolved_base = _SCENARIOS_DIR.resolve()
        imported_ids: list[str] = []

        for scenario_id, entry_names in scenario_entries.items():
            if not _SAFE_ID.fullmatch(scenario_id):
                logger.warning("Skipping scenario with invalid id %r", scenario_id)
                continue

            yaml_key = scenarios_prefix + scenario_id + "/scenario.yaml"
            if yaml_key not in names:
                logger.warning("Skipping %r — no scenario.yaml found", scenario_id)
                continue

            # Compute the per-scenario path prefix inside the archive.
            sc_prefix = scenarios_prefix + scenario_id + "/"

            # Zip-slip guard: every rewritten target must stay inside _SCENARIOS_DIR.
            for entry in entry_names:
                if entry.endswith("/"):
                    continue  # directory entries, no output file
                rel = entry[len(sc_prefix):]
                target = (_SCENARIOS_DIR / scenario_id / rel).resolve()
                if not str(target).startswith(str(resolved_base) + "/"):
                    raise ValueError(
                        f"Zip entry would escape scenarios directory: {entry!r}"
                    )

            # Overwrite existing scenario folder.
            dest = _SCENARIOS_DIR / scenario_id
            if dest.exists():
                shutil.rmtree(dest)

            # Extract files, rewriting arcname from
            # <prefix>/scenarios/<id>/<rel>  →  <scenarios_dir>/<id>/<rel>
            for entry in entry_names:
                if entry.endswith("/"):
                    continue  # skip directory entries
                rel = entry[len(sc_prefix):]
                if not rel:
                    continue
                out_path = _SCENARIOS_DIR / scenario_id / rel
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_bytes(zf.read(entry))

            imported_ids.append(scenario_id)
            logger.debug("Imported scenario %r from archive", scenario_id)

        if not imported_ids:
            raise ValueError(
                "No valid scenarios found under scenarios/ "
                "(each subfolder needs a scenario.yaml)"
            )

    result = reload_registry()
    result["imported_ids"] = imported_ids
    result["imported"] = len(imported_ids)
    return result
