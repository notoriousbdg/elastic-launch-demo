"""Validate scenario YAML files against the JSON Schemas in scenario_engine/schemas/.

    scenario.schema.json    scenarios/<id>/scenario.yaml
    service.schema.json     scenarios/<id>/services/*.yaml
    channel.schema.json     scenarios/<id>/channels/*.yaml
    value-spec.schema.json  value specs shared by all three

The schemas check shape (keys, types, step and value-spec forms). Cross-file
rules — counts, references between files, placeholder parity — stay in
scripts/verify_yaml_scenarios.py.
"""

from __future__ import annotations

import functools
import json
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

SCHEMAS_DIR = Path(__file__).resolve().parent / "schemas"


@functools.lru_cache(maxsize=1)
def _validators() -> dict[str, Draft202012Validator]:
    resources = []
    schemas: dict[str, dict] = {}
    for path in sorted(SCHEMAS_DIR.glob("*.schema.json")):
        schema = json.loads(path.read_text(encoding="utf-8"))
        schemas[path.name.removesuffix(".schema.json")] = schema
        resources.append((schema["$id"], Resource.from_contents(schema)))
    registry = Registry().with_resources(resources)
    return {
        name: Draft202012Validator(schema, registry=registry)
        for name, schema in schemas.items()
    }


def _format_error(rel: str, error) -> str:
    where = "/".join(str(p) for p in error.absolute_path) or "<root>"
    # anyOf/oneOf failures report every branch; the best-matching leaf is clearer.
    leaf = min(error.context, key=lambda e: len(e.absolute_path), default=None) if error.context else None
    message = error.message if leaf is None or len(error.message) < 200 else leaf.message
    return f"{rel}: {where}: {message[:300]}"


def validate_document(kind: str, doc: Any, rel: str = "") -> list[str]:
    """Validate one parsed YAML document of *kind* (scenario | service | channel)."""
    validator = _validators()[kind]
    errors = sorted(validator.iter_errors(doc), key=lambda e: list(e.absolute_path))
    return [_format_error(rel or kind, e) for e in errors]


def validate_scenario_dir(scenario_dir: Path) -> list[str]:
    """Return schema errors for every YAML file of the scenario in *scenario_dir*."""
    scenario_dir = Path(scenario_dir)
    files: list[tuple[str, Path]] = [("scenario", scenario_dir / "scenario.yaml")]
    files += [("service", p) for p in sorted((scenario_dir / "services").glob("*.yaml"))]
    files += [("channel", p) for p in sorted((scenario_dir / "channels").glob("*.yaml"))]

    errors: list[str] = []
    for kind, path in files:
        rel = path.relative_to(scenario_dir).as_posix()
        try:
            doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            errors.append(f"{rel}: cannot be read: {exc}")
            continue
        errors.extend(validate_document(kind, doc if doc is not None else {}, rel))
    return errors
