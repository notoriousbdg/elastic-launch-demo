"""Generator registry.

Discovers :class:`~generator_engine.spec.GeneratorSpec` objects from every
``generators/<id>/`` sub-package.  Each sub-package exposes its spec via a
module-level ``SPEC`` variable in its ``__init__.py``.  The ``__init__.py`` must
import **only** ``generator_engine.spec``; it must not import ``app.*``,
``httpx``, or its own ``generator.py``.  This constraint keeps registry import
lightweight so ``deployer_integrations.py`` and ``verify_yaml_scenarios.py``
run on the host without a Docker environment.

CLI usage::

    python3 -m generator_engine.registry --list
    python3 -m generator_engine.registry --describe postgresql_otel
    python3 -m generator_engine.registry --describe all
"""

from __future__ import annotations

import importlib
import os
import re
import sys
from pathlib import Path
from typing import Any, Callable

from generator_engine.schema_version import (
    CURRENT_SCHEMA_VERSION,
    effective_version,
    version_status,
)
from generator_engine.spec import GeneratorSpec

# Generators content directory — PVC-mounted at /app/generators in k8s.
_GENERATORS_DIR = Path(
    os.environ.get("GENERATORS_DIR")
    or Path(__file__).resolve().parent.parent / "generators"
)

# ── Display order ─────────────────────────────────────────────────────────────
_DISPLAY_ORDER: list[str] = [
    "traces",
    "raw_access",
    "host_metrics",
    "kubernetes_otel",
    "nginx_otel",
    "aws_vpcflow_otel",
    "gcp_vpcflow_otel",
    "azure_vnetflow_otel",
    "mysql_otel",
    "nginx_metrics",
    "jvm",
]


def _display_order_key(item: tuple[str, GeneratorSpec]) -> tuple[int, int, str]:
    gen_id = item[0]
    if gen_id in _DISPLAY_ORDER:
        return (0, _DISPLAY_ORDER.index(gen_id), gen_id)
    return (1, 0, gen_id)


def _ensure_generators_on_path() -> None:
    """Make ``generators`` importable from ``_GENERATORS_DIR``."""
    root = str(_GENERATORS_DIR.parent)
    if root not in sys.path:
        sys.path.insert(0, root)
    # Ensure the package path points at the content dir (PVC or local).
    try:
        pkg = importlib.import_module("generators")
        pkg.__path__ = [str(_GENERATORS_DIR)]  # type: ignore[attr-defined]
    except Exception:
        # Package may not be importable yet; discovery creates imports per-id.
        pass


def _evict_generator_modules() -> None:
    """Drop loaded ``generators.*`` modules so rediscovery picks up disk changes."""
    to_drop = [
        name
        for name in list(sys.modules)
        if name == "generators" or name.startswith("generators.")
    ]
    for name in to_drop:
        del sys.modules[name]


def _discover() -> dict[str, GeneratorSpec]:
    """Scan generators/<id>/ sub-packages and collect their SPEC objects."""
    out: dict[str, GeneratorSpec] = {}
    _ensure_generators_on_path()
    if not _GENERATORS_DIR.is_dir():
        return out

    for child in sorted(_GENERATORS_DIR.iterdir()):
        if not child.is_dir() or child.name.startswith("_"):
            continue
        init_py = child / "__init__.py"
        if not init_py.is_file():
            continue
        try:
            mod = importlib.import_module(f"generators.{child.name}")
        except Exception as exc:
            # Surface later via reload errors; skip broken packages here.
            print(f"generator discovery: failed to import generators.{child.name}: {exc}")
            continue
        spec = getattr(mod, "SPEC", None)
        if not isinstance(spec, GeneratorSpec):
            continue
        if spec.id != child.name:
            raise ValueError(
                f"generators/{child.name}/__init__.py: "
                f"SPEC.id is {spec.id!r} but folder name is {child.name!r}"
            )
        out[spec.id] = spec
    return dict(sorted(out.items(), key=_display_order_key))


GENERATORS: dict[str, GeneratorSpec] = _discover()


def reload_registry() -> dict[str, Any]:
    """Clear and rediscover generator packages from disk.

    Returns a summary with added/removed ids, errors, and version metadata.
    """
    global GENERATORS
    before_ids = set(GENERATORS.keys())
    errors: list[str] = []

    _evict_generator_modules()
    try:
        GENERATORS = _discover()
    except Exception as exc:
        errors.append(str(exc))
        GENERATORS = {}

    after_ids = set(GENERATORS.keys())
    return {
        "generators": list_generators(),
        "added": sorted(after_ids - before_ids),
        "removed": sorted(before_ids - after_ids),
        "errors": errors,
        "count": len(GENERATORS),
        "current_schema_version": CURRENT_SCHEMA_VERSION,
    }


def list_generators() -> list[dict[str, Any]]:
    """Return generator metadata including schema version status."""
    result = []
    for spec in GENERATORS.values():
        sv = getattr(spec, "schema_version", None)
        result.append(
            {
                "id": spec.id,
                "family": spec.family,
                "core": spec.core,
                "default_enabled": spec.default_enabled,
                "schema_version": sv,
                "effective_schema_version": effective_version(sv),
                "current_schema_version": CURRENT_SCHEMA_VERSION,
                "version_status": version_status(sv),
            }
        )
    return result


def enabled_specs(scenario: Any) -> list[GeneratorSpec]:
    """Return the ordered list of enabled GeneratorSpecs for a scenario."""
    try:
        gen_block: dict[str, Any] = getattr(scenario, "generators", {}) or {}
    except Exception:
        gen_block = {}

    result = []
    enabled_ids: set[str] = set()
    for spec in GENERATORS.values():
        if spec.id in gen_block:
            cfg = gen_block[spec.id] or {}
            if isinstance(cfg, bool):
                enabled = cfg
            else:
                enabled = cfg.get("enabled", True)
        else:
            enabled = spec.default_enabled
        if enabled:
            result.append(spec)
            enabled_ids.add(spec.id)

    # [Azure OpenAI] Billing panels read metrics-azure.billing-*; start the
    # billing companion whenever OpenAI is on unless billing is explicitly off.
    if "azure_openai" in enabled_ids and "azure_billing" not in enabled_ids:
        billing = GENERATORS.get("azure_billing")
        if billing is not None:
            cfg = gen_block.get("azure_billing")
            explicitly_off = cfg is False or (
                isinstance(cfg, dict) and cfg.get("enabled") is False
            )
            if not explicitly_off:
                result.append(billing)

    return result


def load_entrypoint(spec: GeneratorSpec) -> Callable:
    """Import and return ``run`` from the generator module.

    The entrypoint signature is
    ``run(client, stop_event, *, scenario_data=None, chaos_controller=None)``.
    """
    _ensure_generators_on_path()
    mod = importlib.import_module(spec.module)
    return getattr(mod, "run")


_APP_ROOT = Path(__file__).resolve().parent.parent
_CITATION = re.compile(r"""^SOURCE_INTEGRATION\s*=\s*[\(\"']([^\n]*)""", re.M)
_PACKAGE_VERSION = re.compile(r"(?:packages/)?([a-z0-9_]+)@(\d[\w.\-]*)")


def cited_package_versions(spec: GeneratorSpec) -> dict[str, str]:
    """``{package: version}`` cited by the generator's ``SOURCE_INTEGRATION``.

    Read from the source text so the registry stays importable without ``app.*``.
    """
    path = _APP_ROOT / (spec.module.replace(".", "/") + ".py")
    try:
        match = _CITATION.search(path.read_text())
    except OSError:
        return {}
    return dict(_PACKAGE_VERSION.findall(match.group(1))) if match else {}


def package_versions(specs: list[GeneratorSpec] | None = None) -> dict[str, str]:
    """The package version each integration is pinned to: the one its generator cites.

    The deployer installs exactly this version (step 20) and the template fixtures are
    snapshotted from it, so the installed dashboards, alerts, SLOs and ML jobs are the
    ones the generator was built and tested against. ``tests/test_package_pins.py``
    keeps every installed package cited exactly once.
    """
    pins: dict[str, str] = {}
    for spec in specs if specs is not None else GENERATORS.values():
        cited = cited_package_versions(spec)
        for pkg in spec.integrations:
            if pkg in cited:
                pins.setdefault(pkg, cited[pkg])
    return pins


def integrations_for_scenario(scenario: Any) -> list[str]:
    """Return the ordered, deduplicated list of Elastic package names to install."""
    enabled = {s.id for s in enabled_specs(scenario)}
    seen: set[str] = set()
    result: list[str] = []

    def _add(pkg: str) -> None:
        if pkg not in seen:
            seen.add(pkg)
            result.append(pkg)

    for spec in GENERATORS.values():
        if spec.always_install:
            for pkg in spec.integrations:
                _add(pkg)

    for spec in GENERATORS.values():
        if spec.id in enabled and not spec.always_install:
            for pkg in spec.integrations:
                _add(pkg)

    return result


def _cli() -> None:
    import sys as _sys

    args = _sys.argv[1:]
    if not args:
        print("Usage: python3 -m generator_engine.registry --list")
        print("       python3 -m generator_engine.registry --describe <id|all>")
        _sys.exit(0)

    if args[0] == "--list":
        for gid in GENERATORS:
            spec = GENERATORS[gid]
            flag = " [core]" if spec.core else ""
            enabled = " [default on]" if spec.default_enabled else " [default off]"
            print(f"  {gid}{flag}{enabled}")
        return

    if args[0] == "--describe":
        target = args[1] if len(args) > 1 else "all"
        ids = list(GENERATORS) if target == "all" else [target]
        for gid in ids:
            if gid not in GENERATORS:
                print(f"Unknown generator id: {gid}")
                _sys.exit(1)
            spec = GENERATORS[gid]
            print(f"\n{'='*60}")
            print(f"Generator: {spec.id}")
            print(f"  Module      : {spec.module}")
            print(f"  Schema      : {spec.schema_version} ({version_status(spec.schema_version)})")
            print(f"  Client      : {spec.client}")
            print(f"  Core        : {spec.core}")
            print(f"  Default on  : {spec.default_enabled}")
            print(f"  Chaos mode  : {spec.chaos_mode}")
            print(f"  Signals     : {', '.join(spec.signals)}")
            print(f"  Datasets    : {', '.join(spec.datasets)}")
            if spec.integrations:
                print(f"  Packages    : {', '.join(spec.integrations)}")
            if spec.config_fields:
                print("  Config fields:")
                for name, meta in spec.config_fields.items():
                    req = " (required)" if meta.get("required") else ""
                    print(f"    {name}{req}: {meta.get('doc', '')}")
                    if meta.get("default") is not None:
                        print(f"      default: {meta['default']}")
            if spec.impact_keys:
                print("  Impact keys (generator_impact: block):")
                for key, meta in spec.impact_keys.items():
                    r = f"  range: {meta['range']}" if meta.get("range") else ""
                    print(f"    {key}: {meta.get('doc', '')}")
                    print(f"      default: {meta['default']}{r}")
        return

    print(f"Unknown flag: {args[0]}")
    _sys.exit(1)


if __name__ == "__main__":
    _cli()
