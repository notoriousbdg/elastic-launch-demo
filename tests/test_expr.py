"""Tests for the expression allowlist and template formatter in app/services/expr.py.

Run with:
    python -m pytest tests/test_expr.py -v
"""

from __future__ import annotations

import io
import random
import sys
import zipfile
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from app.services.expr import (  # noqa: E402
    UnsafeExpressionError,
    find_exprs,
    make_safe_rand,
    safe_eval,
    safe_format,
    validate_expr,
)

SCENARIOS_DIR = REPO_ROOT / "scenarios"


def _all_scenario_exprs() -> list[tuple[str, str]]:
    found = []
    for path in sorted(SCENARIOS_DIR.glob("*/**/*.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        for expr in find_exprs(doc):
            found.append((str(path.relative_to(REPO_ROOT)), expr))
    return found


SCENARIO_EXPRS = _all_scenario_exprs()


def _env() -> dict:
    return {"rand": make_safe_rand(random.Random(1)), "x": 5, "name": "Alpha"}


# ---------------------------------------------------------------------------
# validate_expr
# ---------------------------------------------------------------------------

def test_scenarios_have_exprs():
    assert len(SCENARIO_EXPRS) > 100


@pytest.mark.parametrize("path,expr", SCENARIO_EXPRS, ids=lambda v: v if len(v) < 40 else v[:40])
def test_every_scenario_expr_is_allowed(path, expr):
    validate_expr(expr)


@pytest.mark.parametrize("expr", [
    "().__class__.__base__.__subclasses__()",
    "rand.__init__.__globals__",
    "rand._r",
    "''.join(['a'])",
    "'{0.__class__}'.format(1)",
    "__import__('os')",
    "__builtins__",
    "(lambda: 1)()",
    "[c for c in 'abc']",
    "(y := 1)",
    "10 ** 10 ** 10",
    "x.real",
])
def test_unsafe_exprs_are_rejected(expr):
    with pytest.raises(UnsafeExpressionError):
        validate_expr(expr)
    with pytest.raises(UnsafeExpressionError):
        safe_eval(expr, _env())


def test_syntax_error_is_rejected():
    with pytest.raises(UnsafeExpressionError):
        validate_expr("1 +")


@pytest.mark.parametrize("expr,expected", [
    ("x * 2 + 1", 11),
    ("'hi' if x > 3 else 'lo'", "hi"),
    ("round(7 / 2)", 4),
    ("max(1, x, 3)", 5),
    ("name.lower().replace('a', 'o')", "olpho"),
    ("{'a': 1}.get('b', 2)", 2),
    ("f'{x:03d}-{name}'", "005-Alpha"),
    ("not (x % 2 == 0)", True),
])
def test_allowed_exprs_evaluate(expr, expected):
    assert safe_eval(expr, _env()) == expected


def test_single_underscore_env_names_are_allowed():
    assert safe_eval("sum(_counters.values())", {"_counters": {"a": 1, "b": 2}}) == 3


def test_rand_methods_work():
    value = safe_eval("rand.randint(1, 3)", _env())
    assert value in (1, 2, 3)


# ---------------------------------------------------------------------------
# safe_format
# ---------------------------------------------------------------------------

def test_format_specs_and_index_access():
    params = {"a": 255, "v": 1234.5, "sensor": {"unit": "psi"}}
    assert safe_format("{a:02x} {v:,.1f} {sensor[unit]}", params) == "ff 1,234.5 psi"


def test_strict_format_raises_on_missing_key():
    with pytest.raises(KeyError):
        safe_format("{missing}", {})


def test_keep_missing_leaves_placeholder():
    assert safe_format("a={a} b={b}", {"a": 1}, keep_missing=True) == "a=1 b={b}"
    assert safe_format("{d[k]}", {}, keep_missing=True) == "{d[k]}"


def test_escaped_braces_are_literal():
    assert safe_format("{{literal}} {a}", {"a": 1}) == "{literal} 1"


@pytest.mark.parametrize("template", [
    "{rand.choice.__func__.__globals__}",
    "{rand.__class__}",
    "{x.real}",
    "{d[__class__]}",
    "{__builtins__}",
])
def test_unsafe_templates_are_rejected(template):
    params = {**_env(), "d": {"__class__": "nope"}}
    with pytest.raises(UnsafeExpressionError):
        safe_format(template, params, keep_missing=True)


# ---------------------------------------------------------------------------
# Scenario loading and import
# ---------------------------------------------------------------------------

def _scenario_zip_with_expr(scenario_id: str, expr: str) -> bytes:
    """Copy the space scenario under a new id with one service step using *expr*."""
    src = SCENARIOS_DIR / "space"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for path in sorted(src.rglob("*.yaml")):
            rel = path.relative_to(src)
            text = path.read_text(encoding="utf-8")
            if rel.parts[0] == "services" and rel.name == sorted(
                p.name for p in (src / "services").glob("*.yaml")
            )[0]:
                doc = yaml.safe_load(text)
                doc.setdefault("steps", []).insert(0, {"sample": {"evil": {"expr": expr}}})
                text = yaml.safe_dump(doc)
            if rel.name == "scenario.yaml":
                doc = yaml.safe_load(text)
                doc["scenario_id"] = scenario_id
                text = yaml.safe_dump(doc)
            zf.writestr(f"{scenario_id}/{rel.as_posix()}", text)
    return buf.getvalue()


@pytest.fixture()
def scenarios_tmpdir(tmp_path, monkeypatch):
    import scenario_engine

    monkeypatch.setattr(scenario_engine, "_SCENARIOS_DIR", tmp_path)
    scenario_engine._registry.clear()
    scenario_engine._loaded = False
    yield tmp_path
    # Not monkeypatched: restoring _loaded=True over an emptied registry would
    # leave later tests with no scenarios. Force rediscovery from the real dir.
    scenario_engine._registry.clear()
    scenario_engine._loaded = False


def test_zip_import_rejects_unsafe_expr_and_keeps_existing(scenarios_tmpdir):
    from scenario_engine import import_scenario_zip

    import_scenario_zip(_scenario_zip_with_expr("evilscn", "rand.randint(1, 2)"))
    assert (scenarios_tmpdir / "evilscn" / "scenario.yaml").exists()

    with pytest.raises(ValueError, match="disallowed"):
        import_scenario_zip(_scenario_zip_with_expr("evilscn", "().__class__"))

    # The previously installed, valid copy is untouched.
    installed = (scenarios_tmpdir / "evilscn" / "services")
    assert all("__class__" not in p.read_text() for p in installed.glob("*.yaml"))


def test_discovery_skips_scenario_with_unsafe_expr(scenarios_tmpdir):
    import scenario_engine

    with zipfile.ZipFile(io.BytesIO(_scenario_zip_with_expr("badscn", "().__class__"))) as zf:
        zf.extractall(scenarios_tmpdir)

    result = scenario_engine.reload_registry()
    assert "badscn" not in [s["id"] for s in result["scenarios"]]
    assert any(e["package"] == "badscn" for e in result["errors"])
