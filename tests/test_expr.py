"""Contract tests for the telemetry-DSL expression evaluator (app.services.expr).

Scenario YAML can be uploaded over HTTP, so ``safe_eval`` must reject anything
outside the expression allow-list while still accepting every expression the
shipped scenarios use.
"""
import random
from pathlib import Path

import pytest
import yaml

from app.services.expr import _SAFE_TIME, make_safe_rand, safe_eval

ROOT = Path(__file__).resolve().parent.parent


def _env(**extra):
    return {"rand": make_safe_rand(random.Random(1)), "time": _SAFE_TIME, **extra}


class TestAccepts:
    @pytest.mark.parametrize(
        "expr,env,expected",
        [
            ("1 + 2 * 3", {}, 7),
            ("'A' if x > 1 else 'B'", {"x": 2}, "A"),
            ("60 - token_age_min", {"token_age_min": 15}, 45),
            ("DESKS[name]", {"DESKS": {"a": 1}, "name": "a"}, 1),
            ("ITEMS[_i % len(ITEMS)]", {"ITEMS": [10, 20], "_i": 3}, 20),
            ("ap.lower().replace('-', '_')", {"ap": "AP-1"}, "ap_1"),
            ("f'BATCH-{n:04d}'", {"n": 7}, "BATCH-0007"),
            ("f'{v:+.2f}'", {"v": 1.5}, "+1.50"),
            ("int(size * 0.5)", {"size": 9}, 4),
            ("a and not b", {"a": True, "b": False}, True),
            ("x in (1, 2, 3)", {"x": 2}, True),
        ],
    )
    def test_plain_expressions(self, expr, env, expected):
        assert safe_eval(expr, _env(**env)) == expected

    def test_rand_wrapper(self):
        v = safe_eval("rand.randint(1, 5)", _env())
        assert 1 <= v <= 5

    def test_time_wrapper(self):
        assert safe_eval("time.time()", _env()) > 0


class TestRejects:
    @pytest.mark.parametrize(
        "expr",
        [
            "().__class__.__base__.__subclasses__()",
            "''.__class__.__mro__",
            "rand._r",  # private attribute of the wrapper
            "x.__dict__",
            "'{0.__class__}'.format(x)",
            "'{a.__class__}'.format_map({'a': 1})",
            "__import__('os').system('true')",
            "(lambda: 1)()",
            "[c for c in (1, 2)]",
            "(c for c in (1, 2))",
            "(y := 1)",
            "open('/etc/passwd')",
            "eval('1')",
        ],
    )
    def test_escape_attempts(self, expr):
        with pytest.raises(ValueError):
            safe_eval(expr, _env(x=object()))

    def test_statements_are_not_expressions(self):
        with pytest.raises(ValueError):
            safe_eval("import os", _env())


def _collect_exprs(node, out):
    if isinstance(node, dict):
        for k, v in node.items():
            if k == "expr" and isinstance(v, str):
                out.append(v)
            else:
                _collect_exprs(v, out)
    elif isinstance(node, list):
        for v in node:
            _collect_exprs(v, out)


def _shipped_exprs():
    out: list[str] = []
    for base in ("scenarios", "scenarios_testing"):
        for path in sorted((ROOT / base).rglob("*.yaml")):
            _collect_exprs(yaml.safe_load(path.read_text()) or {}, out)
    return sorted(set(out))


def test_every_shipped_expression_passes_the_allow_list():
    """Names are unbound here, so only the validator (not eval) is exercised."""
    import ast

    from app.services.expr import _validate

    exprs = _shipped_exprs()
    assert exprs, "expected scenario YAML to contain expr: values"
    for e in exprs:
        _validate(ast.parse(e, mode="eval"))
