"""Expression evaluator for the telemetry DSL (internal tooling).

Provides ``safe_eval(expr, env)``: the expression is checked against an AST
allow-list (no lambdas, comprehensions, or underscore/``format`` attribute
access) and then evaluated with an empty ``__builtins__`` dict.

Available in every expression
------------------------------
- Arithmetic / compare / bool / ternary / subscript — all standard operators
- ``round``, ``int``, ``float``, ``len``, ``min``, ``max``, ``sum``, ``abs``,
  ``str``, ``list``, ``tuple``, ``bool``
- ``rand`` — :class:`_SafeRand` wrapper (choice, randint, uniform, gauss,
  sample, random)
- ``time`` — :class:`_SafeTime` wrapper (time.time())
- String methods — accessible naturally on str objects in env
- Star-unpacking in function calls (language feature, no builtins needed)
- f-strings — work through ``eval`` as normal Python expressions
- Any name bound in the env dict (sampled vars, constants, state keys, ch_*)
"""
from __future__ import annotations

import ast
import random as _random
import time as _time
from typing import Any


class _SafeRand:
    """Thin wrapper exposing only the random methods used in telemetry specs."""

    __slots__ = ("_r",)

    def __init__(self, r: _random.Random) -> None:
        self._r = r

    def choice(self, seq: Any) -> Any:
        return self._r.choice(seq)

    def randint(self, a: int, b: int) -> int:
        return self._r.randint(a, b)

    def uniform(self, a: float, b: float) -> float:
        return self._r.uniform(a, b)

    def gauss(self, mu: float, sigma: float) -> float:
        return self._r.gauss(mu, sigma)

    def sample(self, population: Any, k: int) -> list:
        return self._r.sample(population, k)

    def random(self) -> float:
        return self._r.random()


class _SafeTime:
    """Thin wrapper exposing only time.time()."""

    __slots__ = ()

    def time(self) -> float:
        return _time.time()


_SAFE_TIME = _SafeTime()

# Builtins available inside every expression.  ``__builtins__`` is empty so
# dangerous callables (import, exec, open, __import__ …) are not reachable.
_SAFE_BUILTINS: dict[str, Any] = {
    "__builtins__": {},
    "round": round,
    "int": int,
    "float": float,
    "len": len,
    "min": min,
    "max": max,
    "sum": sum,
    "abs": abs,
    "str": str,
    "list": list,
    "tuple": tuple,
    "bool": bool,
    "True": True,
    "False": False,
    "None": None,
}


# AST node types an expression may contain.  Anything else (Lambda, comprehensions,
# generator expressions, Await, Yield, NamedExpr, ...) is rejected before eval.
_ALLOWED_NODES: tuple[type[ast.AST], ...] = (
    ast.Expression, ast.Constant, ast.Name, ast.Load,
    ast.BinOp, ast.UnaryOp, ast.BoolOp, ast.Compare, ast.IfExp,
    ast.Subscript, ast.Slice, ast.Attribute, ast.Call, ast.keyword, ast.Starred,
    ast.Tuple, ast.List, ast.Dict, ast.Set,
    ast.JoinedStr, ast.FormattedValue,
    ast.operator, ast.unaryop, ast.boolop, ast.cmpop,
)

# ``str.format`` / ``format_map`` evaluate attribute lookups inside the format
# string ("{0.__class__}"), which bypasses the underscore check below.
_BLOCKED_ATTRS = frozenset({"format", "format_map"})


def _validate(tree: ast.AST) -> None:
    """Reject any construct outside the expression allow-list.

    Dunder/underscore attribute access is what every ``eval`` sandbox escape
    relies on (``().__class__.__base__.__subclasses__()``), so attributes that
    start with ``_`` are refused outright.
    """
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise ValueError(f"disallowed syntax: {type(node).__name__}")
        if isinstance(node, ast.Attribute) and (
            node.attr.startswith("_") or node.attr in _BLOCKED_ATTRS
        ):
            raise ValueError(f"disallowed attribute: {node.attr!r}")
        if isinstance(node, ast.Name) and node.id.startswith("__"):
            raise ValueError(f"disallowed name: {node.id!r}")


def safe_eval(expr: str, env: dict[str, Any]) -> Any:
    """Evaluate *expr* in a restricted context built from *env*.

    The expression is parsed and checked against an AST allow-list
    (:data:`_ALLOWED_NODES`) before it is evaluated with an empty
    ``__builtins__``; see :func:`_validate` for what is refused.  Scenario YAML
    can be uploaded over HTTP, so this has to hold for untrusted input.

    Parameters
    ----------
    expr:
        A Python expression string (no statements, no imports).
    env:
        The current telemetry evaluation context.  Should include ``rand``
        (_SafeRand), ``time`` (_SafeTime), ``active`` (bool), sampled vars,
        constants, state keys, and optionally ``ch_*`` channel fields.

    Raises
    ------
    ValueError
        If the expression is rejected, or compilation or evaluation fails.
    """
    context = {**_SAFE_BUILTINS, **env}
    try:
        tree = ast.parse(expr, "<expr>", mode="eval")
        _validate(tree)
        return eval(  # noqa: S307  (AST-validated above)
            compile(tree, "<expr>", "eval"), {"__builtins__": {}}, context
        )
    except Exception as exc:
        raise ValueError(f"expr eval failed: {expr!r} — {exc}") from exc


def make_safe_rand(r: _random.Random) -> _SafeRand:
    """Wrap *r* in a :class:`_SafeRand` for use in expressions."""
    return _SafeRand(r)
