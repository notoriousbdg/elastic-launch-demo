"""Expression evaluator and template formatter for the telemetry DSL.

Scenario YAML can be uploaded or imported from GitHub, so both expressions and
format templates are treated as untrusted input.

``safe_eval(expr, env)`` only evaluates expressions that pass
:func:`validate_expr`, an AST allowlist.  An empty ``__builtins__`` alone is
not a sandbox: attribute access such as ``().__class__.__base__`` reaches every
loaded class, so attribute access is limited to the methods listed in
:data:`_ALLOWED_ATTRS`.

``safe_format(template, params)`` replaces ``str.format`` / ``string.Formatter``
for YAML templates.  It refuses attribute fields (``{x.attr}``) and dunder
keys, which would otherwise let a template walk from an env object (e.g.
``rand``) to module globals and ``os.environ``.

Available in every expression
------------------------------
- Arithmetic / compare / bool / ternary / subscript (no ``**``)
- ``round``, ``int``, ``float``, ``len``, ``min``, ``max``, ``sum``, ``abs``,
  ``str``, ``list``, ``tuple``, ``bool``
- ``rand`` — :class:`_SafeRand` wrapper (choice, randint, uniform, gauss,
  sample, random)
- ``time`` — :class:`_SafeTime` wrapper (time.time())
- String methods ``lower``, ``upper``, ``replace``, ``strip``; dict methods
  ``get``, ``keys``, ``values``, ``items``
- Star-unpacking in function calls
- f-strings
- Any name bound in the env dict (sampled vars, constants, state keys, ch_*),
  except dunder names
"""
from __future__ import annotations

import _string
import ast
import functools
import random as _random
import string
import time as _time
from typing import Any, Mapping


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


class UnsafeExpressionError(ValueError):
    """Raised when an expression or template uses a construct outside the allowlist."""


# Node types an expression may contain.  Anything else (lambda, comprehensions,
# walrus, await, yield, ``**`` …) is rejected.
_ALLOWED_NODES: tuple[type[ast.AST], ...] = (
    ast.Expression,
    ast.Constant, ast.Name, ast.Load,
    ast.BinOp, ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod,
    ast.UnaryOp, ast.UAdd, ast.USub, ast.Not,
    ast.BoolOp, ast.And, ast.Or,
    ast.Compare, ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE,
    ast.In, ast.NotIn, ast.Is, ast.IsNot,
    ast.IfExp, ast.Subscript, ast.Slice,
    ast.Call, ast.keyword, ast.Starred, ast.Attribute,
    ast.JoinedStr, ast.FormattedValue,
    ast.List, ast.Tuple, ast.Dict, ast.Set,
)

# The only attributes an expression may touch: the _SafeRand / _SafeTime
# methods and a few read-only str / dict methods.
_ALLOWED_ATTRS: frozenset[str] = frozenset({
    "choice", "randint", "uniform", "gauss", "sample", "random",
    "time",
    "lower", "upper", "replace", "strip",
    "get", "keys", "values", "items",
})


def validate_expr(expr: str) -> None:
    """Raise :class:`UnsafeExpressionError` unless *expr* is within the allowlist."""
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise UnsafeExpressionError(f"expr syntax error: {expr!r} — {exc.msg}") from exc
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise UnsafeExpressionError(
                f"expr uses disallowed syntax {type(node).__name__}: {expr!r}"
            )
        if isinstance(node, ast.Name) and node.id.startswith("__"):
            raise UnsafeExpressionError(f"expr uses dunder name {node.id!r}: {expr!r}")
        if isinstance(node, ast.Attribute) and node.attr not in _ALLOWED_ATTRS:
            raise UnsafeExpressionError(f"expr uses disallowed attribute {node.attr!r}: {expr!r}")


@functools.lru_cache(maxsize=4096)
def _compile_expr(expr: str):
    validate_expr(expr)
    return compile(expr, "<expr>", "eval")


def safe_eval(expr: str, env: dict[str, Any]) -> Any:
    """Evaluate *expr* in a restricted context built from *env*.

    The expression must pass :func:`validate_expr`; it is then evaluated with
    *env* merged over :data:`_SAFE_BUILTINS` and an empty ``__builtins__``.

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
    UnsafeExpressionError
        If *expr* fails validation.
    ValueError
        If evaluation fails; wraps the original exception.
    """
    code = _compile_expr(expr)
    context = {**_SAFE_BUILTINS, **env}
    try:
        return eval(code, {"__builtins__": {}}, context)  # noqa: S307  (validated by validate_expr)
    except Exception as exc:
        raise ValueError(f"expr eval failed: {expr!r} — {exc}") from exc


class SafeFormatter(string.Formatter):
    """``string.Formatter`` that refuses attribute fields and dunder keys.

    ``{name}``, ``{name[key]}`` and format specs work as usual.  With
    ``keep_missing=True`` an unknown top-level name is left in the output as
    ``{name}`` instead of raising ``KeyError``.
    """

    def __init__(self, keep_missing: bool = False) -> None:
        self.keep_missing = keep_missing

    def get_field(self, field_name, args, kwargs):
        first, rest = _string.formatter_field_name_split(field_name)
        if isinstance(first, str) and first.startswith("__"):
            raise UnsafeExpressionError(f"template uses dunder name: {{{field_name}}}")
        try:
            obj = self.get_value(first, args, kwargs)
        except KeyError:
            if not self.keep_missing:
                raise
            return "{" + field_name + "}", first
        for is_attr, key in rest:
            if is_attr:
                raise UnsafeExpressionError(f"template uses attribute access: {{{field_name}}}")
            if isinstance(key, str) and key.startswith("__"):
                raise UnsafeExpressionError(f"template uses dunder key: {{{field_name}}}")
            obj = obj[key]
        return obj, first


_STRICT_FORMATTER = SafeFormatter()
_LENIENT_FORMATTER = SafeFormatter(keep_missing=True)


def safe_format(template: str, params: Mapping[str, Any], *, keep_missing: bool = False) -> str:
    """Format *template* with *params* using :class:`SafeFormatter`.

    ``keep_missing=False`` matches ``str.format(**params)``;
    ``keep_missing=True`` matches the previous ``SafeDict`` behaviour.
    """
    formatter = _LENIENT_FORMATTER if keep_missing else _STRICT_FORMATTER
    return formatter.vformat(template, (), params)


def find_exprs(node: Any) -> list[str]:
    """Return every ``expr:`` string found anywhere inside a loaded YAML document."""
    found: list[str] = []
    if isinstance(node, dict):
        for k, v in node.items():
            if k == "expr" and isinstance(v, str):
                found.append(v)
            else:
                found.extend(find_exprs(v))
    elif isinstance(node, list):
        for v in node:
            found.extend(find_exprs(v))
    return found


def make_safe_rand(r: _random.Random) -> _SafeRand:
    """Wrap *r* in a :class:`_SafeRand` for use in expressions."""
    return _SafeRand(r)
