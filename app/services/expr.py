"""Expression evaluator for the telemetry DSL (internal tooling).

Provides ``safe_eval(expr, env)`` using Python ``eval()`` with an empty
``__builtins__`` dict so the most dangerous builtins (import, exec, open …)
are absent.  Suitable for team-authored YAML expressions; not hardened against
adversarial input.

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


def safe_eval(expr: str, env: dict[str, Any]) -> Any:
    """Evaluate *expr* in a restricted context built from *env*.

    Merges *env* with :data:`_SAFE_BUILTINS` and calls ``eval()`` with an
    empty ``__builtins__``.  Objects already in *env* (like ``rand``) can be
    accessed normally — attribute access on those objects is unrestricted.

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
        If compilation or evaluation fails; wraps the original exception.
    """
    context = {**_SAFE_BUILTINS, **env}
    try:
        return eval(  # noqa: S307  (internal tooling, env is team-authored)
            compile(expr, "<expr>", "eval"), {"__builtins__": {}}, context
        )
    except Exception as exc:
        raise ValueError(f"expr eval failed: {expr!r} — {exc}") from exc


def make_safe_rand(r: _random.Random) -> _SafeRand:
    """Wrap *r* in a :class:`_SafeRand` for use in expressions."""
    return _SafeRand(r)
