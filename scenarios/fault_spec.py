"""Randomization DSL interpreter for YAML scenario specs.

Scalar values are returned as-is.  Dict values whose first recognised key is
a DSL keyword are evaluated against the supplied RNG:

  {choice: [a, b, c]}              → rng.choice([a, b, c])
  {randint: [lo, hi]}              → rng.randint(lo, hi)
  {uniform: [lo, hi]}              → rng.uniform(lo, hi)
  {uniform: [lo, hi], round: n}    → round(rng.uniform(lo, hi), n)
  {random: true}                   → rng.random()
  {random: true, round: n}         → round(rng.random(), n)
  {format: "tmpl", k: spec, ...}   → "tmpl".format(**{k: resolve(spec, rng), ...})

Any other value (list, plain dict without a DSL key, str, int, float, bool,
or None) is returned verbatim.

The ``format`` primitive supports Python format-spec syntax inside the
template (e.g. ``"{n:02x}"`` or ``"{v:X}"``), since we delegate to
:meth:`str.format` after resolving argument values.

Example channel-file usage::

    fault_params:
      mac_address:
        format: "{a:02x}:{b:02x}:{c:02x}:{d:02x}:{e:02x}:{f:02x}"
        a: {randint: [0, 255]}
        b: {randint: [0, 255]}
        c: {randint: [0, 255]}
        d: {randint: [0, 255]}
        e: {randint: [0, 255]}
        f: {randint: [0, 255]}
      vlan_id: {choice: [100, 200, 300, 400, 500, 1000]}
      flap_count: {randint: [10, 50]}
      error_pct: {uniform: [80.0, 99.5], round: 1}
"""

from __future__ import annotations

import random as _random
from typing import Any

# Keys that trigger DSL dispatch when found in a dict.
_DSL_KEYS: frozenset[str] = frozenset({"choice", "randint", "uniform", "random", "format"})


def resolve(spec: Any, rng: _random.Random | None = None) -> Any:
    """Resolve *spec* to a concrete value using *rng*.

    Parameters
    ----------
    spec:
        A value from a YAML spec dict.  May be a scalar (returned verbatim),
        a list (returned verbatim), or a dict that optionally contains a DSL
        key.
    rng:
        A :class:`random.Random` instance.  If ``None`` a fresh (unseeded)
        instance is created — suitable for ``get_fault_params`` which wants
        true run-time randomness.
    """
    if rng is None:
        rng = _random.Random()

    if not isinstance(spec, dict):
        # Scalars and lists pass through unchanged.
        return spec

    # --- choice -------------------------------------------------------------
    if "choice" in spec:
        return rng.choice(spec["choice"])

    # --- randint ------------------------------------------------------------
    if "randint" in spec:
        lo, hi = spec["randint"]
        return rng.randint(int(lo), int(hi))

    # --- uniform ------------------------------------------------------------
    if "uniform" in spec:
        lo, hi = spec["uniform"]
        val: float = rng.uniform(float(lo), float(hi))
        if "round" in spec:
            val = round(val, int(spec["round"]))
        return val

    # --- random -------------------------------------------------------------
    if "random" in spec:
        val = rng.random()
        if "round" in spec:
            val = round(val, int(spec["round"]))
        return val

    # --- format -------------------------------------------------------------
    if "format" in spec:
        template: str = spec["format"]
        kwargs: dict[str, Any] = {
            k: resolve(v, rng) for k, v in spec.items() if k != "format"
        }
        return template.format(**kwargs)

    # Plain dict — no recognised DSL key — return verbatim.
    return spec


def resolve_dict(specs: dict[str, Any], rng: _random.Random | None = None) -> dict[str, Any]:
    """Resolve every value in *specs* and return a new dict.

    Convenience wrapper used by ``get_fault_params`` and ``get_rca_clues``.
    """
    if rng is None:
        rng = _random.Random()
    return {k: resolve(v, rng) for k, v in specs.items()}
