"""Guard for the gcp service DSL: `when:` belongs beside a step's `log:` / `metric:`, not inside it.

Inside the step body it is silently ignored, so both the healthy and the degraded line
of a pair were emitted in every state.
"""
from __future__ import annotations

from pathlib import Path

import yaml

SERVICES = Path(__file__).resolve().parent.parent / "scenarios" / "gcp" / "services"


def _nested_when(steps, path=""):
    for i, step in enumerate(steps or []):
        if not isinstance(step, dict):
            continue
        for kind in ("log", "metric", "sample", "counter"):
            body = step.get(kind)
            if isinstance(body, dict) and "when" in body:
                yield f"{path}{i}:{kind}"
        for key in ("steps", "else"):
            if isinstance(step.get(key), list):
                yield from _nested_when(step[key], f"{path}{i}.{key}.")


def test_when_is_never_nested_inside_a_step_body():
    bad = []
    for f in sorted(SERVICES.glob("*.yaml")):
        data = yaml.safe_load(f.read_text())
        bad += [f"{f.name} step {where}" for where in _nested_when(data.get("steps"))]
    assert not bad, bad
