"""Every generator impact key is driven by a channel in a gt-* test scenario.

Each gt-<generator> scenario wires one channel per impact key (an overflow scenario,
gt-<generator>-b, holds the rest when a generator has more than 15). A key nothing drives
can never trigger its alert, so a package bump that adds keys must wire them too.
"""
from collections import defaultdict
from pathlib import Path

import pytest
import yaml

from generator_engine.registry import GENERATORS

ROOT = Path(__file__).resolve().parent.parent


def _driven_keys() -> dict[str, set[str]]:
    driven: dict[str, set[str]] = defaultdict(set)
    for channel in (ROOT / "scenarios_testing").glob("*/channels/*.yaml"):
        impact = (yaml.safe_load(channel.read_text()) or {}).get("generator_impact") or {}
        for gid, keys in impact.items():
            driven[gid].update(keys or {})
    return driven


DRIVEN = _driven_keys()


@pytest.mark.parametrize("gid", sorted(g for g, spec in GENERATORS.items() if spec.impact_keys))
def test_every_impact_key_is_driven_by_a_gt_channel(gid):
    missing = sorted(set(GENERATORS[gid].impact_keys) - DRIVEN.get(gid, set()))
    assert not missing, f"{gid}: no gt-* channel drives {missing}"


@pytest.mark.parametrize("gid", sorted(DRIVEN))
def test_channels_only_drive_declared_keys(gid):
    """The other direction: a typo or a removed key would otherwise be silently ignored."""
    assert gid in GENERATORS, f"a channel drives unknown generator {gid!r}"
    unknown = sorted(DRIVEN[gid] - set(GENERATORS[gid].impact_keys))
    assert not unknown, f"{gid}: channels set keys the SPEC does not declare: {unknown}"
