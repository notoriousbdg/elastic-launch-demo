"""Tests that a channel's infra_impact reaches the chaos controller and is validated.

Run with:
    python -m pytest tests/test_infra_impact.py -v
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from app.chaos.controller import ChaosController  # noqa: E402
from scenario_engine.yaml_scenario import load_yaml_scenario  # noqa: E402

BASELINE = {"cpu_pct": 0, "memory_pct": 0, "k8s_oom_intensity": 0, "latency_multiplier": 1.0}


@pytest.fixture()
def scenario_copy(tmp_path):
    """A copy of the space scenario whose channels can be edited."""
    dest = tmp_path / "space"
    shutil.copytree(REPO_ROOT / "scenarios" / "space", dest)
    return dest


def _set_infra_impact(scenario_dir: Path, channel: int, impact) -> None:
    path = next((scenario_dir / "channels").glob(f"{channel:02d}-*.yaml"))
    doc = yaml.safe_load(path.read_text())
    doc["infra_impact"] = impact
    path.write_text(yaml.safe_dump(doc))


def test_channel_registry_exposes_infra_impact(scenario_copy):
    _set_infra_impact(scenario_copy, 2, {"cpu_pct": 80, "latency_multiplier": 3.0})
    scenario = load_yaml_scenario(scenario_copy)
    assert scenario.channel_registry[2]["infra_impact"] == {"cpu_pct": 80, "latency_multiplier": 3.0}
    assert "infra_impact" not in scenario.channel_registry[3]


def test_trigger_applies_and_last_resolve_clears_spikes(scenario_copy):
    _set_infra_impact(scenario_copy, 2, {"cpu_pct": 80, "latency_multiplier": 3.0})
    _set_infra_impact(scenario_copy, 5, {"cpu_pct": 40, "memory_pct": 60})
    controller = ChaosController(channel_registry=load_yaml_scenario(scenario_copy).channel_registry)

    controller.trigger(2)
    assert controller.get_infra_spikes() == {**BASELINE, "cpu_pct": 80, "latency_multiplier": 3.0}

    controller.trigger(5)
    assert controller.get_infra_spikes() == {
        **BASELINE, "cpu_pct": 80, "memory_pct": 60, "latency_multiplier": 3.0,
    }

    controller.resolve(2, force=True)
    controller.resolve(5, force=True)
    assert controller.get_infra_spikes() == BASELINE


@pytest.mark.parametrize("impact,expected_error", [
    ({"cpu_pct": 50, "latency_multiplier": 2.5}, None),
    ({"cpu": 50}, "infra_impact key 'cpu'"),
    ({"cpu_pct": 150}, "infra_impact.cpu_pct=150"),
    ({"latency_multiplier": 0.5}, "infra_impact.latency_multiplier=0.5"),
    ({"memory_pct": "high"}, "infra_impact.memory_pct='high'"),
    ([80], "infra_impact must be a mapping"),
])
def test_verify_script_checks_infra_impact(scenario_copy, monkeypatch, impact, expected_error):
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    import verify_yaml_scenarios

    monkeypatch.setattr(verify_yaml_scenarios, "SCENARIOS_DIR", scenario_copy.parent)
    _set_infra_impact(scenario_copy, 2, impact)

    errors, _ = verify_yaml_scenarios.check_scenario("space")
    impact_errors = [e for e in errors if "infra_impact" in e]
    if expected_error is None:
        assert impact_errors == []
    else:
        assert len(impact_errors) == 1 and expected_error in impact_errors[0]
