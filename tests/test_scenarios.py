"""Scenario integrity: channel files and generator wiring.

Runs scripts/verify_yaml_scenarios.py against every shipped scenario, and breaks a
copy of a real scenario to prove each guard actually fails.
"""
import importlib.util
import shutil
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
SCENARIOS = ROOT / "scenarios"
SCENARIOS_TESTING = ROOT / "scenarios_testing"


def _load_verify():
    spec = importlib.util.spec_from_file_location(
        "verify_yaml_scenarios", ROOT / "scripts" / "verify_yaml_scenarios.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


verify = _load_verify()


def _ids(base: Path) -> list[str]:
    return sorted(p.parent.name for p in base.glob("*/scenario.yaml"))


# ── channel_file_problems ──

def _channels_dir(tmp_path: Path, numbers) -> Path:
    d = tmp_path / "channels"
    d.mkdir()
    for n in numbers:
        (d / f"{n:02d}-slug.yaml").write_text("name: x\n")
    return d


def test_exactly_twenty_numbered_files_is_clean(tmp_path):
    assert verify.channel_file_problems(_channels_dir(tmp_path, range(1, 21))) == []


def test_nineteen_files_is_reported(tmp_path):
    problems = verify.channel_file_problems(_channels_dir(tmp_path, range(1, 20)))
    assert any("19 files" in p for p in problems)
    assert any("missing channel files" in p and "20" in p for p in problems)


def test_twenty_one_files_is_reported(tmp_path):
    d = _channels_dir(tmp_path, range(1, 21))
    (d / "05-another.yaml").write_text("name: x\n")
    problems = verify.channel_file_problems(d)
    assert any("21 files" in p for p in problems)
    assert any("duplicate channel number 05" in p and "05-another.yaml" in p for p in problems)


def test_duplicate_number_hides_a_gap_in_the_count(tmp_path):
    """20 files but slot 20 missing and slot 5 doubled: the count alone would pass."""
    d = _channels_dir(tmp_path, range(1, 20))
    (d / "05-another.yaml").write_text("name: x\n")
    problems = verify.channel_file_problems(d)
    assert not any("expected exactly 20" in p for p in problems)
    assert any("duplicate channel number 05" in p for p in problems)
    assert any("missing channel files" in p for p in problems)


def test_numbers_outside_range_and_unnumbered_files_are_reported(tmp_path):
    d = _channels_dir(tmp_path, range(1, 20))
    (d / "21-too-high.yaml").write_text("name: x\n")
    (d / "notes.yaml").write_text("name: x\n")
    problems = verify.channel_file_problems(d)
    assert any("outside 01-20" in p and "21" in p for p in problems)
    assert any("notes.yaml" in p for p in problems)


# ── shipped scenarios ──

@pytest.mark.parametrize("sid", _ids(SCENARIOS))
def test_shipped_scenario_has_no_verify_errors(sid):
    errors, _warnings = verify.check_scenario(sid)
    assert errors == []


@pytest.mark.parametrize("sid", _ids(SCENARIOS_TESTING))
def test_testing_scenario_has_exactly_twenty_channel_files(sid):
    assert verify.channel_file_problems(SCENARIOS_TESTING / sid / "channels") == []


def test_checked_in_host_ids_match_scenario_identity():
    """Covers scenarios/ and scenarios_testing/; check_scenario() alone does not."""
    assert verify.check_checked_in_host_identity() == []


def test_generators_have_no_shared_identity_literals():
    assert verify.check_generator_identity_literals() == []


# ── each guard fails when the scenario is broken ──

@pytest.fixture
def broken(tmp_path, monkeypatch):
    """A copy of the banking scenario that tests can damage, checked via verify."""
    shutil.copytree(SCENARIOS / "banking", tmp_path / "banking")
    monkeypatch.setattr(verify, "SCENARIOS_DIR", tmp_path)
    return tmp_path / "banking"


def _errors() -> list[str]:
    return verify.check_scenario("banking")[0]


def test_copy_of_a_clean_scenario_is_clean(broken):
    assert _errors() == []


def test_duplicate_channel_number_is_an_error(broken):
    shutil.copy(next((broken / "channels").glob("07-*.yaml")), broken / "channels" / "07-zzz-copy.yaml")
    errors = _errors()
    assert any("duplicate channel number 07" in e for e in errors)
    assert any("21 files" in e for e in errors)


def test_missing_channel_is_an_error(broken):
    next((broken / "channels").glob("20-*.yaml")).unlink()
    assert any("19 files" in e for e in _errors())


def test_channel_driving_a_disabled_generator_is_an_error(broken):
    path = next((broken / "channels").glob("01-*.yaml"))
    data = yaml.safe_load(path.read_text())
    data["generator_impact"] = {"aws_waf_otel": {"block_rate": 0.5}}
    path.write_text(yaml.safe_dump(data, sort_keys=False))

    errors = _errors()
    assert any("drives generator 'aws_waf_otel'" in e and "not enabled" in e for e in errors)


def test_enabling_that_generator_clears_the_error(broken):
    path = next((broken / "channels").glob("01-*.yaml"))
    data = yaml.safe_load(path.read_text())
    data["generator_impact"] = {"aws_waf_otel": {"block_rate": 0.5}}
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    scenario = broken / "scenario.yaml"
    text = scenario.read_text()
    scenario.write_text(text + "\ngenerators:\n  aws_waf_otel: true\n" if "\ngenerators:" not in text
                        else text.replace("generators:\n", "generators:\n  aws_waf_otel: true\n", 1))

    assert not any("drives generator" in e for e in _errors())


def test_unknown_generator_id_in_block_is_an_error(broken):
    scenario = broken / "scenario.yaml"
    text = scenario.read_text()
    scenario.write_text(text + "\ngenerators:\n  not_a_real_generator: true\n" if "\ngenerators:" not in text
                        else text.replace("generators:\n", "generators:\n  not_a_real_generator: true\n", 1))

    assert any("unknown generator id 'not_a_real_generator'" in e for e in _errors())


def test_infra_impact_with_unknown_key_is_an_error(broken):
    path = next((broken / "channels").glob("01-*.yaml"))
    data = yaml.safe_load(path.read_text())
    data["infra_impact"] = {"cpu_pct": 80, "not_a_spike": 1}
    path.write_text(yaml.safe_dump(data, sort_keys=False))

    errors = _errors()
    assert any("infra_impact key 'not_a_spike'" in e for e in errors)
    assert not any("infra_impact key 'cpu_pct'" in e for e in errors)


# ── business_impact drives the executive dashboard ──

def _first_channel(broken: Path) -> Path:
    return next((broken / "channels").glob("01-*.yaml"))


def _rewrite_channel(path: Path, mutate) -> None:
    data = yaml.safe_load(path.read_text())
    mutate(data)
    path.write_text(yaml.safe_dump(data, sort_keys=False))


def test_missing_business_impact_is_an_error(broken):
    _rewrite_channel(_first_channel(broken), lambda d: d.pop("business_impact"))
    assert any("no business_impact" in e for e in _errors())


def test_bare_string_business_impact_is_an_error(broken):
    _rewrite_channel(_first_channel(broken), lambda d: d.update(business_impact=["business.gmv_usd_per_min"]))
    assert any("must be a dict" in e for e in _errors())


def test_business_impact_with_unknown_kpi_is_an_error(broken):
    _rewrite_channel(
        _first_channel(broken),
        lambda d: d.update(business_impact=[{"kpi": "business.nope", "factor": {"uniform": [0.2, 0.4]}}]),
    )
    assert any("business.nope" in e for e in _errors())


def test_scaffolded_channel_targets_scaffolded_kpis():
    import scripts.scaffold_scenario as scaffold

    ch = {"slug": "x", "error_type": "XError", "affected_services": ["a"], "cascade_services": []}
    data = yaml.safe_load(scaffold._build_channel_yaml(1, ch))
    scaffolded = {field.replace("metrics.", "") for _, field in scaffold._KPI_SECTION_FIELD_PAIRS}
    assert data["business_impact"]
    assert {i["kpi"] for i in data["business_impact"]} <= scaffolded


def test_emitter_ignores_malformed_impact_and_stays_nominal(caplog):
    from types import SimpleNamespace

    from scenario_engine.yaml_scenario import emit_executive_business_metrics_if_eligible, load_yaml_scenario

    scenario = load_yaml_scenario(SCENARIOS / "banking")
    scenario._channel_registry[1] = {"business_impact": ["business.gmv_usd_per_min"]}
    sent = []
    otlp = SimpleNamespace(build_gauge=lambda n, v, u: {"name": n, "v": v}, send_metrics=lambda r, m: sent.extend(m))
    service = SimpleNamespace(
        SERVICE_NAME=scenario.executive_kpi_emitter_service_name,
        _ctx=SimpleNamespace(scenario=scenario, elastic_url=None, elastic_api_key=None),
        chaos_controller=SimpleNamespace(get_active_channels=lambda: [1]),
        otlp=otlp,
        resource={},
    )
    with caplog.at_level("WARNING"):
        emit_executive_business_metrics_if_eligible(service)
    assert sent
    assert "malformed business_impact" in caplog.text


# ── dashboards react in proportion to the fault's scope (checks 31-33) ──

def test_channel_without_entry_service_is_an_error(broken):
    _rewrite_channel(_first_channel(broken), lambda d: d.update(affected_services=["not-an-entry-service"]))
    assert any("no trace entry service" in e for e in _errors())


def test_green_business_impact_is_an_error(broken):
    def flatten(d):
        for impact in d["business_impact"]:
            impact.pop("value", None)
            impact["factor"] = {"uniform": [1.0, 1.01]}

    _rewrite_channel(_first_channel(broken), flatten)
    assert any("yellow/red KPI tiles" in e for e in _errors())


def test_business_impact_kpi_without_direction_is_an_error(broken):
    path = broken / "scenario.yaml"
    data = yaml.safe_load(path.read_text())
    for em in data["executive_kpi_emissions"]:
        em.pop("better", None)
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    assert any("better: lower|higher" in e for e in _errors())


def test_scalar_incr_key_state_is_an_error(broken):
    svc = next((broken / "services").glob("*.yaml"))
    data = yaml.safe_load(svc.read_text())
    data.setdefault("state", {})["_cnt"] = {"init": 0}
    data["steps"] = list(data.get("steps") or []) + [{"incr_key": {"state_key": "_cnt", "key": "k", "by": 1}}]
    svc.write_text(yaml.safe_dump(data, sort_keys=False))
    assert any("incr_key state '_cnt'" in e for e in _errors())


def test_scope_tiers_grow_with_scope():
    tiers = [verify.scope_tier({"affected_services": ["a"] * n, "cascade_services": []})[2:] for n in (1, 2, 4)]
    assert tiers == sorted(tiers) and tiers[0] != tiers[-1]


def test_topology_edge_to_undefined_service_is_an_error(broken):
    svc = next(p for p in (broken / "services").glob("*.yaml") if yaml.safe_load(p.read_text()).get("topology"))
    data = yaml.safe_load(svc.read_text())
    data["topology"].append(["no-such-service", "/api/v1/x", "POST"])
    svc.write_text(yaml.safe_dump(data, sort_keys=False))
    assert any("topology callee 'no-such-service'" in e for e in _errors())


def test_notes_placeholders_skip_shell_syntax():
    text = "curl -w '%{time_total}' https://x/{tenant} && echo ${HOME} @{ID=1} {{literal}}"
    assert verify._notes_placeholders(text) == {"tenant"}
