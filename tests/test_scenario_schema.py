"""Tests for the scenario JSON Schemas (scenario_engine/schemas/) and where they are enforced.

Run with:
    python -m pytest tests/test_scenario_schema.py -v
"""

from __future__ import annotations

import io
import shutil
import sys
import zipfile
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scenario_engine.schema_validation import validate_document, validate_scenario_dir  # noqa: E402

SCENARIO_DIRS = sorted(p.parent for p in (REPO_ROOT / "scenarios").glob("*/scenario.yaml"))


@pytest.mark.parametrize("scenario_dir", SCENARIO_DIRS, ids=lambda p: p.name)
def test_shipped_scenarios_match_schema(scenario_dir):
    assert validate_scenario_dir(scenario_dir) == []


def _load(rel: str) -> dict:
    return yaml.safe_load((REPO_ROOT / "scenarios" / "space" / rel).read_text())


def _first(kind_dir: str) -> str:
    return sorted(p.name for p in (REPO_ROOT / "scenarios" / "space" / kind_dir).glob("*.yaml"))[0]


SERVICE = f"services/{_first('services')}"
CHANNEL = f"channels/{_first('channels')}"


def _errors(kind: str, rel: str, mutate) -> list[str]:
    doc = _load(rel)
    mutate(doc)
    return validate_document(kind, doc, rel)


@pytest.mark.parametrize("kind,rel,mutate,expected", [
    ("scenario", "scenario.yaml", lambda d: d.update(theme={"bg": "#000"}), "theme"),
    ("scenario", "scenario.yaml", lambda d: d.pop("namespace"), "'namespace' is a required property"),
    ("scenario", "scenario.yaml", lambda d: d["k8s_clusters"][0].update(provider="ibm"), "k8s_clusters/0/provider"),
    ("service", SERVICE, lambda d: d.update(language="cobol"), "language"),
    ("service", SERVICE, lambda d: d["steps"].insert(0, {"metirc": {"name": "x", "value": 1}}), "steps/0"),
    ("service", SERVICE, lambda d: d["steps"].insert(0, {"sample": {"x": {"randint": [1, 2, 3]}}}), "steps/0"),
    ("service", SERVICE, lambda d: d["steps"].insert(0, {"metric": {"name": "x"}}), "steps/0"),
    ("service", SERVICE, lambda d: d.update(topology=[["svc", "/path"]]), "topology/0"),
    ("channel", CHANNEL, lambda d: d.pop("error_type"), "'error_type' is a required property"),
    ("channel", CHANNEL, lambda d: d.update(correlation_attr={"key": "a"}), "correlation_attr"),
    ("channel", CHANNEL, lambda d: d["fault_params"].update(when=yaml.safe_load("2026-01-01T00:00:00Z")),
     "fault_params/when"),
    ("channel", CHANNEL, lambda d: d.update(business_impact=[{"kpi": "business.x"}]), "business_impact/0"),
])
def test_schema_rejects(kind, rel, mutate, expected):
    errors = _errors(kind, rel, mutate)
    assert errors, "expected a schema error"
    assert any(expected in e for e in errors), errors


@pytest.mark.parametrize("kind,rel,mutate", [
    ("channel", CHANNEL, lambda d: d.update(correlation_attr={})),
    ("channel", CHANNEL, lambda d: d.update(infra_impact={"cpu_pct": 50})),
    ("service", SERVICE, lambda d: d["steps"].insert(0, {"sample": {"x": {"format": "{a:02x}", "a": {"randint": [0, 255]}}}})),
    ("service", SERVICE, lambda d: d["steps"].insert(0, {"sample": {"x": {"some": "literal", "dict": 1}}})),
])
def test_schema_accepts(kind, rel, mutate):
    assert _errors(kind, rel, mutate) == []


# ---------------------------------------------------------------------------
# Enforcement: import rejects, discovery warns, verify script errors
# ---------------------------------------------------------------------------

def _zip_of(scenario_dir: Path, scenario_id: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for path in sorted(scenario_dir.rglob("*.yaml")):
            zf.writestr(f"{scenario_id}/{path.relative_to(scenario_dir).as_posix()}", path.read_text())
    return buf.getvalue()


@pytest.fixture()
def broken_copy(tmp_path):
    """A copy of the space scenario (id 'schematest') with an unknown top-level key."""
    dest = tmp_path / "src" / "schematest"
    shutil.copytree(REPO_ROOT / "scenarios" / "space", dest)
    doc = yaml.safe_load((dest / "scenario.yaml").read_text())
    doc["scenario_id"] = "schematest"
    (dest / "scenario.yaml").write_text(yaml.safe_dump(doc))
    good = _zip_of(dest, "schematest")
    doc["not_a_real_key"] = 1
    (dest / "scenario.yaml").write_text(yaml.safe_dump(doc))
    return good, _zip_of(dest, "schematest")


@pytest.fixture()
def scenarios_tmpdir(tmp_path, monkeypatch):
    import scenario_engine

    target = tmp_path / "installed"
    target.mkdir()
    monkeypatch.setattr(scenario_engine, "_SCENARIOS_DIR", target)
    scenario_engine._registry.clear()
    scenario_engine._loaded = False
    yield target
    scenario_engine._registry.clear()
    scenario_engine._loaded = False


def test_zip_import_rejects_schema_violation(scenarios_tmpdir, broken_copy):
    from scenario_engine import import_scenario_zip

    good, bad = broken_copy
    import_scenario_zip(good)
    with pytest.raises(ValueError, match="does not match the schema.*not_a_real_key"):
        import_scenario_zip(bad)
    installed = yaml.safe_load((scenarios_tmpdir / "schematest" / "scenario.yaml").read_text())
    assert "not_a_real_key" not in installed


def test_discovery_loads_but_warns_on_schema_drift(scenarios_tmpdir, broken_copy, caplog):
    import scenario_engine

    _, bad = broken_copy
    with zipfile.ZipFile(io.BytesIO(bad)) as zf:
        zf.extractall(scenarios_tmpdir)
    result = scenario_engine.reload_registry()
    assert "schematest" in [s["id"] for s in result["scenarios"]]
    assert any("not_a_real_key" in r.getMessage() for r in caplog.records)


def test_verify_script_reports_schema_errors(tmp_path, monkeypatch):
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    import verify_yaml_scenarios

    dest = tmp_path / "space"
    shutil.copytree(REPO_ROOT / "scenarios" / "space", dest)
    doc = yaml.safe_load((dest / "scenario.yaml").read_text())
    doc["not_a_real_key"] = 1
    (dest / "scenario.yaml").write_text(yaml.safe_dump(doc))
    monkeypatch.setattr(verify_yaml_scenarios, "SCENARIOS_DIR", tmp_path)

    errors, _ = verify_yaml_scenarios.check_scenario("space")
    assert any(e.startswith("schema: scenario.yaml") and "not_a_real_key" in e for e in errors)


@pytest.mark.parametrize("text,expected", [
    ("{a} {b:02x} {c!r} {d[key]} {{escaped}}", {"a", "b", "c", "d"}),
    ("plain text", set()),
])
def test_placeholders(text, expected):
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    import verify_yaml_scenarios

    assert verify_yaml_scenarios._placeholders(text) == expected
