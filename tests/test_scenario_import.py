"""Tests for the zip-slip guards on scenario zip upload and GitHub archive import.

Schema and expr rejection on import are covered in test_scenario_schema.py and
test_expr.py.

Run with:
    python -m pytest tests/test_scenario_import.py -v
"""

from __future__ import annotations

import io
import zipfile

import pytest

import scenario_engine


@pytest.fixture()
def scenarios_tmpdir(tmp_path, monkeypatch):
    target = tmp_path / "installed"
    target.mkdir()
    monkeypatch.setattr(scenario_engine, "_SCENARIOS_DIR", target)
    scenario_engine._registry.clear()
    scenario_engine._loaded = False
    yield target
    scenario_engine._registry.clear()
    scenario_engine._loaded = False


def _zip(entries: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, text in entries.items():
            zf.writestr(name, text)
    return buf.getvalue()


def test_zip_upload_rejects_path_traversal(scenarios_tmpdir, tmp_path):
    data = _zip({"evil/scenario.yaml": "scenario_id: evil\n", "evil/../../escaped.txt": "x"})
    with pytest.raises(ValueError, match="escape"):
        scenario_engine.import_scenario_zip(data)
    assert not (tmp_path / "escaped.txt").exists()
    assert list(scenarios_tmpdir.iterdir()) == []


def test_github_archive_rejects_path_traversal(scenarios_tmpdir, tmp_path):
    data = _zip({
        "repo-abc/scenarios/evil/scenario.yaml": "scenario_id: evil\n",
        "repo-abc/scenarios/evil/../../../escaped.txt": "x",
    })
    with pytest.raises(ValueError, match="escape"):
        scenario_engine.import_scenarios_from_archive(data)
    assert not (tmp_path / "escaped.txt").exists()
    assert list(scenarios_tmpdir.iterdir()) == []


@pytest.mark.parametrize("entries,message", [
    ({"a/scenario.yaml": "x", "b/scenario.yaml": "x"}, "exactly one top-level directory"),
    ({"Bad Id/scenario.yaml": "x"}, "Invalid scenario id"),
    ({"good/readme.txt": "x"}, "does not contain"),
])
def test_zip_upload_rejects_bad_layout(scenarios_tmpdir, entries, message):
    with pytest.raises(ValueError, match=message):
        scenario_engine.import_scenario_zip(_zip(entries))


def test_not_a_zip(scenarios_tmpdir):
    with pytest.raises(ValueError, match="not a valid zip"):
        scenario_engine.import_scenario_zip(b"definitely not a zip")
