"""Guards against reintroducing import-time scenario globals.

Run with:
    python -m pytest tests/test_no_global_scenario.py -v
"""

from __future__ import annotations

import importlib
import inspect
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

GENERATORS = [
    "trace_generator", "host_metrics_generator", "k8s_metrics_generator",
    "jvm_metrics_generator", "vpc_flow_generator", "raw_access_log_generator",
    "nginx_log_generator", "nginx_metrics_generator", "mysql_log_generator",
]


def test_importing_app_does_not_load_a_scenario():
    code = (
        "import app.config, app.main, scenario_engine;"
        + "".join(f"import log_generators.{g};" for g in GENERATORS)
        + "print(scenario_engine._loaded)"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], cwd=REPO_ROOT, capture_output=True, text=True, check=True,
    )
    assert out.stdout.strip() == "False"


@pytest.mark.parametrize("name", ["SERVICES", "CHANNEL_REGISTRY", "NAMESPACE", "MISSION_ID", "MISSION_NAME"])
def test_config_has_no_scenario_globals(name):
    import app.config

    assert not hasattr(app.config, name)


@pytest.mark.parametrize("module", GENERATORS)
def test_generator_requires_scenario_data(module):
    run = importlib.import_module(f"log_generators.{module}").run
    param = inspect.signature(run).parameters["scenario_data"]
    assert param.kind is inspect.Parameter.KEYWORD_ONLY
    assert param.default is inspect.Parameter.empty


def test_k8s_generator_does_not_mutate_scenario_clusters(monkeypatch):
    """run() used to filter database services out of the scenario's own cluster dicts."""
    import copy
    import threading

    from app.context import ScenarioContext
    from log_generators import k8s_metrics_generator
    from scenario_engine import get_scenario

    scenario = get_scenario("space")
    data = ScenarioContext.from_scenario(scenario).generator_data()
    # Mark one clustered service as a database so run() has something to filter.
    data["k8s_clusters"] = copy.deepcopy(data["k8s_clusters"])
    data["services"] = copy.deepcopy(data["services"])
    db = data["k8s_clusters"][0]["services"][0]
    data["services"][db]["subsystem"] = "database"
    before = copy.deepcopy(data["k8s_clusters"])

    class _Client:
        def __getattr__(self, name):
            return lambda *a, **k: None

    stop = threading.Event()
    stop.set()
    k8s_metrics_generator.run(_Client(), stop, scenario_data=data)
    assert data["k8s_clusters"] == before
