"""Tests that one failing deploy step doesn't abort the rest of deploy_all().

Run with:
    python -m pytest tests/test_deployer_resilience.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from elastic_config.deployer import ScenarioDeployer  # noqa: E402
from elastic_config.deployer_base import StepIdx  # noqa: E402
from scenario_engine import get_scenario  # noqa: E402

# Step methods in deploy_all() execution order (after the connectivity check).
STEP_METHODS = [
    ("_report_elastic_url_step", StepIdx.ES_URL),
    ("_derive_otlp_step", StepIdx.OTLP_ENDPOINT),
    ("_cleanup_all_scenarios_step", StepIdx.CLEANUP),
    ("_configure_platform_settings", StepIdx.PLATFORM_SETTINGS),
    ("_deploy_apm_rollup", StepIdx.APM_ROLLUP),
    ("_setup_apm_anomaly_detection", StepIdx.APM_ANOMALY),
    ("_deploy_workflows", StepIdx.WORKFLOWS),
    ("_deploy_knowledge_base", StepIdx.KNOWLEDGE_BASE),
    ("_deploy_tools", StepIdx.AI_TOOLS),
    ("_deploy_agent", StepIdx.AI_AGENT),
    ("_deploy_scenario_stream", StepIdx.STREAM_CREATE),
    ("_deploy_knowledge_indicators", StepIdx.KNOWLEDGE_INDICATORS),
    ("_deploy_significant_events", StepIdx.SIGNIFICANT_EVENTS),
    ("_deploy_data_views", StepIdx.DATA_VIEWS),
    ("_deploy_dashboard", StepIdx.DASHBOARDS),
    ("_deploy_alerting", StepIdx.ALERT_RULES),
    ("_deploy_ecs_log_backfill", StepIdx.ECS_LOG_BACKFILL),
    ("_deploy_logs_ml_jobs", StepIdx.LOGS_ML_JOBS),
    ("_deploy_slos", StepIdx.SLOS),
    ("_install_integrations", StepIdx.INTEGRATIONS),
    ("_finalize_apm_anomaly_detection", StepIdx.APM_ANOMALY),
]


@pytest.fixture()
def deployer(monkeypatch):
    """A deployer whose steps are stubs that record calls and mark themselves ok."""
    d = ScenarioDeployer(get_scenario("space"), "https://es.example", "https://kb.example", "key")
    calls: list[str] = []

    def stub(name: str, idx: StepIdx, takes_client: bool = True):
        def _step(*args):
            notify = args[-1]
            calls.append(name)
            d._step(idx).status = "ok"
            notify(d.progress)
        return _step

    monkeypatch.setattr(d, "_check_connectivity", stub("_check_connectivity", StepIdx.CONNECTIVITY_CHECK))
    for name, idx in STEP_METHODS:
        monkeypatch.setattr(d, name, stub(name, idx))
    monkeypatch.setattr(d, "_schedule_slo_refresh", lambda: calls.append("_schedule_slo_refresh"))
    d.calls = calls
    return d


def _boom(*args):
    raise RuntimeError("kibana exploded")


def test_all_steps_run_when_nothing_fails(deployer):
    progress = deployer.deploy_all()
    assert progress.finished and progress.error == ""
    assert deployer.calls == (
        ["_check_connectivity"] + [n for n, _ in STEP_METHODS] + ["_schedule_slo_refresh"]
    )


def test_failing_step_is_marked_and_later_steps_still_run(deployer, monkeypatch):
    monkeypatch.setattr(deployer, "_deploy_workflows", _boom)
    monkeypatch.setattr(deployer, "_deploy_dashboard", _boom)
    snapshots: list[dict] = []

    progress = deployer.deploy_all(callback=lambda p: snapshots.append(p.to_dict()))

    later = [n for n, _ in STEP_METHODS if n not in ("_deploy_workflows", "_deploy_dashboard")]
    assert [c for c in deployer.calls if c in later] == later
    assert "_schedule_slo_refresh" in deployer.calls

    workflows = progress.steps[StepIdx.WORKFLOWS]
    assert workflows.status == "failed"
    assert workflows.detail == "RuntimeError: kibana exploded"
    assert progress.steps[StepIdx.DASHBOARDS].status == "failed"
    assert progress.steps[StepIdx.AI_TOOLS].status == "ok"

    assert progress.finished
    assert progress.error == "2 step(s) failed: Deploy workflows, Import Kibana dashboards"
    assert snapshots[-1]["finished"] is True


def test_connectivity_failure_is_fatal(deployer, monkeypatch):
    monkeypatch.setattr(deployer, "_check_connectivity", _boom)
    progress = deployer.deploy_all()

    assert deployer.calls == []
    assert progress.finished
    assert progress.error == "kibana exploded"
