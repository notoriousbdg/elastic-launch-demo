"""Tests for app/launch.py (deploy + instance start) and ScenarioDeployer.probe_otlp.

Run with:
    python -m pytest tests/test_launch.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import app.launch as launch  # noqa: E402
from app.launch import Credentials, Launcher  # noqa: E402
from scenario_engine import get_scenario  # noqa: E402

CREDS = Credentials(kibana_url="https://kb", elastic_url="https://es", api_key="key", cloud_api_key="ck")


class FakeOTLP:
    def __init__(self) -> None:
        self.endpoints: list[str] = []

    def reconfigure(self, endpoint, api_key, auth_type="ApiKey", elastic_url=None):
        self.endpoints.append(endpoint)


class FakeInstance:
    created: list["FakeInstance"] = []
    fail_next = 0

    def __init__(self, ctx, chaos_store=None):
        if FakeInstance.fail_next:
            FakeInstance.fail_next -= 1
            raise RuntimeError("cannot start")
        self.ctx = ctx
        self.otlp = FakeOTLP()
        self.started = False
        FakeInstance.created.append(self)

    def start(self):
        self.started = True


class FakeRegistry:
    def __init__(self) -> None:
        self.registered: dict = {}

    def register(self, dep_id, inst):
        self.registered[dep_id] = inst


class FakeStore:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def upsert(self, **row):
        self.rows.append(row)


class FakeDeployer:
    def __init__(self, early: str | None, final: str = "") -> None:
        self.early, self.final = early, final

    def deploy_all(self, callback=None, on_otlp_ready=None):
        if self.early is not None:
            on_otlp_ready(self.early)
        return SimpleNamespace(otlp_endpoint=self.final)


@pytest.fixture()
def launcher(monkeypatch):
    FakeInstance.created = []
    FakeInstance.fail_next = 0
    monkeypatch.setattr(launch, "ScenarioInstance", FakeInstance)
    return Launcher(FakeRegistry(), FakeStore(), chaos_store=None)


SCENARIO = get_scenario("space")


def test_start_instance_registers_and_persists(launcher):
    inst = launcher.start_instance(SCENARIO, "dep", CREDS, "https://otlp")
    assert inst.started and inst.otlp.endpoints == ["https://otlp"]
    assert inst.ctx.elastic_url == "https://es"
    assert launcher._registry.registered == {"dep": inst}
    assert launcher._store.rows == [{
        "deployment_id": "dep", "scenario_id": "space", "otlp_endpoint": "https://otlp",
        "otlp_api_key": "key", "elastic_url": "https://es", "elastic_api_key": "key",
        "kibana_url": "https://kb", "cloud_api_key": "ck",
    }]


def test_early_start_keeps_instance_when_endpoint_unchanged(launcher):
    inst = launcher.deploy_and_start(FakeDeployer(early="https://a", final="https://a"), SCENARIO, "dep", CREDS)
    assert FakeInstance.created == [inst]
    assert inst.otlp.endpoints == ["https://a"]


def test_early_start_is_repointed_when_endpoint_changes(launcher):
    inst = launcher.deploy_and_start(FakeDeployer(early="https://a", final="https://b"), SCENARIO, "dep", CREDS)
    assert FakeInstance.created == [inst]
    assert inst.otlp.endpoints == ["https://a", "https://b"]


def test_override_wins_for_early_and_final(launcher):
    inst = launcher.deploy_and_start(
        FakeDeployer(early="https://a", final="https://b"), SCENARIO, "dep", CREDS,
        otlp_override="https://env",
    )
    assert inst.otlp.endpoints == ["https://env"]


@pytest.mark.parametrize("final,fallback,expected", [
    ("https://verified", "https://pre", "https://verified"),
    ("", "https://pre", "https://pre"),
])
def test_starts_after_deploy_when_hook_never_fires(launcher, final, fallback, expected):
    inst = launcher.deploy_and_start(
        FakeDeployer(early=None, final=final), SCENARIO, "dep", CREDS, otlp_fallback=fallback,
    )
    assert FakeInstance.created == [inst]
    assert inst.otlp.endpoints == [expected]
    assert launcher._store.rows[-1]["otlp_endpoint"] == expected


def test_retries_after_deploy_when_early_start_fails(launcher):
    FakeInstance.fail_next = 1
    inst = launcher.deploy_and_start(FakeDeployer(early="https://a", final="https://b"), SCENARIO, "dep", CREDS)
    assert inst is not None and inst.otlp.endpoints == ["https://b"]


def test_returns_none_when_instance_cannot_start(launcher):
    FakeInstance.fail_next = 2
    assert launcher.deploy_and_start(FakeDeployer(early="https://a"), SCENARIO, "dep", CREDS) is None
    assert launcher._registry.registered == {}


# ---------------------------------------------------------------------------
# probe_otlp
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status,expected", [(200, "https://otlp"), (401, "https://otlp"), (503, "")])
def test_probe_otlp_explicit(status, expected):
    from elastic_config.deployer import ScenarioDeployer

    deployer = ScenarioDeployer(SCENARIO, "https://es", "https://kb", "key")
    client = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(status)))
    assert deployer.probe_otlp(client, explicit="https://otlp") == expected


def test_probe_otlp_derives_when_not_explicit(monkeypatch):
    from elastic_config.deployer import ScenarioDeployer

    deployer = ScenarioDeployer(SCENARIO, "https://es", "https://kb", "key")
    monkeypatch.setattr(deployer, "_derive_otlp_endpoint", lambda client: None)
    assert deployer.probe_otlp(httpx.Client()) == ""
    monkeypatch.setattr(deployer, "_derive_otlp_endpoint", lambda client: "https://derived")
    assert deployer.probe_otlp(httpx.Client()) == "https://derived"
