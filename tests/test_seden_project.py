"""seden creates Nightshift's service accounts from the serverless project.json; the
deployer then reuses them by name and role.  The two must stay in step."""

import json
from pathlib import Path

import pytest

from elastic_config.deployer_platform import PlatformMixin

PROJECT_JSON = Path(__file__).resolve().parents[3] / "elastic_stack" / "serverless" / "project.json"

if not PROJECT_JSON.is_file():
    # project.json lives in the o11y-launch-next demo, outside this folder; a standalone
    # copy of the container (the nightshift branch) does not have it.
    pytest.skip("o11y-launch-next/elastic_stack/serverless/project.json not present", allow_module_level=True)


def _uiam() -> dict:
    return json.loads(PROJECT_JSON.read_text())["uiam"]


def test_project_json_declares_the_accounts_the_deployer_reuses():
    expected = [
        {"name": f"nightshift_{kind}", "roles": [f"_nightshift_{kind}"], "description": description}
        for kind, description in PlatformMixin._NIGHTSHIFT_SA_DESCRIPTIONS.items()
    ]
    assert _uiam()["service_accounts"] == expected


def test_project_json_asks_for_no_uiam_api_key():
    # Binding the accounts works with the deploy API key; a printed UIAM key would be a
    # long-lived project admin credential in Vault that nothing here uses.
    uiam = _uiam()
    assert not uiam.get("api_key")
    assert "api_key_expiration" not in uiam
