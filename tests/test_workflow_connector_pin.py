"""Unit tests for the workflow LLM connector-pin logic.

Tests:
- _apply_connector_pin: substitution and line-removal paths.
- _generate_workflow_yamls: template parsing with and without connector ids.
- _resolve_llm_connector_ids: connector discovery against a stubbed httpx.Client.
"""

from __future__ import annotations

import textwrap
from unittest.mock import MagicMock, patch

import yaml

from elastic_config.deployer_workflows import (
    _DEFAULT_HAIKU_CONNECTOR_ID,
    _DEFAULT_SONNET_CONNECTOR_ID,
    _apply_connector_pin,
)


# ---------------------------------------------------------------------------
# _apply_connector_pin
# ---------------------------------------------------------------------------

SAMPLE_STEP = textwrap.dedent(
    """\
    steps:
      - name: run_rca
        type: ai.agent
        agent-id: my-agent
        connector-id: "__SONNET_CONNECTOR_ID__"
        create-conversation: true
        with:
          message: "hello"
      - name: create_case_title
        type: ai.agent
        agent-id: my-agent
        connector-id: "__HAIKU_CONNECTOR_ID__"
        with:
          message: "title"
    """
)


class TestApplyConnectorPinSubstitute:
    def test_substitutes_token(self):
        result = _apply_connector_pin(SAMPLE_STEP, "__SONNET_CONNECTOR_ID__", "my-sonnet-id")
        assert "my-sonnet-id" in result
        assert "__SONNET_CONNECTOR_ID__" not in result

    def test_leaves_other_token_intact(self):
        result = _apply_connector_pin(SAMPLE_STEP, "__SONNET_CONNECTOR_ID__", "my-sonnet-id")
        assert "__HAIKU_CONNECTOR_ID__" in result

    def test_preserves_indentation(self):
        result = _apply_connector_pin(SAMPLE_STEP, "__SONNET_CONNECTOR_ID__", "my-sonnet-id")
        # The connector-id line must still be indented (at least one leading space),
        # matching the surrounding step keys (4 spaces in SAMPLE_STEP).
        for line in result.splitlines():
            if "connector-id:" in line and "my-sonnet-id" in line:
                assert line.startswith("    "), f"bad indentation: {line!r}"

    def test_result_is_valid_yaml(self):
        result = _apply_connector_pin(SAMPLE_STEP, "__SONNET_CONNECTOR_ID__", "my-sonnet-id")
        result2 = _apply_connector_pin(result, "__HAIKU_CONNECTOR_ID__", "my-haiku-id")
        parsed = yaml.safe_load(result2)
        assert parsed is not None


class TestApplyConnectorPinRemove:
    def test_removes_line_when_none(self):
        result = _apply_connector_pin(SAMPLE_STEP, "__SONNET_CONNECTOR_ID__", None)
        assert "__SONNET_CONNECTOR_ID__" not in result
        # The line should be gone, not left as an empty connector-id.
        for line in result.splitlines():
            stripped = line.strip()
            assert stripped != "connector-id:", f"orphan connector-id key left: {line!r}"

    def test_leaves_haiku_token_intact_when_removing_sonnet(self):
        result = _apply_connector_pin(SAMPLE_STEP, "__SONNET_CONNECTOR_ID__", None)
        assert "__HAIKU_CONNECTOR_ID__" in result

    def test_neighbouring_keys_survive(self):
        result = _apply_connector_pin(SAMPLE_STEP, "__SONNET_CONNECTOR_ID__", None)
        assert "agent-id: my-agent" in result
        assert "create-conversation: true" in result

    def test_result_is_valid_yaml_after_both_removals(self):
        result = _apply_connector_pin(SAMPLE_STEP, "__SONNET_CONNECTOR_ID__", None)
        result = _apply_connector_pin(result, "__HAIKU_CONNECTOR_ID__", None)
        parsed = yaml.safe_load(result)
        steps = parsed["steps"]
        for step in steps:
            assert "connector-id" not in step, f"step still has connector-id: {step}"

    def test_handles_no_match_gracefully(self):
        """A template without the token is returned unchanged."""
        content = "steps:\n  - name: foo\n    type: http\n"
        result = _apply_connector_pin(content, "__SONNET_CONNECTOR_ID__", None)
        assert result == content


# ---------------------------------------------------------------------------
# _generate_workflow_yamls (integration over the real template files)
# ---------------------------------------------------------------------------

def _make_mock_self(tmp_path=None):
    """Build a minimal mock deployer object for _generate_workflow_yamls."""
    import os

    mock = MagicMock()
    mock.ns = "test-ns"
    mock.kibana_display_url = "https://kibana.example.com"
    mock.scenario.scenario_name = "test-scenario"
    mock.scenario.agent_config = {"id": "test-agent"}
    return mock


class TestGenerateWorkflowYamls:
    """Test _generate_workflow_yamls against the real template files."""

    def _call(self, mock_self, **kwargs):
        from elastic_config.deployer_workflows import WorkflowsMixin
        return WorkflowsMixin._generate_workflow_yamls(mock_self, **kwargs)

    def test_unpinned_yields_valid_yaml_for_all_templates(self):
        mock_self = _make_mock_self()
        yamls = self._call(
            mock_self,
            sonnet_connector_id=None,
            haiku_connector_id=None,
        )
        assert yamls, "expected at least one workflow yaml"
        for name, content in yamls.items():
            parsed = yaml.safe_load(content)
            assert parsed is not None, f"{name}: yaml parse failed"

    def test_unpinned_has_no_llm_connector_id_on_ai_agent_steps(self):
        mock_self = _make_mock_self()
        yamls = self._call(
            mock_self,
            sonnet_connector_id=None,
            haiku_connector_id=None,
        )
        for name, content in yamls.items():
            parsed = yaml.safe_load(content)
            steps = parsed.get("steps", [])
            for step in steps:
                if step.get("type") == "ai.agent":
                    assert "connector-id" not in step, (
                        f"{name}: ai.agent step '{step.get('name')}' still has connector-id"
                    )

    def test_pinned_injects_correct_ids(self):
        mock_self = _make_mock_self()
        yamls = self._call(
            mock_self,
            sonnet_connector_id="my-sonnet",
            haiku_connector_id="my-haiku",
        )
        # Find at least one template that has ai.agent steps.
        found_sonnet = False
        found_haiku = False
        for name, content in yamls.items():
            parsed = yaml.safe_load(content)
            for step in parsed.get("steps", []):
                if step.get("type") == "ai.agent":
                    cid = step.get("connector-id", "")
                    if cid == "my-sonnet":
                        found_sonnet = True
                    if cid == "my-haiku":
                        found_haiku = True
        assert found_sonnet, "expected Sonnet connector-id in at least one ai.agent step"
        assert found_haiku, "expected Haiku connector-id in at least one ai.agent step"

    def test_no_unreplaced_placeholders(self):
        mock_self = _make_mock_self()
        for sonnet, haiku in [("my-sonnet", "my-haiku"), (None, None)]:
            yamls = self._call(
                mock_self,
                sonnet_connector_id=sonnet,
                haiku_connector_id=haiku,
            )
            for name, content in yamls.items():
                assert "__SONNET_CONNECTOR_ID__" not in content, f"{name}: unreplaced SONNET token"
                assert "__HAIKU_CONNECTOR_ID__" not in content, f"{name}: unreplaced HAIKU token"


# ---------------------------------------------------------------------------
# _resolve_llm_connector_ids
# ---------------------------------------------------------------------------

def _make_http_response(status_code: int, json_body):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body
    return resp


def _make_client(connectors: list | None = None, status_code: int = 200):
    """Return a stubbed httpx.Client whose GET returns the given connector list."""
    client = MagicMock()
    if connectors is None and status_code != 200:
        body = []
    else:
        body = connectors or []
    client.get.return_value = _make_http_response(status_code, body)
    return client


def _call_resolve(client):
    from elastic_config.deployer_workflows import WorkflowsMixin
    mock_self = MagicMock()
    mock_self.kibana_url = "https://kibana.example.com"
    mock_self.api_key = "test-key"
    # Patch _retry_http to pass through the call directly (no retry delay in tests).
    with patch("elastic_config.deployer_workflows._retry_http", side_effect=lambda fn, **kw: fn()):
        return WorkflowsMixin._resolve_llm_connector_ids(mock_self, client)


class TestResolveLlmConnectorIds:
    def test_exact_match_returns_ids(self):
        connectors = [
            {"id": _DEFAULT_SONNET_CONNECTOR_ID, "name": "Claude Sonnet"},
            {"id": _DEFAULT_HAIKU_CONNECTOR_ID, "name": "Claude Haiku"},
        ]
        client = _make_client(connectors)
        sonnet, haiku = _call_resolve(client)
        assert sonnet == _DEFAULT_SONNET_CONNECTOR_ID
        assert haiku == _DEFAULT_HAIKU_CONNECTOR_ID

    def test_substring_match_returns_ids(self):
        connectors = [
            {"id": "my-claude-sonnet-connector", "name": "My Claude Sonnet"},
            {"id": "my-claude-haiku-connector", "name": "My Claude Haiku"},
        ]
        client = _make_client(connectors)
        sonnet, haiku = _call_resolve(client)
        assert sonnet == "my-claude-sonnet-connector"
        assert haiku == "my-claude-haiku-connector"

    def test_no_claude_connectors_returns_none_none(self):
        connectors = [
            {"id": "elastic-cloud-email", "name": "Email"},
            {"id": "otel-demo-k8s", "name": "K8s"},
        ]
        client = _make_client(connectors)
        sonnet, haiku = _call_resolve(client)
        assert sonnet is None
        assert haiku is None

    def test_empty_connector_list_returns_none_none(self):
        client = _make_client([])
        sonnet, haiku = _call_resolve(client)
        assert sonnet is None
        assert haiku is None

    def test_http_error_returns_none_none(self):
        client = _make_client(status_code=500)
        sonnet, haiku = _call_resolve(client)
        assert sonnet is None
        assert haiku is None

    def test_network_exception_returns_none_none(self):
        client = MagicMock()
        client.get.side_effect = Exception("network error")
        sonnet, haiku = _call_resolve(client)
        assert sonnet is None
        assert haiku is None

    def test_partial_match_only_sonnet(self):
        """Only Sonnet is present; Haiku is absent — haiku should be None."""
        connectors = [
            {"id": _DEFAULT_SONNET_CONNECTOR_ID, "name": "Claude Sonnet"},
            {"id": "elastic-cloud-email", "name": "Email"},
        ]
        client = _make_client(connectors)
        sonnet, haiku = _call_resolve(client)
        assert sonnet == _DEFAULT_SONNET_CONNECTOR_ID
        assert haiku is None


def test_case_title_extracts_bold_span_and_forbids_commentary():
    """The title agent sometimes prefaces the title ("I'll generate ... **Title**").

    create_case takes the first **bold** span when present, and the prompt tells the
    agent not to add commentary.
    """
    from pathlib import Path

    wf_dir = Path(__file__).resolve().parent.parent / "elastic_config" / "workflows"
    for name in ("significant_event_notification", "significant_event_notification_auto_remediate"):
        text = (wf_dir / f"{name}.yaml").read_text()
        assert "split: '**'" in text, name
        assert "Do not add any preface" in text, name
