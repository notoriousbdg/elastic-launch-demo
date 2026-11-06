"""Every deployed workflow template must declare enabled: true.

Kibana creates a workflow without an explicit `enabled` field in a disabled
state for alert triggers, so the significant-event notification workflows never
ran when an alert fired.
"""
from pathlib import Path

import pytest
import yaml

WORKFLOWS = sorted((Path(__file__).resolve().parent.parent / "elastic_config" / "workflows").glob("*.yaml"))


@pytest.mark.parametrize("path", WORKFLOWS, ids=lambda p: p.name)
def test_workflow_declares_enabled(path):
    assert yaml.safe_load(path.read_text().replace("__", "x")).get("enabled") is True
