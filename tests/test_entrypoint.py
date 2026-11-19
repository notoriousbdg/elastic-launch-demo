"""entrypoint.sh generates /app/CLAUDE.md from a node script.

The script used to sit inside ``node -e "..."``. Inside double quotes the shell
runs every unescaped backtick pair as a command substitution, so the markdown's
backtick-quoted `$KIBANA_PROXY` rules were emptied and the shell printed
``syntax error near unexpected token`` at start-up. The script now lives in a
quoted heredoc, which expands nothing.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ENTRYPOINT = Path(__file__).resolve().parent.parent / "entrypoint.sh"
HEREDOC = re.compile(r"^node <<'JS'\n(?P<body>.*?)\nJS$", re.DOTALL | re.MULTILINE)


def _script() -> str:
    text = ENTRYPOINT.read_text()
    m = HEREDOC.search(text)
    assert m, "CLAUDE.md generator must be a quoted heredoc: node <<'JS' ... JS"
    return m.group("body")


def test_entrypoint_parses():
    assert subprocess.run(["bash", "-n", str(ENTRYPOINT)], capture_output=True).returncode == 0


def test_claude_md_generator_is_a_quoted_heredoc():
    script = _script()
    assert "/app/CLAUDE.md" in script


def test_no_double_quoted_node_script_contains_a_backtick():
    """A backtick inside any ``node -e "..."`` block would be run as a command."""
    text = ENTRYPOINT.read_text()
    for m in re.finditer(r'^node -e "\n(.*?)\n"$', text, re.DOTALL | re.MULTILINE):
        assert "`" not in m.group(1)


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_generated_claude_md_keeps_the_literal_kibana_rules(tmp_path):
    out = tmp_path / "CLAUDE.md"
    script = _script().replace("/app/CLAUDE.md", str(out))
    env = {
        **os.environ,
        "KIBANA_URL": "https://kb.example",
        "KIBANA_PROXY": "https://proxy.example",
        "ELASTICSEARCH_URL": "https://es.example",
        "INGEST_URL": "https://ingest.example",
    }
    r = subprocess.run(["node", "-"], input=script, text=True, env=env, capture_output=True)
    assert r.returncode == 0, r.stderr

    md = out.read_text()
    # the variable table is filled from the environment
    assert "`KIBANA_PROXY` | https://proxy.example |" in md
    # the rules keep their backtick-quoted variable names (these were emptied before)
    assert "must start with `$KIBANA_PROXY`." in md
    assert "Build links as `$KIBANA_PROXY` + the path" in md
    assert "- Dashboard: `$KIBANA_PROXY/app/dashboards#/view/<id>`" in md
    assert "- APM service: `$KIBANA_PROXY/app/apm/services/<name>`" in md
    assert "swap its origin for `$KIBANA_PROXY`" in md
    # the curl examples keep the literal variables and exactly one trailing backslash
    assert '  -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \\\n' in md
    assert 'curl -s \\\n' in md
    assert '"$KIBANA_URL/api/status"' in md
    assert "\\\\" not in md
