"""Configuration — environment settings only.

Nothing here loads a scenario: scenario data reaches the rest of the app
through ScenarioContext (app/context.py).
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Repo-root .env (not only cwd). Systemd/Uvicorn often start with a different
# cwd where a bare load_dotenv() would miss ./.env.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Order: project .env, then service user's ~/.env, then cwd. override=False: first file wins per key.
load_dotenv(_PROJECT_ROOT / ".env")
load_dotenv(Path.home() / ".env")
load_dotenv()  # cwd fallback for local dev

# ── Environment Configuration ──────────────────────────────────────────────
OTLP_ENDPOINT = os.getenv("OTLP_ENDPOINT", "")
OTLP_API_KEY = os.getenv("OTLP_API_KEY", "")
OTLP_AUTH_TYPE = os.getenv("OTLP_AUTH_TYPE", "ApiKey")  # "ApiKey" or "Bearer"

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_FROM = os.getenv("SMTP_FROM", "nova7-ops@mission-control.local")

APP_PORT = int(os.getenv("APP_PORT", "8080"))
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
CHANNEL_TIMEOUT = int(os.getenv("CHANNEL_TIMEOUT", "3600"))

# ── Active Scenario ───────────────────────────────────────────────────────
_active_scenario_raw = os.getenv("ACTIVE_SCENARIO", "")
# True only when ACTIVE_SCENARIO is explicitly provided via the environment.
ACTIVE_SCENARIO_SET = bool(_active_scenario_raw)
# Support comma-separated list; e.g. "space, fanatics" auto-deploys both.
ACTIVE_SCENARIO_LIST: list[str] = (
    [s.strip() for s in _active_scenario_raw.split(",") if s.strip()]
    if _active_scenario_raw
    else []
)
# Primary scenario (first in list, or "space" as the default fallback).
ACTIVE_SCENARIO = ACTIVE_SCENARIO_LIST[0] if ACTIVE_SCENARIO_LIST else "space"

# ── Auto-deploy Credentials (optional) ───────────────────────────────────────
# If set, the app will automatically deploy ACTIVE_SCENARIO_LIST on startup
# without requiring manual input via the UI.
KIBANA_URL = os.getenv("KIBANA_URL", "")
_kibana_proxy_raw = os.getenv("KIBANA_PROXY", "").strip().rstrip("/")
if _kibana_proxy_raw and not _kibana_proxy_raw.startswith(("http://", "https://")):
    _kibana_proxy_raw = "https://" + _kibana_proxy_raw
KIBANA_PROXY = _kibana_proxy_raw
ELASTIC_API_KEY = os.getenv("ELASTIC_API_KEY", "")
ELASTIC_URL = os.getenv("ELASTIC_URL", "")

# Severity Number Mapping (shared across all scenarios)
SEVERITY_MAP = {
    "TRACE": 1,
    "DEBUG": 5,
    "INFO": 9,
    "WARN": 13,
    "ERROR": 17,
    "FATAL": 21,
}
