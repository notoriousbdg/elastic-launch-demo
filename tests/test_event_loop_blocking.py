"""Tests that slow handlers don't block the event loop for other requests.

Run with:
    python -m pytest tests/test_event_loop_blocking.py -v
"""

from __future__ import annotations

import asyncio
import inspect
import sys
import time
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import app.main as main  # noqa: E402

SLOW = 1.0

# Handlers that do blocking network, file or thread-join work. They must be
# plain functions so FastAPI runs them in its threadpool.
BLOCKING_HANDLERS = [
    "chaos_page", "reload_scenarios", "download_scenario", "current_scenario",
    "stop_deployment", "remove_deployment", "test_connection", "launch_setup",
    "detect_existing", "teardown_setup", "stop_and_teardown",
]


@pytest.mark.parametrize("name", BLOCKING_HANDLERS)
def test_blocking_handlers_are_sync(name):
    assert not inspect.iscoroutinefunction(getattr(main, name))


async def _health_latency_while(request_coro) -> tuple[float, httpx.Response]:
    """Start *request_coro*, then time a /health call made while it is running.

    The clock starts before yielding to the slow request: if the handler blocks
    the event loop, even this coroutine's own sleep is delayed until it ends.
    """
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        started = time.monotonic()
        slow = asyncio.create_task(request_coro(client))
        await asyncio.sleep(0.1)
        health = await client.get("/health")
        latency = time.monotonic() - started
        slow_resp = await slow
    assert health.status_code == 200
    return latency, slow_resp


def test_teardown_does_not_block_other_requests(monkeypatch):
    import elastic_config.deployer as deployer_mod

    class SlowDeployer:
        def __init__(self, *args, **kwargs):
            pass

        def teardown(self):
            time.sleep(SLOW)
            return {"status": "torn down"}

    monkeypatch.setattr(deployer_mod, "ScenarioDeployer", SlowDeployer)
    monkeypatch.setattr(main, "_get_instance", lambda _id: None)
    monkeypatch.setattr(main, "_get_default_creds", lambda: ("https://es", "https://kb", "key"))
    monkeypatch.setattr(main, "_get_scenario_for_deployment", lambda _id: object())

    latency, resp = asyncio.run(_health_latency_while(
        lambda c: c.post("/api/setup/teardown", json={"deployment_id": "space"})
    ))
    assert resp.json() == {"status": "torn down"}
    assert latency < SLOW / 2


def test_smtp_send_does_not_block_other_requests(monkeypatch):
    import app.notify.email_handler as email_handler

    sent = []

    def slow_smtp(to, subject, body):
        time.sleep(SLOW)
        sent.append(to)

    monkeypatch.setattr(email_handler, "SMTP_HOST", "smtp.example")
    monkeypatch.setattr(email_handler, "_send_smtp", slow_smtp)

    latency, resp = asyncio.run(_health_latency_while(
        lambda c: c.post("/api/notify/email", json={"to": "a@example.com", "subject": "s", "body": "b"})
    ))
    assert resp.json()["sent"] is True and sent == ["a@example.com"]
    assert latency < SLOW / 2
