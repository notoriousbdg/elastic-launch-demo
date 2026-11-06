"""Shared types and HTTP helper functions for the scenario deployer."""

from __future__ import annotations

import concurrent.futures
import json
import logging
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Callable, Optional

import httpx

logger = logging.getLogger("deployer")

# Indices written by elasticsearch.index workflow steps. Created at deploy so
# the Workflows editor can resolve them (it errors when the target does not
# exist yet, even though the step would auto-create the index at runtime).
WORKFLOW_INDEX_SUFFIXES = (
    "significant-events-audit",
    "remediation-audit",
    "escalation-audit",
    "remediation-queue",
    "daily-report-audit",
)


# ── Step index registry ─────────────────────────────────────────────────────

class StepIdx(IntEnum):
    """Canonical positions of each deploy step in DeployProgress.steps.

    Keep these in sync with the DeployStep list in ScenarioDeployer.deploy_all().
    Using an enum prevents the magic-number drift that broke _ecs_log_backfill's
    docstring (it said "step 14" but returned 15).
    """
    CONNECTIVITY_CHECK   = 0
    ES_URL               = 1
    OTLP_ENDPOINT        = 2
    CLEANUP              = 3
    PLATFORM_SETTINGS    = 4
    APM_ROLLUP           = 5
    WORKFLOWS            = 6
    KNOWLEDGE_BASE       = 7
    AI_TOOLS             = 8
    AI_AGENT             = 9
    STREAM_CREATE        = 10
    KNOWLEDGE_INDICATORS = 11
    SIGNIFICANT_EVENTS   = 12
    DATA_VIEWS           = 13
    DASHBOARDS           = 14
    ALERT_RULES          = 15
    ECS_LOG_BACKFILL     = 16
    APM_ANOMALY          = 17
    LOGS_ML_JOBS         = 18
    SLOS                 = 19
    INTEGRATIONS         = 20
    OOTB_ALERTS          = 21


# ── Progress reporting ──────────────────────────────────────────────────────

@dataclass
class DeployStep:
    name: str
    status: str = "pending"      # pending | running | ok | failed | skipped
    detail: str = ""
    items_total: int = 0
    items_done: int = 0


@dataclass
class DeployProgress:
    steps: list[DeployStep] = field(default_factory=list)
    finished: bool = False
    error: str = ""
    otlp_endpoint: str = ""

    def to_dict(self) -> dict:
        return {
            "finished": self.finished,
            "error": self.error,
            "otlp_endpoint": self.otlp_endpoint,
            "steps": [
                {
                    "name": s.name,
                    "status": s.status,
                    "detail": s.detail,
                    "items_total": s.items_total,
                    "items_done": s.items_done,
                }
                for s in self.steps
            ],
        }


ProgressCallback = Callable[[DeployProgress], None]


# ── HTTP helpers ────────────────────────────────────────────────────────────

def _kibana_headers(api_key: str, *, content_type: str | None = "application/json") -> dict[str, str]:
    headers = {
        "kbn-xsrf": "true",
        "x-elastic-internal-origin": "kibana",
        "Authorization": f"ApiKey {api_key}",
    }
    if content_type:
        headers["Content-Type"] = content_type
    return headers


def _es_headers(api_key: str, *, content_type: str | None = "application/json") -> dict[str, str]:
    headers = {"Authorization": f"ApiKey {api_key}"}
    if content_type:
        headers["Content-Type"] = content_type
    return headers


# ── Retry helper ────────────────────────────────────────────────────────────

# HTTP statuses that warrant a retry: lock contention (409), rate limit (429),
# and 5xx server errors.
_TRANSIENT_STATUSES = {409, 429, 500, 502, 503, 504}


def _retry_http(
    call: Callable[[], httpx.Response],
    *,
    attempts: int = 4,
    base_delay: float = 0.75,
    label: str = "",
) -> Optional[httpx.Response]:
    """Run an HTTP call with exponential-backoff retries on transient failures.

    Retries on httpx timeouts/network errors and on transient HTTP status codes
    (409 conflict from concurrent shared-resource mutation, 429, 5xx).

    Returns the final httpx.Response (which may still be an error status if all
    retries were exhausted), or None if every attempt raised an exception.
    """
    last_resp: Optional[httpx.Response] = None
    for attempt in range(attempts):
        try:
            resp = call()
            if resp.status_code not in _TRANSIENT_STATUSES:
                return resp
            last_resp = resp
            logger.warning(
                "%s returned HTTP %s (attempt %d/%d)",
                label or "request", resp.status_code, attempt + 1, attempts,
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            logger.warning(
                "%s raised %s (attempt %d/%d)",
                label or "request", exc.__class__.__name__, attempt + 1, attempts,
            )
        if attempt < attempts - 1:
            time.sleep(base_delay * (2 ** attempt))
    return last_resp


# ── Concurrent bulk indexer ─────────────────────────────────────────────────

# Default workers for deploy-time backfill. Conservative enough for an ESS
# small cluster; raise or lower via the `workers` kwarg if you see 429s.
_BULK_WORKERS = 6


class ConcurrentBulkIndexer:
    """Bulk-index docs into an ES data stream with N requests in flight.

    Feed docs via add(); full batches are dispatched to a thread pool as they
    fill. Call flush() (or use as a context manager) to drain the remainder,
    wait for all in-flight requests, and get the total inserted count.

    Usage::

        with ConcurrentBulkIndexer(url, key, "logs-foo-default", label="foo") as idx:
            for doc in docs:
                idx.add(doc)
            inserted = idx.flush()
    """

    def __init__(
        self,
        elastic_url: str,
        api_key: str,
        data_stream: str,
        *,
        batch_size: int = 500,
        workers: int = _BULK_WORKERS,
        label: str = "",
        timeout: float = 60.0,
        bulk_path: str | None = None,
    ):
        path = bulk_path if bulk_path is not None else f"{data_stream}/_bulk"
        self._url = f"{elastic_url.rstrip('/')}/{path.lstrip('/')}"
        self._headers = _es_headers(api_key, content_type="application/x-ndjson")
        self._batch_size = batch_size
        self._label = label or "bulk"
        self._buffer: list[tuple[dict, dict]] = []
        self._futures: list[concurrent.futures.Future] = []
        self._pool = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
        # httpx.Client is thread-safe; cap open connections to match the pool.
        self._client = httpx.Client(
            timeout=timeout,
            limits=httpx.Limits(
                max_connections=workers,
                max_keepalive_connections=workers,
            ),
        )

    # ── Public API ──────────────────────────────────────────────────────────

    def add(self, doc: dict, *, meta: dict | None = None) -> None:
        """Buffer a document; submit a bulk request when the batch is full.

        ``meta`` is the bulk action line. The default ``{"create": {}}`` targets
        the data stream in the request URL. Pass an ``index`` action when the
        document needs an explicit id.
        """
        self._buffer.append((meta or {"create": {}}, doc))
        if len(self._buffer) >= self._batch_size:
            self._submit(self._buffer)
            self._buffer = []   # new list — the old reference lives in the future

    def flush(self) -> int:
        """Submit remaining buffer, wait for all in-flight batches, return
        total docs successfully indexed. Safe to call multiple times (idempotent
        once buffer and futures are drained)."""
        if self._buffer:
            self._submit(self._buffer)
            self._buffer = []
        inserted = 0
        for future in concurrent.futures.as_completed(self._futures):
            try:
                inserted += future.result()
            except Exception as exc:
                logger.warning("%s batch raised: %s", self._label, exc)
        self._futures.clear()
        return inserted

    def close(self) -> None:
        self._pool.shutdown(wait=True)
        self._client.close()

    def __enter__(self) -> "ConcurrentBulkIndexer":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.flush()   # no-op if caller already flushed
        finally:
            self.close()
        return False

    # ── Internal ────────────────────────────────────────────────────────────

    def _submit(self, docs: list[tuple[dict, dict]]) -> None:
        future = self._pool.submit(self._send_batch, docs)
        self._futures.append(future)

    def _send_batch(self, docs: list[tuple[dict, dict]]) -> int:
        """Build NDJSON, POST to _bulk, return count of successfully written docs."""
        lines: list[str] = []
        for meta, doc in docs:
            lines.append(json.dumps(meta))
            lines.append(json.dumps(doc))
        body = "\n".join(lines) + "\n"

        resp = _retry_http(
            lambda: self._client.post(self._url, content=body, headers=self._headers),
            label=self._label,
        )
        if resp is None:
            logger.warning("%s: no response after retries, batch dropped", self._label)
            return 0
        if resp.status_code >= 300:
            logger.warning(
                "%s bulk failed: HTTP %d %s",
                self._label, resp.status_code, resp.text[:200],
            )
            return 0

        result = resp.json()
        items = result.get("items", [])
        ok = 0
        for item in items:
            op = next(iter(item), None)
            status = (item.get(op) or {}).get("status") if op else None
            if status in (200, 201):
                ok += 1
            elif result.get("errors") and op:
                err = (item.get(op) or {}).get("error")
                if err:
                    logger.warning(
                        "%s bulk error: %s", self._label, json.dumps(err)[:200]
                    )
                    break
        return ok
