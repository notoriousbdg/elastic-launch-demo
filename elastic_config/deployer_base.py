"""Shared types and HTTP helper functions for the scenario deployer."""

from __future__ import annotations

import concurrent.futures
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

import httpx

logger = logging.getLogger("deployer")


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

def _kibana_headers(api_key: str) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "kbn-xsrf": "true",
        "x-elastic-internal-origin": "kibana",
        "Authorization": f"ApiKey {api_key}",
    }


def _es_headers(api_key: str) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Authorization": f"ApiKey {api_key}",
    }


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
    ):
        self._url = f"{elastic_url}/{data_stream}/_bulk"
        self._headers = {
            "Content-Type": "application/x-ndjson",
            "Authorization": f"ApiKey {api_key}",
        }
        self._batch_size = batch_size
        self._label = label or "bulk"
        self._buffer: list[dict] = []
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

    def add(self, doc: dict) -> None:
        """Buffer a document; submit a bulk request when the batch is full."""
        self._buffer.append(doc)
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

    def _submit(self, docs: list[dict]) -> None:
        future = self._pool.submit(self._send_batch, docs)
        self._futures.append(future)

    def _send_batch(self, docs: list[dict]) -> int:
        """Build NDJSON, POST to _bulk, return count of successfully created docs."""
        lines: list[str] = []
        for doc in docs:
            lines.append('{"create":{}}')
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
        ok = sum(
            1 for item in items
            if item.get("create", {}).get("status") in (200, 201)
        )
        if result.get("errors"):
            for item in items:
                err = item.get("create", {}).get("error")
                if err:
                    logger.warning(
                        "%s bulk error: %s", self._label, json.dumps(err)[:200]
                    )
                    break
        return ok
