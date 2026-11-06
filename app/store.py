"""DeploymentStore — SQLite persistence for active deployments.

Survives app restarts so active deployments can be restored from the DB
rather than being lost when the process recycles.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from typing import Any

logger = logging.getLogger("nova7.store")

_DB_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
_DB_PATH = os.path.join(_DB_DIR, "deployments.db")

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS deployments (
    deployment_id   TEXT PRIMARY KEY,
    scenario_id     TEXT NOT NULL,
    otlp_endpoint   TEXT NOT NULL DEFAULT '',
    otlp_api_key    TEXT NOT NULL DEFAULT '',
    elastic_url     TEXT NOT NULL DEFAULT '',
    elastic_api_key TEXT NOT NULL DEFAULT '',
    kibana_url      TEXT NOT NULL DEFAULT '',
    cloud_api_key   TEXT NOT NULL DEFAULT '',
    status          TEXT NOT NULL DEFAULT 'active',
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);
"""

_MIGRATE_CLOUD_API_KEY = """
ALTER TABLE deployments ADD COLUMN cloud_api_key TEXT NOT NULL DEFAULT '';
"""


class DeploymentStore:
    """Thread-safe SQLite store for deployment records."""

    def __init__(self, db_path: str | None = None):
        self._db_path = db_path or _DB_PATH
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(self._db_path), exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(_CREATE_TABLE)
            # Migrate: add cloud_api_key column if missing (existing DBs)
            try:
                conn.execute(_MIGRATE_CLOUD_API_KEY)
            except sqlite3.OperationalError:
                pass  # column already exists

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def upsert(
        self,
        deployment_id: str,
        scenario_id: str,
        *,
        otlp_endpoint: str = "",
        otlp_api_key: str = "",
        elastic_url: str = "",
        elastic_api_key: str = "",
        kibana_url: str = "",
        cloud_api_key: str = "",
        status: str = "active",
    ) -> None:
        """Insert or replace a deployment record."""
        now = time.time()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO deployments
                       (deployment_id, scenario_id, otlp_endpoint, otlp_api_key,
                        elastic_url, elastic_api_key, kibana_url, cloud_api_key,
                        status, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        deployment_id,
                        scenario_id,
                        otlp_endpoint,
                        otlp_api_key,
                        elastic_url,
                        elastic_api_key,
                        kibana_url,
                        cloud_api_key,
                        status,
                        now,
                        now,
                    ),
                )

    def get_all_active(self) -> list[dict[str, Any]]:
        """Return all deployments with status='active'."""
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM deployments WHERE status = 'active' ORDER BY created_at",
                ).fetchall()
                return [dict(r) for r in rows]

    def set_status(self, deployment_id: str, status: str) -> None:
        """Update status of a deployment."""
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE deployments SET status = ?, updated_at = ? WHERE deployment_id = ?",
                    (status, time.time(), deployment_id),
                )

    def delete(self, deployment_id: str) -> None:
        """Remove a deployment record entirely."""
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "DELETE FROM deployments WHERE deployment_id = ?",
                    (deployment_id,),
                )


# ── Chaos channel persistence ─────────────────────────────────────────────────

_CREATE_CHAOS_TABLE = """
CREATE TABLE IF NOT EXISTS chaos_channels (
    deployment_id   TEXT NOT NULL,
    channel         INTEGER NOT NULL,
    state           TEXT NOT NULL DEFAULT 'STANDBY',
    se_name         TEXT,
    session_id      TEXT,
    triggered_at    REAL,
    resolved_at     REAL,
    callback_url    TEXT NOT NULL DEFAULT '',
    user_email      TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (deployment_id, channel)
);
"""

_CREATE_SPIKES_TABLE = """
CREATE TABLE IF NOT EXISTS chaos_spikes (
    deployment_id             TEXT PRIMARY KEY,
    cpu_pct                   REAL NOT NULL DEFAULT 0,
    memory_pct                REAL NOT NULL DEFAULT 0,
    k8s_oom_intensity         REAL NOT NULL DEFAULT 0,
    k8s_pod_cpu_intensity     REAL NOT NULL DEFAULT 0,
    k8s_pod_memory_intensity  REAL NOT NULL DEFAULT 0,
    latency_multiplier        REAL NOT NULL DEFAULT 1.0,
    extra_json                TEXT NOT NULL DEFAULT '{}'
);
"""

# Columns added after the initial schema — applied via ALTER on existing PVCs.
_SPIKES_MIGRATIONS = (
    ("k8s_pod_cpu_intensity", "REAL NOT NULL DEFAULT 0"),
    ("k8s_pod_memory_intensity", "REAL NOT NULL DEFAULT 0"),
    ("extra_json", "TEXT NOT NULL DEFAULT '{}'"),
)

# Spike keys with their own column; every other key round-trips through extra_json so
# new channel augmentations need no schema change.
_SPIKE_COLUMNS = (
    "cpu_pct", "memory_pct", "k8s_oom_intensity",
    "k8s_pod_cpu_intensity", "k8s_pod_memory_intensity", "latency_multiplier",
)


class ChaosStore:
    """Thread-safe SQLite store for chaos channel state (session ownership)."""

    def __init__(self, db_path: str | None = None):
        self._db_path = db_path or _DB_PATH
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(self._db_path), exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(_CREATE_CHAOS_TABLE)
            conn.execute(_CREATE_SPIKES_TABLE)
            self._migrate_spikes_table(conn)

    def _migrate_spikes_table(self, conn: sqlite3.Connection) -> None:
        """Add new spike columns on long-lived PVC DBs created before those keys existed."""
        existing = {
            row[1]
            for row in conn.execute("PRAGMA table_info(chaos_spikes)").fetchall()
        }
        for col, decl in _SPIKES_MIGRATIONS:
            if col not in existing:
                conn.execute(f"ALTER TABLE chaos_spikes ADD COLUMN {col} {decl}")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def upsert_channel(
        self,
        deployment_id: str,
        channel: int,
        *,
        state: str = "ACTIVE",
        se_name: str = "",
        session_id: str = "",
        triggered_at: float | None = None,
        resolved_at: float | None = None,
        callback_url: str = "",
        user_email: str = "",
    ) -> None:
        """Insert or replace a chaos channel record."""
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO chaos_channels
                       (deployment_id, channel, state, se_name, session_id,
                        triggered_at, resolved_at, callback_url, user_email)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        deployment_id,
                        channel,
                        state,
                        se_name,
                        session_id,
                        triggered_at,
                        resolved_at,
                        callback_url,
                        user_email,
                    ),
                )

    def resolve_channel(
        self, deployment_id: str, channel: int, resolved_at: float
    ) -> None:
        """Mark a channel as STANDBY."""
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """UPDATE chaos_channels
                       SET state = 'STANDBY', session_id = NULL,
                           resolved_at = ?, callback_url = '', user_email = ''
                       WHERE deployment_id = ? AND channel = ?""",
                    (resolved_at, deployment_id, channel),
                )

    def get_all_channels(self, deployment_id: str) -> list[dict[str, Any]]:
        """Return all channel records for a deployment."""
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM chaos_channels WHERE deployment_id = ?",
                    (deployment_id,),
                ).fetchall()
                return [dict(r) for r in rows]

    def upsert_spikes(self, deployment_id: str, spikes: dict) -> None:
        """Persist infra spike values for a deployment."""
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO chaos_spikes
                       (deployment_id, cpu_pct, memory_pct, k8s_oom_intensity,
                        k8s_pod_cpu_intensity, k8s_pod_memory_intensity, latency_multiplier,
                        extra_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        deployment_id,
                        spikes.get("cpu_pct", 0),
                        spikes.get("memory_pct", 0),
                        spikes.get("k8s_oom_intensity", 0),
                        spikes.get("k8s_pod_cpu_intensity", 0),
                        spikes.get("k8s_pod_memory_intensity", 0),
                        spikes.get("latency_multiplier", 1.0),
                        json.dumps({
                            k: v for k, v in spikes.items()
                            if k not in _SPIKE_COLUMNS and isinstance(v, (int, float))
                        }),
                    ),
                )

    def get_spikes(self, deployment_id: str) -> dict | None:
        """Return persisted spike values for a deployment, or None."""
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM chaos_spikes WHERE deployment_id = ?",
                    (deployment_id,),
                ).fetchone()
                if not row:
                    return None
                out = dict(row)
                try:
                    extra = json.loads(out.pop("extra_json", None) or "{}")
                except ValueError:
                    extra = {}
                out.update(extra)
                return out

    def delete_spikes_for_deployment(self, deployment_id: str) -> None:
        """Remove persisted spike values for a deployment (e.g. on teardown)."""
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "DELETE FROM chaos_spikes WHERE deployment_id = ?",
                    (deployment_id,),
                )

    def delete_channels_for_deployment(self, deployment_id: str) -> None:
        """Remove all persisted chaos rows for a deployment (e.g. on teardown)."""
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "DELETE FROM chaos_channels WHERE deployment_id = ?",
                    (deployment_id,),
                )

    def expire_channels(self, deployment_id: str, max_age: float) -> list[int]:
        """Expire channels older than max_age seconds. Returns expired channel IDs."""
        now = time.time()
        cutoff = now - max_age
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """SELECT channel FROM chaos_channels
                       WHERE deployment_id = ? AND state = 'ACTIVE'
                         AND triggered_at IS NOT NULL AND triggered_at < ?""",
                    (deployment_id, cutoff),
                ).fetchall()
                expired = [r["channel"] for r in rows]
                if expired:
                    conn.execute(
                        """UPDATE chaos_channels
                            SET state = 'STANDBY', session_id = NULL,
                                resolved_at = ?, callback_url = '', user_email = ''
                            WHERE deployment_id = ? AND state = 'ACTIVE'
                              AND triggered_at IS NOT NULL AND triggered_at < ?""",
                        (now, deployment_id, cutoff),
                    )
                return expired
