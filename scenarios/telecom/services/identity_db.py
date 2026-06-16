"""Identity DB service — Azure westeurope-1. PostgreSQL for identity and session data."""

from __future__ import annotations

import random
import time

from app.services.base_service import BaseService


class IdentityDbService(BaseService):
    SERVICE_NAME = "identity-db"

    def __init__(self, chaos_controller, otlp_client):
        super().__init__(chaos_controller, otlp_client)
        self._query_count = 0
        self._last_summary = time.time()

    def generate_telemetry(self) -> None:
        active_channels = self.get_active_channels_for_service()
        for ch in active_channels:
            self.emit_fault_logs(ch)

        cascade_channels = self.get_cascade_channels_for_service()
        for ch in cascade_channels:
            self.emit_cascade_logs(ch)

        self._emit_query()
        self._emit_replication_status()

        if time.time() - self._last_summary > 10:
            self._emit_db_summary()
            self._last_summary = time.time()

        latency_ms = round(random.uniform(3, 15), 1) if not active_channels else round(random.uniform(500, 5000), 1)
        self.emit_metric("identity_db.query_latency_ms", latency_ms, "ms")
        self.emit_metric("identity_db.replication_lag_ms", float(random.randint(0, 50) if not active_channels else random.randint(5000, 30000)), "ms")
        self.emit_metric("identity_db.connections_active", float(random.randint(20, 40)), "connections")

    def _emit_query(self) -> None:
        self._query_count += 1
        table = random.choice(["identities", "sessions", "tokens"])
        query_type = random.choice(["SELECT", "INSERT"])
        latency_ms = round(random.uniform(3, 12), 1)
        self.emit_log(
            "INFO",
            f"[DB] query_executed type={query_type} table={table} latency_ms={latency_ms} role=primary status=OK",
            {
                "operation": "query_executed",
                "db.query_type": query_type,
                "db.table": table,
                "db.latency_ms": latency_ms,
            },
        )

    def _emit_replication_status(self) -> None:
        lag_ms = random.randint(0, 50)
        self.emit_log(
            "INFO",
            f"[DB] replication_status primary=OK replica=OK lag_ms={lag_ms} wal_replay=current status=NORMAL",
            {
                "operation": "replication_status",
                "db.replication_lag_ms": lag_ms,
                "db.wal_replay": "current",
            },
        )

    def _emit_db_summary(self) -> None:
        self.emit_log(
            "INFO",
            f"[DB] db_summary queries={self._query_count} avg_latency_ms=8 replication=SYNCED connections=35/100 status=NORMAL",
            {
                "operation": "db_summary",
                "db.total_queries": self._query_count,
                "db.avg_latency_ms": 8,
            },
        )
