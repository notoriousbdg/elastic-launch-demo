"""Subscriber DB service — AWS eu-central-1a. PostgreSQL database for 45M subscriber records."""

from __future__ import annotations

import random
import time

from app.services.base_service import BaseService


class SubscriberDbService(BaseService):
    SERVICE_NAME = "subscriber-db"

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

        self._emit_query(active_channels)
        self._emit_pool_metrics(active_channels)

        if time.time() - self._last_summary > 10:
            self._emit_db_summary(active_channels)
            self._last_summary = time.time()

        latency_ms = round(random.uniform(5, 25), 1) if not active_channels else round(random.uniform(2000, 4500), 1)
        self.emit_metric("subscriber_db.query_latency_ms", latency_ms, "ms")
        self.emit_metric("subscriber_db.pool_active", float(random.randint(30, 60) if not active_channels else random.randint(90, 100)), "connections")
        self.emit_metric("subscriber_db.rows_scanned", float(random.randint(1, 100)), "rows")

    def _emit_query(self, active_channels: list[int]) -> None:
        self._query_count += 1
        query_type = random.choice(["SELECT", "UPDATE", "INSERT"])
        table = random.choice(["subscribers", "activation_log", "subscriber_profiles"])

        if active_channels:
            latency_ms = round(random.uniform(3000, 5000), 1)
            if random.random() < 0.6:
                status = "TIMEOUT"
                level = "ERROR"
            else:
                status = "SLOW"
                level = "WARN"
        else:
            latency_ms = round(random.uniform(5, 20), 1)
            status = "OK"
            level = "INFO"

        self.emit_log(
            level,
            f"[DB] query_executed type={query_type} table={table} latency_ms={latency_ms} rows=1 status={status}",
            {
                "operation": "query_executed",
                "db.query_type": query_type,
                "db.table": table,
                "db.latency_ms": latency_ms,
                "db.status": status,
            },
        )

    def _emit_pool_metrics(self, active_channels: list[int]) -> None:
        if active_channels:
            active = random.randint(95, 100)
            idle = 0
            wait_queue = random.randint(200, 500)
            status = "EXHAUSTED"
            level = "WARN"
        else:
            active = random.randint(30, 60)
            idle = random.randint(20, 40)
            wait_queue = 0
            status = "NORMAL"
            level = "INFO"

        self.emit_log(
            level,
            f"[DB] pool_status active={active} idle={idle} max=100 wait_queue={wait_queue} status={status}",
            {
                "operation": "pool_status",
                "db.pool_active": active,
                "db.pool_idle": idle,
                "db.pool_max": 100,
                "db.pool_wait_queue": wait_queue,
                "db.pool_status": status,
            },
        )

    def _emit_db_summary(self, active_channels: list[int]) -> None:
        if active_channels:
            avg_latency = round(random.uniform(3000, 4000), 0)
            status = "CRITICAL"
            level = "ERROR"
        else:
            avg_latency = 12
            status = "NORMAL"
            level = "INFO"

        self.emit_log(
            level,
            f"[DB] db_summary queries={self._query_count} avg_latency_ms={avg_latency} replication_lag_ms=0 status={status}",
            {
                "operation": "db_summary",
                "db.total_queries": self._query_count,
                "db.avg_latency_ms": avg_latency,
            },
        )
