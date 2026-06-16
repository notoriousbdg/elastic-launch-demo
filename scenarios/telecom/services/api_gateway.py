"""API Gateway service — AWS eu-central-1a. Entry point for 5G activation requests."""

from __future__ import annotations

import random
import time

from app.services.base_service import BaseService


class ApiGatewayService(BaseService):
    SERVICE_NAME = "api-gateway"

    def __init__(self, chaos_controller, otlp_client):
        super().__init__(chaos_controller, otlp_client)
        self._request_count = 0
        self._last_summary = time.time()
        self._endpoints = [
            "/api/v1/activate",
            "/api/v1/bundle/5g-home",
            "/api/v1/customer/status",
        ]

    def generate_telemetry(self) -> None:
        active_channels = self.get_active_channels_for_service()
        for ch in active_channels:
            self.emit_fault_logs(ch)

        cascade_channels = self.get_cascade_channels_for_service()
        for ch in cascade_channels:
            self.emit_cascade_logs(ch)

        self._emit_api_request()
        self._emit_rate_limiter_status()

        if time.time() - self._last_summary > 10:
            self._emit_gateway_summary()
            self._last_summary = time.time()

        latency_ms = round(random.uniform(10, 200), 1) if not active_channels else round(random.uniform(2000, 12000), 1)
        self.emit_metric("api_gateway.request_latency_ms", latency_ms, "ms")
        self.emit_metric("api_gateway.active_connections", float(random.randint(500, 2000)), "connections")
        self.emit_metric("api_gateway.requests_per_sec", round(random.uniform(400, 1200), 1), "req/s")

    def _emit_api_request(self) -> None:
        self._request_count += 1
        endpoint = random.choice(self._endpoints)
        latency_ms = round(random.uniform(15, 180), 1)
        self.emit_log(
            "INFO",
            f"[GW] api_request endpoint={endpoint} method=POST latency_ms={latency_ms} status=200 protocol=HTTP/2",
            {
                "operation": "api_request",
                "api.endpoint": endpoint,
                "api.latency_ms": latency_ms,
                "api.status": 200,
            },
        )

    def _emit_rate_limiter_status(self) -> None:
        tokens = random.randint(600, 1000)
        self.emit_log(
            "INFO",
            f"[GW] rate_limiter tokens_available={tokens} limit=1000/min burst=200 status=OK",
            {
                "operation": "rate_limiter",
                "rate_limiter.tokens": tokens,
                "rate_limiter.limit": 1000,
            },
        )

    def _emit_gateway_summary(self) -> None:
        self.emit_log(
            "INFO",
            f"[GW] gateway_summary total_requests={self._request_count} error_rate=0.01% tls_handshake_avg_ms=12 status=NORMAL",
            {
                "operation": "gateway_summary",
                "gateway.total_requests": self._request_count,
                "gateway.error_rate": 0.01,
            },
        )
