"""Auth Service — Azure westeurope-1. OAuth2/SAML authentication and session management."""

from __future__ import annotations

import random
import time

from app.services.base_service import BaseService


class AuthServiceService(BaseService):
    SERVICE_NAME = "auth-service"

    def __init__(self, chaos_controller, otlp_client):
        super().__init__(chaos_controller, otlp_client)
        self._auth_count = 0
        self._last_summary = time.time()

    def generate_telemetry(self) -> None:
        active_channels = self.get_active_channels_for_service()
        for ch in active_channels:
            self.emit_fault_logs(ch)

        cascade_channels = self.get_cascade_channels_for_service()
        for ch in cascade_channels:
            self.emit_cascade_logs(ch)

        self._emit_auth_request()
        self._emit_session_metrics()

        if time.time() - self._last_summary > 10:
            self._emit_auth_summary()
            self._last_summary = time.time()

        latency_ms = round(random.uniform(20, 150), 1) if not active_channels else round(random.uniform(1000, 10000), 1)
        self.emit_metric("auth_service.validation_latency_ms", latency_ms, "ms")
        self.emit_metric("auth_service.active_sessions", float(random.randint(400000, 600000)), "sessions")
        self.emit_metric("auth_service.token_validation_rate", round(random.uniform(98, 99.9), 2), "%")

    def _emit_auth_request(self) -> None:
        self._auth_count += 1
        method = random.choice(["oauth2", "saml", "api_key", "mtls"])
        provider = random.choice(["internal", "azure_ad", "mvno_partner"])
        self.emit_log(
            "INFO",
            f"[AUTH] token_validated method={method} provider={provider} scope=activation status=VALID",
            {
                "operation": "token_validated",
                "auth.method": method,
                "auth.provider": provider,
                "auth.status": "VALID",
            },
        )

    def _emit_session_metrics(self) -> None:
        sessions = random.randint(400000, 600000)
        self.emit_log(
            "INFO",
            f"[AUTH] session_health active_sessions={sessions} redis_memory=62% jwks_cache_age=1200s status=NORMAL",
            {
                "operation": "session_health",
                "auth.sessions": sessions,
                "auth.redis_memory_pct": 62,
            },
        )

    def _emit_auth_summary(self) -> None:
        self.emit_log(
            "INFO",
            f"[AUTH] auth_summary validations={self._auth_count} success_rate=99.8% federation_status=OK status=NORMAL",
            {
                "operation": "auth_summary",
                "auth.total_validations": self._auth_count,
                "auth.success_rate": 99.8,
            },
        )
