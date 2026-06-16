"""Activation Service — AWS eu-central-1b. Orchestrates 5G subscriber activation workflow."""

from __future__ import annotations

import random
import time

from app.services.base_service import BaseService


class ActivationServiceService(BaseService):
    SERVICE_NAME = "activation-service"

    def __init__(self, chaos_controller, otlp_client):
        super().__init__(chaos_controller, otlp_client)
        self._activation_count = 0
        self._last_summary = time.time()

    def generate_telemetry(self) -> None:
        active_channels = self.get_active_channels_for_service()
        for ch in active_channels:
            self.emit_fault_logs(ch)

        cascade_channels = self.get_cascade_channels_for_service()
        for ch in cascade_channels:
            self.emit_cascade_logs(ch)

        self._emit_activation_request()
        self._emit_workflow_status()

        if time.time() - self._last_summary > 10:
            self._emit_activation_summary()
            self._last_summary = time.time()

        latency_ms = round(random.uniform(200, 2000), 1) if not active_channels else round(random.uniform(5000, 30000), 1)
        self.emit_metric("activation_service.workflow_latency_ms", latency_ms, "ms")
        self.emit_metric("activation_service.active_workflows", float(random.randint(20, 100)), "workflows")
        self.emit_metric("activation_service.success_rate", round(random.uniform(98, 99.9), 2), "%")

    def _emit_activation_request(self) -> None:
        self._activation_count += 1
        activation_type = random.choice(["new_sim", "sim_swap", "plan_upgrade", "5g_activation", "esim_activation"])
        segment = random.choice(["Consumer", "Enterprise", "MVNO"])
        self.emit_log(
            "INFO",
            f"[ACTIVATE] activation_request type={activation_type} segment={segment} step=4/6 status=IN_PROGRESS",
            {
                "operation": "activation_request",
                "activation.type": activation_type,
                "activation.segment": segment,
                "activation.step": "4/6",
            },
        )

    def _emit_workflow_status(self) -> None:
        active = random.randint(20, 100)
        self.emit_log(
            "INFO",
            f"[ACTIVATE] workflow_health active_workflows={active} avg_duration_ms=1200 retry_rate=0.5% status=NORMAL",
            {
                "operation": "workflow_health",
                "workflow.active": active,
                "workflow.avg_duration_ms": 1200,
            },
        )

    def _emit_activation_summary(self) -> None:
        self.emit_log(
            "INFO",
            f"[ACTIVATE] activation_summary total={self._activation_count} success_rate=99.2% avg_latency_ms=1200 status=NORMAL",
            {
                "operation": "activation_summary",
                "activation.total": self._activation_count,
                "activation.success_rate": 99.2,
            },
        )
