"""Provisioning Service — GCP europe-west3-a. SIM provisioning and HLR/HSS registration."""

from __future__ import annotations

import random
import time

from app.services.base_service import BaseService


class ProvisioningServiceService(BaseService):
    SERVICE_NAME = "provisioning-service"

    def __init__(self, chaos_controller, otlp_client):
        super().__init__(chaos_controller, otlp_client)
        self._provision_count = 0
        self._last_summary = time.time()

    def generate_telemetry(self) -> None:
        active_channels = self.get_active_channels_for_service()
        for ch in active_channels:
            self.emit_fault_logs(ch)

        cascade_channels = self.get_cascade_channels_for_service()
        for ch in cascade_channels:
            self.emit_cascade_logs(ch)

        self._emit_provisioning_request(active_channels)
        self._emit_hlr_status(active_channels)

        if time.time() - self._last_summary > 10:
            self._emit_provisioning_summary(active_channels)
            self._last_summary = time.time()

        latency_ms = round(random.uniform(100, 800), 1) if not active_channels else round(random.uniform(3000, 15000), 1)
        self.emit_metric("provisioning_service.operation_latency_ms", latency_ms, "ms")
        self.emit_metric("provisioning_service.hlr_queue_depth", float(random.randint(10, 100) if not active_channels else random.randint(3000, 8000)), "entries")
        self.emit_metric("provisioning_service.sim_activations_per_min", round(random.uniform(80, 200), 1), "activations/min")

    def _emit_provisioning_request(self, active_channels: list[int]) -> None:
        self._provision_count += 1
        sim_type = random.choice(["physical_sim", "esim", "embedded_sim"])
        operation = random.choice(["register", "update", "query"])

        if active_channels:
            db_wait_ms = random.randint(3000, 5000)
            if random.random() < 0.5:
                status = "TIMEOUT"
                level = "ERROR"
            else:
                status = "SLOW"
                level = "WARN"
            self.emit_log(
                level,
                f"[PROV] provisioning_request sim_type={sim_type} hlr_operation={operation} apn=internet.meridian.eu status={status} db_wait_ms={db_wait_ms}",
                {
                    "operation": "provisioning_request",
                    "provisioning.sim_type": sim_type,
                    "provisioning.hlr_operation": operation,
                    "provisioning.status": status,
                    "provisioning.db_wait_ms": db_wait_ms,
                },
            )
        else:
            self.emit_log(
                "INFO",
                f"[PROV] provisioning_request sim_type={sim_type} hlr_operation={operation} apn=internet.meridian.eu status=OK",
                {
                    "operation": "provisioning_request",
                    "provisioning.sim_type": sim_type,
                    "provisioning.hlr_operation": operation,
                },
            )

    def _emit_hlr_status(self, active_channels: list[int]) -> None:
        if active_channels:
            queue = random.randint(3000, 8000)
            rate = random.randint(20, 50)
            status = "DEGRADED"
            level = "WARN"
        else:
            queue = random.randint(10, 100)
            rate = random.randint(180, 220)
            status = "NORMAL"
            level = "INFO"

        self.emit_log(
            level,
            f"[PROV] hlr_status queue_depth={queue} processing_rate={rate}/min registration_success={'45.2' if active_channels else '99.8'}% status={status}",
            {
                "operation": "hlr_status",
                "provisioning.hlr_queue": queue,
                "provisioning.hlr_rate": rate,
            },
        )

    def _emit_provisioning_summary(self, active_channels: list[int]) -> None:
        if active_channels:
            success_rate = round(random.uniform(40, 60), 1)
            avg_latency = 8000
            status = "DEGRADED"
            level = "WARN"
        else:
            success_rate = 99.5
            avg_latency = 450
            status = "NORMAL"
            level = "INFO"

        self.emit_log(
            level,
            f"[PROV] provisioning_summary total={self._provision_count} success_rate={success_rate}% avg_latency_ms={avg_latency} status={status}",
            {
                "operation": "provisioning_summary",
                "provisioning.total": self._provision_count,
                "provisioning.success_rate": success_rate,
            },
        )
