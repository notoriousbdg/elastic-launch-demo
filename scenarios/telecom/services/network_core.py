"""Network Core service — GCP europe-west3-b. 5G NR bearer allocation and signaling."""

from __future__ import annotations

import random
import time

from app.services.base_service import BaseService


class NetworkCoreService(BaseService):
    SERVICE_NAME = "network-core"

    def __init__(self, chaos_controller, otlp_client):
        super().__init__(chaos_controller, otlp_client)
        self._bearer_count = 0
        self._last_summary = time.time()

    def generate_telemetry(self) -> None:
        active_channels = self.get_active_channels_for_service()
        for ch in active_channels:
            self.emit_fault_logs(ch)

        cascade_channels = self.get_cascade_channels_for_service()
        for ch in cascade_channels:
            self.emit_cascade_logs(ch)

        self._emit_bearer_allocation()
        self._emit_signaling_metrics()

        if time.time() - self._last_summary > 10:
            self._emit_network_summary()
            self._last_summary = time.time()

        prb_util = round(random.uniform(40, 75), 1) if not active_channels else round(random.uniform(90, 99), 1)
        self.emit_metric("network_core.prb_utilization_pct", prb_util, "%")
        self.emit_metric("network_core.active_bearers", float(random.randint(100, 200) if not active_channels else random.randint(240, 256)), "bearers")
        self.emit_metric("network_core.gtp_tunnels_active", float(random.randint(30000, 45000)), "tunnels")

    def _emit_bearer_allocation(self) -> None:
        self._bearer_count += 1
        bearer_type = random.choice(["5G_NR", "LTE", "NR-DC", "LTE-A"])
        qos = random.choice(["QCI-1", "QCI-5", "QCI-9", "5QI-1", "5QI-9"])
        self.emit_log(
            "INFO",
            f"[NET] bearer_allocation type={bearer_type} qos={qos} cell=CELL-EU-{random.randint(10000, 99999)} status=ALLOCATED",
            {
                "operation": "bearer_allocation",
                "network.bearer_type": bearer_type,
                "network.qos_class": qos,
            },
        )

    def _emit_signaling_metrics(self) -> None:
        sctp_load = random.randint(30, 70)
        attach_rate = random.randint(200, 500)
        self.emit_log(
            "INFO",
            f"[NET] signaling_health sctp_load={sctp_load}% attach_rate={attach_rate}/min tau_rate={attach_rate * 2}/min status=NORMAL",
            {
                "operation": "signaling_health",
                "network.sctp_load_pct": sctp_load,
                "network.attach_rate": attach_rate,
            },
        )

    def _emit_network_summary(self) -> None:
        self.emit_log(
            "INFO",
            f"[NET] network_summary bearers_allocated={self._bearer_count} gtp_tunnels=42000 mme_pool=HEALTHY status=NORMAL",
            {
                "operation": "network_summary",
                "network.bearers_allocated": self._bearer_count,
                "network.gtp_tunnels": 42000,
            },
        )
