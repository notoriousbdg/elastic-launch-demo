"""BSS Billing service — Azure westeurope-2. CDR processing, mediation, and SLA monitoring."""

from __future__ import annotations

import random
import time

from app.services.base_service import BaseService
from scenarios.telecom.executive_kpis import emit_executive_business_metrics_if_eligible


class BssBillingService(BaseService):
    SERVICE_NAME = "bss-billing"

    def __init__(self, chaos_controller, otlp_client):
        super().__init__(chaos_controller, otlp_client)
        self._billing_count = 0
        self._last_summary = time.time()

    def generate_telemetry(self) -> None:
        active_channels = self.get_active_channels_for_service()
        for ch in active_channels:
            self.emit_fault_logs(ch)

        cascade_channels = self.get_cascade_channels_for_service()
        for ch in cascade_channels:
            self.emit_cascade_logs(ch)

        self._emit_billing_operation()
        self._emit_cdr_pipeline()

        if time.time() - self._last_summary > 10:
            self._emit_billing_summary()
            self._last_summary = time.time()

        cdr_rate = round(random.uniform(1500, 3000), 1) if not active_channels else round(random.uniform(200, 800), 1)
        self.emit_metric("bss_billing.cdr_processing_rate", cdr_rate, "cdr/min")
        self.emit_metric("bss_billing.mediation_queue_depth", float(random.randint(100, 500) if not active_channels else random.randint(50000, 200000)), "cdrs")
        self.emit_metric("bss_billing.revenue_per_hour", round(random.uniform(15000, 45000), 2), "EUR")

        emit_executive_business_metrics_if_eligible(self)

    def _emit_billing_operation(self) -> None:
        self._billing_count += 1
        operation = random.choice(["account_create", "cdr_rate", "invoice_generate", "credit_apply"])
        plan = random.choice(["5G-BASIC-EU-v3", "5G-PLUS-EU-v3", "5G-MAX-EU-v3", "ENT-BUS-L-v2"])
        self.emit_log(
            "INFO",
            f"[BSS] billing_operation type={operation} plan={plan} currency=EUR status=COMPLETED",
            {
                "operation": "billing_operation",
                "billing.operation": operation,
                "billing.plan_code": plan,
            },
        )

    def _emit_cdr_pipeline(self) -> None:
        rate = random.randint(1500, 3000)
        queue = random.randint(100, 500)
        self.emit_log(
            "INFO",
            f"[BSS] cdr_pipeline processing_rate={rate}/min queue_depth={queue} rating_engine=OK mediation=HEALTHY status=NORMAL",
            {
                "operation": "cdr_pipeline",
                "billing.cdr_rate": rate,
                "billing.queue_depth": queue,
            },
        )

    def _emit_billing_summary(self) -> None:
        self.emit_log(
            "INFO",
            f"[BSS] billing_summary operations={self._billing_count} cdr_processed=45000 sla_compliance=99.8% status=NORMAL",
            {
                "operation": "billing_summary",
                "billing.total_operations": self._billing_count,
                "billing.sla_compliance": 99.8,
            },
        )
