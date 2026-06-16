"""Synthetic `business.*` OTLP gauges for the Meridian Telecom Executive Dashboard."""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.services.base_service import BaseService


def emit_executive_business_metrics_if_eligible(service: "BaseService") -> None:
    """Emit live 5G subscriber platform KPI gauges once per telemetry cycle from the designated service."""
    ctx = getattr(service, "_ctx", None)
    if not ctx:
        return
    want = getattr(ctx.scenario, "executive_kpi_emitter_service_name", None)
    if not want or want != service.SERVICE_NAME:
        return

    emit = service.emit_metric

    # Subscriber growth
    emit("business.activations_per_min", float(random.randint(380, 1_850)), "activations/min")
    emit("business.mvno_net_adds_per_min", float(random.randint(45, 280)), "net-adds/min")
    emit("business.port_ins_per_min", float(random.randint(120, 420)), "port-ins/min")
    emit("business.port_outs_per_min", float(random.randint(90, 380)), "port-outs/min")
    emit("business.esim_provisions_per_min", float(random.randint(200, 1_100)), "provisions/min")
    emit("business.active_subscribers_m", round(random.uniform(18.4, 19.2), 3), "subscribers (M)")

    # Revenue & ARPU
    emit("business.revenue_usd_per_min", round(random.uniform(82_000.0, 145_000.0), 1), "USD/min")
    emit("business.consumer_arpu_usd", round(random.uniform(38.2, 52.6), 2), "USD")
    emit("business.enterprise_arpu_usd", round(random.uniform(285.0, 480.0), 2), "USD")
    emit("business.mvno_partner_revenue_usd_per_min", round(random.uniform(4_500.0, 12_800.0), 1), "USD/min")
    emit("business.data_overage_charges_usd_per_min", round(random.uniform(1_800.0, 8_500.0), 1), "USD/min")
    emit("business.roaming_revenue_usd_per_min", round(random.uniform(2_200.0, 9_400.0), 1), "USD/min")

    # Network & QoE
    emit("business.handover_success_pct", round(random.uniform(98.2, 99.8), 3), "%")
    emit("business.ran_latency_p95_ms", round(random.uniform(18.0, 38.0), 1), "ms")
    emit("business.voice_mos", round(random.uniform(3.9, 4.4), 2), "MOS")
    emit("business.data_session_success_pct", round(random.uniform(97.5, 99.6), 3), "%")
    emit("business.volte_call_setup_success_pct", round(random.uniform(98.1, 99.7), 3), "%")
    emit("business.throughput_p50_mbps", round(random.uniform(85.0, 220.0), 1), "Mbps")

    # Retention & support
    emit("business.voluntary_churn_rate_pct", round(random.uniform(0.8, 2.4), 3), "%")
    emit("business.nps", float(random.randint(32, 58)), "NPS")
    emit("business.support_tickets_per_min", float(random.randint(28, 180)), "tickets/min")
    emit("business.avg_ticket_resolution_min", round(random.uniform(8.0, 42.0), 1), "min")
    emit("business.first_call_resolution_pct", round(random.uniform(62.0, 84.0), 2), "%")
    emit("business.trouble_to_billing_ratio_pct", round(random.uniform(1.2, 4.8), 3), "%")
