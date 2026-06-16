"""Notification Service — GCP europe-west3-a. SMS, push, email, and MVNO webhook delivery."""

from __future__ import annotations

import random
import time

from app.services.base_service import BaseService


class NotificationServiceService(BaseService):
    SERVICE_NAME = "notification-service"

    def __init__(self, chaos_controller, otlp_client):
        super().__init__(chaos_controller, otlp_client)
        self._notification_count = 0
        self._last_summary = time.time()

    def generate_telemetry(self) -> None:
        active_channels = self.get_active_channels_for_service()
        for ch in active_channels:
            self.emit_fault_logs(ch)

        cascade_channels = self.get_cascade_channels_for_service()
        for ch in cascade_channels:
            self.emit_cascade_logs(ch)

        self._emit_notification()
        self._emit_channel_health()

        if time.time() - self._last_summary > 10:
            self._emit_notification_summary()
            self._last_summary = time.time()

        queue = float(random.randint(10, 100) if not active_channels else random.randint(500, 5000))
        self.emit_metric("notification_service.queue_depth", queue, "messages")
        self.emit_metric("notification_service.delivery_rate", round(random.uniform(95, 99.5), 2) if not active_channels else round(random.uniform(10, 60), 2), "%")
        self.emit_metric("notification_service.sms_throughput", round(random.uniform(200, 500), 1), "msg/min")

    def _emit_notification(self) -> None:
        self._notification_count += 1
        channel = random.choice(["sms", "push", "email", "webhook"])
        template = random.choice(["activation_confirm", "plan_change", "usage_alert", "partner_callback"])
        self.emit_log(
            "INFO",
            f"[NOTIF] notification_sent channel={channel} template={template} priority=high delivery=SUCCESS",
            {
                "operation": "notification_sent",
                "notification.channel": channel,
                "notification.template": template,
                "notification.delivery": "SUCCESS",
            },
        )

    def _emit_channel_health(self) -> None:
        self.emit_log(
            "INFO",
            f"[NOTIF] channel_health sms=98.5% push=99.2% email=99.8% webhook=97.5% smpp_bind=BOUND status=NORMAL",
            {
                "operation": "channel_health",
                "notification.sms_rate": 98.5,
                "notification.push_rate": 99.2,
                "notification.webhook_rate": 97.5,
            },
        )

    def _emit_notification_summary(self) -> None:
        self.emit_log(
            "INFO",
            f"[NOTIF] notification_summary total={self._notification_count} delivery_rate=98.8% queue_depth=45 status=NORMAL",
            {
                "operation": "notification_summary",
                "notification.total": self._notification_count,
                "notification.delivery_rate": 98.8,
            },
        )
