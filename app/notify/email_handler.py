"""Email notification handler — send alerts via SMTP or log in demo mode.

If SMTP_HOST is configured, sends real emails via smtplib.
Otherwise, logs the email content and returns success (demo mode).
"""

import asyncio
import logging
import smtplib
from email.mime.text import MIMEText
from typing import Any

from app.config import SMTP_FROM, SMTP_HOST, SMTP_PASSWORD, SMTP_PORT, SMTP_USER

logger = logging.getLogger("nova7.notify.email")


async def send_email(to: str, subject: str, body: str) -> dict[str, Any]:
    """Send an email notification, or log it in demo mode.

    Parameters
    ----------
    to : str
        Recipient email address.
    subject : str
        Email subject line.
    body : str
        Plain-text email body.

    Returns
    -------
    dict
        Result indicating success or failure.
    """
    if not to:
        return {"sent": False, "error": "No recipient specified"}

    if SMTP_HOST:
        try:
            # smtplib blocks; run it off the event loop.
            await asyncio.to_thread(_send_smtp, to, subject, body)
            logger.info("Email sent to %s: %s", to, subject)
            return {"sent": True, "to": to, "subject": subject, "mode": "smtp"}
        except Exception as exc:
            logger.error("SMTP send failed: %s", exc)
            return {"sent": False, "error": str(exc)}
    else:
        # Demo mode — log the email
        logger.info(
            "EMAIL (demo mode) To: %s | Subject: %s | Body: %s",
            to,
            subject,
            body[:200],
        )
        return {"sent": True, "to": to, "subject": subject, "mode": "demo"}


def _send_smtp(to: str, subject: str, body: str) -> None:
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = to

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
        server.ehlo()
        if SMTP_PORT != 25:
            server.starttls()
        if SMTP_USER and SMTP_PASSWORD:
            server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_FROM, [to], msg.as_string())
