"""ChaosSource — the interface services and generators use to read fault state."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ChaosSource(Protocol):
    """Read-only view of active fault channels.

    ``ChaosController`` is the implementation. Generators and ``BaseService``
    depend on this surface, not on the controller's trigger/resolve API.
    """

    def is_active(self, channel: int) -> bool:
        """True when *channel* is currently faulted."""

    def get_active_channels(self) -> list[int]:
        """Channel ids whose state is active."""

    def get_infra_spikes(self) -> dict[str, float]:
        """Infrastructure multipliers applied while any channel is active."""

    def get_channel_metadata(self, channel: int) -> dict[str, str]:
        """``callback_url`` and ``user_email`` recorded when *channel* was triggered."""
