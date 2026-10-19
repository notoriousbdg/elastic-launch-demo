"""InstanceRegistry — thread-safe dict of deployment_id -> ScenarioInstance.

Provides lookup, registration, and cleanup for simultaneously-running deployments.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from app.instance import ScenarioInstance

logger = logging.getLogger("nova7.registry")


class InstanceRegistry:
    """Thread-safe registry of running scenario instances."""

    def __init__(self):
        self._lock = threading.Lock()
        self._instances: dict[str, ScenarioInstance] = {}

    def register(self, deployment_id: str, instance: ScenarioInstance) -> None:
        """Register an instance, stopping any existing one with the same id.

        The old instance is stopped after the lock is released: stopping joins
        threads for several seconds and must not block other registry calls.
        """
        with self._lock:
            old = self._instances.get(deployment_id)
            instance.deployment_id = deployment_id
            self._instances[deployment_id] = instance
        if old and old is not instance:
            logger.info("Replacing existing instance %s", deployment_id)
            try:
                old.stop()
            except Exception:
                logger.exception("Error stopping old instance %s", deployment_id)

    def get(self, deployment_id: str) -> ScenarioInstance | None:
        """Get an instance by deployment id."""
        with self._lock:
            return self._instances.get(deployment_id)

    def remove(self, deployment_id: str) -> ScenarioInstance | None:
        """Remove and return an instance (does NOT stop it)."""
        with self._lock:
            return self._instances.pop(deployment_id, None)

    def all_instances(self) -> dict[str, ScenarioInstance]:
        """Return a snapshot of all registered instances."""
        with self._lock:
            return dict(self._instances)

    def first(self) -> ScenarioInstance | None:
        """Return the first registered instance, or None."""
        with self._lock:
            if self._instances:
                return next(iter(self._instances.values()))
            return None

    def stop_all(self) -> None:
        """Stop all registered instances."""
        with self._lock:
            instances = dict(self._instances)
            self._instances.clear()
        for dep_id, inst in instances.items():
            try:
                inst.stop()
                logger.info("Stopped instance %s", dep_id)
            except Exception:
                logger.exception("Error stopping instance %s", dep_id)

    def __len__(self) -> int:
        with self._lock:
            return len(self._instances)

    def __contains__(self, deployment_id: str) -> bool:
        with self._lock:
            return deployment_id in self._instances
