"""Launcher — deploy a scenario to Elastic and bring its telemetry instance up.

Shared by the selector's ``/api/setup/launch`` and the env-var auto-deploy at
startup. Telemetry starts early, from the deployer's ``on_otlp_ready`` hook,
so dashboards populate while the slow tail of the deploy (ML, alerts, SLOs,
integrations) is still running. If the hook never fires, the instance is
started after the deploy instead.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

from app.context import ScenarioContext
from app.instance import ScenarioInstance

logger = logging.getLogger("nova7.launch")


@dataclass(frozen=True)
class Credentials:
    """Elastic connection details for one deployment."""

    kibana_url: str
    elastic_url: str
    api_key: str
    cloud_api_key: str = ""


class Launcher:
    """Starts scenario instances and records them in the registry and store."""

    def __init__(self, registry, store, chaos_store) -> None:
        self._registry = registry
        self._store = store
        self._chaos_store = chaos_store

    def start_instance(
        self,
        scenario: Any,
        deployment_id: str,
        creds: Credentials,
        otlp_endpoint: str,
    ) -> ScenarioInstance:
        """Create, start, register and persist an instance for *scenario*."""
        ctx = ScenarioContext.from_scenario(
            scenario,
            otlp_endpoint=otlp_endpoint or "",
            otlp_api_key=creds.api_key,
            elastic_url=creds.elastic_url,
            elastic_api_key=creds.api_key,
            kibana_url=creds.kibana_url,
        )
        instance = ScenarioInstance(ctx, chaos_store=self._chaos_store)
        if otlp_endpoint:
            instance.otlp.reconfigure(otlp_endpoint, creds.api_key)
        instance.start()
        self._registry.register(deployment_id, instance)
        self._store.upsert(
            deployment_id=deployment_id,
            scenario_id=scenario.scenario_id,
            otlp_endpoint=otlp_endpoint or "",
            otlp_api_key=creds.api_key,
            elastic_url=creds.elastic_url,
            elastic_api_key=creds.api_key,
            kibana_url=creds.kibana_url,
            cloud_api_key=creds.cloud_api_key,
        )
        return instance

    def deploy_and_start(
        self,
        deployer: Any,
        scenario: Any,
        deployment_id: str,
        creds: Credentials,
        *,
        otlp_override: str = "",
        otlp_fallback: str = "",
        progress_cb: Callable[[Any], None] | None = None,
    ) -> ScenarioInstance | None:
        """Run ``deployer.deploy_all()`` and make sure an instance is running after.

        OTLP endpoint priority: *otlp_override* → the deployer's endpoint →
        *otlp_fallback*. Returns the running instance, or None if it could not
        be started (the error is logged).
        """
        early: dict[str, Any] = {}

        def _on_otlp_ready(endpoint: str) -> None:
            ep = otlp_override or endpoint
            try:
                early["instance"] = self.start_instance(scenario, deployment_id, creds, ep)
                early["endpoint"] = ep
                logger.info("Instance %s started early after OTLP derive (%s)", deployment_id, ep)
            except Exception:
                logger.exception(
                    "Early start failed for %s; will retry after deploy_all", deployment_id
                )

        result = deployer.deploy_all(callback=progress_cb, on_otlp_ready=_on_otlp_ready)
        final_endpoint = otlp_override or result.otlp_endpoint or otlp_fallback

        instance = early.get("instance")
        if instance is not None:
            # Already running; re-point it if the verified endpoint changed.
            if final_endpoint and final_endpoint != early["endpoint"]:
                instance.otlp.reconfigure(final_endpoint, creds.api_key)
                logger.info("OTLP endpoint for %s updated post-deploy to %s",
                            deployment_id, final_endpoint)
            logger.info("Deployment %s live", deployment_id)
            return instance

        # The hook never fired (OTLP derivation failed or raised).
        try:
            instance = self.start_instance(scenario, deployment_id, creds, final_endpoint)
        except Exception:
            logger.exception("Failed to start instance for %s", deployment_id)
            return None
        logger.info("Deployment %s live", deployment_id)
        return instance
