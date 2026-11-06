"""ScenarioInstance — self-contained runtime for one deployment.

Owns its own OTLPClient, ChaosController, ServiceManager, and stop_event.
Multiple instances can run simultaneously, each targeting a different
Elastic cluster with different scenario configurations.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from app.chaos.controller import ChaosController
from app.chaos.remediation_poller import RemediationPoller
from app.context import ScenarioContext
from app.services.manager import ServiceManager
from app.telemetry import OTLPClient

if TYPE_CHECKING:
    from app.store import ChaosStore

logger = logging.getLogger("nova7.instance")


class ScenarioInstance:
    """One running deployment: scenario + credentials + generators."""

    def __init__(self, ctx: ScenarioContext, chaos_store: ChaosStore | None = None):
        self.ctx = ctx
        self.scenario_id = ctx.scenario_id
        self.deployment_id = ctx.scenario_id  # default; can be overridden

        # Build per-instance OTLPClient
        self.otlp = OTLPClient(
            endpoint=ctx.otlp_endpoint or None,
            api_key=ctx.otlp_api_key or None,
            elastic_url=ctx.elastic_url or None,
        )

        # Per-instance chaos controller with this scenario's channel registry.
        # Wire the on_resolve hook to revert the APM ML model to its baseline
        # snapshot whenever the last active channel resolves — keeps the model
        # from drifting across iterative demo cycles.
        def _revert_ml_baseline():
            if not (ctx.elastic_url and ctx.elastic_api_key and ctx.namespace):
                return
            from elastic_config.deployer_apm import revert_apm_ml_baseline
            revert_apm_ml_baseline(ctx.elastic_url, ctx.elastic_api_key, ctx.namespace)

        self.chaos_controller = ChaosController(
            channel_registry=ctx.channel_registry,
            chaos_store=chaos_store,
            deployment_id=self.deployment_id,
            on_resolve=_revert_ml_baseline,
        )

        # ServiceManager — owns service threads + generator threads
        self.service_manager = ServiceManager(
            chaos_controller=self.chaos_controller,
            ctx=ctx,
            otlp_client=self.otlp,
        )

        # Remediation poller — only if ES credentials are available
        self.remediation_poller: RemediationPoller | None = None
        if ctx.elastic_url and ctx.elastic_api_key:
            self.remediation_poller = RemediationPoller(
                elastic_url=ctx.elastic_url,
                elastic_api_key=ctx.elastic_api_key,
                namespace=ctx.namespace,
                chaos_controller=self.chaos_controller,
                stop_event=self.service_manager.stop_event,
            )

        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    def start(self) -> None:
        """Start all services and generators for this deployment."""
        if self._running:
            logger.warning("Instance %s already running", self.scenario_id)
            return
        self.service_manager.start_all()
        if self.remediation_poller:
            self.remediation_poller.start()
        self._running = True
        logger.info(
            "Instance %s started (%d services)",
            self.scenario_id,
            len(self.service_manager.services),
        )

    def stop(self) -> None:
        """Stop all services and generators."""
        if not self._running:
            return
        self.service_manager.stop_all()
        self._running = False
        logger.info("Instance %s stopped", self.scenario_id)

    def reconfigure_otlp(self, endpoint: str, api_key: str) -> None:
        if endpoint:
            self.otlp.reconfigure(endpoint, api_key)

    @property
    def scenario(self):
        return self.ctx.scenario

    @property
    def namespace(self) -> str:
        return self.ctx.namespace

    @property
    def mission_id(self) -> str:
        return self.ctx.mission_id

    @property
    def scenario_name(self) -> str:
        return self.ctx.scenario.scenario_name

    @property
    def kibana_url(self) -> str:
        return self.ctx.kibana_url

    @property
    def elastic_url(self) -> str:
        return self.ctx.elastic_url

    @property
    def elastic_api_key(self) -> str:
        return self.ctx.elastic_api_key

    def elastic_connection(self):
        """Credentials a deployer needs: url, kibana, api key, scenario."""
        return (
            self.ctx.elastic_url,
            self.ctx.kibana_url,
            self.ctx.elastic_api_key,
            self.ctx.scenario,
        )

    def trigger_channel(self, channel: int, **kwargs):
        return self.chaos_controller.trigger(channel, **kwargs)

    def resolve_channel(self, channel: int, **kwargs):
        return self.chaos_controller.resolve(channel, **kwargs)

    def set_infra_spikes(self, spikes: dict) -> None:
        self.chaos_controller.set_infra_spikes(spikes)

    def infra_spikes(self) -> dict:
        return self.chaos_controller.get_infra_spikes()

    def chaos_status(self) -> dict:
        return self.chaos_controller.get_status()

    def channel_status(self, channel: int) -> dict:
        return self.chaos_controller.get_channel_status(channel)

    def validate_session(self, session_id: str, user_email: str = "") -> list:
        return self.chaos_controller.validate_session(session_id, user_email=user_email)

    def runtime_status(self) -> dict:
        return {
            "scenario": self.scenario_id,
            "mission_id": self.mission_id,
            "mission_name": self.scenario_name,
            "namespace": self.namespace,
            "services": self.service_manager.get_all_status(),
            "generators": self.service_manager.get_generator_status(),
            "chaos": self.chaos_status(),
        }


def start_deployment(
    scenario,
    *,
    deployment_id: str,
    registry,
    store,
    chaos_store,
    otlp_endpoint: str = "",
    otlp_api_key: str = "",
    elastic_url: str = "",
    elastic_api_key: str = "",
    kibana_url: str = "",
    cloud_api_key: str = "",
    persist: bool = True,
) -> ScenarioInstance:
    """Build, start, register, and optionally persist one deployment."""
    ctx = ScenarioContext.from_scenario(
        scenario,
        otlp_endpoint=otlp_endpoint,
        otlp_api_key=otlp_api_key,
        elastic_url=elastic_url,
        elastic_api_key=elastic_api_key,
        kibana_url=kibana_url,
    )
    instance = ScenarioInstance(ctx, chaos_store=chaos_store)
    instance.reconfigure_otlp(otlp_endpoint, otlp_api_key)
    instance.start()
    registry.register(deployment_id, instance)
    if persist:
        store.upsert(
            deployment_id=deployment_id,
            scenario_id=scenario.scenario_id,
            otlp_endpoint=otlp_endpoint,
            otlp_api_key=otlp_api_key,
            elastic_url=elastic_url,
            elastic_api_key=elastic_api_key,
            kibana_url=kibana_url,
            cloud_api_key=cloud_api_key,
        )
    return instance
