"""Service Manager — starts/stops all simulated services and generators."""

from __future__ import annotations

import logging
import threading
from typing import Any

from app.chaos.protocol import ChaosSource
from app.config import ACTIVE_SCENARIO
from app.telemetry import ESBulkClient, OTLPClient

logger = logging.getLogger("nova7.manager")


class ServiceManager:
    """Manages all service instances and log generators."""

    def __init__(
        self,
        chaos_controller: ChaosSource,
        ctx=None,
        otlp_client: OTLPClient | None = None,
    ):
        self.chaos_controller = chaos_controller
        self._ctx = ctx  # ScenarioContext or None
        self.otlp = otlp_client or OTLPClient()
        self.es_bulk = (
            ESBulkClient(
                elastic_url=ctx.elastic_url,
                api_key=ctx.elastic_api_key,
            )
            if ctx and ctx.elastic_url and ctx.elastic_api_key
            else ESBulkClient()
        )
        self.services: dict[str, Any] = {}
        self._stop_event = threading.Event()

        # Generator threads
        self._generator_threads: list[threading.Thread] = []

        self._init_services()

    @property
    def stop_event(self) -> threading.Event:
        return self._stop_event

    def _init_services(self) -> None:
        """Dynamically load and instantiate services from the active scenario."""
        from app.services.base_service import BaseService

        if self._ctx:
            scenario = self._ctx.scenario
        else:
            import os
            from scenario_engine import get_scenario

            active = os.environ.get("ACTIVE_SCENARIO", ACTIVE_SCENARIO)
            scenario = get_scenario(active)

        service_classes = scenario.get_service_classes()

        with BaseService._context_lock:
            BaseService.set_context(self._ctx)
            try:
                for cls in service_classes:
                    svc = cls(self.chaos_controller, self.otlp)
                    self.services[svc.SERVICE_NAME] = svc
            finally:
                BaseService.clear_context()

    def start_all(self) -> None:
        for svc in self.services.values():
            svc.start()
        self._start_generators()
        logger.info("All %d services + generators started", len(self.services))

    def stop_all(self) -> None:
        self._stop_event.set()
        for t in self._generator_threads:
            t.join(timeout=5)
        for svc in self.services.values():
            svc.stop()
        self.otlp.close()
        self.es_bulk.close()
        logger.info("All services and generators stopped")

    # ── Generators ────────────────────────────────────────────────────

    def _start_generators(self) -> None:
        """Start log/trace/metrics generators as daemon threads.

        The set of generators and their order comes from the generator registry.
        The active scenario's ``generators:`` block controls which generators
        are enabled and supplies domain-specific configuration for each one.
        """
        from generator_engine.registry import enabled_specs, load_entrypoint

        # Build scenario_data dict from context for scenario-dependent generators
        scenario_data = None
        scenario = None
        if self._ctx:
            scenario = self._ctx.scenario
            scenario_data = {
                "services": self._ctx.services,
                "channel_registry": self._ctx.channel_registry,
                "namespace": self._ctx.namespace,
                "hosts": scenario.hosts,
                "k8s_clusters": scenario.k8s_clusters,
                "service_topology": scenario.service_topology,
                "entry_endpoints": scenario.entry_endpoints,
                "db_operations": scenario.db_operations,
                "scenario": scenario,
            }

        # Resolve which generators to start.
        # enabled_specs handles None gracefully: it returns all default-enabled
        # generators when the scenario has no generators: block.
        specs = enabled_specs(scenario)

        # Build generator config lookup from the scenario generators: block
        gen_block: dict = {}
        if scenario is not None:
            try:
                gen_block = getattr(scenario, "generators", {}) or {}
            except Exception:
                gen_block = {}

        for spec in specs:
            try:
                fn = load_entrypoint(spec)
            except (ImportError, AttributeError) as exc:
                logger.error(
                    "Cannot load generator '%s' from %s: %s",
                    spec.id, spec.module, exc,
                )
                continue

            # Pick the right transport client
            client = self.es_bulk if spec.client == "es_bulk" else self.otlp

            # Build generator config from the scenario generators: block
            cfg = gen_block.get(spec.id, {}) or {}
            if isinstance(cfg, bool):
                cfg = {}

            # run(client, stop_event, *, scenario_data, chaos_controller)
            kwargs: dict[str, Any] = {}
            if scenario_data is not None:
                if cfg:
                    sd = dict(scenario_data)
                    sd["generator_config"] = cfg
                    kwargs["scenario_data"] = sd
                else:
                    kwargs["scenario_data"] = scenario_data

            if spec.wants_chaos:
                kwargs["chaos_controller"] = self.chaos_controller

            t = threading.Thread(
                target=fn,
                args=(client, self._stop_event),
                kwargs=kwargs,
                name=spec.thread_name,
                daemon=True,
            )
            t.start()
            self._generator_threads.append(t)
            logger.info("Started generator thread: %s", spec.thread_name)

    def get_generator_status(self) -> dict[str, str]:
        """Return status of each generator thread."""
        return {
            t.name: "running" if t.is_alive() else "stopped"
            for t in self._generator_threads
        }

    def get_all_status(self) -> dict[str, Any]:
        return {name: svc.get_status() for name, svc in self.services.items()}
