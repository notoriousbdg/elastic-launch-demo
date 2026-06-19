"""Service Manager — starts/stops all simulated services and generators."""

from __future__ import annotations

import logging
import threading
from typing import Any

from app.config import (
    SERVICES,
    ACTIVE_SCENARIO,
)
from app.telemetry import ESBulkClient, OTLPClient

logger = logging.getLogger("nova7.manager")


class ServiceManager:
    """Manages all service instances and log generators."""

    def __init__(
        self,
        chaos_controller,
        ctx=None,
        otlp_client: OTLPClient | None = None,
    ):
        self.chaos_controller = chaos_controller
        self._ctx = ctx  # ScenarioContext or None
        self.otlp = otlp_client or OTLPClient()
        self.es_bulk = ESBulkClient()
        self.services: dict[str, Any] = {}
        self._stop_event = threading.Event()

        # Generator threads
        self._generator_threads: list[threading.Thread] = []

        self._init_services()

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
        """Start log/trace/metrics generators as daemon threads."""
        from log_generators.trace_generator import run as run_traces
        from log_generators.host_metrics_generator import run as run_metrics
        from log_generators.nginx_log_generator import run as run_nginx
        from log_generators.mysql_log_generator import run as run_mysql
        from log_generators.k8s_metrics_generator import run as run_k8s
        from log_generators.nginx_metrics_generator import run as run_nginx_metrics
        from log_generators.vpc_flow_generator import run as run_vpc
        from log_generators.jvm_metrics_generator import run as run_jvm
        from log_generators.raw_access_log_generator import run as run_raw_access

        # Build scenario_data dict from context for scenario-dependent generators
        scenario_data = None
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

        # Trace generator needs chaos_controller and scenario_data
        trace_args = (self.otlp, self._stop_event, self.chaos_controller)
        trace_kwargs = {"scenario_data": scenario_data} if scenario_data else {}

        # Host metrics generator needs chaos_controller and scenario_data
        host_args = (self.otlp, self._stop_event)
        host_kwargs = {"scenario_data": scenario_data} if scenario_data else {}
        host_kwargs["chaos_controller"] = self.chaos_controller

        # K8s metrics generator needs chaos_controller and scenario_data
        k8s_args = (self.otlp, self._stop_event)
        k8s_kwargs = {"scenario_data": scenario_data} if scenario_data else {}
        k8s_kwargs["chaos_controller"] = self.chaos_controller

        # Common args/kwargs for generators that accept scenario_data
        common_args = (self.otlp, self._stop_event)
        common_kwargs = {"scenario_data": scenario_data} if scenario_data else {}

        # Chaos-aware kwargs for infra log generators (nginx, mysql, jvm)
        chaos_kwargs = dict(common_kwargs)
        chaos_kwargs["chaos_controller"] = self.chaos_controller

        # Raw access-log generator uses ESBulkClient instead of OTLPClient
        raw_access_args = (self.es_bulk, self._stop_event)
        raw_access_kwargs = {"scenario_data": scenario_data} if scenario_data else {}

        generators = [
            ("gen-traces", run_traces, trace_args, trace_kwargs),
            ("gen-host-metrics", run_metrics, host_args, host_kwargs),
            ("gen-k8s-metrics", run_k8s, k8s_args, k8s_kwargs),
            ("gen-jvm-metrics", run_jvm, common_args, chaos_kwargs),
            ("gen-vpc-flow", run_vpc, common_args, common_kwargs),
            ("gen-raw-access", run_raw_access, raw_access_args, raw_access_kwargs),
            ("gen-nginx", run_nginx, common_args, chaos_kwargs),
            ("gen-nginx-metrics", run_nginx_metrics, common_args, common_kwargs),
            ("gen-mysql", run_mysql, common_args, chaos_kwargs),
        ]
        for name, fn, args, kwargs in generators:
            t = threading.Thread(
                target=fn,
                args=args,
                kwargs=kwargs,
                name=name,
                daemon=True,
            )
            t.start()
            self._generator_threads.append(t)
            logger.info("Started generator thread: %s", name)

    def get_generator_status(self) -> dict[str, str]:
        """Return status of each generator thread."""
        return {
            t.name: "running" if t.is_alive() else "stopped"
            for t in self._generator_threads
        }

    def get_all_status(self) -> dict[str, Any]:
        return {name: svc.get_status() for name, svc in self.services.items()}
