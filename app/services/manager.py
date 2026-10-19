"""Service Manager — starts/stops all simulated services and generators."""

from __future__ import annotations

import logging
import threading
from typing import Any

from app.telemetry import ESBulkClient, OTLPClient

logger = logging.getLogger("nova7.manager")


class ServiceManager:
    """Manages all service instances and log generators."""

    def __init__(
        self,
        chaos_controller,
        ctx,
        otlp_client: OTLPClient | None = None,
    ):
        self.chaos_controller = chaos_controller
        self._ctx = ctx  # ScenarioContext
        self.otlp = otlp_client or OTLPClient()
        self.es_bulk = (
            ESBulkClient(
                elastic_url=ctx.elastic_url,
                api_key=ctx.elastic_api_key,
            )
            if ctx.elastic_url and ctx.elastic_api_key
            else ESBulkClient()
        )
        self.services: dict[str, Any] = {}
        self._stop_event = threading.Event()

        # Generator threads
        self._generator_threads: list[threading.Thread] = []

        self._init_services()

    def _init_services(self) -> None:
        """Instantiate one service per entry in the scenario's services/ directory."""
        for cls in self._ctx.scenario.get_service_classes():
            svc = cls(self.chaos_controller, self.otlp, self._ctx)
            self.services[svc.SERVICE_NAME] = svc

    def start_all(self) -> None:
        # stop_all() sets the event and closes the clients; start from a clean
        # slate so a stopped manager can be started again.
        self._stop_event = threading.Event()
        self._generator_threads = []
        self.otlp.ensure_open()
        self.es_bulk.ensure_open()
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

        scenario_data = self._ctx.generator_data()
        otlp_args = (self.otlp, self._stop_event)
        data_kwargs = {"scenario_data": scenario_data}
        chaos_kwargs = {**data_kwargs, "chaos_controller": self.chaos_controller}

        generators = [
            ("gen-traces", run_traces, otlp_args, chaos_kwargs),
            ("gen-host-metrics", run_metrics, otlp_args, chaos_kwargs),
            ("gen-k8s-metrics", run_k8s, otlp_args, chaos_kwargs),
            ("gen-jvm-metrics", run_jvm, otlp_args, chaos_kwargs),
            ("gen-vpc-flow", run_vpc, otlp_args, data_kwargs),
            # The raw access-log generator writes via ES _bulk, not OTLP.
            ("gen-raw-access", run_raw_access, (self.es_bulk, self._stop_event), data_kwargs),
            ("gen-nginx", run_nginx, otlp_args, chaos_kwargs),
            ("gen-nginx-metrics", run_nginx_metrics, otlp_args, data_kwargs),
            ("gen-mysql", run_mysql, otlp_args, chaos_kwargs),
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
