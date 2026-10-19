"""ScenarioContext — bundles all scenario-derived state + per-deployment credentials."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from scenario_engine.base import BaseScenario


@dataclass
class ScenarioContext:
    """Immutable snapshot of a scenario's configuration + deployment credentials.

    Replaces module-level globals (SERVICES, CHANNEL_REGISTRY, MISSION_ID, etc.)
    so multiple scenarios can run simultaneously with independent state.
    """

    scenario: BaseScenario
    scenario_id: str
    namespace: str
    mission_id: str
    services: dict[str, dict[str, Any]]
    channel_registry: dict[int, dict[str, Any]]

    # Per-deployment credentials (empty = use env defaults)
    otlp_endpoint: str = ""
    otlp_api_key: str = ""
    elastic_url: str = ""
    elastic_api_key: str = ""
    kibana_url: str = ""

    @classmethod
    def from_scenario(
        cls,
        scenario: BaseScenario,
        *,
        otlp_endpoint: str = "",
        otlp_api_key: str = "",
        elastic_url: str = "",
        elastic_api_key: str = "",
        kibana_url: str = "",
    ) -> ScenarioContext:
        """Build a context from a scenario instance + optional credentials."""
        return cls(
            scenario=scenario,
            scenario_id=scenario.scenario_id,
            namespace=scenario.namespace,
            mission_id=scenario.namespace.upper(),
            services=scenario.services,
            channel_registry=scenario.channel_registry,
            otlp_endpoint=otlp_endpoint,
            otlp_api_key=otlp_api_key,
            elastic_url=elastic_url,
            elastic_api_key=elastic_api_key,
            kibana_url=kibana_url,
        )

    def generator_data(self) -> dict[str, Any]:
        """The ``scenario_data`` dict every log generator's ``run()`` takes."""
        scenario = self.scenario
        return {
            "services": self.services,
            "channel_registry": self.channel_registry,
            "namespace": self.namespace,
            "hosts": scenario.hosts,
            "k8s_clusters": scenario.k8s_clusters,
            "service_topology": scenario.service_topology,
            "entry_endpoints": scenario.entry_endpoints,
            "db_operations": scenario.db_operations,
            "scenario": scenario,
        }


def standalone_generator_data() -> dict[str, Any]:
    """``scenario_data`` for running a generator on its own (``python -m log_generators.x``).

    Uses the first scenario in ``ACTIVE_SCENARIO`` (default ``space``).
    """
    from app.config import ACTIVE_SCENARIO
    from scenario_engine import get_scenario

    return ScenarioContext.from_scenario(get_scenario(ACTIVE_SCENARIO)).generator_data()
