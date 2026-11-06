"""Base scenario class — all scenarios implement this interface."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Any


def _substitute_fault_params(notes: str, params: dict) -> str:
    """Safely substitute resolved fault_params into investigation_notes.

    Uses targeted per-key regex replacement so literal braces that are NOT
    fault_param placeholders (e.g. PowerShell ``@{ID=7031,7034}``) are never
    touched.  Only tokens of the exact form ``{key}`` where *key* is a known
    fault_params key are replaced; all other brace content is left intact.
    Falls back to the raw notes on any unexpected error.
    """
    try:
        result = notes
        for key, value in params.items():
            result = re.sub(r"\{" + re.escape(key) + r"\}", str(value), result)
        return result
    except Exception:
        return notes


def _field_hygiene_block(ns: str) -> str:
    """Return the CRITICAL: Field Names block, shared across triage and investigation skills."""
    return f"""## CRITICAL: Field Names

- Log message field is `body.text` — NEVER use `body` alone (causes "Unknown column [body]")
- NEVER use `message` — this field does not exist; the correct field is `body.text`
- Service name field is `service.name`
- Always query `FROM logs.otel.{ns},logs.otel.{ns}.*` (includes sub-streams)
- Use `LIKE` or `KQL()` for text matching — NEVER use `MATCH()`
- Traces for every scenario are in `traces-generic.otel-default` — there is no `traces.otel.{ns}`; filter on `service.name`
- Error log details: `attributes.error.type`, `attributes.exception.message`, `attributes.exception.stacktrace`; join logs to spans on `trace_id`"""


class BaseScenario(ABC):
    """Abstract base class that all scenarios must implement."""

    # ── Identity ──────────────────────────────────────────────────────

    @property
    @abstractmethod
    def scenario_id(self) -> str:
        """Unique key: 'space', 'fanatics', 'financial', etc."""
        ...

    @property
    @abstractmethod
    def scenario_name(self) -> str:
        """Display name: 'NOVA-7 Space Mission'."""
        ...

    @property
    @abstractmethod
    def scenario_description(self) -> str:
        """Card description for the scenario selector."""
        ...

    @property
    @abstractmethod
    def namespace(self) -> str:
        """ES/telemetry namespace prefix: 'nova7', 'fanatics', etc."""
        ...

    @property
    def scenario_icon(self) -> str:
        """Emoji icon displayed on the scenario selector card."""
        return "🔧"

    @property
    def sort_order(self) -> int:
        """Display order on the scenario selector. Lower numbers appear first. Default 999."""
        return 999

    @property
    def apm_ml_bucket_span(self) -> str:
        """Bucket span for the APM transaction-metrics ML anomaly detection job.

        Shorter spans surface anomaly scores faster (useful for short live demos)
        at the cost of more noise. Default "15m" matches the Kibana APM ML module.
        Override per scenario for faster demo flows.
        """
        return "15m"

    @property
    def executive_kpi_emitter_service_name(self) -> str | None:
        """`SERVICE_NAME` of the one microservice that emits synthetic `business.*` executive KPI gauges.

        Used by the Kibana Executive dashboard (Lens). Override in each scenario.
        """
        return None

    @property
    def executive_dashboard_intro(self) -> str:
        """Markdown intro shown at the top of the Executive Dashboard. Override in each scenario."""
        svc = self.executive_kpi_emitter_service_name or "executive-service"
        return (
            f"**Executive view** — cross-functional KPIs for {self.scenario_name} "
            f"(synthetic `business.*` from `{svc}`)."
        )

    @property
    def executive_kpi_sections(self) -> list[dict]:
        """KPI tile sections for the Executive Dashboard.

        Each dict: {"header": str, "specs": [(display_title, metrics_field), ...]}
        Override in each scenario with industry-appropriate metrics.
        """
        return []

    @property
    def executive_trend_charts(self) -> list[dict]:
        """Trend line charts for the Executive Dashboard (6, arranged 3×2).

        Each dict: {"title": str, "field": str, "y_label": str}
        Override in each scenario with the most important metrics to trend.
        """
        return []

    # ── Services & Topology ──────────────────────────────────────────

    @property
    @abstractmethod
    def services(self) -> dict[str, dict[str, Any]]:
        """9 service definitions with cloud/region/subsystem/language."""
        ...

    @property
    @abstractmethod
    def channel_registry(self) -> dict[int, dict[str, Any]]:
        """20 fault channels with error types, messages, stack traces."""
        ...

    @property
    @abstractmethod
    def service_topology(self) -> dict[str, list[tuple[str, str, str]]]:
        """Trace call graph: caller -> [(callee, endpoint, method)]."""
        ...

    @property
    @abstractmethod
    def entry_endpoints(self) -> dict[str, list[tuple[str, str]]]:
        """API endpoints per service: service -> [(path, method)]."""
        ...

    @property
    @abstractmethod
    def db_operations(self) -> dict[str, list[tuple[str, str, str]]]:
        """DB operations: service -> [(op, table, statement)]."""
        ...

    # ── Infrastructure ───────────────────────────────────────────────

    @property
    @abstractmethod
    def hosts(self) -> list[dict[str, Any]]:
        """3 host definitions (one per cloud)."""
        ...

    @property
    @abstractmethod
    def k8s_clusters(self) -> list[dict[str, Any]]:
        """3 K8s cluster definitions."""
        ...

    @property
    def nominal_label(self) -> str:
        """Status label for 'all clear'. Override for domain jargon (e.g. space uses 'NOMINAL')."""
        return "NORMAL"

    # ── Agent & Elastic Config ───────────────────────────────────────

    @property
    @abstractmethod
    def agent_config(self) -> dict[str, Any]:
        """Agent ID, name, system prompt, and assessment_tool_name for Agent Builder.

        Required keys:
          - id: agent ID (e.g. "finserv-trading-analyst")
          - name: display name (e.g. "Trading Operations Analyst")
          - system_prompt: identity + domain expertise text
          - assessment_tool_name: scenario-specific assessment tool name
            (e.g. "launch_safety_assessment", "trading_risk_assessment")
        """
        ...

    @property
    @abstractmethod
    def assessment_tool_config(self) -> dict[str, Any]:
        """Scenario-specific assessment tool definition.

        Returns a dict with keys: id, description.
        Example for space: {"id": "launch_safety_assessment",
                            "description": "GO/NO-GO launch readiness evaluation..."}
        """
        ...

    def prefixed_tool_id(self, base_id: str) -> str:
        """Stable per-scenario tool id so multiple scenarios can coexist in Agent Builder."""
        p = f"{self.namespace}_"
        if base_id.startswith(p):
            return base_id
        return f"{p}{base_id}"

    @property
    def tool_definitions(self) -> list[dict[str, Any]]:
        """Agent Builder tool configurations — auto-generated from scenario properties.

        Override in a subclass for fully custom tools.  By default generates
        6 generic tools + the scenario-specific assessment tool.
        """
        return self._default_tool_definitions()

    # ── Service Classes ──────────────────────────────────────────────

    @abstractmethod
    def get_service_classes(self) -> list[type]:
        """Return list of 9 service implementation classes."""
        ...

    # ── Fault Parameters ─────────────────────────────────────────────

    def get_trace_attributes(self, service_name: str, rng) -> dict:
        """Domain-specific attributes on ALL traces (always present)."""
        return {}

    def get_rca_clues(self, channel: int, service_name: str, rng) -> dict:
        """Partial RCA clues on traces for services in active fault channels.
        Different services get different clues — no single service has full picture."""
        return {}

    def get_correlation_attribute(self, channel: int, is_error: bool, rng) -> dict:
        """Attribute correlated with errors: appears on ~90% of error traces,
        ~5% of healthy traces. Discoverable via Elastic correlation analysis."""
        return {}

    @abstractmethod
    def get_fault_params(self, channel: int) -> dict[str, Any]:
        """Generate realistic random fault parameters for a channel."""
        ...

    def fault_param_ranges(self, channel: int) -> dict[str, str]:
        """Range of each fault parameter, for filling investigation_notes in docs."""
        return {}

    def rca_clue_ranges(self, channel: int, service_name: str) -> dict[str, str]:
        """Values each RCA clue can take on *service_name* while *channel* is active."""
        return {}

    # ── Convenience ──────────────────────────────────────────────────

    @property
    def cloud_groups(self) -> dict[str, list[str]]:
        """Group services by cloud provider."""
        groups: dict[str, list[str]] = {}
        for svc_name, svc_cfg in self.services.items():
            provider = svc_cfg["cloud_provider"]
            groups.setdefault(provider, []).append(svc_name)
        return groups

    @property
    def dashboard_cloud_groups(self) -> list[dict[str, Any]]:
        """Cloud groups for exec dashboard layout (AWS/GCP/Azure columns).

        Multi-provider scenarios (the common case) get one column per cloud
        provider (aws / gcp / azure).  Single-provider scenarios like gcpnet
        — where all clusters share the same cloud — get one column per cluster
        instead, so services don't pile up in a single column and overflow.
        """
        x_starts = [0, 16, 33]
        col_widths = [15, 16, 15]

        # Single-provider: use clusters as columns (up to 3)
        providers = {c["provider"] for c in self.k8s_clusters}
        if len(providers) == 1 and len(self.k8s_clusters) <= 3:
            groups = []
            for i, cluster in enumerate(self.k8s_clusters[:3]):
                region = cluster.get("region", "")
                label = f"**{cluster['provider'].upper()}** {region}"
                groups.append({
                    "label": label,
                    "services": cluster.get("services", []),
                    "x_start": x_starts[i],
                    "col_width": col_widths[i],
                    "cluster": cluster.get("name", ""),
                })
            return groups

        # Multi-provider: one column per cloud provider (aws / gcp / azure)
        cloud_order = ["aws", "gcp", "azure"]
        groups = []
        for i, provider in enumerate(cloud_order):
            svcs = self.cloud_groups.get(provider, [])
            cluster = next(
                (c for c in self.k8s_clusters if c["provider"] == provider), {}
            )
            groups.append(
                {
                    "label": f"**{provider.upper()}** {cluster.get('region', '')}",
                    "services": svcs,
                    "x_start": x_starts[i],
                    "col_width": col_widths[i],
                    "cluster": cluster.get("name", ""),
                }
            )
        return groups

    @property
    def workflow_template_inputs(self) -> dict[str, Any]:
        """Input values for library investigation workflows (``elastic_config/workflow_templates.py``).

        Flat ``{input_name: value}`` entries apply to every deployed library workflow
        that declares that manual input; a ``{workflow_slug: {input_name: value}}``
        entry overrides them for one workflow. Generators supply resource ids
        (``load_balancer``, ``db_instance_identifier``, …) themselves through
        ``workflow_input_values``; use this for scenario semantics such as the
        dependent ``service_name``.
        """
        return {}

    @property
    def raw_log_profile(self) -> dict[str, Any]:
        """Profile for the raw ECS access-log generator.

        Drives a per-scenario unparsed `logs.ecs.{ns}` stream so SAs can demo
        Streams partition/parsing on the fly and use the visualization-creation
        skill to build dashboards from domain-relatable raw data.

        Override per scenario for industry-appropriate paths, user-id prefix,
        tier field, and the change-point path that spikes during simulated faults.
        """
        return {
            "service_name": f"{self.namespace}-edge-gateway",
            "user_id_prefix": "u",
            "tier_field": "user_tier",
            "tier_values": [("free", 75), ("pro", 20), ("enterprise", 5)],
            "country_weights": {"US": 35, "GB": 12, "DE": 12, "FR": 8, "JP": 8, "IN": 10, "BR": 8, "CA": 7},
            "methods": ["GET", "POST", "PUT", "DELETE"],
            "paths": ["/api/v1/health"],
            "change_point_path": "/api/v1/health",
        }

    @property
    def llm_profile(self) -> dict[str, Any]:
        """Profile for the LLM provider telemetry generators.

        Drives scenario-branded data for the AWS Bedrock, Azure OpenAI and
        Google Vertex AI integrations.  All values that a reader sees in
        Discover come from this profile.  Field names, metric names and provider
        enum values (``end_turn``, ``ThrottlingException``, etc.) are fixed by
        the package schema and never change between scenarios.

        Override per scenario with industry-appropriate prompt text, model
        deployments, guardrail definitions and caller personas.  The base
        implementation uses namespace-derived values that are syntactically valid
        but not domain-specific.
        """
        ns = self.namespace
        from generator_engine.identity import ScenarioIdentity

        ident = ScenarioIdentity(ns)
        aws_account = ident.aws_account_id
        gcp_project = ident.gcp_project_id
        azure_subscription = ident.azure_subscription_id

        return {
            # ── AWS Bedrock ───────────────────────────────────────────────
            "aws_account_id": aws_account,
            "aws_account_name": ident.label("aws-account"),
            "aws_region": "us-east-1",
            "bedrock_models": [
                "us.anthropic.claude-haiku-4-5-20251001-v1:0",
                "us.anthropic.claude-sonnet-5",
                "amazon.titan-image-generator-v2:0",
            ],
            "bedrock_guardrail_id": ident.label("pii-guardrail"),
            "bedrock_guardrail_arn": (
                f"arn:aws:bedrock:us-east-1:{aws_account}:guardrail/{ns}-guardrail-01"
            ),
            "bedrock_guardrail_version": "1",
            # Scenario-branded prompt text; each entry is one user turn.
            "prompt_templates": [
                f"What is the status of my account with {ns}?",
                f"Can you help me understand my recent transactions on {ns}?",
                f"I need assistance with a dispute for {ns}.",
            ],
            "response_templates": [
                f"I can help you with your {ns} account. Let me look that up.",
                f"Your recent {ns} transactions show normal activity.",
                f"I'll open a dispute case for your {ns} account now.",
            ],
            # Caller personas used for client.user.email and source.geo
            "callers": [
                {
                    "email": f"user.01@{ns}.internal",
                    "country": "US",
                    "city": "New York",
                    "region": "New York",
                    "lat": 40.7128,
                    "lon": -74.0060,
                },
                {
                    "email": f"user.02@{ns}.internal",
                    "country": "US",
                    "city": "Chicago",
                    "region": "Illinois",
                    "lat": 41.8781,
                    "lon": -87.6298,
                },
                {
                    "email": f"user.03@{ns}.internal",
                    "country": "GB",
                    "city": "London",
                    "region": "England",
                    "lat": 51.5074,
                    "lon": -0.1278,
                },
                {
                    "email": f"user.04@{ns}.internal",
                    "country": "DE",
                    "city": "Frankfurt",
                    "region": "Hesse",
                    "lat": 50.1109,
                    "lon": 8.6821,
                },
                {
                    "email": f"user.05@{ns}.internal",
                    "country": "SG",
                    "city": "Singapore",
                    "region": "Central Singapore",
                    "lat": 1.3521,
                    "lon": 103.8198,
                },
                {
                    "email": f"user.06@{ns}.internal",
                    "country": "JP",
                    "city": "Tokyo",
                    "region": "Tokyo",
                    "lat": 35.6762,
                    "lon": 139.6503,
                },
                {
                    "email": f"user.07@{ns}.internal",
                    "country": "IN",
                    "city": "Mumbai",
                    "region": "Maharashtra",
                    "lat": 19.0760,
                    "lon": 72.8777,
                },
                {
                    "email": f"user.08@{ns}.internal",
                    "country": "AU",
                    "city": "Sydney",
                    "region": "New South Wales",
                    "lat": -33.8688,
                    "lon": 151.2093,
                },
                {
                    "email": f"user.09@{ns}.internal",
                    "country": "BR",
                    "city": "Sao Paulo",
                    "region": "Sao Paulo",
                    "lat": -23.5505,
                    "lon": -46.6333,
                },
                {
                    "email": f"user.10@{ns}.internal",
                    "country": "AE",
                    "city": "Dubai",
                    "region": "Dubai",
                    "lat": 25.2048,
                    "lon": 55.2708,
                },
            ],
            # ── Azure OpenAI ──────────────────────────────────────────────
            "azure_subscription_id": azure_subscription,
            "azure_subscription_name": f"{ns.title()} Demo Azure",
            "azure_resource_group": ns,
            "azure_openai_resources": [ident.label("openai-eastus")],
            "azure_openai_models": ["gpt-4o", "gpt-4.1-mini"],
            "azure_region": "eastus",
            # ── Google Vertex AI ──────────────────────────────────────────
            "gcp_project_id": gcp_project,
            "gcp_region": "us-central1",
            "gcp_regions": [
                "us-central1",
                "us-east1",
                "us-west1",
                "northamerica-northeast1",
                "europe-west1",
                "europe-west2",
                "europe-west3",
                "europe-west4",
                "asia-southeast1",
                "asia-northeast1",
                "asia-south1",
                "australia-southeast1",
                "southamerica-east1",
                "me-central1",
            ],
            "vertex_models": ["gemini-2.5-flash", "gemini-2.5-pro"],
        }

    # ── Generator configuration ──────────────────────────────────────

    @property
    def generators(self) -> dict[str, Any]:
        """Raw ``generators:`` block from the scenario YAML.

        Returns an empty dict when the block is absent.  Subclasses that read
        the scenario YAML override this property.

        Keys are generator ids (``"postgresql"``, ``"mysql"``, etc.).
        Values are the per-generator config dict, which may include an
        ``enabled`` boolean and any config fields declared by the generator spec.
        """
        return {}

    def get_generator_config(self, gen_id: str) -> dict[str, Any]:
        """Return the config dict for a single generator.

        Returns an empty dict when the generator is not mentioned in the
        ``generators:`` block.  Never raises.
        """
        cfg = self.generators.get(gen_id, {})
        if isinstance(cfg, bool):
            return {}
        return cfg or {}

    # ── Default Tool Generation ──────────────────────────────────────

    def _default_tool_definitions(self) -> list[dict[str, Any]]:
        """Generate the standard 7 agent tools from scenario properties."""
        svc_names = ", ".join(sorted(self.services.keys()))
        kb_index = f"{self.namespace}-knowledge-base"

        registry_values = list(self.channel_registry.values())
        example_error = (
            registry_values[0]["error_type"] if registry_values else "SomeException"
        )

        tools = [
            {
                "id": self.prefixed_tool_id("search_error_logs"),
                "type": "esql",
                "description": (
                    f"Search telemetry logs for a specific error or exception type. "
                    f"Returns the 50 most recent ERROR-level log entries matching the "
                    f"error type. Services: {svc_names}. "
                    f"The error_type parameter is matched against body.text."
                ),
                "configuration": {
                    "query": (
                        f"FROM logs.otel.{self.namespace},logs.otel.{self.namespace}.* "
                        "| WHERE @timestamp > NOW() - 15 MINUTES "
                        'AND body.text LIKE ?error_type AND severity_text == "ERROR" '
                        "| KEEP @timestamp, body.text, service.name, severity_text, event_name "
                        "| SORT @timestamp DESC | LIMIT 50"
                    ),
                    "params": {
                        "error_type": {
                            "description": f"Wildcard pattern for the error type, e.g. *{example_error}*",
                            "type": "string",
                            "optional": False,
                        }
                    },
                },
            },
            {
                "id": self.prefixed_tool_id("search_subsystem_health"),
                "type": "esql",
                "description": (
                    f"Query health status by aggregating recent telemetry. "
                    f"Returns error/warning counts per service. "
                    f"Services: {svc_names}. "
                    f"Log message field: body.text (never use 'body' alone)."
                ),
                "configuration": {
                    "query": (
                        f"FROM logs.otel.{self.namespace},logs.otel.{self.namespace}.* "
                        "| WHERE @timestamp > NOW() - 15 MINUTES "
                        '| STATS error_count = COUNT(*) WHERE severity_text == "ERROR", '
                        'warn_count = COUNT(*) WHERE severity_text == "WARN", '
                        "total = COUNT(*) BY service.name "
                        "| SORT error_count DESC"
                    ),
                    "params": {},
                },
            },
            {
                "id": self.prefixed_tool_id("search_service_logs"),
                "type": "esql",
                "description": (
                    f"Search telemetry logs for a specific service. "
                    f"Returns the 50 most recent ERROR and WARN entries. "
                    f"Available services: {svc_names}."
                ),
                "configuration": {
                    "query": (
                        f"FROM logs.otel.{self.namespace},logs.otel.{self.namespace}.* "
                        "| WHERE @timestamp > NOW() - 15 MINUTES "
                        "AND service.name == ?service_name "
                        'AND severity_text IN ("ERROR", "WARN") '
                        "| KEEP @timestamp, body.text, service.name, severity_text "
                        "| SORT @timestamp DESC | LIMIT 50"
                    ),
                    "params": {
                        "service_name": {
                            "description": f"The service to investigate ({svc_names})",
                            "type": "string",
                            "optional": False,
                        }
                    },
                },
            },
            {
                "id": self.prefixed_tool_id("search_known_anomalies"),
                "type": "index_search",
                "description": (
                    f"Search the knowledge base for documented anomalies, failure "
                    f"patterns, and resolution procedures. Contains RCA guides for "
                    f"all 20 fault channels."
                ),
                "configuration": {
                    "pattern": kb_index,
                },
            },
            {
                "id": self.prefixed_tool_id("trace_anomaly_propagation"),
                "type": "esql",
                "description": (
                    "Trace the propagation path of anomalies across services over time. "
                    "Returns a time series (1-minute buckets, last 15 minutes) showing "
                    "error and warning counts per service — summarise the output rather "
                    "than quoting all rows. Use to identify cascade chains and when each "
                    "service started degrading. "
                    "Note: search_subsystem_health intentionally omits the severity "
                    "pre-filter to report total log volume alongside errors. "
                    "Log message field: body.text (never use 'body' alone)."
                ),
                "configuration": {
                    "query": (
                        f"FROM logs.otel.{self.namespace},logs.otel.{self.namespace}.* "
                        "| WHERE @timestamp > NOW() - 15 MINUTES "
                        'AND severity_text IN ("ERROR", "WARN") '
                        "| EVAL bucket = DATE_TRUNC(1 minute, @timestamp) "
                        '| STATS error_count = COUNT(*) WHERE severity_text == "ERROR", '
                        'warn_count = COUNT(*) WHERE severity_text == "WARN" '
                        "BY service.name, bucket "
                        "| SORT bucket ASC, error_count DESC | LIMIT 100"
                    ),
                    "params": {},
                },
            },
            {
                "id": self.prefixed_tool_id("browse_recent_errors"),
                "type": "esql",
                "description": (
                    "Browse all recent ERROR and WARN log entries across all services. "
                    "Use for general situation awareness when you do not yet know the "
                    "specific error type or service."
                ),
                "configuration": {
                    "query": (
                        f"FROM logs.otel.{self.namespace},logs.otel.{self.namespace}.* "
                        "| WHERE @timestamp > NOW() - 15 MINUTES "
                        'AND severity_text IN ("ERROR", "WARN") '
                        "| KEEP @timestamp, body.text, service.name, severity_text "
                        "| SORT @timestamp DESC | LIMIT 50"
                    ),
                    "params": {},
                },
            },
        ]

        tools.append(
            {
                "id": self.prefixed_tool_id("verify_recovery"),
                "type": "esql",
                "description": (
                    "Confirm whether a fault has cleared after remediation. Returns "
                    "error/warning counts per service over a short recent window (last "
                    "3 minutes only) — zero results, or all counts at zero, means the "
                    "system has recovered. Use this instead of the 15-minute tools when "
                    "checking remediation/recovery status: the 15-minute tools are for "
                    "investigating active incidents and will keep showing pre-fix errors "
                    "as residual history for several minutes after a fix lands."
                ),
                "configuration": {
                    "query": (
                        f"FROM logs.otel.{self.namespace},logs.otel.{self.namespace}.* "
                        "| WHERE @timestamp > NOW() - 3 MINUTES "
                        'AND severity_text IN ("ERROR", "WARN") '
                        '| STATS error_count = COUNT(*) WHERE severity_text == "ERROR", '
                        'warn_count = COUNT(*) WHERE severity_text == "WARN" '
                        "BY service.name | SORT error_count DESC"
                    ),
                    "params": {},
                },
            }
        )

        # Add scenario-specific assessment tool
        assessment = self.assessment_tool_config
        tools.append(
            {
                "id": self.prefixed_tool_id(assessment["id"]),
                "type": "esql",
                "description": assessment["description"],
                "configuration": {
                    "query": (
                        f"FROM logs.otel.{self.namespace},logs.otel.{self.namespace}.* "
                        "| WHERE @timestamp > NOW() - 15 MINUTES "
                        'AND severity_text IN ("ERROR", "WARN") '
                        '| STATS error_count = COUNT(*) WHERE severity_text == "ERROR", '
                        'warn_count = COUNT(*) WHERE severity_text == "WARN" '
                        "BY service.name | SORT error_count DESC"
                    ),
                    "params": {},
                },
            }
        )

        return tools

    # ── Skill Definitions ────────────────────────────────────────────

    @property
    def builtin_skill_names(self) -> list[str]:
        """Names/IDs of built-in Agent Builder skills to discover and attach."""
        return [
            "visualization-creation",
            "observability.investigation",
            "observability.investigate-service-map",
        ]

    @property
    def skill_definitions(self) -> list[dict[str, Any]]:
        """Custom skill definitions deployed alongside agent tools."""
        ns = self.namespace
        agent_cfg = self.agent_config
        assessment_tool = self.prefixed_tool_id(agent_cfg.get(
            "assessment_tool_name",
            self.assessment_tool_config.get("id", "operational_assessment"),
        ))
        p_search_error       = self.prefixed_tool_id("search_error_logs")
        p_search_service     = self.prefixed_tool_id("search_service_logs")
        p_browse_recent      = self.prefixed_tool_id("browse_recent_errors")
        p_subsystem_health   = self.prefixed_tool_id("search_subsystem_health")
        p_search_known       = self.prefixed_tool_id("search_known_anomalies")
        p_trace_anomaly      = self.prefixed_tool_id("trace_anomaly_propagation")
        p_remediation_action = self.prefixed_tool_id("remediation_action")
        p_escalation_action  = self.prefixed_tool_id("escalation_action")
        p_verify_recovery    = self.prefixed_tool_id("verify_recovery")

        return [
            self._triage_skill(
                ns, p_browse_recent, p_subsystem_health, p_trace_anomaly, assessment_tool,
            ),
            self._investigation_playbook_skill(ns, p_search_error, p_search_service, p_search_known),
            self._channel_runbook_skill(ns, p_search_known),
            self._remediation_guide_skill(
                ns, p_remediation_action, p_escalation_action, p_verify_recovery,
            ),
        ]

    def _triage_skill(
        self,
        ns: str,
        p_browse_recent: str,
        p_subsystem_health: str,
        p_trace_anomaly: str,
        assessment_tool: str,
    ) -> dict[str, Any]:
        description = (
            f"Use at incident start when no error signature or service has been "
            f"identified: initial situational awareness, current operational status, "
            f"and structured triage sequencing across all subsystems. "
            f"Do not use for targeted investigation of a known error type or service — "
            f"use {ns}-investigation-playbook once a signature is identified. "
            f"Do not use these 15-minute tools to judge whether a fix has taken effect."
        )
        content = f"""{_field_hygiene_block(ns)}

## When to use

Use this procedure when:
- An alert has been received and the affected service or error signature is not yet known
- An operator or workflow requests current operational status across the environment
- You need to establish blast radius before narrowing to a specific subsystem

Do not use when you already have a specific error type or service to investigate — proceed to `{ns}-investigation-playbook` instead.

## Assessment sequence

Work through these tools in order. Each answers a different operational question.

1. **`{p_browse_recent}`** — What is actively failing right now across all services? Returns the most recent ERROR and WARN entries fleet-wide. Identifies which services and error patterns are present in the current window.

2. **`{p_subsystem_health}`** — How severe is the degradation? Returns aggregated error and warning counts ranked by service. Note: this tool reports total log volume alongside error counts — a high total with low errors is normal for a high-throughput service.

3. **`{p_trace_anomaly}`** — When did each service start degrading and in what order? Returns 1-minute time-bucketed error counts per service (last 15 minutes). Summarise the trend rather than quoting all rows; use it to identify which service degraded first and whether errors are spreading.

4. **`{assessment_tool}`** — What is the overall operational readiness? Provides a summary of current system health across all monitored services.

## Handoff

Once an error signature or specific service is identified, continue the investigation in `{ns}-investigation-playbook`.

For the documented procedure for a known failure mode, consult `{ns}-channel-runbook`.

## Scope limits

- Assessment only. Do not execute remediation from this procedure.
- Do not use these 15-minute tools to judge whether a fix has taken effect — use the `{ns}-remediation-guide` skill for that.
"""
        return {
            "id": f"{ns}-triage",
            "name": f"{self.scenario_name} Triage",
            "description": description,
            "content": content,
            "tool_ids": [p_browse_recent, p_subsystem_health, p_trace_anomaly, assessment_tool],
            "agent_prompt_hint": (
                f"Use **{ns}-triage** at incident start — no error signature or service "
                f"identified yet, or when current operational status is requested."
            ),
        }

    def _investigation_playbook_skill(
        self,
        ns: str,
        p_search_error: str,
        p_search_service: str,
        p_search_known: str,
    ) -> dict[str, Any]:
        description = (
            f"Use when you have a specific error signature or service to investigate: "
            f"search error logs by type, query logs for a named service, or look up "
            f"documented failure patterns in the knowledge base. Provides ES|QL field "
            f"guidance, structured RCA methodology, and evidence link formatting. "
            f"Do not use for initial situational awareness when no signature is identified — "
            f"use {ns}-triage first."
        )
        providers = sorted(self.cloud_groups)
        _names = {"aws": "AWS", "gcp": "GCP", "azure": "Azure"}
        if len(providers) > 1:
            listed = ", ".join(_names.get(p, p) for p in providers[:-1])
            tracing_step = (
                f"**Cross-Cloud Tracing**: Trace propagation across {listed} and "
                f"{_names.get(providers[-1], providers[-1])}"
            )
        else:
            tracing_step = "**Service Tracing**: Trace propagation across the service topology"
        content = f"""{_field_hygiene_block(ns)}

## Tool Selection Guide

1. **Known error type** → `{p_search_error}` — parameterised, returns matching ERROR logs for the past 15 minutes
2. **Specific service** → `{p_search_service}` — parameterised by service name, returns recent ERROR and WARN entries
3. **Historical patterns** → `{p_search_known}` — knowledge base lookup; required during alert-driven RCA

For initial situational awareness or cascade analysis, use `{ns}-triage` first.

Built-in observability tools (from `observability.investigation`) are first-class: use them for topology, change points, and correlations. Parameterised log tools remain valid, especially for a known `error_type`. Custom ES|QL is allowed when a builtin or parameterised tool cannot answer the question; include a query in the report only if you actually ran it.

## Time windows

When an alert payload is present, time-scope Discover/APM links and second-pass queries to the **alert** window (start/end from the alert), not `NOW()`. The 15-minute parameterised tools are fine as a first look while the alert is fresh.

## Root Cause Analysis Methodology

1. **Identify the Event**: Determine which channel(s) triggered and the error signature
2. **Scope the Blast Radius**: Identify affected and cascade services
3. **Temporal Correlation**: Find first occurrence, correlate with preceding events
4. {tracing_step}
5. **Subsystem Impact**: Evaluate if fault is isolated or propagating
6. **Known Pattern Matching**: Check knowledge base for similar anomalies
7. **Confirm with evidence**: Query the channel's **Evidence** span attributes (listed per channel in `{ns}-channel-runbook`) in `traces-generic.otel-default` and compare their values with the thresholds in its Vendor Investigation notes
8. **Severity Classification**: ADVISORY, CAUTION, WARNING, or CRITICAL

Use `confirmed` only when evidence directly demonstrates the cause; otherwise `likely`, `probable`, or `is consistent with`. Distinguish root cause, contributing factor, downstream impact, and correlation.

## Response Format

Begin with `## What This Means`. Then:

1. **What This Means** — 2–4 plain-English sentences
2. **Alert Summary** — table with linked service names
3. **Executive Summary** — root cause, impact, blast radius, cascade/cross-cloud
4. **Quick Links** — service, logs, key trace, dashboards, View Full Conversation
5. **Recommended Actions** — Immediate / Short-Term
6. **Technical Detail** — cascade, key findings, possible root causes (with Gaps), timeline
7. **Evidence & Validated Queries** — each item MUST include an **Evidence links** subsection with at least one real Markdown hyperlink `[label](/app/...)`. Plain-text "confirmed via logs" is not allowed.

Construct links from identifiers you observed (do not invent IDs):
- Service: `/app/apm/services/{{name}}`
- Service logs: `/app/apm/services/{{name}}/logs`
- Trace: `/app/apm/link-to/trace/{{id}}`
- Discover logs (alert window): `/app/discover#/?_g=(time:(from:'{{start}}',to:'{{end}}'))&_a=(index:'logs.otel.{ns}',query:(language:kuery,query:'{{kql}}'))`
- Discover traces: same pattern with `traces-generic.otel-default`, adding `service.name` to the KQL
- Operations dashboard: `/app/dashboards#/view/{ns}-exec-dashboard`
- Executive dashboard: `/app/dashboards#/view/{ns}-business-exec-dashboard`
"""
        return {
            "id": f"{ns}-investigation-playbook",
            "name": f"{self.scenario_name} Investigation Playbook",
            "description": description,
            "content": content,
            "tool_ids": [p_search_error, p_search_service, p_search_known],
            "agent_prompt_hint": (
                f"Use **{ns}-investigation-playbook** when you have a specific error "
                f"type or service to investigate: error log search, service log query, "
                f"knowledge base lookup, RCA methodology, and evidence link formatting."
            ),
        }

    def _channel_runbook_skill(self, ns: str, p_search_known: str) -> dict[str, Any]:
        registry = self.channel_registry
        description = (
            f"Use to look up the documented procedure for a known fault channel: "
            f"maps error types to action_type values for the remediation tool. "
            f"Do not use for initial assessment or investigation — use {ns}-triage "
            f"or {ns}-investigation-playbook first. "
            f"Do not use to execute remediation — use {ns}-remediation-guide for that."
        )
        # Build channel summary table
        rows = []
        for ch_num, ch in sorted(registry.items()):
            rows.append(
                f"| {ch_num} | {ch['name']} | `{ch['error_type']}` "
                f"| {ch.get('remediation_action', 'N/A')} |"
            )
        table = "\n".join(rows)

        # Build per-channel runbook sections inline (avoids referenced_content format issues)
        channel_sections = []
        for ch_num, ch in sorted(registry.items()):
            ch_name = ch["name"]
            error_type = ch["error_type"]
            subsystem = ch.get("subsystem", "unknown")
            affected = ", ".join(ch.get("affected_services", []))
            cascade = ", ".join(ch.get("cascade_services", []))
            description_text = ch.get("description", "")
            remediation_action = ch.get("remediation_action", "remediate")
            investigation_notes = ch.get("investigation_notes", "")
            if investigation_notes:
                investigation_notes = _substitute_fault_params(
                    investigation_notes, self.fault_param_ranges(ch_num)
                )
            investigation_section = (
                f"\n**Vendor Investigation:** {investigation_notes}\n"
                if investigation_notes else ""
            )
            clue_parts = []
            for svc in dict.fromkeys(
                ch.get("affected_services", []) + ch.get("cascade_services", [])
            ):
                clues = self.rca_clue_ranges(int(ch_num), svc)
                if clues:
                    clue_parts.append(
                        f"{svc} " + ", ".join(f"`{k}` ({v})" for k, v in clues.items())
                    )
            evidence_line = (
                "- **Evidence** (span attributes reported only while this fault is active): "
                + "; ".join(clue_parts) + "\n"
                if clue_parts else ""
            )
            channel_sections.append(
                f"### Channel {ch_num}: {ch_name}\n"
                f"- **Error Type**: `{error_type}` — subsystem: {subsystem}\n"
                f"- **Affected**: {affected} | **Cascade**: {cascade}\n"
                f"- **Description**: {description_text}\n"
                f"- **Remediation action_type**: `{remediation_action}` (channel: {ch_num})\n"
                f"{evidence_line}"
                f"{investigation_section}"
            )
        channels_content = "\n".join(channel_sections)

        content = f"""# {self.scenario_name} Fault Channel Reference

Use `{p_search_known}` for dynamic keyword searches across the knowledge base.
The error type appears in `body.text` log entries — search with `LIKE *ERROR_TYPE*`.
The `remediation_action` value below is the `action_type` to pass to the remediation tool.

## Channel Summary

| # | Name | Error Type | Remediation |
|---|------|-----------|-------------|
{table}

## Channel Runbooks

{channels_content}
"""
        return {
            "id": f"{ns}-channel-runbook",
            "name": f"{self.scenario_name} Channel Runbook",
            "description": description,
            "content": content,
            "tool_ids": [p_search_known],
            "agent_prompt_hint": (
                f"Use **{ns}-channel-runbook** to look up the documented procedure for "
                f"a known fault channel or identify the action_type for the remediation tool."
            ),
        }

    def _remediation_guide_skill(
        self,
        ns: str,
        p_remediation_action: str,
        p_escalation_action: str,
        p_verify_recovery: str,
    ) -> dict[str, Any]:
        description = (
            f"Use when ready to execute remediation, escalate an incident, manage an "
            f"operational hold, or confirm that a fix resolved the fault. Requires a "
            f"confirmed root cause and a known action_type from the channel runbook. "
            f"Do not use during investigation — use {ns}-triage or "
            f"{ns}-investigation-playbook first."
        )
        content = f"""## Remediation Decision Framework

### When to Remediate
Execute remediation when:
- Root cause is identified and confirmed
- The `action_type` is known (from the channel runbook)
- The user explicitly requests it

### When to Escalate
Use escalation when:
- Severity is CRITICAL and multiple subsystems are affected
- Automated remediation may trigger cascade effects
- A hold decision is needed before proceeding

## How to Remediate

Use `{p_remediation_action}` with these parameters:
- `action_type` — from the channel runbook (e.g. `recalibrate_engine`, `reset_fuel_system`)
- `channel` — the fault channel number (1–20)
- `error_type` — the error type identifier
- `justification` — brief explanation
- `dry_run` — `false` for actual remediation (required for the remediation service to resolve the fault)
- `case_id` — **REQUIRED whenever a case exists for this incident.** Pass the case ID
  you retrieved earlier in the conversation; never rely on tag-based search to find the case.
- `approval_mode` — **string** `required` or `skip`; see HITL vs auto-remediate rules below

Once `{p_remediation_action}` returns successfully, remediation has been executed —
but do not declare the incident resolved yet.
Do NOT execute remediation unless the user or workflow explicitly asks you to invoke the tool.

## Case Handling — read before calling {p_remediation_action}

- **Never call {p_remediation_action} with no case in context.** If you weren't handed
  a case ID and don't have one open in this conversation, use `platform.core.cases` to
  find or open the matching case for the `channel`/`error_type` you're about to remediate
  *before* invoking {p_remediation_action} — or ask the operator to confirm there's no
  case to close. Do not invoke the tool caseless "to be safe"; an empty `case_id` closes
  nothing and the incident's real case is left open.
- **The `channel`/`error_type` you remediate must match the case you're working.** If
  your root-cause analysis points to a different upstream channel than the one the open
  case is about, do not silently remediate that other channel instead. Say so explicitly,
  get the operator's confirmation, and — if they agree — remediate it against *that*
  channel's own case (find/open one), not the original case.
- **Never leave the case you were handed unresolved.** After remediating, the case you
  were given must end up commented and closed, or explicitly left open with a reason. If
  you pivot to a different root-cause channel, still resolve the original case's status —
  don't just abandon it.

## How to Confirm Recovery

The fix typically propagates within 1–2 minutes. When the user asks whether remediation
was successful, or before you report an incident as resolved:

1. Wait for propagation if the remediation tool call just returned (a minute or two is enough).
2. Call `{p_verify_recovery}` — it checks only the last few minutes, so it reflects current
   state rather than the incident's full history.
3. If `{p_verify_recovery}` shows zero (or near-zero) error/warn counts for the affected
   services, report the incident as **resolved**.
4. Do NOT use the 15-minute tools in the triage and investigation skills to judge current
   status — they intentionally cover a wider window and will keep showing the pre-remediation
   errors as residual history for several minutes after the fix. That is expected and does NOT
   mean the fault is still active.
5. If `{p_verify_recovery}` still shows active errors after ~2 minutes, treat the incident as
   still ongoing and continue investigating rather than assuming propagation delay.

## How to Escalate

Use `{p_escalation_action}` with:
- `action` — `"escalate"`, `"request_hold"`, `"resolve"`, or `"request_resume"`
- `channel` — the fault channel number
- `severity` — `"advisory"`, `"caution"`, `"warning"`, or `"critical"`
- `justification` — investigation summary

## HITL Channels (1–15)

Always pass `approval_mode: required`; never pass `approval_mode: skip` for channels 1–15.
The remediation workflow includes a human-in-the-loop step that requests the operator's
approval. Call the tool and continue until the workflow stops and requests approval natively
through its waitForInput step. Relay that approval request to the operator and end your turn.
The operator's reply to that request is the decision: resume the execution with exactly what
they answer. The workflow queues the remediation on approval and records the denial otherwise.

## Auto-Remediate Channels (16–20)

Pass `approval_mode: skip` only when the Significant Event Auto-Remediate workflow
message explicitly includes it. Always pass `dry_run: false`.
"""
        return {
            "id": f"{ns}-remediation-guide",
            "name": f"{self.scenario_name} Remediation Guide",
            "description": description,
            "content": content,
            "tool_ids": [
                p_remediation_action, p_escalation_action, p_verify_recovery,
                "platform.core.cases",
            ],
            "agent_prompt_hint": (
                f"Use **{ns}-remediation-guide** before executing remediation, "
                f"escalating an incident, or verifying that a fix resolved the fault."
            ),
        }
