"""YAML-backed BaseScenario implementation.

Each scenario directory is pure YAML — no Python files:

    <id>/
        scenario.yaml      # identity, infra, agent config, KPIs, trace attrs
        channels/
            01-<slug>.yaml  (×20, one per fault channel)
        services/
            <svc>.yaml      # per-service: identity, topology, telemetry DSL (×9)

The registry (``scenario_engine/__init__.py``) discovers scenarios by globbing
``scenarios/*/scenario.yaml`` and calling :func:`load_yaml_scenario` directly —
no ``scenario.py`` shim is needed.

All per-channel data (error_message, stack_trace, fault_params, rca_clues) lives
inside that channel's YAML file so parity is locally visible — no more chasing
``{placeholder}`` strings 1000 lines away from the matching ``fault_params`` key.

``emit_executive_business_metrics_if_eligible`` is exposed as a module-level
function and called directly by the telemetry DSL executor:

    from scenario_engine.yaml_scenario import emit_executive_business_metrics_if_eligible
"""

from __future__ import annotations

import random
from pathlib import Path
from typing import Any

import yaml

from scenario_engine.base import BaseScenario
from scenario_engine.fault_spec import resolve, resolve_dict


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_yaml(path: Path) -> dict:
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _channel_num_from_path(p: Path) -> int:
    """Extract the leading integer from a channel filename like '01-some-slug.yaml'."""
    return int(p.stem.split("-", 1)[0])


# ---------------------------------------------------------------------------
# YamlScenario
# ---------------------------------------------------------------------------

class YamlScenario(BaseScenario):
    """A :class:`~scenarios.base.BaseScenario` whose data lives in YAML files.

    Instantiate via :func:`load_yaml_scenario` rather than directly.
    """

    def __init__(
        self,
        data: dict,
        channels: dict[int, dict],
        scenario_dir: "Path | None" = None,
    ) -> None:
        self._data = data
        self._channels = channels  # {1: {...}, 2: {...}, ..., 20: {...}}
        self._scenario_dir = scenario_dir  # None only in legacy/test usage

        # Pre-build the channel_registry property value once (it's static data).
        self._channel_registry: dict[int, dict[str, Any]] = {}
        for ch_num, ch in self._channels.items():
            entry = {
                k: ch[k]
                for k in (
                    "name", "subsystem", "vehicle_section", "error_type",
                    "sensor_type", "affected_services", "cascade_services",
                    "description", "investigation_notes", "remediation_action",
                    "error_message", "stack_trace", "infrastructure_events",
                )
                if k in ch
            }
            self._channel_registry[ch_num] = entry

        # KPI emissions list (used by emit_executive_business_metrics_if_eligible).
        self._kpi_emissions: list[dict] = self._data.get("executive_kpi_emissions", [])

        # Load per-service specs from services/*.yaml, ordered by sort_order.
        # Each file is the single source of truth for a service's identity,
        # topology, and telemetry DSL.  Falls back to the legacy data dict for
        # tests that construct YamlScenario directly without a scenario_dir.
        self._service_specs: dict[str, dict] = self._load_service_specs()

    # ── Identity ─────────────────────────────────────────────────────────

    @property
    def scenario_id(self) -> str:
        return self._data["scenario_id"]

    @property
    def scenario_name(self) -> str:
        return self._data["scenario_name"]

    @property
    def scenario_description(self) -> str:
        return self._data["scenario_description"]

    @property
    def namespace(self) -> str:
        return self._data["namespace"]

    @property
    def scenario_icon(self) -> str:
        return self._data.get("scenario_icon", "🔧")

    @property
    def sort_order(self) -> int:
        return self._data.get("sort_order", 999)

    @property
    def nominal_label(self) -> str:
        return self._data.get("nominal_label", "NORMAL")

    @property
    def apm_ml_bucket_span(self) -> str:
        return self._data.get("apm_ml_bucket_span", "15m")

    @property
    def schema_version(self) -> "str | None":
        """Raw schema_version from scenario.yaml, or None if absent (treated as 1.0)."""
        return self._data.get("schema_version")

    # ── Executive KPIs ───────────────────────────────────────────────────

    @property
    def executive_kpi_emitter_service_name(self) -> str | None:
        return self._data.get("executive_kpi_emitter_service_name")

    @property
    def executive_dashboard_intro(self) -> str:
        return self._data.get(
            "executive_dashboard_intro",
            super().executive_dashboard_intro,
        )

    @property
    def executive_kpi_sections(self) -> list[dict]:
        raw = self._data.get("executive_kpi_sections", [])
        # Convert inner specs lists from [title, field] to (title, field) tuples.
        result = []
        for section in raw:
            result.append({
                "header": section["header"],
                "specs": [tuple(s) for s in section.get("specs", [])],
            })
        return result

    @property
    def executive_trend_charts(self) -> list[dict]:
        return self._data.get("executive_trend_charts", [])

    # ── Service spec loading ─────────────────────────────────────────────

    _IDENTITY_KEYS = (
        "cloud_provider", "cloud_region", "cloud_platform",
        "cloud_availability_zone", "subsystem", "language",
        "generates_traces",  # optional bool; False skips trace generation for infra-only services
    )

    def _load_service_specs(self) -> "dict[str, dict]":
        """Load per-service YAML files, ordered by sort_order.

        Returns an ordered dict mapping service-key → full spec dict.
        Falls back gracefully to an empty dict if no services/ directory exists
        (e.g. legacy unit-test construction without a real scenario_dir).
        """
        if not self._scenario_dir:
            return {}
        services_dir = self._scenario_dir / "services"
        if not services_dir.is_dir():
            return {}
        specs: list[tuple[int, str, dict]] = []  # (sort_order, name, spec)
        for p in services_dir.glob("*.yaml"):
            spec = _load_yaml(p)
            svc_name = spec.get("service", p.stem)
            order = spec.get("sort_order", 999)
            specs.append((order, svc_name, spec))
        specs.sort(key=lambda t: (t[0], t[1]))
        return {name: spec for _, name, spec in specs}

    # ── Services & Topology ──────────────────────────────────────────────

    @property
    def services(self) -> dict[str, dict[str, Any]]:
        """Identity/resource metadata for each service (cloud, region, language, subsystem).

        Derived from per-service YAML files in sort_order; falls back to the
        legacy ``services:`` block in scenario.yaml when no services/ dir exists.
        """
        if self._service_specs:
            return {
                svc: {k: spec[k] for k in self._IDENTITY_KEYS if k in spec}
                for svc, spec in self._service_specs.items()
            }
        # Legacy / test fallback: services: block in scenario.yaml
        result: dict[str, dict[str, Any]] = {}
        for svc, cfg in self._data.get("services", {}).items():
            result[svc] = {k: v for k, v in cfg.items()
                           if k in self._IDENTITY_KEYS}
        return result

    @property
    def channel_registry(self) -> dict[int, dict[str, Any]]:
        return self._channel_registry

    @property
    def service_topology(self) -> dict[str, list[tuple[str, str, str]]]:
        if self._service_specs:
            return {
                svc: [tuple(e) for e in spec.get("topology", [])]
                for svc, spec in self._service_specs.items()
                if spec.get("topology")
            }
        raw = self._data.get("service_topology", {})
        return {svc: [tuple(e) for e in edges] for svc, edges in raw.items()}

    @property
    def entry_endpoints(self) -> dict[str, list[tuple[str, str]]]:
        if self._service_specs:
            return {
                svc: [tuple(e) for e in spec.get("entry_endpoints", [])]
                for svc, spec in self._service_specs.items()
                if spec.get("entry_endpoints")
            }
        raw = self._data.get("entry_endpoints", {})
        return {svc: [tuple(e) for e in eps] for svc, eps in raw.items()}

    @property
    def db_operations(self) -> dict[str, list[tuple[str, str, str]]]:
        if self._service_specs:
            return {
                svc: [tuple(op) for op in spec.get("db_operations", [])]
                for svc, spec in self._service_specs.items()
                if spec.get("db_operations")
            }
        raw = self._data.get("db_operations", {})
        return {svc: [tuple(op) for op in ops] for svc, ops in raw.items()}

    # ── Infrastructure ───────────────────────────────────────────────────

    @property
    def hosts(self) -> list[dict[str, Any]]:
        return self._data.get("hosts", [])

    @property
    def k8s_clusters(self) -> list[dict[str, Any]]:
        return self._data.get("k8s_clusters", [])

    # ── Agent & Elastic Config ───────────────────────────────────────────

    @property
    def agent_config(self) -> dict[str, Any]:
        return self._data.get("agent_config", {})

    @property
    def assessment_tool_config(self) -> dict[str, Any]:
        return self._data.get("assessment_tool_config", {})

    @property
    def knowledge_base_docs(self) -> list[dict[str, Any]]:
        return []  # Populated at deploy time from channel_registry

    # ── Raw Log Profile ──────────────────────────────────────────────────

    @property
    def raw_log_profile(self) -> dict[str, Any]:
        raw = self._data.get("raw_log_profile")
        if raw is None:
            return super().raw_log_profile
        # tier_values stored as [[tier, weight], ...] in YAML; convert to tuples.
        if "tier_values" in raw:
            raw = dict(raw)
            raw["tier_values"] = [tuple(t) for t in raw["tier_values"]]
        return raw

    # ── Service Classes ──────────────────────────────────────────────────

    def get_service_classes(self) -> list[type]:
        """Return one :class:`~app.services.telemetry_dsl.YamlService` subclass per service.

        Services are returned in ``sort_order`` (the order written to ``services/*.yaml``).
        The spec is already loaded into ``_service_specs`` at construction time, so
        no additional file I/O is required here.
        """
        from app.services.telemetry_dsl import YamlService  # lazy to avoid circular import

        classes: list[type] = []
        for svc_name, spec in self._service_specs.items():
            cls_name = (
                "".join(w.capitalize() for w in svc_name.replace("-", "_").split("_"))
                + "Service"
            )
            cls = type(cls_name, (YamlService,), {
                "SERVICE_NAME": svc_name,
                "_telemetry_spec": spec,
            })
            classes.append(cls)

        return classes

    # ── Fault Parameters ─────────────────────────────────────────────────

    def get_fault_params(self, channel: int) -> dict[str, Any]:
        ch = self._channels.get(channel, {})
        raw = ch.get("fault_params", {})
        return resolve_dict(raw)  # unseeded rng for true randomness

    # ── Trace Attributes & RCA ───────────────────────────────────────────

    def get_trace_attributes(self, service_name: str, rng) -> dict:
        trace_cfg = self._data.get("trace_attributes", {})
        base = resolve_dict(trace_cfg.get("base", {}), rng)
        svc = resolve_dict(
            trace_cfg.get("services", {}).get(service_name, {}), rng
        )
        base.update(svc)
        return base

    def get_rca_clues(self, channel: int, service_name: str, rng) -> dict:
        ch = self._channels.get(channel, {})
        svc_clues = ch.get("rca_clues", {}).get(service_name, {})
        return resolve_dict(svc_clues, rng)

    def get_correlation_attribute(self, channel: int, is_error: bool, rng) -> dict:
        ch = self._channels.get(channel, {})
        ca = ch.get("correlation_attr")
        if not ca:
            return {}
        threshold = 0.90 if is_error else 0.05
        if rng.random() < threshold:
            return {ca["key"]: ca["value"]}
        return {}


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def load_yaml_scenario(scenario_dir: Path) -> YamlScenario:
    """Load a :class:`YamlScenario` from *scenario_dir*.

    Expects:
    - ``<scenario_dir>/scenario.yaml``
    - ``<scenario_dir>/channels/NN-<slug>.yaml`` (×20)
    """
    data = _load_yaml(scenario_dir / "scenario.yaml")

    channels_dir = scenario_dir / "channels"
    channels: dict[int, dict] = {}
    if channels_dir.is_dir():
        for ch_path in sorted(channels_dir.glob("*.yaml")):
            ch_num = _channel_num_from_path(ch_path)
            channels[ch_num] = _load_yaml(ch_path)

    return YamlScenario(data, channels, scenario_dir=scenario_dir)


# ---------------------------------------------------------------------------
# Executive KPI emitter (called directly by the telemetry DSL executor)
# ---------------------------------------------------------------------------

def emit_executive_business_metrics_if_eligible(service: Any) -> None:
    """Emit executive ``business.*`` gauges for YAML scenarios.

    Called once per telemetry cycle from the designated emitter service
    via ``app/services/telemetry_dsl.py``. The service's scenario must be
    a :class:`YamlScenario`; all other scenario types are silently skipped.
    """
    ctx = getattr(service, "_ctx", None)
    if not ctx:
        return
    scenario = getattr(ctx, "scenario", None)
    # Duck-type instead of isinstance: the YamlScenario class identity changes
    # when scenarios.yaml_scenario is evicted and reimported after a zip import
    # or /api/scenarios/reload, so isinstance silently returns False.
    if not (hasattr(scenario, "_kpi_emissions") and hasattr(scenario, "executive_kpi_emitter_service_name")):
        return
    want = scenario.executive_kpi_emitter_service_name
    if not want or want != service.SERVICE_NAME:
        return

    # Build all KPI gauges in one pass then send in a single OTLP POST.
    # Previously each emit_metric() call issued its own POST, serialized behind
    # _send_lock, causing tiles to trickle onto the dashboard one at a time.
    rng = random.Random()
    metrics = [
        service.otlp.build_gauge(
            kpi["name"], float(resolve(kpi["value"], rng)), kpi.get("unit", "")
        )
        for kpi in scenario._kpi_emissions
    ]
    if metrics:
        service.otlp.send_metrics(service.resource, metrics)
