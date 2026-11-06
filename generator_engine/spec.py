"""Generator specification dataclass.

One GeneratorSpec describes everything the framework needs to know about a
telemetry generator. The registry holds one spec per generator id.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class GeneratorSpec:
    """Describes one telemetry generator."""

    id: str
    """Short identifier, for example ``"postgresql"``.  Used in scenario YAML."""

    thread_name: str
    """Thread name, for example ``"gen-postgresql"``."""

    module: str
    """Dotted module path, for example ``"generators.postgresql_otel.generator"``.
    The registry imports this lazily via importlib."""

    client: str
    """Which transport client to pass to ``run()``.
    ``"otlp"`` passes :class:`app.telemetry.OTLPClient`.
    ``"es_bulk"`` passes :class:`app.telemetry.ESBulkClient`."""

    signals: tuple
    """Signals produced, for example ``("metrics", "logs")``."""

    datasets: tuple
    """OTel data-stream dataset names, for example ``("mysqlreceiver.otel",)``."""

    integrations: tuple
    """Elastic package names to install, for example ``("mysql_otel",)``."""

    always_install: bool
    """Install the packages even when the generator is disabled.
    Set True for ``kubernetes_otel`` so Kubernetes dashboards are always present."""

    data_views: tuple
    """Optional dedicated Kibana data views.
    Each entry is a dict with ``title``, ``name``, ``timeFieldName``, and
    ``allowNoIndex``.  Most generators leave this empty because the Elastic
    content package supplies the views."""

    wants_chaos: bool
    """True when the generator reads the chaos controller."""

    default_enabled: bool
    """True for the 9 original generators so they start without a
    ``generators:`` block in the scenario file."""

    core: bool
    """True when the demo depends on this generator.
    A scenario must not set ``enabled: false`` on a core generator."""

    config_fields: dict
    """Scenario-configurable fields.

    ``{name: {"type": str, "required": bool, "default": Any, "doc": str}}``

    ``required=True`` fields must appear in the scenario ``generators:`` block.
    ``default`` is the value used when the field is absent (matches what the
    module hard-coded before the registry existed)."""

    impact_keys: dict
    """Keys a channel can set in ``generator_impact:``.

    ``{key: {"type": str, "default": Any, "range": list|None, "doc": str}}``

    The verification script checks that every key used in a channel file is
    declared here.  The ``--describe`` CLI and the skill CONTRACT files all
    read this one structure."""

    chaos_mode: str = "any"
    """Default chaos mode.
    ``"any"`` reacts to every active channel (original behaviour).
    ``"explicit"`` reacts only when a channel names this generator in
    ``generator_impact:``."""

    family: str = ""
    """Logical family for credibility checks.

    Values: ``"core"``, ``"database"``, ``"cache"``, ``"queue"``,
    ``"proxy"``, ``"web"``, ``"platform"``, ``"cloud_compute"``,
    ``"cloud_audit"``, ``"ai"``.

    ``scripts/verify_yaml_scenarios.py`` warns when a scenario enables more
    than 2 generators from the same non-core family.  The ``add-scenario``
    skill groups the selection question by family."""

    schema_version: str = "1.0"
    """Generator plugin schema version (MAJOR.MINOR).

    Compared against ``generator_engine/schema.yaml`` ``current``.  Missing
    values are treated as ``default_unversioned``.  Major gaps block Launch
    for any scenario that enables this generator."""

    identity_fields: tuple = (
        "host.name",
        "service.name",
        "service.instance.id",
    )
    """Resource and datapoint fields a package dashboard uses as an instance picker.

    Values must come from :class:`generator_engine.identity.ScenarioIdentity`
    so two scenarios never share a filter entry. Override when the package
    control is a database name, URL, queue, or cloud account instead of the
    three defaults."""
