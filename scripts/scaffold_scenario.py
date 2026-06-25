#!/usr/bin/env python3
"""Generate a complete, structurally-consistent YAML scenario skeleton.

Reads a brief YAML file and emits the full ``scenarios/<id>/`` tree:
  - ``scenario.yaml``      — all top-level keys pre-wired and cross-refs in sync
  - ``services/<svc>.yaml`` ×9  — identity, topology stubs, steps skeleton
  - ``channels/NN-<slug>.yaml`` ×20 — error_type/affected/cascade filled, stubs for content

The generated skeleton passes all **structural** checks in ``scripts/verify_yaml_scenarios.py``
immediately (counts, sort_order, cross-ref keys, system_prompt error_types).  Only **domain
content** — realistic error messages, metric values, step telemetry logic — is left for an
LLM to fill in afterwards.

Usage::

    # Write a brief YAML file, then:
    python3 scripts/scaffold_scenario.py --brief my-brief.yaml

    # Dry-run (print to stdout instead of writing files):
    python3 scripts/scaffold_scenario.py --brief my-brief.yaml --dry-run

    # Overwrite an existing scenario (dangerous — be sure):
    python3 scripts/scaffold_scenario.py --brief my-brief.yaml --overwrite

Brief YAML format
-----------------

::

    # Required
    scenario_id: logistics               # short lowercase slug
    scenario_name: "GlobalShip Platform"
    scenario_icon: "🚢"
    namespace: gship                     # telemetry prefix
    sort_order: 11                       # display order in the selector UI
    nominal_label: SHIPPING              # status when nothing is broken

    services:
      - name: order-intake               # kebab-case; becomes filename (order-intake.yaml)
        cloud_provider: aws              # aws | gcp | azure
        cloud_region: us-east-1
        cloud_platform: aws_ec2          # aws_ec2 | gcp_compute_engine | azure_vm
        cloud_availability_zone: us-east-1a
        subsystem: order_management
        language: java                   # python | java | go | dotnet | rust | cpp
        entry_service: true             # exactly one: the sort_order:1 service
        kpi_emitter: false              # exactly one must be true
        generates_traces: true          # omit to default true; false for infra-only DBs
      # ... 8 more services (9 total, in desired sort_order) ...

    channels:
      - slug: order-routing-failure      # becomes: 01-order-routing-failure.yaml
        error_type: ORD-ROUTING-FAIL     # UPPER-KEBAB-CASE; unique per channel
        affected_services: [order-intake]
        cascade_services: [storefront-gateway]
      # ... 19 more channels (20 total) ...

    # Optional overrides
    apm_ml_bucket_span: 1m              # default: 1m
    executive_dashboard_intro: |        # default: auto-generated
      **Executive view** — ...
"""

from __future__ import annotations

import sys
import textwrap
from collections import defaultdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import yaml  # noqa: E402  (after sys.path modification)

# ---------------------------------------------------------------------------
# Host templates — one per cloud provider
# ---------------------------------------------------------------------------

_HOST_TEMPLATES: dict[str, dict[str, Any]] = {
    "aws": {
        "host.arch": "amd64",
        "host.type": "m5.2xlarge",
        "host.cpu.model.name": "Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz",
        "host.cpu.vendor.id": "GenuineIntel",
        "host.cpu.family": "6",
        "host.cpu.model.id": "85",
        "host.cpu.stepping": "7",
        "host.cpu.cache.l2.size": 1310720,
        "os.type": "linux",
        "os.description": "Amazon Linux 2023.6.20250115",
        "cpu_count": 8,
        "memory_total_bytes": 34359738368,
        "disk_total_bytes": 536870912000,
    },
    "gcp": {
        "host.arch": "amd64",
        "host.type": "n2-standard-8",
        "host.cpu.model.name": "Intel(R) Xeon(R) CPU @ 2.80GHz",
        "host.cpu.vendor.id": "GenuineIntel",
        "host.cpu.family": "6",
        "host.cpu.model.id": "85",
        "host.cpu.stepping": "7",
        "host.cpu.cache.l2.size": 1048576,
        "os.type": "linux",
        "os.description": "Debian GNU/Linux 12 (bookworm)",
        "cpu_count": 8,
        "memory_total_bytes": 34359738368,
        "disk_total_bytes": 274877906944,
    },
    "azure": {
        "host.arch": "amd64",
        "host.type": "Standard_D4s_v3",
        "host.cpu.model.name": "Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz",
        "host.cpu.vendor.id": "GenuineIntel",
        "host.cpu.family": "6",
        "host.cpu.model.id": "106",
        "host.cpu.stepping": "6",
        "host.cpu.cache.l2.size": 1310720,
        "os.type": "linux",
        "os.description": "Ubuntu 22.04.5 LTS",
        "cpu_count": 4,
        "memory_total_bytes": 17179869184,
        "disk_total_bytes": 137438953472,
    },
}

_CLOUD_PLATFORM_MAP = {
    "aws": "aws_ec2",
    "gcp": "gcp_compute_engine",
    "azure": "azure_vm",
}

# k8s cluster schema (matches the shape consumed by base.py and real scenario YAMLs)
_K8S_PLATFORM_MAP = {
    "aws": "aws_eks",
    "gcp": "gcp_gke",
    "azure": "azure_aks",
}
_K8S_OS_MAP = {
    "aws": "Amazon Linux 2",
    "gcp": "Container-Optimized OS",
    "azure": "Ubuntu 22.04",
}

# KPI emission value ranges keyed by the field-name suffix.
# Heuristic defaults so the executive dashboard shows plausible numbers out of the box.
# Authors should still adjust ranges to match their vertical in Phase 5.
_KPI_RANGE_BY_SUFFIX: dict[str, tuple[list[float], int]] = {
    "throughput_rpm":  ([1000.0, 50000.0], 0),
    "error_rate_pct":  ([0.0, 5.0],        2),
    "latency_p99_ms":  ([50.0, 500.0],     0),
    "availability_pct": ([99.0, 100.0],    2),
}
_KPI_RANGE_DEFAULT: tuple[list[float], int] = ([100.0, 1000.0], 1)


# ---------------------------------------------------------------------------
# YAML dump helpers — produce clean, readable output
# ---------------------------------------------------------------------------

class _IndentDumper(yaml.Dumper):
    """Dumper that indents lists by 2 spaces instead of 0."""

    def increase_indent(self, flow=False, indentless=False):  # type: ignore[override]
        return super().increase_indent(flow=flow, indentless=False)


def _dump(data: Any, *, width: int = 120) -> str:
    return yaml.dump(
        data,
        Dumper=_IndentDumper,
        default_flow_style=False,
        allow_unicode=True,
        width=width,
        sort_keys=False,
    )


def _dump_block(data: Any) -> str:
    return _dump(data).rstrip("\n")


# ---------------------------------------------------------------------------
# Host generation
# ---------------------------------------------------------------------------

def _build_hosts(services: list[dict], namespace: str) -> list[dict]:
    """Build 3 host dicts, one per cloud provider, based on the services list."""
    # Collect one region+az per provider (first occurrence wins)
    provider_info: dict[str, dict[str, str]] = {}
    for svc in services:
        p = svc["cloud_provider"]
        if p not in provider_info:
            provider_info[p] = {
                "region": svc["cloud_region"],
                "az": svc["cloud_availability_zone"],
                "platform": svc["cloud_platform"],
            }

    hosts = []
    idx = 1
    # Emit in a stable order (aws, gcp, azure) where present
    for provider in ("aws", "gcp", "azure"):
        if provider not in provider_info:
            continue
        info = provider_info[provider]
        region = info["region"]
        az = info["az"]
        tmpl = _HOST_TEMPLATES.get(provider, _HOST_TEMPLATES["aws"])

        if provider == "aws":
            host_id = f"i-0{'a1b2c3d4e5f6789' + str(idx)[:1]}"
            account_id = "123456789012"
            instance_id = host_id
            host_ip = ["10.0.1.10", "172.16.0.10"]
            host_mac = ["0a:1b:2c:3d:4e:5f", "0a:1b:2c:3d:4e:60"]
        elif provider == "gcp":
            host_id = str(1234567890123456789 + idx)
            account_id = "my-gcp-project"
            instance_id = host_id
            host_ip = ["10.128.0.10", "10.128.0.11"]
            host_mac = ["42:01:0a:80:00:0a", "42:01:0a:80:00:0b"]
        else:  # azure
            host_id = f"/subscriptions/sub-{idx:03d}/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm-01"
            account_id = f"sub-{idx:03d}"
            instance_id = f"vm-01-{idx:03d}"
            host_ip = ["10.0.2.10", "10.0.2.11"]
            host_mac = ["00:0d:3a:1b:2c:3d", "00:0d:3a:1b:2c:3e"]

        host: dict[str, Any] = {
            "host.name": f"{namespace}-{provider}-host-{idx:02d}",
            "host.id": host_id,
            **{k: v for k, v in tmpl.items() if k.startswith("host.")},
            "host.ip": host_ip,
            "host.mac": host_mac,
            **{k: v for k, v in tmpl.items() if k.startswith("os.")},
            "cloud.provider": provider,
            "cloud.platform": _CLOUD_PLATFORM_MAP.get(provider, provider),
            "cloud.region": region,
            "cloud.availability_zone": az,
            "cloud.account.id": account_id,
            "cloud.instance.id": instance_id,
            **{k: v for k, v in tmpl.items()
               if not k.startswith("host.") and not k.startswith("os.")},
        }
        hosts.append(host)
        idx += 1

    return hosts


# ---------------------------------------------------------------------------
# k8s cluster generation
# ---------------------------------------------------------------------------

def _build_k8s_clusters(
    services: list[dict], namespace: str
) -> list[dict]:
    """Build 3 k8s cluster dicts, one per cloud provider (k8s-eligible services only).

    Emits the schema expected by base.py and all existing scenarios:
    provider / platform / region / zones / os_description / services.
    """
    # Services with generates_traces enabled (i.e. not infra-only) go on k8s clusters
    provider_svcs: dict[str, list[str]] = defaultdict(list)
    provider_zones: dict[str, set[str]] = defaultdict(set)
    for svc in services:
        if not svc.get("generates_traces", True):
            continue
        p = svc["cloud_provider"]
        provider_svcs[p].append(svc["name"])
        if svc.get("cloud_availability_zone"):
            provider_zones[p].add(svc["cloud_availability_zone"])

    clusters = []
    for provider in ("aws", "gcp", "azure"):
        svc_list = provider_svcs.get(provider, [])
        region_info = next(
            (s for s in services if s["cloud_provider"] == provider), None
        )
        region = region_info["cloud_region"] if region_info else f"{provider}-region-1"
        zones = sorted(provider_zones.get(provider, set()))

        cluster: dict[str, Any] = {
            "name": f"{namespace}-{provider}-k8s",
            "provider": provider,
            "platform": _K8S_PLATFORM_MAP.get(provider, f"{provider}_k8s"),
            "region": region,
            "zones": zones,
            "os_description": _K8S_OS_MAP.get(provider, "Linux"),
            "services": svc_list,
        }
        clusters.append(cluster)

    return clusters


# ---------------------------------------------------------------------------
# service YAML generation
# ---------------------------------------------------------------------------

_STEPS_SKELETON_TEMPLATE = """\
# TODO: Replace these stub steps with domain-specific telemetry.
#
# DSL verbs: sample, metric, log, counter, every, for_each, incr_key
# Value specs: {{randint: [lo, hi]}}  {{uniform: [lo, hi], round: N}}  {{choice: LIST}}
#              {{if_active: <spec>, else: <spec>}}  {{var: name}}  {{expr: "..."}}
- sample:
    latency_ms:
      if_active: {{uniform: [500.0, 3000.0], round: 1}}
      else: {{uniform: [20.0, 150.0], round: 1}}
- metric:
    name: {svc_key}.latency_ms
    value: {{var: latency_ms}}
    unit: ms
- log:
    level: INFO
    message: '[{svc_key}] health_check latency={{latency_ms}}ms status=OK'
    attrs:
      operation: health_check
      svc.latency_ms: {{var: latency_ms}}
"""


def _build_service_yaml(svc: dict, sort_order: int) -> str:
    """Return the YAML string for a single service file."""
    lines = []

    lines.append(f"service: {svc['name']}")
    lines.append(f"sort_order: {sort_order}")
    lines.append(f"cloud_provider: {svc['cloud_provider']}")
    lines.append(f"cloud_region: {svc['cloud_region']}")
    lines.append(f"cloud_platform: {svc['cloud_platform']}")
    lines.append(f"cloud_availability_zone: {svc['cloud_availability_zone']}")
    lines.append(f"subsystem: {svc['subsystem']}")
    lines.append(f"language: {svc['language']}")

    generates_traces = svc.get("generates_traces", True)
    if not generates_traces:
        lines.append("generates_traces: false")

    lines.append("")
    lines.append("# Topology: [downstream-service, endpoint, METHOD]")
    lines.append("topology: []")
    lines.append("")
    if svc.get("entry_service"):
        lines.append("# Required: entry_endpoints for the sort_order:1 service")
        lines.append("# Format: [[/path, METHOD], ...]")
        lines.append("entry_endpoints:")
        lines.append(f"  - [/api/v1/{svc['name']}/health, GET]")
        lines.append(f"  - [/api/v1/{svc['name']}/process, POST]")
        lines.append("")

    lines.append("db_operations: []")
    lines.append("")
    lines.append("emit_fault_logs: true")
    kpi_emitter = svc.get("kpi_emitter", False)
    lines.append(f"kpi_emitter: {'true' if kpi_emitter else 'false'}")
    lines.append("")
    lines.append("constants: {}")
    lines.append("")
    svc_key_norm = svc["name"].replace("-", "_")
    steps_body = _STEPS_SKELETON_TEMPLATE.format(svc_key=svc_key_norm)
    lines.append("steps:")
    for step_line in steps_body.rstrip("\n").split("\n"):
        lines.append(f"  {step_line}" if step_line else "")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# channel YAML generation
# ---------------------------------------------------------------------------

def _build_channel_yaml(ch_num: int, ch: dict) -> str:
    """Return the YAML string for a single channel file."""
    slug = ch["slug"]
    error_type = ch["error_type"]
    affected = ch.get("affected_services", [])
    cascade = ch.get("cascade_services", [])

    # Build placeholder-safe fault_params stubs (one key per placeholder referenced)
    # We pre-define placeholder names that match the stub error_message below.
    placeholder_keys = ["affected_service", "error_detail"]

    lines = [
        f"name: {_slug_to_title(slug)}",
        f"subsystem: {_infer_subsystem(slug)}",
        f"vehicle_section: {_infer_subsystem(slug)}_pipeline",
        f"error_type: {error_type}",
        f"sensor_type: {_infer_sensor_type(slug)}",
    ]

    affected_yaml = yaml.dump(affected, default_flow_style=True).strip()
    cascade_yaml = yaml.dump(cascade, default_flow_style=True).strip()
    lines.append(f"affected_services: {affected_yaml}")
    lines.append(f"cascade_services: {cascade_yaml}")

    lines.extend([
        "",
        "description: >",
        f"  TODO: describe the observable failure for {error_type}.",
        "",
        "investigation_notes: |",
        f"  1. Search for body.text containing *{error_type}*",
        "  2. Check service metrics for the affected services listed above",
        "  3. TODO: add investigation steps",
        "",
        "remediation_action: restart_service",
        "",
        f"error_message: |",
        f"  {error_type}: service={{affected_service}} detail={{error_detail}}",
        "",
        "stack_trace: |",
        f"  Error: {error_type}",
        f"    at ServiceHandler.process()",
        f"    detail: {{error_detail}}",
        "",
        "# fault_params: one key per {placeholder} in error_message + stack_trace above",
        "fault_params:",
        "  affected_service: {choice: [" + ", ".join(f"'{s}'" for s in (affected or ["unknown"])) + "]}",
        "  error_detail: {choice: [timeout, connection_refused, internal_error]}",
        "",
        "rca_clues: {}",
    ])

    return "\n".join(lines) + "\n"


def _slug_to_title(slug: str) -> str:
    return " ".join(w.capitalize() for w in slug.replace("-", " ").split())


def _infer_subsystem(slug: str) -> str:
    """Infer a plausible subsystem from a channel slug."""
    mapping = {
        "auth": "authentication", "login": "authentication", "token": "authentication",
        "payment": "payments", "billing": "billing", "invoice": "billing",
        "db": "database", "database": "database", "sql": "database",
        "network": "network", "dns": "network", "cert": "security",
        "api": "api_gateway", "gateway": "api_gateway",
        "queue": "messaging", "kafka": "messaging", "mq": "messaging",
        "cache": "caching", "redis": "caching",
        "ml": "ml_inference", "model": "ml_inference",
        "storage": "storage", "s3": "storage", "gcs": "storage",
        "deploy": "deployment", "k8s": "deployment", "pod": "deployment",
    }
    for kw, subsystem in mapping.items():
        if kw in slug:
            return subsystem
    return "platform"


def _infer_sensor_type(slug: str) -> str:
    for kw, sensor in [
        ("latency", "response_latency"), ("timeout", "response_latency"),
        ("error", "error_rate"), ("fail", "error_rate"),
        ("memory", "memory_usage"), ("cpu", "cpu_usage"),
        ("queue", "queue_depth"), ("backlog", "queue_depth"),
        ("pool", "connection_pool"), ("connection", "connection_pool"),
        ("cert", "certificate_expiry"), ("tls", "certificate_expiry"),
    ]:
        if kw in slug:
            return sensor
    return "health_check"


# ---------------------------------------------------------------------------
# scenario.yaml generation
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_TEMPLATE = """\
You are an expert SRE assistant for the {scenario_name}.

When an incident is reported, your job is to:
1. Diagnose the root cause from the error_type, stack traces, and RCA clues
2. Recommend a specific remediation action
3. Explain business impact in terms the executive team understands

Known fault channels and their error types:
{error_type_list}

For each fault, examine the affected_services first, then look for cascade_services that may be experiencing downstream effects.
"""


def _build_scenario_yaml(brief: dict, services: list[dict]) -> str:
    """Return the YAML string for scenario.yaml."""
    namespace = brief["namespace"]
    scenario_name = brief["scenario_name"]
    channels = brief["channels"]
    kpi_emitter_name = next(
        (s["name"] for s in services if s.get("kpi_emitter")), services[0]["name"]
    )

    error_type_list = "\n".join(
        f"  - {ch['error_type']}: {_slug_to_title(ch['slug'])}"
        for ch in channels
    )
    system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(
        scenario_name=scenario_name,
        error_type_list=error_type_list,
    ).strip()

    hosts = _build_hosts(services, namespace)
    k8s_clusters = _build_k8s_clusters(services, namespace)

    # Scaffold KPI sections/emissions/charts in sync
    kpi_section_field_pairs = [
        ("Primary KPI (unit)", f"metrics.business.primary_kpi"),
        ("Secondary KPI (unit)", f"metrics.business.secondary_kpi"),
        ("Throughput (req/min)", f"metrics.business.throughput_rpm"),
        ("Error Rate (%)", f"metrics.business.error_rate_pct"),
        ("P99 Latency (ms)", f"metrics.business.latency_p99_ms"),
        ("Availability (%)", f"metrics.business.availability_pct"),
    ]
    executive_kpi_sections = [
        {
            "header": "**Performance** — TODO: rename to match your vertical",
            "specs": [[title, field] for title, field in kpi_section_field_pairs],
        }
    ]
    executive_kpi_emissions = []
    for _, field in kpi_section_field_pairs:
        suffix = field.split(".")[-1]
        kpi_range, kpi_round = _KPI_RANGE_BY_SUFFIX.get(suffix, _KPI_RANGE_DEFAULT)
        executive_kpi_emissions.append({
            "name": field.replace("metrics.", ""),
            "value": {"uniform": kpi_range, "round": kpi_round},
            "unit": "TODO",
        })
    executive_trend_charts = [
        {"title": "Primary KPI trend", "field": "metrics.business.primary_kpi", "y_label": "TODO"},
        {"title": "Throughput trend", "field": "metrics.business.throughput_rpm", "y_label": "req/min"},
        {"title": "Error Rate trend", "field": "metrics.business.error_rate_pct", "y_label": "%"},
        {"title": "Latency P99 trend", "field": "metrics.business.latency_p99_ms", "y_label": "ms"},
        {"title": "Availability trend", "field": "metrics.business.availability_pct", "y_label": "%"},
    ]

    # trace_attributes.services — keyed by every service name
    trace_services: dict[str, dict] = {}
    for svc in services:
        trace_services[svc["name"]] = {
            "svc.version": {"choice": ["1.0.0", "1.0.1", "1.1.0"]},
            "svc.environment": {"choice": ["prod", "prod", "staging"]},
        }

    data: dict[str, Any] = {
        "scenario_id": brief["scenario_id"],
        "scenario_name": scenario_name,
        "scenario_description": f"TODO: describe the {scenario_name} scenario.",
        "namespace": namespace,
        "scenario_icon": brief.get("scenario_icon", "🔧"),
        "sort_order": brief["sort_order"],
        "nominal_label": brief.get("nominal_label", "NORMAL"),
        "apm_ml_bucket_span": brief.get("apm_ml_bucket_span", "1m"),
        "hosts": hosts,
        "k8s_clusters": k8s_clusters,
        "agent_config": {
            "id": f"{namespace}-analyst",
            "name": f"{scenario_name} Operations Analyst",
            "assessment_tool_name": f"{namespace}_readiness_assessment",
            "system_prompt": system_prompt,
        },
        "assessment_tool_config": {
            "id": f"{namespace}_readiness_assessment",
            "description": f"TODO: describe the overall {scenario_name} operational assessment — what it evaluates, key health dimensions, and how it surfaces risk.",
        },
        "executive_kpi_emitter_service_name": kpi_emitter_name,
        "executive_dashboard_intro": (
            brief.get("executive_dashboard_intro")
            or f"**Executive view** — {scenario_name} business metrics. "
               f"TODO: describe KPIs and thresholds."
        ),
        "executive_kpi_sections": executive_kpi_sections,
        "executive_trend_charts": executive_trend_charts,
        "executive_kpi_emissions": executive_kpi_emissions,
        "raw_log_profile": {
            "service_name": f"{namespace}-{services[0]['name']}",
            "user_id_prefix": "u",
            "tier_field": "user_tier",
            "tier_values": [["standard", 80], ["premium", 20]],
            "country_weights": {"US": 35, "GB": 15, "DE": 15, "FR": 10, "JP": 10, "IN": 15},
            "methods": ["GET", "POST", "PUT", "DELETE"],
            "paths": ["/api/v1/TODO", "/api/v1/health"],
            "change_point_path": "/api/v1/TODO",
        },
        "trace_attributes": {
            "base": {
                "platform.region": {"choice": [s["cloud_region"] for s in services[:3]]},
                "platform.traffic_tier": {"choice": ["normal", "normal", "elevated", "peak"]},
            },
            "services": trace_services,
        },
    }

    return _dump(data)


# ---------------------------------------------------------------------------
# Main scaffold entry point
# ---------------------------------------------------------------------------

def scaffold(brief_path: Path, *, dry_run: bool = False, overwrite: bool = False) -> None:
    with brief_path.open(encoding="utf-8") as fh:
        brief = yaml.safe_load(fh)

    _validate_brief(brief)

    scenario_id: str = brief["scenario_id"]
    services_raw: list[dict] = brief["services"]
    channels_raw: list[dict] = brief["channels"]

    # Assign sort_order by brief list position (1-based)
    services = [dict(svc, sort_order=i + 1) for i, svc in enumerate(services_raw)]

    out_dir = REPO_ROOT / "scenarios" / scenario_id
    if out_dir.exists() and not overwrite:
        print(
            f"Error: scenarios/{scenario_id}/ already exists. "
            f"Use --overwrite to force.",
            file=sys.stderr,
        )
        sys.exit(1)

    files: dict[Path, str] = {}

    # scenario.yaml
    files[out_dir / "scenario.yaml"] = _build_scenario_yaml(brief, services)

    # services/*.yaml
    for svc in services:
        svc_path = out_dir / "services" / f"{svc['name']}.yaml"
        files[svc_path] = _build_service_yaml(svc, svc["sort_order"])

    # channels/NN-<slug>.yaml
    for i, ch in enumerate(channels_raw):
        num = i + 1
        ch_path = out_dir / "channels" / f"{num:02d}-{ch['slug']}.yaml"
        files[ch_path] = _build_channel_yaml(num, ch)

    if dry_run:
        print(f"[dry-run] Would write {len(files)} files to scenarios/{scenario_id}/\n")
        for p, content in sorted(files.items()):
            rel = p.relative_to(REPO_ROOT)
            print(f"=== {rel} ({'%d lines' % content.count(chr(10))}) ===")
        return

    for file_path, content in files.items():
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content, encoding="utf-8")
        print(f"  wrote {file_path.relative_to(REPO_ROOT)}")

    print(f"\n✓ Scaffolded {len(files)} files into scenarios/{scenario_id}/")
    print(f"\nNext steps:")
    print(f"  1. Fill in domain content (error messages, metric ranges, step telemetry)")
    print(f"  2. python3 scripts/verify_yaml_scenarios.py {scenario_id}")
    print(f"  3. ACTIVE_SCENARIO={scenario_id} ./start.sh")


def _validate_brief(brief: dict) -> None:
    """Raise ValueError with a clear message if the brief is missing required keys."""
    required_top = ["scenario_id", "scenario_name", "scenario_icon", "namespace",
                    "sort_order", "nominal_label", "services", "channels"]
    for key in required_top:
        if key not in brief:
            raise ValueError(f"Brief is missing required key: '{key}'")

    services = brief["services"]
    if len(services) != 9:
        raise ValueError(f"Brief must list exactly 9 services (got {len(services)})")

    channels = brief["channels"]
    if len(channels) != 20:
        raise ValueError(f"Brief must list exactly 20 channels (got {len(channels)})")

    required_svc_keys = [
        "name", "cloud_provider", "cloud_region", "cloud_platform",
        "cloud_availability_zone", "subsystem", "language",
    ]
    for i, svc in enumerate(services):
        for key in required_svc_keys:
            if key not in svc:
                raise ValueError(
                    f"Service #{i + 1} ('{svc.get('name', '?')}') is missing key: '{key}'"
                )

    entry_count = sum(1 for s in services if s.get("entry_service"))
    if entry_count != 1:
        raise ValueError(
            f"Brief must mark exactly one service as 'entry_service: true' (found {entry_count})"
        )

    kpi_count = sum(1 for s in services if s.get("kpi_emitter"))
    if kpi_count != 1:
        raise ValueError(
            f"Brief must mark exactly one service as 'kpi_emitter: true' (found {kpi_count})"
        )

    required_ch_keys = ["slug", "error_type", "affected_services", "cascade_services"]
    for i, ch in enumerate(channels):
        for key in required_ch_keys:
            if key not in ch:
                raise ValueError(
                    f"Channel #{i + 1} ('{ch.get('slug', '?')}') is missing key: '{key}'"
                )

    error_types = [ch["error_type"] for ch in channels]
    dupes = [et for et in error_types if error_types.count(et) > 1]
    if dupes:
        raise ValueError(f"Duplicate error_types in channels: {sorted(set(dupes))}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Scaffold a new scenario skeleton from a brief YAML file."
    )
    parser.add_argument(
        "--brief",
        type=Path,
        required=True,
        help="Path to the brief YAML file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written without creating files.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite an existing scenario directory.",
    )
    args = parser.parse_args()

    if not args.brief.exists():
        print(f"Error: brief file not found: {args.brief}", file=sys.stderr)
        sys.exit(1)

    try:
        scaffold(args.brief, dry_run=args.dry_run, overwrite=args.overwrite)
    except (ValueError, KeyError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
