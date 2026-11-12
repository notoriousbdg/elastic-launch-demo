#!/usr/bin/env python3
"""Verify that YAML scenarios pass all integrity checks.

Checks performed for each scenario:
  1.  scenario.yaml loads without errors
  1b. schema_version: if present, must parse as MAJOR.MINOR; malformed → error.
      Missing → warning only (treated as '1.0', compatible with current engine).
      If older MAJOR than engine → error (upgrade_required).
      If same MAJOR, older MINOR → warning (outdated_compatible).
  2.  channel count == 20; numbers are contiguous 01–20. Counted on the channel
      *files* as well as the loaded registry: the loader keeps one file per
      number, so a duplicate number would otherwise hide a dead channel.
  3.  each channel has required fields (name, error_type, affected_services,
      error_message, stack_trace, fault_params)
  4.  service count == 9
  5.  sort_order values are exactly {1..9} — unique, no gaps
  6.  each service has all identity keys (cloud_provider, cloud_region,
      cloud_platform, cloud_availability_zone, subsystem, language)
  7.  each service's language is in the allowed set
  8.  hosts count == 3; k8s_clusters count == 3
  9.  every {placeholder} in error_message + stack_trace has a key in
      get_fault_params(ch) — and every fault_params key is referenced
      by at least one placeholder (orphan check)
  10. every affected_services / cascade_services value is a key in services
  11. every k8s_clusters[].services value is a key in services
  12. agent_config.system_prompt references all 20 channel error_types
      (channels -> prompt only; stale error_types left in the prompt are not detected)
  13. trace_attributes.services keys are all real service names
  14. executive_kpi_emissions is a list; executive_kpi_emitter_service_name
      names a real service; KPI section field names match emissions names
  15. the sort_order:1 service defines entry_endpoints
  16. get_service_classes() instantiates without errors and returns 9 classes;
      each class that doesn't disable telemetry (generates_traces/emit_fault_logs)
      has a non-empty steps: body
  17. get_rca_clues() returns a dict (spot-check)
  18. get_trace_attributes() returns a non-empty dict when trace_attributes defined
  19. (warning only) cloud_provider distribution is balanced
  20. k8s_clusters entries have provider/region/platform (not cloud_-prefixed keys)
  21. (warning only) host.name values don't use the generic 'scaffold-' prefix
  22. (warning only) at least one channel has non-empty rca_clues
  23. (warning only) at least one service has non-empty topology
  24. (warning only) entry_endpoints for the sort_order:1 service aren't generic stubs
  25. (warning only) trace_attributes richness (>=3 attrs) per service
  26. generator wiring: generator_impact only references real generator ids and
      impact_keys (error); every generator enabled via scenario.yaml's
      generators: block has >=1 channel wiring it via generator_impact
      (warning — see Check 26 comments for why this isn't a hard error); and
      lists each enabled generator's alert knobs that no channel sets (warning)
  27. generator enablement (error): a channel's generator_impact may only name a
      generator that is enabled for the scenario; every id in the generators:
      block must be a real generator; every enabled generator has its required
      config_fields set
  28. checked-in host account and instance ids match ScenarioIdentity, and no
      account or instance id is shared by two namespaces (scenarios/ and
      scenarios_testing/)
  29. generators/**/generator.py does not format f"{ns}- / f"{db_prefix}_ identity
      or embed the shared AWS account 123456789012
  30. every channel carries dict-form business_impact entries ({kpi, factor|value})
      whose kpi names exist in executive_kpi_emissions, so faults move the
      executive dashboard (error); factor ranges straddling 1.0 (warning)
  31. every channel lists at least one *trace entry* service in affected_services
      (sort_order-first service or one with entry_endpoints, excluding database /
      generates_traces:false services) — the trace generator only raises the
      error rate for entry services, so otherwise the ops dashboard stays green
  32. business_impact is scope-proportional: classified with the real executive
      palette bands, each channel needs enough red/yellow KPI tiles for its scope
      tier (see SCOPE_TIERS) — otherwise the executive tiles stay green
  34. every topology callee is a defined service (an edge to an undefined service
      used to crash the trace generator, leaving every ops tile at N/A)
  33. incr_key state keys are initialised as a dict ({init: {}} / init_per_key),
      not a scalar, or the service telemetry loop crashes every tick
  35. every {placeholder} in investigation_notes is a fault_params key — the
      KB docs, Wiki runbooks and channel runbook skill fill them with the
      parameter's range, and an unknown one is shown as a literal {name}.
      Shell syntax such as curl's %{time_total} or ${VAR} is not a placeholder.
  36. (warning only) evidence quality: a choice rca_clue that can emit a
      healthy-looking value (OFF, INTACT, false...) does not always indicate the
      fault, which leaves investigations unable to confirm the cause; and a choice
      fault_param that can pick a unit the stack_trace lists as healthy

Usage::

    cd /path/to/elastic-launch-demo

    # Validate all scenarios (exit non-zero on any failure):
    python3 scripts/verify_yaml_scenarios.py

    # Validate specific scenarios:
    python3 scripts/verify_yaml_scenarios.py banking telecom

    # Report failures but exit 0 (e.g. in non-blocking CI hooks):
    python3 scripts/verify_yaml_scenarios.py --warn-only
"""

from __future__ import annotations

import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

SCENARIOS_DIR = REPO_ROOT / "scenarios"

_IDENTITY_KEYS = (
    "cloud_provider",
    "cloud_region",
    "cloud_platform",
    "cloud_availability_zone",
    "subsystem",
    "language",
)

_ALLOWED_LANGUAGES = frozenset(
    {"python", "java", "go", "dotnet", "rust", "cpp"}
)

_CHANNEL_REQUIRED_FIELDS = (
    "name",
    "error_type",
    "affected_services",
    "error_message",
    "stack_trace",
    "fault_params",
)


def _placeholders(text: str) -> set[str]:
    # Match {word} but not {{word}} (double-brace escapes are literal in log lines).
    return set(re.findall(r"(?<!\{)\{(\w+)\}(?!\})", text))


def _notes_placeholders(text: str) -> set[str]:
    # As _placeholders, but shell syntax (%{x}, ${x}, @{x}) in commands is literal.
    return set(re.findall(r"(?<![{%$@])\{(\w+)\}(?!\})", text))


# Choice values that read as healthy: a clue that can emit one does not always
# point at the fault.  Choices between fault identities (tank, port) are fine.
_HEALTHY_CHOICE_VALUES = frozenset({
    "off", "ok", "okay", "normal", "nominal", "healthy", "intact", "pass",
    "passed", "green", "none", "false", "no", "idle", "stable", "online", "up",
})


def channel_file_problems(channels_dir: Path) -> list[str]:
    """Problems with the channel *files* in *channels_dir* (empty list = fine).

    A scenario has exactly 20 channels, numbered 01-20, one file each. The loader
    keeps a single file per number, so this must look at the files themselves.
    """
    files = sorted(channels_dir.glob("*.yaml")) if channels_dir.is_dir() else []
    problems: list[str] = []
    by_num: dict[int, list[str]] = {}
    for f in files:
        head = f.stem.split("-", 1)[0]
        if not head.isdigit():
            problems.append(f"channel file '{f.name}' does not start with a number")
            continue
        by_num.setdefault(int(head), []).append(f.name)

    if len(files) != 20:
        problems.append(f"channels/ has {len(files)} files (expected exactly 20)")
    for num, names in sorted(by_num.items()):
        if len(names) > 1:
            problems.append(
                f"duplicate channel number {num:02d}: {', '.join(names)} "
                "(only the last by filename is loaded)"
            )
    outside = sorted(n for n in by_num if not 1 <= n <= 20)
    if outside:
        problems.append(f"channel numbers outside 01-20: {outside}")
    missing = [n for n in range(1, 21) if n not in by_num]
    if missing:
        problems.append(f"missing channel files for numbers: {missing}")
    return problems



# Scope-proportional dashboard impact (Check 32).  scope = 2 x affected + cascade;
# each tier lists the minimum number of non-green and red executive KPI tiles a
# channel's business_impact must produce.  Larger faults must light up more tiles.
SCOPE_TIERS = (
    # (max_scope, name, min_non_green, min_red)
    (3, "minor", 2, 1),
    (5, "moderate", 3, 2),
    (10**6, "major", 4, 3),
)


def scope_tier(channel: dict[str, Any]) -> tuple[str, int, int, int]:
    """Return ``(tier_name, scope, min_non_green, min_red)`` for a channel dict."""
    scope = 2 * len(channel.get("affected_services") or []) + len(channel.get("cascade_services") or [])
    for max_scope, name, non_green, red in SCOPE_TIERS:
        if scope <= max_scope:
            return name, scope, non_green, red
    raise AssertionError("unreachable")


def _uniform_mid(spec: Any) -> float | None:
    if isinstance(spec, dict):
        b = spec.get("uniform") or spec.get("randint")
        if b and len(b) == 2:
            return (float(b[0]) + float(b[1])) / 2
    return None


def impact_band_counts(
    impacts: list[Any], emissions: dict[str, Any],
) -> dict[str, int]:
    # ``emissions`` maps KPI name -> the executive_kpi_emissions entry dict
    # ({name, value, unit, better?}).
    """Count green/yellow/red KPI tiles a channel's business_impact produces.

    Uses the executive dashboard's own palette bands (``kpi_band``) on the
    midpoint of the faulted value (nominal midpoint x factor midpoint, or the
    ``value`` midpoint), so what the verifier accepts is what the tiles show.
    """
    from elastic_config.dashboards.generate_exec_dashboard import kpi_band, kpi_band_usable

    counts = {"green": 0, "yellow": 0, "red": 0}
    for impact in impacts or []:
        if not isinstance(impact, dict):
            continue
        emission = emissions.get(impact.get("kpi")) or {}
        spec = emission.get("value")
        better = emission.get("better")
        nominal = _uniform_mid(spec)
        if nominal is None:
            continue
        if "factor" in impact:
            fm = _uniform_mid(impact["factor"])
            val = nominal * fm if fm is not None else None
        else:
            val = _uniform_mid(impact.get("value"))
        band = (
            kpi_band(impact["kpi"], spec, val, better)
            if val is not None and kpi_band_usable(impact["kpi"], spec, better) else None
        )
        if band:
            counts[band] += 1
    return counts


def _incr_key_state_keys(steps: Any) -> set[str]:
    """Collect ``state_key`` of every incr_key step, recursing into nested step lists."""
    found: set[str] = set()
    if isinstance(steps, list):
        for st in steps:
            found |= _incr_key_state_keys(st)
    elif isinstance(steps, dict):
        ik = steps.get("incr_key")
        if isinstance(ik, dict) and ik.get("state_key"):
            found.add(str(ik["state_key"]))
        for v in steps.values():
            if isinstance(v, (list, dict)):
                found |= _incr_key_state_keys(v)
    return found


def check_scenario(scenario_id: str) -> tuple[list[str], list[str]]:
    """Return (errors, warnings). Empty lists mean all checks passed."""
    errors: list[str] = []
    warnings: list[str] = []

    scenario_dir = SCENARIOS_DIR / scenario_id
    yaml_path = scenario_dir / "scenario.yaml"

    if not yaml_path.exists():
        return [f"scenario.yaml not found under scenarios/{scenario_id}/"], []

    # Load via YamlScenario
    try:
        from scenario_engine.yaml_scenario import load_yaml_scenario
        s = load_yaml_scenario(scenario_dir)
    except Exception as exc:
        return [f"load_yaml_scenario() raised: {exc}"], []

    # ── Check 1b: schema_version ──────────────────────────────────────────────
    # Missing is a warning (treated as 1.0, compatible). Malformed is an error.
    sv = s.schema_version
    if sv is None:
        warnings.append(
            "schema_version is not set in scenario.yaml "
            "(treated as '1.0' — run upgrade-scenario to add it)"
        )
    else:
        try:
            from scenario_engine.schema_version import parse_version, version_status
            parse_version(sv)  # raises ValueError if malformed
            status = version_status(sv)
            if status == "upgrade_required":
                from scenario_engine.schema_version import CURRENT_SCHEMA_VERSION
                errors.append(
                    f"schema_version '{sv}' is incompatible with the current engine "
                    f"schema '{CURRENT_SCHEMA_VERSION}' (upgrade_required — "
                    "run upgrade-scenario before launching)"
                )
            elif status == "outdated_compatible":
                from scenario_engine.schema_version import CURRENT_SCHEMA_VERSION
                warnings.append(
                    f"schema_version '{sv}' is behind current '{CURRENT_SCHEMA_VERSION}' "
                    "(outdated_compatible — consider running upgrade-scenario)"
                )
        except ValueError as exc:
            errors.append(f"schema_version '{sv}' is malformed: {exc}")

    # ── Check 2+3: Channels count, contiguity, required fields ───────────────
    reg = s.channel_registry           # filtered view (display fields only)
    raw_channels = s._channels         # full channel data (includes fault_params, etc.)

    errors.extend(channel_file_problems(scenario_dir / "channels"))

    if len(reg) != 20:
        errors.append(f"channel_registry has {len(reg)} entries (expected 20)")

    expected_nums = set(range(1, 21))
    missing_nums = expected_nums - set(reg.keys())
    extra_nums = set(reg.keys()) - expected_nums
    if missing_nums:
        errors.append(f"missing channel numbers: {sorted(missing_nums)}")
    if extra_nums:
        errors.append(f"unexpected channel numbers: {sorted(extra_nums)}")

    # Check required fields — registry fields against channel_registry,
    # fault_params against the raw _channels dict (not filtered by channel_registry).
    registry_fields = [f for f in _CHANNEL_REQUIRED_FIELDS if f != "fault_params"]
    for ch_num, ch in sorted(reg.items()):
        for field in registry_fields:
            if field not in ch:
                errors.append(f"ch {ch_num} '{ch.get('name', '')}': missing required field '{field}'")
        raw = raw_channels.get(ch_num, {})
        if "fault_params" not in raw:
            errors.append(f"ch {ch_num} '{ch.get('name', '')}': missing required field 'fault_params'")

    # ── Check 4+5: Service count and sort_order coverage ─────────────────────
    svcs = s.services
    if len(svcs) != 9:
        errors.append(f"services has {len(svcs)} entries (expected 9)")

    # Load raw service specs to access sort_order and other flags
    svc_specs = s._service_specs  # dict: name → full spec
    sort_orders = [spec.get("sort_order") for spec in svc_specs.values()]
    if sorted(so for so in sort_orders if so is not None) != list(range(1, len(svc_specs) + 1)):
        errors.append(
            f"sort_order values are not a clean 1–{len(svc_specs)} sequence: {sorted(sort_orders)}"
        )

    # ── Check 6+7: Service identity keys and language ─────────────────────────
    svc_keys = set(svcs.keys())
    for svc_name, spec in svc_specs.items():
        for key in _IDENTITY_KEYS:
            if key not in spec:
                errors.append(f"service '{svc_name}': missing identity key '{key}'")
        lang = spec.get("language")
        if lang and lang not in _ALLOWED_LANGUAGES:
            errors.append(
                f"service '{svc_name}': language '{lang}' not in allowed set "
                f"{sorted(_ALLOWED_LANGUAGES)}"
            )

    # ── Check 8: Hosts and k8s cluster counts ─────────────────────────────────
    if len(s.hosts) != 3:
        errors.append(f"hosts has {len(s.hosts)} entries (expected 3)")
    if len(s.k8s_clusters) != 3:
        errors.append(f"k8s_clusters has {len(s.k8s_clusters)} entries (expected 3)")

    # ── Check 9: Placeholder parity (missing AND orphan fault_params) ─────────
    for ch_num, ch in sorted(reg.items()):
        raw = raw_channels.get(ch_num, ch)
        # "missing" side: only error_message + stack_trace (log signature fields).
        # Widening this to include investigation_notes would create false errors
        # from descriptive placeholders that intentionally have no fault_param.
        log_placeholders = (
            _placeholders(raw.get("error_message", ch.get("error_message", "")))
            | _placeholders(raw.get("stack_trace", ch.get("stack_trace", "")))
        )
        # "orphan" side: include investigation_notes as a valid reference site —
        # a fault_param used only in the agent runbook is still genuinely consumed.
        referenced = log_placeholders | _placeholders(
            raw.get("investigation_notes", ch.get("investigation_notes", ""))
        )
        try:
            params = s.get_fault_params(ch_num)
        except Exception as exc:
            errors.append(f"ch {ch_num}: get_fault_params() raised: {exc}")
            continue
        param_keys = set(params.keys())
        missing = log_placeholders - param_keys
        orphans = param_keys - referenced
        if missing:
            errors.append(
                f"ch {ch_num} '{ch.get('name', '')}': missing fault_params keys: {sorted(missing)}"
            )
        if orphans:
            warnings.append(
                f"ch {ch_num} '{ch.get('name', '')}': orphan fault_params keys "
                f"(not referenced by any placeholder — may be intentional AI context): "
                f"{sorted(orphans)}"
            )

    # ── Check 35: investigation_notes placeholders are fault_params ───────────
    # ── Check 36 (warning): evidence quality ──────────────────────────────────
    for ch_num, ch in sorted(reg.items()):
        raw = raw_channels.get(ch_num, ch)
        label = f"ch {ch_num} '{ch.get('name', '')}'"
        param_keys = set((raw.get("fault_params") or {}).keys())
        unknown = _notes_placeholders(raw.get("investigation_notes", "")) - param_keys
        if unknown:
            errors.append(
                f"{label}: investigation_notes placeholders with no fault_params key "
                f"(rendered as literal text): {sorted(unknown)}"
            )
        choice_clues = sorted(
            f"{svc}:{key}"
            for svc, clues in (raw.get("rca_clues") or {}).items()
            for key, spec in (clues or {}).items()
            if isinstance(spec, dict) and "choice" in spec
            and any(str(v).strip().lower() in _HEALTHY_CHOICE_VALUES for v in spec["choice"])
        )
        if choice_clues:
            warnings.append(
                f"{label}: choice rca_clues can emit a healthy-looking value, so they "
                f"do not always indicate the fault — keep only fault values: {choice_clues}"
            )
        # A faulty unit the frame dump also lists as healthy contradicts itself.
        healthy_lines = [
            line for line in (raw.get("stack_trace", "") or "").splitlines()
            if re.search(r"\b(NOMINAL|PASS|TRACKING|OK|HEALTHY)\b", line)
        ]
        for key, spec in (raw.get("fault_params") or {}).items():
            if not (isinstance(spec, dict) and "choice" in spec):
                continue
            clashes = sorted({
                str(opt) for opt in spec["choice"]
                if len(str(opt)) > 2
                and any(re.search(rf"(?<![\w-]){re.escape(str(opt))}(?![\w-])", ln) for ln in healthy_lines)
            })
            if clashes:
                warnings.append(
                    f"{label}: fault_params '{key}' can pick {clashes}, which the "
                    f"stack_trace lists as healthy — pin the faulty unit"
                )

    # ── Check 10: Referential integrity — affected_services / cascade_services ─
    for ch_num, ch in sorted(reg.items()):
        for field in ("affected_services", "cascade_services"):
            for svc in ch.get(field, []):
                if svc not in svc_keys:
                    errors.append(f"ch {ch_num}: {field} '{svc}' not in services")

    # ── Check 10b: topology callees must be real services ──────────────────────
    # An edge to an undefined service has no OTLP resource: the trace generator
    # used to KeyError on it and emit no spans at all (every ops tile N/A).
    for caller, calls in sorted(s.service_topology.items()):
        for callee, *_ in calls:
            if callee not in svc_keys:
                errors.append(
                    f"service '{caller}': topology callee '{callee}' is not a defined service "
                    f"(add services/{callee}.yaml or remove the edge; external systems are "
                    f"modelled as db_operations or log attributes, not topology)"
                )

    # ── Check 11: k8s_clusters services membership ────────────────────────────
    for cluster in s.k8s_clusters:
        for svc in cluster.get("services", []):
            if svc not in svc_keys:
                errors.append(
                    f"k8s cluster '{cluster.get('name')}': service '{svc}' not in services"
                )

    # ── Check 12: assessment_tool_config completeness ─────────────────────────
    atc = s._data.get("assessment_tool_config")
    if not atc or not isinstance(atc, dict):
        errors.append(
            "assessment_tool_config is missing or empty — must have 'id' and 'description'"
        )
    else:
        if not atc.get("id"):
            errors.append("assessment_tool_config.id is missing or empty")
        if not atc.get("description"):
            errors.append("assessment_tool_config.description is missing or empty")

    # ── Check 12a: agent_config completeness + cross-consistency ──────────────
    agent_cfg = s.agent_config
    for field in ("id", "name", "assessment_tool_name", "system_prompt"):
        if not (agent_cfg or {}).get(field):
            errors.append(f"agent_config.{field} is missing or empty")
    if atc and isinstance(atc, dict) and agent_cfg:
        atc_id = atc.get("id", "")
        atn = agent_cfg.get("assessment_tool_name", "")
        if atc_id and atn and atn != atc_id:
            errors.append(
                f"agent_config.assessment_tool_name '{atn}' must equal "
                f"assessment_tool_config.id '{atc_id}'"
            )

    # ── Check 12b: system_prompt ↔ error_types (bidirectional) ────────────────
    system_prompt = (agent_cfg or {}).get("system_prompt", "")
    channel_error_types = {
        ch.get("error_type") for ch in reg.values() if ch.get("error_type")
    }
    if system_prompt:
        for et in sorted(channel_error_types):
            if et not in system_prompt:
                errors.append(
                    f"agent_config.system_prompt does not mention channel error_type '{et}'"
                )

    # ── Check 12c: leftover TODO placeholders in scenario YAML files ───────────
    scenario_dir = SCENARIOS_DIR / scenario_id
    todo_files = [scenario_dir / "scenario.yaml"]
    todo_files += sorted((scenario_dir / "channels").glob("*.yaml"))
    todo_files += sorted((scenario_dir / "services").glob("*.yaml"))
    for fpath in todo_files:
        if not fpath.exists():
            continue
        raw = fpath.read_text(encoding="utf-8")
        if "TODO" in raw:
            # Report the first hit with a snippet so the author knows exactly where to look
            for lineno, line in enumerate(raw.splitlines(), 1):
                if "TODO" in line:
                    rel = fpath.relative_to(scenario_dir)
                    errors.append(
                        f"leftover TODO placeholder in {rel} line {lineno}: "
                        f"{line.strip()[:80]}"
                    )

    # ── Check 13: trace_attributes.services keys ──────────────────────────────
    trace_cfg = s._data.get("trace_attributes", {})
    trace_svc_keys = set(trace_cfg.get("services", {}).keys())
    orphan_trace_svcs = trace_svc_keys - svc_keys
    if orphan_trace_svcs:
        errors.append(
            f"trace_attributes.services contains keys that are not real services: "
            f"{sorted(orphan_trace_svcs)}"
        )

    # ── Check 14: KPI triple-redundancy + emitter validity ────────────────────
    kpi_em = s._data.get("executive_kpi_emissions")
    if kpi_em is not None and not isinstance(kpi_em, list):
        errors.append("executive_kpi_emissions is not a list")
    else:
        emission_names = {e["name"] for e in (kpi_em or []) if isinstance(e, dict) and "name" in e}
        # Check that executive_kpi_emitter_service_name names a real service
        emitter_svc = s.executive_kpi_emitter_service_name
        if emitter_svc and emitter_svc not in svc_keys:
            errors.append(
                f"executive_kpi_emitter_service_name '{emitter_svc}' is not a real service"
            )
        # Check that sections reference field names that exist in emissions
        for section in s._data.get("executive_kpi_sections", []):
            for spec_pair in section.get("specs", []):
                if len(spec_pair) >= 2:
                    field = spec_pair[1]  # e.g. "metrics.business.<field>"
                    # Extract the trailing segment to match against emission names
                    field_suffix = field.split(".")[-1] if "." in field else field
                    match = any(field_suffix in name for name in emission_names)
                    if not match and emission_names:
                        errors.append(
                            f"executive_kpi_sections spec field '{field}' does not match "
                            f"any executive_kpi_emissions name"
                        )
                        break  # one error per section is enough

    # ── Check 30: business_impact drives the executive dashboard ──────────────
    # Without a dict-form business_impact the emitter leaves every KPI at its
    # nominal draw, so the executive dashboard stays green during a fault.
    if s.executive_kpi_emitter_service_name and isinstance(kpi_em, list):
        unwired = []
        for num, ch in sorted(raw_channels.items()):
            impacts = ch.get("business_impact")
            if not impacts:
                unwired.append(num)
                continue
            if not isinstance(impacts, list):
                errors.append(f"ch {num}: business_impact must be a list of {{kpi, factor|value}} entries")
                continue
            for impact in impacts:
                if not isinstance(impact, dict) or not ("factor" in impact or "value" in impact):
                    errors.append(
                        f"ch {num}: business_impact entry {impact!r} must be a dict with "
                        f"'kpi' and 'factor' or 'value' (bare KPI names are ignored at runtime)"
                    )
                elif impact.get("kpi") not in emission_names:
                    errors.append(
                        f"ch {num}: business_impact kpi '{impact.get('kpi')}' is not in executive_kpi_emissions"
                    )
                else:
                    bounds = (impact.get("factor") or {}).get("uniform")
                    if bounds and min(bounds) < 1.0 < max(bounds):
                        warnings.append(
                            f"ch {num}: business_impact factor {bounds} for '{impact['kpi']}' "
                            f"straddles 1.0 — the KPI may not visibly move"
                        )
        if unwired:
            errors.append(
                f"channel(s) {unwired} have no business_impact — the executive "
                f"dashboard will not react to them (see contract/channels.md §Business impact)"
            )

    # ── Check 31: a trace entry service is in affected_services ───────────────
    # generators/traces/generator.py only raises the error rate when a trace's
    # entry service is in the channel's affected_services, so a fault that only
    # names a DB / non-entry service never turns the ops dashboard tiles red.
    _no_trace = {
        n for n, sp in svc_specs.items()
        if sp.get("generates_traces") is False or sp.get("subsystem") == "database"
    }
    _ordered = sorted(
        (n for n in svc_specs if n not in _no_trace),
        key=lambda n: svc_specs[n].get("sort_order", 99),
    )
    entry_services = set(_ordered[:1]) | {
        n for n, eps in s.entry_endpoints.items() if eps and n not in _no_trace
    }
    for num, ch in sorted(raw_channels.items()):
        if not set(ch.get("affected_services") or []) & entry_services:
            errors.append(
                f"ch {num}: affected_services {ch.get('affected_services')} contains no trace "
                f"entry service {sorted(entry_services)} — the ops dashboard will not react "
                f"(add an entry service, see contract/channels.md §Dashboard visibility)"
            )

    # ── Check 32: business_impact is scope-proportional ───────────────────────
    if s.executive_kpi_emitter_service_name and isinstance(kpi_em, list):
        _emissions = {e["name"]: e for e in kpi_em if isinstance(e, dict) and "name" in e}
        for _name, _em in sorted(_emissions.items()):
            if _em.get("better") not in (None, "lower", "higher"):
                errors.append(f"executive_kpi_emissions '{_name}': better must be 'lower' or 'higher', got {_em['better']!r}")
        _impacted = {
            i.get("kpi") for ch in raw_channels.values() for i in (ch.get("business_impact") or [])
            if isinstance(i, dict)
        }
        _undirected = sorted(
            k for k in _impacted
            if k in _emissions and _emissions[k].get("better") is None
        )
        if _undirected:
            errors.append(
                f"executive_kpi_emissions {_undirected} are used by business_impact but have no "
                f"`better: lower|higher` — tile colours would depend on a name-keyword guess"
            )
        for num, ch in sorted(raw_channels.items()):
            if not ch.get("business_impact"):
                continue  # already reported by Check 30
            tier, scope, need_non_green, need_red = scope_tier(ch)
            counts = impact_band_counts(ch["business_impact"], _emissions)
            non_green = counts["yellow"] + counts["red"]
            if non_green < need_non_green or counts["red"] < need_red:
                errors.append(
                    f"ch {num}: {tier} fault (scope {scope}) needs >= {need_non_green} yellow/red "
                    f"KPI tiles incl. >= {need_red} red, business_impact gives {counts} "
                    f"(see contract/channels.md §Dashboard visibility)"
                )

    # ── Check 33: incr_key state keys are dicts ───────────────────────────────
    for name, spec in svc_specs.items():
        keys = _incr_key_state_keys(spec.get("steps")) | _incr_key_state_keys(
            spec.get("per_active_channel_steps")
        )
        for key in sorted(keys):
            init = (spec.get("state") or {}).get(key)
            ok = isinstance(init, dict) and (
                "init_per_key" in init or ("init" in init and isinstance(init["init"], dict))
            )
            if not ok:
                errors.append(
                    f"service '{name}': incr_key state '{key}' must be initialised as a dict "
                    f"(`{key}: {{init: {{}}}}`), got {init!r} — a scalar init crashes the loop"
                )

    # ── Check 15: entry_endpoints on the sort_order:1 service ─────────────────
    entry_svc_name: str | None = None
    for name, spec in svc_specs.items():
        if spec.get("sort_order") == 1:
            entry_svc_name = name
            break
    if entry_svc_name:
        entry_eps = s.entry_endpoints
        if entry_svc_name not in entry_eps or not entry_eps[entry_svc_name]:
            errors.append(
                f"sort_order:1 service '{entry_svc_name}' must define entry_endpoints "
                f"(it receives 4× trace-entry weighting)"
            )

    # ── Check 16: DSL service classes — no silent swallow ─────────────────────
    try:
        classes = s.get_service_classes()
        if len(classes) != 9:
            errors.append(f"get_service_classes() returned {len(classes)} (expected 9)")
        # Verify each service has a non-empty steps body unless it explicitly
        # disables telemetry (generates_traces: false AND emit_fault_logs: false).
        for cls in classes:
            svc_name = cls.SERVICE_NAME
            spec = svc_specs.get(svc_name, {})
            no_traces = not spec.get("generates_traces", True)
            no_logs = not spec.get("emit_fault_logs", True)
            if no_traces and no_logs:
                continue  # explicitly disabled — skip
            steps = spec.get("steps")
            if not steps:
                errors.append(
                    f"service '{svc_name}': empty or missing steps: body "
                    f"(set generates_traces: false AND emit_fault_logs: false to suppress this check)"
                )
    except Exception as exc:
        errors.append(f"get_service_classes() raised: {exc}")

    # ── Check 17: get_rca_clues returns dict ──────────────────────────────────
    import random
    rng = random.Random(42)
    for ch_num in range(1, 21):
        for svc in list(svc_keys)[:2]:  # spot-check 2 services per channel
            try:
                result = s.get_rca_clues(ch_num, svc, rng)
                if not isinstance(result, dict):
                    errors.append(f"ch {ch_num}: get_rca_clues() returned non-dict")
                    break
            except Exception as exc:
                errors.append(f"ch {ch_num}: get_rca_clues() raised: {exc}")

    # ── Check 18: get_trace_attributes returns dict ────────────────────────────
    if trace_cfg.get("base") or trace_cfg.get("services"):
        for svc in list(svc_keys)[:2]:
            try:
                result = s.get_trace_attributes(svc, rng)
                if not isinstance(result, dict):
                    errors.append(f"get_trace_attributes() returned non-dict for {svc}")
            except Exception as exc:
                errors.append(f"get_trace_attributes() raised for {svc}: {exc}")

    # ── Check 19 (warning): cloud_provider distribution ──────────────────────
    providers = [spec.get("cloud_provider") for spec in svc_specs.values()]
    provider_counts = Counter(p for p in providers if p)
    if len(provider_counts) < 3:
        warnings.append(
            f"cloud_provider distribution uses fewer than 3 providers: {dict(provider_counts)} "
            f"(expected aws, gcp, azure each)"
        )
    else:
        min_count = min(provider_counts.values())
        max_count = max(provider_counts.values())
        if max_count - min_count > 3:
            warnings.append(
                f"cloud_provider distribution is unbalanced: {dict(provider_counts)} "
                f"(aim for ~3/3/3)"
            )

    # ── Check 20: k8s_clusters schema completeness ────────────────────────────
    # Scaffolder used to emit `cloud_provider`/`cloud_region`; runtime expects
    # `provider`/`region`/`platform`.  Catch the mismatch before deploy.
    for i, cluster in enumerate(s.k8s_clusters):
        cname = cluster.get("name", f"[{i}]")
        for req_field in ("provider", "region", "platform"):
            if not cluster.get(req_field):
                errors.append(
                    f"k8s_clusters '{cname}': missing or empty '{req_field}' "
                    f"(scaffold may have emitted 'cloud_{req_field}' — wrong key)"
                )

    # ── Check 21 (warning): host names must not use the generic scaffold- default ──
    # Branded persona prefixes (finserv, meridian, nova7, …) are unique and intentional;
    # only the generic "scaffold-" default causes cross-scenario log collisions.
    for host in s._data.get("hosts", []):
        hname = host.get("host.name", "")
        if hname.startswith("scaffold-"):
            warnings.append(
                f"host.name '{hname}' uses the generic 'scaffold-' prefix — these collide "
                f"across simultaneously-deployed scenarios; rename to a unique prefix "
                f"(e.g. '{scenario_id}-<provider>-host-NN')"
            )
            break  # one warning is enough

    # ── Check 22 (warning): rca_clues completeness ────────────────────────────
    # NOTE: s.channel_registry is a filtered display-only view that EXCLUDES rca_clues;
    # must use s._channels (raw channel dicts keyed 1..20) to inspect this field.
    channels_with_rca = sum(
        1 for ch in s._channels.values()
        if ch.get("rca_clues") and isinstance(ch["rca_clues"], dict) and ch["rca_clues"]
    )
    if channels_with_rca == 0:
        warnings.append(
            "all 20 channels have empty rca_clues — the AI agent will have no "
            "domain-specific investigation signals; add 2–3 per-service clues to "
            "each channel (see SKILL.md Phase 5 and an ecommerce channel for examples)"
        )

    # ── Check 23 (warning): topology coverage ─────────────────────────────────
    svcs_with_topology = sum(
        1 for spec in svc_specs.values() if spec.get("topology")
    )
    if svcs_with_topology == 0:
        warnings.append(
            "all services have empty topology — APM service map will show no call "
            "edges; build realistic call chains in Phase 5 using the "
            "[downstream-service, endpoint, METHOD] DSL"
        )

    # ── Check 24 (warning): entry_endpoints are not generic stubs ─────────────
    if entry_svc_name:
        all_eps = s.entry_endpoints
        eps = all_eps.get(entry_svc_name, [])
        if eps:
            ep_path_endings = {
                e[0].rstrip("/").split("/")[-1]
                for e in eps if isinstance(e, (list, tuple)) and e
            }
            if ep_path_endings.issubset({"health", "process"}):
                warnings.append(
                    f"sort_order:1 service '{entry_svc_name}' still has the generic "
                    f"stub entry_endpoints (health/process only) — replace with 4–6 "
                    f"real business routes in Phase 5 (e.g. /checkout, /search)"
                )

    # ── Check 25 (warning): trace_attributes richness ─────────────────────────
    trace_svc_attrs = s._data.get("trace_attributes", {}).get("services", {})
    thin_svcs = sorted(
        svc for svc in svc_keys if len(trace_svc_attrs.get(svc, {})) < 3
    )
    if thin_svcs:
        sample = ", ".join(thin_svcs[:3]) + ("…" if len(thin_svcs) > 3 else "")
        warnings.append(
            f"{len(thin_svcs)} service(s) have fewer than 3 trace_attributes "
            f"({sample}) — CONTRACT.md recommends 4–5 per service; expand in Phase 5 "
            f"(e.g. add tier, feature flags, customer segment, region)"
        )

    # ── Check 26: generator wiring ─────────────────────────────────────────────
    # 26a/26b (error): generator_impact blocks may only reference real generator
    # ids and real impact_keys for that generator — typos here silently no-op
    # and the alert never breaches.
    # 26c (warning, not error): every enabled generator should have at least one
    # channel wiring it via generator_impact. This is a warning rather than a
    # hard error because some enabled generators are legitimately unwired —
    # e.g. QA/integration-testing harnesses that enable generators purely to
    # keep telemetry flowing, or companion generators (like azure_billing
    # riding along with azure_openai) for which a dedicated fault channel may
    # not make sense. The scaffolder enforces 1:1 wiring at creation time for
    # new scenarios; this check surfaces (without blocking) gaps in existing ones.
    from generator_engine.registry import GENERATORS

    # A `gid: false` (or `enabled: false`) entry turns a generator off, so it is
    # not "enabled" and has nothing to wire.
    enabled_gens = {
        gid
        for gid, cfg in (s.generators or {}).items()
        if cfg is not False and not (isinstance(cfg, dict) and cfg.get("enabled") is False)
    }
    impacted_gens: set[str] = set()
    impacted_keys: dict[str, set[str]] = {}
    for ch in raw_channels.values():
        gi = ch.get("generator_impact") or {}
        for gid, keys in gi.items():
            if gid not in GENERATORS:
                errors.append(
                    f"generator_impact references unknown generator id '{gid}' "
                    f"(check python3 -m generator_engine.registry --list)"
                )
                continue
            impacted_gens.add(gid)
            spec = GENERATORS[gid]
            for key in keys or {}:
                impacted_keys.setdefault(gid, set()).add(key)
                if key not in spec.impact_keys:
                    errors.append(
                        f"generator_impact.{gid} references unknown impact_key "
                        f"'{key}' (check python3 -m generator_engine.registry "
                        f"--describe {gid})"
                    )

    # Check 27: enablement. enabled_specs() applies the scenario's generators:
    # block on top of each generator's default_enabled and ignores unknown ids,
    # so each of these would otherwise fail silently at runtime.
    from generator_engine.registry import enabled_specs

    gen_block = s.generators or {}
    enabled = {spec.id: spec for spec in enabled_specs(s)}
    for gid in sorted(gen_block):
        if gid not in GENERATORS:
            errors.append(
                f"generators: block references unknown generator id '{gid}' "
                "(ignored at runtime; check python3 -m generator_engine.registry --list)"
            )
    for num, ch in sorted(raw_channels.items()):
        for gid in (ch.get("generator_impact") or {}):
            if gid in GENERATORS and gid not in enabled:
                errors.append(
                    f"channel {num:02d} ({ch.get('error_type')}) drives generator "
                    f"'{gid}', which is not enabled for this scenario — the fault "
                    f"would never be generated; add '{gid}: true' to generators:"
                )
    for gid, spec in sorted(enabled.items()):
        cfg = gen_block.get(gid)
        provided = cfg if isinstance(cfg, dict) else {}
        for fld, meta in (spec.config_fields or {}).items():
            if meta.get("required") and meta.get("default") is None and fld not in provided:
                errors.append(
                    f"generator '{gid}' is enabled but required config '{fld}' is not set"
                )

    # infra_impact: keys are applied by ChaosController.set_infra_spikes; an unknown key
    # is stored and then never read by any generator.
    from app.chaos.controller import INFRA_SPIKE_DEFAULTS

    for num, ch in sorted(raw_channels.items()):
        for key in (ch.get("infra_impact") or {}):
            if key not in INFRA_SPIKE_DEFAULTS:
                errors.append(
                    f"channel {num:02d} ({ch.get('error_type')}) infra_impact key "
                    f"'{key}' is not an infra spike (valid: {sorted(INFRA_SPIKE_DEFAULTS)})"
                )

    unwired = sorted(enabled_gens - impacted_gens)
    if unwired:
        warnings.append(
            f"generator(s) enabled in scenario.yaml but not wired to any "
            f"channel's generator_impact: {unwired} — their alerts will "
            f"never fire; add a dedicated chaos channel or confirm this is "
            f"intentional (e.g. a companion/QA-only generator)"
        )

    # 26d (warning): alert knobs no channel sets. Covers every enabled generator,
    # core ones included, so a scenario shows which package alerts it can never
    # fire. Knobs are the impact keys whose doc names an alert.
    for gid, spec in sorted(enabled.items()):
        if gid in unwired:
            continue
        knobs = {
            key for key, meta in spec.impact_keys.items()
            if "Drive alert" in meta.get("doc", "") or "Alert:" in meta.get("doc", "")
        }
        missing = sorted(knobs - impacted_keys.get(gid, set()))
        if missing:
            warnings.append(
                f"{gid}: {len(missing)}/{len(knobs)} alert knob(s) not set by any "
                f"channel, so those alerts never fire: {missing}"
            )

    return errors, warnings


_HOST_ID_FIELDS = ("host.id", "cloud.account.id", "cloud.instance.id")
_GENERATOR_IDENTITY_BANS = (
    'f"{ns}-',
    "f'{ns}-",
    'f"{db_prefix}_',
    "f'{db_prefix}_",
    "123456789012",
)


def check_checked_in_host_identity() -> list[str]:
    """Fail when a scenario file's host ids disagree with ScenarioIdentity."""
    import yaml
    from generator_engine.identity import apply_host_identity

    errors: list[str] = []
    accounts: dict[str, set[str]] = defaultdict(set)
    instances: dict[str, list[str]] = defaultdict(list)
    roots = [SCENARIOS_DIR, REPO_ROOT / "scenarios_testing"]
    for root in roots:
        if not root.is_dir():
            continue
        for path in sorted(root.glob("*/scenario.yaml")):
            data = yaml.safe_load(path.read_text()) or {}
            ns = str(data.get("namespace") or path.parent.name)
            hosts = data.get("hosts") or []
            expected = apply_host_identity(list(hosts), ns)
            label = f"{root.name}/{path.parent.name}"
            if len(hosts) != len(expected):
                errors.append(f"{label}: host count mismatch")
                continue
            for index, (raw, exp) in enumerate(zip(hosts, expected), start=1):
                for field in _HOST_ID_FIELDS:
                    got = "" if raw.get(field) is None else str(raw.get(field))
                    want = "" if exp.get(field) is None else str(exp.get(field))
                    if got != want:
                        errors.append(
                            f"{label} host {index} {field} is {got!r}, expected {want!r}"
                        )
                account = str(raw.get("cloud.account.id") or "")
                if account:
                    accounts[account].add(ns)
                for field in ("host.id", "cloud.instance.id"):
                    value = str(raw.get(field) or "")
                    if value:
                        instances[value].append(f"{ns} {field} host {index}")
    for account, namespaces in sorted(accounts.items()):
        if len(namespaces) > 1:
            errors.append(
                f"cloud account {account} is shared by {sorted(namespaces)}"
            )
    for value, where in sorted(instances.items()):
        namespaces = {item.split()[0] for item in where}
        if len(namespaces) > 1 or len(where) > len(namespaces):
            # Same namespace may repeat host.id and cloud.instance.id once each.
            per_ns: dict[str, int] = Counter(item.split()[0] for item in where)
            if len(namespaces) > 1 or any(count > 2 for count in per_ns.values()):
                errors.append(f"instance id {value} is shared: {where}")
    return errors


def check_generator_identity_literals() -> list[str]:
    """Fail when a generator still formats identity or uses the shared AWS account."""
    errors: list[str] = []
    for path in sorted((REPO_ROOT / "generators").glob("*/generator.py")):
        text = path.read_text()
        for ban in _GENERATOR_IDENTITY_BANS:
            if ban in text:
                errors.append(
                    f"{path.relative_to(REPO_ROOT)} contains {ban!r}"
                )
    return errors


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Verify YAML scenario integrity")
    parser.add_argument(
        "scenarios", nargs="*", help="Scenario IDs (default: all with scenario.yaml)"
    )
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help="Print failures but exit 0 (do not fail CI)",
    )
    args = parser.parse_args()

    if args.scenarios:
        target_ids = args.scenarios
    else:
        target_ids = sorted(
            p.parent.name for p in SCENARIOS_DIR.glob("*/scenario.yaml")
        )

    if not target_ids:
        print("No scenarios with scenario.yaml found under scenarios/.")
        sys.exit(1)

    overall_ok = True
    for sid in target_ids:
        errs, warns = check_scenario(sid)
        if errs or warns:
            if errs:
                overall_ok = False
            marker = "✗" if errs else "⚠"
            print(f"{marker} {sid}:")
            for e in errs:
                print(f"    - {e}")
            for w in warns:
                print(f"    ~ {w}")
        else:
            print(f"✓ {sid}")

    for title, findings in (
        ("host identity", check_checked_in_host_identity()),
        ("generator identity literals", check_generator_identity_literals()),
    ):
        if findings:
            overall_ok = False
            print(f"\n✗ {title}:")
            for finding in findings:
                print(f"    - {finding}")
        else:
            print(f"\n✓ {title}")

    if overall_ok:
        print("\nAll checks passed.")
    else:
        print("\nSome checks failed.")
        if not args.warn_only:
            sys.exit(1)


if __name__ == "__main__":
    main()
