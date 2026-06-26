#!/usr/bin/env python3
"""Verify that YAML scenarios pass all integrity checks.

Checks performed for each scenario:
  1.  scenario.yaml loads without errors
  1b. schema_version: if present, must parse as MAJOR.MINOR; malformed → error.
      Missing → warning only (treated as '1.0', compatible with current engine).
      If older MAJOR than engine → error (upgrade_required).
      If same MAJOR, older MINOR → warning (outdated_compatible).
  2.  channel count == 20; numbers are contiguous 01–20
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
  12. agent_config.system_prompt references all 20 channel error_types, and
      every error_type in the prompt is present in a channel (bidirectional)
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
from collections import Counter
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

    # ── Check 10: Referential integrity — affected_services / cascade_services ─
    for ch_num, ch in sorted(reg.items()):
        for field in ("affected_services", "cascade_services"):
            for svc in ch.get(field, []):
                if svc not in svc_keys:
                    errors.append(f"ch {ch_num}: {field} '{svc}' not in services")

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

    return errors, warnings


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

    if overall_ok:
        print("\nAll checks passed.")
    else:
        print("\nSome checks failed.")
        if not args.warn_only:
            sys.exit(1)


if __name__ == "__main__":
    main()
