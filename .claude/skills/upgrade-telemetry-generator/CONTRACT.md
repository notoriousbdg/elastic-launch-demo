# upgrade-telemetry-generator CONTRACT

## Capabilities

| Capability | Meaning |
|---|---|
| **validate** | Read-only: engine schema status + package citation drift + alert ES\|QL emit gaps (+ optional live rule errors) |
| **upgrade** | Write under `generators/<id>/`: package re-ground and/or pending engine migrations |

## Version semantics

Same as scenarios (`scenario_engine/schema_version.py`):

| Status | Meaning | Launch |
|--------|---------|--------|
| `up_to_date` | Matches `generator_engine/schema.yaml` `current` | Allowed |
| `outdated_compatible` | Same MAJOR, older MINOR | Allowed; recommend upgrade |
| `upgrade_required` | Older MAJOR | **Blocked** for any scenario that enables the generator |
| `ahead` | Newer MAJOR than engine | Treat as incompatible |

Unversioned SPECs use `default_unversioned` (currently `"1.0"`).

## Where version lives

```python
# generators/<id>/__init__.py
SPEC = GeneratorSpec(
    ...
    schema_version="1.0",
)
```

## Migration entry shape (`generator_engine/schema.yaml`)

```yaml
migrations:
  - to: "1.1"
    breaking: false
    summary: "One-line description"
    guidance: |
      Step-by-step edits under generators/<id>/ only.
    needs_input: false
```

Maintainer edits to `schema.yaml` are rare; after registering a new `current`,
run **upgrade** on affected generators.

## Deployability

- Scenario Launch is disabled (API 409 + UI pill) when any **enabled** generator is `upgrade_required`.
- Import UI shows per-generator and per-scenario version rows plus `blocked_scenarios`.

## Package citation (upgrade)

Generators created with add-telemetry-generator declare:

```python
SOURCE_INTEGRATION = "elastic/integrations packages/<id>@<version>"
```

Place the constant **after** `from __future__ import annotations` (never before
`__future__` or the module docstring/shebang). The citation is the **install pin**: step 20 installs exactly this package version (`generator_engine.registry.package_versions`), so the deployed dashboards, alerts, SLOs and ML jobs are the ones the generator was built against. Cite every package in `SPEC.integrations`; `tests/test_package_pins.py` fails on a missing or conflicting citation, or on fixtures snapshotted from another version.

**validate** compares that version to `packages/<id>/manifest.yml` on
`elastic/integrations`. **upgrade** re-runs add-telemetry-generator
extract/merge helpers and inventory-diff (including
`extract_alert_esql_fields.py` + `verify_alert_emit.py`; plus
`verify_metric_coverage.py --check-emit` when present) before editing
`generators/<id>/`. Merged dashboards/**alerts**/SLOs/ML modules are the emit set;
Authority A is types/units/attrs only.

## Alert ES|QL emit (validate + upgrade)

- When `alerting_rule_template` exists, produce `alert_required_fields.json`
  and pass `verify_alert_emit.py` (exit 0) before upgrade is done.
- Honor `counter_required`, `coalesce_operands`, CloudWatch
  `Namespace`/`MetricName`, and `from_streams` vs `SPEC.datasets` per
  [ALERTS.md](ALERTS.md) / [../add-telemetry-generator/ALERTS.md](../add-telemetry-generator/ALERTS.md).
- Live `verification_exception` on Kibana rules → recommend **upgrade** on
  generators; never edit OOTB rules from this skill.
- `impact_keys` 1:1 does **not** replace field coverage.

## SLO template / ML module emit (validate + upgrade)

- When the package ships `slo_template` or `ml_module` assets, run
  `extract_package_templates.py` + `verify_template_emit.py` (exit 0), refresh
  `python3 scripts/snapshot_package_templates.py <id>`, and pass
  `tests/test_slo_template_emit.py` + `tests/test_ml_module_emit.py -k <id>` before
  upgrade is done. **validate** reports their failures as gaps.
- Remove the generator's entries from `tests/fixtures/template_emit_pending.json` as
  the upgrade fixes them (strict xfail fails once fixed). Never edit package templates.
- See [TEMPLATES.md](TEMPLATES.md).

## Alert / SLO impact_keys (upgrade)

Package alerts (and synthetic faults) are scenario-selectable via generator
`impact_keys`:

- **Minimum 2:** `len(SPEC.impact_keys) >= 2` for every generator.
- **1:1:** when the package has `N >= 1` alert rules, include **exactly one
  key per alert** (and per SLO target). Count alerts from the inventory
  **alerts** list / rule templates — not flattened thresholds. If `N == 1`,
  add ≥1 non-alert emit knob so total ≥ 2. If `N == 0`, invent ≥2 emit-backed
  knobs.
- Every key must be **applied** in `generator.py` when chaos is active
  (`active_chaos["<key>"]`). Declare-without-apply is a hard miss.
- `wants_chaos=True` whenever `impact_keys` is non-empty.
- **Resolvable:** with chaos inactive, emit stays under thresholds / healthy
  baseline.
- Do not edit `scenarios/` from this skill — channels pick from these keys later.
- Do not synthesize `kibana.alert.*` or fake SLO documents.
