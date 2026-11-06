# add-telemetry-generator CONTRACT

## Deliverable

A new package under `generators/<id>/` with:

- `__init__.py` — `GeneratorSpec` (`schema_version`, datasets, integrations,
  `impact_keys`, `wants_chaos`, …)
- `generator.py` — emit loop using `resolve_active_chaos` when chaos is enabled
- `test_generator.py` — coverage for emit shape and chaos on/off

## Alert ES|QL emit (when templates exist)

- Run `scripts/extract_alert_esql_fields.py` → `alert_required_fields.json`.
- Run `scripts/verify_alert_emit.py` against `generators/<id>/` (must exit 0).
- Emit every required field / COALESCE operand; honor `counter_required` vs
  `gauge_preferred` per [ALERTS.md](ALERTS.md).
- CloudWatch: `Namespace` + `MetricName` on every CW datapoint.
- Dataset mismatch: companion emit or fail — never edit OOTB templates.
- Do not treat `impact_keys` 1:1 as a substitute for field coverage.

## SLO template / ML module emit (when templates exist)

- Run `scripts/extract_package_templates.py` → `slo_required_fields.json`,
  `ml_required_fields.json`; run `scripts/verify_template_emit.py` (must exit 0).
- `scripts/snapshot_package_templates.py <id>`, then
  `tests/test_slo_template_emit.py` and `tests/test_ml_module_emit.py` must pass for
  the generator: every SLO filter matched, SLI metrics numeric, `groupBy` fields
  present, SLI healthy on the baseline; every ML detector fed with its split fields.
- Dimensions SLOs group on and ML jobs split on go where the template reads them
  (`resource.attributes.*` vs `attributes.*`); see [TEMPLATES.md](TEMPLATES.md).
- No new entries in `tests/fixtures/template_emit_pending.json`.

## Impact_keys

- **Minimum 2:** `len(SPEC.impact_keys) >= 2` for every new generator.
- **1:1 alerts:** when the package has `N >= 1` alert rules, include exactly
  one key per alert (and per SLO). If `N == 1`, add ≥1 non-alert emit knob.
  If `N == 0` or `integrations=()`, invent ≥2 emit-backed knobs.
- Count alerts from the inventory **alerts** list / rule templates — not
  flattened threshold expressions.
- Every key **applied** in `generator.py` (`active_chaos["<key>"]`).
- `wants_chaos=True` whenever `impact_keys` is non-empty.
- Healthy baseline when chaos is inactive.
- Do not edit `scenarios/` from this skill.
- Do not synthesize `kibana.alert.*` or fake SLO documents.

## Package grounding

Merged dashboards/**alerts**/SLOs/ML modules are the emit set. Authority A (fields.yml /
receiver metadata) is types/units/attrs. Run extract/merge/verify scripts
before inventing metric names. Alert field inventory is mandatory when
`alerting_rule_template` exists.

## Citation

```python
SOURCE_INTEGRATION = "elastic/integrations packages/<id>@<version>"
```

Place after `from __future__ import annotations`. The citation is the **install pin**: step 20 installs exactly this package version (`generator_engine.registry.package_versions`), so the deployed dashboards, alerts, SLOs and ML jobs are the ones the generator was built against. Cite every package in `SPEC.integrations`; `tests/test_package_pins.py` fails on a missing or conflicting citation, or on fixtures snapshotted from another version.
