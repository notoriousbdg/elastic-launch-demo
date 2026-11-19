# add-telemetry-generator scripts

Shipped gates for package-backed generators. Prefer these over ad-hoc greps.

| Script | Purpose |
|---|---|
| `extract_alert_esql_fields.py` | Parse OOTB `alerting_rule_template` ES\|QL → `alert_required_fields.json` |
| `verify_alert_emit.py` | Static check that `generators/<id>/` sources mention required alert tokens |
| `extract_package_templates.py` | Parse `slo_template` / `ml_module` assets → `slo_required_fields.json`, `ml_required_fields.json`; also the KQL, query-DSL and SLI helpers the runtime tests import |
| `verify_template_emit.py` | Static check that `generators/<id>/` sources mention what the SLO templates / ML modules read |

`extract_alert_esql_fields.alert_identity(esql)` also reports how a rule groups rows into
alerts (`STATS ... BY` or `KEEP` columns, pre-grouping filters, minimum `COUNT`).
`scripts/snapshot_alert_identity.py` writes it for every package to
`tests/fixtures/alert_identity.json`, which `tests/test_alert_identity_stable.py` reads.

## Typical flow

```bash
PKG=.cache/integrations/packages/<id>
OUT=/tmp/<id>

python3 .claude/skills/add-telemetry-generator/scripts/extract_alert_esql_fields.py \
  --package-dir "$PKG" --out "$OUT/alert_required_fields.json"

python3 .claude/skills/add-telemetry-generator/scripts/verify_alert_emit.py \
  --inventory "$OUT/alert_required_fields.json" \
  --generator-dir "generators/<id>"
```

Dashboard / constraint extract-merge scripts referenced in GUARDRAILS
(`extract_dashboard_constraints.py`, `verify_metric_coverage.py`, …) may live
alongside a fuller skill checkout. When they are absent, still run the two
scripts above for every package that ships alerting rule templates.

## `alert_required_fields.json` keys

- `fields` — union of identifiers referenced by alert ES\|QL
- `counter_required` — `INCREASE()` / `RATE()` operands (must be `counter_long`)
- `gauge_preferred` — `SUM`/`AVG`/`MAX`/`MIN` targets not also counter-required
- `coalesce_operands` — every named `COALESCE(...)` arm (all must exist as columns)
- `from_streams` — `FROM` index patterns (check vs `SPEC.datasets`)
- `requires_cloudwatch_namespace_metricname` — emit `Namespace` + `MetricName`

## SLO / ML template flow

```bash
python3 .claude/skills/add-telemetry-generator/scripts/extract_package_templates.py \
  --package-dir "$PKG" --out-dir "$OUT"
python3 .claude/skills/add-telemetry-generator/scripts/verify_template_emit.py \
  --inventory-dir "$OUT" --generator-dir "generators/<id>"
python3 scripts/snapshot_package_templates.py <id>      # tests/fixtures/{slo_templates,ml_modules}.json
python3 -m pytest -q tests/test_slo_template_emit.py tests/test_ml_module_emit.py -k <id>
```

`slo_required_fields.json` entries: `index`, `filter`, `required_terms`, `metrics[]`
(`part`, `name`, `aggregation`, `field`, `filter`), `equations`, `comparator`, `threshold`,
`budgeting`, `objective`, `group_by`, `fields`.

`ml_required_fields.json` entries: `default_index`, `index_mismatch`, `discovery_datasets`,
`query`, `datafeeds[]` (`query`, `fields` = aggregation sources, `aggregated`), `jobs[]`
(`detectors[]` with `field`/`by`/`over`/`partition`, `influencers`, `bucket_span`), `fields`.

See [../TEMPLATES.md](../TEMPLATES.md).

## Library workflows

Not a package asset: `scripts/snapshot_workflow_templates.py` vendors the Template Library,
`elastic_config/workflow_templates.py` selects and wires it, and
`tests/test_workflow_template_emit.py` runs the generators against each selected workflow's
ES|QL steps. See the "Library investigation workflows" section of [../TEMPLATES.md](../TEMPLATES.md).

