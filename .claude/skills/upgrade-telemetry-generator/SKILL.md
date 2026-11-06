---
name: upgrade-telemetry-generator
description: >
  Validate and upgrade telemetry generators under generators/<id>/ in
  elastic-launch-demo. Use when the user asks to: check generator version;
  validate generators; list generators that need upgrading; upgrade or refresh
  a generator for a new elastic/integrations package release; compare
  SOURCE_INTEGRATION to upstream; fix empty stock package dashboards /
  Unknown column / invented metrics on an existing generator; wire impact_keys
  for every package alert/SLO; fix OOTB alert verification_exception, SLOs
  with no data / unhealthy baselines and ML jobs with no documents by
  correcting generator emit; apply pending generator_engine schema
  migrations. For brand-new generators use add-telemetry-generator instead.
version: 3.5.0
---

# upgrade-telemetry-generator

Two capabilities for generators under `generators/<id>/`:

| Capability | Purpose | Edits |
|---|---|---|
| **validate** | Read-only status: engine `schema_version` vs `generator_engine/schema.yaml` `current`; `SOURCE_INTEGRATION` vs upstream package version; alert ES\|QL emit gaps; SLO template / ML module emit gaps; optional live Kibana rule errors | None |
| **upgrade** | Bring generator(s) current: re-ground against latest `elastic/integrations` package (primary path); if `schema_version` lags engine `current`, also apply pending `schema.yaml` migration guidance in the same pass | `generators/<id>/` only |

Read [GUARDRAILS.md](GUARDRAILS.md), [CONTRACT.md](CONTRACT.md),
[ALERTS.md](ALERTS.md) and [TEMPLATES.md](TEMPLATES.md) now.

---

## Phase 0: Choose validate or upgrade

- Checking / listing status / “what needs upgrading” → **validate**
- Refresh for package release / citation drift / empty dashboards /
  `Unknown column` / invented metrics / alert `verification_exception` → **upgrade**
- Generator behind engine schema → **upgrade** (applies migration guidance)
- Ambiguous → ask validate vs upgrade (and which generator ids)

Synthetic generators with `integrations=()` (`traces`, `raw_access`,
`host_metrics`, `jvm`, `nginx_metrics`) have no upstream package — **validate**
still reports engine schema; **upgrade** package re-ground does not apply.

---

## validate

Report engine schema status:

```bash
cat generator_engine/schema.yaml
python3 -c "
from generator_engine.registry import list_generators
print(f'{\\"ID\\":<28} {\\"VERSION\\":<10} {\\"CURRENT\\":<10} STATUS')
print('-' * 70)
for g in list_generators():
    flag = '  upgrade required' if g['version_status'] == 'upgrade_required' else '  update available' if g['version_status'] == 'outdated_compatible' else ''
    print(f\\"{g['id']:<28} {g['effective_schema_version']:<10} {g['current_schema_version']:<10} {g['version_status']}{flag}\\")
"
```

Also report blocked scenarios:

```bash
python3 -c "
from scenario_engine import list_scenarios
for s in list_scenarios():
    if s.get('blocking_generators'):
        ids = ', '.join(b['id'] for b in s['blocking_generators'])
        print(f\\"{s['id']}: blocked by {ids}\\")
"
```

Also scan `SOURCE_INTEGRATION` citations. The citation is the install pin: step 20
installs exactly that version, so a newer upstream release is not deployed until the
generator is upgraded. Compare each cited version to the newest release
(`python3 scripts/audit_package_coverage.py <id>` reports it as a stale citation; add
`--latest` to list what the newer version would need). List generators with package
drift or missing citations and recommend **upgrade**.

Also report **impact_keys** gaps when inventories / SPECs are available:

- **Minimum 2:** recommend **upgrade** when `len(SPEC.impact_keys) < 2`
  (every generator must expose at least two scenario-usable knobs).
- **1:1 alerts:** when `alert_thresholds.json` has an **alerts** list / rule
  templates, recommend **upgrade** when alert keys are not exactly covered
  (`len(alerts) > 0` and the alert-slug set is not a subset of
  `impact_keys`, or alert-bearing packages are missing 1:1 keys). If there
  is exactly one alert, total keys must still be ≥ 2 (alert key + at least
  one extra emit knob).
- Recommend **upgrade** when keys are unused in `generator.py`, when
  `wants_chaos` is false despite impact_keys / alerts / SLOs, or when keys
  are declare-without-apply.

### Alert ES|QL emit gaps (mandatory when templates exist)

For each package-backed generator with `alerting_rule_template`:

```bash
python3 .claude/skills/add-telemetry-generator/scripts/extract_alert_esql_fields.py \
  --package-dir "$PKG" --out "$OUT/alert_required_fields.json"
python3 .claude/skills/add-telemetry-generator/scripts/verify_alert_emit.py \
  --inventory "$OUT/alert_required_fields.json" \
  --generator-dir "generators/<id>"
```

Recommend **upgrade** when verify exits non-zero, when `from_streams` is
outside `SPEC.datasets` without a companion emit, or when
`counter_required` fields are still emitted as gauges. See [ALERTS.md](ALERTS.md).

### SLO template / ML module emit gaps (mandatory when templates exist)

For each generator whose packages ship `slo_template` or `ml_module` assets:

```bash
python3 .claude/skills/add-telemetry-generator/scripts/extract_package_templates.py \
  --package-dir "$PKG" --out-dir "$OUT"
python3 .claude/skills/add-telemetry-generator/scripts/verify_template_emit.py \
  --inventory-dir "$OUT" --generator-dir "generators/<id>"
python3 -m pytest -q tests/test_slo_template_emit.py tests/test_ml_module_emit.py -k <id>
```

Recommend **upgrade** when verify exits non-zero, a runtime test fails, or the
generator still has entries in `tests/fixtures/template_emit_pending.json`. See
[TEMPLATES.md](TEMPLATES.md).

### Optional live Kibana check

When Kibana credentials are available, find rules with
`executionStatus.status:error` whose message contains `verification_exception`.
Recommend **upgrade** on the owning generator — **do not** edit OOTB rules.

---

## upgrade

Use when `SOURCE_INTEGRATION` is behind the current package version, missing,
**or** when stock package dashboards / OOTB alerts fail with
`verification_exception: Unknown column` / wrong metric types / invented
metric aliases (learned from `vsphere_otel` and OOTB alert backfill 2026-08).
Change generator telemetry only. Do not edit out-of-the-box package content.

### Engine schema (when behind)

If `list_generators()` shows `outdated_compatible` or `upgrade_required`:

1. Read pending migrations from `generator_engine.schema_version.pending_migrations`.
2. Apply each migration's `guidance` under `generators/<id>/` only.
3. Set `schema_version="<to>"` on the package SPEC in `__init__.py`.
4. Never edit `generator_engine/` during upgrade (except reading `schema.yaml`).

### Package re-ground (primary path)


1. Read `SOURCE_INTEGRATION` (after `__future__`) and the package id/version.
2. Fetch or reuse `.cache/integrations/packages/<id>/` (sparse clone or API).
3. Run add-telemetry-generator extract/merge scripts into `$OUT` (or the
   cached package dir): fields, dashboards, constraints, alerts, SLOs, merge.
4. **Always** run alert ES|QL extract + verify:
   `extract_alert_esql_fields.py` → `verify_alert_emit.py` (must exit 0), and the
   SLO / ML gate: `extract_package_templates.py` → `verify_template_emit.py` (must
   exit 0), then `scripts/snapshot_package_templates.py <id>` and the runtime tests
   `tests/test_slo_template_emit.py` / `tests/test_ml_module_emit.py -k <id>`.
   Remove the generator's fixed entries from `tests/fixtures/template_emit_pending.json`.
5. Diff dashboard inventories vs current emit (`verify_metric_coverage.py
   --check-emit` when that script is present in the checkout).
6. Edit `generators/<id>/` only to close emit gaps (metrics, attrs, datasets,
   resource shape, counter vs gauge). Do not invent aliases; do not edit OOTB
   package content.
7. Update `SOURCE_INTEGRATION` to the current package version — this is what makes
   demos install the new version — then refresh the fixtures from it:
   `scripts/snapshot_alert_identity.py <id>` and `scripts/snapshot_package_templates.py <id>`.
   `tests/test_package_pins.py` fails until they match.
8. If CloudWatch / classic mapping constraints apply, run the matching verify
   scripts before calling upgrade done.
9. Metric points must come from the `db_common` builders or `scrape_now_ns()`
   (`generator_engine/scrape_clock.py`), sums need a stable start, and a
   generator with its own loop wraps each iteration in `scrape_clock()`
   ([add GUARDRAILS](../add-telemetry-generator/GUARDRAILS.md) #34). Run
   `scripts/audit_colocation.py <id>` and add an ES bulk companion only for pairs
   it reports UNCOVERED.
10. Scenario identity: every resource the upgrade adds (bucket, VNet, replica,
    endpoint, ...) takes its name and ids from `ScenarioIdentity`, and any
    hardcoded identity literal already in the generator is fixed in the same
    pass ([add GUARDRAILS](../add-telemetry-generator/GUARDRAILS.md) #36).
    `tests/test_generator_identity.py -k <id>` must pass.

If the generator has no citation yet, add one from the current package version,
then continue with drift checks on the next release.

### Alert and SLO impact_keys (mandatory)

Mirror add-telemetry-generator Phase 6 on every **upgrade**. Do not edit
scenarios — only expose knobs on the generator so channels can pick them
later via `generator_impact`.

Combined rule:

```
if package has N alert rules (N >= 1):
    impact_keys must include exactly those N alert keys (1:1)
    if N == 1: add at least one more non-alert emit knob so total >= 2
if package has 0 alerts (or integrations=()):
    impact_keys must have >= 2 scenario-usable knobs
```

1. Read `$OUT/alert_thresholds.json` (the **alerts** list / rule templates —
   one entry per alert rule, not per threshold expression),
   `$OUT/alert_required_fields.json`, and `$OUT/slo_required_fields.json` when present.
   Map each alert to emit metric(s) + healthy vs breach values.
2. **1:1 rule:** when `N >= 1`, `SPEC.impact_keys` must include **exactly one
   key per package alert** (and per SLO target). Do not fan unrelated alerts
   into one mega-fault key. Alert-slug keys should match the inventory.
3. **Minimum 2:** `len(SPEC.impact_keys) >= 2` always. If `N == 0`, invent ≥2
   emit-backed knobs (error/latency/volume/cost/…). If `N == 1`, keep the
   alert key and add ≥1 extra knob.
4. Set `wants_chaos=True` whenever `impact_keys` is non-empty (always after
   this step) — required so scenarios can drive faults via `generator_impact`.
5. **Apply** every impact key in `run()` via `active_chaos["<key>"]` (or
   equivalent). Declaring keys in SPEC without reading them is a hard miss
   (learned from `cassandra_otel` / `aws_rds_otel` / `jvm.heap_pressure`).
6. **Resolvable / baseline:** when `resolve_active_chaos` returns `None`,
   emit must stay under alert thresholds / healthy defaults, and every package
   SLO must meet its objective ([TEMPLATES.md](TEMPLATES.md)). See
   [ALERTS.md](../add-telemetry-generator/ALERTS.md) **Healthy baseline
   must stay under thresholds** (INCREASE seed=0, clamp growing gauges,
   SUM-absolute / ratio traps, stable start_ns).
7. Unit tests: for each impact key, assert chaos-on moves the signal (crosses
   the alert threshold when applicable) and chaos-off is healthy.
8. Do not synthesize `kibana.alert.*` or fake SLO documents.

---

## Maintainer note (not a user-facing capability)

To change the shared plugin contract, edit `generator_engine/schema.yaml`
directly: append a migration, bump `current`, set `breaking` for MAJOR.
Then run **upgrade** on affected generators so they apply the new `guidance`.
Do not present this as a third mode in chat.

---

## Verify

```bash
python3 -m generator_engine.registry --list
python3 -c "from generator_engine.registry import reload_registry; print(reload_registry()['count'])"
python3 .claude/skills/add-telemetry-generator/scripts/verify_alert_emit.py \
  --inventory "$OUT/alert_required_fields.json" \
  --generator-dir "generators/<id>"
python3 -m pytest -q tests/test_scrape_timestamp.py -k <id>
python3 -m pytest -q tests/test_generator_identity.py tests/test_scenarios.py -k <id>
# Alert ids stay fixed while each fault is active (refresh the rule snapshot first):
python3 scripts/snapshot_alert_identity.py <id>   # network (EPR)
python3 -m pytest -q tests/test_alert_identity_stable.py -k <id>
# Package SLOs / ML jobs get data and SLOs are healthy (refresh the template snapshot first):
python3 scripts/snapshot_package_templates.py <id>   # network (EPR)
python3 -m pytest -q tests/test_slo_template_emit.py tests/test_ml_module_emit.py tests/test_package_template_coverage.py -k <id>
# Library investigation workflows that read this generator's data find their evidence:
python3 -m pytest -q tests/test_workflow_template_eligibility.py tests/test_workflow_template_emit.py -k <id>
python3 scripts/audit_package_coverage.py <id>   # network (EPR)
python3 scripts/audit_colocation.py <id>         # network (EPR)
```

After **upgrade**, remind the user to click **Refresh list** in the UI.
