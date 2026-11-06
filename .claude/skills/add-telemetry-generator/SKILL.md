---
name: add-telemetry-generator
description: >
  Create a new telemetry generator under generators/<id>/ in elastic-launch-demo,
  grounded on an elastic/integrations package (or a synthetic demo signal). Use
  when the user asks to add a generator for a package, wire OTel/ES bulk emit,
  or scaffold impact_keys / chaos for scenario faults. For upgrading an existing
  generator use upgrade-telemetry-generator instead.
version: 2.3.0
---

# add-telemetry-generator

Scaffold and ground a new generator under `generators/<id>/`.

Read [GUARDRAILS.md](GUARDRAILS.md), [CONTRACT.md](CONTRACT.md),
[ALERTS.md](ALERTS.md) and [TEMPLATES.md](TEMPLATES.md) now.

Reuse scripts under [scripts/](scripts/) for alert ES|QL and SLO / ML template
extract/verify (and any package fetch/extract/merge helpers present in this checkout).

---

## Phase overview

1. **Identify** package id (or synthetic) and generator id
2. **Fetch / extract / merge** inventories into `$OUT` (package-backed)
3. **Package template gate (mandatory when templates exist)**: alerts, SLOs and ML jobs — see below
4. **Emit** metrics/logs/traces to match dashboards + alerts + SLOs + ML jobs + constraints
5. **SPEC** in `__init__.py` (`schema_version`, datasets, integrations, `identity_fields`, …). Every host, service, instance, database, URL, bucket, endpoint and cloud-account, subscription, tenant or project value comes from `ScenarioIdentity` (`generator_engine/identity.py`), so no two scenarios share it. Do not hardcode these or format `{namespace}-` prefixes in the generator. See guardrail 36 in [GUARDRAILS.md](GUARDRAILS.md) for the helper table; `tests/test_generator_identity.py` enforces it.
6. **Impact keys (mandatory)** — see below
7. **Tests** + registry reload; remind **Refresh list**

Synthetics (`integrations=()`) skip package extract but still need impact_keys.

---

## Phase 3: Package template gate — alerts, SLOs, ML (mandatory)

When the package ships `kibana/alerting_rule_template/`, alerts are part of the
**emit set** — not only `impact_keys`. Do not edit OOTB templates.

```bash
python3 .claude/skills/add-telemetry-generator/scripts/extract_alert_esql_fields.py \
  --package-dir "$PKG" --out "$OUT/alert_required_fields.json"

python3 .claude/skills/add-telemetry-generator/scripts/verify_alert_emit.py \
  --inventory "$OUT/alert_required_fields.json" \
  --generator-dir "generators/<id>"
```

Before calling the generator done:

- Emit every `fields[]` / `coalesce_operands[]` column (COALESCE arms included).
- Map `counter_required[]` as `counter_long`; reset sticky gauges on startup.
- Align `from_streams[]` with `SPEC.datasets` or emit a companion dataset /
  flag package template mismatch ([ALERTS.md](ALERTS.md)).
- CloudWatch: always emit `Namespace` + `MetricName` via `build_cloudwatch_metric`.
- Build metric points with the `db_common` builders (or `scrape_now_ns()` from
  `generator_engine/scrape_clock.py`), give every sum a stable start, and run the
  generator through `run_tick_loop` (or wrap your own loop in `scrape_clock()`), so
  one scrape lands in as few documents as a real collector's
  ([GUARDRAILS.md](GUARDRAILS.md) #34).
- Queries that read two metrics in one row: run `scripts/audit_colocation.py <id>`
  and add an ES bulk companion only for pairs it reports UNCOVERED.

Details: [ALERTS.md](ALERTS.md).

**SLO templates and ML modules** are deployed too (step 21). When the package ships
`kibana/slo_template/` or `kibana/ml_module/`:

```bash
python3 .claude/skills/add-telemetry-generator/scripts/extract_package_templates.py \
  --package-dir "$PKG" --out-dir "$OUT"
python3 .claude/skills/add-telemetry-generator/scripts/verify_template_emit.py \
  --inventory-dir "$OUT" --generator-dir "generators/<id>"
```

Then snapshot and run the runtime tests (see Verify). Every SLO must get data, carry
its `groupBy` fields and stay healthy on the chaos-off baseline; every ML detector
needs documents with its field and split fields. Details: [TEMPLATES.md](TEMPLATES.md).

---

## Phase 6: impact_keys (mandatory)

Every new generator must ship scenario-usable faults via `SPEC.impact_keys`.

Combined rule:

```
if package has N alert rules (N >= 1):
    impact_keys must include exactly those N alert keys (1:1)
    if N == 1: add at least one more non-alert emit knob so total >= 2
if package has 0 alerts (or integrations=()):
    impact_keys must have >= 2 scenario-usable knobs
```

Steps:

1. Read `$OUT/alert_thresholds.json` (**alerts** list / rule templates — one
   entry per alert rule, not per threshold expression) and
   `$OUT/slo_required_fields.json` when present. Also read
   `$OUT/alert_required_fields.json` from Phase 3.
2. Map each alert to emit metric(s) + healthy vs breach values. Name keys as
   stable snake_case slugs from the alert title.
3. Ensure **`len(SPEC.impact_keys) >= 2`**. With zero alerts, invent ≥2 knobs
   that actually change emit (error rate, latency, volume, cost, …).
4. Set `wants_chaos=True`.
5. **Apply** every key in `run()` via `resolve_active_chaos` +
   `active_chaos["<key>"]` (declare-without-apply is a hard miss).
6. When chaos is `None`, emit stays at healthy baseline (under alert thresholds,
   and every package SLO meets its objective — [TEMPLATES.md](TEMPLATES.md)).
   See [ALERTS.md](ALERTS.md) **Healthy baseline must stay under thresholds**
   (zero seeds for INCREASE metrics, clamp growing gauges, SUM-absolute
   traps, stable `start_time_ns`).
7. Unit tests: chaos-on moves each key’s signal; chaos-off is healthy.
8. Do not edit `scenarios/`. Do not synthesize `kibana.alert.*` docs.

Citation (package-backed), after `from __future__ import annotations`:

```python
SOURCE_INTEGRATION = "elastic/integrations packages/<id>@<version>"
```

The citation is the **install pin**: step 20 installs exactly this package version (`generator_engine.registry.package_versions`), so the deployed dashboards, alerts, SLOs and ML jobs are the ones the generator was built against. Cite every package in `SPEC.integrations`; `tests/test_package_pins.py` fails on a missing or conflicting citation, or on fixtures snapshotted from another version.

---

## Verify

```bash
python3 -m generator_engine.registry --list
python3 -c "from generator_engine.registry import reload_registry; print(reload_registry()['count'])"
# When templates exist:
python3 .claude/skills/add-telemetry-generator/scripts/verify_alert_emit.py \
  --inventory "$OUT/alert_required_fields.json" \
  --generator-dir "generators/<id>"
# One timestamp per scrape, and no row-level query left without a document:
python3 -m pytest -q tests/test_scrape_timestamp.py -k <id>
# Scenario-unique identity (two scenarios must not share a name or id), plus scenario files:
python3 -m pytest -q tests/test_generator_identity.py tests/test_scenarios.py -k <id>
# Package SLOs and ML jobs get data and the SLOs are healthy (refresh the snapshot first):
python3 scripts/snapshot_package_templates.py <id>   # network (EPR)
python3 -m pytest -q tests/test_slo_template_emit.py tests/test_ml_module_emit.py tests/test_package_template_coverage.py -k <id>
# Library investigation workflows that read this generator's data find their evidence:
python3 -m pytest -q tests/test_workflow_template_eligibility.py tests/test_workflow_template_emit.py -k <id>
# Alert ids stay fixed while each fault is active (refresh the rule snapshot first):
python3 scripts/snapshot_alert_identity.py <id>   # network (EPR)
python3 -m pytest -q tests/test_alert_identity_stable.py -k <id>
python3 scripts/audit_colocation.py <id>      # network (EPR)
```

Remind the user to click **Refresh list** in the UI.
