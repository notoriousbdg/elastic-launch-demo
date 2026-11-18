# CONTRACT — upgrade-scenario skill

This document is the canonical reference for schema versioning in
elastic-launch-demo. It defines version semantics, status values, the
`schema.yaml` data model, and the rules for writing migration guidance.

---

## Version format

All schema versions use **`MAJOR.MINOR`** semver (two-part, string-typed):

```yaml
schema_version: "1.1"
```

- **MAJOR** — incremented for **breaking** changes: the scenario contract has
  changed in a way that older scenarios will not work correctly with the current
  engine without migration.
- **MINOR** — incremented for **additive/compatible** changes: older scenarios
  still run fine; the new version just introduces an optional or enriching
  feature.

A `schema_version` key absent from `scenario.yaml` is treated as
`default_unversioned` (currently `"1.0"`).

---

## Status values

| Status | Meaning | Launch allowed? |
|---|---|---|
| `up_to_date` | Scenario is at `current` | ✓ |
| `outdated_compatible` | Same MAJOR, older MINOR | ✓ (with "update available" badge) |
| `upgrade_required` | Older MAJOR (breaking gap) | ✗ (button disabled) |
| `ahead` | Scenario MAJOR > engine | ✗ (engine too old; warn) |

Computed by `scenario_engine/schema_version.py :: version_status(scenario_version)`.

---

## `scenario_engine/schema.yaml` format

```yaml
current: "1.3"                 # engine's expected schema version
default_unversioned: "1.0"    # version assumed when schema_version key is absent

migrations:                   # ordered oldest → newest; DO NOT reorder
  - to: "1.1"                 # version this migration produces
    breaking: false           # false = MINOR bump (compatible)
    summary: "..."            # one-line shown in the status table
    guidance: |               # instructions for Mode B to apply; see below
      ...
    needs_input: false        # true = Mode B must AskUserQuestion before applying
```

### Rules for `migrations`

- Entries are **append-only** (never edit or reorder past entries).
- Each `to` version must be strictly greater than the previous entry's `to`.
- `breaking: true` → bump MAJOR in `to`. `breaking: false` → bump MINOR only.
- `current` must equal the `to` of the last entry in `migrations`.

### Registered versions

| Version | Breaking | Summary | `needs_input` |
|---|---|---|---|
| 1.1 | no | `schema_version` key in `scenario.yaml` | no |
| 1.2 | no | `business_impact` on every channel | no |
| 1.3 | no | Realism and verifiable remediation: `remediation_command` (opsctl), `fault_scope`, cause-before-effect `infrastructure_events` (`offset_seconds`), `cascade_messages`, `remediation_effects` (verification, state, log_events, steady_attributes), before/during/after signals in the service DSL (`all_active`, `elapsed`, `fixed_channels`, `since_fix`, `applied`), no demo wording, coherence; scenario-level `ops.yaml` (opsctl profile); optional `k8s_topology` | **yes** — per channel |

The `guidance` text in `schema.yaml` is authoritative; this table is a summary. 1.3 is per-channel work: Mode B must ask the user for the remediation commands and realistic after-state values (see SKILL.md Mode B Step 2). Checks 37-44 in `scripts/verify_yaml_scenarios.py` run only for scenarios at >= 1.3. Check 45 validates `scenarios/<id>/ops.yaml` whenever the file exists; Check 37 accepts only verbs that profile defines, so write `ops.yaml` before the commands. `ops.yaml` is a scenario file and inside Mode B's `scenarios/<id>/` scope.

---

## Writing the `guidance` field (Mode C)

The guidance must be **self-contained, file-level instructions** an agent can
execute with only the scenario folder in view. Write as if explaining to a
developer who knows nothing about what changed.

**Good guidance:**

```
Add a top-level `schema_version: "1.1"` key to scenario.yaml, immediately
after the `sort_order` line (or after `scenario_icon` if sort_order is
absent). No other edits are required.
```

```
In scenario.yaml, under each entry in `k8s_clusters`:
  - Rename the key `cloud_provider` → `provider`
  - Rename the key `cloud_region` → `region`
Leave all other keys (`platform`, `zones`, `services`, `os_description`) unchanged.
No changes required in channels/ or services/.
```

**Guidance must specify:**
- Which file(s) to edit (scenario.yaml / channels/*.yaml / services/*.yaml)
- The exact key names to add, rename, or remove
- Default values for new required fields (or `needs_input: true` if none exists)
- Which files are NOT affected (helps avoid accidental over-editing)

---

## Version bump rules (for Mode C)

| What changed | MAJOR? | Example new version |
|---|---|---|
| Removed a top-level required key from scenario.yaml | yes | 1.1 → 2.0 |
| Renamed a top-level key (old name no longer accepted) | yes | 1.1 → 2.0 |
| Changed a DSL operator name in channels/services | yes | 1.1 → 2.0 |
| Added a new **required** field with no default | yes | 1.1 → 2.0 |
| Added a new **optional** field (engine defaults gracefully) | no | 1.1 → 1.2 |
| Added a new required field that has a safe default | no | 1.1 → 1.2 |
| Renamed a key but old name still accepted (alias) | no | 1.1 → 1.2 |

When in doubt, ask the user. Prefer MINOR (compatible) over MAJOR (breaking).

---

## Key paths in the source tree

```
scenario_engine/
  schema.yaml            ← version registry (source of truth)
  schema_version.py      ← Python helpers: parse_version, version_status, pending_migrations
  yaml_scenario.py       ← YamlScenario.schema_version property
  __init__.py            ← list_scenarios(), import_scenario_zip() surface version fields

app/
  main.py                ← /api/setup/launch guard (blocks upgrade_required)
  selector/static/index.html  ← UI: disabled Launch button, version pills
  ops_profile.py         ← ops profile contract (module docstring); merges presets + ops.yaml
  ops_presets/*.yaml     ← scenario-neutral presets: neutral, kubernetes, aws-cloudtrail, gcp-audit, azure-activity

scripts/
  scaffold_scenario.py   ← emits schema_version: "<current>" in new scenarios
  verify_yaml_scenarios.py    ← check 1b: validates schema_version; checks 37-44 for >= 1.3; check 45 (ops.yaml)

scenarios/<id>/
  scenario.yaml          ← has `schema_version: "1.3"` (or older; absent → 1.0)
  ops.yaml               ← opsctl profile (1.3): extends, actors, auto_remediate_channels, audit, verbs, guide

.claude/skills/upgrade-scenario/
  SKILL.md               ← skill instructions (3 modes)
  GUARDRAILS.md          ← non-negotiable rules
  CONTRACT.md            ← this file
```
