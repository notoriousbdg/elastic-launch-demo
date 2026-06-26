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
current: "1.1"                 # engine's expected schema version
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

scripts/
  scaffold_scenario.py   ← emits schema_version: "<current>" in new scenarios
  verify_yaml_scenarios.py    ← check 1b: validates schema_version

scenarios/<id>/
  scenario.yaml          ← has `schema_version: "1.1"` (or absent → 1.0)

.claude/skills/upgrade-scenario/
  SKILL.md               ← skill instructions (3 modes)
  GUARDRAILS.md          ← non-negotiable rules
  CONTRACT.md            ← this file
```
