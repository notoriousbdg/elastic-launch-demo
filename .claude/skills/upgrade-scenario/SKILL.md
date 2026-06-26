---
name: upgrade-scenario
description: >
  Check and upgrade scenario schema versions in elastic-launch-demo.
  Use when the user asks to:
  - "check scenario version" / "is this scenario out of date"
  - "upgrade scenario" / "migrate scenario" / "bring scenario up to date"
  - "bump schema version" / "register a breaking change" / "register a schema change"
  - "what scenarios need upgrading" / "show me the version status"
  - "add schema version to a scenario"
  - run Mode A / Mode B / Mode C of the upgrade skill
---

# upgrade-scenario

Manages schema versioning for YAML scenarios. Operates in three modes:

- **Mode A — Check**: read-only status report across all or specific scenarios.
- **Mode B — Upgrade (consumer)**: apply pending migrations to bring a scenario up to the current schema version.
- **Mode C — Maintainer**: register a new schema version (breaking or additive) after an engine contract change, then optionally upgrade all bundled scenarios.

Read [GUARDRAILS.md](.claude/skills/upgrade-scenario/GUARDRAILS.md) now and hold every rule for this session.
Read [CONTRACT.md](.claude/skills/upgrade-scenario/CONTRACT.md) now — it is the canonical reference for version semantics, migration-entry shape, and status definitions.

---

## Phase 0: Determine the mode

Parse the user's request:

- **Mentions checking, listing, or showing version status** → Mode A.
- **Mentions upgrading, migrating, or bringing a scenario up to date** → Mode B. Ask which scenario (or "all") if not specified.
- **Mentions a change they just made to the engine contract (new required field, renamed key, DSL change, etc.)** → Mode C.
- **Ambiguous** → ask one `AskUserQuestion` with options A / B / C.

Identify the target scenario id(s) from the user's message (or prompt if needed).

---

## Mode A — Check (read-only)

### Step 1: Read the schema registry

```bash
cat scenario_engine/schema.yaml
```

Note `current` and `default_unversioned`.

### Step 2: Check scenario version status

Run for all scenarios (or the specified subset):

```bash
python3 -c "
from scenario_engine import list_scenarios
import json
rows = [(s['id'], s.get('schema_version') or '<unversioned>', s['current_schema_version'], s['version_status'])
        for s in list_scenarios()]
print(f'{'ID':<22} {'VERSION':<12} {'CURRENT':<10} STATUS')
print('-' * 60)
for id, sv, cv, st in rows:
    flag = '  ⚠ upgrade required' if st == 'upgrade_required' else '  ↑ update available' if st == 'outdated_compatible' else ''
    print(f'{id:<22} {sv:<12} {cv:<10} {st}{flag}')
"
```

### Step 3: List pending migrations for any out-of-date scenarios

For each scenario that is not `up_to_date`:

```bash
python3 -c "
from scenario_engine.schema_version import pending_migrations
import yaml
migrations = pending_migrations('<version>')
for m in migrations:
    print(f'  → {m[\"to\"]} ({\"BREAKING\" if m[\"breaking\"] else \"compatible\"}): {m[\"summary\"]}')
"
```

### Step 4: Report

Print the status table, flag any `upgrade_required` scenarios (launch is blocked), and list their pending migrations. Suggest running Mode B if any scenarios need updating.

---

## Mode B — Upgrade (consumer)

Apply pending migrations to bring a scenario up to the current schema version.
**Never edits files outside `scenarios/<id>/`.**

### Step 1: Identify pending migrations

```bash
python3 -c "
from scenario_engine.schema_version import pending_migrations, effective_version
import yaml

sv = None  # will be replaced with actual schema_version from scenario.yaml
with open('scenarios/<id>/scenario.yaml') as f:
    data = yaml.safe_load(f)
sv = data.get('schema_version')
eff = effective_version(sv)
print(f'Scenario version: {sv!r}  (effective: {eff})')

from scenario_engine.schema_version import pending_migrations
migs = pending_migrations(sv)
print(f'{len(migs)} pending migration(s):')
for m in migs:
    print(f'  → {m[\"to\"]} ({\"BREAKING\" if m[\"breaking\"] else \"compatible\"}): {m[\"summary\"]}')
"
```

If there are no pending migrations, report that the scenario is already up to date and stop.

### Step 2: Apply each migration in order

For each pending migration (oldest → newest):

1. Print the migration summary and its `guidance` text.
2. Read the `guidance` field from `scenario_engine/schema.yaml` for this migration entry.
3. Apply the described edits to the scenario files under `scenarios/<id>/`.
4. If the entry has `needs_input: true`, use `AskUserQuestion` to gather the required value before editing.

**The `1.0 → 1.1` migration** (adding the `schema_version` field) is handled like any other: read its guidance from `schema.yaml` and apply it. The guidance says to add `schema_version: "1.1"` immediately after the `sort_order` line in `scenario.yaml`.

After each migration is applied, confirm the target version is correct.

### Step 3: Verify

After all migrations are applied, run the verifier:

```bash
python3 scripts/verify_yaml_scenarios.py <id>
```

Also run the auto-discovery check:

```bash
python3 -c "
from scenario_engine import list_scenarios
hit = next((s for s in list_scenarios() if s['id'] == '<id>'), None)
print('schema_version:', hit['schema_version'], '  status:', hit['version_status'])
"
```

If the verifier exits non-zero, diagnose and fix, then re-run. Iterate until it exits 0.

### Step 4: Scope check

```bash
git diff --name-only
```

Output must show **only files under `scenarios/<id>/`**. If any other file appears, stop and alert the user — do not commit.

### Step 5: Report

State what was migrated, which files were changed, and the new schema version. Remind the user to commit the changes.

---

## Mode C — Maintainer (register a new schema version)

Use when you have changed the engine contract (renamed/removed keys, new required fields, DSL changes) and need to register the new version so the upgrade skill can later apply it to older scenarios.

**Only this mode may edit `scenario_engine/schema.yaml`.**

### Step 1: Characterise the change

Ask (via `AskUserQuestion`) if not obvious from context:
- Is this a **breaking** change (older scenarios will fail to load or misbehave without migration) or an **additive/compatible** change (older scenarios still work fine, just missing the new feature)?
- What is the new version string? Follow MAJOR.MINOR: bump MAJOR for breaking, MINOR for additive.

### Step 2: Review git diff for guidance

```bash
git diff -- scenario_engine/ scripts/ app/
```

Use the diff to understand exactly what changed. This will inform the `guidance` field of the new migration entry.

### Step 3: Draft the migration entry

Compose the new migration entry (see [CONTRACT.md](CONTRACT.md) for the full shape). The `guidance` field must be self-contained step-by-step instructions that Mode B can follow to edit a scenario folder — written as if instructing an agent who has only the scenario files in front of them.

Show the draft to the user via a code block and ask for confirmation before writing.

### Step 4: Append to schema.yaml and bump current

Read `scenario_engine/schema.yaml`, append the new migration entry to `migrations:`, and update `current:` to the new version. Write the file.

```bash
# Verify the edit parsed correctly
python3 -c "
import yaml
with open('scenario_engine/schema.yaml') as f:
    d = yaml.safe_load(f)
print('current:', d['current'])
print('migrations:', len(d['migrations']), 'entries')
for m in d['migrations']:
    print(f'  {m[\"to\"]} breaking={m[\"breaking\"]} — {m[\"summary\"]}')
"
```

### Step 5: Offer to upgrade all bundled scenarios

After registering the new version, offer to run Mode B across all bundled scenarios:

> The registry is updated. Would you like me to upgrade all bundled scenarios in `scenarios/*/` to `<new-version>` now?

If the user confirms, run Mode B for each scenario in turn. If the migration has `breaking: true`, make sure the user understands that any external (exported) scenarios will need upgrading before they can be re-imported and launched.
