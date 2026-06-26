# GUARDRAILS — upgrade-scenario skill

These rules are non-negotiable for the duration of the session. Every action
must be checked against this list before execution.

---

## Scope limits

1. **Mode A (Check) is strictly read-only.** No files may be written, edited, or
   deleted — not even `schema.yaml`.

2. **Mode B (Upgrade) may only modify files under `scenarios/<id>/`** for the
   target scenario being upgraded. No other directory may be touched — not
   `scenario_engine/`, not `scripts/`, not `app/`. If a migration's `guidance`
   appears to require editing files outside this tree, stop and ask the user.

3. **Mode C (Maintainer) may edit `scenario_engine/schema.yaml` and nothing
   else** beyond offering to run Mode B for bundled scenarios. It must not touch
   any scenario file directly (Mode B handles that).

4. **Never edit `scenario_engine/schema.yaml` in Mode B.** Only Mode C may
   change the registry.

---

## Version integrity

5. **Never bump `schema_version` in a scenario file without first applying all
   pending migrations.** The version number is a promise that the file content
   matches the schema; setting the number without doing the work is a lie.

6. **Apply migrations in strict order (oldest → newest)** as returned by
   `pending_migrations()`. Never skip an intermediate version.

7. **Always run `scripts/verify_yaml_scenarios.py <id>` after upgrading** and
   iterate until it exits 0. Do not report the upgrade as complete if the
   verifier fails.

---

## Content preservation

8. **Preserve all domain content.** Migrations add, rename, or remove structural
   keys — they never regenerate or replace existing domain values (error
   messages, telemetry steps, KPI ranges, channel descriptions, etc.).

9. **Never use `scaffold_scenario.py` during an upgrade.** That tool generates
   skeleton content; upgrades work with existing content.

---

## Mode C — registration safety

10. **Always show the draft migration entry and get explicit user confirmation**
    before writing to `schema.yaml`.

11. **Do not invent breaking changes.** If you are unsure whether a change is
    breaking (MAJOR) or additive (MINOR), ask the user. Err on the side of
    MINOR (compatible) if the difference is ambiguous.

12. **The `guidance` field must be self-contained.** A developer using Mode B
    months from now — with no other context — must be able to apply the
    migration by following the guidance alone.

---

## General

13. After completing any mode, run the scope check:

    ```bash
    git diff --name-only
    ```

    If files outside the expected scope appear in the diff, stop and alert the
    user before proceeding.
