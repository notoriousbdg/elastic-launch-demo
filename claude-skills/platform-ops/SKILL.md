---
name: platform-ops
description: Apply changes to the running environment with opsctl. Use when carrying out a remediation recommendation or runbook step, such as rolling back a configuration or firmware revision, restoring a policy version, patching settings on a component, switching to a redundant unit, draining or replacing a unit, expanding a capacity, rotating or syncing a credential, applying a rate limit, or running a one-off job or procedure. Covers dry run, apply and verification of recovery in telemetry.
---

# Changes with opsctl

All changes to the environment go through `opsctl`. Each applied change is written to
the audit log with a change id. Do not use other tools or ad-hoc API calls to change the
environment.

`opsctl` is on PATH. If it is not found, call it by its full path:
`/root/.claude/skills/platform-ops/bin/opsctl`.

The verbs, resource kinds and help come from the running scenario, so they differ
between environments:

- `opsctl --help` lists this environment's verbs; `opsctl <verb> --help` shows examples.
- `opsctl guide` says where this environment's telemetry is (indices, field names,
  services) and how to filter it. **Read it before you query.**

## Change procedure

1. **Read the recommendation and its runbook.** Take the remediation commands exactly as
   written: the same target, namespace and values. A runbook can list more than one
   command; apply all of them. If you were given only a command, or no runbook, look the
   runbook up in the Wiki first (see "Finding the runbook") and check whether it lists
   more commands than you were given. Note its **Scope** and the signals under "Recovery
   is confirmed when" with their transition times. Do not substitute a restart for the
   documented fix.
2. **Dry run each command** with `opsctl plan <verb> ...`. Check that the target and the
   changes are what the runbook describes.
3. **Apply each command** in the order given. Record the change id that each one prints.
   If a command fails, stop and report the error. Do not improvise a different change.
4. **Verify recovery in telemetry.** A command that succeeds tells you only that the change
   was applied, not that the problem is gone. Wait for the stated transition time, then
   run the queries under "Recovery is confirmed when". They already name the right index
   and fields; `opsctl guide` covers anything else you need. Stay inside the runbook's
   Scope: components outside it were not changed and can legitimately still show the old
   values. Compare the window after the change with the window before it, and find your
   change in the audit trail by its `change.id`. If the signals have not recovered after
   the transition time, report that. Do not apply more changes than the runbook lists.
5. **Report** what changed (each command and its change id), when it was applied, and
   the telemetry that confirms recovery, or the telemetry that shows it has not recovered.

## Querying telemetry

The data is OpenTelemetry, not ECS. Run ES|QL with curl (or the Elastic MCP tools):

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" -H "Content-Type: application/json" \
  "$ELASTICSEARCH_URL/_query?format=txt" -d '{"query": "<ES|QL>"}'
```

Common to every environment:

- **Logs:** `FROM logs.otel.*`. Message in `body.text` (never `body` alone), level in
  `severity_text`, emitter in `resource.attributes.service.name`. Root-cause and change
  details are in `attributes.*` (for example `attributes.change.id`).
- **Service metrics:** `FROM metrics-generic.otel-default`, keyed by
  `resource.attributes.service.name`, metric fields under `metrics.*`.
- Field names that contain `/` must be backtick-quoted in ES|QL.

`opsctl guide` adds the environment-specific indices and fields.

## Finding the runbook

Runbooks live in the Wiki (the Nightshift knowledge store). List the pages and pick the
runbook (`entity_type` `runbook`) for the failing component or error signature:

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" -H "kbn-xsrf: true" \
  -H "x-elastic-internal-origin: kibana" "$KIBANA_URL/internal/nightshift/wiki/pages" \
  | jq -r '.. | objects | select(.entity_type? == "runbook") | "\(.id)\t\(.title)"'
```

Fetch one page with `.../internal/nightshift/wiki/pages/<id>`. Its "Established facts"
section has the Scope, the remediation commands and "Recovery is confirmed when".
