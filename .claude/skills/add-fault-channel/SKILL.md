---
name: add-fault-channel
description: Replace one fault channel in an existing scenario. Use when the user wants to add, swap, or improve a fault in scenarios/<id>/channels/. Replaces the channel YAML file, updates agent_config.system_prompt in scenario.yaml and, at schema 1.3, the signal service's before/during/after branches (and ops.yaml when a verb is missing). Never touches any file outside scenarios/<id>/.
---

# Replace a fault channel

This skill replaces one existing fault channel in a scenario with a new or improved one. It replaces `scenarios/<id>/channels/NN-<slug>.yaml` and updates `agent_config.system_prompt` in `scenario.yaml`. At schema 1.3 it also edits the service that emits the channel's signal (its during-fault and after-fix branches) and, only when no existing verb fits the fix, adds a verb to `scenarios/<id>/ops.yaml`.

A channel's `generator_impact` keys drive the generator's signals. The package alert rules, SLOs and ML anomaly detection jobs that read those signals are created by the deployer (step 21), so there is nothing to wire for them here; the description may name the SLO a fault burns.

Read [GUARDRAILS.md](GUARDRAILS.md) now and hold every rule in it for the duration of this session.

---

## Phase 1: Gather the brief

Parse any context the user already provided. Then read [BRIEF.md](BRIEF.md) and ask — via `AskUserQuestion` — only about signals that are missing. Ask at most 3 questions per call.

Required before Phase 2:
- **Scenario** — which scenario_id to modify
- **Channel number** — which channel (1–20) to replace
- **Fault description** — what the new channel should represent

---

## Phase 2: Read current state

Read the target channel file `scenarios/<id>/channels/NN-<slug>.yaml` in full. Extract:

1. The **current channel** fields: name, subsystem, error_type, error_message, fault_params, rca_clues
2. The **valid service keys** — list the filenames under `scenarios/<id>/services/` (each filename stem is a service key)
3. The **`executive_kpi_emissions` names** (for `business_impact`) from the same file, then the **agent_config.system_prompt** from `scenarios/<id>/scenario.yaml` — use a targeted read to avoid loading the full 15KB file:

```bash
grep -n "agent_config\|system_prompt" scenarios/<id>/scenario.yaml | head -5
```

Then `Read` only the `agent_config:` block (use `offset`/`limit` from the line numbers returned above). You need only the `system_prompt` string to locate the `error_type` list.

4. The scenario's **`schema_version`** and, if present, its **`k8s_topology`** names (clusters, node pools, workloads) — `fault_scope` and `state` must use them:

```bash
grep -n "schema_version\|k8s_topology" scenarios/<id>/scenario.yaml
```

At schema 1.3 (`k8splatform`, `space` and `gcp` today) the new channel must carry the 1.3 fields (Checks 37-44). Below 1.3 they are optional and unchecked; add them anyway when the user wants the channel to work with `opsctl` and show an after-state, and suggest the upgrade-scenario skill to bring the whole scenario to 1.3.

Also read [CONTRACT.md](CONTRACT.md) now as a generation checklist, and the Realism checklist in [../add-scenario/BRIEF.md](../add-scenario/BRIEF.md). Reference channels: `scenarios/k8splatform/channels/02-edge-conntrack-exhaustion.yaml` (rollback fix) and `13-ingress-oom-reload-storm.yaml` (fix that sets a new value); `scenarios/space/channels/02-*.yaml` with `scenarios/space/services/fuel-system.yaml` for a non-Kubernetes fix and its before/during/after signal.

5. At 1.3, the scenario's **ops profile**: the verbs `remediation_command` may use and the audit format the fix's `{actor}` line must satisfy. `opsctl` verbs come from the presets in `scenarios/<id>/ops.yaml` `extends` plus the file itself; list them with:

```bash
.venv/bin/python -c "
from scenario_engine.yaml_scenario import load_yaml_scenario; from pathlib import Path
p = load_yaml_scenario(Path('scenarios/<id>')).ops_profile
print(sorted(p['verbs'])); a = p['audit']; print(a.get('service'), a['formats'][a['default']])
"
```

6. The **services that read channel N** today (`grep -n "\bN in \(all_active\|fixed_channels\|active\)\|get(N" scenarios/<id>/services/*.yaml`, with N the channel number): those branches belong to the old channel and must be rewritten for the new one.

---

## Phase 3: Design — propose then confirm

Present the proposed new channel via `AskUserQuestion` (single question, long description field) and ask the user to confirm, adjust, or replace before writing any files.

The proposal must cover:

- **Channel number** being replaced, and current channel name being replaced
- **New channel name, subsystem, vehicle_section, error_type**
- **error_message** template (showing all `{placeholder}` names)
- **affected_services** and **cascade_services** (service keys, not display names)
- **remediation_action** and whether it's HITL (1–15) or auto-remediable (16–20)
- **Three phases** of the fault:
  - *baseline* — what the healthy signals look like before the trigger (and what stays healthy outside the scope during the fault);
  - *fault* — the cause event (with its negative `offset_seconds`), when symptoms start (`onset_delay_seconds`, `ramp_seconds`), the error line and the coupled signals that move with it;
  - *remediated* — the `remediation_command` (`opsctl ...`, fixing the root cause, using verbs from the ops profile), the lines the fix leaves (`log_events`, with one `{actor}` line per command line that satisfies the audit format's `match`; `steady_attributes`), and any config value that changes (`remediation_effects.state`).
  - *signal* — the metric that is in band before, out of band during and converges back after (with values and recovery time), the service that emits it, and the status attribute that shows the fix's configuration afterwards.
- **New verb** (only if no existing verb reads naturally): its name, `summary` and `example` for `ops.yaml`
- **fault_scope** — cluster, node pool, nodes, AZ or workloads, and the healthy comparison outside it
- **Verification signals** — 2–4 `{signal, where, expect, within_seconds}` entries that confirm recovery
- **Differential clue** — if another channel in the scenario looks similar, the clue that tells them apart
- **investigation_notes** summary (key steps; full text in Phase 4)
- **agent_config.system_prompt** change — old error_type → new error_type

If the channel number is 16–20, confirm the `remediation_action` passes the auto-remediate test from GUARDRAILS.md §3.

---

## Phase 4: Make the edits

After the user confirms, make the edits (Edits 1-2; at 1.3 also Edits 3-4 as needed). Do not ask permission for each.

### Edit 1 — Replace the channel YAML file

Write a new `scenarios/<id>/channels/NN-<new-slug>.yaml`. Keep the same channel number prefix (`NN`). Delete the old file if the slug changes.

Follow the field order from CONTRACT.md. See the full schema there.

Include `business_impact` so BOTH dashboards react in proportion to the fault's scope: dict entries targeting `executive_kpi_emissions` names, enough to turn tiles yellow/red (scope = 2×affected + cascade; minor ≥2 non-green incl. 1 red, moderate ≥3 incl. 2 red, major ≥4 incl. 3 red), with `value:` ranges inside each KPI's red/yellow band. `affected_services` must include a trace entry service (a service with `entry_endpoints`), not only a database, or the ops dashboard stays green. Reference: `scenarios/space/channels/01-*.yaml`.

`investigation_notes`: 5–6 numbered steps, or the k8splatform shape (`Root cause:` / `Confirm:` bullets / `Remediation:`). Reference specific log field names, metric names, and `{placeholder}` values; name the change event that preceded the errors and the healthy comparison outside `fault_scope`. Tell the agent exactly what to search for and how to confirm the fix.

Schema 1.3 fields (see CONTRACT.md §Schema 1.3 fields and §Remediation and signal at 1.3): `remediation_command` (verbs from the ops profile), `fault_scope`, `infrastructure_events` with the cause at a negative `offset_seconds`, `onset_delay_seconds` / `ramp_seconds`, `cascade_messages` (and `error_services` when needed), `k8s_events` for Kubernetes reasons, `fault_state` for values that differ during the fault, and `remediation_effects` (`verification`, plus `state` / `log_events` / `steady_attributes` as the fix requires). Do not add `chaos.*` or other demo attributes anywhere.

`fault_params` and `rca_clues` use the YAML DSL (see CONTRACT.md):

```yaml
fault_params:
  payment_provider: {choice: [stripe, adyen, braintree]}
  timeout_ms: {randint: [3000, 15000]}
  order_id: {format: "ORD-{n}", n: {randint: [100000, 999999]}}

rca_clues:
  payment-processor:
    payment.gateway_timeout_ms: {randint: [3000, 15000]}
    payment.provider_circuit_open: true
  order-management:
    order.checkout_failure_rate_pct: {uniform: [15, 60], round: 1}
```

### Edit 2 — Update `agent_config.system_prompt` in `scenario.yaml`

Find the subsystem grouping that contained the old `error_type` and remove it. Add the new `error_type` to the appropriate subsystem grouping (or create a new grouping if the subsystem is new).

Pattern used in existing scenarios:
```
"<Subsystem> faults (TYPE-A, TYPE-B, TYPE-C), "
```

### Edit 3 — Signal service branches (schema 1.3)

In the service that emits the channel's signal metric (`scenarios/<id>/services/<svc>.yaml`), replace the old channel's branches for number N with a during-fault and an after-fix branch for the new one: out of band while `N in all_active` (ramp with `elapsed.get(N, 0)`), converging back while `N in fixed_channels` (`min(1.0, since_fix.get(N, 0) / <recovery_s>)`), baseline otherwise; the status log carries the configuration the fix changes, keyed on `fixed_channels` or `applied`. Pattern: §Before / during / after in [../add-scenario/contract/services.md](../add-scenario/contract/services.md). Touch only the steps and expressions for channel N.

### Edit 4 — Ops profile verb (only when needed)

If no verb in the ops profile reads naturally for the fix, add it under `verbs:` in `scenarios/<id>/ops.yaml` with `summary` and `example` (and `describe` when the default wording does not fit). Prefer an existing verb; never rename or remove verbs other channels use.

---

## Phase 5: Validate

Run the CONTRACT.md checklist inline (no external script needed — inspect the edited content):

1. **Placeholder parity** — collect every `{placeholder}` from the new `error_message` and `stack_trace`; confirm each is a key in `fault_params`.
2. **Service validity** — every value in `affected_services` and `cascade_services` is a filename stem under `scenarios/<id>/services/`.
3. **Channel count** — run `ls scenarios/<id>/channels/*.yaml | wc -l` → must be 20.
4. **HITL/auto-remediate** — channel ≤ 15: HITL action; channel 16–20: plausible auto-runbook action.
5. **system_prompt sync** — new `error_type` appears in the prompt; old `error_type` (if changed) does not.
6. **Schema 1.3** (when the scenario is at 1.3) — `remediation_command` lines are `opsctl <verb> ...` with a verb the scenario's ops profile defines (Check 37); one `{actor}` `log_events` line per command line that satisfies the audit format's `match`; the signal service has the during-fault and after-fix branches for N; `fault_state` / `remediation_effects.state` keys are in the generator's `SPEC.state_keys` and name workloads / pools in `k8s_topology` (38); `fault_scope` names exist (39); `error_services` ⊆ `affected_services` and `cascade_messages` keys and placeholders are valid (40); `remediation_effects.verification` present (41), and no Check 41 warning that a verification `metrics.<name>` or log `attributes.<key>` is emitted by nothing; `ops.yaml` still valid if edited (45); a cause event precedes the symptoms (42, warning); coherence (43, warning); `error_type` has no quotes (44).
7. **Realism** — walk the Realism checklist in `../add-scenario/BRIEF.md`; in particular the new `error_type` is a substring of the new `error_message`, unique among the 20 channels, and no text says "channel N", "chaos" or "demo".
8. **Dashboards react** — `business_impact` has dict entries targeting names from `executive_kpi_emissions` that land enough tiles in yellow/red for the channel's scope tier (copy the old channel's block as a starting point; re-check the values; see CONTRACT.md), every impacted KPI has `better:` set, and `affected_services` includes a trace entry service. `python3 scripts/verify_yaml_scenarios.py <id>` (Checks 30–32) is the arbiter.

**Full integrity check:**
```bash
python3 scripts/verify_yaml_scenarios.py <id>
```

**Scope check:**
```bash
git diff --name-only
```
Output must show only files under `scenarios/<id>/`. If any other file appears, stop and alert the user.

**Report:** Summarize what changed (old channel → new channel), which files were edited, and any validation warnings. Include the channel number and error_type for easy cross-reference.
