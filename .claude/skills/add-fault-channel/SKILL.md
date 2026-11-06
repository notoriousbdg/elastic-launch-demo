---
name: add-fault-channel
description: Replace one fault channel in an existing scenario. Use when the user wants to add, swap, or improve a fault in scenarios/<id>/channels/. Replaces the channel YAML file and updates agent_config.system_prompt in scenario.yaml. Never touches any file outside scenarios/<id>/.
---

# Replace a fault channel

This skill replaces one existing fault channel in a scenario with a new or improved one. It replaces `scenarios/<id>/channels/NN-<slug>.yaml` and updates `agent_config.system_prompt` in `scenario.yaml`.

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

Also read [CONTRACT.md](CONTRACT.md) now as a generation checklist.

---

## Phase 3: Design — propose then confirm

Present the proposed new channel via `AskUserQuestion` (single question, long description field) and ask the user to confirm, adjust, or replace before writing any files.

The proposal must cover:

- **Channel number** being replaced, and current channel name being replaced
- **New channel name, subsystem, vehicle_section, error_type**
- **error_message** template (showing all `{placeholder}` names)
- **affected_services** and **cascade_services** (service keys, not display names)
- **remediation_action** and whether it's HITL (1–15) or auto-remediable (16–20)
- **investigation_notes** summary (key steps; full 5–6 lines in Phase 4)
- **agent_config.system_prompt** change — old error_type → new error_type

If the channel number is 16–20, confirm the `remediation_action` passes the auto-remediate test from GUARDRAILS.md §3.

---

## Phase 4: Make the edits

After the user confirms, make both edits. Do not ask permission for each.

### Edit 1 — Replace the channel YAML file

Write a new `scenarios/<id>/channels/NN-<new-slug>.yaml`. Keep the same channel number prefix (`NN`). Delete the old file if the slug changes.

Follow the field order from CONTRACT.md. See the full schema there.

Include `business_impact` so BOTH dashboards react in proportion to the fault's scope: dict entries targeting `executive_kpi_emissions` names, enough to turn tiles yellow/red (scope = 2×affected + cascade; minor ≥2 non-green incl. 1 red, moderate ≥3 incl. 2 red, major ≥4 incl. 3 red), with `value:` ranges inside each KPI's red/yellow band. `affected_services` must include a trace entry service (a service with `entry_endpoints`), not only a database, or the ops dashboard stays green. Reference: `scenarios/space/channels/01-*.yaml`.

`investigation_notes`: 5–6 numbered steps. Reference specific log field names, metric names, and `{placeholder}` values. Tell the agent exactly what to search for and how to confirm the fix.

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

---

## Phase 5: Validate

Run the CONTRACT.md checklist inline (no external script needed — inspect the edited content):

1. **Placeholder parity** — collect every `{placeholder}` from the new `error_message` and `stack_trace`; confirm each is a key in `fault_params`.
2. **Service validity** — every value in `affected_services` and `cascade_services` is a filename stem under `scenarios/<id>/services/`.
3. **Channel count** — run `ls scenarios/<id>/channels/*.yaml | wc -l` → must be 20.
4. **HITL/auto-remediate** — channel ≤ 15: HITL action; channel 16–20: plausible auto-runbook action.
5. **system_prompt sync** — new `error_type` appears in the prompt; old `error_type` (if changed) does not.
6. **Dashboards react** — `business_impact` has dict entries targeting names from `executive_kpi_emissions` that land enough tiles in yellow/red for the channel's scope tier (copy the old channel's block as a starting point; re-check the values; see CONTRACT.md), every impacted KPI has `better:` set, and `affected_services` includes a trace entry service. `python3 scripts/verify_yaml_scenarios.py <id>` (Checks 30–32) is the arbiter.

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
