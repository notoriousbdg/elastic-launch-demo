# Channel brief guide

Use this file during Phase 1 to determine what to ask. Parse what the user already provided, then ask only about genuinely missing signals. Ask at most 3 questions per `AskUserQuestion` call.

---

## Required signals

All three must be confirmed before moving to Phase 2.

### 1. Scenario

Which scenario is being modified. Accept either the `scenario_id` slug (any existing scenario under `scenarios/` — e.g. `banking`, `ecommerce`, `telecom`, `space`, `fanatics`, `financial`, `healthcare`, `gaming`, `gcp`, `manufacturing`) or a display name.

If the user names a customer or vertical without specifying a scenario, pick the closest existing scenario and confirm: "It sounds like you want the `financial` scenario — is that right?"

### 2. Channel number

Which channel (1–20) to replace. Accept any of:
- An explicit number ("channel 7", "channel 14")
- A subsystem area ("a propulsion fault", "something in the payments subsystem")
- A description of what they want to change ("the GPS fault is too generic, I want something more realistic")

If the user describes a fault area but not a number, list `scenarios/<id>/channels/` — the filenames encode number and slug — and suggest the best-fit slot. Present the current channel name and ask if they want to replace it.

### 3. Fault description

What the new channel should represent — the observable failure, what causes it, and ideally what remediation looks like.

**Sufficient:** "payment gateway connection pool exhaustion — all outbound payment connections backed up, causing checkout failures. Remediation is a connection pool restart."

**Too vague:** "something with payments" — ask for the observable failure and remediation action.

---

## Helpful signals (ask if absent, but don't block)

### 4. HITL or auto-remediate intent

Ask only if the channel number is 16–20 and the fault doesn't obviously fit the auto-remediate criteria. See GUARDRAILS.md §3 for the decision rule.

### 5. Specific services to target

Which of the 9 services should be `affected_services` (direct fault) vs `cascade_services` (downstream warning). List `scenarios/<id>/services/` to present the filename stems as options — don't ask the user to recall service names from memory.

---

## What NOT to ask

- Real customer system names or internal codenames
- Anything the user has already told you
- The scenario's existing channels — read them from the file instead of asking
