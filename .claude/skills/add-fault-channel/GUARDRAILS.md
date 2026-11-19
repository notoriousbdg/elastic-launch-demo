# Guardrails

These rules are non-negotiable. Hold them for the entire session.

---

## 1. Scope: one scenario folder only

**Only modify files under `scenarios/<id>/`** — specifically the channel YAML file and `scenario.yaml` (for the system_prompt update). At schema 1.3 two more edits are in scope:

- the service file that emits the channel's signal metric, limited to the steps and expressions for this channel's number (its during-fault and after-fix branches);
- `scenarios/<id>/ops.yaml`, only to add a verb (with `summary` and `example`) when no existing verb reads naturally for the fix. Never rename or remove a verb another channel uses.

Never touch:
- Any file in `generators/`, `generator_engine/`, `app/` (including `app/ops_presets/`), `elastic_config/`, or `scenario_engine/`
- Any other scenario folder
- Other steps in service files under `scenarios/<id>/services/`

If you think a change elsewhere is needed, stop and ask the user.

If the new fault needs a workload or node pool that `k8s_topology` in `scenario.yaml` does not have, stop and ask the user before adding it there.

---

## 2. Channel count stays at exactly 20

This is a **replace** operation, not an append. The new channel takes the same number as the one it replaces. The `channels/` directory must always contain exactly 20 YAML files.

Why: the deployer, dashboard generator, and ML jobs hardcode channel counts.

---

## 3. HITL rule: channels 1–15 vs 16–20

**Channels 1–15** — Human-In-The-Loop faults. These require human approval before remediation. `remediation_action` should describe an action that needs judgment: data recovery, fraud review, multi-service cascade, novel anomaly, compliance sign-off.

**Channels 16–20** — Auto-remediable faults. The workflow resolves these without human approval. The fault must be one a real SRE team would automate:

Good candidates for 16–20:
- Pod restart on OOM kill
- Autoscaler bumping replicas
- Circuit breaker tripping an unresponsive upstream
- Cache flush/warmup after staleness exceeds TTL
- Credential or API token rotation after expiry
- TLS certificate auto-renewal
- Budget cap reset or threshold adjustment
- Database connection pool restart after connection leak
- Static route failover after health check failure

Bad candidates for 16–20 (belong in 1–15):
- Data corruption (requires human scope verification)
- Fraud or security anomaly (requires human review)
- Multi-service cascade (remediation may have downstream effects)
- Novel or first-seen error (no runbook yet)
- Regulatory/compliance violation (requires audit trail + sign-off)

Test: "Would a well-run SRE team add this to their auto-remediation runbook?" If the answer is "maybe, with caveats" → it belongs in 1–15.

---

## 4. Placeholder parity is mandatory

Every `{placeholder}` name in `error_message` and `stack_trace` must be a key in `fault_params` in the same channel YAML file. Missing keys cause `KeyError` at runtime.

Collect all placeholder names from both strings before writing `fault_params`. The runtime placeholders the engine fills (`{self_node}`, `{self_pod}`, `{self_pod_ip}`, `{node_count}`, `{pool_<name>_count}`, `{klog_time}`, `{nginx_time}`, `{iso_now}`, `{epoch_now}`, ...) need no key.

The same holds for `investigation_notes`: every `{placeholder}` there must be a `fault_params` key, or it is shown as literal text in the KB doc, Wiki runbook and channel runbook skill (verifier Check 35).

---

## 5. Service names must exist

Every value in `affected_services` and `cascade_services` must be a filename stem under `scenarios/<id>/services/` (e.g. `payment-processor` → `services/payment-processor.yaml` must exist). Verify before writing.

---

## 6. `error_type` changes require a system_prompt update

The `agent_config.system_prompt` in `scenario.yaml` contains an explicit list of all 20 `error_type` values grouped by subsystem. If you change or add an `error_type`, you must update the system_prompt to match.

The agent will fail to find fault logs for any `error_type` not listed in its prompt.

---

## 7. No invented service names or subsystems

Do not introduce a `subsystem` label that doesn't already exist in service files. Do not reference a service key that isn't a filename stem under `scenarios/<id>/services/`.

---

## 8. Telemetry must read like a real incident (schema 1.3)

Follow the Realism checklist in [../add-scenario/BRIEF.md](../add-scenario/BRIEF.md). In particular:

- `error_type` is a natural substring of `error_message`, contains no quote characters (Check 44), and is unique among the 20 channels.
- `remediation_command` fixes the root cause (roll back the change, restore the policy), not the symptom (restart pods).
- There is a `fault_scope`, a cause event with a negative `offset_seconds`, and `remediation_effects.verification`.
- Every `remediation_command` verb exists in the scenario's ops profile (`ops.yaml` + its `extends` presets; Check 37), and `remediation_effects.log_events` has one `{actor}` line per command line that satisfies the audit format's `match`.
- Every verification signal is emitted by something: a `metrics.<name>` by a service `metric` step, a log `attributes.<key>` by a service step, `steady_attributes`, `rca_clues` or a fix line. Treat Check 41 warnings as failures.
- The signal service has a during-fault branch (`N in all_active`, ramped with `elapsed`) and an after-fix branch (`N in fixed_channels`, converging with `since_fix`), and shows the fix's configuration keyed on `fixed_channels` or `applied`.
- Fault logs no longer carry `chaos.*` fields (or `ops.*`, `system.*`, `cascade.*`, an `event_name` blob). Never add such attributes in `rca_clues`, `infrastructure_events`, `remediation_effects` or anywhere else.
- No "channel N", "chaos", "demo" or mission jargon in any text that becomes telemetry or a runbook.

