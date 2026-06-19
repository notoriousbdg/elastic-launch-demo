# Guardrails

These rules are non-negotiable. Hold them for the entire session.

---

## 1. Scope: one scenario folder only

**Only modify files under `scenarios/<id>/`** — specifically the channel YAML file and `scenario.yaml` (for the system_prompt update).

Never touch:
- Any file in `log_generators/`, `app/`, `elastic_config/`, or `scenarios/base.py`
- Any other scenario folder
- Service files under `scenarios/<id>/services/`

If you think a change elsewhere is needed, stop and ask the user.

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

Collect all placeholder names from both strings before writing `fault_params`.

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
