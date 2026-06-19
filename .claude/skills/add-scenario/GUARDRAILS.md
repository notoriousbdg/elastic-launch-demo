# Guardrails

These rules are non-negotiable. Hold them for the entire session. They encode hard-won conventions from AGENTS.MD rule #44 and team experience.

---

## 1. Scope: scenario folder only

**Only create or modify files under `scenarios/<new-id>/`.**

Never edit:
- `log_generators/` — data generators are shared and scenario-agnostic
- `app/` — runtime wiring; scenarios plug in via the `BaseScenario` interface
- `elastic_config/` — deployer; reads scenario properties via duck typing
- `scenarios/base.py` — the contract; adding properties here affects all scenarios
- `scenarios/__init__.py` — auto-discovery; no registration needed
- Any other existing scenario folder

If you believe a change outside `scenarios/<new-id>/` is necessary, **stop and ask the user** rather than proceeding. The default answer is no.

Why: the auto-discovery mechanism and the deployer are designed to accommodate new scenarios without any changes outside their folder. A change outside breaks this invariant and risks silently affecting existing scenarios.

---

## 2. No scenario-specific logic in shared code

Never add an `if scenario_id == "..."` branch, a lookup dict keyed by scenario ID, or any scenario-specific constant to code outside `scenarios/<id>/`.

Every scenario-specific value must be a property on the scenario class. The deployer, dashboard generator, and agent code call `scenario.<property>` — they never branch on `scenario.scenario_id`.

Why: AGENTS.MD rule #44. Adding scenario branches to shared code is the primary source of "adding a scenario breaks other scenarios" bugs.

---

## 3. No new data generators without explicit user approval

Do not create new files in `log_generators/`. The existing generators (trace, host_metrics, k8s_metrics, jvm_metrics, nginx, mysql, vpc_flow, raw_access_log) cover all current telemetry shapes.

If a new generator is genuinely needed, it must:
- Be completely scenario-agnostic (parameterized via `scenario_data` only, no hardcoded scenario values)
- Accept `chaos_controller=` for fault-awareness consistency
- Be discussed and approved by the user before implementation

---

## 4. Telemetry shapes are fixed

The Elastic integrations installed by `elastic_config/deployer_integrations.py` are global (not per-scenario): `kubernetes_otel`, `aws_vpcflow_otel`, `gcp_vpcflow_otel`, `nginx_otel`, `mysql_otel`. New scenarios must re-use these data shapes. Do not invent new index patterns or data stream names.

---

## 5. No real customer names or proprietary jargon

The scenario must be relatable to any company in the vertical, not identifiable as any specific customer.

**Forbidden:**
- Real company names (customer or competitor)
- Internal project codenames
- Proprietary acronyms that only one customer uses
- Regulatory program names specific to one organization

**Allowed:**
- Industry-standard terms (PCI DSS, HIPAA, FCC, SCADA, OEE, ERP, MES)
- Generic role labels (warehouse, carrier, provider, subscriber, clinician)
- Standard product-category names (payment gateway, recommendation engine, order management)

When in doubt, ask: "Would a different company in the same vertical recognize this name without context?" If no, generalize it.

---

## 6. Auto-remediate channels (16–20) must be plausibly auto-remediable

Channels 16–20 trigger an automated workflow that resolves the fault without human approval. The fault type must be one that a real SRE team would legitimately automate:

**Good candidates for 16–20:**
- Pod restart on OOM kill
- Autoscaler bumping replicas under sustained CPU/memory spike
- Circuit breaker tripping an unresponsive upstream
- Cache flush/warmup after staleness exceeds TTL
- Credential or API token rotation after expiry
- TLS certificate auto-renewal
- Budget cap reset or alert threshold adjustment
- Database connection pool restart after connection leak
- Static route failover after health check failure

**Bad candidates for 16–20 (belong in 1–15):**
- Data corruption or inconsistency (requires human judgment to verify scope)
- Fraud or security anomaly (requires human review before any action)
- Multi-service cascade (remediation may have unintended downstream effects)
- Novel or first-seen error pattern (no runbook yet)
- Regulatory/compliance violation (requires audit trail + human sign-off)
- Any fault where automated remediation could make things worse

Test: "Would a well-run SRE team add this to their auto-remediation runbook?" If the answer is "maybe, with caveats" → put it in 1–15.

---

## 7. Service count and channel count are fixed

- Exactly **9 services** (3 per cloud: AWS, GCP, Azure)
- Exactly **20 channels** (1–20)
- Exactly **3 hosts** (one per cloud)
- Exactly **3 k8s clusters** (EKS, GKE, AKS)

These numbers are baked into the deployer, dashboard generator, ML jobs, and the executive dashboard layout. Deviation causes silent failures or misaligned dashboards.

---

## 8. Service language must be in the allowed set

Language values in `services[<svc>]["language"]` must be one of:

`python` | `java` | `go` | `dotnet` | `rust` | `cpp`

These map to exception type tables in `log_generators/trace_generator.py`. Any other value will cause the trace generator to emit empty exception fields.

---

## 9. Each service is fully defined in `services/<svc>.yaml` — `scenario.yaml` carries no per-service data

A service's complete definition — identity/resource metadata, call topology, entry endpoints, DB operations, and telemetry DSL — all live in one file per service:

```
scenarios/<id>/services/
    payment-processor.yaml     # sort_order, cloud_*, subsystem, language, topology,
    order-management.yaml      # entry_endpoints, db_operations, emit_fault_logs,
    ... (9 total)              # kpi_emitter, constants, state, steps
```

`YamlScenario` auto-discovers these files, sorts by `sort_order`, and assembles `scenario.services`, `service_topology`, `entry_endpoints`, and `db_operations` at load time. **`scenario.yaml` must NOT contain `services:`, `service_topology:`, `entry_endpoints:`, or `db_operations:` blocks** — those are now per-service concerns.

The telemetry DSL is interpreted by `app/services/telemetry_dsl.py` (shared infra — see §11). **Never write a Python `BaseService` subclass for service telemetry.** The `services/` directory must contain only `.yaml` files (no `.py` files, no `__init__.py`).

See the **Service telemetry DSL** section in CONTRACT.md for the full spec schema.

---

## 10. Scenario folders are pure YAML — no Python files

A scenario folder contains **only**:

```text
scenarios/<id>/
    scenario.yaml
    channels/
        01-<slug>.yaml  (×20)
        ...
    services/
        <svc>.yaml      (×9)
        ...
```

**Do NOT create `scenario.py`, `__init__.py`, or `executive_kpis.py`** in a scenario folder. The registry (`scenarios/__init__.py`) discovers scenarios by globbing `scenarios/*/scenario.yaml` and calling `load_yaml_scenario` directly — no Python shim is needed or expected.

---

## 11. `scenarios/fault_spec.py` and `scenarios/yaml_scenario.py` are shared infrastructure

These are shared across all scenarios. **Never modify them for a new scenario.**

If you believe a change to these files is necessary, stop and ask the user. The default answer is no.

These files live outside any scenario subfolder and are not scenario-specific:
- `scenarios/fault_spec.py` — YAML DSL resolver for fault params / rca_clues / trace_attributes
- `scenarios/yaml_scenario.py` — `YamlScenario` class + `load_yaml_scenario()` + `emit_executive_business_metrics_if_eligible()`
- `app/services/telemetry_dsl.py` — `YamlService` executor (interprets `services/<svc>.yaml` specs each cycle)
- `app/services/expr.py` — sandboxed expression evaluator used by the DSL (`{expr: "..."}` values)
