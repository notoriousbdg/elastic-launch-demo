# Channel property contract

Use this as a generation checklist. Work top-to-bottom and verify every property is present and correctly shaped before writing the channel YAML file.

Reference source: [`scenarios/ecommerce/channels/01-payment-gateway-timeout.yaml`](scenarios/ecommerce/channels/01-payment-gateway-timeout.yaml)

---

## `channels/NN-<slug>.yaml` — all required fields

| Field | Required | Type | Notes |
|---|---|---|---|
| `name` | ✅ | `str` | Display name. E.g. `"Payment Gateway Timeout"` |
| `subsystem` | ✅ | `str` | Matches a `subsystem` value in a service file under `scenarios/<id>/services/` |
| `vehicle_section` | ✅ | `str` | Domain-appropriate logical zone. E.g. `"checkout_pipeline"`, `"engine_bay"`, `"auth_layer"` |
| `error_type` | ✅ | `str` | ALLCAPS-HYPHENATED. Used in `body.text` log search. E.g. `"PAYMENT-GATEWAY-TIMEOUT"` |
| `sensor_type` | ✅ | `str` | Domain flavor — what sensor/metric triggered this. E.g. `"gateway_latency"`, `"pressure"` |
| `affected_services` | ✅ | `list[str]` | Direct fault targets. Every entry must be a filename stem under `services/`. **Must include at least one trace entry service** (a service with `entry_endpoints`), not only a database — the trace generator only raises error spans for entry services, so otherwise the ops dashboard stays green (Check 31). |
| `cascade_services` | ✅ | `list[str]` | Downstream victims. Every entry must be a filename stem under `services/`. May be `[]`. |
| `description` | ✅ | `str` | 1–2 sentence customer-facing description of the observable failure |
| `investigation_notes` | ✅ | `str` | 5–6 numbered steps for the runbook skill. See shape below. |
| `remediation_action` | ✅ | `str` | `snake_case` action identifier. E.g. `"restart_payment_gateway"` |
| `error_message` | ✅ | `str` | Log line template. All `{placeholder}` names must be keys in `fault_params`. |
| `stack_trace` | ✅ | `str` | Multi-line dump template. All `{placeholder}` names must be keys in `fault_params`. |
| `correlation_attr` | optional | `dict` | `{key: "attr.name", value: "value"}` — injected on error spans at 90% rate, 5% on normal spans. |
| `fault_params` | ✅ | `dict` | YAML DSL expressions for every `{placeholder}` in `error_message` + `stack_trace`. |
| `rca_clues` | ✅ | `dict` | Per-service partial clues. Keys = service names from `affected_services` + `cascade_services`. |
| `infrastructure_events` | optional | `list` | Extra infrastructure log events during the fault window. |
| `business_impact` | ✅ when the scenario has `executive_kpi_emitter_service_name` | `list[dict]` | Entries `{kpi, value: {uniform: [lo, hi], round: n}}` (absolute override, preferred — place it inside the KPI's red/yellow band) or `{kpi: business.<name>, factor: {uniform: [lo, hi]}}` (scales the nominal draw). `kpi` must match a `name:` in `executive_kpi_emissions`, which must declare `better: lower\|higher`. Count scales with scope (2×affected + cascade): minor ≥2 yellow/red tiles incl. 1 red, moderate ≥3 incl. 2 red, major ≥4 incl. 3 red (Check 32). This is what moves the executive dashboard during a fault; bare KPI-name strings are ignored. When replacing a channel, **copy `business_impact` from the old channel if present** rather than dropping it. |

### `investigation_notes` shape

5–6 numbered steps. Should reference:
- Specific log field names (always `body.text`, never `body` alone)
- Specific metric names from the scenario's service telemetry
- `{placeholder}` values from `error_message` to anchor searches
- Only `{placeholder}` names that are `fault_params` keys — the KB doc, Cortex runbook and channel runbook skill replace each one with the parameter's range, and any other `{name}` is shown literally (verifier Check 35). Shell syntax such as curl's `%{time_total}` is fine
- Thresholds that the `rca_clues` values actually cross — investigators compare the clue values with these numbers, and a clue that may sit on the healthy side keeps them from confirming the cause
- The root cause hypothesis (what component or condition triggers this)
- The remediation action and how to confirm it worked

Good example:

```text
1. Search body.text for PAYMENT-GATEWAY-TIMEOUT in logs.otel.{ns} to establish blast radius.
2. Check {payment_provider} gateway health endpoint — compare {timeout_ms}ms against p99 baseline (typically <800ms).
3. Inspect connection pool metrics on payment-processor: active_connections vs pool_max.
4. Cross-reference with order-management error rate spike at the same timestamp.
5. Trigger restart_payment_gateway action and confirm new connections establish within 30s.
6. Verify no in-flight transactions were dropped (check transaction_id rollback logs).
```

---

## `fault_params` — YAML DSL reference

All values use the same DSL as `trace_attributes` and `rca_clues`. Every `{placeholder}` in `error_message` and `stack_trace` must be a key here.

| Python equivalent | YAML DSL |
| --- | --- |
| `random.choice(["a","b"])` | `{choice: [a, b]}` |
| `random.randint(1, 100)` | `{randint: [1, 100]}` |
| `round(random.uniform(80.0, 99.5), 1)` | `{uniform: [80.0, 99.5], round: 1}` |
| `f"ORD-{random.randint(1000,9999)}"` | `{format: "ORD-{n}", n: {randint: [1000, 9999]}}` |
| `True` | `true` |

Example:

```yaml
fault_params:
  payment_provider: {choice: [stripe, adyen, braintree]}
  order_id: {format: "ORD-{n}", n: {randint: [100000, 999999]}}
  timeout_ms: {randint: [3000, 15000]}
  pool_size: {randint: [50, 200]}
  active_connections: {randint: [48, 200]}
```

Rules:

- Every `{name}` in `error_message` **and** `stack_trace` must be a key here
- Domain-appropriate ranges (don't use `randint: [0, 9999]` for a latency in milliseconds)
- 2–5 params per channel is typical

---

## `rca_clues` — per-service partial clues

```yaml
rca_clues:
  payment-processor:
    payment.gateway_timeout_ms: {randint: [3000, 15000]}
    payment.provider_circuit_open: true
  order-management:
    order.checkout_failure_rate_pct: {uniform: [15, 60], round: 1}
    order.pending_payment_count: {randint: [50, 500]}
  storefront-gateway:
    storefront.checkout_error_rate_pct: {uniform: [10, 45], round: 1}
    storefront.error_page_shown: true
```

Rules:
- Keys must be service names from `affected_services` + `cascade_services`
- 2–3 attributes per service
- Attributes should suggest (but not directly name) the root cause — different services see different partial symptoms
- Use OTel-style dotted names: `"payment.pool_exhaustion_pct"`, `"gateway.connection_wait_ms"`
- Numeric values with domain-appropriate ranges
- **Evidence contract** — the clues are what investigations (the agent and Nightshift) use to confirm the cause, and they are only emitted while the fault is active:
  - Every range sits entirely on the fault side of the threshold the notes give (notes say ">3.5A indicates binding" → `{uniform: [3.6, 4.5]}`, not `[2.5, 4.0]`)
  - A `choice` lists only fault values — never `OFF`, `INTACT`, `false` or another healthy reading (verifier Check 36 warns)
  - Each reading the notes tell an investigator to check has a clue on the service that would report it
  - Readings in `stack_trace` frame dumps that the notes rely on are `{fault_params}` placeholders, so the dump agrees with the span clues instead of showing a fixed healthy-looking number

---

## `agent_config.system_prompt` in `scenario.yaml` — error_type list

After writing the new channel, verify the `system_prompt` string in `agent_config` lists the new `error_type` under the correct subsystem grouping. If the `error_type` changed, the old value must be removed and the new value added.

Format used in existing scenarios:

```text
"<Subsystem> faults (<ERR-TYPE-1>, <ERR-TYPE-2>, <ERR-TYPE-3>), "
```

If the new channel adds a first fault in a new subsystem, add a new grouping line.

---

## Validation checklist (run after editing)

- [ ] `ls scenarios/<id>/channels/*.yaml | wc -l` → 20
- [ ] Channel number unchanged (same `NN` prefix, file replaced in-place)
- [ ] All `{placeholder}` in `error_message` are keys in `fault_params`
- [ ] All `{placeholder}` in `stack_trace` are keys in `fault_params`
- [ ] All `affected_services` values are filename stems under `services/`
- [ ] All `cascade_services` values are filename stems under `services/`
- [ ] `error_type` appears in `agent_config.system_prompt` under the correct subsystem
- [ ] Old `error_type` removed from `system_prompt` (if it changed)
- [ ] Channel number ≤ 15 → `remediation_action` is a HITL action
- [ ] Channel number 16–20 → `remediation_action` is a plausible auto-runbook action
- [ ] `business_impact` has dict entries targeting names in `executive_kpi_emissions` that turn enough KPI tiles yellow/red for the channel's scope tier, impacted KPIs declare `better:`, and `affected_services` includes a trace entry service (copy the old channel's block if it had one; re-check the values; don't silently drop it)
- [ ] `python3 scripts/verify_yaml_scenarios.py <id>` passes
- [ ] `git diff --name-only` shows only files under `scenarios/<id>/`
