# Channel property contract

Use this as a generation checklist. Work top-to-bottom and verify every property is present and correctly shaped before writing the channel YAML file.

Reference sources:
- Schema 1.3: [`scenarios/k8splatform/channels/02-edge-conntrack-exhaustion.yaml`](scenarios/k8splatform/channels/02-edge-conntrack-exhaustion.yaml) (rollback fix) and [`scenarios/k8splatform/channels/13-ingress-oom-reload-storm.yaml`](scenarios/k8splatform/channels/13-ingress-oom-reload-storm.yaml) (fix that sets a new value)
- Schema 1.2 application vertical: [`scenarios/ecommerce/channels/01-payment-gateway-timeout.yaml`](scenarios/ecommerce/channels/01-payment-gateway-timeout.yaml)

Hold every field to the Realism checklist in [../add-scenario/BRIEF.md](../add-scenario/BRIEF.md).

---

## `channels/NN-<slug>.yaml` — all required fields

| Field | Required | Type | Notes |
|---|---|---|---|
| `name` | ✅ | `str` | Display name. E.g. `"Payment Gateway Timeout"` |
| `subsystem` | ✅ | `str` | Matches a `subsystem` value in a service file under `scenarios/<id>/services/` |
| `vehicle_section` | ✅ | `str` | Domain-appropriate logical zone. E.g. `"checkout_pipeline"`, `"engine_bay"`, `"auth_layer"` |
| `error_type` | ✅ | `str` | Used in the `body.text` search of the alert rule and significant-event query. At schema 1.3: a natural substring of `error_message` (e.g. `"nf_conntrack: table full, dropping packet"`), no quote characters (Check 44), unique across the 20 channels and absent from healthy lines. Older scenarios use ALLCAPS-HYPHENATED codes (`"PAYMENT-GATEWAY-TIMEOUT"`); do not create new ones. |
| `sensor_type` | ✅ | `str` | Domain flavor — what sensor/metric triggered this. E.g. `"gateway_latency"`, `"pressure"` |
| `affected_services` | ✅ | `list[str]` | Direct fault targets. Every entry must be a filename stem under `services/`. **Must include at least one trace entry service** (a service with `entry_endpoints`), not only a database — the trace generator only raises error spans for entry services, so otherwise the ops dashboard stays green (Check 31; a warning instead of an error for a 1.3 channel with a `fault_scope`). |
| `cascade_services` | ✅ | `list[str]` | Downstream victims. Every entry must be a filename stem under `services/`. May be `[]`. |
| `description` | ✅ | `str` | 1–2 sentence customer-facing description of the observable failure |
| `investigation_notes` | ✅ | `str` | 5–6 numbered steps for the runbook skill. See shape below. |
| `remediation_action` | ✅ | `str` | `snake_case` action identifier. E.g. `"restart_payment_gateway"` |
| `error_message` | ✅ | `str` | Log line template in the component's real log format. All `{placeholder}` names must be keys in `fault_params` or runtime placeholders (`{self_node}`, `{self_pod_ip}`, `{node_count}`, `{klog_time}`, `{nginx_time}`, ...; see §Schema 1.3 fields). |
| `stack_trace` | ✅ | `str` | Multi-line dump template, or `''` when the component logs no stack trace (the engine then adds no `exception.*` attributes). Same placeholder rule. |
| `correlation_attr` | optional | `dict` | `{key: "attr.name", value: "value"}` — injected on the `affected_services`' spans only (90% of error or affected-service spans, 5% of others); cascade and unrelated services never carry it. |
| `fault_params` | ✅ | `dict` | YAML DSL expressions for every `{placeholder}` in `error_message` + `stack_trace`. |
| `rca_clues` | ✅ | `dict` | Per-service partial clues. Keys = service names from `affected_services` + `cascade_services`. |
| `infrastructure_events` | optional (1.3: one is the cause) | `list` | One-shot log events per trigger: `{service, severity, body, attributes, offset_seconds}`. At 1.3 one entry is the change or provider event that caused the fault, with a negative `offset_seconds` so it is logged before the errors (Check 42). |
| `remediation_command` | ✅ at 1.3 | `str` | One `opsctl <verb> <kind>[/<name>] [-n ns] [--flags]` per line (Check 37). See §Schema 1.3 fields. |
| `fault_scope` | ✅ at 1.3 (warning) | `dict` | `{cluster, node_pool, nodes, az, workloads, namespaces}` (Check 39). |
| `onset_delay_seconds` / `ramp_seconds` | optional | `number` | Symptoms start after the delay; Kubernetes impacts ramp in. |
| `service_lag_seconds` | optional | `{service: number}` | Extra delay for one service on top of `onset_delay_seconds`, so a dependent degrades after the component at fault. Keys must be affected or cascade services. In a service's DSL, `lag.get(N, 0)` is that total delay for channel N. |
| `generator_hold_seconds` | optional | `{generator_id: number}` | Keep that generator's `generator_impact` applied this many seconds after the fix, so a package stream (Azure OpenAI, Application Gateway...) recovers together with the scenario's own metrics instead of snapping to baseline. Keys must be in the channel's `generator_impact`. Leave out event-style generators (audit and security events): their records stop when the fix lands. |
| `cascade_messages` | optional | `dict` | `{service: [str or {severity, message}]}` in each dependent's real log format (Check 40). |
| `error_services` | optional | `list` | Subset of `affected_services` that emits `error_message`; the others emit their `cascade_messages` (Check 40). |
| `k8s_events` | optional | `list` | Kubernetes events while the fault is active (pinned-topology scenarios). |
| `fault_state` | optional | `dict` | `{<generator_id>: {<state_key>: ...}}` values that differ during the fault (Check 38). |
| `remediation_effects` | ✅ at 1.3 (`verification`) | `dict` | `verification`, `state`, `transition_seconds`, `log_events`, `steady_attributes` (Checks 38, 41). |
| `business_impact` | ✅ when the scenario has `executive_kpi_emitter_service_name` | `list[dict]` | Entries `{kpi, value: {uniform: [lo, hi], round: n}}` (absolute override, preferred — place it inside the KPI's red/yellow band) or `{kpi: business.<name>, factor: {uniform: [lo, hi]}}` (scales the nominal draw). `kpi` must match a `name:` in `executive_kpi_emissions`, which must declare `better: lower\|higher`. Count scales with scope (2×affected + cascade): minor ≥2 yellow/red tiles incl. 1 red, moderate ≥3 incl. 2 red, major ≥4 incl. 3 red (Check 32). This is what moves the executive dashboard during a fault; bare KPI-name strings are ignored. When replacing a channel, **copy `business_impact` from the old channel if present** rather than dropping it. |

### `investigation_notes` shape

5–6 numbered steps. Should reference:
- Specific log field names (always `body.text`, never `body` alone)
- Specific metric names from the scenario's service telemetry
- `{placeholder}` values from `error_message` to anchor searches
- Only `{placeholder}` names that are `fault_params` keys — the KB doc, Wiki runbook and channel runbook skill replace each one with the parameter's range, and any other `{name}` is shown literally (verifier Check 35). Shell syntax such as curl's `%{time_total}` is fine
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

## Schema 1.3 fields

The full reference (shapes, the `opsctl` verbs and matching rule, `remediation_effects`, `k8s_events`, `state_keys`, runtime placeholders) is §Schema 1.3 fields in [../add-scenario/contract/channels.md](../add-scenario/contract/channels.md). The short version:

```yaml
remediation_command: |-
  opsctl rollback configmap/ingress-nginx-controller -n ingress-nginx --to-revision previous
fault_scope:
  cluster: prod-use1
  node_pool: edge-c6i
onset_delay_seconds: 60
ramp_seconds: 180
infrastructure_events:
  - service: kube-apiserver
    severity: INFO
    offset_seconds: -90          # the cause, logged before the first error
    body: 'audit: level=RequestResponse ... verb=update resource=configmaps namespace=ingress-nginx name=ingress-nginx-controller ...'
remediation_effects:
  transition_seconds: 120
  log_events:
    - {service: ingress-nginx, severity: INFO, offset_seconds: 8, body: '"Backend successfully reloaded" ...'}
  steady_attributes:
    ingress-nginx: {nginx.upstream_keepalive_connections: 320}
  # state: {kubernetes_otel: {workloads: {<name>: {memory_limit: 3Gi}}}}   # only when a value changes (ch 13)
  verification:
    - signal: kernel conntrack drops on edge-c6i
      where: 'resource.attributes.service.name == "kubelet" AND body.text LIKE "*nf_conntrack: table full*"'
      expect: '0 new lines'
      within_seconds: 180
```

`remediation_effects` apply when the channel is resolved by `opsctl`, by the remediation workflow or from the Chaos page (not on auto-expiry). `verification` becomes "Recovery is confirmed when" in the Wiki runbook.

### Remediation and signal at 1.3

- **Verbs come from the ops profile.** Each `remediation_command` verb must exist in the scenario's profile: the presets named in `scenarios/<id>/ops.yaml` `extends` plus the file's own `verbs` (Check 37). If none reads naturally, add the verb to `ops.yaml` with `summary` and `example` (Check 45 requires both). Contract: AGENTS.MD "Ops profile", `app/ops_profile.py`.
- **One `{actor}` line per command line.** `remediation_effects.log_events` carries, for each command line, one line naming `{actor}` that satisfies the profile's audit format `match` (body starts with `prefix`, contains every rendered `tokens` entry: the target for the neutral and space formats, `resource=` / `namespace=` / `name=` for kube audit, `eventName=` for CloudTrail). That line stands in for the command's audit record, so the fix leaves exactly one record per command. Example (space, audit on mission-control, `match: {tokens: ["{target}"]}`):

  ```yaml
  remediation_command: |-
    opsctl rollback firmware/pms-reg-a --to-revision previous
  remediation_effects:
    log_events:
      - service: mission-control
        severity: INFO
        offset_seconds: 1
        body: '[MCC] CMD ACCEPTED opsctl rollback firmware/pms-reg-a --to-revision previous from {actor}: REG-A controller fw-4.2.0-rc1 -> fw-4.1.7'
  ```
- **Verification signals must be emitted.** A `verification` entry on `metrics-generic` names a `metrics.<name>` some service's `metric` step emits; a log `attributes.<key>` is set by a service step, `steady_attributes`, `rca_clues` or a `log_events` line. Check 41 warns when nothing emits it.
- **The signal service has a during-fault and an after-fix branch.** The service emitting the signal metric reads it out of band while `N in all_active` (ramped with `elapsed`), converging back while `N in fixed_channels` (with `since_fix`), baseline otherwise, and puts the fix's configuration on its status log keyed on `fixed_channels` or `applied`. See §Before / during / after in [../add-scenario/contract/services.md](../add-scenario/contract/services.md). When replacing channel N, the service's existing branches for N belong to the old channel: rewrite them.

Fault logs carry `error.type`, the channel's `rca_clues` as log attributes and, with a stack trace, `exception.*`. They no longer carry `chaos.*`, `ops.*`, `system.*` or `cascade.*` fields or an `event_name` blob; never add such attributes.

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

- Every `{name}` in `error_message` **and** `stack_trace` must be a key here, except the runtime placeholders the engine fills (`{self_*}`, `{node_count}`, `{cluster_name}`, `{pool_<name>_count}`, `{pool_<name>_others_count}`, `{klog_time}`, `{nginx_time}`, `{iso_now}`, `{epoch_now}`)
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
- They are attached as log attributes to the ERROR lines of that service (and to spans), so name them as the component would (`nf_conntrack.count`, `k8s.container.memory_limit`), never `chaos.*`
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
- [ ] Schema 1.3 scenario: `remediation_command`, `fault_scope`, a cause event with negative `offset_seconds`, and `remediation_effects.verification` present; `state` keys are in the generator's `SPEC.state_keys`; Checks 37-44 clean
- [ ] Schema 1.3 scenario: every `remediation_command` verb is in the ops profile (or added to `ops.yaml` with `summary` + `example`); one `{actor}` `log_events` line per command line satisfying the audit `match`; verification signals emitted by a service (no Check 41 warnings); the signal service has during-fault and after-fix branches for N; Check 45 clean
- [ ] Realism checklist (`../add-scenario/BRIEF.md`) passes: real log format, `error_type` is a unique quote-free substring of `error_message`, no demo wording, no `chaos.*` attributes
- [ ] `python3 scripts/verify_yaml_scenarios.py <id>` passes
- [ ] `git diff --name-only` shows only files under `scenarios/<id>/`
