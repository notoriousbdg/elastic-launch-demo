# Scenario YAML contract — channel files

Use this as a generation checklist when authoring `channels/NN-<slug>.yaml` files.

---

## Channel files — one per fault channel

**Filename:** `channels/NN-<slug>.yaml` where `NN` is zero-padded channel number (01–20).

**CRITICAL parity rule:** Every `{placeholder}` name in `error_message` and `stack_trace` must appear as a key in `fault_params` **in the same file**. Parity is locally visible — no need to chase across files.

```yaml
name: Payment gateway timeouts on checkout
subsystem: payments
vehicle_section: checkout_pipeline
error_type: 'context deadline exceeded (Client.Timeout exceeded while awaiting headers)'  # natural substring of error_message; no quotes inside; unique per channel
sensor_type: gateway_latency
affected_services: [payment-processor, order-management]   # Direct fault targets
cascade_services: [storefront-gateway]                     # Downstream victims
description: Payment provider calls time out during checkout after a client SDK upgrade.
investigation_notes: |
  Root cause: ... (one cause, naming the change that caused it)
  Confirm:
    - ... (log lines, attributes, metrics and the change event to check)
  Remediation: ... (what the fix does and why the symptom fix is wrong)
remediation_action: rollback_payment_sdk      # snake_case action identifier
remediation_command: |-                       # schema 1.3: one opsctl command per line
  opsctl rollback deployment/payment-processor -n payments
error_message: 'level=error msg="charge failed" provider={payment_provider} timeout_ms={timeout_ms} err="context deadline exceeded (Client.Timeout exceeded while awaiting headers)"'
stack_trace: ''                               # empty unless the component really logs one
correlation_attr:
  key: deployment.gateway_sdk_version     # attribute correlated with errors for this channel
  value: payments-sdk-v14.2.0
fault_params:
  # One key per {placeholder} in error_message + stack_trace above — parity is LOCAL
  # (runtime placeholders such as {self_pod} or {klog_time} need no key)
  payment_provider: {choice: [stripe, braintree, adyen]}
  timeout_ms: {randint: [3000, 30000]}
rca_clues:
  # Partial clues per service (2–3 attrs each, no single service has the full picture).
  # They are attached to the ERROR log lines as attributes (and to spans).
  payment-processor:
    gateway.retry_count: {randint: [3, 10]}
    gateway.last_success_s_ago: {randint: [30, 300]}
  order-management:
    order.pending_count: {randint: [50, 500]}
    order.stuck_at_payment: true
```

The schema 1.3 fields (`fault_scope`, `cascade_messages`, `remediation_effects`, ...) are described in §Schema 1.3 fields below. Full references: `scenarios/k8splatform/channels/02-edge-conntrack-exhaustion.yaml` (rollback fix) and `13-ingress-oom-reload-storm.yaml` (fix that sets a new value).

Channel registry rules:
- Exactly 20 channels, files `01-*.yaml` through `20-*.yaml`.
- Every value in `affected_services` and `cascade_services` must be a key in `services`.
- `investigation_notes`: either 5–6 numbered steps, or the k8splatform shape (`Root cause:` / `Confirm:` bullets / `Remediation:`). Reference specific field names and `{placeholder}` values, name the change event that preceded the errors, and give the healthy comparison outside `fault_scope`.
- Channels 1–15: HITL faults. Channels 16–20: auto-remediate only.
- `investigation_notes` placeholders must be `fault_params` keys (verifier Check 35). The KB doc, Wiki runbook and channel runbook skill replace each one with the parameter's range; any other `{name}` shows as literal text. Shell syntax such as curl's `%{time_total}` is fine.

### Evidence contract

`rca_clues` are what investigations (the Agent Builder agent and Nightshift) use to confirm the cause. The engine attaches them as log attributes on the ERROR lines (and the spans carry them) only while the fault is active, and the KB doc and Wiki runbook list each clue with its range. So:

- **One cause.** Notes often name two or three candidate causes. Pick the one the frame dump supports, give it clues that confirm it, and give the alternatives clues at nominal values that rule them out.
- **Beyond the threshold.** Every clue range sits wholly on the fault side of the threshold the notes give (">3.5A indicates binding" → `{uniform: [3.6, 4.5]}`, never `[2.5, 4.0]`).
- **No healthy choices.** A `choice` clue lists fault values only — never `OFF`, `INTACT`, `false` or another healthy reading (Check 36 warns).
- **Pin the faulty unit.** If the `stack_trace` lists sibling units (tanks, radars, zones, buses) as NOMINAL, the fault param that names the faulty unit must not be able to pick one of them (Check 36 warns).
- **Dump agrees with spans.** Readings in the frame dump that the notes rely on are `{fault_params}` placeholders with the same range as the clue, not fixed healthy-looking numbers.
- **Every reading has a clue.** Each reading the notes tell an investigator to check has a clue on the service that would report it, or the notes give the threshold the clue is judged against.

---

## Schema 1.3 fields

New scenarios are generated at schema 1.3 (`schema_version: "1.3"`). Checks 37-44 of `scripts/verify_yaml_scenarios.py` run only for schema >= 1.3. All fields below are read through the channel registry in `scenario_engine/yaml_scenario.py`; the engine behaviour is in `app/services/base_service.py`, `app/chaos/controller.py` and `app/ops.py`. Follow the Realism checklist in [BRIEF.md](../BRIEF.md).

| Field | Required at 1.3 | Shape | What it does |
|---|---|---|---|
| `remediation_command` | ✅ (Check 37) | block string, one `opsctl <verb> <kind>[/<name>] [-n ns] [--flag value]` per line | The fix. It is written into the Wiki runbook as a code block. When `opsctl` (platform-ops skill) runs every line while the channel is active, the channel resolves and its `remediation_effects` apply. Verbs: `rollback`, `restore`, `renew`, `rotate`, `sync`, `drain`, `replace`, `patch`, `apply`, `expand`, `run`, `delete`, `restart` (`app.ops.VERBS`). A run matches a line when verb, target and namespace are equal and every flag on the line is present with an equal value. Fix the root cause, not the symptom. |
| `fault_scope` | warning if absent (Check 39) | `{cluster, node_pool, nodes, az, workloads, namespaces}` | Where the fault lives. With a pinned `k8s_topology`, fault logs come from pods or nodes inside the scope, `kubernetes_otel` and `host_metrics` degrade only those entities, and the runbook states the scope as a fact. Every name must exist in `k8s_topology` (nodes may also be added by the channel's own state). Without `k8s_topology`, use only `cluster` and `workloads`. |
| `infrastructure_events[].offset_seconds` | Check 42 warns without a preceding event | number | Negative: the record is backdated relative to the trigger (the change that caused the fault). Positive: the record is held back until that many seconds after the trigger. Events fire from every service a triggered channel names, before the symptoms start. Bodies may use runtime placeholders. |
| `onset_delay_seconds` | optional | number | Seconds after the trigger before the error lines, cascade lines and generator impacts start. |
| `ramp_seconds` | optional | number | `kubernetes_otel` ramps the impact in over this many seconds after onset. |
| `cascade_messages` | optional (Check 40) | `{service: [str or {severity, message}]}` | Per-service lines a real dependent logs, instead of the neutral fallback. Keys must be cascade services, or affected services outside `error_services`. Placeholders must be `fault_params` keys or runtime placeholders. |
| `error_services` | optional (Check 40) | list, subset of `affected_services` | Only these services emit `error_message`; the other affected services (typically the API gateway whose requests fail as a result) emit their `cascade_messages` lines instead. |
| `k8s_events` | optional (Check 43) | list of `{type, reason, object: {kind, name, namespace}, message, component, every_seconds, per_object}` | Kubernetes events that `kubernetes_otel` emits for pods or nodes in scope while the fault is active (pinned-topology scenarios). `message` and `object.name` may use `{self_pod}`, `{self_node}`, `{node_count}`, `{scope_node_count}`, `{other_node_count}`, `{pool_<name>_count}`. Pending pods need a reason here or in `cascade_messages` (Check 43). |
| `fault_state` | optional (Check 38) | `{<generator_id>: {<state_key>: ...}}` | Config values that differ while the fault is active (a new image, a node pool that was just added). Applied once onset is reached; keys must be in that generator's `SPEC.state_keys`. |
| `remediation_effects` | ✅ `verification` (Check 41) | see below | The after-state the fix leaves. |

`remediation_effects` keys:

```yaml
remediation_effects:
  transition_seconds: 120            # state changes play out over this time (node joins, rolling updates)
  state:                             # only when the fix changes a value a generator reports (Check 38)
    kubernetes_otel:
      workloads:
        ingress-nginx-controller: {memory_limit: 3Gi, memory_util: [0.12, 0.18]}
  log_events:                        # one-shot lines the fix leaves (audit record, reload line)
    - service: kube-apiserver
      severity: INFO
      offset_seconds: 6              # after the fix
      body: 'audit: level=Metadata verb=patch resource=deployments ... user=opsctl/claude-code ...'
  steady_attributes:                 # attributes every later log line of that service carries
    ingress-nginx:
      nginx.upstream_keepalive_connections: 320
  verification:                      # 2-4 signals; rendered as "Recovery is confirmed when" in the runbook
    - signal: k8s.container.memory_limit
      where: 'resource.attributes.k8s.deployment.name == "ingress-nginx-controller"'
      expect: '3221225472 (3Gi) on every controller pod'
      within_seconds: 150
```

The effects apply on every resolve path except auto-expiry: `opsctl`, the remediation workflow and the Chaos page resolve button. Triggering the channel again undoes its earlier fix, so the fault repeats from the same baseline. `POST /api/ops/reset` forgets every applied fix.

`state` keys: read `SPEC.state_keys` in `generators/<gen_id>/__init__.py` (`--describe` does not list them). Today `kubernetes_otel` declares `workloads` (image, replicas, cpu/memory requests and limits, cpu_util, memory_util, hpa_max, hpa_target_cpu, pvc_capacity, pvc_used, node_pool, placement, tolerations), `node_pools` (instance_types, add_nodes, kubelet_version + upgraded_count, kubelet_resolv_conf, cordoned), `remove_nodes` and `cluster`. Workload and pool names must exist in `k8s_topology`.

### Runtime placeholders

`error_message`, `stack_trace`, `cascade_messages`, `infrastructure_events[].body` and `remediation_effects.log_events[].body` may use these without a `fault_params` key (Checks 9 and 40 accept them):

| Placeholder | Value |
|---|---|
| `{self_node}`, `{self_node_ip}`, `{self_az}`, `{self_instance_type}`, `{self_instance_id}`, `{self_cluster}` | The node the record comes from (pinned topology) |
| `{self_pod}`, `{self_pod_ip}` | The pod the record comes from (pinned topology) |
| `{node_count}`, `{cluster_name}` | Live node count and name of the cluster (after fixes and fault state) |
| `{pool_<name>_count}`, `{pool_<name>_others_count}` | Nodes in a pool, and nodes in all other pools (dashes in the pool name become underscores: `{pool_edge_c6i_count}`) |
| `{klog_time}` | `MMDD HH:MM:SS.ffffff`, for klog lines (`I{klog_time}    1 file.go:12] "..."`) |
| `{nginx_time}` | `YYYY/MM/DD HH:MM:SS`, for the nginx error log |
| `{iso_now}`, `{epoch_now}` | ISO-8601 with milliseconds, and epoch seconds |

The `self_*` and count placeholders resolve only in scenarios with `k8s_topology`; the time placeholders work everywhere. A backdated event's time placeholders use the event's own timestamp.

### What the engine no longer adds

Fault logs carry `error.type`, the channel's `rca_clues` for that service as attributes, and `exception.*` only when `stack_trace` is non-empty. The engine no longer stamps `chaos.*`, `ops.*`, `system.*`, `cascade.*`, `sensor.type`, `vehicle_section` or an `event_name` JSON blob. Do not add such fields yourself (GUARDRAILS §14).

---

## Generator impact

One channel per selected integration must have a `generator_impact` block. This is what makes the generator breach its alert threshold when the channel is active.

First, look up the valid impact keys and breach ranges for the generator:

```bash
python3 -m generator_engine.registry --describe <gen_id>
```

Then add the block to the dedicated channel, using values inside the breach range:

```yaml
generator_impact:
  redis_otel:
    high_eviction_rate: 350.0
    memory_approaching_maxmemory: 95.0
```

Rules:
- The generator id (`redis_otel`) must match a key in `scenario.yaml`'s `generators:` block.
- Each `impact_key` must be a key reported by `--describe` for that generator.
- Values must be inside the breach range (not merely elevated) — otherwise the alert never fires.
- Prune the scaffolded stub to 1–3 keys that fit the fault's narrative; don't keep all seeded keys.

Two reference patterns in the repo:
- `scenarios/ecommerce/channels/10-catalog-redis-purge-storm.yaml` — multi-key, rich RCA story.
- `scenarios/gt-*/channels/*.yaml` — single-key, minimal (coverage testing; don't copy their sparse style).

---

## Dashboard visibility (ops + executive) — required for every channel

A channel that does not turn dashboard tiles yellow/red is a broken channel. Both
dashboards must react, **in proportion to the fault's scope**. Reference:
`scenarios/space/channels/01-thermal-calibration-drift.yaml` + `scenarios/space/services/fuel-system.yaml`.
`verify_yaml_scenarios.py` Checks 30–33 enforce all of this.

**Scope** = `2 × len(affected_services) + len(cascade_services)`.

| Tier | Scope | KPI tiles that must be yellow/red | of which red |
|---|---|---|---|
| minor | ≤ 3 | ≥ 2 | ≥ 1 |
| moderate | 4–5 | ≥ 3 | ≥ 2 |
| major | ≥ 6 | ≥ 4 | ≥ 3 |

### Ops dashboard — error spans (Check 31)

The per-service tiles are `error spans / spans` (green < 12 %, yellow 12–20 %, red > 20 %).
The trace generator only raises the error rate when a trace's **entry service** is in `affected_services`
(entry service = `sort_order: 1` service, or any service with `entry_endpoints`; database services and
`generates_traces: false` services never emit spans). So:

- `affected_services` must contain **at least one entry service** (an API/worker that handles requests) —
  never only a database. A database fault lists the DB **and** the service that calls it
  (e.g. `affected_services: [product-catalog-db, order-service]`).
- Give every traced service `entry_endpoints` so any of them can be a fault target (the scaffolder does this).
- `cascade_services` are downstream victims: they get a lower error rate (yellow tiles). Do not repeat an
  affected service there.
- Schema 1.3 exception: a channel with a `fault_scope` may leave the entry service out (an infrastructure fault
  scoped to nodes or platform components can leave the tenant-facing API healthy). Check 31 is then a warning,
  not an error, and the ops dashboard stays green for that channel; `business_impact` still has to move the
  executive dashboard.

### Executive dashboard — `business_impact` (Checks 30, 32)

Every channel needs a `business_impact` list (dict entries only — bare KPI-name strings are ignored at runtime).
Each entry targets a `name:` from `executive_kpi_emissions`. Tile colour comes from each KPI's own nominal range and
its `better:` direction, so:

```yaml
# scenario.yaml
executive_kpi_emissions:
  - name: business.checkout_abandonment_rate
    value: {uniform: [15.0, 35.0], round: 2}
    better: lower            # REQUIRED for any KPI used by a business_impact entry
    unit: '%'
# channels/NN-*.yaml
business_impact:
  - kpi: business.checkout_abandonment_rate
    value: {uniform: [58.0, 66.0], round: 2}   # lands in the RED band (above hi + 30% of span)
  - kpi: business.cart_conversion_rate
    value: {uniform: [0.5, 0.8], round: 2}      # lands in the YELLOW/RED band (below lo - 5% / 30% of span)
```

Rules:
- Declare `better: lower|higher` on every KPI that a channel impacts (lower = errors, latency, abandonment,
  backlog, incidents; higher = revenue, throughput, uptime, success rates). Don't rely on the name-keyword fallback.
- Bands (`kpi_band_edges()` in `elastic_config/dashboards/generate_exec_dashboard.py`): lower-is-better → yellow above
  `hi + 5 % span`, red above `hi + 30 % span`; higher-is-better → yellow below `lo − 5 % span`, red below `lo − 30 % span`
  (floored at 0.8·lo / 0.5·lo for wide ranges).
- Prefer `value:` (absolute) for the red/yellow KPIs — a `factor:` is multiplied into a random nominal draw and can
  stay inside the green band. Use the `value:` ranges inside the red/yellow band, ~±7 % wide.
- Keep nominal ranges **narrow** (e.g. 92–99.8, not 50 000–250 000): a wide range makes every plausible faulted
  value look "nominal".
- Higher-is-better KPIs whose nominal range includes 0 or negatives (e.g. NPS −8…68) can never turn red — don't
  impact them.
- Strongest single deviation wins when several active channels hit the same KPI.
- Run `python3 scripts/retune_business_impact.py <id>` (dry run) to see what the verifier would change; `--apply`
  rewrites failing channels in place.

## YAML DSL reference (fault_params, rca_clues)

| Python expression | YAML DSL |
|---|---|
| `random.choice(["a","b","c"])` | `{choice: [a, b, c]}` |
| `rng.choice([100, 200, 300])` | `{choice: [100, 200, 300]}` |
| `random.randint(1, 100)` | `{randint: [1, 100]}` |
| `round(random.uniform(80.0, 99.5), 1)` | `{uniform: [80.0, 99.5], round: 1}` |
| `random.uniform(0.0, 1.0)` | `{uniform: [0.0, 1.0]}` |
| `random.random()` | `{random: true}` |
| `f"ORD-{random.randint(1000,9999)}"` | `{format: "ORD-{n}", n: {randint: [1000, 9999]}}` |
| `f"{a:02x}:{b:02x}:..."` with randint | `{format: "{a:02x}:{b:02x}:...", a: {randint:[0,255]}, ...}` |
| `f"10.{x}.{y}.{z}"` with randint | `{format: "10.{x}.{y}.{z}", x: {randint:[0,255]}, ...}` |
| `f"{n:X}"` with randint | `{format: "{n:X}", n: {randint:[100000,999999]}}` |
| `True` / `False` | `true` / `false` |
| `"plain string"` | `plain string` |
