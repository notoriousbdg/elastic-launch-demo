# Scenario YAML contract — service files

Use this as a generation checklist when authoring `services/<svc>.yaml` files.

---

## Services

**All per-service data lives in `services/<svc>.yaml` — `scenario.yaml` carries no service blocks.**

`YamlScenario` auto-discovers `services/*.yaml`, reads `sort_order` from each file to preserve insertion order, and assembles `scenario.services`, `service_topology`, `entry_endpoints`, and `db_operations` at load time.

Rules:
- Exactly 9 service files; exactly 3 per cloud.
- File names use kebab-case matching the `service:` key inside the file (e.g. `payment-processor.yaml`).
- `sort_order: 1`–`9` — controls iteration order (first service gets 4× trace-entry weighting; controls exec-dashboard tile order).
- Language must be one of: `python`, `java`, `go`, `dotnet`, `rust`, `cpp`.

---

## Service telemetry DSL

Each service's nominal-state telemetry lives in `scenarios/<id>/services/<svc>.yaml` — one file per service key, interpreted by `app/services/telemetry_dsl.py`.

### Top-level structure

```yaml
service: payment-processor        # kebab-case key; filename must match (payment-processor.yaml)
sort_order: 7                      # 1–9; controls iteration/trace-entry order
# ── Resource identity ──────────────────────────────────────────────────────
cloud_provider: azure              # aws | gcp | azure
cloud_region: eastus
cloud_platform: azure_vm           # aws_ec2 | gcp_compute_engine | azure_vm
cloud_availability_zone: eastus-1
subsystem: payments                # short functional area
language: java                     # python | java | go | dotnet | rust | cpp
generates_traces: true             # optional; false for infra-only services (no trace spans)
# ── Topology ───────────────────────────────────────────────────────────────
topology:                          # outbound calls this service makes. Every callee MUST be a defined
                                   # service (services/<callee>.yaml) - never an external system like a card
                                   # network; model those as db_operations or log attrs (Check 10b)
  - [fraud-detector, /api/v1/fraud/check, POST]
  - [settlement-processor, /api/v1/settle, POST]
entry_endpoints:                   # inbound API endpoints (seeds root trace spans). Give EVERY traced
                                   # (non-database) service these: a fault channel's affected_services must
                                   # include an entry service or the ops dashboard never reacts (Check 31)
  - [/api/v1/payment/charge, POST]
  - [/api/v1/payment/status, GET]
db_operations:                     # DB calls (seeds DB trace spans)
  - [SELECT, transactions, "SELECT id, amount, status FROM transactions WHERE id = ?"]
  - [INSERT, transactions, "INSERT INTO transactions (id, amount, ...) VALUES (...)"]
# ── Telemetry DSL ──────────────────────────────────────────────────────────
emit_fault_logs: true              # default true; false only for cascade-only services
kpi_emitter: false                 # true for the executive_kpi_emitter_service_name service

constants:                         # immutable per-instance lists/dicts referenced by steps
  STATUSES: [OK, WARN, ERROR]
  SENSORS:
    - {key: cpu_temp, unit: degC, lo: 40.0, hi: 85.0, metric: hw.cpu_temp}

state:                             # mutable per-instance variables, initialized once
  _counter: {init: 0}              # integer/float starting value
  _last_report: {init: now}        # timestamp (for `every` throttling)
  _idx: {init: 0}                  # round-robin index

steps:                             # ordered list; executed once per generate_telemetry() call
  - ...
```

### Step types

| Step | Purpose |
|---|---|
| `sample` | Resolve value specs into named variables for use later in this cycle |
| `metric` | Emit one metric via `emit_metric(name, value, unit)` |
| `log` | Emit one log record via `emit_log(level, body, attrs)` |
| `for_each` | Iterate over a constant list; inner steps run once per element |
| `every` | Time-throttled block: inner steps run only when `now - state[key] > seconds` |
| `counter` | Increment a state variable by a value spec |
| `incr_key` | Increment a per-key counter within a state dict |

### Value specs

| Value spec | Resolves to |
|---|---|
| `{var: name}` | Current value of sample var or state key `name` |
| `{expr: "python_expr"}` | Sandboxed Python eval; env includes all vars, state, constants, `rand`, `active`, `all_active`, `fixed_channels` (see Realism additions) |
| `{if_active: spec, else: spec}` | First spec during a fault, second spec nominally |
| `{choice: [a, b, c]}` | `random.choice` — list literal or name of a constant list |
| `{randint: [lo, hi]}` | `random.randint(lo, hi)` |
| `{uniform: [lo, hi], round: n}` | `round(random.uniform(lo, hi), n)` |
| `{gauss: [mu, sigma], clamp: [lo, hi], round: n}` | Clamped Gaussian |
| `{format: "tmpl", k: spec}` | f-string template with named sub-specs |
| scalar | Literal string / int / float / bool |

State init specs: `{init: 0}`, `{init: now}`, `{init_per_key: {keys_from: CONST, spec: value_spec}}`.

### Realism additions (schema 1.3)

Reference: `scenarios/k8splatform/services/` (`kubelet.yaml`, `ingress-nginx.yaml`, `kube-apiserver.yaml`).

- **`when: "<expr>"` on any step.** The step runs only when the expression is true, e.g. `when: "13 in all_active"` to log extra reload lines while channel 13's fault is active, or `when: "20 not in all_active"`.
- **`scope:` on a `log` step.** Narrows the pod or node the record comes from, with the same keys as a channel's `fault_scope`.
- **`log_identity: pod | node | cluster`** (top level, default `pod`). With a pinned `k8s_topology`, each record takes the resource identity of a pod of the workload mapped to the service; `node` services (kubelet, kernel, journald) rotate across nodes; `cluster` services (a managed control plane such as the EKS API server) carry only the cluster and cloud attributes. `log_identity_pools: [pool, ...]` limits `node` services to some pools outside a fault.
- **Expression environment.** Besides `active` / `active_channels` (faults affecting this service): `all_active` (every active channel), `fixed_channels` (channels whose `remediation_effects` were applied, so a service can render post-fix values), `elapsed` (`{ch: seconds since trigger}`), `since_fix` (`{ch: seconds since the fix landed}`), `applied` (`{"kind/name": {flag: value, "_verb": verb}}`, the latest applied opsctl flags per target, for any command, fix or not), `pod_ips` (`{workload: [ip, ...]}`), `node_names`, and timestamps `klog_time`, `iso_now`, `epoch_now`. Log messages may also use the runtime placeholders listed in channels.md (`{self_node}`, `{self_pod_ip}`, `{node_count}`, `{klog_time}`, `{nginx_time}`...).
- **Before / during / after.** See the next section.
- **Real log formats.** Write each line in the component's own format (klog, nginx, CoreDNS, logfmt, JSON) and no demo bookkeeping attributes. `with_remediation_meta` is still accepted on older specs but no longer stamps anything.

### Before / during / after

Each channel's signal metric reads three ways, fault first, so Nightshift sees the deviation (to find the fault) and the convergence (to confirm the fix). A 1.2-style service that only reads `active` looks the same in all three phases.

| Phase | Condition | Value |
|---|---|---|
| During | `N in all_active` | out of band, ramped in with `min(1.0, elapsed.get(N, 0) / <ramp_s>)` |
| After | `N in fixed_channels` | converging from the fault value to baseline with `min(1.0, since_fix.get(N, 0) / <recovery_s>)` |
| Before | otherwise | baseline |

The configuration the fix changes (active unit, firmware revision, mode) goes on the subsystem's status log as an attribute keyed on `fixed_channels`, or on `applied` when it should follow the opsctl command itself (a wrong unit selected shows the wrong unit). The channel's `remediation_effects.verification` names that metric (`from: metrics-generic.otel-default`, `field: metrics.<name>`) and attribute (`from: logs.otel.<ns>`); Check 41 warns when no service emits them.

Example (after `scenarios/space/services/fuel-system.yaml`, channel 2):

```yaml
- sample:
    lox_pressure_val:
      expr: "round(265.0 + (rand.uniform(150.0, 195.0) - 265.0) * min(1.0, elapsed.get(2, 0) / 45.0), 2) if 2 in all_active
        else (round(172.0 + (rand.uniform(250.0, 280.0) - 172.0) * min(1.0, since_fix.get(2, 0) / 60.0), 2) if 2 in fixed_channels
        else round(rand.uniform(245.0, 285.0), 2)"
    lox_status:
      expr: "'ANOMALY' if 2 in all_active or lox_pressure_val < 240.0 else 'NOMINAL'"
    reg_a_fw:
      expr: "'fw-4.2.0-rc1' if 2 in all_active else 'fw-4.1.7'"
- metric:
    name: fuel_system.lox_pressure
    value: {var: lox_pressure_val}
    unit: PSI
- log:
    level: INFO
    message: '[PMS] sensor=lox_pressure tank=LOX-1 reading={lox_pressure_val}PSI firmware={reg_a_fw} status={lox_status}'
    attrs:
      sensor.status: {var: lox_status}
      infra.firmware_rev: {var: reg_a_fw}
```

To follow the command rather than the fix (for example which redundant unit is selected): `expr: "applied.get('link/x-band', {}).get('active', 'xb-primary')"`.

### Example: request service

```yaml
service: payment-processor
sort_order: 7
cloud_provider: azure
cloud_region: eastus
cloud_platform: azure_vm
cloud_availability_zone: eastus-1
subsystem: payments
language: java
topology:
  - [fraud-detector, /api/v1/fraud/check, POST]
entry_endpoints:
  - [/api/v1/payment/charge, POST]
db_operations:
  - [SELECT, transactions, "SELECT id, amount FROM transactions WHERE id = ?"]
emit_fault_logs: true
kpi_emitter: false

constants:
  PROVIDERS: [stripe, braintree, adyen]

state:
  _tx_count: {init: 0}
  _last_summary: {init: now}

steps:
  - sample:
      provider: {choice: PROVIDERS}
      latency_ms:
        if_active: {uniform: [500.0, 3000.0], round: 1}
        else: {uniform: [20.0, 150.0], round: 1}
      tx_id: {format: "TX-{n}", n: {randint: [100000, 999999]}}
  - counter:
      state_key: _tx_count
      by: 1
  - log:
      level: INFO
      message: "[PAY] charge provider={provider} latency={latency_ms}ms tx={tx_id} status=OK"
      attrs:
        operation: charge
        payment.provider: {var: provider}
        payment.latency_ms: {var: latency_ms}
        payment.tx_id: {var: tx_id}
  - metric:
      name: payment_processor.latency_ms
      value: {var: latency_ms}
      unit: ms
  - every:
      seconds: 30
      state_key: _last_summary
      steps:
        - log:
            level: INFO
            message: "[PAY] summary total_tx={_tx_count} status=NOMINAL"
            attrs:
              operation: summary
              payment.total_tx: {var: _tx_count}
```

### `for_each` step

```yaml
- for_each:
    in: SENSORS          # constant list name
    as: s                # loop variable name
    steps:
      - metric:
          name: {expr: "f'hw.{s[\"key\"]}'"}
          value: {expr: "round(rand.gauss(s['lo'] + (s['hi']-s['lo'])/2, (s['hi']-s['lo'])/6), 2)"}
          unit: {expr: "s['unit']"}
```

### `incr_key` step

The state key **must be a dict** — initialise it with `{init: {}}` or `init_per_key`. `{init: 0}` (a scalar, as
used by `counter`) crashes the service loop on every tick with `AttributeError: 'int' object has no attribute 'get'`
(Check 33).

```yaml
state:
  _refunds: {init: {}}                      # per-key counter that starts empty
  _seq: {init_per_key: {keys_from: STREAMS, spec: {randint: [1000000, 5000000]}}}

steps:
  - sample:
      stream: {choice: STREAMS}
  - incr_key:
      state_key: _seq
      key: {var: stream}
      by: 1
```

---

## Trace attributes (per-service portion)

```yaml
trace_attributes:
  services:
    <service-key>:
      service.attribute: {choice: [val1, val2]}
      service.count: {randint: [1, 100]}
      service.rate: {uniform: [0.0, 100.0], round: 1}
    # all 9 services should appear, 4–5 attributes each
```

---

## YAML DSL reference (trace_attributes)

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
