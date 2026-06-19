# Scenario YAML contract

Use this as a generation checklist during Phase 5. Work top-to-bottom and verify every property is present and correctly shaped.

The runtime reads scenario data from three sources:
- `scenarios/<id>/scenario.yaml` — scenario-level properties (identity, infra, agent, KPIs, trace attrs)
- `scenarios/<id>/channels/NN-<slug>.yaml` — one file per fault channel
- `scenarios/<id>/services/<svc>.yaml` — one file per service (identity, topology, telemetry DSL)

Reference implementation: [`scenarios/yaml_scenario.py`](scenarios/yaml_scenario.py)

---

## Identity properties

| Property | Required | Type | Notes |
|---|---|---|---|
| `scenario_id` | ✅ | `str` | Lowercase slug. E.g. `"logistics"` |
| `scenario_name` | ✅ | `str` | Display name. E.g. `"Global Logistics Platform"` |
| `scenario_description` | ✅ | `str` | 2–3 sentence card blurb. |
| `namespace` | ✅ | `str` | Short telemetry prefix. Used in index names: `logs.otel.<ns>`. |
| `scenario_icon` | optional | `str` | Emoji. Default `"🔧"` |
| `sort_order` | optional | `int` | Lower = earlier. Default 999. Use 10–99 for new scenarios. |
| `nominal_label` | optional | `str` | Label for the "nominal" state. Default `"NORMAL"`. |

---

## Services

**All per-service data lives in `services/<svc>.yaml` — `scenario.yaml` carries no service blocks.**

`YamlScenario` auto-discovers `services/*.yaml`, reads `sort_order` from each file to preserve insertion order, and assembles `scenario.services`, `service_topology`, `entry_endpoints`, and `db_operations` at load time. See **Service telemetry DSL** below for the full per-service file schema.

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
# ── Resource identity (was scenario.yaml services: block) ──────────────────
cloud_provider: azure              # aws | gcp | azure
cloud_region: eastus
cloud_platform: azure_vm           # aws_ec2 | gcp_compute_engine | azure_vm
cloud_availability_zone: eastus-1
subsystem: payments                # short functional area
language: java                     # python | java | go | dotnet | rust | cpp
generates_traces: true             # optional; false for infra-only services (no trace spans)
# ── Topology (was scenario.yaml service_topology / entry_endpoints / db_operations) ──
topology:                          # outbound calls this service makes
  - [fraud-detector, /api/v1/fraud/check, POST]
  - [settlement-processor, /api/v1/settle, POST]
entry_endpoints:                   # inbound API endpoints (seeds root trace spans)
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
| `{expr: "python_expr"}` | Sandboxed Python eval; env includes all vars, state, constants, `rand`, `active` |
| `{if_active: spec, else: spec}` | First spec during a fault, second spec nominally |
| `{choice: [a, b, c]}` | `random.choice` — list literal or name of a constant list |
| `{randint: [lo, hi]}` | `random.randint(lo, hi)` |
| `{uniform: [lo, hi], round: n}` | `round(random.uniform(lo, hi), n)` |
| `{gauss: [mu, sigma], clamp: [lo, hi], round: n}` | Clamped Gaussian |
| `{format: "tmpl", k: spec}` | f-string template with named sub-specs |
| scalar | Literal string / int / float / bool |

State init specs: `{init: 0}`, `{init: now}`, `{init_per_key: {keys_from: CONST, spec: value_spec}}`.

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

```yaml
state:
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

## Channel files — one per fault channel

**Filename:** `channels/NN-<slug>.yaml` where `NN` is zero-padded channel number (01–20).

**CRITICAL parity rule:** Every `{placeholder}` name in `error_message` and `stack_trace` must appear as a key in `fault_params` **in the same file**. Parity is locally visible — no need to chase across files.

```yaml
name: Payment Gateway Timeout
subsystem: payments
vehicle_section: checkout_pipeline
error_type: PAYMENT-GATEWAY-TIMEOUT    # ALLCAPS-HYPHENATED; appears in body.text logs
sensor_type: gateway_latency
affected_services: [payment-processor, order-management]   # Direct fault targets
cascade_services: [storefront-gateway]                     # Downstream victims
description: Payment provider repeatedly timing out during checkout.
investigation_notes: |
  1. Search body.text LIKE *PAYMENT-GATEWAY-TIMEOUT* to confirm error rate.
  2. Check recent deployment: search for deployment.gateway_sdk_version in traces.
  3. ...
remediation_action: restart_payment_gateway   # snake_case action identifier
error_message: |
  PAYMENT-GATEWAY-TIMEOUT: provider={payment_provider} timeout_ms={timeout_ms}
stack_trace: |
  TimeoutException: provider={payment_provider}
  at PaymentGateway.charge()
  timeout: {timeout_ms}ms
correlation_attr:
  key: deployment.gateway_sdk_version     # attribute correlated with errors for this channel
  value: stripe-sdk-v14.2.0-beta3
fault_params:
  # One key per {placeholder} in error_message + stack_trace above — parity is LOCAL
  payment_provider: {choice: [stripe, braintree, adyen]}
  timeout_ms: {randint: [3000, 30000]}
rca_clues:
  # Partial clues per service (2–3 attrs each, no single service has the full picture)
  payment-processor:
    gateway.retry_count: {randint: [3, 10]}
    gateway.last_success_s_ago: {randint: [30, 300]}
  order-management:
    order.pending_count: {randint: [50, 500]}
    order.stuck_at_payment: true
```

Channel registry rules:
- Exactly 20 channels, files `01-*.yaml` through `20-*.yaml`.
- Every value in `affected_services` and `cascade_services` must be a key in `services`.
- `investigation_notes`: 5–6 numbered steps referencing specific field names and `{placeholder}` values.
- Channels 1–15: HITL faults. Channels 16–20: auto-remediate only.

---

## YAML DSL reference (fault_params, rca_clues, trace_attributes)

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

---

> **Note:** `topology`, `entry_endpoints`, and `db_operations` are **per-service** fields — they belong in each `services/<svc>.yaml` file, not in `scenario.yaml`. See the **Service telemetry DSL** section above for the correct format.

---

## Infrastructure

### `hosts` — 3 hosts, one per cloud

```yaml
hosts:
  - host.name: <ns>-aws-host-01
    host.id: i-0a1b2c3d4e5f67890          # AWS instance ID
    host.arch: amd64
    host.type: m6i.2xlarge
    host.image.id: ami-0123456789abcdef0
    host.cpu.model.name: "Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz"
    host.cpu.vendor.id: GenuineIntel
    host.cpu.family: "6"
    host.cpu.model.id: "106"
    host.cpu.stepping: "6"
    host.cpu.cache.l2.size: 1310720
    host.ip: [10.0.1.100, 172.31.0.10]
    host.mac: [0e:1a:2b:3c:4d:5e]
    os.type: linux
    os.description: Amazon Linux 2023.6.20250115
    cloud.provider: aws
    cloud.platform: aws_ec2
    cloud.region: us-east-1
    cloud.availability_zone: us-east-1a
    cloud.account.id: "112233445566"
    cloud.instance.id: i-0a1b2c3d4e5f67890
    cpu_count: 8
    memory_total_bytes: 34359738368     # 32 GB
    disk_total_bytes: 536870912000      # 500 GB
  # GCP: host.id = numeric string, cloud.platform = gcp_compute_engine
  # Azure: host.id = ARM resource path, cloud.platform = azure_vm
```

### `k8s_clusters` — 3 clusters

```yaml
k8s_clusters:
  - name: <ns>-eks-cluster
    provider: aws
    platform: aws_eks
    region: us-east-1
    zones: [us-east-1a, us-east-1b, us-east-1c]
    os_description: Amazon Linux 2
    services: [<aws-svc-1>, <aws-svc-2>, <aws-svc-3>]
  - name: <ns>-gke-cluster
    provider: gcp
    platform: gcp_gke
    region: us-central1
    zones: [us-central1-a, us-central1-b, us-central1-c]
    os_description: Container-Optimized OS
    services: [<gcp-svc-1>, <gcp-svc-2>, <gcp-svc-3>]
  - name: <ns>-aks-cluster
    provider: azure
    platform: azure_aks
    region: eastus
    zones: [eastus-1, eastus-2, eastus-3]
    os_description: Ubuntu 22.04 LTS
    services: [<azure-svc-1>, <azure-svc-2>, <azure-svc-3>]
```

---

## Agent config

```yaml
agent_config:
  id: <ns>-ops-analyst
  name: <Vertical> Operations Analyst
  assessment_tool_name: <ns>_readiness_assessment   # must match assessment_tool_config.id
  system_prompt: |
    You are the <Role> for a <description>.
    You help engineering teams investigate incidents and perform root cause analysis
    across 9 services spanning AWS, GCP, and Azure.
    When investigating, search for these error identifiers in logs (field: body.text):
    <subsystem 1>: ERROR-TYPE-1, ERROR-TYPE-2, ...
    <subsystem 2>: ERROR-TYPE-3, ERROR-TYPE-4, ...
    (list all 20 error_type values from channel files, grouped by subsystem)
    Log messages are in body.text — NEVER search the body field alone.

assessment_tool_config:
  id: <ns>_readiness_assessment
  description: |
    Comprehensive platform readiness assessment for <Scenario Name>.
    Evaluates <key subsystems>.
    Log message field: body.text (never use 'body' alone).
```

The `system_prompt` MUST list all 20 `error_type` values from the channel files.

---

## Executive KPIs

```yaml
executive_kpi_emitter_service_name: <service-key>
executive_dashboard_intro: |
  **Executive view** — cross-functional KPIs (synthetic `business.*` from `<service>`).
executive_kpi_sections:
  - header: "**Revenue** — GMV, conversion, ..."
    specs:
      - ["Display Title (unit)", "metrics.business.<field_name>"]
      # 6 entries
  # 4 sections total
executive_trend_charts:
  - title: "GMV trend"
    field: "metrics.business.gmv_usd_per_min"
    y_label: "USD/min"
  # 6 entries

executive_kpi_emissions:
  - name: business.gmv_usd_per_min
    value: {uniform: [1000.0, 9000.0], round: 1}
    unit: USD/min
  # one entry per metric; field name must match executive_kpi_sections specs
```

`field` in `executive_kpi_sections.specs` must be `"metrics.business.<name>"` where `<name>` matches the `name:` in `executive_kpi_emissions` with `"business."` prefix stripped.

---

## Trace attributes

```yaml
trace_attributes:
  base:
    platform.region: {choice: [us-east-1, us-central1, eastus]}
    platform.traffic_tier: {choice: [normal, normal, elevated, peak]}
  services:
    <service-key>:
      service.attribute: {choice: [val1, val2]}
      service.count: {randint: [1, 100]}
      service.rate: {uniform: [0.0, 100.0], round: 1}
    # all 9 services should appear, 4–5 attributes each
```

---

## Raw log profile

```yaml
raw_log_profile:
  service_name: <ns>-edge-gateway
  user_id_prefix: u
  tier_field: user_tier
  tier_values:
    - [free, 75]
    - [pro, 20]
    - [enterprise, 5]
  country_weights: {US: 35, GB: 12, DE: 12, FR: 8, JP: 8, IN: 10, BR: 8, CA: 7}
  methods: [GET, POST, PUT, DELETE]
  paths:
    - /api/v1/checkout
    - /api/v1/search
    - /api/v1/products
  change_point_path: /api/v1/checkout   # highest-revenue path; spikes during faults
```
