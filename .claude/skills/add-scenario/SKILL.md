---
name: add-scenario
description: Add a new customer-vertical scenario (e.g. retail, logistics, telco) to elastic-launch-demo. Use when the user wants to onboard a new demo persona — gather their customer brief, design 9 services + 20 fault channels, and generate the scenario folder. Enforces that all scenario-specific code lives in scenarios/<id>/ and generators/deployer stay untouched.
---

# Add a new scenario

This skill gathers a customer brief, designs a vertically-appropriate scenario, and generates the full `scenarios/<id>/` folder using the **YAML-driven layout**. No files outside `scenarios/<id>/` are ever modified.

Read [GUARDRAILS.md](.claude/skills/add-scenario/GUARDRAILS.md) now and hold every rule in it for the duration of this session.

---

## Phase 1: Gather the customer brief

Parse any context the user already provided. Then read [BRIEF.md](.claude/skills/add-scenario/BRIEF.md) and ask — via `AskUserQuestion` — only about signals that are missing. Ask at most 3 questions per call.

Required signals before proceeding to Phase 2:
- **Vertical** — broad industry label
- **Primary workflow** — the user-visible business flow the demo will center on
- **Pain points** — top 2–3 observability gaps the customer feels

Helpful but not blocking: goals/wins, personas in the room, compliance angle, tone/theme hint.

---

## Phase 2: Pick a reference scenario

Read the most recently committed scenario's `scenario.yaml` to understand the shape of all properties. To find the most recent one, run:

```bash
for id in $(ls scenarios/*/scenario.yaml 2>/dev/null | xargs -I{} dirname {} | xargs -I{} basename {}); do
  git log -1 --format="%cI $id" -- scenarios/$id/scenario.yaml
done | sort -r | head -1
```

Read `scenarios/<chosen-id>/scenario.yaml` and one of its `channels/*.yaml` files.  
Also read [CONTRACT.md](.claude/skills/add-scenario/CONTRACT.md) as your generation checklist.

---

## Phase 3: Design — propose then confirm

Synthesize the brief into a one-screen design draft. Present it via `AskUserQuestion` (single question, long description field) and ask the user to confirm, adjust, or replace before you write any code.

The draft must cover:

**Identity**
- `scenario_id` — short lowercase slug (e.g. `logistics`, `telco`, `medtech`)
- `namespace` — telemetry prefix, same as or abbreviated from scenario_id
- `scenario_name` — display name (e.g. "Global Logistics Platform")
- `scenario_description` — 2–3 sentence card blurb
- `scenario_icon` — emoji

**9 services** (3 per cloud — AWS, GCP, Azure)
- Name, subsystem, language (∈ python/java/go/dotnet/rust/cpp), cloud, purpose
- Designate which service is the `executive_kpi_emitter_service_name`

**20 fault channels summary** — channel number, name, subsystem, and one-line description
- Channels 1–15: HITL faults (human must approve remediation)
- Channels 16–20: Auto-remediate faults (pod restart, cache flush, cert renewal, etc.) — see [GUARDRAILS.md](GUARDRAILS.md) §6

**Executive KPI categories** — 4 sections of 6 metrics each (titles only)

---

## Phase 4: Generate scaffolding

After the user confirms the design, use the scaffold generator to create the consistent skeleton first, then flesh out domain content.

### Step 1 — Write a brief YAML and run the scaffolder

Write `scripts/<id>-brief.yaml` with this structure (see `scripts/scaffold_scenario.py` docstring for the full spec):

```yaml
scenario_id: <id>
scenario_name: "..."
scenario_icon: "🔧"
namespace: <id>
sort_order: <n>
nominal_label: ONLINE

services:       # exactly 9, in desired sort_order
  - name: <svc>
    cloud_provider: aws | gcp | azure
    cloud_region: <region>
    cloud_platform: aws_ec2 | gcp_compute_engine | azure_vm
    cloud_availability_zone: <az>
    subsystem: <subsystem>
    language: python | java | go | dotnet | rust | cpp | nodejs
    entry_service: true      # exactly one — gets 4× trace-entry weighting
    kpi_emitter: false       # exactly one must be true
    generates_traces: false  # only for infra-only services (DBs, etc.)

channels:       # exactly 20
  - slug: <kebab-case-slug>  # becomes: NN-<slug>.yaml
    error_type: SVC-FAULT-CODE
    affected_services: [<svc1>]
    cascade_services: [<svc2>]
```

Then run:

```bash
python3 scripts/scaffold_scenario.py --brief scripts/<id>-brief.yaml
```

This emits all 30 files with:
- `scenario.yaml` — hosts, k8s_clusters, agent_config.system_prompt (all 20 error_types pre-wired), trace_attributes.services (all 9 keys), executive KPI scaffolding in sync
- `services/<svc>.yaml` ×9 — identity keys + sort_order + stub steps
- `channels/NN-<slug>.yaml` ×20 — error_type/affected/cascade filled, error_message + fault_params stubs with matching placeholders

The generated skeleton passes all **structural** verifier checks immediately. Only domain content (realistic error messages, metric values, telemetry step logic) is left to fill in.

### Step 2 — File list (what was generated)

```text
scenarios/<id>/
    scenario.yaml                   ← all static scenario data (pre-wired)
    channels/
        01-<slug>.yaml              ← one per fault channel (×20)
        ...
        20-<slug>.yaml
    services/
        <svc1>.yaml                 ← one per service (×9)
        <svc2>.yaml
        ...
```

No `__init__.py`, no `scenario.py`, no `executive_kpis.py` — the registry discovers scenarios by globbing `scenarios/*/scenario.yaml` directly.

### `scenario.yaml` structure

See [CONTRACT.md](CONTRACT.md) for the full property list. Top-level structure:

```yaml
scenario_id: <id>
scenario_name: <Name>
scenario_description: |
  2–3 sentence blurb...
namespace: <id>
scenario_icon: "🔧"
sort_order: 50

# No services:, service_topology:, entry_endpoints:, or db_operations: here.
# All per-service data lives in services/<svc>.yaml (see Per-service YAML spec below).

hosts:
  - host.name: <ns>-aws-host-01
    # ... full host dict per CONTRACT.md ...

k8s_clusters:
  - name: <ns>-eks-cluster
    provider: aws
    platform: aws_eks
    region: us-east-1
    zones: [us-east-1a, us-east-1b, us-east-1c]
    os_description: Amazon Linux 2
    services: [<aws-svc-1>, <aws-svc-2>, <aws-svc-3>]
  # ... gcp and azure clusters ...

agent_config:
  id: <ns>-ops-analyst
  name: <Vertical> Operations Analyst
  assessment_tool_name: <ns>_readiness_assessment
  system_prompt: |
    You are the <Role>...
    (Must list all 20 error_type values from channels.)

assessment_tool_config:
  id: <ns>_readiness_assessment
  description: |
    Comprehensive platform assessment...

executive_kpi_emitter_service_name: <service-key>
executive_dashboard_intro: |
  **Executive view** — ...
executive_kpi_sections:
  - header: "**Revenue** — ..."
    specs:
      - ["Display Title (unit)", "metrics.business.<field>"]
      # 6 entries
  # 4 sections total
executive_trend_charts:
  - title: "GMV trend"
    field: "metrics.business.gmv_usd_per_min"
    y_label: "USD/min"
  # 6 entries

executive_kpi_emissions:
  - name: business.<field>
    value: {uniform: [lo, hi], round: n}
    unit: USD/min
  # one entry per metric emitted

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
  paths: [/api/v1/checkout, /api/v1/search, ...]
  change_point_path: /api/v1/checkout

trace_attributes:
  base:
    platform.region: {choice: [us-east-1, us-central1, eastus]}
    platform.traffic_tier: {choice: [normal, normal, elevated, peak]}
  services:
    <service-key>:
      svc.attribute: {choice: [val1, val2]}
      svc.count: {randint: [1, 100]}
    # ... all 9 services ...
```

### Channel YAML schema (one file per channel)

Each channel file is self-contained — placeholders and fault params are co-located for easy parity checking:

```yaml
name: Payment Gateway Timeout
subsystem: payments
vehicle_section: checkout_pipeline
error_type: PAYMENT-GATEWAY-TIMEOUT
sensor_type: gateway_latency
affected_services: [payment-processor, order-management]
cascade_services: [storefront-gateway]
description: Payment provider repeatedly timing out during checkout.
investigation_notes: |
  1. Check recent error rate: search body.text LIKE *PAYMENT-GATEWAY-TIMEOUT*
  2. ...
remediation_action: restart_payment_gateway
error_message: |
  PAYMENT-GATEWAY-TIMEOUT: provider={payment_provider} timeout_ms={timeout_ms}
stack_trace: |
  TimeoutException: provider={payment_provider}
  at PaymentGateway.charge()
  timeout: {timeout_ms}ms
correlation_attr:
  key: deployment.gateway_sdk_version
  value: stripe-sdk-v14.2.0-beta3
fault_params:
  # One key per {placeholder} in error_message + stack_trace above
  payment_provider: {choice: [stripe, braintree, adyen]}
  timeout_ms: {randint: [3000, 30000]}
rca_clues:
  # Per-service partial clues (2–3 per affected/cascade service)
  payment-processor:
    gateway.retry_count: {randint: [3, 10]}
    gateway.last_success_s_ago: {randint: [30, 300]}
  order-management:
    order.pending_count: {randint: [50, 500]}
    order.stuck_at_payment: true
```

**YAML DSL reference** for `fault_params`, `rca_clues`, `trace_attributes`:

| Python | YAML DSL |
|--------|----------|
| `random.choice(["a","b"])` | `{choice: [a, b]}` |
| `random.randint(1, 100)` | `{randint: [1, 100]}` |
| `round(random.uniform(80.0, 99.5), 1)` | `{uniform: [80.0, 99.5], round: 1}` |
| `random.random()` | `{random: true}` |
| `f"ORD-{random.randint(1000,9999)}"` | `{format: "ORD-{n}", n: {randint: [1000, 9999]}}` |
| `f"{random.randint(0,255):02x}:..."` | `{format: "{a:02x}:{b:02x}...", a: {randint: [0,255]}, ...}` |
| `True` | `true` |

### Per-service YAML spec (`services/<svc>.yaml`)

Each of the 9 service YAML files (filename = service key, e.g. `payment-processor.yaml`):

```yaml
service: payment-processor      # kebab-case; filename must match (payment-processor.yaml)
sort_order: 7                    # 1–9; first service gets 4× trace-entry weighting
cloud_provider: azure            # aws | gcp | azure
cloud_region: eastus
cloud_platform: azure_vm         # aws_ec2 | gcp_compute_engine | azure_vm
cloud_availability_zone: eastus-1
subsystem: payments
language: java                   # python | java | go | dotnet | rust | cpp | nodejs
topology:
  - [fraud-detector, /api/v1/fraud/check, POST]
entry_endpoints:
  - [/api/v1/payment/charge, POST]
db_operations:
  - [SELECT, transactions, "SELECT id, amount FROM transactions WHERE id = ?"]
emit_fault_logs: true            # default true; false only for cascade-only services
kpi_emitter: false               # true for the executive_kpi_emitter_service_name

constants:
  STATUSES: [OK, DEGRADED, ERROR]

state:
  _counter: {init: 0}
  _last_report: {init: now}

steps:
  - sample:
      status:
        if_active: {choice: [DEGRADED, ERROR]}
        else: OK
      latency_ms:
        if_active: {uniform: [500.0, 3000.0], round: 1}
        else: {uniform: [20.0, 150.0], round: 1}
  - counter:
      state_key: _counter
      by: 1
  - log:
      level: INFO
      message: "[SVC] operation status={status} latency={latency_ms}ms count={_counter}"
      attrs:
        operation: process
        svc.status: {var: status}
        svc.latency_ms: {var: latency_ms}
  - metric:
      name: payment_processor.latency_ms
      value: {var: latency_ms}
      unit: ms
```

See the **Service telemetry DSL** section in CONTRACT.md for all step types, value specs, and examples (including `for_each`, `every`, `incr_key`, `{expr: ...}`, and state initialization).

The `services/` directory must contain **only `.yaml` files** — no Python files and no `__init__.py`.

---

## Phase 5: Flesh out all properties

Work through [CONTRACT.md](CONTRACT.md) top-to-bottom. Pay extra attention to:
- Language allowlist (python|java|go|dotnet|rust|cpp|nodejs)
- `fault_params` parity: every `{placeholder}` in `error_message` + `stack_trace` must have a matching key in `fault_params` of the **same channel file** — this is locally visible now
- `k8s_clusters[].services` must contain exactly the 3 service keys for that cloud

One property not in CONTRACT.md:
- **`raw_log_profile.change_point_path`**: the highest-revenue path (checkout, payment, etc.)

---

## Phase 6: Validate

After generation, run these checks:

**Auto-discovery check:**
```bash
python3 -c "
from scenarios import list_scenarios
hit = next((s for s in list_scenarios() if s['id'] == '<id>'), None)
print('FOUND:', hit) if hit else print('NOT FOUND — check that scenarios/<id>/scenario.yaml exists and is picked up by the glob')
"
```

**Full integrity check (must exit 0 — exits non-zero on any failure):**
```bash
python3 scripts/verify_yaml_scenarios.py <id>
```

This verifies all structural invariants including:
- 20 channels (contiguous 01–20) each with required fields (`fault_params`, `error_message`, etc.)
- 9 services, `sort_order` values are exactly {1..9}, all identity keys present on every service
- Language is in allowed set (python/java/go/dotnet/rust/cpp/nodejs)
- `agent_config.system_prompt` mentions all 20 channel `error_type`s
- `trace_attributes.services` keys match actual service names
- `executive_kpi_emitter_service_name` names a real service; KPI sections/emissions in sync
- Sort_order:1 service has `entry_endpoints` defined
- `affected_services`/`cascade_services`/`k8s_clusters.services` reference real service names
- All `{placeholder}`s in `error_message`+`stack_trace` have a matching `fault_params` key
- Every service has a non-empty `steps:` body (unless `generates_traces: false AND emit_fault_logs: false`)

Iterate until the verifier exits 0 before calling the work done.

**Service telemetry DSL check:**
```bash
python3 -c "
from pathlib import Path
from scenarios.yaml_scenario import load_yaml_scenario
sc = load_yaml_scenario(Path('scenarios/<id>'))
classes = sc.get_service_classes()
print(f'{len(classes)} service classes loaded:')
for cls in classes: print(f'  {cls.SERVICE_NAME}')
"
```

Must print exactly 9 service classes, all with the correct service keys.

**Scope check:**
```bash
git diff --name-only
```
Output must show only files under `scenarios/<id>/`. If any other file appears, stop and alert the user.

**Report:** Summarize what was generated, the file count, any validation warnings, and how to activate:
```
ACTIVE_SCENARIO=<id> ./start.sh
```
