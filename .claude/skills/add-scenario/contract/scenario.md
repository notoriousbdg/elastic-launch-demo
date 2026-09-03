# Scenario YAML contract — scenario.yaml

Use this as a generation checklist for `scenario.yaml`. Work top-to-bottom and verify every property is present and correctly shaped.

The runtime reads scenario data from three sources:
- `scenarios/<id>/scenario.yaml` — scenario-level properties (identity, infra, agent, KPIs, trace attrs)
- `scenarios/<id>/channels/NN-<slug>.yaml` — one file per fault channel
- `scenarios/<id>/services/<svc>.yaml` — one file per service (identity, topology, telemetry DSL)

Reference implementation: [`scenario_engine/yaml_scenario.py`](scenario_engine/yaml_scenario.py)

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
