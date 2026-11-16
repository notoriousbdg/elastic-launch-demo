# Scenario YAML contract — scenario.yaml

Use this as a generation checklist for `scenario.yaml`. Work top-to-bottom and verify every property is present and correctly shaped.

The runtime reads scenario data from three sources:
- `scenarios/<id>/scenario.yaml` — scenario-level properties (identity, infra, agent, KPIs, trace attrs, generators)
- `scenarios/<id>/channels/NN-<slug>.yaml` — one file per fault channel
- `scenarios/<id>/services/<svc>.yaml` — one file per service (identity, topology, telemetry DSL)

Reference implementation: [`scenario_engine/yaml_scenario.py`](scenario_engine/yaml_scenario.py)

Reference scenarios: `scenarios/k8splatform/` (schema 1.3, pinned `k8s_topology`, infrastructure faults on one cloud) and `scenarios/space/` (business-impact and dashboard visibility for an application vertical).

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
| `schema_version` | ✅ | `str` | `"1.3"` for new scenarios (the scaffolder writes the current version from `scenario_engine/schema.yaml`). Checks 37-44 apply from 1.3. |

---

## Generators

Beyond the 9 services and 8 always-on `core` generators (traces, raw_access, host_metrics, kubernetes_otel, nginx_otel, aws_vpcflow_otel, gcp_vpcflow_otel, azure_vnetflow_otel), a scenario can opt in to any number of the 60+ generators registered in `generator_engine.registry.GENERATORS`. Selection happens in Phase 3 Step A.

Enable a generator by adding it to a top-level `generators:` block in `scenario.yaml`:

```yaml
generators:
  redis_otel: true               # enable with defaults
  zookeeper_otel:                 # enable with config_fields overrides
    node_names: [zk-node-1, zk-node-2, zk-node-3]
```

Inspect a generator before wiring it up — this is the source of truth for its `config_fields` and the impact keys/ranges its channels can drive:

```bash
python3 -m generator_engine.registry --describe <gen_id>
```

**Every selected generator must have exactly one chaos channel with a `generator_impact` block** — otherwise the generator is enabled but no channel ever breaches its alert threshold. See `contract/channels.md` §Generator impact for the channel-side schema.

Two reference patterns already in the repo:
- `scenarios/ecommerce/channels/10-catalog-redis-purge-storm.yaml` — one channel driving multiple impact keys on one generator, alongside a rich business-impact/RCA story.
- `scenarios/gt-*/channels/*.yaml` — one channel per impact key, minimal (built for generator coverage testing, not vertical storytelling — don't copy their sparse style into a real scenario).

---


## Infrastructure

### `hosts` — 3 hosts, one per cloud (or 3 nodes of one cloud for a single-cloud scenario such as `k8splatform`)

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

### `k8s_topology` — pinned platform (optional, schema 1.3)

Without this block every cluster gets three generated nodes and one pod per service (`generator_engine/infra_topology.py`). That is enough for an application scenario. An infrastructure story needs the numbers to agree everywhere: a scheduler line that says `0/51 nodes are available` must match the node count in the metrics, and a CoreDNS line that reads from `10.42.33.12` must come from a pod with that IP. `k8s_topology` pins that model (`generator_engine/platform_topology.py`). When it is present:

- `kubernetes_otel` reports these nodes, pools, workloads and pods (`generators/kubernetes_otel/pinned.py`), honours each channel's `fault_scope`, ramps, and applies `fault_state` / `remediation_effects.state`.
- Every service log record takes the identity of a real pod of the workload mapped to it (`service:`), or of a node for `log_identity: node` services; the runtime placeholders (`{self_node}`, `{self_pod_ip}`, `{node_count}`, `{pool_<name>_count}`) read the same model.
- `host_metrics` limits a scoped fault to the hosts inside the scope (matched by node name).

Keep `k8s_clusters` alongside it: generators that predate `k8s_topology` still read it, and Check 8 still wants 3 hosts and 3 clusters. Pin the 3 `hosts` as real nodes of the topology (list them under `node_pools[].nodes`) so host and node identity agree.

```yaml
k8s_topology:
  default_cluster: prod-use1           # cluster used when a scope or state names none
  clusters:
    - name: prod-use1
      provider: aws
      platform: aws_eks
      region: us-east-1
      vpc_cidr: 10.42.0.0/16
      service_cidr: 172.20.0.0/16
      kube_dns_ip: 172.20.0.10
      node_name_format: 'ip-{ip_dashed}.ec2.internal'
      subnets:                          # AZ -> CIDR; node and pod IPs come from these
        us-east-1a: 10.42.32.0/23       # (or `zones: [...]` for a minimal comparison cluster)
        us-east-1b: 10.42.36.0/23
        us-east-1c: 10.42.40.0/23
      node_pools:
        - name: edge-c6i
          count: 6
          instance_types: [c6i.2xlarge]
          cpu: 8
          memory_gib: 16
          os_image: Amazon Linux 2023.6.20250115
          kubelet_version: v1.30.4-eks-a737599
          nodes: [ip-10-42-33-51.ec2.internal]   # pinned names (a scenario host); the rest are generated
          # optional: capacity_type: SPOT, arch: arm64, taints: [...], labels: {...}, zones: [...]
        - name: arm-m7g
          count: 0                        # added by a channel's fault_state (node_pools.<name>.add_nodes)
          arch: arm64
          instance_types: [m7g.2xlarge]
      workloads:
        - name: ingress-nginx-controller
          kind: Deployment                # Deployment | DaemonSet | StatefulSet | Job
          namespace: ingress-nginx
          service: ingress-nginx          # the scenario service whose logs come from these pods
          node_pool: edge-c6i
          replicas: 6
          image: registry.k8s.io/ingress-nginx/controller:v1.11.3
          container: controller
          cpu_request: 1
          memory_request: 512Mi
          memory_limit: 1Gi
          cpu_util: [0.35, 0.50]          # baseline utilisation ranges
          memory_util: [0.55, 0.65]
          # optional: cpu_limit, hpa: {min, max, target_cpu}, pvc: {name, capacity, used},
          # tolerations, placement: [pool, ...], arch_images, selector, labels
```

Pools that a change adds (a new node group) start at `count: 0`; the channel that adds them sets `fault_state`. Full example: `scenarios/k8splatform/scenario.yaml` (three EKS clusters; only `prod-use1` is fully pinned, the other two are smaller comparison clusters).

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
    When investigating, search for these error signatures in logs (field: body.text):
      - <error_type 1>: <one-line meaning>
      - <error_type 2>: <one-line meaning>
    (list all 20 error_type values from the channel files; at 1.3 these are natural
    substrings of the error lines, e.g. "nf_conntrack: table full, dropping packet")
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
    better: higher            # lower | higher — drives tile colours; required if a channel impacts it
    unit: USD/min
  # one entry per metric; field name must match executive_kpi_sections specs
```

`field` in `executive_kpi_sections.specs` must be `"metrics.business.<name>"` where `<name>` matches the `name:` in `executive_kpi_emissions` with `"business."` prefix stripped.

Keep nominal ranges narrow and always set `better:` — the executive tile is green inside the nominal band and
yellow/red outside it (see contract/channels.md §Dashboard visibility). Renaming or removing a KPI means retargeting every channel's `business_impact` (see contract/channels.md §Business impact); the verifier errors on stale names.

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

---

## Library workflow inputs (optional)

When the scenario enables generators whose data a Template Library investigation
workflow reads (today the AWS RDS, ALB, Lambda and SQS generators), the deployer
installs that workflow and wires it to the matching OOTB package alert rule. The
generators supply the resource ids it needs (`load_balancer`, `db_instance_identifier`,
`function_name`, `queue_name`). Add the scenario's own meaning with:

```yaml
workflow_template_inputs:
  service_name: orders-gateway                       # every workflow with a service_name input
  aws-rds-connection-exhaustion-investigation-otel:  # one workflow (slug) overrides
    service_name: orders-service
```

Values become the manual-run defaults and the fallback alert-triggered runs use.
Omit the block when the generic wording ("the backend service") is fine.

