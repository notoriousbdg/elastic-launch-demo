# Scenario YAML contract — channel files

Use this as a generation checklist when authoring `channels/NN-<slug>.yaml` files.

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
- `investigation_notes` placeholders must be `fault_params` keys (verifier Check 35). The KB doc, Cortex runbook and channel runbook skill replace each one with the parameter's range; any other `{name}` shows as literal text. Shell syntax such as curl's `%{time_total}` is fine.

### Evidence contract

`rca_clues` are what investigations (the Agent Builder agent and Nightshift) use to confirm the cause. The spans carry them only while the channel is active, and the docs list each clue with its range. So:

- **One cause.** Notes often name two or three candidate causes. Pick the one the frame dump supports, give it clues that confirm it, and give the alternatives clues at nominal values that rule them out.
- **Beyond the threshold.** Every clue range sits wholly on the fault side of the threshold the notes give (">3.5A indicates binding" → `{uniform: [3.6, 4.5]}`, never `[2.5, 4.0]`).
- **No healthy choices.** A `choice` clue lists fault values only — never `OFF`, `INTACT`, `false` or another healthy reading (Check 36 warns).
- **Pin the faulty unit.** If the `stack_trace` lists sibling units (tanks, radars, zones, buses) as NOMINAL, the fault param that names the faulty unit must not be able to pick one of them (Check 36 warns).
- **Dump agrees with spans.** Readings in the frame dump that the notes rely on are `{fault_params}` placeholders with the same range as the clue, not fixed healthy-looking numbers.
- **Every reading has a clue.** Each reading the notes tell an investigator to check has a clue on the service that would report it, or the notes give the threshold the clue is judged against.

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
