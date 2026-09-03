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

---

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
