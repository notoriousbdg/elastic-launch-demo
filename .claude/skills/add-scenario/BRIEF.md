# Customer brief guide

Use this file during Phase 1 to determine what to ask the user. Parse what they have already provided, then ask only about genuinely missing signals. Ask at most 3 questions per `AskUserQuestion` call.

---

## Required signals

These three must be confirmed before moving to Phase 2. Infer them from the brief if reasonable; ask only if missing or too vague to act on.

### 1. Vertical

The broad industry segment. Needs to be specific enough to know what services, workflows, and failure modes are realistic.

**Sufficient:** "regional telco", "global freight forwarder", "digital-first health insurer", "tier-1 investment bank", "cloud gaming platform"
**Too vague:** "tech company", "enterprise", "big company"

If the user says a company name instead of a vertical, infer the vertical from the company type and confirm: "It sounds like this is a retail/e-commerce customer — is that right?"

### 2. Primary workflow

The single user-visible business flow the demo will center on. This determines the service names, topology, and which fault scenarios will feel compelling.

**Examples by vertical:**
- Logistics: "carrier dispatch → route optimization → last-mile delivery → proof of delivery"
- Telco: "customer activates SIM → network provisioning → call routing → billing"
- Healthcare: "patient schedules → clinician reviews → prescription → pharmacy fulfillment"
- Retail banking: "customer applies → underwriter reviews → loan funded → repayment tracked"
- Energy/utilities: "sensor reading → grid balancing → demand response → billing"
- Manufacturing: "production order → shop-floor execution → quality check → shipment"

One sentence describing the key data flow is enough.

### 3. Pain points

The top 2–3 observability gaps the customer is feeling. These drive which fault channels are most compelling.

**Common patterns:**
- "MTTR is too slow — it takes hours to find the root cause" → compelling fault channels + AI-driven RCA
- "Alert fatigue — we get thousands of alerts and can't find signal" → chaos channels should produce a clear signal hierarchy
- "We have no business context — we don't know if an incident is hurting revenue" → executive KPI dashboard + business correlation
- "Our three observability tools don't talk to each other" → cross-signal correlation story
- "Our compliance team needs audit trails for incidents" → HITL channels with case management
- "Our on-call team is reactive, not proactive" → anomaly detection + proactive alerting

---

## Helpful signals (ask if absent, but don't block)

### 4. Goals / wins

What makes this customer say "yes" at the end of the demo? What's the one capability they'd come back for?

**Examples:** "one platform replacing Datadog + Splunk", "AI that writes the RCA for me", "business dashboard I can show my CISO", "prove we can meet SLA on 99.9%"

### 5. Personas in the room

Who will be watching the demo? This determines the agent persona, executive KPI emphasis, and which scenarios to lead with.

| Persona | Emphasis |
|---|---|
| SRE / platform engineer | Fault channels, trace correlation, MTTR story |
| VP Engineering / CTO | Platform consolidation, AI-driven ops |
| CFO / FinOps | Executive KPI dashboard, cost-of-incidents story |
| CISO / compliance | Audit trail, HITL approval workflow, regulatory channels |
| Product / business | Business KPIs correlated with technical incidents |

### 6. Compliance / regulatory angle

Regulatory context shapes which fault channels are most compelling (and which feel irrelevant).

| Regulation | Relevant fault types |
|---|---|
| PCI DSS | Payment token exposure, TLS cert expiry, cardholder data access |
| HIPAA | PHI access anomaly, audit log gap, auth bypass |
| SOX | Financial data integrity, audit trail, change management |
| GDPR | Data residency violation, consent management failure, data export anomaly |
| FCC / NERC CIP | Network availability, critical infrastructure fault isolation |

### 7. Tone / naming hint

Any vocabulary or domain-register cue that helps choose good service naming style.

**Examples:** "they use the word 'node' not 'service'"; "this is a safety-critical ops center" → use ops/mission-oriented service names.

---

## What NOT to ask

- Real customer name (the generated scenario must be customer-neutral)
- Internal system names or internal codenames
- Competitor names
- Anything the user has already told you

---

## Example: minimal brief that unblocks Phase 2

> "Mid-size regional telco. Their main flow is network provisioning when a customer activates a new line. Pain points: slow MTTR on provisioning failures, and their NOC team is drowning in alerts with no priority ranking."

That gives you vertical (telco), workflow (network provisioning), and pain points (MTTR + alert fatigue). Enough to proceed.
