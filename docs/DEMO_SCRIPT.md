# Demo Script — Presenter Talk Track

> **Duration:** 15-25 minutes (adjustable)
> **Audience:** SEs, prospects, partners, or internal teams
> **Setup:** Ensure the platform is running and a scenario is deployed. See [SETUP_GUIDE.md](SETUP_GUIDE.md).

---

## Pre-Demo Checklist

- [ ] App is running (`http://<host>/health` returns OK)
- [ ] A scenario is deployed (via the scenario selector at `http://<host>/`)
- [ ] Dashboard is open at `http://<host>/dashboard`
- [ ] Chaos UI is open in a separate tab at `http://<host>/chaos`
- [ ] Kibana is accessible and showing live telemetry
- [ ] (Optional) Verify alert rules are active in Kibana > Rules

---

## Opening: Choose Your Scenario

> "What I am about to show you is an observability demo platform. We have nine industry verticals to choose from — space launch, sports streaming, financial services, healthcare, gaming, retail banking, e-commerce, manufacturing, and GCP network operations. Each one simulates nine microservices running across AWS, GCP, and Azure, all generating real OpenTelemetry telemetry that flows into Elastic."

If selecting a scenario live:

> "Let me pick [scenario name] for this audience. I will connect it to our Elastic Cloud deployment — and watch: the platform automatically provisions everything in Elastic. AI agent, investigation tools, alert rules, workflows, dashboards, knowledge base — all configured in about a minute."

---

## Act 1: Streams — Normal Telemetry Flow

**Duration:** ~5 minutes

### Show the Dashboard

Navigate to the dashboard.

> "This is our live dashboard. Each card represents a service — [describe services relevant to the scenario]. They are spread across three cloud providers: AWS, GCP, and Azure."

Point out the all-green NOMINAL status across services.

> "Every service is currently green — NOMINAL. They are continuously emitting logs, metrics, and distributed traces through OpenTelemetry, directly into Elastic."

### Show Elastic / Kibana

Switch to Kibana Discover.

> "Here in Elastic, we can see the telemetry arriving in real time. Each log record carries rich metadata — cloud provider, region, subsystem, service name, severity level."

Highlight the multi-cloud aspect:

> "This is a key architectural point. [Service A] runs on AWS. [Service B] runs on GCP. [Service C] runs on Azure. Elastic is the single pane of glass that unifies observability across all three clouds."

### Show Background Telemetry

> "Beyond the application logs, we also have host metrics — CPU, memory, disk, network — from simulated infrastructure. Kubernetes metrics for the container orchestration layer. Nginx metrics for the web tier. VPC flow logs for network visibility. And distributed traces showing request flows between services."

### Transition

> "Everything looks nominal. Now let us see what happens when something goes wrong."

---

## The Interruption — Trigger a Fault

**Duration:** ~2 minutes

### Trigger via Chaos UI

Open the Chaos Controller or use the API.

> "I am going to trigger a fault — [describe the fault]. Watch the dashboard."

Click the trigger button or run:

```bash
curl -X POST http://<host>/api/chaos/trigger \
  -H 'Content-Type: application/json' \
  -d '{"channel": 2, "deployment_id": "<scenario-id>"}'
```

### Watch the Dashboard React

> "Immediately, [affected services] go CRITICAL. And notice — [cascade services] have gone to WARNING. That is the cascade effect. When a primary subsystem fails, dependent services detect degraded conditions."

Point out the status changes on the dashboard.

> "Now let us look at what Elastic sees."

---

## Act 2: Significant Events + AI Agent

**Duration:** ~5-8 minutes

### Show Error Logs in Kibana

Switch to Kibana Discover. Filter to the error type.

> "The error logs are flowing in immediately. Each one carries the exact error type, stack trace, sensor readings, and subsystem context. This is not a generic error message — it is a richly structured signal."

### Show Significant Event Detection

> "This is where Elastic's detection engine comes in. We have ES|QL rules watching for these error signatures. When the error rate exceeds the threshold, Elastic fires a significant event — an alert."

Show the Alerts view in Kibana.

> "There it is — Elastic has detected the anomaly. It knows the channel, the subsystem, the affected services, and the time window."

### Show AI Agent Investigation

> "Now watch this. The alert automatically triggers an Elastic workflow. The workflow runs our AI investigation agent. The agent has tools — it queries Elastic for error logs, checks service health, searches known anomalies, traces the cascade pattern."

Show the workflow execution or agent conversation.

> "The agent has completed its root cause analysis. It identified the fault, traced the cascade to dependent services, and determined the remediation action."

### Show Auto-Remediation

> "The workflow includes a remediation step. The AI agent calls back to our platform's remediation API to resolve the fault channel."

Watch the dashboard — services should return to NOMINAL.

> "And an email notification goes out to the on-call engineer with the full RCA summary — what happened, what was affected, and what was done to fix it."

---

## Act 3: Recovery

**Duration:** ~3-5 minutes

### Watch Recovery (if not already resolved by the workflow)

If demonstrating manual resolution:

```bash
curl -X POST 'http://<host>/api/remediate/2?deployment_id=<scenario-id>'
```

> "The affected services are returning to NOMINAL. The cascade warnings are clearing. Telemetry is flowing cleanly again."

Switch to Kibana to show the error rate dropping.

> "In Elastic, the error rate drops to zero. The alert resolves. Everything is green."

---

## Closing

> "What you have just seen is the full Elastic observability lifecycle:
>
> 1. **Collect** — OpenTelemetry ingests telemetry from nine services across three clouds
> 2. **Detect** — Elastic's ES|QL rules identify the anomaly in real time
> 3. **Investigate** — An AI agent queries Elastic to determine root cause
> 4. **Remediate** — Automated workflows resolve the fault and notify the team
>
> This is the same architecture you would use in production — the same OTLP protocol, the same Elastic detection engine, the same AI-powered investigation. And we can switch to any of our nine industry scenarios to tell the story that resonates with your environment."

### Optional: Second Fault

For longer demos, trigger a second fault from a different subsystem:

> "Let me trigger a second fault — this time in a different subsystem on a different cloud provider. Watch how Elastic correlates two independent faults simultaneously."

---

## Q&A Talking Points

**"Is the telemetry real OpenTelemetry?"**
> Yes. The services emit OTLP JSON over HTTP directly to Elastic Cloud. The same OTLP protocol works with any OpenTelemetry-compatible source — your existing instrumentation would work the same way.

**"How many channels can be active at once?"**
> All 20 channels are independent. You can trigger multiple faults simultaneously to demonstrate Elastic's ability to correlate and prioritize.

**"How does the AI agent work?"**
> It uses Elastic's Agent Builder — a configured AI agent with parameterized ES|QL tools, a knowledge base of runbooks, and a system prompt tailored to the scenario. When an alert fires, an Elastic workflow invokes the agent, which uses its tools to query Elasticsearch and reason about the root cause.

**"Can I add my own scenarios?"**
> Yes. Each scenario is a Python class inheriting from `BaseScenario` in the `scenarios/` directory. You define 9 services, 20 fault channels, a UI theme, and the scenario framework handles everything else — telemetry generation, chaos injection, dashboard theming, and Elastic resource deployment.

**"What gets deployed to Elastic?"**
> The Python deployer creates: 3 workflows, 20 alert rules, 20 significant event definitions, 20 KB documents, 7-8 agent tools, an AI agent, data views, and an executive dashboard — all automatically configured for the selected scenario.
