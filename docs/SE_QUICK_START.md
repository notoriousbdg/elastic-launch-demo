# SE Quick Start Guide

> Get the demo running and presenting in under 10 minutes.

---

## 1. Prerequisites

You need:
- Access to a running instance of the demo platform (EC2 or similar)
- An Elastic Cloud deployment with an API key
- A web browser

If the platform is not yet running, see [SETUP_GUIDE.md](SETUP_GUIDE.md) for full deployment instructions.

## 2. Open the Scenario Selector

Navigate to `http://<host>/` in your browser. You will see the **Scenario Selector** with 9 industry verticals.

## 3. Choose a Scenario

Pick the scenario that best fits your audience:

| Audience | Recommended Scenario | Why |
|----------|---------------------|-----|
| General / Technical | **Space** (NOVA-7 Space Mission) | Dramatic, easy to understand, great visuals |
| Sports / Media | **Fanatics** (Fanatics Collectibles) | Live streaming, real-time engagement |
| Banking / Finance | **Financial** or **Banking** | Trading systems, compliance, fraud detection |
| Healthcare / Life Sciences | **Healthcare** | Patient systems, regulatory, clinical data |
| Gaming / Entertainment | **Gaming** | Real-time multiplayer, matchmaking, payments |
| E-Commerce / Retail | **E-Commerce** | Order management, checkout, fulfillment |
| Manufacturing / OT | **Manufacturing** | Shop-floor ops, quality, OT/IT convergence |
| Cloud / Network Ops | **GCP Network Ops** | VPC flows, BGP routing, multi-cloud networking |

## 4. Connect and Deploy

1. Enter your Elastic Cloud credentials (Elasticsearch URL, Kibana URL, OTLP endpoint, API key)
2. Click **Launch**
3. Watch the deployer provision everything in Elastic (takes ~1-2 minutes):
   - Workflows, alert rules, AI agent + tools, knowledge base, dashboards, significant events

## 5. Verify

Once deployment completes, the selector page shows links to:

| Page | What It Shows |
|------|---------------|
| Dashboard | Live service status with real-time updates |
| Chaos Controller | Fault trigger/resolve UI |
| Kibana | Logs, metrics, traces flowing into Elastic |

## 6. Pick a Fault Channel

Each scenario has 20 fault channels. Choose the one that best fits your demo narrative.

### For Security / Ops Audiences

Pick channels involving safety-critical systems, pipeline failures, or tracking loss — these resonate with security-focused viewers.

### For Cloud / Infrastructure Audiences

Pick channels involving cross-cloud latency, relay corruption, or power failures — these highlight the multi-cloud story.

### For Developer / APM Audiences

Pick channels involving calibration drift, pressure anomalies, or signal processing — these show rich stack traces and distributed tracing.

### For Executive / High-Level Audiences

Pick channels with simple, universally relatable failures — fuel pressure, communications degradation, payload risk.

## 7. Demo Flow

1. **Show normal state** — all-green dashboard, telemetry flowing in Kibana
2. **Trigger a fault** — use the Chaos Controller UI or curl:
   ```bash
   curl -X POST http://<host>/api/chaos/trigger \
     -H 'Content-Type: application/json' \
     -d '{"channel": 2, "deployment_id": "<scenario-id>"}'
   ```
3. **Watch the dashboard** — services go CRITICAL/WARNING with cascade effects
4. **Switch to Kibana** — show error logs with structured attributes in Discover
5. **Show detection** — Elastic alert fires in the Rules / Alerts view
6. **Show AI investigation** — workflow triggers the AI agent for root cause analysis
7. **Show remediation** — automated workflow resolves the fault
8. **Show notification** — user gets email with RCA summary

## 8. Resolve the Fault

**Option A — Chaos Controller UI:** Click the resolve button.

**Option B — curl:**
```bash
curl -X POST 'http://<host>/api/remediate/2?deployment_id=<scenario-id>'
```

**Option C — Let Elastic Do It:** If the full workflow pipeline is configured, the AI agent calls the remediation API automatically after completing its investigation.

## 9. Multiple Faults

For longer demos, trigger a second fault from a different subsystem while the first is still active. This shows Elastic correlating multiple independent faults across different cloud providers.

---

## Troubleshooting Quick Fixes

| Problem | Fix |
|---------|-----|
| Dashboard is blank | Hard refresh (Ctrl+Shift+R). Check browser console for errors. |
| No data in Kibana | Verify credentials in the scenario selector. Check the deployment progress for errors. |
| Deployment failed | Check the progress panel for the specific step that failed. Common: invalid API key or missing permissions. |
| Fault trigger has no effect | Check channel status: `curl 'http://<host>/api/chaos/status/2?deployment_id=<scenario-id>'`. Channel may already be active. |
| App is not responding | Check if the process is running: `ps aux | grep uvicorn`. Restart if needed. |

For more details, see [TROUBLESHOOTING.md](TROUBLESHOOTING.md).
