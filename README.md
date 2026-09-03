# Elastic Observability Demo Platform

**A multi-scenario, multi-cloud observability platform that demonstrates Elastic with OpenTelemetry, AI-powered anomaly detection, and automated remediation.**

Choose from 6 industry verticals — space launch, sports streaming, financial services, healthcare, gaming, and insurance — each with 9 simulated microservices across AWS, GCP, and Azure. Every scenario emits real OpenTelemetry telemetry (logs, metrics, traces) directly into Elastic Cloud. A 20-channel chaos system injects realistic faults that Elastic detects via ES|QL rules, an AI agent investigates, and automated workflows resolve.

---

## Architecture

```
                 Elastic Observability Demo Platform
  ┌─────────────────────────────────────────────────────────────┐
  │                                                             │
  │  ┌─ Scenario: 9 Simulated Microservices ─────────────────┐  │
  │  │                                                       │  │
  │  │  AWS us-east-1       GCP us-central1    Azure eastus  │  │
  │  │  ┌────────────┐     ┌────────────┐    ┌────────────┐  │  │
  │  │  │ Service 1  │     │ Service 4  │    │ Service 7  │  │  │
  │  │  │ Service 2  │     │ Service 5  │    │ Service 8  │  │  │
  │  │  │ Service 3  │     │ Service 6  │    │ Service 9  │  │  │
  │  │  └────────────┘     └────────────┘    └────────────┘  │  │
  │  └───────────────────────────────────────────────────────┘  │
  │                                                             │
  │  FastAPI (:8080)                  OTLP/HTTP (direct)        │
  │  ┌──────────────────┐            ┌──────────────────────┐   │
  │  │ Scenario Selector │──deploy──>│ Elastic Cloud        │   │
  │  │ Chaos Controller  │──OTLP───> │ ┌──────────────────┐ │   │
  │  │ Python Deployer   │──REST───> │ │ Elasticsearch    │ │   │
  │  │ Log Generators    │           │ │ Kibana           │ │   │
  │  │                   │           │ │ AI Agent         │ │   │
  │  │                   │           │ │ Workflows        │ │   │
  │  └──────────────────┘            │ │ Alerting Rules   │ │   │
  │                                  │ └──────────────────┘ │   │
  │                                  └──────────────────────┘   │
  └─────────────────────────────────────────────────────────────┘
```

**Key design:** The app sends OTLP telemetry directly to Elastic Cloud (no OTel Collector required). A built-in Python deployer configures all Elastic-side resources (agent, tools, workflows, alert rules, dashboards, significant events) automatically for each scenario.

---

## Key Features

- **9 industry scenarios** — space launch, sports streaming, financial trading, healthcare, live gaming, retail banking, e-commerce, GCP network ops, and manufacturing
- **9 simulated microservices per scenario** across 3 cloud providers (AWS, GCP, Azure)
- **Real OpenTelemetry telemetry** — logs, metrics, and traces sent directly via OTLP
- **20 independent fault channels** per scenario covering scenario-specific subsystems
- **Cascade effects** — primary faults propagate warnings to dependent services
- **Web-based scenario selector** — choose an industry vertical, connect to Elastic, deploy with one click
- **Python deployer** — automatically provisions agent, tools, workflows, alert rules, dashboards, KB docs, and significant events in Elastic
- **Chaos controller UI** for triggering and resolving faults during demos
- **Background telemetry generators** — host metrics, Kubernetes metrics, Nginx metrics, VPC flow logs, MySQL logs, distributed traces
- **Elastic integration** — significant event detection with ES|QL rules, AI agent investigation, automated remediation workflows
- **Auto-remediation** — alert fires, workflow runs AI agent for RCA, agent calls remediation API, user gets email notification
- **Multi-tenancy** — multiple scenarios can run simultaneously with independent deployment tracking

---

## Scenarios

| ID | Name | Industry | Namespace |
|----|------|----------|-----------|
| space | NOVA-7 Space Mission | Space / Aerospace | nova7 |
| fanatics | Fanatics Collectibles | Sports Streaming | fanatics |
| financial | Financial Trading Platform | Financial Services | finserv |
| healthcare | Healthcare Systems | Healthcare | healthcare |
| gaming | Live Gaming Platform | Gaming | gaming |
| banking | Retail Banking Platform | Retail Banking | banking |
| ecommerce | Global Commerce Platform | E-Commerce | ecommerce |
| manufacturing | Manufacturing Operations Platform | Manufacturing / OT | mfg |
| gcp | Google Cloud Network Operations | GCP / Networking | gcpnet |

Each scenario provides its own services, fault channels, UI theme, terminology, and configuration. The scenario framework (`scenarios/base.py`) defines the interface; each scenario directory implements it.

---

## Quick Start

### 1. Prerequisites

- An EC2 instance (or similar server) with Python 3.11+
- An Elastic Cloud deployment with an API key

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Start the App

```bash
./start.sh
# or manually:
python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8080
```

### 4. Open the Scenario Selector

Navigate to `http://<your-host>/` — this opens the scenario selector where you:

1. Choose an industry vertical
2. Enter your Elastic Cloud credentials
3. Click **Launch** to deploy all Elastic-side resources and start telemetry

### 5. Demo

| URL | Description |
|-----|-------------|
| `http://<host>/` | Scenario selector (choose and deploy) |
| `http://<host>/chaos?deployment_id=...` | Chaos controller UI |
| `http://<host>/health` | Health check endpoint |
| `http://<host>/api/status` | Full system status API |

### 6. Trigger a Fault

```bash
# Trigger Channel 2
curl -X POST http://<host>/api/chaos/trigger \
  -H 'Content-Type: application/json' \
  -d '{"channel": 2}'

# Resolve
curl -X POST http://<host>/api/remediate/2
```

---

## Fault Channels (Space Scenario Example)

Each scenario defines 20 fault channels. Here are the channels for the default **space** scenario:

| Ch | Name | Subsystem | Cloud |
|----|------|-----------|-------|
| 1 | Thermal Calibration Drift | propulsion | AWS |
| 2 | Fuel Pressure Anomaly | propulsion | AWS |
| 3 | Oxidizer Flow Rate Deviation | propulsion | AWS |
| 4 | GPS Multipath Interference | guidance | GCP |
| 5 | IMU Synchronization Loss | guidance | GCP |
| 6 | Star Tracker Alignment Fault | guidance | GCP |
| 7 | Flight Termination System Check Failure | safety | Azure |
| 8 | X-Band Packet Loss | communications | GCP |
| 9 | UHF Antenna Pointing Error | communications | GCP |
| 10 | Payload Thermal Excursion | payload | GCP |
| 11 | Payload Vibration Anomaly | payload | GCP |
| 12 | Pad Hydraulic Pressure Loss | ground | AWS |
| 13 | Relay Packet Corruption | relay | Azure |
| 14 | Ground Power Bus Fault | ground | AWS |
| 15 | Range Safety Tracking Loss | safety | Azure |
| 16 | Cross-Cloud Relay Latency | relay | Azure |
| 17 | Sensor Validation Pipeline Stall | validation | Azure |
| 18 | Calibration Epoch Mismatch | validation | Azure |
| 19 | S-Band Signal Degradation | communications | GCP |
| 20 | Weather Station Data Gap | ground | AWS |

See [docs/CHANNEL_REFERENCE.md](docs/CHANNEL_REFERENCE.md) for full details. Other scenarios have different channel names and subsystems appropriate to their industry.

---

## API Reference

### Health & Status

```
GET  /health                        # Health check
GET  /api/status                    # All services status
GET  /api/scenarios                 # List available scenarios
GET  /api/scenario                  # Active scenario details
```

### Deployments

```
GET  /api/deployments               # List active deployments
POST /api/deployments/{id}/stop     # Stop a deployment
DELETE /api/deployments/{id}        # Remove a deployment
```

### Setup & Deployment

```
POST /api/setup/test-connection     # Test Elastic Cloud connectivity
POST /api/setup/launch              # Deploy a scenario to Elastic
GET  /api/setup/progress            # Deployment progress (SSE)
GET  /api/setup/detect              # Auto-detect Elastic credentials
GET  /api/setup/env-creds           # Check if env creds are pre-configured
GET  /api/setup/active-deploys      # List in-progress deployments
GET  /api/setup/auto-deploy         # Auto-deploy status (in_progress, deployment_id)
POST /api/setup/teardown            # Tear down Elastic resources
POST /api/setup/stop-and-teardown   # Stop telemetry and tear down
GET  /api/setup/teardown-progress   # Teardown progress
GET  /api/setup/active-teardowns    # List in-progress teardowns
```

### Chaos Control

```
POST /api/chaos/trigger             # {"channel": 1}
POST /api/chaos/resolve             # {"channel": 1}
GET  /api/chaos/status              # All channels
GET  /api/chaos/status/{channel}    # Single channel
POST /api/chaos/spikes              # Inject infrastructure spike
GET  /api/chaos/spikes              # Current spike state
GET  /api/chaos/session/validate    # Validate chaos session token
```

### Remediation

```
POST /api/remediate/{channel}       # Resolve a fault channel
POST /api/daily-update              # Trigger the daily-update workflow
```

### Notifications

```
POST /api/notify/email              # Send email notification
GET  /api/user/info                 # Current user info
```

---

## Project Structure

```
elastic-launch-demo/
├── app/
│   ├── main.py                      # FastAPI entry point — all routes
│   ├── config.py                    # Environment config + scenario loading
│   ├── telemetry.py                 # OTLPClient (direct HTTP to Elastic)
│   ├── trace_context.py             # Log-trace correlation (shared context)
│   ├── context.py                   # ScenarioContext (per-deployment state)
│   ├── instance.py                  # ScenarioInstance (running deployment)
│   ├── registry.py                  # InstanceRegistry (multi-tenancy)
│   ├── store.py                     # DeploymentStore (SQLite persistence)
│   ├── op_queue.py                  # Remediation operation queue
│   ├── services/                    # 9 simulated microservices
│   │   ├── base_service.py          # Abstract base with telemetry helpers
│   │   ├── manager.py               # Service lifecycle manager
│   │   └── *.py                     # Individual service implementations
│   ├── chaos/                       # Chaos injection system
│   │   ├── controller.py            # Channel state management
│   │   ├── channels.py              # Channel definitions helper
│   │   └── remediation_poller.py    # Polls op_queue and calls controller.resolve()
│   ├── selector/static/             # Scenario selector UI (front page)
│   ├── chaos_ui/static/             # Chaos controller UI
│   └── notify/                      # Notification handlers
│       ├── email_handler.py         # Elastic Cloud SMTP
│       ├── twilio_handler.py        # SMS + voice via Twilio
│       └── slack_handler.py         # Slack webhooks
├── scenarios/
│   ├── base.py                      # BaseScenario ABC
│   ├── __init__.py                  # ScenarioRegistry — discovers scenarios/*/scenario.yaml
│   ├── space/                       # NOVA-7 Space Mission
│   │   ├── scenario.yaml            # identity, infra, agent, KPIs, trace attrs
│   │   ├── channels/                # 20 fault channel YAML files
│   │   └── services/                # 9 per-service YAML files (identity + telemetry DSL)
│   ├── fanatics/                    # Fanatics Collectibles (sports streaming)
│   ├── financial/                   # Financial Trading Platform
│   ├── healthcare/                  # Healthcare Systems
│   ├── gaming/                      # Live Gaming Platform
│   ├── banking/                     # Retail Banking Platform
│   ├── ecommerce/                   # Global Commerce Platform
│   ├── manufacturing/               # Manufacturing Operations Platform
│   └── gcp/                         # Google Cloud Network Operations
├── log_generators/
│   ├── host_metrics_generator.py    # system.* host metrics (3 hosts)
│   ├── k8s_metrics_generator.py     # Kubernetes node/pod/container metrics
│   ├── nginx_metrics_generator.py   # Nginx receiver metrics
│   ├── nginx_log_generator.py       # Nginx access/error logs
│   ├── mysql_log_generator.py       # MySQL query logs
│   ├── vpc_flow_generator.py        # VPC flow logs
│   ├── jvm_metrics_generator.py     # JVM metrics for Java services
│   ├── raw_access_log_generator.py  # Raw HTTP access logs
│   ├── infra_topology.py            # Infrastructure topology helpers
│   └── trace_generator.py           # Distributed traces
├── elastic_config/
│   ├── deployer.py                  # Orchestrates all deploy/teardown steps
│   ├── deployer_base.py             # Shared mixin base
│   ├── deployer_agent.py            # AI agent + tools + skills
│   ├── deployer_aiops.py            # Logs ML jobs (rate + categorization)
│   ├── deployer_alerting.py         # Alert rules
│   ├── deployer_apm.py              # APM rollup + anomaly detection
│   ├── deployer_backfill.py         # ECS log backfill
│   ├── deployer_dashboard.py        # Executive dashboard import
│   ├── deployer_integrations.py     # OTel integrations install
│   ├── deployer_kb.py               # Knowledge base indexing
│   ├── deployer_otlp.py             # OTLP endpoint derivation + verification
│   ├── deployer_platform.py         # Platform settings
│   ├── deployer_skills.py           # Agent Builder skills
│   ├── deployer_slos.py             # SLO creation
│   ├── deployer_streams.py          # Significant events + stream forks
│   ├── deployer_views.py            # Data views
│   ├── deployer_workflows.py        # Workflow YAML deploy
│   ├── apm_rollup.py                # APM rollup data helper
│   ├── workflows/                   # Workflow YAML templates
│   └── dashboards/                  # Executive dashboard generator
├── docs/
│   ├── DEMO_SCRIPT.md               # Presenter talk track
│   ├── SE_QUICK_START.md            # Quick start for SEs
│   ├── SETUP_GUIDE.md               # Full deployment guide
│   ├── CHANNEL_REFERENCE.md         # Channel details (space scenario)
│   └── TROUBLESHOOTING.md           # Common issues and solutions
├── start.sh                         # Start the app (backgrounds, logs to /tmp/nova7.log)
├── stop.sh                          # Stop the app
├── validate.sh                      # Comprehensive validation
├── Dockerfile                       # Container image
├── requirements.txt                 # Python dependencies
├── .env.example                     # Environment variable template
├── AGENTS.MD                        # Full Kibana/ES API reference
└── README.md                        # This file
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [SE Quick Start](docs/SE_QUICK_START.md) | Get a demo running in under 10 minutes |
| [Setup Guide](docs/SETUP_GUIDE.md) | Full deployment from scratch |
| [Demo Script](docs/DEMO_SCRIPT.md) | Presenter talk track (3 acts) |
| [Channel Reference](docs/CHANNEL_REFERENCE.md) | All 20 fault channels (space scenario) |
| [Troubleshooting](docs/TROUBLESHOOTING.md) | Common issues and solutions |
| [AGENTS.MD](AGENTS.MD) | Full Kibana and Elasticsearch API reference |

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Application | Python 3.11, FastAPI, uvicorn |
| Telemetry Protocol | OpenTelemetry (OTLP JSON over HTTP, direct to Elastic) |
| HTTP Client | httpx with HTTP/2 |
| Observability Platform | Elastic Cloud (Elasticsearch, Kibana) |
| AI Agent | Elastic Agent Builder (tools, KB, system prompt) |
| Workflows | Elastic Workflows (alert → search → AI agent → remediate → email) |
| Persistence | SQLite (deployment state) |
| Deployment | EC2 (uvicorn on port 8080) |
| Notifications | Elastic Cloud SMTP, Twilio (SMS + voice), Slack Webhooks |
