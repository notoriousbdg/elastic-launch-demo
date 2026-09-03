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

## Phase 2: Pick a reference scenario id

Run the following to find the most recently committed scenario, and record its id — but **do not read its files**:

```bash
for id in $(ls scenarios/*/scenario.yaml 2>/dev/null | xargs -I{} dirname {} | xargs -I{} basename {}); do
  git log -1 --format="%cI $id" -- scenarios/$id/scenario.yaml
done | sort -r | head -1
```

You will pass this id to subagents in Phase 5 so each reads one concrete reference file of its own type (a single channel YAML or service YAML from that scenario — not the full scenario.yaml).

The spec for `scenario.yaml` is in [contract/scenario.md](.claude/skills/add-scenario/contract/scenario.md). Read it now as your checklist for Phase 5's `scenario.yaml` authoring.

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

# Add a design_notes block — used by Phase 5 subagents for domain context
design_notes:
  vertical: <industry label>
  primary_workflow: <user-visible business flow>
  pain_points:
    - <pain 1>
    - <pain 2>
  tone: <optional theme hint>

services:       # exactly 9, in desired sort_order
  - name: <svc>
    cloud_provider: aws | gcp | azure
    cloud_region: <region>
    cloud_platform: aws_ec2 | gcp_compute_engine | azure_vm
    cloud_availability_zone: <az>
    subsystem: <subsystem>
    language: python | java | go | dotnet | rust | cpp
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

---

## Phase 5: Flesh out all properties — fan out to subagents

The 30 generated files are self-contained by design (GUARDRAILS §9/§10). Delegate their authoring to disposable subagents launched in **one message** so they run concurrently. You retain `scenario.yaml` and run verification.

### What you author directly (main agent)

Polish `scenario.yaml`: identity, hosts, k8s_clusters, agent_config, executive KPIs, raw_log_profile, trace_attributes. Use [contract/scenario.md](.claude/skills/add-scenario/contract/scenario.md) as your checklist.

Completion checklist for `scenario.yaml`:

| Field | What to write |
|---|---|
| `assessment_tool_config.description` | Domain-specific description of what the assessment evaluates (health dimensions, risk surfaces). ≥2 sentences. |
| `agent_config.id` / `.name` / `.assessment_tool_name` | Auto-derived from namespace — verify they read naturally for the vertical. |
| `scenario_description` | 1–2 sentences describing the platform and its operational context. |
| `executive_dashboard_intro` | Bold label + brief description of the KPIs shown and their thresholds/significance. |
| `executive_kpi_sections[].header` | Replace `"TODO: rename to match your vertical"` with a vertical-specific section title. |
| KPI spec titles (e.g. `"Primary KPI (unit)"`) | Replace with real metric names and units (e.g. `"Orders per Hour (ord/hr)"`). |
| `executive_kpi_emissions[].unit` | Replace every `TODO` unit with the real unit string (e.g. `"ord/hr"`, `"%"`, `"ms"`). |
| `executive_kpi_emissions[].value` ranges | Scaffolder seeds realistic ranges by field name. Verify they're plausible for your vertical and adjust if needed. |
| `executive_trend_charts[0].y_label` | Replace `TODO` with the primary KPI unit. |
| `raw_log_profile.paths` / `.change_point_path` | Replace `/api/v1/TODO` paths with real API paths for the vertical. |
| `trace_attributes.services[*]` | Expand each service from 2 to 4–5 domain-specific attributes. |

### What subagents author (delegate all at once)

Launch all 7 agents in a **single message** — they run concurrently and their file paths are disjoint:

**4 channel agents** (5 channels each):
- Agent C1: `channels/01-*.yaml` through `channels/05-*.yaml`
- Agent C2: `channels/06-*.yaml` through `channels/10-*.yaml`
- Agent C3: `channels/11-*.yaml` through `channels/15-*.yaml`
- Agent C4: `channels/16-*.yaml` through `channels/20-*.yaml` *(auto-remediate — see §6 rule below)*

**3 service agents** (3 services each, grouped by cloud):
- Agent S1: the 3 AWS services
- Agent S2: the 3 GCP services
- Agent S3: the 3 Azure services

### Subagent prompt template

Use this prompt for **each channel agent** (fill in `<BATCH>`, `<FILES>`, `<REF_ID>`):

```
You are authoring fault channel YAML files for a new scenario. Your job is to replace every stub
placeholder with realistic, domain-appropriate content. Write the files directly; return a one-line
summary per file (filename + "done"), no file contents.

## Context
Read `scripts/<id>-brief.yaml` for scenario identity and domain context (design_notes block).
Read ONE reference channel from `scenarios/<REF_ID>/channels/` — pick any channel whose
error_type matches a similar subsystem to yours. Read ONLY that one file; do not glob the folder.
Read `.claude/skills/add-scenario/contract/channels.md` for the schema and DSL reference.

## Your files
<BATCH> (<FILES>)

## Completion checklist (all items required — zero TODO tokens may remain)
- `rca_clues`: 2–3 key/value clues per affected and cascade service. Use the YAML DSL reference
  in contract/channels.md. Every affected_services and cascade_services entry must have clues.
- `error_message` + `fault_params` parity: every {placeholder} has a matching key in fault_params.
- `investigation_notes`: 5–6 numbered steps referencing specific field names and {placeholder} values.
- `correlation_attr.value`: realistic version string, config value, or identifier.
- `description`: one-sentence summary of what goes wrong and why the user would observe it.

## Hard rules (non-negotiable)
- No real company names or proprietary jargon (generic vertical terms only)
- fault_params parity: every {placeholder} in error_message + stack_trace must have a key in fault_params IN THE SAME FILE
- Zero TODO tokens may remain
- Do not read or modify any file outside scenarios/<id>/channels/
```

Add this additional rule for **Agent C4** (channels 16–20 only):

```
GUARDRAILS §6 — auto-remediate channels: these faults must be plausibly auto-remediable
by a real SRE team without human approval. Good: pod restart on OOM, cache flush after
TTL, TLS cert auto-renewal, circuit breaker trip, connection pool restart. Bad: data
corruption, fraud anomaly, multi-service cascade, first-seen errors. Test: "Would a
well-run SRE team add this to their auto-remediation runbook?" If caveats → escalate
to channels 1–15 instead.
```

Use this prompt for **each service agent** (fill in `<CLOUD>`, `<SERVICES>`, `<REF_ID>`):

```
You are authoring service YAML files for a new scenario. Your job is to replace every stub
placeholder with realistic, domain-appropriate content. Write the files directly; return a
one-line summary per file (filename + "done"), no file contents.

## Context
Read `scripts/<id>-brief.yaml` for scenario identity and domain context (design_notes block).
Read ONE reference service from `scenarios/<REF_ID>/services/` — pick a service in a similar
subsystem to yours. Read ONLY that one file; do not glob the folder.
Read `.claude/skills/add-scenario/contract/services.md` for the schema and DSL reference.

## Your files
<CLOUD> services: <SERVICES>

## Completion checklist (all items required — zero TODO tokens may remain)
- `steps:` body: replace the stub skeleton with domain-specific telemetry — realistic latency
  distributions, domain metric names, log messages with actual business context. Remove the
  `# TODO: Replace these stub steps...` comment.
- `topology`: replace `[]` with realistic downstream call edges matching the primary workflow
  (e.g. [[fraud-detector, /api/v1/fraud/check, POST]]). Build call chains that match the service
  architecture from the brief.
- `entry_endpoints` (sort_order:1 service only): replace stub /health+/process routes with 4–6
  real business endpoints (e.g. /api/v1/checkout, /api/v1/search). All traces root here.
- `db_operations`: realistic SQL matching the service's domain (SELECT/INSERT/UPDATE on named tables).

## Hard rules (non-negotiable)
- No real company names or proprietary jargon (generic vertical terms only)
- language must be one of: python | java | go | dotnet | rust | cpp
- Each service is fully defined in services/<svc>.yaml — do NOT add service data to scenario.yaml
- Only .yaml files — no .py files, no __init__.py
- Zero TODO tokens may remain
- Do not read or modify any file outside scenarios/<id>/services/
```

---

## Phase 6: Validate

After the main agent and all subagents complete, run these checks. On failure, dispatch a fix-up subagent per affected file (up to 3 rounds); if errors remain after 3 rounds, report them to the user.

**Auto-discovery check:**
```bash
python3 -c "
from scenario_engine import list_scenarios
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
- Language is in allowed set (`python | java | go | dotnet | rust | cpp`)
- `assessment_tool_config` has a non-empty `id` and `description`
- `agent_config` has all four fields (`id`, `name`, `assessment_tool_name`, `system_prompt`); `assessment_tool_name` equals `assessment_tool_config.id`
- `agent_config.system_prompt` mentions all 20 channel `error_type`s
- **Zero `TODO` placeholders** in `scenario.yaml`, all `channels/*.yaml`, and all `services/*.yaml`
- `k8s_clusters` entries have `provider`, `region`, and `platform`
- `trace_attributes.services` keys match actual service names
- `executive_kpi_emitter_service_name` names a real service; KPI sections/emissions in sync
- Sort_order:1 service has `entry_endpoints` defined
- `affected_services`/`cascade_services`/`k8s_clusters.services` reference real service names
- All `{placeholder}`s in `error_message`+`stack_trace` have a matching `fault_params` key
- Every service has a non-empty `steps:` body (unless `generates_traces: false AND emit_fault_logs: false`)

Warnings (exit 0, but fix before demo):
- Host names should start with `{scenario_id}-` to avoid log cross-contamination
- All 20 channels should have populated `rca_clues` (not empty `{}`)
- At least one service should have a non-empty `topology`
- Sort_order:1 entry_endpoints should not be the generic `health`/`process` stubs
- Each service should have ≥ 3 `trace_attributes` entries

**Fix-up subagent prompt (use when verifier reports errors for specific files):**

```
Fix validation errors in these scenario files. Do not change anything else.

Errors to fix:
<paste verifier output lines for the affected files>

Files to fix: <list>

Hard rules: zero TODO tokens, fault_params parity, language allowlist (python|java|go|dotnet|rust|cpp),
no .py files in services/, do not modify files outside scenarios/<id>/.
Return a one-line summary per file (filename + what was fixed).
```

**Service telemetry DSL check:**
```bash
python3 -c "
from pathlib import Path
from scenario_engine.yaml_scenario import load_yaml_scenario
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
Output must show only files under `scenarios/<id>/` and `scripts/<id>-brief.yaml`. If any other file appears, stop and alert the user.

**Report:** Summarize what was generated, the file count, any validation warnings, and how to activate:
```
ACTIVE_SCENARIO=<id> ./start.sh
```
