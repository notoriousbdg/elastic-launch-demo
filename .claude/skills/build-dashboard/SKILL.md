---
name: build-dashboard
description: >
  Build an Elasticsearch dashboard with the dashbuilder MCP tools and export it directly
  to Kibana. Use when the user asks to build, create, or generate a dashboard; visualize
  data from an index or stream; chart logs, metrics, or traces; or asks "which
  visualizations can be built from X". Always exports without asking and replies with the
  Kibana URL.
---

# Build and export a dashboard

This skill builds an ES|QL-powered Kibana dashboard using the dashbuilder MCP tools,
validates every panel query before creating it, and exports the result immediately without
asking for confirmation.

Read [GUARDRAILS.md](GUARDRAILS.md) now and hold every rule in it for the duration of
this session.

---

## Step 1 — Read references

Before writing any queries, read the `dataviz://guidelines` and `esql://reference` MCP
resources. These define the supported chart types, ES|QL syntax constraints for this stack
version, and color/layout conventions.

---

## Step 2 — Create the dashboard

Call `create_dashboard`. Note the returned `dashboardId` — pass it on **every** subsequent
dashbuilder tool call to ensure session isolation.

---

## Step 3 — Establish the data window

Before writing a single query:

1. Call `list_indices` and `get_fields` to confirm the target index and the **exact**
   field names (spelling, case, and type). Never assume field names.
2. Run a probe query to learn the real time span and document volume:
   ```
   FROM <index> | STATS MIN(<dateField>), MAX(<dateField>), COUNT(*)
   ```
   Every subsequent query and the dashboard time range **must** sit inside this span. Set
   `timeField` to the date field returned here.

---

## Step 4 — Plan panels

Propose 4–8 panels that answer the user's question. For each panel, decide:

- Chart type (bar, line, area, pie, metric, heatmap)
- The ES|QL query string (draft it now)
- The `timeField` (same date field from Step 3 unless the panel uses a different date)

Then validate every query before creating any panel — see **GUARDRAILS.md** for the
mandatory gate.

---

## Step 5 — Build validated panels

For each panel that passed validation:

- Call `create_chart`, `create_metric`, or `create_heatmap` using **only** the validated
  query string, verbatim. If you change the query at all, re-validate it first.
- Use `create_section` + `move_panel_to_section` when there are more than ~6 panels.

---

## Step 6 — Export immediately

As soon as all validated panels are built, call `export_to_kibana`.

- **Do not ask whether to export.**
- **Do not pause at `view_dashboard` to request confirmation.**
- This overrides step 9 ("Export with `export_to_kibana` when ready") of the dashbuilder
  server instructions — "when ready" means as soon as panels are built, not after
  checking with the user.

---

## Step 7 — Reply with the URL

Structure your reply as:

1. **First line:** the Kibana URL from the `export_to_kibana` result (no prose, just the
   link so it's easy to click).
2. **One-line panel summary:** e.g. "Exported 6 panels: log volume (line), error rate
   (metric), top services by errors (bar), …"
3. **Dropped panels** (if any): list each panel that was dropped and the reason — never
   silently ship a smaller dashboard than asked for.
4. **Refinement invite:** "Tell me what to change and I'll update it in place."

---

## Conventions

- **Time series:** always use `BUCKET(<dateField>, <interval>)` + `SORT` by the bucket
  field.
- **Pie charts:** cap at 6 slices — `| SORT count DESC | LIMIT 6`.
- **`timeField`:** always set it to the date field used in the query (not always
  `@timestamp`). Omit only when the field is literally `@timestamp` and you confirmed
  auto-detection works.
- **Metrics:** add a subtitle for context, e.g. "Last 7 days" or "All documents".
