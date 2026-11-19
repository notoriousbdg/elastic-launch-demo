# upgrade-telemetry-generator GUARDRAILS

1. **validate** is strictly read-only.
2. **upgrade** may edit only `generators/<id>/` (SPEC + generator.py + tests as
   required). Reuse `add-telemetry-generator/scripts/`. Never edit out-of-the-box
   package content.
   **Do not edit generator.py until** `$OUT/alert_required_fields.json` exists
   (from `extract_alert_esql_fields.py`) **and** `verify_alert_emit.py` has been
   run; also prefer `$OUT/merged/required_fields.json` +
   `verify_metric_coverage.py --check-emit` when those scripts exist in the
   checkout. Merged dashboards/alerts/SLOs are the emit set;
   Authority A is types/units/attrs — not a hard intersection allowlist.
   Follow [ALERTS.md](ALERTS.md) / [../add-telemetry-generator/ALERTS.md](../add-telemetry-generator/ALERTS.md)
   and add-telemetry-generator `GROUNDING.md` classic pitfalls when present
   (post-pipeline shape, dimension series including presence/absence,
   case-sensitive KQL literals, **exact resource-type literals (no ARM
   “completion”)**, Discover columns, global filters, geo, baseline failures,
   `-default` dual-write, **histogram / special-mapping race + `backfill()`
   stream reset**, **cross-package shared-field types under one data view**
   (`azure.dimensions` object vs flattened — `@custom` + rollover + prune
   stale indices), **hand-emitted `user_agent` / audit constants /
   administrative `region_name`**) and OTel pitfalls (resource vs datapoint
   attrs, bare metrics missing `metric_datapoint_attrs_required` dimensions,
   Lens `sourceField` attrs, colocation, counters, **`delta_required`**,
   **`metric_stat_pairs`**, **`gauge_required`**, **`over_time_metrics`**,
   **`control_fields`** (including snake_case Options List `field_name`),
   **`null_absent_required`**, **`from_streams` / `otlp_incompatible_*` /
   `requires_es_bulk_for_stock_dashboards`**, **`manifest_vs_dashboard`**,
   **`ratio_float_fields`**, **`case_rate_expressions` /
   `prefer_double_metrics`**, **`body_structured_required` /
   `requires_attributes_dual_write` / `attributes_dual_write_keys` /
   `passthrough_top_level_keys` / `map_geo_fields` /
   `map_apply_global_time_false` / `suspicious_lens_metric_ops`** (flattened
   body ladder + Maps geo + volume ops)). Always run
   `extract_dashboard_constraints.py` (and merge) **when present** so upgrade
   sees `gauge_required` / `over_time_metrics` / `delta_required` /
   `metric_stat_pairs` / `control_fields` / `null_absent_required` /
   `from_streams` / `otlp_incompatible_streams` / `manifest_vs_dashboard` /
   `body_structured_required` / `passthrough_top_level_keys` /
   `map_geo_fields` / `map_apply_global_time_false` /
   `suspicious_lens_metric_ops`. Read merged `metric_datapoint_attrs_required`,
   `histogram_metric_ops`, `requires_package_template_before_write`,
   `metric_stat_pairs`, `gauge_required`, `over_time_metrics`, `delta_required`,
   `control_fields`, `null_absent_required`,
   `requires_es_bulk_for_stock_dashboards`, `body_structured_required`,
   `attributes_dual_write_keys`, `passthrough_top_level_keys`, and
   `map_geo_fields` during upgrade.
   When `requires_es_bulk_for_stock_dashboards` is true, keep
   `client="es_bulk"` into the stock stream (do not switch to OTLP or patch
   dashboards). When `manifest_vs_dashboard.mismatch` later clears because
   upstream panels moved to `.otel`, flip back to `otlp`. When CloudWatch pairs exist, run
   `verify_cloudwatch_emit.py --check-emit-stats` against merged
   `required_fields.json` when present. When histogram fields exist **or** companions share
   `metrics-*`, run `verify_classic_mappings.py` on a live cluster (include
   `--index-pattern metrics-* --shared-conflict-field azure.dimensions` for
   Azure companions) before calling percentile / Overview panels done.

   **Alert ES|QL gate (learned OOTB alert backfill 2026-08):**
   - Always run `extract_alert_esql_fields.py` + `verify_alert_emit.py`.
   - Emit every `COALESCE` operand column; unknown columns fail verification
     even inside COALESCE.
   - `INCREASE`/`RATE` → `counter_long` (+ sticky gauge→counter reset). When
     the same field is also used with dashboard `SUM`/`AVG`, **alert wins**.
   - CloudWatch: `Namespace` + `MetricName` on every datapoint
     (`build_cloudwatch_metric`).
   - Template `FROM` outside `SPEC.datasets` → companion emit or fail as
     package-template mismatch — never edit OOTB rules.
   - Optional live validate: Kibana rules with `verification_exception` →
     **upgrade** generators.

   **CloudWatch / Lens dashboard emit pitfalls (learned `aws_bedrock_agentcore`):**
   - Enumerate **every** panel column KQL filter that asserts dimension
     presence (`field : *`) or absence (`NOT field : *`). Emit one document
     series per distinct presence/absence combination — not a single
     “full dimensions” doc.
   - Honor **dashboard-level** `searchSourceJSON` filters (and Options List
     `control_fields`) on those series. Example: Gateway global
     `Operation:InvokeGateway` excludes a Method-only series that omits
     `Operation`, so Lambda/MCP target KPIs stay at 0 even when
     `TargetType_*` is present on the doc.
   - Lens **`emptyAsNull: true`** on sum/count maps a literal aggregated
     **`0` → `null`** in tables (Kibana `metric_agg_type` getValue). Healthy
     baseline must emit small non-zero error/throttle/count metrics so
     columns populate, while staying **under** package alert thresholds.
     Chaos-off / `chaos is None` → baseline; when a chaos dict is active,
     missing keys stay `0` (do not re-apply baseline on top of a fault).

   **Multi-dashboard packages (learned `aws_ec2_otel`):**
   - Ground against **every** package dashboard (Overview **and** Detail /
     drill-down), not only the primary Overview. Instance Detail may require
     metrics Overview never mentions (e.g. `EBSIOBalance%`, classic
     `DiskReadBytes` / `DiskWriteBytes` alongside `EBS*`).
   - CloudWatch alert ES|QL often filters `Namespace` + `MetricName` attrs —
     emit those datapoint attributes (see `aws_ecs_otel`), not only the
     `amazonaws.com/AWS/...` metric name. Prefer `build_cloudwatch_metric`.

   **Dimension absence filters (learned `aws_bedrock`):**
   - Some Lens metrics filter `field:* AND NOT other_field:*`. Emitting the
     “other” dimension on the same series zeros the panel (e.g. Overview
     Generated images requires `model_id` **without** `quality` /
     `image_size`). Prefer omitting unused dimension keys entirely — do not
     write JSON `null` for them.

   **OTel receiver attribute `name_override` (learned `apache_otel`):**
   - Datapoint attrs must match receiver metadata (`state`), not invented
     compound keys (`workers.state` / `scoreboard.state`). Dashboard ES|QL uses
     `attributes.state`; wrong keys → `Unknown column [attributes.state]`.
   - Honor receiver **resource attributes** (`apache.server.name`,
     `apache.server.port`) via `build_signal_resource(..., extra_attrs=...)`
     — Options Lists / alert `BY` clauses depend on them.
   - Match metric **type** from receiver docs (workers/scoreboard =
     non-monotonic sum, not gauge).
   - Cumulative counters used with `RATE()` must start near zero; a random
     multi-million initial value creates a one-bucket spike that makes the
     real rate look like flat 0 on autoscaled charts.

   **Table KEEP columns across categories (learned `azure_activity_otel`):**
   - Overview tables that KEEP identity columns (`principal.type`, `user.id`,
     `network.peer.address`) for all `azure.category` values need those attrs
     on every category builder (ServiceHealth / ResourceHealth /
     Recommendation / Alert), not only Administrative — otherwise ~half the
     Recent Activity rows show nulls.
   - Alert `VALUES(azure.identity.authorization.evidence.role)`,
     `azure.resourcehealth.type`, `azure.servicehealth.is_synthetic` /
     `tracking.id` need the same treatment on matching category builders.

   **Billing / cost pies (learned `azure_billing` / `azure_openai`):**
   - Thin `llm_profile` (1 sub, 1 RG, 1 resource) makes overview pies a single
     100% slice. Enrich inside the generator (extra RGs/resources/models/subs)
     and skew weights. Emit `forecast_cost`. Mix CognitiveServices OpenAI
     lines (for OpenAI Billing filter) with other Azure SKUs for Billing
     overview. Keep baseline 429s rare (quota alert is COUNT > 5).

   **RATE spike-then-flatline (learned `apache_otel` / `cockroachdb_otel`):**
   - `CounterState()` defaults to 50k–5M. First `RATE()`/`INCREASE()` bucket
     after start/restart dwarfs real tick deltas → charts look dead. Pass
     `initial=` near zero. Replication-style counters that often tick with
     `drift=0` stay flat after that spike — give them steady non-zero drift.

   **Do not compound `business_load()` into gauge state (learned `docker_otel`):**
   - `util *= load` each tick decays utilization to the floor (0.001) under
     off-peak load and makes CPU charts look like all zeros. Walk a baseline
     and apply load as an emit scale, or pull toward a load-scaled setpoint.

   **Ratio panels + integer division (learned `mongodb_otel` / `apache_tomcat_otel`):**
   - Stock ES|QL like `current / (current + avail)` without `TO_DOUBLE` does
     `long / long` → truncates to 0 (Capacity utilization shows 0% / 70%).
     Emit the ratio operands as OTLP `asDouble` and clear sticky `long`
     mappings (delete the metrics data stream on startup when field_caps
     still report `long`), same as Tomcat. Do not patch OOTB dashboards.

   **`body.structured` flattened (learned `nginx_ingress_controller_otel`):**
   - OTel intake maps kvlist bodies to `body.structured` **flattened**. Lens
     cannot sum/filter `body.structured.*` (`illegal_argument_exception` on
     `_keyed`). Do **not** rewrite stock dashboards. Override
     `logs-otel@custom` so `body.structured` is a dynamic `object` with typed
     leaves, reset sticky flattened data streams, and dual-write the same keys
     to `attributes` (passthrough). Keep package panels on `body.structured.*`.

   **ES|QL `AVG`/`SUM` reject `counter_long` (learned `redisenterprise_otel`):**
   - Stock ES|QL `AVG(metric)` / `SUM(metric)` fails with
     `verification_exception` when the field is OTLP cumulative
     `counter_long`. Panel ops win over receiver “counter” semantics: emit
     those series as **gauge doubles** (`gauge_required` /
     `prefer_double_metrics` / suspicious AVG|SUM ops in merged
     constraints). Delete/reset the metrics data stream on startup when
     field_caps still show `counter_long`/`long`. Close `Unknown column`
     gaps from inventory (attrs like `path`/`endpoint`/`port`, missing
     `bdb_*` / listener / shard metrics). Multi-field ES|QL rows read one
     document: same-unit, same-type, same-attribute metrics share one once the
     generator is on the scrape clock; add co-located ES bulk docs only for the
     pairs `scripts/audit_colocation.py` reports UNCOVERED.
   - **Alert exception:** `INCREASE`/`RATE` on the same field → keep
     `counter_long` and reset sticky gauges ([ALERTS.md](ALERTS.md)).

   **Co-located bulk docs need gauge templates (learned `rabbitmq_otel`):**
   - Node Resource Summary–style panels use TS `LAST_OVER_TIME` across many
     gauge fields on one doc. Bulk-indexed co-located docs without
     `gauge_long` dynamic templates map as plain `long` (no
     `time_series_metric: gauge`). Mixed with OTLP gauges → query returns
     0 rows (“No results found”) despite documents existing. Always set
     gauge templates on bulk write paths; reset the stream if mappings were
     poisoned before the fix.

   **Empty-string table attrs → UI null (learned `postgresql_otel`):**
   - Active Queries Wait Type / Wait Event (`attributes.postgresql.wait_*`)
     and similar KEEP columns show `(null)` when the generator emits `""`.
     Always populate non-empty documented values for every table column the
     stock panel reads.

   **Null `message` on shared host logs (learned `traces` / `system_otel`):**
   - Host Details `message` aliases `body.text`. Span exception events with
     no body in `logs-generic.otel` render as `(null)`. Emit OTLP error
     **logs** with `body` = exception message; keep span ERROR +
     `exception.*` attrs; do not attach empty exception events onto shared
     log streams.

   **Billing `dropPartials` 24h histograms (learned `azure_billing`):**
   - Forecast / Daily usage bucket `usage_date` / `usage_start` at 24h with
     `dropPartials: true`. Emitting wall-clock `usage_*` = now drops the
     only bucket. Dual-write usage (completed UTC day bounds + pretax only)
     vs forecast docs (`actual_cost` XOR `forecast_cost` + day-start
     `usage_date`); keep `@timestamp` recent; span multiple past days.
     If Daily usage costs look blank, set **Last 7 days** first — not a
     generator bug. **`now-15m` / `now-1h` / rolling `now-24h`→`now` cannot
     populate these panels** (no complete UTC day bucket). Forecast needs
     `to` in the future (e.g. `now-7d`→`now+15d`).

   **Resetting counter start kills `RATE()` (learned `mysql_otel`):**
   - Local builders that set `startTimeUnixNano = now - 60s` every scrape
     make 15m `SUM(RATE(metric))` panels empty, and keep sums out of the
     scrape's shared document. Use `CounterState` + stable
     `start_time_ns` via `db_common.build_cumulative_sum`. Align OTel enums
     (`page_operations` created/read/written; buffer pool
     read_requests/reads; etc.). Start counters near zero.
   - Queries dashboard: `sum_timer_wait` / `timer_wait` are **delta seconds**
     (then `* 1000` → ms). Emitting ns-scale values → absurd Total time /
     Avg Query Time (years / billions of ms). Use unit `s`.

   **Intermittent boolean activity gauges (learned `redisenterprise_otel`):**
   - `*_in_progress` charts look “broken” when always 0. Rare short pulses
     (AOF rewrite / RDB bgsave) animate demos; keep failure counters at 0
     unless chaos asks for them.
   **Package double-percent Used % (learned `cockroachdb_otel`):**
   - Storage `EVAL used/total * 100` with Lens format `percent` double-scales
     (~35 → 3500%). Generator capacity emit is fine — do not “fix” via
     emit. Store Summary nulls still need co-located
     `ranges`+`replicas`+`replicas_leaseholders` bulk docs (`gauge_long`).

   **Instance Detail Status `"N/A"` (learned `aws_ec2_otel`):**
   - Header Status is `N/A` when `COUNT_DISTINCT(InstanceId) > 1`. Multi-ID
     fleets without an InstanceId filter are expected; StatusCheckFailed
     emit is already correct. Drill down or set a single `instance_ids`.

   **Maps geo attrs stringified (learned `nginx_ingress_controller_otel`):**
   - Maps bind `attributes.geo.location` as geo_point. Dict lat/lon values
     become `stringValue: "{'lat':…}"` via `_format_attributes` → empty map.
     Emit `"lat,lon"` (or kvlist doubles); reset if mapped as keyword.
     Do not trust `"lat" in geo_string` in tests.

   **PostgreSQL connection utilization >100% (learned `postgresql_otel`):**
   - Panel is `SUM(backends)/MAX(connection.max)` vs goal 80%. Emitting
     per-DB `max_conn * util` independently overshoots (e.g. 175%). One
     instance max_conn; partition total backends across DBs.

   **Negative `DELTA()` KPIs (learned `apache_tomcat_otel`):**
   - `SUM(DELTA(failureCount))` goes largely negative when gauges reseed
     lower on restart. Seed DELTA gauges from `int(time.time()) + …`.

   **FD util 0% with goal 80% (learned `apache_tomcat_otel`):**
   - `open_fds / max_fds` as long → 0. Sticky `gauge_long` still divides as
     int — require `gauge_double`/`asDouble` + stream reset. The `/ 80%` is
     the panel goal tick, not a second utilization reading.
   - Co-located Open+Max (and other empty-attr) bulk docs that share the
     **same `@timestamp` ms** collide in TSDS (`409 version_conflict`) — only
     the first create in the batch lands (often heap), so FD util stays
     `max_fds=null` → 0%. Stagger `@timestamp` by `ts + i` per bulk doc.

   **Bulk co-location must keep running (learned `rabbitmq_otel` /
   `redisenterprise_otel`):**
   - After a TSDB dimension widen or mapping fix, delete the stream and
     clear ESBulk backoff so co-located docs emit every tick (not once).
     Poison = plain long without `time_series_metric:gauge` or
     `counter_long`; healthy `long`+gauge is OK. Listener/shard attrs
     (`endpoint`/`port`/`node`) must match on OTLP and bulk.

   **Sibling `WHERE A` then read B/C (learned `vsphere_otel`):**
   - Cluster Headroom / Host Top-N / VM Swapped+Ballooned / Disk Used % all
     filter one metric then STATS siblings — siblings in another OTLP document
     (different unit, type or attributes) → nulls or “No results”. Co-locate
     those via ES bulk (`gauge_long`/`gauge_double`).
     For panels that `SUM(LAST_OVER_TIME(...))` (cluster effective/limit,
     host usage/capacity), use **bulk-only** for those fields — OTLP+bulk
     doubles the SUM. VM latency+utilization must share `object` dims.
3. Never edit `app/`, `scenario_engine/`, `scenarios/`, or Helm/Dockerfile from this skill.
4. Never bump `schema_version` on a SPEC without applying the corresponding migrations.
5. Never disable Launch guards or bypass import allowlists.
6. After **upgrade**, remind the user to click **Refresh list**.
7. Do not invent migration steps — follow `guidance` in `schema.yaml` when applying engine migrations during upgrade.
8. Brand-new generators belong to **add-telemetry-generator**, not this skill.
9. Changing `generator_engine/schema.yaml` is a rare maintainer edit (see SKILL
   Maintainer note), not a third user-facing capability. After that edit, run
   **upgrade** on affected generators.
10. **upgrade** must leave `len(SPEC.impact_keys) >= 2` on every generator.
    When the package has alert rules, include a **1:1** key per alert (and per
    SLO); if only one alert exists, add another emit knob so total ≥ 2. Every
    key applied in `run()`, `wants_chaos=True`, healthy baseline when chaos is
    off. No declare-without-apply. No fan-out mega-fault as the only way to
    fire unrelated alerts. Never edit scenarios to “finish” alert wiring.
11. **upgrade** is not done until `verify_alert_emit.py` exits 0 for packages
    that ship alerting rule templates (or the package has zero templates).
12. **Scenario identity (branding).** Every value a user can filter or pick on comes from
    `ScenarioIdentity` (`generator_engine/identity.py`), never a literal: hosts, services,
    instances, databases, domains and URLs, buckets, clusters, ARNs, endpoint ids, and cloud
    account, subscription, tenant and project ids. Any resource an upgrade adds (a bucket,
    VNet, replica, endpoint, ...) follows the same rule, and a config override goes through
    `ident.qualify(...)`. Fix hardcoded identity values already in a generator you touch. See
    guardrail 36 in `add-telemetry-generator/GUARDRAILS.md` for the helper table. Exempt:
    public image names, upstream API paths, metric names, and IP addresses.
    **upgrade** is not done until `tests/test_generator_identity.py -k <id>` passes.
13. **Stable alert identity while a fault is active.** A fault keeps the values its alert
    groups on (the rule's `BY` or `KEEP` columns) fixed for as long as it is active. See
    guardrail 37 in `add-telemetry-generator/GUARDRAILS.md`. After a package bump, refresh
    `tests/fixtures/alert_identity.json` with `python3 scripts/snapshot_alert_identity.py <id>`;
    **upgrade** is not done until `tests/test_alert_identity_stable.py -k <id>` passes.
14. **Package SLOs and ML jobs.** SLO templates and ML modules from the generator's packages
    are deployed with it; their filters, groupBy / detector split fields and the
    healthy-baseline SLI are part of the emit set. See guardrail 38 in
    `add-telemetry-generator/GUARDRAILS.md` and [TEMPLATES.md](TEMPLATES.md). After a package
    bump, refresh the fixtures with `python3 scripts/snapshot_package_templates.py <id>`;
    **upgrade** is not done until `tests/test_slo_template_emit.py` and
    `tests/test_ml_module_emit.py -k <id>` pass with no pending entries left for the generator.
15. **Library investigation workflows.** When a Template Library workflow reads the
    generator's dataset, **upgrade** is not done until
    `tests/test_workflow_template_emit.py -k <id>` passes. See guardrail 39 in
    `add-telemetry-generator/GUARDRAILS.md`.

