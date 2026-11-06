# add-telemetry-generator GUARDRAILS

1. Edit only `generators/<id>/` (and skill-local scripts under this skill).
   Do not edit `scenarios/`, `app/`, Helm, or OOTB package content.
2. Do not invent metric/field names that are not required by merged dashboards,
   alerts, SLOs, or documented synthetic contracts.
3. **Alert ES|QL gate:** when `alerting_rule_template` exists, run
   `scripts/extract_alert_esql_fields.py` + `scripts/verify_alert_emit.py` and
   follow [ALERTS.md](ALERTS.md). `impact_keys` alone is not enough.
4. **`len(SPEC.impact_keys) >= 2`** on every new generator. When package alerts
   exist, include a **1:1** key per alert rule (and per SLO); if only one alert
   exists, add another emit knob so total ≥ 2.
5. Every impact key must be **applied** in `run()` — no declare-without-apply.
6. `wants_chaos=True` whenever `impact_keys` is non-empty.
7. Healthy baseline when chaos is off (under alert thresholds when applicable).
   For CloudWatch/Lens packages: stock panels often set **`emptyAsNull: true`**
   on sum/count metrics, and Kibana maps a literal aggregated **`0` → `null`**
   in tables. Emit small non-zero healthy baselines for error/throttle/count
   fields so columns populate, while staying under alert thresholds. When a
   chaos dict is active, missing keys stay `0` (do not re-apply baseline).
8. Do not synthesize `kibana.alert.*` or fake SLO documents.
9. After creating the generator, remind the user to click **Refresh list**.
10. Existing-generator refreshes belong to **upgrade-telemetry-generator**, not
    this skill.
11. CloudWatch dimension series: enumerate panel column KQL filters that assert
    dimension **presence** (`field : *`) or **absence** (`NOT field : *`) and
    emit one series per combination. Also apply **dashboard-level**
    `searchSourceJSON` filters (e.g. global `Operation:InvokeGateway`) to every
    series those panels read — a Method-only doc missing `Operation` is dropped
    by the global filter even when the metric fields are present.
12. Ground against **every** package dashboard (Overview and Detail), not only
    the primary one. Emit `Namespace` + `MetricName` datapoint attrs when alert
    ES|QL filters on them — use `build_cloudwatch_metric` so this is automatic.
13. When Lens filters `field:* AND NOT other:*`, omit `other` on that series
    entirely (do not emit JSON null). Example: Bedrock Generated images needs
    `model_id` without `quality` / `image_size`.
14. OTel receiver attrs: use metadata `name_override` values (e.g. `state`,
    not `workers.state`). Emit documented resource attrs
    (`apache.server.name`). Match sum vs gauge from receiver docs. Start
    `RATE()` counters near zero so charts are not dominated by an initial
    spike.
15. Table / Discover KEEP columns (e.g. Azure Activity Recent Events
    `principal.type`, `user.id`, `network.peer.address`) must be populated on
    **every** event category the panel reads — not only Administrative. Platform
    categories (ServiceHealth, ResourceHealth, …) still need those identity
    fields or rows show sparse nulls. Alert `VALUES(...)` / `BY` columns have
    the same bar ([ALERTS.md](ALERTS.md)).
16. Billing / cost pie dashboards: enrich thin `llm_profile` lists so
    subscription / resource-group / resource / product breakdowns are not a
    single 100% slice. Emit `forecast_cost`, skew model weights, and mix
    CognitiveServices with other Azure SKUs for overview billing.
17. `CounterState` defaults to 50k–5M — pass `initial=` near zero for any
    metric charted with `RATE()`/`INCREASE()`, and keep counter drift
    non-zero every tick (else spike-then-flatline).
18. ES|QL `AVG`/`SUM` vs OTLP counters (learned `redisenterprise_otel`):
    stock panels that `AVG()`/`SUM()` a metric reject `counter_long`
    (`verification_exception`). When merged constraints list
    `gauge_required` / `prefer_double_metrics` / suspicious AVG|SUM ops —
    or you see that error live — emit those fields as **gauge doubles**,
    not cumulative OTLP sums. Reset the metrics data stream on startup
    when field_caps still report `counter_long`/`long` for them.
    **Exception:** if the same field is `counter_required` for an OOTB alert
    (`INCREASE`/`RATE`), alert wins — emit counter + gauge→counter reset
    ([ALERTS.md](ALERTS.md)).
    Missing panel columns (`Unknown column [path|endpoint|…]`) mean emit
    the attrs/metrics the inventory requires. Multi-field panels read one row:
    metrics with the same unit, type and attributes already share a document
    when built on the scrape clock (#34); co-locate via ES bulk only the pairs
    `scripts/audit_colocation.py` reports as differing in unit, type or attributes.
19. Co-located ES bulk + TS gauges (learned `rabbitmq_otel`):
    when bulk-indexing co-located node/resource docs into an OTel metrics
    stream, register `gauge_long` (and matching) dynamic templates so
    fields get `time_series_metric: gauge`. Plain `long` mixed with OTLP
    gauges makes `LAST_OVER_TIME` / multi-field TS queries return 0 rows
    (“No results found”) even though docs exist. Reset the stream if
    mappings were poisoned before the template fix.
20. Empty-string KEEP columns (learned `postgresql_otel`):
    table panels that show Wait Type / Wait Event (or similar attrs) treat
    `""` like null. Always emit non-empty documented values for every KEEP
    / table column the stock panel reads — do not use empty string as a
    “no wait” stand-in unless the package docs say so.
21. Shared logs + null `message` (learned `traces` / `system_otel`):
    Host Details and other panels alias `message` → `body.text`. Span
    exception events with no body land in `logs-generic.otel` as
    `(null)` message. Prefer OTLP error **logs** with `body` set to the
    exception message; keep span ERROR + `exception.*` attrs, and do not
    attach empty exception events that pollute shared log streams.
22. Billing day histograms + `dropPartials` (learned `azure_billing`):
    Forecast / Daily usage date-histogram `usage_date` / `usage_start` at
    **24h with `dropPartials: true`**. Wall-clock `usage_*` = now leaves only
    today’s partial bucket → “No results found”. Dual-write like beats:
    usage docs with completed UTC day bounds + `pretax_cost` only; separate
    forecast docs with only `actual_cost` or `forecast_cost` + day-start
    `usage_date`. Keep `@timestamp` recent; emit several past complete days.
    **Time picker:** If Daily usage costs look blank, set **Last 7 days**
    first (`now-7d`→`now`). `now-15m` / `now-1h` / rolling `now-24h`→`now`
    cannot contain a complete UTC day bucket with `dropPartials` — not a
    generator failure. Forecast needs a future `to`
    (e.g. `now-7d`→`now+15d`) so forecast series are in range.
23. Stable counter `startTimeUnixNano` for `RATE()` (learned `mysql_otel`):
    Never reset cumulative-sum start to `now - 60s` each scrape. Subclass
    `CounterState` and pass `state.start_time_ns` into builders — otherwise
    15m `SUM(RATE(...))` panels show “No results found”. Start counters near
    zero (not multi-million) so first RATE buckets stay readable.
23b. Statement timer units are seconds (learned `mysql_otel` Queries):
    Stock Queries KPIs do `SUM(sum_timer_wait)` as seconds then
    `avg_ms = total_s / calls * 1000`. Emitting ns-scale values shows as
    millions of years / billions of ms. Emit delta **seconds** for
    `sum_timer_wait` and `events_waits_current.timer_wait` (unit `s`).
24. Multi-instance Status `"N/A"` (learned `aws_ec2_otel`):
    Some Instance Detail headers `CASE(count_distinct(InstanceId) > 1, "N/A",
    …)`. A multi-ID fleet without an InstanceId filter is expected N/A —
    not a missing StatusCheckFailed emit. Prefer Overview drilldown or a
    single-`instance_ids` config for Detail demos.
25. Maps `geo_point` attrs (learned `nginx_ingress_controller_otel`):
    `_format_attributes` stringifies dicts → `attributes.geo.location` becomes
    `"{'lat':…}"` (invalid geo_point → empty Maps). Emit `"lat,lon"` strings
    (or OTLP kvlist of doubles). Reset the stream if field_caps is `keyword`.
    Tighten tests — `"lat" in string` is a false positive.
26. Per-DB backends vs instance max (learned `postgresql_otel`):
    `SUM(backends)/MAX(connection.max)` must stay ≤ ~1.0. Do not give each DB
    its own `max_conn * util` independently — partition one instance budget.
27. `DELTA()` gauges after restart (learned `apache_tomcat_otel`):
    Seed cumulative DELTA gauges from a wall-clock floor so process restart
    cannot produce large negative New Failures / similar KPIs.
28. Ratio panels + sticky `gauge_long` (learned `apache_tomcat_otel` FD util):
    `open/max` as long → integer division 0. Treat sticky `gauge_long` like
    plain long when panels need double; emit `asDouble` / `gauge_double` and
    reset the stream. Gauge overlay `80%` is often `goal`, not utilization.
    Co-located bulk docs with identical dimensions + the same `@timestamp`
    ms collide in TSDS (409) — stagger timestamps (`ts + i`) per bulk doc.
29. Sibling metric panels + SUM double-count (learned `vsphere_otel`):
    `WHERE metric_A` then STATS B/C needs co-located ES bulk docs. For
    `SUM(LAST_OVER_TIME(...))` headroom/usage panels, emit those siblings
    **bulk-only** (OTLP+bulk doubles). Match `object` dims when latency and
    utilization share a TS series.
30. **COALESCE unknown columns** (learned OOTB alert backfill 2026-08):
    ES|QL `COALESCE(a, b)` fails if either named column was never indexed.
    Emit every COALESCE operand series (e.g. Envoy `admin` + `ingress_http`).
31. **Alert dataset mismatch** (learned nginx → `myservicereceiver.otel`):
    If a template `FROM`s a stream outside this generator’s datasets, emit a
    companion stream with required fields or fail with package-template
    mismatch — do not edit the OOTB rule.
32. **APM OTel rollup latency** (learned `metrics-transaction.1m.otel`):
    Put `transaction.duration.summary` as `aggregate_metric_double` (and
    histogram) under `metrics-otel@custom` **before** the first rollup doc.
    Dynamic float children → Services latency 0 ms / overview N/A. Backfill
    ≥24h to match the default APM time picker; reset sticky streams on heal.
33. **`SOURCE_INTEGRATION` pins the installed package** (learned `aws_ecs_otel` 0.10.0 → 0.12.0
    Request count N/A, and `nginx_otel` 0.9.0 whose new dashboards read fields 0.8.0 did not):
    step 20 installs exactly the cited version, so bumping the citation upgrades every demo
    that uses the generator. Bump it only together with the generator changes the new version
    needs: preview with `python3 scripts/audit_package_coverage.py <generator_id> --latest`,
    bump, run the audit again (it checks the cited version) and close every reported gap —
    alerts without an impact key, missing metric+stat pairs, fields the dashboards read —
    then refresh `scripts/snapshot_alert_identity.py <id>` and
    `scripts/snapshot_package_templates.py <id>` (`tests/test_package_pins.py`). A generator may only read chaos keys its SPEC declares
    (`tests/test_generator_contract.py`), and every key must be driven by a `gt-*` channel
    (`tests/test_gt_wiring.py`).
34. **One timestamp per scrape** (learned 2026-10, almost every OTLP metrics doc held one
    metric): Elasticsearch's OTLP intake stores points in one document only when resource,
    scope, data-point attributes, `timeUnixNano`, `startTimeUnixNano`, unit and metric type
    all match; points 1 ns apart become separate documents. Build metric points with the
    `db_common` builders or `generator_engine.scrape_clock.scrape_now_ns()`, never
    `_now_ns()` / `time.time_ns()`; `run_tick_loop` opens the scrape clock, and a generator
    with its own loop wraps each iteration in `scrape_clock()`. Give every sum a stable
    start (`CounterState.start_time_ns`). Same-unit, same-attribute metrics then share a
    document, so a bulk companion is only needed for co-location across units, metric types
    or attributes. `tests/test_scrape_timestamp.py` enforces this, and
    `python3 scripts/audit_colocation.py <generator_id>` (network) lists package queries that
    combine metrics in one row and whether each pair is merged, covered by a companion, or
    UNCOVERED.
35. **Backfilled history must fit the TSDS look-back** (learned `gcp_cloudstorage_otel`,
    daily `storage/*` panels empty on a fresh demo): OTel metrics streams are TSDS and
    reject documents older than `index.look_back_time` (2h unless `metrics@custom` sets
    more, and only for backing indices created after it does). Panels that bucket by day
    need history, so backfill at startup the way `gcp_cloudstorage_otel`
    `backfill_daily_history` does: set `look_back_time` on `metrics@custom` without
    dropping its other settings, recreate your own stream if its first backing index
    cannot reach back, stamp each past scrape with `scrape_clock(at_ns=...)`, and use
    fixed timestamps (noon UTC) so a restart's repeat backfill is rejected as a duplicate.
36. **Scenario identity (branding)** (learned from `nginx_otel`, `mysql_otel`, `gcp_vpcflow_otel`,
    `aws_waf_otel`, `azure_ai_foundry`, `gcp_vertexai`, which all emitted one name for every
    scenario). Every value a user can filter or pick on comes from `ScenarioIdentity`
    (`generator_engine/identity.py`), so two scenarios never share it: hosts, services,
    instances, databases, domains and URLs, buckets, clusters, ARNs, endpoint ids, and cloud
    account, subscription, tenant and project ids. Build one `ident = ScenarioIdentity.from_scenario(scenario_data, ns)`
    in `run()` and never hardcode these values or format a `{ns}-` prefix by hand.

    | Need | Helper |
    | --- | --- |
    | Host, service, instance name | `ident.host(role, i)`, `ident.service(role)`, `ident.instance(role, i)` |
    | Any other named resource (bucket, VNet, endpoint, cluster) | `ident.label(role, i)` or `ident.qualify(name)` |
    | Database name | `ident.database(name)`, `ident.databases([...])` |
    | URL or hostname | `ident.domain(sub)` |
    | AWS | `ident.aws_account_id`, `ident.aws_id(kind, role, i)` |
    | Azure | `ident.azure_subscription_id`, `ident.azure_tenant_id` |
    | GCP | `ident.gcp_project(cfg.get("project_id"))`, `ident.gcp_instance_id(i)` |
    | Kubernetes node | `ident.k8s_node(region, i, rng)` |
    | Scenario service that traces join on | `ident.app_service(name)`, the only helper that leaves the name unchanged |

    - A value read from config (`cfg.get("project_id")`, `resource_name`, endpoint or
      database lists) also goes through `ident.qualify(...)` or the matching helper. An
      override must not undo the scoping.
    - Exempt: public image names, upstream API paths, metric names, and IP addresses.
    - Enforced by `tests/test_generator_identity.py`: it runs one cycle for two scenarios and
      fails on any shared identity value, and it scans `generator.py` for a plain string
      stored under `service.name`, `host.name`, `cloud.account.id` and the other watched
      fields. Set `SPEC.identity_fields` to the fields the package dashboards filter on.
      Run it, and `tests/test_scenarios.py`, before finishing.
37. **Stable alert identity while a fault is active** (learned from `postgresql_otel`,
    `azure_activity_otel`, `influxdb_otel`; ALERTS #121). Kibana keys each ES|QL alert on the
    rule's `STATS ... BY` columns, or on every `KEEP` column without `STATS`. Any of those
    values a fault emits (caller, pid, query text, tracking id, shard, resource, and for raw
    rows the numbers too) is drawn once when the impact key turns on, re-emitted every tick,
    and cleared when it turns off. Enforced by `tests/test_alert_identity_stable.py`, which
    reads the rule shapes from `tests/fixtures/alert_identity.json`; refresh that snapshot with
    `python3 scripts/snapshot_alert_identity.py <id>` when you add or bump a package.
38. **Package SLOs and ML jobs read the same documents as alerts** (learned from
    `aws_rds_otel`, `vsphere_otel`, `aws_sqs_otel`, `kubernetes_otel`). Step 21 creates an SLO
    per `slo_template` and sets up every `ml_module`, so their filters, `groupBy` fields,
    detector fields and split fields are part of the emit set, with the same attribute
    placement the template uses (`resource.attributes.vcenter.host.name` is not the
    data-point `vcenter.host.name`). On the chaos-off baseline every SLO meets its objective:
    a generator whose healthy state already burns budget (2% errors against a 99.5% target,
    p75 above the web-vitals threshold, replicas short of desired) fails. Enforced by
    `tests/test_slo_template_emit.py` and `tests/test_ml_module_emit.py`, which read
    `tests/fixtures/slo_templates.json` / `ml_modules.json`; refresh them with
    `python3 scripts/snapshot_package_templates.py <id>`. See [TEMPLATES.md](TEMPLATES.md).
39. **Library investigation workflows read your data verbatim** (learned from `aws_rds_otel`,
    `aws_elb_metrics_otel`). A Template Library workflow whose required ES|QL steps read your
    dataset is deployed and started by your OOTB rules, so its `WHERE` terms must hold on
    your documents with the exact field path (`attributes.DBInstanceIdentifier` is a
    data-point attribute), dimension level (`TargetGroup IS NULL AND AvailabilityZone IS
    NULL`) and `stat`. Expose the resource ids it takes as inputs through
    `workflow_input_values(scenario_data)`. Enforced by `tests/test_workflow_template_emit.py`;
    see [TEMPLATES.md](TEMPLATES.md).

