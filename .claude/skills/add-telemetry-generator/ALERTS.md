# Alert ES|QL emit contract

> SLO templates and ML modules from the same package are deployed too; their contract is
> [TEMPLATES.md](TEMPLATES.md).

OOTB package **alerting rule templates** are part of the emit set — same
authority as dashboards. Do **not** edit templates or live Kibana rules to
fix `verification_exception`; fix generators.

Learned from mass `verification_exception` failures when OOTB rules were
created from templates against demo telemetry (2026-08).

> **"OTLP is one metric per doc" in the findings below** described the
> generators before 2026-10, when every point carried its own nanosecond
> timestamp. Points of one scrape now share a timestamp ([GUARDRAILS.md](GUARDRAILS.md)
> #34), so same-unit, same-type, same-attribute metrics land in one document.
> The bulk companions named below stay: their pairs differ in unit, type or
> attributes, or a panel needs bulk-only fields. Check a pair with
> `scripts/audit_colocation.py <id>`.

---

## Mandatory gate (add + upgrade)

For every package-backed generator with `kibana/alerting_rule_template/`:

```bash
python3 .claude/skills/add-telemetry-generator/scripts/extract_alert_esql_fields.py \
  --package-dir "$PKG" --out "$OUT/alert_required_fields.json"

python3 .claude/skills/add-telemetry-generator/scripts/verify_alert_emit.py \
  --inventory "$OUT/alert_required_fields.json" \
  --generator-dir "generators/<id>"
```

Hard miss if `verify_alert_emit.py` exits non-zero. Also manually confirm:

1. Every `fields[]` / `coalesce_operands[]` token is emitted (column must exist).
2. Every `counter_required[]` field maps as **`counter_long`** (use
   `build_cumulative_sum` + sticky **gauge→counter** stream reset when needed).
3. `from_streams[]` aligns with `SPEC.datasets` / generator-owned streams, or
   the generator emits a **companion** dataset (see Dataset mismatch).
4. CloudWatch packages: `requires_cloudwatch_namespace_metricname` → every
   datapoint has `Namespace` + `MetricName` (prefer `build_cloudwatch_metric`).

`impact_keys` 1:1 remains mandatory but is **not** sufficient — keys wire chaos;
this gate wires **fields**.

---

## Rules

### COALESCE completeness

`COALESCE(a, b)` still fails ES|QL if `a` or `b` is an **unknown column**.
Emit a series for **every** named operand (near-zero is fine), e.g. Envoy
`envoy.http.admin.downstream_rq_*` alongside `ingress_http`.

### INCREASE / RATE vs dashboard SUM / AVG

| Alert / panel op | Required mapping |
|---|---|
| `INCREASE(x)` / `RATE(x)` | `counter_long` (cumulative monotonic sum) |
| `SUM`/`AVG`/`MAX`/`MIN` only | prefer **gauge** (period delta or level) |

When the **same field** is `counter_required` for an alert and used with `SUM`
on a dashboard, **alert wins**: emit counter + reset sticky gauges. Do not
leave `INCREASE` on a gauge to keep a SUM panel happy.

### Healthy baseline must stay under thresholds (false-positive traps)

When chaos is `None` / STANDBY, emit must remain **under** every OOTB alert
threshold for the full rule window. Common traps (learned 2026-08 FP sweep):

1. **Non-zero seeds on `INCREASE`/`RATE`/`DELTA` metrics** — ES|QL treats a
   counter that first appears in the window at value `N` as growth from 0.
   Seed error/blocked/failed/evicted/timeout counters at **0**; only grow
   under the matching `impact_key`.
2. **Ambient growth that crosses the threshold over time** — e.g. queue depth
   `current += enq - deq` with healthy dequeue lag climbs past `> 1000`.
   Clamp gauges / levels when healthy; do not rely on “it starts low”.
3. **`SUM(absolute samples)` alerts** (not `INCREASE`) — cumulative counter
   snapshots summed over the window explode. Prefer **per-interval gauges**
   for those fields (e.g. PostgreSQL `bgwriter.duration`), or keep absolute
   values tiny enough that `SUM` over the window stays under threshold.
4. **Ratio of `SUM(absolute)` counters** — seed and increment at a healthy
   ratio (e.g. buffer hit ≥ 99.5%); do not force extra “miss” increments on
   the healthy path.
5. **Unstable `startTimeUnixNano`** — a new start_ns every tick creates
   overlapping series; `MAX−MIN` / `INCREASE` then false-positives. Use
   `CounterState` / `stable_counter_start_ns()` (PVC-persisted).
6. **`impact_value(..., default=1.0)` multipliers** — when a key is absent,
   `1.0` means “full ambient”; for fault multipliers prefer `default=0.0`
   and only apply growth when the key is > 0 (or document that `1.0` is
   healthy and chaos uses `>> 1`).
7. **`resolve_active_chaos` fills SPEC defaults** — if any channel is active
   for the generator, keys defaulted to `0.0` in SPEC can zero a healthy
   ratio (e.g. `low_buffer_hit_ratio` default 0 → 0% hit). Treat `<= 0` as
   healthy baseline in `run()`/`tick()`.
8. **`chaos_mode="any"` + empty `raw_impact`** — `resolve_active_chaos` must
   **not** inject SPEC defaults when the active channel’s `raw_impact` is
   `{}`. Foreign GT channels otherwise fill keys with `0.0`, and
   `impact_value` treats that as real chaos (e.g. k8s `ready_fraction=0` →
   Deployment/StatefulSet/Failed-pod FPs). Prefer `chaos_mode="explicit"`
   and only apply ready/replica chaos when the corresponding keys are
   actually `> 0`.
9. **`RATE` / `INCREASE` alerts clear slowly** — after chaos resolves, the
   tip still sits in the rule window (often 10–15m) and group alerts may
   stay `active` for **an extra 1–2 schedule cycles** even when
   `evaluation.value` is already `0`. Prefer **omitting** the counter
   metric entirely on the healthy path (emit only while the matching
   `impact_key` is > 0) so the series goes cold; avoid broker-level emits
   when the OOTB query groups `BY destination.*` (null-dimension groups
   stick around). Do not edit the OOTB rule to “fix” clear latency.
10. **`MAX`/`MIN`/`AVG` gauges over long windows** — Airflow OTel OOTB rules
    use **15m** windows and **5m** schedules. After resolve, `MAX` still
    sees the chaos tip until it ages out (~15m) plus up to one schedule
    cycle. Expect **~15–20m clear** per channel; harness clear timeouts
    need to be ≥ that (e.g. 2400s). Healthy baseline must stay under the
    threshold (e.g. queue size `0–10` when alert is `MAX > 20`).
11. **Impact-key → alert name mapping** — GT harness `key_to_alert` must
    include every `impact_key` (e.g. `airflow_otel.tasks_killed_externally`
    → `[Airflow OTel] Tasks killed externally`). A `MISSING_MAP:` entry
    means **no OOTB package alert** for that key (e.g. `aws_bedrock` has
    zero templates) — skip the channel as `no_ootb_alert`; never fall back
    to fire-any (false-passes on leftover noise).
12. **`apache.uptime` is a gauge for MAX/MIN restart alerts** — OOTB
    `[Apache OTel] Server restart detected` uses `MAX`/`MIN` on uptime;
    emit `build_gauge` (not `counter_long`) and delete the metrics stream
    when sticky counter mapping blocks. Healthy seed must stay **≥ 300**
    (alert threshold `min_uptime < 300`) so startup samples never FP.
13. **Package template conflicts** — e.g. `[Apache OTel] High request rate`
    uses `MAX`/`MIN` on `apache.requests` while `[Apache OTel] No requests
    received` needs `RATE(apache.requests)` → `counter_long`. **Generator
    workaround (2026-08):** emit `apache.requests` as a **gauge** (still
    cumulative tip values) by default so High request rate can execute; while
    `no_requests_received` chaos is active, rematerialize the metrics stream
    and emit as `counter_long` for RATE. Also treat any `high_request_rate > 0`
    (GT often sets `85.0`) as breach — do not require `> 1000` on the impact
    key — and clamp healthy deltas so tip rate stays under 1000/min. Prefer
    this over editing the OOTB template. Also ensure GT deploy creates **all**
    package alert templates (apache had 8 templates / only 3 rules until
    backfilled).
14. **GT channel values at the alert threshold** — channels often set the
    impact key to the alert threshold (e.g. `low_idle_workers: 5.0` for
    `AVG < 5`). Treat the chaos key as present and clamp emit **strictly
    under** the threshold (e.g. idle ≤ 4), not `key < threshold` alone.
    Same for RDS `burst_balance_low: 20` (`MIN < 20` → emit ≤ 19),
    `free_storage_space_low` / `freeable_memory_low` at the byte floor.
15. **Impact key names must match SPEC/GT exactly** — `aws_rds_otel` looked
    up legacy aliases (`burst_balance`, `cpu_spike`, `replica_lag_s`) while
    GT sets `burst_balance_low` / `cpu_utilization_high` / `replica_lag_high`,
    so chaos never applied. Wire generator reads to SPEC `impact_keys` ids.
15. **Missing live OOTB rules despite package templates** —
    `aws_bedrock_agentcore@0.7.0` ships **15** templates; GT deploy can leave
    **0** live rules (only GT probe/`SE CH` rules exist). Harness then waits
    forever for names like `[AWS Bedrock AgentCore] Agent runtime high latency`
    while metrics already breach. **Backfill by creating rules from Fleet
    templates** (`POST /api/alerting/rule/{template_id}` from
    `/internal/alerting/rule_template/{id}`) — create, do not edit queries.
    Skip `fleet-*-inactivity-monitoring`. After backfill, AgentCore ch1 fired
    immediately on the same chaos emit. Root cause in deployer: EPM
    `assets.kibana.alerting_rule_template[]` often has `path` but **no** `id`;
    `_list_package_alert_templates` must parse the filename from `path` when
    `installationInfo.installed_kibana` is empty/stale.
16. **`SUM(...) > 0` alerts vs emptyAsNull baselines** — AgentCore (and similar)
    error/throttle templates alert on `SUM(SystemErrors|Throttles|…) > 0`.
    Emitting `1` for Lens `emptyAsNull` tables **is not under threshold** —
    every rule FPs as soon as it evaluates. Healthy path must emit **0** (or
    omit the series); only grow under the matching `impact_key`.
17. **ES|QL integer division in rate alerts** — `[AWS Lambda OTel] High throttle
    rate` does `ROUND(total_throttles / (invocations+throttles) * 100.0, 2)`.
    Long/long division truncates, so any `invocations > 0` yields rate **0**
    even with huge throttles. Do not edit the template: under
    `high_throttle_rate` chaos emit **invocations=0** and an absolute
    throttle count `>= 11` (also satisfies `denominator > 10`) so the
    quotient is `1` → 100%. Error-rate alerts that divide by invocations
    alone still work with `errors >> invocations`.
18. **CloudWatch `stat` must match the OOTB filter** — SQS High backlog /
    In-flight saturation filter `stat == "Average"`; DLQ has messages and
    Oldest message age filter `stat == "Maximum"`; send/receive counts use
    `stat == "Sum"`. Emitting only `Maximum` for queue depth made the High
    backlog TS query return **0 documents** while `FROM` still showed the
    gauge. Dual-emit `Average`+`Maximum` when different rules need both, or
    emit the exact stat each template filters on. Producer send-rate drop
    needs near-zero `Sum` over **30m** — when the impact key is below its
    healthy SPEC default, emit `NumberOfMessagesSent=0` (GT may set `10.0`,
    not `< 1`). Pre-chaos healthy `Sum` samples stay in the window, so the
    harness **fire timeout must be ≥ ~35–45m** (e.g. `GT_FIRE_TIMEOUT=2700`);
    the default 1200s will `no_fire` even when zeros are emitting correctly.
19. **Alert `STATS … BY` cardinality** — `[Azure Activity OTel] Failed
    administrative operations` needs `failed_count >= 3` **per**
    `(operation.name, caller, network.peer.address)`. Raising a random
    `error_rate` across many ops/callers/peers never concentrates a bucket.
    Under chaos, pin those dimensions and emit `max(3, int(impact))`
    identical failures. Same class of bug: Security-category alerts need
    `azure.category == "Security"` — emitting `Alert` category will not fire.
20. **Missing live OOTB rules (Foundry / AgentCore class)** — GT deploy can
    leave **0** live rules despite Fleet `alerting_rule_template` assets
    (only GT probe rules exist). Backfill by reading
    `/api/fleet/epm/packages/<pkg>/<ver>/kibana/alerting_rule_template/<file>`
    and `POST /api/alerting/rule` (create, do not edit queries). Skip
    `fleet-*-inactivity-monitoring`. After backfill, Foundry Latency high
    went `active` on existing chaos emit.
21. **ES|QL long/long division on gauge ratios** — CockroachDB
    ``usage_ratio = capacity_used / capacity`` truncates to **0** when both
    fields are `gauge_long`. Bulk-emit **non-integral floats** with
    `dynamic_templates: gauge_double`, and **do not** also OTLP-emit those
    fields — whole-number `asDouble` still maps `gauge_long` and wins the
    race. Add a sticky-mapping reset that deletes the metrics stream when
    capacity fields are stuck as long; do not edit the OOTB template.
    Healthy ``tick()`` must also cap utilization **under** the OOTB ratio
    (e.g. ≤0.45 when the alert is ``> 0.6``); a healthy ceiling of 0.85
    fires forever after remediate.
22. **RATE alerts need absolute chaos increments** — CouchDB Conflict storm
    filters ``http.status_code == "409"`` then ``SUM(RATE(...)) > 10``. SPEC/GT
    set the impact key to a **target rate** (e.g. ``50.0``), not a 0–1 share.
    Clamping to ``min(1.0, key)`` only scales a small ``business_load`` delta
    and often stays under 10/s. Under chaos, add
    ``int(target_rps * BATCH_INTERVAL_MAX)`` to the 409 counter each scrape.
23. **Latency multiplier ≠ absolute ms** — CouchDB
    ``MAX_OVER_TIME(couchdb.average_request_time) > 1000``. GT sets
    ``high_average_request_time_latency: 100.0`` as a multiplier on
    ``uniform(1,20)*load``, which often stays under 1000ms. When the key is
    ``> 1``, force ``latency_ms = max(1100, …)``. SPEC default ``1000.0`` is
    the healthy sentinel — do not treat it as chaos.
24. **SPEC-defaulted sibling keys break ``impact_value``** — CouchDB
    ``resolve_active_chaos`` fills unset keys with SPEC defaults (``0.0``).
    ``impact_value(chaos, "high_5xx…", "high_error_rate…")`` then returns
    ``0.0`` and never sees the active ``high_error_rate…: 50.0``. Prefer the
    first **non-zero** among related keys.
25. **Long-interval OOTB schedules (1h/1d)** — Envoy Certificate expiring
    soon ships with ``schedule.interval: 1d`` and ``timeWindowSize: 1d``.
    GT fire timeouts cannot wait a day. After chaos trigger, call Kibana
    ``POST /internal/alerting/rule/{id}/_run_soon`` (with
    ``x-elastic-internal-origin: Kibana``). Do **not** edit the OOTB schedule.
    Clear also needs the 1d MIN window free of fault samples — rematerialize
    the statsdreceiver stream (or remove ``days < 7`` docs) after remediate,
    then ``_run_soon`` again.
26. **Cumulative gauge ratios need rebase under chaos** — Envoy High
    downstream error rate does ``(completed-2xx)/completed*100 > 5`` on
    absolute StatsD gauges. Drifting ``2xx`` while a large healthy cumulative
    remains never reaches 5% in time. When the impact key is a percent
    (e.g. ``75.0``) or fraction ``≥ 0.05``, set
    ``2xx = completed * (1 - err_frac)`` each tick.
27. **``MAX > 0`` warming gauges stick across the window** — Envoy
    ``[Envoy OTel] Warming clusters stuck`` / Listeners warming use
    ``MAX(...warming...) > 0`` over **15m**. After remediate, emit ``0`` every
    tick; if a single chaos tip (or a far-future ``@timestamp``) remains in
    the window, clear waits the full window. Prefer rematerializing
    ``metrics-statsdreceiver-default`` after resolve, then ``_run_soon``.
28. **Map ``listeners_warming`` explicitly** — GT inventory
    ``key_to_alert`` must include
    ``envoyproxy_otel.listeners_warming`` →
    ``[Envoy OTel] Listeners warming``. Without it the harness emits
    ``MISSING_MAP:`` and skips a live OOTB rule.
29. **Healthy DB size must stay under OOTB quota ratio** — etcd
    ``[etcd OTel] Database size approaching quota`` fires when
    ``usage_ratio > 0.9``. A healthy tick cap of ``quota * 0.95`` never
    clears after chaos. Cap healthy at ``≤ 0.85`` and **snap** ``db_size``
    down when the impact key is inactive.
30. **Co-locate FD open/max on one doc** — etcd
    ``[etcd OTel] File descriptor exhaustion`` ANDs
    ``process_open_fds`` and ``process_max_fds`` on the same document.
    OTLP is one metric per doc, so emit a bulk companion (same pattern as
    DB size vs quota). GT may send percent ``85.0`` (< 90% threshold) —
    normalize percent→fraction and force ``≥ 0.95`` while the channel is on.
31. **``0.0`` is a valid chaos value** — HAProxy
    ``backend_has_no_active_servers: 0.0`` means desired ``haproxy.active``
    (SPEC: ``< 1`` breaches; ``100.0`` = healthy). ``int(x or 0)`` drops
    ``0.0``, and ``is_down`` without zeroing ``active_servers`` leaves
    BACKEND gauges at ``SERVERS_PER_BACKEND``. Treat ``< 1`` as breach and
    set ``active_servers = 0`` on down backends.
32. **Server health-check failures need server ``is_down``** — OOTB
    ``SUM(INCREASE(haproxy.failed_checks)) > 0`` only on server rows.
    ``failed_checks`` increments only when ``is_down``. GT's
    ``server_health_check_failures: 50.0`` is a positive signal, not a
    ``backends_down`` count gated by backend ``is_down``. Fail server ticks
    directly when the key is ``> 0``.
33. **Latency keys are absolute ms** — HAProxy
    ``high_backend_connection_time: 750.0`` (and queue/response siblings)
    are millisecond targets. Using them only as ``backends_down`` flags
    with ``_latency_ms(..., base=8)`` stays ~10ms and misses OOTB
    ``max_ctime > 100``. Emit the impact value as the gauge when ``> 0``.
34. **Co-locate sessions.count + sessions.limit** — OOTB session
    saturation ANDs both fields on the same document before STATS. Emit a
    bulk companion; wire ``session_saturation_approaching_limit`` as
    percent/fraction of ``SESSION_LIMIT`` (force ``≥ 0.85`` when active
    but ``≤ 0.8``).
35. **``AVG_OVER_TIME`` dilutes chaos tips** — IBM MQ CPU load uses
    ``MAX(AVG_OVER_TIME(...)) > 85`` over a **15m** window. A short tip of
    100% after a healthy period averages ~70%. Rematerialize the metrics
    stream after trigger (or keep emitting ≥90% for the whole window);
    clamp oversized GT values (e.g. ``637``) into ``(85, 100]``.
36. **Wire impact keys through to metric helpers** — IIS
    ``application_pool_not_running`` was read in ``run()`` but never
    passed into ``_pool_metrics``, so pool state stayed ``3`` (Running)
    and the OOTB ``state != 3`` alert never fired. Pass the key through.
37. **Document-count alerts keep the 5m lookback** — IIS
    ``Application pool not running`` counts docs with ``state != 3`` over
    **5m**. After remediation, fault samples still in the window keep the
    alert ``active``. Clear by waiting out the window or
    ``_delete_by_query`` those fault docs (then ``_run_soon``); deleting
    the whole data stream also works but forces a full rematerialize.
38. **``INCREASE`` / ``RATE`` alerts need rematerialize to clear** — IIS
    Bandwidth throttling uses ``SUM(INCREASE(iis.network.blocked)) > 0``
    over 5m. After chaos stops, the ramp still sits in the window.
    Delete ``metrics-iisreceiver.otel-default`` (or wait out the window)
    and ``_run_soon`` so healthy flat counters show ``INCREASE = 0``.
39. **ES|QL long/long division on HTTP error rate** — InfluxDB
    ``error_rate_pct = ROUND(errors / total * 100.0, 2)`` truncates to
    **0** when ``http_api_requests_total`` is mapped/emitted as long.
    Emit the gauge as **float** (``asDouble``) and rematerialize the
    metrics stream; do not edit the OOTB template.
40. **Gauge-total error-rate alerts need a healthy snap** — InfluxDB
    HTTP error rate SUM()s absolute gauge tips, not INCREASE. After
    remediation, leftover 4XX/5XX totals keep ``error_rate_pct`` high.
    Zero non-2XX buckets when ``http_api_high_error_rate`` is off (and
    rematerialize / wait out the 15m window for clear).
41. **GT probe values are not always literal metric values** — Kafka
    ``broker_count_below_threshold: 20.0`` means "activate this key",
    while the OOTB alert is ``MAX(kafka.brokers) < 2``. Treating 20 as
    the broker count left the cluster "healthy". When the key is
    active/positive, emit a value that breaches the OOTB threshold
    (e.g. ``1``), not the raw GT number when it is above the threshold.
42. **``MAX`` over lookback blocks ``== 0`` / ``< N`` fires** — Kafka
    Empty consumer groups uses ``MAX(members) == 0`` over **15m**. Healthy
    samples (members 1–5) earlier in the window keep MAX > 0. Rematerialize
    the metrics stream **after** triggering empty-groups chaos so the
    window only contains zeros (same class of bug as broker-count MAX).
43. **Wire impact keys to the right metric path** — Kubernetes
    ``container_cpu_throttling`` was mapped to restart probability instead of
    ``k8s.container.cpu_limit_utilization``. OOTB alert needs avg util > 0.9;
    GT may pass ``75.0`` (percent) — normalize ``>1 → /100`` and clamp to
    ≥0.95 when the key is active.
44. **Condition-flag impact keys ≠ filesystem fill %** — Kubernetes
    ``node_disk_pressure: 0.8`` must set ``k8s.node.condition_disk_pressure=1``
    on ``metrics-k8sclusterreceiver.otel-*``. Wiring only
    ``node_filesystem_saturation`` / ``fill >= 0.85`` left the condition at 0
    while chaos was ACTIVE. Treat ``node_disk_pressure > 0`` as the condition
    flag (same pattern for other ``condition_*`` keys).
45. **GT ``0.8`` fails a ``>= 0.85`` condition gate** — Kubernetes
    ``node_memory_pressure: 0.8`` did not set ``condition_memory_pressure`` when
    the gate was ``mem_util >= 0.85``. Treat the key as a condition flag
    (``> 0`` → ``condition_*=1``) and optionally bump util to ≥0.9 for related
    panels; rematerialize ``metrics-k8sclusterreceiver.otel-default``.
46. **Normalize absolute-byte GT probes to util fractions** — Kubernetes
    ``node_memory_saturation: 103079215000`` is a legacy absolute-bytes probe,
    not a 0–1 utilization. Clamp/normalize (``>100 → 0.95``, ``1–100 → /100``)
    before driving ``k8s.node.memory.working_set`` so
    ``working_set / allocatable > 0.85`` can fire.
47. **Snap sticky gauges on clear** — Kubernetes CrashLoopBackOff uses
    ``MAX(k8s.container.restarts) > 0`` over 15m. After chaos, in-memory
    restart counters kept climbing (and GT ``75.0`` was treated as 7500%
    probability). Normalize percent→probability, set ``restarts=0`` when the
    key is off, rematerialize ``metrics-k8sclusterreceiver.otel-default``.
48. **Pending vs Failed need distinct forced phases** — OOTB Pending alert is
    ``MAX(k8s.pod.phase) == 1``; Failed is ``VALUES`` contains ``4``. A shared
    ready-fraction that randomly picks Pending/Failed makes Pending unfireable
    (MAX becomes 4). Force ``phase_override=1`` or ``4`` from the matching
    impact key; rematerialize after trigger so the window is pure.
49. **``impact_value`` + SPEC defaults mask later keys** — ``resolve_active_chaos``
    fills every SPEC key with its default (often ``0.0``). ``impact_value(a,b,c)``
    returns the first *present* key, so ``deployment_unavailable_replicas: 0``
    shadowed ``statefulset_replicas_not_ready: 50`` and left ready==desired.
    When OR-ing keys, skip non-positive defaults (first value ``> 0`` wins).
50. **Memcached low throughput is not a command multiplier** — GT
    ``low_command_throughput: 5.0`` multiplied ``cmd_mult`` and *raised* RATE.
    Alert is ``SUM(RATE(commands)) < 1``. SPEC default ``100.0`` = healthy;
    ``< 1`` or GT probe tokens (``1 ≤ v < 100``) → freeze commands
    (``cmd_mult=0``). Normalize CPU percent ``>5 → /100`` for the ``> 0.8``
    cores alert.
51. **Memcached hit-ratio default is a sentinel, not ``miss_mult``** —
    SPEC ``low_hit_ratio_for_get_operations`` default ``800.0`` means healthy.
    ``resolve_active_chaos`` fills it on sibling channels; using ``800`` as a
    miss multiplier exploded ``memcached.commands`` RATE and blocked low
    throughput. Treat ``≥ 800`` as ``miss_mult=1``; ``≤ 80`` (GT probe) as
    hit/miss split only (do not multiply total command volume).
52. **Memcached hit-ratio gauge must breach immediately** — OOTB alert is
    ``AVG(operation_hit_ratio) > 20`` over 15m (field is miss %). Lifetime
    hit/miss counters dilute for hours after healthy history. Under hit-ratio
    chaos, emit a forced miss gauge (e.g. ``35``) instead of
    ``misses/(hits+misses)``; rematerialize so the window is pure.
53. **SQL Server low-ratio probes are not gauge values** — GT
    ``buffer_cache_hit_ratio_low: 200`` was written as the hit % (never
    ``< 95``). Treat ``≥ 95`` / ``> 100`` as activate → emit ``~70``. Same for
    ``page_life_expectancy_low``: SPEC default ``3000`` = healthy; GT ``300``
    is a probe token → emit ``~120`` (alert is low PLE, not the token itself).
54. **SQL Server I/O latency needs seconds/op, not a tiny multiplier** — OOTB
    alert is ``INCREASE(latency)/INCREASE(ops) > 0.1``. Multiplying
    ``0.001–0.05`` by GT ``75`` still yields ≪100ms/op. When the key is
    active (``> 1``), add ``ops_delta * 0.15–0.35`` seconds of latency.
55. **SQL Server I/O latency needs co-located latency+ops docs** — OOTB
    ``WHERE latency IS NOT NULL AND operations IS NOT NULL`` before STATS.
    OTLP is one metric per doc (0 hits). Bulk-index companions with both
    counters + ``counter_double``/``counter_long`` dynamic templates (same
    pattern as CockroachDB block-cache / etcd FD).
56. **SQL Server TempDB free-space probe sits on the threshold** — GT
    ``tempdb_space_low: 102400`` equals the OOTB ``< 102400`` cutover.
    SPEC default ``1024000`` = healthy. Emit ``~50000`` when the probe is
    at/above the threshold; treat ``≥ 1024000`` as healthy sibling fill.
57. **MongoDB connection_exhaustion probe is available count** — SPEC
    default ``100`` = healthy; alert is ``available < 10``. GT ``5.0`` was
    treated as a multiplier (``> 1`` false) and left available ≥ 50. Values
    ``< 10`` set available directly; ``≥ 100`` stay healthy; mid-range
    still use the legacy shrink-multiplier path.
58. **Materialize live rules before fire (MongoDB)** — Package can be
    ``installed`` with ``alerting_rule_template`` assets while **0** live
    rules exist (deploy step 20 skipped / prior cleanup). Harness then
    ``no_fire``. Upsert from templates via
    ``/internal/alerting/rule_template/{id}`` → ``POST /api/alerting/rule/{id}``,
    filling required ``.es-query`` params (``size``, ``threshold``, …) from
    any working ESQL rule. Do not edit OOTB templates.
59. **MongoDB global_lock_contention is ms/s rate, not per-tick bump** —
    OOTB alert is ``SUM(RATE(mongodb.global_lock.time)) > 500``. Treating
    GT ``3750`` as a raw counter delta at ~12–15s cadence yields only
    ~250–310/s. Scale bump by emit cadence (``max(rate, 600) * 15s``).
60. **MongoDB High cache miss (FORK ESQL) may not write AAD docs** — Rule
    ``last_run.alerts_count.active`` / event-log ``active-instance`` can be
    1 while ``.internal.alerts-stack.alerts-*`` stays empty. Coverage harness
    falls back to ``last_run.alerts_count.active > 0`` when alert docs are
    missing. Generator still drives miss share ``> 5%`` (GT ``75.0``).
61. **MySQL dirty-page ratio needs double usage + percent→fraction** — OOTB
    does ``dirty / total`` without ``TO_DOUBLE`` (``long/long`` → ``0``). Emit
    ``mysql.buffer_pool.usage`` as ``asDouble`` and rematerialize sticky
    ``long`` mappings. GT ``75.0`` is percent → ``0.75``; alert is ``> 0.75``
    so bump probes at/above the cutover to ``≥ 0.85``.
62. **Do not copy non-ESQL params into OOTB ESQL rules** — Filling
    ``termField`` / ``termSize`` / etc. from a KQL working rule into an
    ``esqlQuery`` template can make the rule ``succeed`` with 0 hits while
    the same ESQL in ``_query`` returns rows. When materializing templates,
    only add required defaults (``size``, ``threshold``, …); strip KQL-only
    fields for ``searchType: esqlQuery``.
63. **impact_value + SPEC defaults can mask the active GT key** —
    ``resolve_active_chaos`` injects every impact_key default (``0.0``).
    ``impact_value(chaos, "high_5xx_error_rate", "high_error_rate_by_service")``
    then returns ``0.0`` and never sees GT ``75.0``. Prefer the first
    **positive** key (and treat ``>1`` as percent).
64. **Nginx myservice companion: aliases + int division** — OTel logs map
    ``log.level``/``service.name``/``message`` as aliases; writing them sends
    docs to the failure store. Bulk ``severity_text`` +
    ``resource.attributes.service.name`` + ``body.text`` with
    ``pipeline=_none``. OOTB ``errors/total*100`` is integer division → emit
    ~100% errors when the companion probe is active so pct becomes ``100``.
65. **Nginx Error log spike needs lowercase severity_text** — OOTB filters
    ``log.level IN ("error","crit","alert","emerg")`` (alias →
    ``severity_text``). ``build_log_record`` used to force ``severityText`` to
    ``ERROR``, which never matches. Preserve caller case; emit nginx-style
    lowercase levels. Do not write ``attributes.log.level`` (alias → failure
    store). Also re-enable disabled ``[Nginx OTel] *`` rules before fire waits
    (Fleet templates can land ``enabled: false``).
66. **Nginx High active connections threshold is avg > 256** — GT probe
    ``75.0`` must not be used as the gauge value. When the probe is positive,
    emit ``nginx.connections_current{state=active} >= 300`` so
    ``AVG(...) > 256``. SPEC ``sample_count >= 3`` is only a minimum-sample
    guard, not the breach threshold.
67. **NVIDIA clock throttle needs co-located boost+throttled docs** — OOTB
    requires ``SM_CLOCK``, ``GPU_UTIL``, and ``GPU_TEMP`` on the **same**
    document (OTLP is one-metric-per-doc). Bulk co-locate those fields.
    Also need ``avg_util > 50`` and ``avg_sm_clock/max_sm_clock*100 < 70``:
    emit boost (``1593`` MHz) + throttled samples so ``avg!=max``. Idle
    probe default ``100.0`` must not override util on sibling channels
    (only apply when ``0 < idle < 50`` → emit ``util=0`` for ``max_util < 1``;
    GT probe is often ``5.0``, not the util value). Lift FB util probes to
    ``≥95%``; temp spike must stay ``≥84°C``. Emit ``FB_USED``/``FB_FREE`` as
    **doubles** (OTLP ``asDouble`` + bulk floats) and rematerialize sticky
    ``long`` mappings — OOTB ``USED/total*100`` is integer division on longs.
68. **Oracle buffer cache hit ratio: percent probes + no forced physical** —
    GT ``10.0`` is percent (→ ``0.10``), not a fraction already ≤1. Treating
    ``min(10,1)=1`` zeroed physical then ``physical=randint(10,200)`` kept the
    ratio healthy. Convert ``>1`` to percent and emit
    ``physical/logical ≥ 0.15`` when the probe is below the healthy band.
69. **Oracle util probes are percents; session shares must sum to 1** —
    ``process/session/transaction/dml/enqueue`` GT values like ``75.0`` are
    percents (→ ``0.75``), not multipliers. Lift positive probes to
    ``≥0.90`` for OOTB ``>85%``. Session-type×status shares must be
    normalized so ``SUM(sessions.usage)`` matches the intended total.
    Do **not** delete the Oracle metrics stream between channels — clears
    that wipe the stream cause cascading ``Unknown column`` rule_errors
    on the next fire.

70. **PostgreSQL absolute-SUM counters must reset on resolve** —
    OOTB rules like Bgwriter maxwritten / Backend buffer writes / temp files /
    sequential scans use ``SUM(absolute snapshots) > N`` (not ``INCREASE``).
    Stopping the per-tick delta leaves the tip elevated, so rematerialized
    docs keep firing for the whole lookback window. When the chaos probe is
    ``0``, reset ``bgwriter.maxwritten``, ``bgwriter.buffers.writes[backend]``,
    ``temp_files``, ``temp.io``, and ``sequential_scans`` to ``0``. Stream
    deletes alone are not enough without the tip reset; after a wipe wait for
    rematerialize before the next channel fire.
71. **PostgreSQL connection utilization: ES|QL integer division** —
    OOTB is ``ROUND(SUM(backends)/MAX(connection.max)*100.0, 2) > 80``.
    ES|QL integer-divides ``SUM/MAX`` first, so util stays ``0.0`` until
    ``SUM(backends) > MAX(connection.max)``. Keep a stable modest
    ``max_conn`` (e.g. ``10000``) for healthy and chaos — never raise then
    shrink max, or ``MAX()`` keeps the large ceiling. Breach by emitting
    enough per-tick ``backends`` that a few cycles push windowed ``SUM`` over
    that ceiling. Wipe stale ``max_conn=1e6`` docs before fire if needed.
72. **Do not full-delete metrics streams between PostgreSQL channels** —
    Clearing via ``DELETE _data_stream`` causes ``Unknown column`` rule_errors
    on the next fire (mappings rematerialize slowly / chaos-only fields). Prefer
    tip resets + ``_delete_by_query`` on old ``@timestamp`` docs (keep recent
    healthy samples and field mappings) and AAD cleanup. Stream wipe only as a
    last resort, then wait for rematerialize before the next trigger.
73. **PostgreSQL long-running queries ≠ slow_top_queries** —
    OOTB Long-running queries filters ``query_sample`` with
    ``total_exec_time > 300000``. Do not gate that on the ``slow_top_queries``
    fraction. When ``long_running_queries > 0``, emit query_sample exec time at
    least ``300001`` ms (lift smaller positive probes).
74. **PostgreSQL low buffer hit ratio needs tip dominance** —
    OOTB is ``SUM(blks_hit)/(SUM(hit)+SUM(read))*100 < 99`` on absolute
    counter snapshots. Healthy tips seed in the tens of billions, so a small
    low-ratio increment never moves the windowed ratio. Under
    ``low_buffer_hit_ratio``, reset tips to a huge low-ratio pair (e.g. 1e12
    total at ≤98%) so chaos dominates ``SUM()``. Treat probes in ``(1,100]``
    as percents. On resolve, **re-seed** healthy tips if the tip ratio is still
    ``<0.99`` — accumulating onto a swamp tip never clears.

Also keep advisory/system destinations out of user-facing depth alerts when
the OOTB query already excludes them — and still clamp those depths so
dashboards stay sane.

### CloudWatch

Always set datapoint attrs `Namespace` (`AWS/<service>`) and `MetricName`
(bare name). Emit dedicated MetricNames the templates filter on
(`DeadLetterErrors`, `HTTPCode_ELB_5XX_Count`, …), not only related proxies.
Match each template's `stat` filter (`Average` / `Maximum` / `Sum`) — see
rule 18.

### STATS VALUES / BY / KEEP

Alert `VALUES(azure.….role)`, `BY azure.servicehealth.tracking.id`, etc. need
those attributes present on matching documents (same bar as dashboard KEEP).

### Dataset mismatch

If a template `FROM`s a stream the generator does not own (e.g. nginx rule →
`logs-myservicereceiver.otel-*`):

1. Prefer emitting a **companion** stream with the required fields, **or**
2. Fail validate / upgrade with `package template mismatch` and file/raise
   upstream — **never** edit the OOTB template from these skills.

When setting `data_stream.dataset` for OTLP intake, pass the **bare** name
(`myservicereceiver`). The endpoint appends `.otel`; passing
`myservicereceiver.otel` creates `logs-myservicereceiver.otel.otel-*`, which
OOTB rules will not query (learned nginx companion emit 2026-08).

### Live validate (optional)

When Kibana creds exist, list rules with
`executionStatus.status:error` and `verification_exception`. Treat those as
**upgrade** work on generators — not rule edits.

---

## Sticky mappings

| Prior mapping | Needed | Action |
|---|---|---|
| `counter_long` | gauge (`SUM`/`AVG`) | delete metrics data stream on startup |
| `gauge` / plain `long` | `counter_long` (`INCREASE`) | delete metrics data stream on startup |

Follow existing generator patterns (`postgresql_otel` temp_*, `mongodb_otel`,
`airflow_otel` processor_timeouts, `mysql_otel` row_locks).
75. **PostgreSQL slow_top_queries drives top_query logs** —
    OOTB Slow top queries filters ``db.server.top_query`` with
    ``total_exec_time > 10000``. Drive that from ``slow_top_queries``, not
    ``long_running_queries`` (query_sample > 300000). Healthy top_query tips
    must stay ``≤8000`` so the alert can clear.
76. **PostgreSQL temp_files_created vs high_temp_i_o_volume key order** —
    Both keys bump ``temp_files`` / ``temp.io``. Put the channel's active key
    first in ``impact_value(...)`` — SPEC defaults fill the sibling with
    ``0.0``, and the first present key wins (so ``high_temp_i_o_volume`` first
    would shadow ``temp_files_created: 75``).
77. **RabbitMQ disk_free healthy floor must be ≥2GiB** —
    OOTB Disk alarm is ``disk_free_alarm > 0`` derived from ``disk_free < 2GiB``.
    A healthy floor of ``1GiB`` keeps the alarm stuck on after resolve. Keep
    healthy ``disk_free ≥ 20GiB`` and only dip below ``2GiB`` under
    ``disk_alarm_triggered``; re-seed when clearing a prior fault tip.
78. **RabbitMQ zero-consumers probe is not a consumer count** —
    GT ``queue_with_zero_consumers_and_ready_messages: 75`` means force
    ``consumers=0`` with ready messages (use 75 as ready hint). Do not assign
    the probe to ``consumers``. Also ``impact_value(..., -1)`` passes ``-1`` as
    a *key*, not ``default=-1``.
79. **Redis fragmentation ratio swapping is RSS < used** —
    OOTB is ``AVG(fragmentation_ratio) < 1.0``. Emitting ``rss = used * 1.5–2.5``
    is healthy, not swapping. Under ``memory_fragmentation_indicates_swapping``,
    set ``rss = used * 0.55–0.95``. GT severity ``10.0`` means force swap (not
    ratio=10). Keep ``memory_approaching_maxmemory`` out of that companion
    ``impact_value`` list so a SPEC default ``0`` does not steal the branch.
80. **Redis maxmemory alert needs used ≥ maxmemory (integer div)** —
    OOTB ``ROUND(used / max_mem * 100.0, 2) > 90`` evaluates ``used/max_mem``
    as long/long first, so any tip with ``used < maxmemory`` yields
    ``usage_pct = 0``. Under ``memory_approaching_maxmemory``, emit
    ``redis.memory.used >= redis.maxmemory`` (not 91–99%). Keep healthy tips
    below maxmemory so integer div stays 0 after resolve.
81. **Redis fragmentation SPEC default 100.0 must stay healthy** —
    ``memory_fragmentation_indicates_swapping`` default is ``100.0`` (=healthy).
    Treat only ``0 < value < 100`` as swap fault (``(0,1)`` = explicit ratio;
    ``1–99`` e.g. GT ``10`` = force swap). ``>= 100`` must take the healthy
    RSS≥used branch so injected SPEC defaults cannot keep ``AVG(frag)<1``.
82. **Redis Enterprise TLS expiry probe must be < 30 days** —
    OOTB is ``days_until_expiry < 30``. GT ``tls_certificate_expiring_soon: 30.0``
    sits on the boundary and never fires. Clamp active probes to ``<= 29`` days
    (``node_cert_expiration_seconds = days * 86400``).
83. **Redis Enterprise ``database_down`` default 100.0 is healthy** —
    SPEC/GT inject ``database_down: 100.0`` as the healthy default; GT fault
    tips are ``10.0`` (not ``1``/bool). Treat ``value < 100`` as down
    (``bdb_up=0``); ``>= 100`` must keep ``bdb_up=1`` so sibling channels and
    resolve cannot stick Database down.
84. **Redis Enterprise free-mem / storage probes need hard clamps** —
    OOTB Node free memory is roughly ``< ~1GiB`` and storage alerts trip near
    single-digit GiB free. GT may send ``node_free_memory_low`` /
    ``node_storage_running_low`` as severity percents (e.g. ``75``) that are
    far above the breach band if used as GiB. When the probe is ``< 100``,
    clamp free memory to ``<= 0.5`` GiB and free storage to ``<= 4`` GiB.
85. **Redis Enterprise memory-pressure must exceed ``bdb`` limit** —
    OOTB high memory utilization compares used vs limit. Under
    ``memory_usage_approaching_limit`` / mem pressure, set
    ``bdb_used_memory = limit * uniform(1.0, 1.05)`` (not a sub-limit percent
    tip) so the alert window can fire; keep healthy tips strictly below limit.

86. **Redis Enterprise shard connection needs co-located ``redis_maxclients``** —
    OOTB ``[Redis Enterprise OTel] Shard connection saturation`` filters
    ``redis_connected_clients IS NOT NULL AND redis_maxclients IS NOT NULL`` on the
    **same** docs, then ``avg_clients / avg_max * 100 > 80``. Bulk shard docs were
    emitting clients without ``redis_maxclients``, so the tip never matched. Also
    keep ``clients >= maxclients`` under pressure (integer-division risk when
    clients < max). Add ``redis_maxclients`` to ``_GAUGE_DOUBLE_FIELDS`` + bulk.

87. **Traefik TLS expiry probe must be < 30 days** —
    Same boundary as Redis Enterprise: GT ``tls_certificate_expiry_imminent: 30.0``
    must be clamped to ``<= 29`` so ``days_until_expiry < 30`` can fire. Also for
    FD pressure probes ``>= 80``, set ``open_fds >= process_max_fds`` so integer
    division ``open/max`` is at least 1 before ``*100``.
88. **Traefik 5xx rate alerts need 5xx-only tips (integer div)** —
    OOTB ``[Traefik OTel] High 5xx error rate by {entrypoint,router,service}``
    does ``SUM`` of period-delta gauges then
    ``ROUND(errors_5xx / total * 100.0, 2) > 5`` with ``WHERE total > 100|50``.
    ES|QL long/long-divides first, so any tip with ``errors_5xx < total`` yields
    ``error_rate_pct = 0``. GT probes (750/375) are absolute volume hints, not
    percent — do not map them via ``probe/100``. Under fault emit **only**
    ``attributes.code LIKE "5*"`` (skip OK and 4xx; ``_HTTP_CODES_ERR`` includes
    400/404 which dilute the 5xx filter), boost absolute deltas so
    ``errors_5xx >= total``, and soft-trim prior healthy window docs so the
    lookback is tip-dominated. Keep sibling dimensions on OK-only tips so
    channels do not cross-fire. On resolve, return to OK-heavy tips (+ soft-trim)
    so integer-div pct returns to 0.
89. **Traefik sticky gauges must snap on resolve** —
    ``high_memory_usage`` sets ``state.rss`` to tens of GiB; ``tick()`` only
    drifts a few MiB, so clear never recovers. Same class of stickiness for
    FD pressure (``open>=max``), high goroutines, stale reload, and TLS
    expiry. When the impact key is idle, snap those gauges back to the
    healthy band so soft-trim + clear can succeed.

90. **ZooKeeper data-tree probe must exceed 1 GiB in bytes** —
    OOTB ``[ZooKeeper OTel] Data tree size excessive`` floors at
    ``max_data_size > 1073741824``. Auto-calibrated channel YAMLs may pass a
    small sentinel; promote ``0 < data_tree_size_excessive <= 1GiB`` to
    ``1.5GiB`` (1610612736) so the tip can fire. Keep healthy tips under 1GiB.

91. **ZooKeeper FD utilization needs co-located open+limit** —
    OOTB ``[ZooKeeper OTel] File descriptor utilization high`` AND-filters
    ``file_descriptor.open`` and ``file_descriptor.limit`` on the **same**
    document before ``open*100/limit > 80``. OTLP is one-metric-per-doc, so
    bulk-index co-located FD docs via ``ESBulkClient`` into
    ``metrics-generic.otel-default``. Treat impact as percent (>1) or
    fraction; when the channel is on and ratio would be ``<=80%``, promote to
    ``~95%``.

92. **ZooKeeper node-health probe is fail-when-positive** —
    OOTB ``[ZooKeeper OTel] Node health check failed`` triggers on
    ``min_ruok == 0``. The impact key ``node_health_check_failed`` must treat
    **any value >0** as failed (emit ``ruok=0``). Do not treat the probe as a
    healthy fraction (``>=0.5 => ruok=1``) — calibrated values like ``0.8``
    would never fire.

93. **ZooKeeper outstanding requests: AVG dilution + shared ``generic`` timeouts** —
    OOTB ``[ZooKeeper OTel] Outstanding requests saturation`` is
    ``AVG(request.active) > 10`` on ``metrics-generic.otel-default`` with **no**
    ``@timestamp`` filter. On a shared generic stream the rule can exceed the
    5m rule-type timeout before returning (``execution cancelled due to timeout``)
    even when live tips are clearly ``75+``. Soft-trim old ``metrics.zookeeper.*``
    docs (and/or add a temporary live lookback) so the unfiltered query finishes;
    under fault emit absolute tips ``>=500`` so AVG dominates residual healthy
    ``0–8`` samples. ZooKeeper generators must use ``dataset=generic`` (not
    ``zookeeper.otel``).

94. **Shared-stream rule timeouts under concurrent `_delete_by_query` storms** —
    OOTB ES|QL metric rules on large shared streams (e.g.
    ``metrics-generic.otel-default``) can sit ``running=true`` /
    ``outcome=failed`` with ``warning=timeout`` (5m rule-type ceiling) while
    many ``*delete/byquery`` tasks contend for the cluster. Prefer canceling
    those DBQs, keeping the rule ``enabled=true``, then
    ``_update_api_key`` + ``_run_soon`` (or one disable→enable bounce) and
    waiting 1–2 schedule intervals. Do **not** launch large tip-trim DBQs on
    multi-million-doc generic metrics streams during fire waits; tip-trim only
    when the cluster is quiet and the query is narrowly filtered. Assist loops
    that DBQ ``metrics-generic.otel-*`` on every fire poll will re-wedge the
    rule — disable those assists for ZooKeeper outstanding-requests fire.

95. **Azure OpenAI OOTB rules hardcode ``*-default``** — package alerts
    ``FROM metrics-azure.open_ai-default`` / ``logs-azure_openai.logs-default``.
    Scenario namespaces (``gtazureopenai``, ``banking``, ``inttest``) alone leave
    those indices missing → ``Unknown index`` / `rule_error`. Dual-write each
    live/backfill batch to ``default`` when the scenario namespace differs so
    OOTB fire/clear works without editing alert templates.

96. **Apache High request rate: gauge mapping + GT tip scale + ES bulk
    companion** — see finding #13 update. Emit `apache.requests` as gauge by
    default (rematerialize stream when stuck as `counter_long`); tip
    `>=3000` per batch when `high_request_rate > 0` (GT often sets `85.0`).
    Also dual-write a plain-long ES bulk companion doc to
    `metrics-apachereceiver.otel-default` when OTLP metrics ingest is pointed
    at a mismatched host (accepted 200 but never indexed on the demo ES) so
    OOTB `MAX`/`MIN` can still fire/clear. Verified fire→clear on
    `[Apache OTel] High request rate` (2026-08-19).
97. **Nginx Dropped connections flaps from nginx_metrics 5% drops** — OOTB
    `[Nginx OTel] Dropped connections detected` is
    ``Δaccepted - Δhandled > 0`` (host-level MAX−MIN over 5m) on
    ``metrics-nginxreceiver.otel-*``. The legacy ``nginx_metrics`` generator
    (often running while ``nginx_otel`` is stopped) seeded
    ``handled = accepted - rand(0,100)`` and dropped 1 connection on ~5% of
    ticks, so steady-state demos flapped with no chaos. Keep
    ``handled == accepted`` when healthy; only diverge under an explicit
    ``dropped_connections`` / ``dropped_connections_detected`` chaos impact.
    Snap handled back to accepted when the impact clears so the gap cannot
    linger across the 5m window.

98. **Package dashboards hardcode ``*-default`` (Bedrock / Vertex)** —
    AWS Bedrock and GCP Vertex AI package dashboards/data views target
    ``metrics-*-default`` / ``logs-*-default``. Scenario-only bulk (e.g.
    ``xborder``) leaves Overview empty even when ES ingest succeeds.
    Dual-write each live/backfill batch to ``default`` when the scenario
    namespace differs (same pattern as Azure OpenAI #95). Also apply to
    ``azure_billing`` (metrics), ``azure_ai_foundry`` (metrics + logs), and
    ``aws_bedrock_agentcore`` (metrics + runtime/gateway/memory logs).

99. **Redis / Kafka OTel Overview empty when OTLP never indexes** — same
    ingest mismatch as #96: OTLP may return 200 but documents never land
    on the demo ES, while ES-bulk AND-filter companions alone leave
    Overview / Consumer Groups panels blank. Bulk-index overview companions
    too: Redis ``redis.clients.connected`` / commands / etc.; Kafka
    ``kafka.brokers``, ``kafka.topic.partitions``, consumer-group lag/members
    alongside the existing partition co-located docs.

100. **Systems Ops / Business exec dashboards N/A when OTLP metrics stall** —
    ``host_metrics``, ``kubernetes_otel``, and executive ``business.*`` KPIs
    are OTLP-only. When the ingest host accepts/fails silently (same class as
    #96), Systems Ops USE/CPU/Mem tiles and the Business Executive dashboard
    show N/A even though generators keep "sending". Dual-write ES bulk
    companions: ``metrics-hostmetricsreceiver.otel-default`` (USE fields),
    ``metrics-kubeletstatsreceiver.otel-default`` (node CPU/mem + pod memory),
    ``metrics-generic.otel-default`` (all executive ``business.*`` gauges on
    one doc per tick, filtered by ``resource.attributes.service.name``).

101. **[Azure OpenAI] Billing N/A without ``azure_billing`` + date mapping** —
    That dashboard (`azure_openai-f5bbc591-…`) sums ``azure.billing.pretax_cost``
    and date_histograms ``azure.billing.usage_start``. Enabling only
    ``azure_openai`` leaves ``metrics-azure.billing-*`` empty. Auto-start
    ``azure_billing`` from ``enabled_specs`` whenever ``azure_openai`` is on
    (unless ``azure_billing: false``). ``azure_billing`` is often not Fleet-
    installed when only OpenAI is — orphan ``metrics-azure.billing@custom``
    never applies, so ISO ``usage_start`` maps as ``keyword`` and Lens returns
    N/A. Force ``usage_start`` / ``usage_end`` as ``date`` on the @custom
    component, put index template ``metrics-azure.billing`` (priority 250)
    that composes it, rollover/recreate streams, and set
    ``always_install=True`` on the billing SPEC so Fleet assets exist too.

102. **Nginx OTel Overview N/A when OTLP metrics stall** — Traffic & Capacity /
    Request Health / Server Internals metric tiles
    (``nginx.requests``, ``nginx.connections_*``) read
    ``metrics-nginxreceiver.otel-default``. Access logs can keep flowing while
    OTLP metrics stop indexing (same class as #96/#99/#100). Dual-write ES bulk
    companions from ``nginx_metrics``: co-locate request/accepted/handled on one
    doc (avoids OTel fingerprint collisions when ``handled == accepted``) and
    emit one ``nginx.connections_current`` doc per ``attributes.state``. Do
    **not** also bulk from ``nginx_otel`` — both are default-enabled and collide
    on document IDs. Still emit ``nginx.requests`` on the ``nginx_otel`` OTLP
    path so metrics recover when OTLP ingest works.

103. **system_otel Host Details N/A with thin USE companion** — Overview /
    Metrics dashboards need ``metrics.system.cpu.utilization``,
    ``memory.utilization``, ``filesystem.usage``, ``disk.*``, ``network.*``,
    ``processes.*``, and load/logical.count — not just the Systems Ops USE
    triple (load.1m / filesystem.utilization / network.errors). Expand
    ``host_metrics`` ES bulk to convert the full OTLP ``metrics_by_scope``
    set into attributed companion docs (co-locate same-attribute series).

104. **Redis OTel RATE/INCREASE fails on plain ``long`` bulk companions** —
    Overview panels call ``RATE``/``INCREASE`` on connections/keys/net
    counters; Memory & Persistence calls ``RATE(redis.cpu.time)``. ES bulk
    companions without ``dynamic_templates`` map as plain ``long``, so ES|QL
    rejects them. Pass ``counter_long`` / ``counter_double`` templates on
    bulk create, emit ``redis.cpu.time`` per ``attributes.state``, and delete
    the sticky ``metrics-redisreceiver.otel-default`` stream when counters
    are still plain ``long``.

105. **[OTel] Hosts Overview null CPU/Mem/Load for service hosts** — The
    Hosts Overview table (CPU Utilization, Normalized Load, Memory
    Utilization, Memory Free) is fine for scenario cloud hosts when
    ``data_stream.dataset: hostmetricsreceiver.otel`` is on. If that filter
    is cleared, kafka/nginx/redis hosts rank first by doc count and show
    nulls because only the 3 scenario hosts had hostmetrics. Emit
    companion hostmetrics for ``{ns}-kafka-broker``, ``{ns}-redis-host``,
    and ``{ns}-nginx-0{1,2}`` via ``_with_infra_hosts``.

106. **Nginx Request Health "Field was not found" for error attrs** —
    Error Logs by Severity / Error Counts by Severity filter root
    ``log.level`` (an alias of ``severity_text``; ``attributes.log.level`` before
    ``nginx_otel@0.7.0``) and terms ``attributes.process.pid`` on
    ``nginx.error.otel``. Error logs only emitted during ``error_log_spike``
    chaos, so those fields never mapped. Always dual-write a small baseline
    of error docs to ``logs-nginx.error.otel-default`` (with the lowercase level
    in ``severity_text`` + ``attributes.process.pid``, ``pipeline=_none``);
    raise volume when the spike probe is active. Do not write root
    ``log.level`` (ECS alias → failure store). Healthy baseline must use
    ``warn``/``notice`` only — mixing ``error``/``crit`` at baseline fires
    OOTB Error log spike (``>50`` severe / 15m) at steady state (#108).

107. **Azure OpenAI Billing Daily usage empty on short time pickers** —
    **First response when someone reports blank "Daily usage costs" /
    "Daily usage costs - by model" (dashboard ``azure_openai-f5bbc591-…``):**
    tell them to set the Kibana time picker to **Last 7 days**
    (``now-7d``→``now``). Do **not** chase a generator bug until that fails.
    ``now-15m`` / ``now-1h`` / rolling ``now-24h`` stay empty by design —
    panels histogram ``azure.billing.usage_start`` at ``24h`` with OOTB
    ``dropPartials: true`` + ``ignoreTimeRange: false``, and usage docs use
    completed UTC day bounds (``@timestamp`` stays recent). A rolling 24h
    window cannot contain a full UTC day bucket. Prior fix (#101) was date
    mapping + enable/dual-write billing (still required). Optional demo UX:
    patch live Daily panels to ``ignoreTimeRange: true`` and
    ``dropPartials: false`` for short pickers; Fleet reinstall of
    ``azure_openai`` may revert that patch. Prefer documenting Last 7 days
    over changing OOTB dashboards unless the user asks.

108. **Oracle healthy buffer-cache hit ratio stuck at 85%** —
    Healthy ``buffer_cache_hit_ratio_low`` default ``0.95`` computed
    ``physical_frac = 1.0 - 0.95`` which is slightly ``> 0.05`` in float
    math, so the code took the breach branch
    ``physical = logical * max(frac, 0.15)`` → constant **85%** hit and
    OOTB Buffer cache hit ratio low stayed active with chaos STANDBY.
    Gate on ``hit_ratio >= 0.90`` (alert threshold) instead of
    ``physical_frac <= 0.05``.

109. **Nginx Error log spike at steady state from Request Health baseline** —
    Baseline error docs (#106) sampled the full ``ERROR_MESSAGES`` list
    including ``error``/``crit``. At ~4 docs/scrape/host that exceeded the
    OOTB ``error_count > 50`` / 15m threshold with no chaos. Emit
    ``warn``/``notice`` only when healthy; reserve severe levels for
    ``error_log_spike``.

110. **Oracle capacity alerts Unknown column when only ratio bulk companions rematerialize** —
    After a sticky-mapping stream reset, OTLP scoped batches for
    ``oracledbreceiver`` may not rematerialize capacity fields while bulk
    still writes physical/logical/parse/tablespace. OOTB rules then fail with
    ``Unknown column [oracledb.processes.usage]`` (and sessions/locks/commits/
    deadlocks). Dual-write usage+limit gauges, session parts, commits+rollbacks,
    and enqueue_deadlocks via ``ESBulkClient`` with ``gauge_long`` /
    ``counter_long`` templates — same pattern as buffer-cache companions.

111. **Orphaned package alerts after scenario switch** —
    Installing ``cassandra_otel`` / ``etcd_otel`` (or any GT scenario) materializes
    OOTB rules that survive a switch to a scenario without those generators.
    Rules then fail every interval with ``Unknown index`` / ``Unknown column``.
    After upserting active-package rules, disable OOTB rules whose package is
    not in ``_packages_to_install()`` for the current scenario.

112. **Fleet EPM fallback when rule_template API is disabled** —
    On some hosted/serverless Kibana builds
    ``/internal/alerting/rule_template/{id}`` returns HTTP 400
    ("not available with the current configuration"). Package install still
    ships ``alerting_rule_template`` assets, but step 20 created **0** rules
    for packages whose templates were only reachable via that API (e.g.
    ``azure_openai`` latency/quota/provisioned). Fetch templates from
    ``/api/fleet/epm/packages/<pkg>/<ver>/kibana/alerting_rule_template/<file>.json``,
    flatten ``attributes`` (``ruleTypeId``, ``params``, …), fill required
    ``.es-query`` defaults, and ``POST /api/alerting/rule/{id}``. Do not skip
    materialization when the internal probe is 400 — only 404/501 mean the
    feature is absent; still use Fleet assets.

113. **Vertex AI latency "Field ... is of the wrong type"** —
    ``prediction_latencies`` / ``model_invocation_latencies`` must be ES
    ``histogram``. Early-start before the ``gcp_vertexai`` package template
    dynamically maps ``{values,counts}`` as ``object``, then Lens percentiles
    fail across mixed backing indices. Heal by deleting
    ``metrics-gcp_vertexai.metrics-<ns>`` once the package histogram mapping
    exists (``_maybe_heal_metrics_stream``, also re-checks every ~50 cycles).

114. **Kubernetes Overview Top 10 non-running / desired-vs-actual blank** —
    Panels filter ``non_running_pods > 0`` and ``pod_delta > 0``. Healthy
    service pods stay Running and ``available == desired`` so OOTB Pending /
    Failed / Deployment-unavailable alerts stay quiet. Populate the non-running
    panel with Succeeded (phase=3) Job pods (no Pending/Failed alert). The
    desired-vs-actual panel stays empty on a healthy baseline: a back-dated
    desired>available breadcrumb burns the package "Deployment Replica
    Availability" SLO (GUARDRAILS #38), so only faults populate it.

115. **system_otel Host Details Logs "Failed to transform field/value pairs
    into groups"** — Log Rate Analysis grouping runs ES ``frequent_item_sets``
    over significant keyword fields on ``logs-*``. Two failure modes:

    (1) Claude Code MCP sets ``attributes.error`` as a **string**; OTel
    passthrough promotes keyword ``error``, conflicting with Bedrock's object
    ``error``. Nesting into an object maps as ``flattened`` (still conflicts).
    Pipeline renames string → ``attributes.error_message``;
    ``logs-otel@custom`` sets ``index.default_pipeline`` (do **not** redefine
    ``attributes`` mappings — breaks package aliases).

    (2) A significant field present on only some streams (e.g. Bedrock
    ``error.type``) is **unmapped** on others (e.g. ``azure_openai.logs``).
    FIS then fails with ``scripts are not supported``. Stub
    ``error.type`` / ``event.type`` / ``source.geo.region_name`` on
    ``logs@custom`` and recreate sticky azure_openai log streams. Heal lives
    in ``generator_engine.logs_otel_error_heal``, called from ``traces`` start.

116. **APM Services latency 0 ms / overview N/A** — APM UI reads
    ``metrics-transaction.1m.otel-*`` (synthetic rollup), not live traces.
    Without an explicit mapping, passthrough ``metrics`` dynamically maps
    ``transaction.duration.summary`` as float/long children; ``avg`` on the
    parent returns ~0 and charts show N/A. Also, rollup was only a 12h
    deploy-time backfill — after it ages out of the picker window, overview
    charts go empty even when traces still have ``transaction.duration.us``.
    Fix: put ``aggregate_metric_double`` (+ histogram) under
    ``metrics-otel@custom``, reset sticky streams, backfill **24h** in
    ``ApmRollupGenerator.generate_all``.

117. **k8s pod ML "no such index [.ds-metrics-gcp_vertexai…]"** — Pod CPU/memory
    datafeeds used ``metrics-kubeletstatsreceiver.otel-*`` **and** ``metrics-*``.
    The wildcard expands to every metrics stream; when a heal deletes a
    Vertex (or other) backing index, the datafeed errors until the next
    successful search. Scope indices to kubeletstats only and set
    ``ignore_unavailable: true``.

118. **Managed APM ML "Datafeed has been retrieving no data for a while"** —
    ``apm-production_<ns>-****-apm_tx_metrics`` trains on
    ``metrics-transaction.1m.otel``. Deploy-time backfill is finite; once
    realtime passes the last rollup minute, searches return empty. Fix:
    ``ApmRollupGenerator.emit_live_minute`` via a daemon thread started from
    ``traces`` (``start_live_rollup_thread``). Custom
    ``apm-{ns}-transaction-metrics`` may stay ``opening`` when ML RAM is
    exhausted by native inference (~2GB on a 2.1GB ML node).

119. **APM ML ignores chaos latency slider** — Trace generator applies
    ``latency_multiplier`` to spans, but APM ML jobs read
    ``metrics-transaction.1m.otel`` rollup. Live rollup must pass the same
    multiplier (via ``start_live_rollup_thread(get_latency_multiplier=…)``)
    or anomalies never fire no matter how long you wait. Custom job
    ``bucket_span`` is ``1m`` (minutes); managed APM module job is often
    ``15m``.

120. **Custom APM ML latency always 0 / no anomalies** — Datafeed
    ``transaction_latency`` bucket_script did ``(parent_sum + flat_sum) /
    value_count``. With correct ``aggregate_metric_double`` mapping,
    ``sum`` on ``…summary.value_count`` is 0, so the script returns 0 every
    bucket and ``high_mean(transaction_latency)`` never scores. Prefer
    ``avg`` on ``metrics.transaction.duration.summary``, with flat-sum
    fallback. Reset the job after fixing so the model is not trained on zeros.

121. **Alert identity must be sticky while a fault is active** — Package ES|QL
    rules use ``groupBy: row``: Kibana keys each alert on the ``STATS ... BY``
    columns, or on every ``KEEP`` column when there is no ``STATS``. A fault
    that draws one of those values per record (a pid, a UUID tracking id, a
    random caller or shard) opens new alerts and recovers the old ones on
    every rule run, and never fires when ``alertDelay`` > 1. Seen on
    postgresql_otel long-running / slow top queries, azure_activity_otel
    service health / resource health / role assignment / sensitive key, and
    influxdb_otel shard write errors. Draw the fault's identity once when the
    key turns on, re-emit it every tick, clear it on resolve. A raw-row rule
    also keys on its numbers (``total_exec_time``, ``calls``), so hold those
    constant too. Related: a counter alert on ``MAX - MIN`` needs the counter
    to keep rising every scrape (Tomcat session rejection), and a strict
    ``> x`` threshold needs a floor above ``x``, not on it.
