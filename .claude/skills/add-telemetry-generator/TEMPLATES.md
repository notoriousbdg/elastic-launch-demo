# Package template emit contract (SLOs, ML jobs, library workflows)

Step 21 of the deployer turns three kinds of package asset into live objects for
every package a scenario installs (`SPEC.integrations`):

| Asset | Becomes | Gate |
|---|---|---|
| `kibana/alerting_rule_template` | alert rule | [ALERTS.md](ALERTS.md) |
| `kibana/slo_template` | SLO | this file |
| `kibana/ml_module` | anomaly detection jobs + datafeeds | this file |
| Template Library workflow reading the package's data | investigation workflow started by an OOTB rule | this file |

All three are part of the **emit set**. A generator is not done while an SLO it
installs has no data or burns budget on a healthy baseline, or while an ML job it
installs has no documents for a detector. Never edit package templates.

## Mandatory gate (add + upgrade)

```bash
python3 .claude/skills/add-telemetry-generator/scripts/extract_package_templates.py \
  --package-dir "$PKG" --out-dir "$OUT"          # slo_required_fields.json, ml_required_fields.json
python3 .claude/skills/add-telemetry-generator/scripts/verify_template_emit.py \
  --inventory-dir "$OUT" --generator-dir "generators/<id>"   # static, must exit 0
python3 scripts/snapshot_package_templates.py <id>              # network (EPR)
python3 -m pytest -q tests/test_slo_template_emit.py tests/test_ml_module_emit.py \
  tests/test_package_template_coverage.py -k <id>              # runtime proof
```

The static gate only finds tokens in the source. The runtime tests run the
generator, rebuild the documents Elasticsearch would index and evaluate each template
against them, so they also catch wrong placement, a wrong `stat`, a missing group
field and an unhealthy baseline.

## SLO templates

For each `slo_template` (`sli.metric.timeslice`, `sli.metric.custom`,
`sli.histogram.custom`, `sli.kql.custom`):

1. **Filter.** Some document in the SLO `index` must pass its KQL `filter`
   (`data_stream.dataset`, `Namespace`, `MetricName`, `stat`, label values such as
   `attributes.percentile: "p99"` or `service.name: "cockroachdb"`). Emit each value
   exactly as the filter names it.
2. **Metrics.** Every metric the SLI reads (`metric` / `good` / `total`, each with its
   own optional `filter`) needs documents where its `field` is numeric. Histogram SLIs
   need an OTLP histogram data point on that field.
3. **groupBy.** Some of those documents carry every `groupBy` field. Mind the prefix:
   `resource.attributes.vcenter.host.name` is a **resource** attribute, and
   `attributes.qmgr` is a **data-point** attribute; a bare name (`DBInstanceIdentifier`)
   resolves through the passthrough mapping from either. Documents missing a group field
   are dropped from the SLO, which is fine for other copies of the metric (e.g. ALB
   load-balancer-level copies of a per-target-group series).
4. **Healthy baseline.** With chaos off, the SLI must meet the objective
   (`timesliceTarget` of the slices for timeslice SLOs, `target` for occurrence SLOs), so
   a fresh deploy burns no error budget. Error rates, lag, p75/p99 latencies and
   replica counts must sit on the good side of the template threshold at baseline. A
   fault that should burn the SLO moves it through an `impact_keys` entry.

## ML modules

For each `ml_module`:

1. **Index.** The deployer points the datafeeds at the module's `defaultIndexPattern`,
   unless that matches none of the package's `discovery.datasets`. Then it uses those
   datasets instead (`ml_index_pattern` in `elastic_config/deployer_package_templates.py`;
   aws_elb_metrics_otel 0.12.x reads `metrics-aws.applicationelb.otel-*` while the package
   writes `aws.elb.otel`). Emit to the package datasets; do not add companion streams for
   a module's index.
2. **Queries.** Documents must match the module's recognizer `query` and each
   datafeed's `query` (CloudWatch modules pin `stat` and a `MetricName` list: emit every
   listed metric with that `stat`).
3. **Aggregation sources.** Aggregated datafeeds (`summary_count_field_name: doc_count`)
   bucket on composite sources such as `cloud.account.id`, `cloud.region` and the
   resource id. A document missing any of them never reaches the job.
4. **Detectors.** Each detector's `field_name` is numeric on some datafeed document that
   also carries its `by_field_name`, `over_field_name` and `partition_field_name`
   (e.g. `k8s.pod.memory.working_set` by `k8s.statefulset.name` needs StatefulSet pods).
   Every influencer is emitted.

## Exemptions

- `tests/fixtures/template_emit_skips.json` — permanent, reasoned skips for cases a
  generator cannot meet (an upstream template bug). `{kind: {template_id: {reason,
  generators?}}}`. Report the bug upstream.
- `tests/fixtures/template_emit_pending.json` — known generator gaps not fixed yet, run
  as **strict** xfail: fixing the gap makes the test fail until its entry is removed.
  New generators must not add entries.

## Library investigation workflows

Packages ship no workflows, but the Elastic Workflow Template Library
(`elastic_config/workflows/library/`, vendored by `scripts/snapshot_workflow_templates.py`)
has workflows that investigate one integration's data, e.g.
`aws-rds-connection-exhaustion-investigation-otel`. `elastic_config/workflow_templates.py`
deploys one whenever every **required** ES|QL step (no `on-failure: continue`) reads a
dataset an enabled generator writes, and wires it to the OOTB rules that read its evidence
step's data, group by its `kibana.alert.grouping.<Dim>` and read one of its metrics.

So a generator whose dataset such a workflow reads must also satisfy the workflow:

1. **Every term the step filters on is true on its documents**, with the exact field
   path the step uses: `attributes.DBInstanceIdentifier == "<id>"` needs a **data-point**
   attribute, unlike an SLO's bare `DBInstanceIdentifier`. Dimension levels matter too:
   `attributes.TargetGroup IS NULL AND attributes.AvailabilityZone IS NULL` reads the
   load-balancer-level copy of a CloudWatch series.
2. **Every aggregate has data**: `MAX(metric) WHERE attributes.stat == "Maximum"` needs that
   metric emitted with that `stat`. Sparse CloudWatch metrics an optional step reads
   (`HTTPCode_ELB_503_Count`, `TargetConnectionErrorCount`) are emitted as zeros at
   baseline, so the report says "0" rather than "not available".
3. **The resource id an input names comes from the generator.** Expose
   `workflow_input_values(scenario_data) -> {input_name: value}` in `generator.py`
   (`load_balancer`, `db_instance_identifier`, …), computed by the same helper `run()` uses.
   The deployer fills manual-run defaults and alert-run fallbacks with it; the test
   substitutes it for `{{ steps.resolve.output.<input> }}`.

```bash
python3 scripts/snapshot_workflow_templates.py      # network: refresh the vendored library
python3 -m pytest -q tests/test_workflow_template_eligibility.py tests/test_workflow_template_emit.py -k <id>
```

Workflow step gaps use the same `template_emit_pending.json` (strict xfail) and
`template_emit_skips.json` (`"workflow": {"<slug>.<step>": {reason, generators?}}`).
A step whose dataset starts with a wildcard (`logs-*.otel-*`, generic application logs)
belongs to no one generator and is not tested per generator.

