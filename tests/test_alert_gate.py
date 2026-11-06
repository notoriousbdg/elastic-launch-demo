"""The add-telemetry-generator alert-emit gate: no false positives for query aliases or
resource attributes every generator gets from build_signal_resource, and real gaps still found.
"""
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / ".claude" / "skills" / "add-telemetry-generator" / "scripts"


def _load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


extract = _load("extract_alert_esql_fields")
verify = _load("verify_alert_emit")

ALIAS_QUERY = """FROM metrics-demo.otel-*
| STATS retry_inc = SUM(INCREASE(demo.retries)), conns = MAX(demo.conns) BY service.name
| EVAL ratio = COALESCE(retry_inc, 0) / COALESCE(conns, 1)
| WHERE ratio > 0.5"""


def _fields(query):
    return extract.parse_esql(query)


def test_stats_alias_in_coalesce_is_not_a_required_field():
    inv = _fields(ALIAS_QUERY)
    assert "retry_inc" not in inv["coalesce_operands"]
    assert "conns" not in inv["coalesce_operands"]
    assert "retry_inc" not in inv["fields"]


def test_real_column_in_coalesce_is_still_required():
    inv = _fields(
        "FROM metrics-demo.otel-* | STATS a = MAX(COALESCE(demo.real_metric, demo.other)) BY service.name"
    )
    assert "demo.real_metric" in inv["coalesce_operands"]


def test_equality_comparison_is_not_an_alias():
    inv = _fields('FROM metrics-demo.otel-* | WHERE stat == "Sum" AND demo.x IS NOT NULL | EVAL v = COALESCE(demo.x, 0)')
    assert "demo.x" in inv["coalesce_operands"]


def _inventory(tmp_path, operands):
    path = tmp_path / "inv.json"
    path.write_text(json.dumps({"alert_count": 1, "fields": [], "coalesce_operands": operands,
                                "counter_required": [], "from_streams": []}))
    return path


def _generator(tmp_path, source, uses_resource_builder):
    gen = tmp_path / "generators" / "demo"
    gen.mkdir(parents=True)
    (gen / "generator.py").write_text(source)
    engine = tmp_path / "generator_engine"
    engine.mkdir()
    (engine / "db_common.py").write_text(
        'def build_signal_resource():\n    return {"service.instance.id": 1, "service.name": 2}\n'
    )
    return gen


def test_resource_attribute_from_build_signal_resource_is_found(tmp_path):
    gen = _generator(tmp_path, "build_signal_resource(service_instance_id=x)\n", True)
    inv = _inventory(tmp_path, ["service.instance.id"])
    assert verify.main(["--inventory", str(inv), "--generator-dir", str(gen)]) == 0


def test_missing_field_is_still_reported_without_the_builder(tmp_path):
    gen = _generator(tmp_path, "emit('other')\n", False)
    inv = _inventory(tmp_path, ["service.instance.id"])
    assert verify.main(["--inventory", str(inv), "--generator-dir", str(gen)]) == 1


def test_audit_field_match_is_not_a_substring_match():
    spec = importlib.util.spec_from_file_location("audit_package_coverage", ROOT / "scripts" / "audit_package_coverage.py")
    audit = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(audit)

    source = '_gauge("k8s.container.cpu.usage", "1", 1)'
    assert audit.mentions(source, "k8s.container.cpu.usage")
    assert not audit.mentions(source, "container.cpu.usage")  # only a suffix of a longer name
    assert audit.mentions('"container.cpu.usage"', "container.cpu.usage")
    assert not audit.mentions('"container.cpu.usage_total"', "container.cpu.usage")


# ── SLO-template / ML-module gate (extract_package_templates + verify_template_emit) ──

templates = _load("extract_package_templates")
verify_templates = _load("verify_template_emit")


def _kql(text, doc):
    return templates.kql_matches(templates.parse_kql(text), doc.get)


def test_kql_terms_wildcards_lists_ranges_and_not():
    doc = {"MetricName": "ApproximateNumberOfMessagesVisible", "QueueName": "orders-dlq",
           "vercel.log.source": "edge", "http.response.status_code": 200}
    assert _kql('MetricName: "ApproximateNumberOfMessagesVisible" AND QueueName: *dlq*', doc)
    assert not _kql('NOT QueueName: *dlq*', doc)
    assert _kql('vercel.log.source: ("lambda" or "edge") and http.response.status_code >= 0', doc)
    assert not _kql('missing.field: *', doc)
    assert _kql('not (a: "x" or b: "y")', doc)
    assert _kql("", doc)


def test_kql_required_terms_are_the_top_level_and_terms():
    node = templates.parse_kql('data_stream.dataset: "aws.rds.otel" AND stat: "Average" AND NOT x: *')
    assert templates.kql_required_terms(node) == [
        {"field": "data_stream.dataset", "value": "aws.rds.otel"},
        {"field": "stat", "value": "Average"},
    ]


def test_query_dsl_bool_term_terms_exists_and_should():
    query = {"bool": {
        "filter": [{"term": {"stat": "Average"}}, {"terms": {"MetricName": ["A", "B"]}}],
        "must_not": {"terms": {"_tier": ["data_frozen"]}},
        "should": [{"exists": {"field": "k8s.deployment.name"}}, {"exists": {"field": "k8s.statefulset.name"}}],
        "minimum_should_match": 1,
    }}
    doc = {"stat": "Average", "MetricName": "B", "k8s.statefulset.name": "db"}
    assert templates.dsl_matches(query, doc.get)
    assert not templates.dsl_matches(query, {**doc, "k8s.statefulset.name": None}.get)
    assert templates.dsl_fields(query) == ["stat", "MetricName", "k8s.deployment.name", "k8s.statefulset.name"]


def test_slo_equation_and_aggregations():
    assert templates.evaluate_equation("(A-B)/(C-D)", {"A": 9, "B": 1, "C": 5, "D": 1}) == 2.0
    assert templates.evaluate_equation("(A-B)/(C-D)", {"A": 1, "B": 1, "C": 1, "D": 1}) is None
    assert templates.evaluate_equation("__import__('os')", {}) is None
    assert templates.aggregate("percentile", [1, 2, 3, 4], percentile=75) == 3
    assert templates.aggregate("doc_count", [None, None]) == 2.0


def test_ml_index_mismatch_is_flagged_against_package_datasets():
    module = {"attributes": {"id": "m", "defaultIndexPattern": "metrics-aws.applicationelb.otel-*",
                             "jobs": [], "datafeeds": []}}
    assert templates.ml_inventory(module, ["aws.elb.otel"])["index_mismatch"]
    assert not templates.ml_inventory(module, ["aws.applicationelb.otel"])["index_mismatch"]
    manifest = "name: x\ndiscovery:\n  fields: []\n  datasets:\n    - name: aws.elb.otel\n    - name: aws.elb_network.otel\nicons: []\n"
    assert templates.discovery_datasets(manifest) == ["aws.elb.otel", "aws.elb_network.otel"]


def test_template_gate_reports_a_missing_slo_group_field(tmp_path):
    gen = _generator(tmp_path, '"aws.rds.otel"\n"ReadLatency"\nbuild_cloudwatch_metric("RDS", "ReadLatency")\n', False)
    out = tmp_path / "inv"
    out.mkdir()
    slo = templates.slo_inventory({"id": "s", "attributes": {
        "name": "s", "budgetingMethod": "timeslices", "objective": {}, "timeWindow": {},
        "groupBy": ["DBInstanceIdentifier"],
        "indicator": {"type": "sli.metric.timeslice", "params": {
            "index": "metrics-*",
            "filter": 'Namespace: "AWS/RDS" AND MetricName: "ReadLatency" AND stat: "Average"',
            "metric": {"metrics": [{"name": "A", "aggregation": "avg",
                                    "field": "metrics.amazonaws.com/AWS/RDS/ReadLatency"}],
                       "equation": "A", "comparator": "LT", "threshold": 0.01}}}}})
    (out / "slo_required_fields.json").write_text(json.dumps([slo]))
    assert verify_templates.main(["--inventory-dir", str(out), "--generator-dir", str(gen)]) == 1
    (gen / "generator.py").write_text((gen / "generator.py").read_text() + '"Average"\n"DBInstanceIdentifier"\n')
    assert verify_templates.main(["--inventory-dir", str(out), "--generator-dir", str(gen)]) == 0
