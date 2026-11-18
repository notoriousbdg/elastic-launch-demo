#!/usr/bin/env python3
"""Generate the generators/faults reference in the docs site from generator SPECs.

Rewrites the block between ``<!-- generators:start -->`` and ``<!-- generators:end -->``
in ``docker/docs/docs/create-scenario.md``.  Run it after adding or changing a
generator (or one of its ``impact_keys``).

    python scripts/gen_generator_docs.py          # rewrite the docs
    python scripts/gen_generator_docs.py --check  # exit 1 if the docs are stale
"""

from __future__ import annotations

import argparse
import ast
import html
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DOCS_FILE = ROOT.parent / "docs" / "docs" / "create-scenario.md"
START, END = "<!-- generators:start -->", "<!-- generators:end -->"

# (family key, heading, one-line description), in display order.
FAMILIES = [
    ("core", "Core", "Always-on building blocks: traces, host and Kubernetes metrics, web logs and network flow logs."),
    ("database", "Databases", "Relational, document and time-series databases."),
    ("cache", "Caches", "In-memory data stores."),
    ("queue", "Queues and messaging", "Message brokers and queues."),
    ("proxy", "Proxies and ingress", "Load balancers, proxies and ingress controllers."),
    ("web", "Web and frontend", "Web servers, application servers and client-side telemetry."),
    ("platform", "Platform", "Runtimes, orchestration, coordination and infrastructure services."),
    ("cloud_compute", "Cloud compute and services", "AWS and GCP compute, storage, database and load balancer services."),
    ("cloud_audit", "Cloud audit and network", "Cloud audit logs, WAF and gateway logs."),
    ("ai", "AI", "Model-serving and agent platforms."),
]

ALERT_PATTERNS = (re.compile(r'Drive alert "([^"]+)"'), re.compile(r"Alert: (.+?)\.?\s*$"))

# Human-readable generator names (shown in the docs); a new generator falls back to its id.
NAMES = {
    'activemq_otel': 'ActiveMQ',
    'airflow_otel': 'Apache Airflow',
    'apache_otel': 'Apache HTTP Server',
    'apache_tomcat_otel': 'Apache Tomcat',
    'aws_bedrock': 'AWS Bedrock',
    'aws_bedrock_agentcore': 'AWS Bedrock AgentCore',
    'aws_cloudtrail_otel': 'AWS CloudTrail logs',
    'aws_cloudwatch_logs': 'AWS CloudWatch logs',
    'aws_ec2_otel': 'AWS EC2',
    'aws_ecs_otel': 'AWS ECS',
    'aws_elb_metrics_otel': 'AWS ELB metrics',
    'aws_elb_otel': 'AWS ELB access logs',
    'aws_lambda_otel': 'AWS Lambda',
    'aws_rds_otel': 'AWS RDS',
    'aws_sqs_otel': 'AWS SQS',
    'aws_vpcflow_otel': 'AWS VPC flow logs',
    'aws_waf_otel': 'AWS WAF logs',
    'azure_activity_otel': 'Azure Activity logs',
    'azure_ai_foundry': 'Azure AI Foundry',
    'azure_billing': 'Azure Billing',
    'azure_gateway_otel': 'Azure Application Gateway logs',
    'azure_openai': 'Azure OpenAI',
    'azure_vnetflow_otel': 'Azure VNet flow logs',
    'cassandra_otel': 'Apache Cassandra',
    'cockroachdb_otel': 'CockroachDB',
    'couchdb_otel': 'Apache CouchDB',
    'docker_otel': 'Docker',
    'envoyproxy_otel': 'Envoy Proxy',
    'etcd_otel': 'etcd',
    'gcp_audit_otel': 'GCP Audit logs',
    'gcp_cloudrun_otel': 'GCP Cloud Run',
    'gcp_cloudsql_mysql_otel': 'GCP Cloud SQL for MySQL',
    'gcp_cloudsql_postgresql_otel': 'GCP Cloud SQL for PostgreSQL',
    'gcp_cloudsql_sqlserver_otel': 'GCP Cloud SQL for SQL Server',
    'gcp_cloudstorage_otel': 'GCP Cloud Storage',
    'gcp_compute_otel': 'GCP Compute Engine',
    'gcp_vertexai': 'GCP Vertex AI',
    'gcp_vpcflow_otel': 'GCP VPC flow logs',
    'haproxy_otel': 'HAProxy',
    'host_metrics': 'Host metrics',
    'ibmmq_otel': 'IBM MQ',
    'iis_otel': 'Microsoft IIS',
    'influxdb_otel': 'InfluxDB',
    'jvm': 'JVM metrics',
    'kafka_otel': 'Apache Kafka',
    'kubernetes_otel': 'Kubernetes',
    'memcached_otel': 'Memcached',
    'microsoft_sqlserver_otel': 'Microsoft SQL Server',
    'mongodb_otel': 'MongoDB',
    'mysql_otel': 'MySQL',
    'nginx_ingress_controller_otel': 'NGINX Ingress Controller',
    'nginx_metrics': 'NGINX metrics',
    'nginx_otel': 'NGINX logs',
    'nvidia_gpu_otel': 'NVIDIA GPU',
    'oracle_otel': 'Oracle Database',
    'otel_android_dashboards': 'Android app (OpenTelemetry)',
    'otel_collector_internal_telemetry': 'OpenTelemetry Collector internal telemetry',
    'otel_ios_dashboards': 'iOS app (OpenTelemetry)',
    'otel_rum_dashboards': 'Browser RUM (OpenTelemetry)',
    'postgresql_otel': 'PostgreSQL',
    'profilingmetrics_otel': 'Profiling metrics',
    'rabbitmq_otel': 'RabbitMQ',
    'raw_access': 'Raw access logs',
    'redis_otel': 'Redis',
    'redisenterprise_otel': 'Redis Enterprise',
    'supabase_otel': 'Supabase',
    'temporal_otel': 'Temporal Cloud',
    'traces': 'Distributed traces',
    'traefik_otel': 'Traefik',
    'vercel_otel': 'Vercel',
    'vsphere_otel': 'VMware vSphere',
    'zookeeper_otel': 'Apache ZooKeeper',
}

# What each generator sends (reviewed wording); a new generator falls back to its docstring.
OVERRIDES = {
    'activemq_otel': 'ActiveMQ broker, JVM and destination metrics',
    'airflow_otel': 'Apache Airflow StatsD-style metrics',
    'apache_otel': 'Apache HTTP Server metrics',
    'apache_tomcat_otel': 'Tomcat JMX metrics',
    'aws_bedrock': 'AWS Bedrock model invocation telemetry',
    'aws_bedrock_agentcore': 'AWS Bedrock AgentCore agent telemetry',
    'aws_cloudtrail_otel': 'CloudTrail API audit logs',
    'aws_cloudwatch_logs': 'CloudWatch logs for RDS Postgres and an ECS service access log',
    'aws_ec2_otel': 'EC2 CloudWatch metrics',
    'aws_ecs_otel': 'ECS CloudWatch metrics',
    'aws_elb_metrics_otel': 'ALB, NLB and GWLB CloudWatch metrics',
    'aws_elb_otel': 'ALB access log records',
    'aws_lambda_otel': 'Lambda CloudWatch metrics',
    'aws_rds_otel': 'AWS RDS CloudWatch metrics',
    'aws_sqs_otel': 'SQS CloudWatch metrics',
    'aws_vpcflow_otel': 'AWS VPC flow logs',
    'aws_waf_otel': 'WAF web request logs',
    'azure_activity_otel': 'Azure control-plane audit logs',
    'azure_ai_foundry': 'Microsoft Foundry (Azure AI Foundry) telemetry',
    'azure_billing': 'Azure billing and cost telemetry',
    'azure_gateway_otel': 'Azure Application Gateway access and firewall logs',
    'azure_openai': 'Azure OpenAI telemetry',
    'azure_vnetflow_otel': 'Azure VNet flow logs',
    'cassandra_otel': 'Cassandra JMX metrics',
    'cockroachdb_otel': 'CockroachDB Prometheus metrics',
    'couchdb_otel': 'CouchDB metrics',
    'docker_otel': 'Docker container stats metrics',
    'envoyproxy_otel': 'Envoy Proxy StatsD-shaped metrics',
    'etcd_otel': 'etcd Prometheus metrics',
    'gcp_audit_otel': 'GCP Cloud Audit Logs',
    'gcp_cloudrun_otel': 'GCP Cloud Run Cloud Monitoring metrics',
    'gcp_cloudsql_mysql_otel': 'Cloud SQL for MySQL Cloud Monitoring metrics',
    'gcp_cloudsql_postgresql_otel': 'Cloud SQL for PostgreSQL Cloud Monitoring metrics',
    'gcp_cloudsql_sqlserver_otel': 'Cloud SQL for SQL Server Cloud Monitoring metrics',
    'gcp_cloudstorage_otel': 'GCP Cloud Storage Cloud Monitoring metrics',
    'gcp_compute_otel': 'GCP Compute Engine Cloud Monitoring metrics',
    'gcp_vertexai': 'Google Vertex AI telemetry',
    'gcp_vpcflow_otel': 'GCP VPC flow logs',
    'haproxy_otel': 'HAProxy metrics',
    'host_metrics': 'Host `system.*` metrics for the Infrastructure UI',
    'ibmmq_otel': 'IBM MQ queue manager metrics',
    'iis_otel': 'IIS metrics',
    'influxdb_otel': 'InfluxDB Prometheus metrics',
    'jvm': 'JVM runtime metrics',
    'kafka_otel': 'Kafka metrics',
    'kubernetes_otel': 'Kubernetes node, pod, container and deployment metrics',
    'memcached_otel': 'Memcached metrics',
    'microsoft_sqlserver_otel': 'SQL Server metrics and logs',
    'mongodb_otel': 'MongoDB metrics',
    'mysql_otel': 'MySQL events and metrics',
    'nginx_ingress_controller_otel': 'NGINX Ingress Controller access and error logs',
    'nginx_metrics': 'Nginx `stub_status` metrics',
    'nginx_otel': 'Nginx access and error logs',
    'nvidia_gpu_otel': 'NVIDIA GPU (DCGM exporter) metrics',
    'oracle_otel': 'Oracle Database metrics and logs',
    'otel_android_dashboards': 'Android app telemetry (EDOT / opentelemetry-android)',
    'otel_collector_internal_telemetry': 'OTel Collector internal (`otelcol_*`) metrics',
    'otel_ios_dashboards': 'iOS app telemetry (EDOT iOS)',
    'otel_rum_dashboards': 'Browser RUM telemetry (OpenTelemetry JS SDK)',
    'postgresql_otel': 'PostgreSQL events and metrics',
    'profilingmetrics_otel': 'Profiling metrics (EDOT profilingmetrics connector)',
    'rabbitmq_otel': 'RabbitMQ metrics',
    'raw_access': 'Unparsed CLF-style access logs, for Streams parsing',
    'redis_otel': 'Redis metrics',
    'redisenterprise_otel': 'Redis Enterprise cluster metrics',
    'supabase_otel': 'Supabase Prometheus metrics',
    'temporal_otel': 'Temporal Cloud (`temporal_cloud_v1_*`) metrics',
    'traces': 'Distributed traces for the APM Service Map',
    'traefik_otel': 'Traefik Prometheus metrics',
    'vercel_otel': 'Vercel drain payloads',
    'vsphere_otel': 'vSphere (vCenter) metrics',
    'zookeeper_otel': 'ZooKeeper metrics',
}

def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def display_name(gen_id: str) -> str:
    return NAMES.get(gen_id, gen_id)


def describe(gen_id: str) -> str:
    """What the generator sends: reviewed text first, then the module docstring."""
    if gen_id in OVERRIDES:
        return OVERRIDES[gen_id]
    path = ROOT / "generators" / gen_id / "generator.py"
    doc = ""
    if path.is_file():
        doc = (ast.get_docstring(ast.parse(path.read_text())) or "").split("\n")[0]
    for sep in (" — ", ": "):
        if sep in doc:
            doc = doc.split(sep, 1)[1]
            break
    doc = re.sub(r"^(sends|writes) ", "", doc.rstrip("."))
    doc = re.sub(r" via (OTLP|ES bulk)$", "", doc).replace("``", "`")
    return (doc[:1].upper() + doc[1:]) or gen_id


def alert_for(doc: str) -> str:
    for pattern in ALERT_PATTERNS:
        match = pattern.search(doc)
        if match:
            return match.group(1)
    return ""


def how_to_trigger(doc: str) -> str:
    """The knob doc without the alert name (it has its own column)."""
    text = re.sub(r'^Drive alert "[^"]+":\s*', "", doc)
    text = re.sub(r"\s*Alert: .+?\.?\s*$", "", text)
    return _clean(text).replace("|", "\\|")


def cell(text: str) -> str:
    return text.replace("|", "\\|")


def render(generators: dict) -> str:
    by_family: dict[str, list] = {}
    for spec in generators.values():
        by_family.setdefault(spec.family, []).append(spec)
    known = {key for key, _, _ in FAMILIES}
    unknown = sorted(set(by_family) - known)
    if unknown:
        raise SystemExit(f"gen_generator_docs: unknown generator family {unknown}; add it to FAMILIES")
    titles = {key: title for key, title, _ in FAMILIES}

    all_specs = list(generators.values())
    n_faults = sum(len(s.impact_keys) for s in all_specs)
    out = [f"{len(all_specs)} generators, {n_faults} fault knobs.", ""]

    for key, title, blurb in FAMILIES:
        specs = sorted(by_family.get(key, []), key=lambda s: display_name(s.id).lower())
        if not specs:
            continue
        out += [f"### {title}", "", blurb, ""]
        for spec in specs:
            n = len(spec.impact_keys)
            summary = f"{html.escape(display_name(spec.id), quote=False)} : <code>{spec.id}</code> - {n} fault{'s' if n != 1 else ''}"
            out += [f'<details class="note" id="gen-{spec.id}" markdown="1">',
                    f"<summary>{summary}</summary>", "",
                    f"Sends: {describe(spec.id)}.", "",
                    "| Fault (impact key) | OOTB alert | How to trigger |", "|---|---|---|"]
            for k, meta in spec.impact_keys.items():
                doc = meta.get("doc", "")
                out.append(f"| `{k}` | {cell(alert_for(doc))} | {how_to_trigger(doc)} |")
            out += ["", "</details>", ""]
    return "\n".join(out).rstrip() + "\n"


def build(text: str) -> str:
    from generator_engine.registry import GENERATORS

    if START not in text or END not in text:
        raise SystemExit(f"gen_generator_docs: {START} / {END} markers missing in {DOCS_FILE}")
    head, rest = text.split(START, 1)
    _, tail = rest.split(END, 1)
    return f"{head}{START}\n\n{render(GENERATORS)}\n{END}{tail}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--check", action="store_true", help="exit 1 if the docs are out of date")
    args = parser.parse_args()
    current = DOCS_FILE.read_text()
    new = build(current)
    if args.check:
        if new != current:
            print(f"{DOCS_FILE} is out of date; run: python scripts/gen_generator_docs.py", file=sys.stderr)
            return 1
        return 0
    if new != current:
        DOCS_FILE.write_text(new)
        print(f"updated {DOCS_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
