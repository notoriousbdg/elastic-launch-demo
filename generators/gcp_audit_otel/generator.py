#!/usr/bin/env python3
"""GCP Audit Log Generator — sends GCP Cloud Audit Logs via OTLP.

Emits data matching the gcp_audit_otel content package
(discovery.datasets: gcp.auditlog.otel), which populates the bundled
GCP Audit Logs dashboards.

Field names verified against:
  - elastic/integrations/packages/gcp_audit_otel/kibana/dashboard/*.json

Key attributes:
  gcp.audit.service.name, gcp.audit.method.name, gcp.audit.resource.name,
  user.email, user_agent.original, rpc.jsonrpc.error_code

Usage (standalone):
    python3 -m generators.gcp_audit_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/gcp_audit_otel@0.3.0"

import logging
import os
import random
import signal
import threading
import time

from app.telemetry import OTLPClient
from app.config import NAMESPACE
from generator_engine.db_common import (
    build_audit_log_record,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

GCP_AUDIT_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudpubsubreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("gcp-audit-generator")
BATCH_INTERVAL_MIN = 5
BATCH_INTERVAL_MAX = 15

_GCP_SERVICES = [
    ("storage.googleapis.com", "storage.objects.get"),
    ("storage.googleapis.com", "storage.objects.create"),
    ("compute.googleapis.com", "beta.compute.instances.get"),
    ("compute.googleapis.com", "v1.compute.instances.insert"),
    ("iam.googleapis.com", "google.iam.v1.IAMPolicy.GetIamPolicy"),
    ("iam.googleapis.com", "google.iam.v1.IAMPolicy.SetIamPolicy"),
    ("bigquery.googleapis.com", "jobservice.query"),
    ("bigquery.googleapis.com", "tableservice.list"),
    ("cloudsql.googleapis.com", "cloudsql.instances.get"),
    ("container.googleapis.com", "io.k8s.core.v1.pods.list"),
    ("secretmanager.googleapis.com", "google.cloud.secretmanager.v1.SecretManagerService.AccessSecretVersion"),
    ("cloudkms.googleapis.com", "google.cloud.kms.v1.KeyManagementService.Decrypt"),
]

_JSON_RPC_ERRORS = [
    "PERMISSION_DENIED", "NOT_FOUND", "INVALID_ARGUMENT",
    "QUOTA_EXCEEDED", "UNAUTHENTICATED",
]

_USER_AGENTS = [
    "python-requests/2.31.0",
    "google-cloud-python/2.8.0",
    "Terraform/1.7.0",
    "gcloud/468.0.0",
    "k8s-client/1.28",
]


def _default_cloud() -> dict:
    return {"provider": "gcp", "platform": "gcp_compute_engine",
            "region": "us-central1", "availability_zone": "us-central1-b"}


def _generate_records(
    principals: list[str],
    project_id: str,
    rng: random.Random,
    count: int,
    error_rate: float = 0.0,
) -> list[dict]:
    records = []
    for _ in range(count):
        principal = rng.choice(principals)
        svc, method = rng.choice(_GCP_SERVICES)
        resource_name = f"projects/{project_id}/{svc.split('.')[0]}/test-resource-{rng.randint(1,100)}"

        is_error = rng.random() < max(error_rate, 0.005)
        outcome = "failure" if is_error else "success"
        # The dashboard's Event outcome / error table panels filter root log.level ==
        # "ERROR"; in logs-*.otel log.level is an alias of severity_text, so the record's
        # uppercase severity populates it. Never write log.level as a concrete field.
        severity = "ERROR" if is_error else "INFO"
        error_code = rng.choice(_JSON_RPC_ERRORS) if is_error else None

        jitter = rng.randint(0, int(BATCH_INTERVAL_MAX * 1e9))
        extra: dict = {
            "gcp.audit.service.name": svc,
            "gcp.audit.method.name": method,
            "gcp.audit.resource.name": resource_name,
            "user.email": principal,
            "user_agent.original": rng.choice(_USER_AGENTS),
            "cloud.project.id": project_id,
        }
        if error_code:
            extra["rpc.jsonrpc.error_code"] = error_code

        records.append(build_audit_log_record(
            actor=principal,
            action=method,
            outcome=outcome,
            source_address=f"203.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}",
            event_name="gcp.audit.log",
            severity=severity,
            extra_attrs=extra,
            jitter_ns=jitter,
        ))
    return records



from generators.gcp_audit_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop

def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    rng = random.Random()
    ns = scenario_data["namespace"] if scenario_data else NAMESPACE
    cfg = scenario_data.get("generator_config", {}) if scenario_data else {}
    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("gcp-audit")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("gcp-audit")
    cloud = cfg.get("cloud") or _default_cloud()
    project_id = ScenarioIdentity(ns).gcp_project_id
    records_per_cycle: int = cfg.get("records_per_cycle", 10)

    default_principals = [
        f"{role}@{ScenarioIdentity(ns).gcp_project_id}.iam.gserviceaccount.com"
        for role in ("admin", "developer", "ci-runner", "readonly", "infra-svc")
    ]
    principals: list[str] = cfg.get("principals") or default_principals


    resource = build_signal_resource(
        ns=ns, signal_type="logs", dataset=otel_dataset("gcp.auditlog"),
        service_name=service_name, service_version="1.0.0",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name, cloud=cloud,
        extra_attrs={"cloud.project.id": project_id},
    )
    scope = {"name": GCP_AUDIT_SCOPE, "version": SCOPE_VERSION}
    logger.info("GCP Audit log generator started (namespace=%s, project=%s)", ns, project_id)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="gcp_audit_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        error_rate = float((active_chaos or {}).get("error_rate", 0.0))
        n = int((active_chaos or {}).get("records_per_cycle", records_per_cycle))
        load = business_load()
        n = max(1, int(n * load))

        records = _generate_records(principals, project_id, rng, n, error_rate)
        client.send_logs_scoped(resource, scope, records)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("GCP Audit log generator stopped.")


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "60"))
    def _stop(sig, frame): stop_event.set()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    client = OTLPClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()

if __name__ == "__main__":
    main()
