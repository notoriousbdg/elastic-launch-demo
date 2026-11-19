#!/usr/bin/env python3
"""AWS CloudWatch Logs generator — RDS Postgres log and an ECS service access log.

Writes the two CloudWatch log groups the Template Library RDS investigation workflow
(``aws-rds-connection-exhaustion-investigation-otel``) reads to turn "connections were
refused" and "the dependent service failed" from inferences into observations:

  * ``logs-aws_logs.rds-<ns>`` — the RDS PostgreSQL log, log group
    ``/aws/rds/instance/<DBInstanceIdentifier>/postgresql``. Healthy batches hold
    connection, statement-duration and checkpoint lines; ``connection_refusals`` adds
    one ``FATAL: remaining connection slots are reserved …`` line per refused session.
  * ``logs-aws_logs.ecs-<ns>`` — the access log of an ECS service (the ALB's target),
    log group ``/ecs/<gateway>``, in Common Log Format with an ``HTTP/1.1`` request
    line; ``gateway_error_fraction`` answers that share of requests with 5xx.

The documents use the field layout of CloudWatch logs shipped to Elastic
(``message``, ``aws.cloudwatch.log_group`` / ``log_stream``) and are bulk-written to
classic data streams. The DBInstanceIdentifier is the one ``aws_rds_otel`` emits for
the same scenario, so the workflow's metric and log steps name the same instance.

Synthetic (no integration package). Usage (standalone):
    python3 -m generators.aws_cloudwatch_logs.generator
"""

from __future__ import annotations

import logging
import os
import random
import re
import signal
import threading
import time
from datetime import datetime, timezone

from app.config import NAMESPACE
from app.telemetry import ESBulkClient
from generator_engine.db_common import business_load, resolve_active_chaos
from generator_engine.identity import ScenarioIdentity
from generator_engine.tick import run_tick_loop
from generators.aws_cloudwatch_logs import SPEC as _SPEC
from generators.aws_rds_otel.generator import _instance_id as _rds_instance_id

logger = logging.getLogger("aws-cloudwatch-logs-generator")

BATCH_INTERVAL_MIN = 30
BATCH_INTERVAL_MAX = 60

RDS_DATASET = "aws_logs.rds"
ECS_DATASET = "aws_logs.ecs"
_REFUSAL = 'remaining connection slots are reserved for roles with privileges of the "rds_reserved" role'
_DEFAULT_PATHS = ["/api/v1/orders", "/api/v1/cart", "/api/v1/catalog/search", "/api/v1/health"]
_AGENTS = ["Mozilla/5.0", "okhttp/4.12.0", "python-requests/2.32.3", "ELB-HealthChecker/2.0"]


def data_stream_namespace(ns: str) -> str:
    """Data stream namespaces may not contain '-' or upper case."""
    return re.sub(r"[^a-z0-9_]", "_", (ns or "default").lower())


def rds_log_group(instance_id: str) -> str:
    return f"/aws/rds/instance/{instance_id}/postgresql"


def postgres_line(ts: float, client_ip: str, port: int, user: str, db: str, pid: int,
                  level: str, text: str) -> str:
    """One RDS PostgreSQL log line (default ``log_line_prefix`` ``%t:%r:%u@%d:[%p]:``)."""
    stamp = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(ts))
    return f"{stamp}:{client_ip}({port}):{user}@{db}:[{pid}]:{level}:  {text}"


def access_line(ts: float, client_ip: str, method: str, path: str, status: int, size: int,
                agent: str) -> str:
    """One access-log line in Common Log Format (``… "GET /path HTTP/1.1" 200 …``)."""
    stamp = time.strftime("%d/%b/%Y:%H:%M:%S +0000", time.gmtime(ts))
    return f'{client_ip} - - [{stamp}] "{method} {path} HTTP/1.1" {status} {size} "-" "{agent}"'


def _doc(ts: float, message: str, dataset: str, ds_ns: str, group: str, stream: str,
         region: str, account_id: str, level: str) -> dict:
    return {
        "@timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="milliseconds"),
        "message": message,
        "data_stream": {"type": "logs", "dataset": dataset, "namespace": ds_ns},
        "event": {"dataset": dataset},
        "log": {"level": level},
        "aws": {"cloudwatch": {"log_group": group, "log_stream": stream}},
        "cloud": {"provider": "aws", "region": region, "account": {"id": account_id}},
    }


def _ip(rng: random.Random, subnet: str = "10.1") -> str:
    return f"{subnet}.{rng.randint(0, 15)}.{rng.randint(2, 250)}"


def rds_records(rng: random.Random, now: float, interval: float, refusals: int,
                instance_id: str, app_user: str, database: str) -> list[tuple[float, str, str]]:
    """``(timestamp, level, line)`` for one batch of the RDS PostgreSQL log."""
    records = []
    for _ in range(rng.randint(2, 6)):
        ts = now - rng.uniform(0, interval)
        pid = rng.randint(10_000, 60_000)
        text = rng.choice([
            f"connection authorized: user={app_user} database={database} SSL enabled",
            f"duration: {rng.uniform(0.4, 40.0):.3f} ms  statement: SELECT id, status FROM orders WHERE id = $1",
            "checkpoint complete: wrote 412 buffers (2.5%); 0 WAL file(s) added, 0 removed, 1 recycled",
        ])
        records.append((ts, "info", postgres_line(ts, _ip(rng, "10.2"), rng.randint(30_000, 60_000),
                                                  app_user, database, pid, "LOG", text)))
    for _ in range(max(0, int(refusals))):
        ts = now - rng.uniform(0, interval)
        records.append((ts, "fatal", postgres_line(
            ts, _ip(rng, "10.2"), rng.randint(30_000, 60_000), app_user, database,
            rng.randint(10_000, 60_000), "FATAL", _REFUSAL,
        )))
    return sorted(records)


def access_records(rng: random.Random, now: float, interval: float, error_fraction: float,
                   paths: list[str]) -> list[tuple[float, str, str]]:
    """``(timestamp, level, line)`` for one batch of the service access log."""
    records = []
    for _ in range(max(1, int(rng.randint(20, 60) * business_load()))):
        ts = now - rng.uniform(0, interval)
        if error_fraction > 0 and rng.random() < error_fraction:
            status = rng.choice([503, 503, 500, 502])
        else:
            status = rng.choices([200, 201, 204, 404, 400], weights=[80, 8, 6, 4, 2])[0]
        method = "POST" if status == 201 else rng.choice(["GET", "GET", "GET", "POST", "PUT"])
        line = access_line(ts, _ip(rng), method, rng.choice(paths), status,
                           rng.randint(120, 8_000) if status < 500 else rng.randint(80, 200),
                           rng.choice(_AGENTS))
        records.append((ts, "error" if status >= 500 else "info", line))
    return sorted(records)


def run(
    client: ESBulkClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run the CloudWatch logs generator loop until stop_event is set."""
    rng = random.Random()
    ns = (scenario_data or {}).get("namespace") or NAMESPACE
    cfg = (scenario_data or {}).get("generator_config") or {}
    scenario = (scenario_data or {}).get("scenario")
    ident = ScenarioIdentity.from_scenario(scenario_data, ns)

    if not getattr(client, "configured", True):
        logger.warning("ESBulkClient not configured; CloudWatch logs generator will not emit.")
        return

    rds_cfg = scenario.get_generator_config("aws_rds_otel") if scenario is not None else {}
    instance_id = _rds_instance_id(ns, rds_cfg or {})
    gateway = ident.qualify(str(cfg.get("gateway_service") or "gateway"))
    region = (cfg.get("cloud") or {}).get("region", "us-east-1")
    account_id = ident.aws_account_id
    app_user = ident.database("app")
    database = ident.database("orders")
    profile = getattr(scenario, "raw_log_profile", None) or {}
    paths = [p for p in profile.get("paths") or [] if p] or _DEFAULT_PATHS

    ds_ns = data_stream_namespace(ns)
    rds_stream = f"logs-{RDS_DATASET}-{ds_ns}"
    ecs_stream = f"logs-{ECS_DATASET}-{ds_ns}"
    rds_group = rds_log_group(instance_id)
    ecs_group = f"/ecs/{gateway}"
    ecs_log_stream = f"ecs/{gateway}/{ident.aws_id('instance', 'task', 1)[2:]}"
    _channel_registry: dict = (scenario_data or {}).get("channel_registry", {})
    chaos_mode = cfg.get("chaos", "explicit")
    sleep_time = float(BATCH_INTERVAL_MIN)

    logger.info("CloudWatch logs generator started (namespace=%s, rds=%s, ecs=%s)",
                ns, rds_group, ecs_group)

    def _emit_tick() -> None:
        nonlocal sleep_time
        active = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="aws_cloudwatch_logs",
            chaos_mode=chaos_mode,
            impact_keys=_SPEC.impact_keys,
        ) or {}
        refusals = int(active.get("connection_refusals", 0) or 0)
        error_fraction = float(active.get("gateway_error_fraction", 0.0) or 0.0)
        now = time.time()
        rds_docs = [
            _doc(ts, line, RDS_DATASET, ds_ns, rds_group, f"{instance_id}.0", region, account_id, level)
            for ts, level, line in rds_records(rng, now, sleep_time, refusals, instance_id, app_user, database)
        ]
        ecs_docs = [
            _doc(ts, line, ECS_DATASET, ds_ns, ecs_group, ecs_log_stream, region, account_id, level)
            for ts, level, line in access_records(rng, now, sleep_time, error_fraction, paths)
        ]
        client.send_bulk(rds_stream, rds_docs)
        client.send_bulk(ecs_stream, ecs_docs)
        sleep_time = rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: sleep_time)
    logger.info("CloudWatch logs generator stopped.")


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = ESBulkClient()
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    threading.Timer(int(os.environ.get("RUN_DURATION", "60")), stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
