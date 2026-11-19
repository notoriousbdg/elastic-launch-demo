"""BackfillMixin — synthesize historical data so ML jobs have training material
and log rate analysis has visible change-points right after deploy.

Covers:
  - raw ECS access-log stream (log-rate / categorization ML)
  - healthy hostmetrics + k8s pod util (host/pod metrics ML baselines)
"""

from __future__ import annotations

import json
import logging
import random
import time

import httpx

from elastic_config.deployer_base import _es_headers, ConcurrentBulkIndexer, ProgressCallback, StepIdx

logger = logging.getLogger("deployer")


# Tuning constants — keep total bulk size modest so an ESS small cluster
# completes the backfill in under a minute.
_BACKFILL_HOURS = 12
_BACKFILL_AVG_LINES_PER_SEC = 5      # ~216k docs over 12h
_BULK_BATCH_DOCS = 500
# Metrics ML baseline: one sample every 5 minutes for 12h (~144 points).
_METRICS_BACKFILL_INTERVAL_SEC = 300
_METRICS_HEALTHY_CPU = 0.35
_METRICS_HEALTHY_MEM = 0.45


class BackfillMixin:

    def _deploy_ecs_log_backfill(self, client: httpx.Client, notify: ProgressCallback):
        """Bulk-index ~12h of synthetic raw ECS access logs to
        the `logs.ecs` stream so log-rate analysis and categorization have
        immediate signal."""
        # Import lazily so the deployer doesn't drag the generator's app.telemetry
        # (and its transitive app.config) dependency into other deploy paths.
        from generators.raw_access.generator import (
            data_stream_for,
            generate_record,
        )

        step = self._step(StepIdx.ECS_LOG_BACKFILL)
        step.status = "running"
        notify(self.progress)

        try:
            ns = self.scenario.namespace
            profile = self.scenario.raw_log_profile
            data_stream = data_stream_for(ns)

            # Ensure the data stream + Streams entity exist before bulk-writing.
            if hasattr(self, "_create_ecs_stream"):
                self._create_ecs_stream(client)

            # Concentrate spike traffic on the smallest-weighted country, same
            # behavior as the live generator (so visual signal matches).
            country_weights = profile["country_weights"]
            spike_country = min(country_weights.keys(), key=lambda k: country_weights[k])

            rng = random.Random(0xE15C)  # deterministic so re-deploys are reproducible
            now = time.time()
            start = now - _BACKFILL_HOURS * 3600
            from generators.raw_access.generator import (
                SPIKE_VOLUME_MULTIPLIER,
                plan_backfill_spike_windows,
                is_in_spike_window,
            )

            # Place a few irregular spike windows in the 12h history. Long
            # stretches of baseline between them so the rate ML job learns a
            # flat normal and surfaces the spikes as anomalies.
            spike_windows = plan_backfill_spike_windows(start, now, rng=rng)
            interval = 1.0 / _BACKFILL_AVG_LINES_PER_SEC

            with ConcurrentBulkIndexer(
                self.elastic_url, self.api_key, data_stream,
                batch_size=_BULK_BATCH_DOCS, label="ecs-log",
            ) as idx:
                ts = start
                while ts < now:
                    spike_active = is_in_spike_window(ts, spike_windows)
                    docs_this_step = SPIKE_VOLUME_MULTIPLIER if spike_active else 1
                    for _ in range(docs_this_step):
                        doc = generate_record(
                            profile, ns, rng,
                            ts=ts,
                            spike_active=spike_active,
                            spike_country=spike_country,
                        )
                        idx.add(doc)
                    ts += interval
                sent = idx.flush()

            step.status = "ok"
            step.detail = f"Backfilled ~{sent} docs into {data_stream} ({_BACKFILL_HOURS}h)"
        except Exception as exc:
            step.status = "failed"
            step.detail = str(exc)
            logger.warning("ECS log backfill failed (non-fatal): %s", exc)
        notify(self.progress)

    def _get_ecs_log_min_ts(self, client: httpx.Client) -> str | None:
        """Earliest @timestamp in this scenario's `logs.ecs.{ns}` partition
        (wired-stream forking routes all docs to the child, leaving the parent
        empty), used as the ML datafeed start time so the model trains on
        the backfilled history."""
        try:
            ns = self.scenario.namespace
            resp = client.post(
                f"{self.elastic_url}/logs.ecs.{ns}/_search",
                headers=_es_headers(self.api_key),
                json={
                    "size": 0,
                    "aggs": {"min_ts": {"min": {"field": "@timestamp"}}},
                },
            )
            if resp.status_code < 300:
                val = (
                    resp.json()
                    .get("aggregations", {})
                    .get("min_ts", {})
                    .get("value_as_string")
                )
                if val:
                    return val
        except Exception:
            pass
        return None

    def _deploy_metrics_ml_backfill(self):
        """Bulk-index ~12h of healthy host + k8s pod util so metrics ML has a baseline.

        Intentionally quiet (no spikes) — live chaos sliders create the anomaly later.
        Non-fatal on failure.
        """
        ns = self.scenario.namespace
        rng = random.Random(0x4D37)
        host_docs = 0
        pod_docs = 0
        try:
            from generators.host_metrics.generator import HOSTS, build_use_metrics_doc
            from generators.kubernetes_otel.generator import (
                CLUSTERS,
                _init_pod_data,
                build_pod_util_doc,
                _build_pod_resource,
            )
            from app.telemetry import _format_attributes, SCHEMA_URL

            now_ms = int(time.time() * 1000)
            start_ms = now_ms - _BACKFILL_HOURS * 3600 * 1000
            hosts = list(HOSTS)[:8] or [
                {"host.name": f"{ns}-host-1", "cloud.provider": "aws"},
            ]
            cluster = CLUSTERS[0] if CLUSTERS else {
                "name": f"{ns}-cluster",
                "provider": "aws",
                "platform": "aws_eks",
                "region": "us-east-1",
                "services": list(getattr(self.scenario, "services", {}).keys())[:6] or ["api"],
            }
            services = list(cluster.get("services") or [])[:8]
            if not services:
                services = list(getattr(self.scenario, "services", {}).keys())[:8] or ["api"]
                cluster = {**cluster, "services": services}
            pod_data = _init_pod_data(cluster, namespace=ns)

            with ConcurrentBulkIndexer(
                self.elastic_url, self.api_key,
                "metrics-hostmetricsreceiver.otel-default",
                batch_size=_BULK_BATCH_DOCS, label="host-ml-bf",
            ) as host_idx:
                with ConcurrentBulkIndexer(
                    self.elastic_url, self.api_key,
                    "metrics-kubeletstatsreceiver.otel-default",
                    batch_size=_BULK_BATCH_DOCS, label="k8s-ml-bf",
                ) as pod_idx:
                    ts = start_ms
                    while ts < now_ms:
                        cpu = _METRICS_HEALTHY_CPU + rng.uniform(-0.05, 0.05)
                        mem = _METRICS_HEALTHY_MEM + rng.uniform(-0.05, 0.05)
                        for host_cfg in hosts:
                            resource = {
                                "attributes": _format_attributes({
                                    "host.name": host_cfg.get("host.name", "host"),
                                    "cloud.provider": host_cfg.get("cloud.provider", "aws"),
                                    "data_stream.type": "metrics",
                                    "data_stream.dataset": "hostmetricsreceiver",
                                    "data_stream.namespace": "default",
                                }),
                                "schemaUrl": SCHEMA_URL,
                            }
                            host_idx.add(
                                build_use_metrics_doc(
                                    resource,
                                    {
                                        "system.cpu.utilization": round(cpu, 4),
                                        "system.memory.utilization": round(mem, 4),
                                    },
                                    timestamp_ms=ts,
                                    attributes={"cpu": "cpu0", "state": "user"},
                                )
                            )
                            host_docs += 1
                        for svc in services:
                            if svc not in pod_data.get("pods", {}):
                                continue
                            pod_res = _build_pod_resource(svc, pod_data, cluster, ns)
                            mem_limit = 512 * 2**20
                            mem_util = mem
                            working_set = int(mem_limit * mem_util * 0.9)
                            pod_idx.add(
                                build_pod_util_doc(
                                    pod_res,
                                    cpu_node_util=round(cpu, 4),
                                    mem_working_set=working_set,
                                    cpu_limit_util=round(cpu, 4),
                                    mem_limit_util=round(mem_util, 4),
                                    timestamp_ms=ts,
                                )
                            )
                            pod_docs += 1
                        ts += _METRICS_BACKFILL_INTERVAL_SEC * 1000
                    host_sent = host_idx.flush()
                    pod_sent = pod_idx.flush()

            self._metrics_ml_backfill_min_ts = time.strftime(
                "%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(start_ms / 1000.0)
            )
            logger.info(
                "Metrics ML backfill: host=%s pod=%s min_ts=%s",
                host_sent, pod_sent, self._metrics_ml_backfill_min_ts,
            )
            return host_sent + pod_sent
        except Exception as exc:
            logger.warning("Metrics ML backfill failed (non-fatal): %s", exc)
            self._metrics_ml_backfill_min_ts = None
            return 0

    def _get_metrics_ml_min_ts(self, client: httpx.Client) -> str | None:
        """Earliest backfilled hostmetrics timestamp for metrics ML datafeed start."""
        cached = getattr(self, "_metrics_ml_backfill_min_ts", None)
        if cached:
            return cached
        try:
            resp = client.post(
                f"{self.elastic_url}/metrics-hostmetricsreceiver.otel-*/_search",
                headers=_es_headers(self.api_key),
                json={
                    "size": 0,
                    "aggs": {"min_ts": {"min": {"field": "@timestamp"}}},
                },
            )
            if resp.status_code < 300:
                val = (
                    resp.json()
                    .get("aggregations", {})
                    .get("min_ts", {})
                    .get("value_as_string")
                )
                if val:
                    return val
        except Exception:
            pass
        return None
