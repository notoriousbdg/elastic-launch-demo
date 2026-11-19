"""AiopsMixin — out-of-the-box Logs UI ML jobs for the o11y AIOps slide.

Creates the two standard Logs UI anomaly-detection jobs per scenario, modelled
on Kibana's `logs_ui_analysis` and `logs_ui_categories` modules. The configs
mirror what Kibana's "ML Setup" wizard produces (verified against a real
managed deploy):

  | Slide feature                          | Job ID                                |
  |----------------------------------------|---------------------------------------|
  | Log rate analysis                      | `{ns}-log_entry_rate`                 |
  | Log categorization & anomaly detection | `{ns}-log_entry_categories_count`     |
  | Host CPU/mem metrics anomaly           | `{ns}-host-cpu` / `{ns}-host-memory`  |
  | K8s pod CPU/mem metrics anomaly        | `k8s-{ns}-pod-cpu` / `k8s-{ns}-pod-memory` |
  | Trace analysis                         | exists: `apm-{ns}-transaction-metrics`|

Datafeeds target the per-scenario wired-stream partition `logs.ecs.{ns}` (and
sub-partitions). Wired-stream routing sends docs to the most-specific matching
child, so the parent `logs.ecs` is empty — we have to query the partition
directly.
"""

from __future__ import annotations

import logging

import httpx

from elastic_config.deployer_base import _es_headers, ProgressCallback, StepIdx

logger = logging.getLogger("deployer")


# Stop words used by Kibana's logs_ui_categories module — improves categorization
# quality by ignoring weekday/month names and timezone abbreviations that would
# otherwise create noisy categories.
_ML_STOP_WORDS = [
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun",
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    "GMT", "UTC",
]

# Mirrors the categorization_analyzer Kibana's logs_ui_categories module installs.
_CATEGORIZATION_ANALYZER = {
    "char_filter": ["first_line_with_letters"],
    "tokenizer": "ml_standard",
    "filter": [
        {"type": "stop", "stopwords": _ML_STOP_WORDS},
        {"type": "limit", "max_token_count": "100"},
    ],
}

# Prefer ignore_unavailable so transient backing-index deletes (sticky-mapping
# heals, package stream resets) do not error the datafeed. Managed Logs UI
# jobs use False; our generators intentionally recreate streams.
_INDICES_OPTIONS = {
    "ignore_unavailable": True,
    "expand_wildcards": ["open"],
    "allow_no_indices": True,
    "ignore_throttled": True,
}


class AiopsMixin:

    def _deploy_logs_ml_jobs(self, client: httpx.Client, notify: ProgressCallback):
        """Create Logs UI ML jobs + host/pod metrics ML jobs; start datafeeds
        from backfilled timestamps."""
        step = self._step(StepIdx.LOGS_ML_JOBS)
        step.status = "running"
        notify(self.progress)

        try:
            ns = self.scenario.namespace
            # Healthy host/k8s util history so metrics ML has a baseline before
            # chaos sliders create mid/high anomalies.
            if hasattr(self, "_deploy_metrics_ml_backfill"):
                self._deploy_metrics_ml_backfill()

            start_ts = (
                self._get_ecs_log_min_ts(client)
                if hasattr(self, "_get_ecs_log_min_ts")
                else None
            )
            metrics_start_ts = (
                self._get_metrics_ml_min_ts(client)
                if hasattr(self, "_get_metrics_ml_min_ts")
                else None
            )

            # Target this scenario's wired-stream partition directly. The
            # backfill + live generator write into `logs.ecs.{ns}` via the
            # parent fork's where-filter, so the parent `logs.ecs` itself is
            # empty.
            partition_indices = [f"logs.ecs.{ns}", f"logs.ecs.{ns}.*"]

            created: list[str] = []

            # ── Job 1: log_entry_rate (log volume anomaly) ─────────────────
            rate_job_id = f"{ns}-log_entry_rate"
            rate_job_cfg = {
                "job_id": rate_job_id,
                "groups": ["logs-ui", "kibana", "o11y-demo"],
                "description": (
                    f"Detects anomalies in the log entry ingestion rate for {ns}. "
                    "Modelled on Kibana's logs_ui_analysis module."
                ),
                "analysis_config": {
                    "bucket_span": "15m",
                    "detectors": [
                        {
                            "detector_description": "count",
                            "function": "count",
                            "partition_field_name": "service.name",
                            "use_null": True,
                        },
                    ],
                    "influencers": ["service.name"],
                    "model_prune_window": "30d",
                },
                "analysis_limits": {"model_memory_limit": "11mb"},
                "data_description": {"time_field": "@timestamp", "time_format": "epoch_ms"},
                "model_plot_config": {"enabled": True, "annotations_enabled": True},
                "model_snapshot_retention_days": 10,
                "daily_model_snapshot_retention_after_days": 1,
                "results_retention_days": 120,
                "results_index_name": f"custom-{ns}-log-entry-rate",
                "allow_lazy_open": True,
            }
            rate_datafeed_cfg = {
                "job_id": rate_job_id,
                "indices": partition_indices,
                "query": {"match_all": {}},
                "indices_options": _INDICES_OPTIONS,
                "chunking_config": {"mode": "auto"},
                "scroll_size": 1000,
                "delayed_data_check_config": {"enabled": True},
            }
            self._recreate_ml_job(client, rate_job_id, rate_job_cfg, rate_datafeed_cfg, start_ts)
            created.append(rate_job_id)

            # ── Job 2: log_entry_categories_count (categorization) ─────────
            cat_job_id = f"{ns}-log_entry_categories_count"
            cat_job_cfg = {
                "job_id": cat_job_id,
                "groups": ["logs-ui", "kibana", "o11y-demo"],
                "description": (
                    f"Detects anomalies in count of log entries by category for {ns}. "
                    "Modelled on Kibana's logs_ui_categories module."
                ),
                "analysis_config": {
                    "bucket_span": "15m",
                    "categorization_field_name": "message",
                    "categorization_analyzer": _CATEGORIZATION_ANALYZER,
                    # stop_on_warn=False so the synthetic CLF data (which is
                    # more uniform than real log streams) doesn't trip the
                    # category-quality threshold and kill half its categories.
                    "per_partition_categorization": {
                        "enabled": True,
                        "stop_on_warn": False,
                    },
                    "detectors": [
                        {
                            "detector_description": "count by learned log entry category",
                            "function": "count",
                            "by_field_name": "mlcategory",
                            "partition_field_name": "service.name",
                            "use_null": True,
                        },
                    ],
                    "influencers": ["service.name", "mlcategory"],
                    "model_prune_window": "30d",
                },
                "analysis_limits": {
                    "model_memory_limit": "128mb",
                    "categorization_examples_limit": 1,
                },
                "data_description": {"time_field": "@timestamp", "time_format": "epoch_ms"},
                "model_plot_config": {"enabled": True, "annotations_enabled": True},
                "model_snapshot_retention_days": 10,
                "daily_model_snapshot_retention_after_days": 1,
                "results_retention_days": 120,
                "results_index_name": f"custom-{ns}-log-entry-categories-count",
                "allow_lazy_open": True,
            }
            cat_datafeed_cfg = {
                "job_id": cat_job_id,
                "indices": partition_indices,
                "query": {"bool": {"filter": [{"exists": {"field": "message"}}]}},
                "indices_options": _INDICES_OPTIONS,
                "chunking_config": {"mode": "auto"},
                "scroll_size": 1000,
                "delayed_data_check_config": {"enabled": True},
            }
            self._recreate_ml_job(client, cat_job_id, cat_job_cfg, cat_datafeed_cfg, start_ts)
            created.append(cat_job_id)

            # ── Host + pod metrics ML (chaos UI host/pod CPU & memory spikes) ─
            created.extend(
                self._deploy_host_pod_metrics_ml_jobs(client, ns, metrics_start_ts)
            )

            step.status = "ok"
            step.detail = (
                f"Started {len(created)} ML jobs: {', '.join(created)}"
                + (f" (logs from {start_ts})" if start_ts else "")
                + (f" (metrics from {metrics_start_ts})" if metrics_start_ts else "")
            )
        except Exception as exc:
            step.status = "failed"
            step.detail = str(exc)
            logger.warning("Logs/metrics ML setup failed (non-fatal): %s", exc)
        notify(self.progress)

    def _deploy_host_pod_metrics_ml_jobs(
        self,
        client: httpx.Client,
        ns: str,
        start_ts: str | None,
    ) -> list[str]:
        """Create hostmetrics + k8s pod CPU/memory anomaly jobs for chaos sliders."""
        created: list[str] = []

        def _metric_job(
            job_id: str,
            *,
            groups: list[str],
            description: str,
            field_name: str,
            partition_field: str,
            influencers: list[str],
            indices: list[str],
            extra_filters: list[dict] | None = None,
        ) -> None:
            filters = [{"exists": {"field": field_name}}]
            if extra_filters:
                filters.extend(extra_filters)
            job_cfg = {
                "job_id": job_id,
                "groups": groups,
                "description": description,
                "analysis_config": {
                    "bucket_span": "1m",
                    "detectors": [
                        {
                            "detector_description": (
                                f"high_mean({field_name}) partitionfield=\"{partition_field}\""
                            ),
                            "function": "high_mean",
                            "field_name": field_name,
                            "partition_field_name": partition_field,
                            "detector_index": 0,
                        }
                    ],
                    "influencers": influencers,
                },
                "analysis_limits": {"model_memory_limit": "12mb"},
                "data_description": {"time_field": "@timestamp", "time_format": "epoch_ms"},
                "model_plot_config": {"enabled": False, "annotations_enabled": False},
                "model_snapshot_retention_days": 10,
                "daily_model_snapshot_retention_after_days": 1,
                "results_index_name": "shared",
                "allow_lazy_open": True,
            }
            datafeed_cfg = {
                "job_id": job_id,
                "indices": indices,
                "query": {"bool": {"filter": filters}},
                "indices_options": _INDICES_OPTIONS,
                "chunking_config": {"mode": "auto"},
                "query_delay": "3m",
                "delayed_data_check_config": {"enabled": True},
                "scroll_size": 1000,
            }
            self._recreate_ml_job(client, job_id, job_cfg, datafeed_cfg, start_ts)
            created.append(job_id)

        # Prefer mapped OTel `metrics.*` paths; also accept bare names via exists
        # on the primary field the live bulk/OTLP path writes.
        _metric_job(
            f"{ns}-host-cpu",
            groups=["metrics", "hosts", "o11y-demo"],
            description=f"Detects anomalies in host CPU utilization for {ns} (chaos Host CPU spike).",
            field_name="metrics.system.cpu.utilization",
            partition_field="resource.attributes.host.name",
            influencers=["resource.attributes.host.name"],
            indices=["metrics-hostmetricsreceiver.otel-*"],
        )
        _metric_job(
            f"{ns}-host-memory",
            groups=["metrics", "hosts", "o11y-demo"],
            description=f"Detects anomalies in host memory utilization for {ns} (chaos Host Memory spike).",
            field_name="metrics.system.memory.utilization",
            partition_field="resource.attributes.host.name",
            influencers=["resource.attributes.host.name"],
            indices=["metrics-hostmetricsreceiver.otel-*"],
        )
        ns_filter = {
            "term": {"resource.attributes.k8s.namespace.name": ns},
        }
        _metric_job(
            f"k8s-{ns}-pod-cpu",
            groups=["metrics", "k8s", "o11y-demo"],
            description=(
                f"Detects anomalies in pod CPU utilization for {ns} "
                "(chaos Pod CPU spike / Anomaly Explorer)."
            ),
            field_name="metrics.k8s.pod.cpu.node.utilization",
            partition_field="resource.attributes.k8s.pod.name",
            influencers=[
                "resource.attributes.k8s.pod.name",
                "resource.attributes.k8s.container.name",
            ],
            indices=["metrics-kubeletstatsreceiver.otel-*"],
            extra_filters=[ns_filter],
        )
        _metric_job(
            f"k8s-{ns}-pod-memory",
            groups=["metrics", "k8s", "o11y-demo"],
            description=(
                f"Detects anomalies in pod memory working set for {ns} "
                "(chaos Pod Memory spike / Anomaly Explorer)."
            ),
            field_name="metrics.k8s.pod.memory.working_set",
            partition_field="resource.attributes.k8s.pod.name",
            influencers=[
                "resource.attributes.k8s.pod.name",
                "resource.attributes.k8s.container.name",
            ],
            indices=["metrics-kubeletstatsreceiver.otel-*"],
            extra_filters=[ns_filter],
        )
        return created

    def _recreate_ml_job(
        self,
        client: httpx.Client,
        job_id: str,
        job_cfg: dict,
        datafeed_cfg: dict,
        start_ts: str | None,
    ) -> None:
        """Idempotent ML job lifecycle: stop+delete existing -> PUT -> open -> start."""
        # Cleanup existing
        client.post(
            f"{self.elastic_url}/_ml/datafeeds/datafeed-{job_id}/_stop",
            headers=_es_headers(self.api_key),
        )
        client.delete(
            f"{self.elastic_url}/_ml/datafeeds/datafeed-{job_id}",
            headers=_es_headers(self.api_key),
        )
        client.post(
            f"{self.elastic_url}/_ml/anomaly_detectors/{job_id}/_close",
            headers=_es_headers(self.api_key),
            json={"force": True},
        )
        client.delete(
            f"{self.elastic_url}/_ml/anomaly_detectors/{job_id}",
            headers=_es_headers(self.api_key),
        )

        # Create
        resp = client.put(
            f"{self.elastic_url}/_ml/anomaly_detectors/{job_id}",
            headers=_es_headers(self.api_key),
            json=job_cfg,
        )
        if resp.status_code >= 300:
            raise RuntimeError(f"ML job {job_id} create failed: {resp.text[:500]}")

        resp = client.put(
            f"{self.elastic_url}/_ml/datafeeds/datafeed-{job_id}",
            headers=_es_headers(self.api_key),
            json=datafeed_cfg,
        )
        if resp.status_code >= 300:
            raise RuntimeError(f"ML datafeed for {job_id} create failed: {resp.text[:500]}")

        # Open + start
        resp = client.post(
            f"{self.elastic_url}/_ml/anomaly_detectors/{job_id}/_open",
            headers=_es_headers(self.api_key),
        )
        if resp.status_code >= 300:
            raise RuntimeError(f"ML job {job_id} open failed: {resp.text[:500]}")

        start_body: dict = {}
        if start_ts:
            start_body["start"] = start_ts
        resp = client.post(
            f"{self.elastic_url}/_ml/datafeeds/datafeed-{job_id}/_start",
            headers=_es_headers(self.api_key),
            json=start_body,
        )
        if resp.status_code >= 300:
            raise RuntimeError(f"ML datafeed for {job_id} start failed: {resp.text[:500]}")

    def _cleanup_aiops_ml(self, client: httpx.Client) -> None:
        """Stop and delete this scenario's AIOps ML jobs."""
        ns = self.ns
        job_ids = [
            f"{ns}-log_entry_rate",
            f"{ns}-log_entry_categories_count",
            f"{ns}-host-cpu",
            f"{ns}-host-memory",
            f"k8s-{ns}-pod-cpu",
            f"k8s-{ns}-pod-memory",
        ]
        for job_id in job_ids:
            try:
                client.post(
                    f"{self.elastic_url}/_ml/datafeeds/datafeed-{job_id}/_stop",
                    headers=_es_headers(self.api_key),
                )
                client.delete(
                    f"{self.elastic_url}/_ml/datafeeds/datafeed-{job_id}",
                    headers=_es_headers(self.api_key),
                )
                client.post(
                    f"{self.elastic_url}/_ml/anomaly_detectors/{job_id}/_close",
                    headers=_es_headers(self.api_key),
                    json={"force": True},
                )
                client.delete(
                    f"{self.elastic_url}/_ml/anomaly_detectors/{job_id}",
                    headers=_es_headers(self.api_key),
                )
            except Exception:
                pass
