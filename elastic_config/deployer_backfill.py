"""BackfillMixin — synthesize historical data so ML jobs have training material
and log rate analysis has visible change-points right after deploy.

Currently covers the raw ECS access-log stream. Host-metrics backfill is a
follow-up: it requires aligning the backfill doc shape with the live OTLP-routed
metrics shape, which is out of scope for this iteration.
"""

from __future__ import annotations

import json
import logging
import random
import time
from typing import Any

import httpx

from elastic_config.deployer_base import _es_headers, ConcurrentBulkIndexer, ProgressCallback

logger = logging.getLogger("deployer")


# Tuning constants — keep total bulk size modest so an ESS small cluster
# completes the backfill in under a minute.
_BACKFILL_HOURS = 12
_BACKFILL_AVG_LINES_PER_SEC = 5      # ~216k docs over 12h
_BULK_BATCH_DOCS = 500


class BackfillMixin:

    def _ecs_log_backfill_step_index(self) -> int:
        """Sub-classes can override if step layout shifts. Default: step 14."""
        return 14

    def _deploy_ecs_log_backfill(self, client: httpx.Client, notify: ProgressCallback):
        """Bulk-index ~12h of synthetic raw ECS access logs to
        `logs-ecs.{ns}-default` so log-rate analysis and categorization have
        immediate signal."""
        # Import lazily so the deployer doesn't drag the generator's app.config
        # dependency into other deploy paths.
        from log_generators.raw_access_log_generator import (
            data_stream_for,
            generate_record,
        )

        step = self._step(self._ecs_log_backfill_step_index())
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
            from log_generators.raw_access_log_generator import (
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
