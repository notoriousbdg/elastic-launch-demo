"""SloMixin — SLO deploy and cleanup methods."""

from __future__ import annotations

import logging
import math
import threading
import time

import httpx

from elastic_config.deployer_base import ProgressCallback, _es_headers, _kibana_headers

# Tuning constants for the post-deploy SLO refresh worker.
_SLO_REFRESH_INTERVAL = 15.0    # seconds between service-presence polls
_SLO_REFRESH_SETTLE_ROUNDS = 2  # proceed once count is stable for N rounds (and non-zero)
_SLO_REFRESH_THRESHOLD = 0.9    # proceed once this fraction of expected services appear
_SLO_REFRESH_MAX_WAIT = 600     # hard timeout (seconds) before triggering anyway

logger = logging.getLogger("deployer")


class SloMixin:

    def _deploy_slos(self, client: httpx.Client, notify: ProgressCallback):
        """Create the three standard SLOs via the Kibana SLO API."""
        step = self._step(18)
        step.status = "running"
        notify(self.progress)

        _slo_headers = {
            "Content-Type": "application/json",
            "kbn-xsrf": "true",
            "Authorization": f"ApiKey {self.api_key}",
        }
        _slo_index = "traces-apm*,traces-*.otel-*"

        scenario_name = self.scenario.scenario_name
        ns = self.ns
        slo_definitions = [
            {
                "name": "Availability",
                "description": "Service availability - 95% target, grouped by service name",
                "indicator": {
                    "type": "sli.kql.custom",
                    "params": {
                        "index": _slo_index,
                        "good": "NOT event.outcome:failure",
                        "filter": "processor.event:transaction",
                        "total": "*",
                        "timestampField": "@timestamp",
                    },
                },
                "groupBy": ["service.name"],
                "budgetingMethod": "occurrences",
                "timeWindow": {"duration": "7d", "type": "rolling"},
                "objective": {"target": 0.95},
                "tags": ["auto-created", ns, "availability"],
            },
            {
                "name": "Latency",
                "description": "Service latency - 85% of requests under 2s, grouped by service name",
                "indicator": {
                    "type": "sli.kql.custom",
                    "params": {
                        "index": _slo_index,
                        "filter": "processor.event:transaction AND transaction.duration.us:*",
                        "good": "transaction.duration.us <= 2000000",
                        "total": "transaction.duration.us:*",
                        "timestampField": "@timestamp",
                    },
                },
                "groupBy": ["service.name"],
                "budgetingMethod": "occurrences",
                "timeWindow": {"duration": "7d", "type": "rolling"},
                "objective": {"target": 0.85},
                "tags": ["auto-created", ns, "latency"],
            },
            {
                "name": "Error Rate",
                "description": "Service error rate - less than 5% errors, grouped by service name",
                "indicator": {
                    "type": "sli.kql.custom",
                    "params": {
                        "index": _slo_index,
                        "filter": "processor.event:transaction",
                        "good": "NOT event.outcome:failure",
                        "total": "*",
                        "timestampField": "@timestamp",
                    },
                },
                "groupBy": ["service.name"],
                "budgetingMethod": "occurrences",
                "timeWindow": {"duration": "7d", "type": "rolling"},
                "objective": {"target": 0.95},
                "tags": ["auto-created", ns, "error-rate"],
            },
        ]

        # Delete any pre-existing SLOs with these names to avoid duplicates
        self._cleanup_slos(client)

        created = 0
        for slo in slo_definitions:
            resp = client.post(
                f"{self.kibana_url}/api/observability/slos",
                headers=_slo_headers,
                json=slo,
            )
            if resp.status_code < 300:
                created += 1
                step.items_done = created
                step.detail = f"Created: {slo['name']}"
            else:
                logger.warning("SLO create failed %s: %s", slo["name"], resp.text)
            notify(self.progress)

        step.status = "ok" if created > 0 else "failed"
        step.detail = f"Created {created}/3 SLOs"
        notify(self.progress)

    def _cleanup_slos(self, client: httpx.Client) -> int:
        """Delete SLOs belonging to this scenario (matched by namespace tag)."""
        _headers = {
            "Content-Type": "application/json",
            "kbn-xsrf": "true",
            "Authorization": f"ApiKey {self.api_key}",
        }
        deleted = 0
        try:
            resp = client.get(
                f"{self.kibana_url}/api/observability/slos?perPage=500",
                headers=_headers,
            )
            if resp.status_code >= 300:
                return 0
            for slo in resp.json().get("results", []):
                if self.ns in slo.get("tags", []):
                    slo_id = slo.get("id", "")
                    if slo_id:
                        client.delete(
                            f"{self.kibana_url}/api/observability/slos/{slo_id}",
                            headers=_headers,
                        )
                        deleted += 1
        except Exception:
            pass
        return deleted

    def _schedule_slo_refresh(self) -> None:
        """Spawn a daemon thread that waits for services to appear in traces, then
        re-runs the slo_management workflow so the final SLO list is complete.
        Returns immediately; never raises."""
        def _worker():
            try:
                self._slo_refresh_worker()
            except Exception as exc:
                logger.warning("SLO refresh worker failed: %s", exc)

        t = threading.Thread(target=_worker, daemon=True, name="slo-refresh")
        t.start()
        logger.info("SLO refresh: background worker started")

    def _slo_refresh_worker(self) -> None:
        """Poll ES until all expected services appear in traces, then re-trigger the
        slo_management workflow. Uses its own httpx.Client (the deploy client is
        already closed by the time this runs)."""
        expected = set(self.scenario.services.keys())
        if not expected:
            logger.info("SLO refresh: no expected services, skipping")
            return

        threshold_count = math.ceil(_SLO_REFRESH_THRESHOLD * len(expected))
        deadline = time.time() + _SLO_REFRESH_MAX_WAIT
        prev_count = -1
        stable_rounds = 0

        search_body = {
            "size": 0,
            "query": {"term": {"processor.event": "transaction"}},
            "aggs": {"services": {"terms": {"field": "service.name", "size": 1000}}},
        }

        with httpx.Client(timeout=60.0) as client:
            timed_out = True
            while time.time() < deadline:
                try:
                    resp = client.post(
                        f"{self.elastic_url}/traces-apm*,traces-*.otel-*/_search",
                        headers=_es_headers(self.api_key),
                        json=search_body,
                    )
                    if resp.status_code < 300:
                        buckets = (
                            resp.json()
                            .get("aggregations", {})
                            .get("services", {})
                            .get("buckets", [])
                        )
                        discovered = {b["key"] for b in buckets}
                        count = len(discovered)
                        missing = expected - discovered
                        logger.info(
                            "SLO refresh: %d/%d expected services visible (missing: %s)",
                            count, len(expected),
                            sorted(missing) if missing else "none",
                        )

                        if count >= threshold_count:
                            logger.info(
                                "SLO refresh: threshold reached (%d/%d), triggering workflow",
                                count, len(expected),
                            )
                            timed_out = False
                            break

                        # Settle check — non-zero guard prevents premature fire
                        if count > 0 and count == prev_count:
                            stable_rounds += 1
                            if stable_rounds >= _SLO_REFRESH_SETTLE_ROUNDS:
                                logger.info(
                                    "SLO refresh: count stable at %d for %d rounds, triggering workflow",
                                    count, stable_rounds,
                                )
                                timed_out = False
                                break
                        else:
                            stable_rounds = 0
                        prev_count = count
                    else:
                        logger.warning(
                            "SLO refresh: ES search returned HTTP %d", resp.status_code
                        )
                except Exception as exc:
                    logger.warning("SLO refresh: poll error: %s", exc)

                time.sleep(_SLO_REFRESH_INTERVAL)

            if timed_out:
                logger.warning(
                    "SLO refresh: timed out after %ds, triggering workflow anyway",
                    _SLO_REFRESH_MAX_WAIT,
                )

            self._trigger_slo_workflow(client)

    def _trigger_slo_workflow(self, client: httpx.Client) -> None:
        """POST to the slo_management workflow run endpoint (non-fatal)."""
        wf_id = self._workflow_ids.get("slo_management")

        if not wf_id:
            try:
                search_resp = client.get(
                    f"{self.kibana_url}/api/workflows",
                    headers=_kibana_headers(self.api_key),
                )
                if search_resp.status_code in (404, 405):
                    search_resp = client.post(
                        f"{self.kibana_url}/api/workflows/search",
                        headers=_kibana_headers(self.api_key),
                        json={"page": 1, "size": 100},
                    )
                if search_resp.status_code < 300:
                    data = search_resp.json()
                    workflows = (
                        data if isinstance(data, list)
                        else data.get("results", data.get("items", []))
                    )
                    for wf in workflows:
                        if "SLO Management" in wf.get("name", ""):
                            wf_id = wf["id"]
                            break
            except Exception as exc:
                logger.warning("SLO refresh: workflow search failed: %s", exc)

        if not wf_id:
            logger.warning("SLO refresh: slo_management workflow not found, cannot trigger")
            return

        try:
            run_resp = client.post(
                f"{self.kibana_url}/api/workflows/workflow/{wf_id}/run",
                headers=_kibana_headers(self.api_key),
                json={"inputs": {}},
            )
            if run_resp.status_code in (404, 405):
                run_resp = client.post(
                    f"{self.kibana_url}/api/workflows/{wf_id}/run",
                    headers=_kibana_headers(self.api_key),
                    json={"inputs": {}},
                )
            if run_resp.status_code < 300:
                logger.info("SLO refresh: workflow run triggered (id=%s)", wf_id)
            else:
                logger.warning(
                    "SLO refresh: workflow run failed HTTP %d: %s",
                    run_resp.status_code, run_resp.text[:200],
                )
        except Exception as exc:
            logger.warning("SLO refresh: workflow trigger error: %s", exc)
