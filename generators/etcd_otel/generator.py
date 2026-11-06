#!/usr/bin/env python3
"""etcd Generator — sends etcd Prometheus metrics via OTLP.

Emits data matching the etcd_otel content package
(discovery.datasets: etcd.otel).

Metric names match the etcd_otel dashboard ES|QL queries (snake_case
Prometheus style from the etcd ``/metrics`` endpoint).

``etcd_server_proposals_committed_total`` and
``etcd_server_proposals_applied_total`` are Prometheus *gauges* in etcd
(despite the ``_total`` suffix). The Raft Consensus dashboard calls
``DELTA()`` on them; monotonic cumulative sums map to ``counter_long`` and
reject ``DELTA()``, so we emit them as gauges.

Quota utilization panels AND-filter ``etcd_mvcc_db_total_size_in_bytes`` with
``etcd_server_quota_backend_bytes`` on the same document. OTLP stores one
metric per document, so those filters never match OTLP-only data — each tick
also bulk-indexes a co-located doc via ``ESBulkClient``.

The key alerting rule shipped by the package is etcd_otel-no-leader.json,
which triggers when etcd_server_has_leader == 0. The impact key
``has_leader`` drives that alert.

Usage (standalone):
    python3 -m generators.etcd_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/etcd_otel@0.3.0"

import logging
import os
import random
import signal
import threading
import time
from typing import Any

from app.telemetry import ESBulkClient, OTLPClient
from app.config import NAMESPACE
try:
    from generators.etcd_otel import SPEC
except Exception:  # pragma: no cover
    SPEC = None

from generator_engine.db_common import (
    CounterState,
    MultiEntityState,
    build_cumulative_sum,
    build_gauge,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

ETCD_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("etcd-generator")
EMIT_INTERVAL_S = 15.0

_METRICS_DATA_STREAM = "metrics-etcd.otel-default"

# Raft Consensus panels call DELTA() on these — must map as gauges, not counters.
_PROPOSAL_GAUGE_FIELDS = (
    "etcd_server_proposals_committed_total",
    "etcd_server_proposals_applied_total",
)

# Default etcd backend quota (8 GiB) — OpenShift / Kubernetes default.
DEFAULT_QUOTA_BYTES = 8 * 1024 * 1024 * 1024

# Metric names grounded from etcd_otel dashboard ES|QL queries.
METRIC_NAMES = (
    # Server / Raft
    "etcd_server_has_leader",
    "etcd_server_is_leader",
    "etcd_server_leader_changes_seen_total",
    "etcd_server_proposals_committed_total",
    "etcd_server_proposals_applied_total",
    "etcd_server_proposals_pending",
    "etcd_server_proposals_failed_total",
    "etcd_server_heartbeat_send_failures_total",
    "etcd_server_slow_apply_total",
    "etcd_server_read_indexes_failed_total",
    "etcd_server_client_requests_total",
    "etcd_server_quota_backend_bytes",
    # Network / gRPC
    "etcd_network_client_grpc_received_bytes_total",
    "etcd_network_client_grpc_sent_bytes_total",
    "grpc_server_handled_total",
    # MVCC / storage
    "etcd_mvcc_db_total_size_in_bytes",
    "etcd_mvcc_db_total_size_in_use_in_bytes",
    "etcd_mvcc_put_total",
    "etcd_mvcc_range_total",
    "etcd_mvcc_delete_total",
    "etcd_mvcc_txn_total",
    "etcd_debugging_mvcc_keys_total",
    "etcd_debugging_mvcc_current_revision",
    "etcd_debugging_mvcc_watcher_total",
    "etcd_debugging_mvcc_watch_stream_total",
    # Leases
    "etcd_debugging_lease_granted_total",
    "etcd_debugging_lease_renewed_total",
    "etcd_debugging_lease_revoked_total",
    "etcd_debugging_server_lease_expired_total",
    # Process / Go
    "go_goroutines",
    "process_open_fds",
    "process_resident_memory_bytes",
)

CLIENT_REQUEST_TYPES = (
    "read-only",
    "read-serializable",
    "writeable",
)

GRPC_METHODS = (
    ("etcdserverpb.KV", "Range"),
    ("etcdserverpb.KV", "Put"),
    ("etcdserverpb.KV", "DeleteRange"),
    ("etcdserverpb.KV", "Txn"),
    ("etcdserverpb.Watch", "Watch"),
    ("etcdserverpb.Lease", "LeaseGrant"),
    ("etcdserverpb.Lease", "LeaseKeepAlive"),
)

GRPC_ERROR_CODES = ("Unavailable", "DeadlineExceeded", "ResourceExhausted")


def _default_cloud() -> dict:
    return {"provider": "aws", "platform": "aws_ec2",
            "region": "us-east-1", "availability_zone": "us-east-1a"}


def _resource_attr_map(resource: dict) -> dict[str, Any]:
    """Flatten OTLP resource attributes; drop data_stream.* (set on the doc root)."""
    out: dict[str, Any] = {}
    for attr in resource.get("attributes", []):
        key = attr["key"]
        if key.startswith("data_stream."):
            continue
        val = attr["value"]
        if "stringValue" in val:
            out[key] = val["stringValue"]
        elif "intValue" in val:
            out[key] = int(val["intValue"])
        elif "doubleValue" in val:
            out[key] = float(val["doubleValue"])
        elif "boolValue" in val:
            out[key] = bool(val["boolValue"])
    return out


def build_quota_utilization_doc(
    resource: dict,
    *,
    db_size: int,
    quota: int,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Build one ES doc with DB size + quota co-located.

    Overview / Storage "DB Size vs Quota" panels AND-filter both metrics on
    the same document. OTLP stores one metric per document, so those filters
    never match OTLP-only data.
    """
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "By",
        "data_stream": {
            "type": "metrics",
            "dataset": "etcd.otel",
            "namespace": "default",
        },
        "attributes": {},
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": ETCD_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "etcd_mvcc_db_total_size_in_bytes": int(db_size),
            "etcd_server_quota_backend_bytes": int(quota),
        },
    }


def build_fd_utilization_doc(
    resource: dict,
    *,
    open_fds: int,
    max_fds: int,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Co-locate open/max FDs for OOTB ``fd_ratio`` ES|QL (same-doc AND)."""
    return {
        "@timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "unit": "1",
        "data_stream": {
            "type": "metrics",
            "dataset": "etcd.otel",
            "namespace": "default",
        },
        "attributes": {},
        "resource": {"attributes": _resource_attr_map(resource)},
        "scope": {"name": ETCD_SCOPE, "version": SCOPE_VERSION},
        "metrics": {
            "process_open_fds": int(open_fds),
            "process_max_fds": int(max_fds),
        },
    }


def _sum(name: str, unit: str, counter: CounterState, attrs: dict | None = None) -> dict:
    return build_cumulative_sum(name, unit, counter.value, counter.start_time_ns, attrs or {})


def _proposal_fields_mapped_as_counter(elastic_url: str, api_key: str) -> bool:
    """Return True if proposal gauge fields are stuck as counter_long."""
    import httpx

    url = (
        f"{elastic_url.rstrip('/')}/{_METRICS_DATA_STREAM}/_field_caps"
        f"?fields={','.join(_PROPOSAL_GAUGE_FIELDS)}"
    )
    try:
        resp = httpx.get(
            url,
            headers={
                "Authorization": f"ApiKey {api_key}",
                "Content-Type": "application/json",
            },
            timeout=15,
        )
    except Exception as exc:
        logger.warning("Could not inspect %s field caps: %s", _METRICS_DATA_STREAM, exc)
        return False
    if resp.status_code == 404:
        return False
    if resp.status_code >= 300:
        logger.warning(
            "field_caps for %s failed (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )
        return False

    fields = resp.json().get("fields", {})
    for name in _PROPOSAL_GAUGE_FIELDS:
        for meta in fields.get(name, {}).values():
            if meta.get("time_series_metric") == "counter" or meta.get("type") == "counter_long":
                return True
    return False


def _reset_proposal_counter_mapping() -> None:
    """Delete the metrics data stream if proposal gauges are stuck as counters.

    Fresh gauge samples then recreate the stream without ``counter_long``,
    unblocking Raft Consensus ``DELTA()`` panels.  No-op when ES credentials
    are absent or mapping is already correct.
    """
    try:
        from app.telemetry import ESBulkClient
    except Exception as exc:
        logger.warning("ESBulkClient unavailable for proposal mapping check: %s", exc)
        return

    client = ESBulkClient()
    if not client.configured:
        return

    if not _proposal_fields_mapped_as_counter(client.elastic_url, client.api_key):
        logger.info(
            "etcd proposal gauge fields are not mapped as counters; leaving %s alone",
            _METRICS_DATA_STREAM,
        )
        return

    import httpx

    try:
        resp = httpx.delete(
            f"{client.elastic_url.rstrip('/')}/_data_stream/{_METRICS_DATA_STREAM}",
            headers={
                "Authorization": f"ApiKey {client.api_key}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )
    except Exception as exc:
        logger.warning("Could not delete %s: %s", _METRICS_DATA_STREAM, exc)
        return

    if resp.status_code < 300:
        logger.info(
            "Deleted %s so proposal metrics remap as gauges (were counter_long)",
            _METRICS_DATA_STREAM,
        )
    elif resp.status_code == 404:
        logger.info("Data stream %s did not exist yet", _METRICS_DATA_STREAM)
    else:
        logger.warning(
            "Could not delete %s (%s): %s",
            _METRICS_DATA_STREAM, resp.status_code, resp.text[:200],
        )


class _MemberState(CounterState):
    """Per-member cumulative counters + gauge baselines."""

    def __init__(self, rng: random.Random):
        super().__init__(rng, initial=0)
        # OOTB "Frequent leader elections" alert fires on INCREASE > 0; start
        # flat and only grow via chaos (frequent_leader_elections / no leader).
        self.leader_changes = CounterState(rng, initial=0)
        # All high-volume counters start at 0 so TSDB sees a clean reset on
        # generator restart rather than a random-initial spike.
        self.proposals_committed = CounterState(rng, initial=0)
        self.proposals_applied = CounterState(rng, initial=0)
        self.proposals_failed = CounterState(rng, initial=0)
        self.heartbeat_failures = CounterState(rng, initial=0)
        self.slow_apply = CounterState(rng, initial=0)
        self.read_index_failures = CounterState(rng, initial=0)
        self.grpc_rx_bytes = CounterState(rng, initial=0)
        self.grpc_tx_bytes = CounterState(rng, initial=0)
        self.client_requests = {
            t: CounterState(rng, initial=0) for t in CLIENT_REQUEST_TYPES
        }
        self.grpc_handled: dict[tuple[str, str, str], CounterState] = {}
        for svc, method in GRPC_METHODS:
            self.grpc_handled[(svc, method, "OK")] = CounterState(rng, initial=0)
            for code in GRPC_ERROR_CODES:
                # OOTB "gRPC errors" alert fires on INCREASE > 0; keep flat
                # unless grpc_errors chaos is active or the leader is down.
                self.grpc_handled[(svc, method, code)] = CounterState(
                    rng, initial=0,
                )
        self.mvcc_put = CounterState(rng, initial=0)
        self.mvcc_range = CounterState(rng, initial=0)
        self.mvcc_delete = CounterState(rng, initial=0)
        self.mvcc_txn = CounterState(rng, initial=0)
        self.lease_granted = CounterState(rng, initial=0)
        self.lease_renewed = CounterState(rng, initial=0)
        self.lease_revoked = CounterState(rng, initial=0)
        self.lease_expired = CounterState(rng, initial=0)

        self.keys = rng.randint(5_000, 50_000)
        self.revision = rng.randint(100_000, 5_000_000)
        self.watchers = rng.randint(10, 200)
        self.watch_streams = rng.randint(5, 80)
        self.db_size = rng.randint(200, 2_000) * 1024 * 1024
        self.db_size_in_use = int(self.db_size * rng.uniform(0.6, 0.95))
        self.quota = DEFAULT_QUOTA_BYTES

    def tick(self, rng: random.Random, load: float = 1.0,
             has_leader: bool = True, failure_rate: float = 0.0) -> None:
        ops = max(1, int(rng.randint(40, 200) * load))
        self.proposals_committed.tick(rng, drift=ops)
        # Applied tracks committed with a small lag.
        applied_drift = max(0, ops - rng.randint(0, 3))
        self.proposals_applied.tick(rng, drift=applied_drift)
        fail_drift = int(ops * failure_rate) + (rng.randint(0, 2) if failure_rate > 0 else 0)
        self.proposals_failed.tick(rng, drift=fail_drift)
        if not has_leader:
            self.heartbeat_failures.tick(rng, drift=rng.randint(1, 5))
            self.leader_changes.tick(rng, drift=1 if rng.random() < 0.3 else 0)
            self.read_index_failures.tick(rng, drift=rng.randint(1, 10))
            self.slow_apply.tick(rng, drift=rng.randint(1, 5))
        # Healthy (has_leader): no ambient slow_apply — OOTB alert fires on INCREASE > 0.

        self.grpc_rx_bytes.tick(rng, drift=ops * rng.randint(200, 2_000))
        self.grpc_tx_bytes.tick(rng, drift=ops * rng.randint(200, 2_000))

        self.client_requests["read-only"].tick(rng, drift=int(ops * 0.45))
        self.client_requests["read-serializable"].tick(rng, drift=int(ops * 0.35))
        self.client_requests["writeable"].tick(rng, drift=int(ops * 0.20))

        # Weight Range/Put higher than lease/watch.
        weights = {
            ("etcdserverpb.KV", "Range"): 0.40,
            ("etcdserverpb.KV", "Put"): 0.20,
            ("etcdserverpb.KV", "DeleteRange"): 0.05,
            ("etcdserverpb.KV", "Txn"): 0.10,
            ("etcdserverpb.Watch", "Watch"): 0.10,
            ("etcdserverpb.Lease", "LeaseGrant"): 0.05,
            ("etcdserverpb.Lease", "LeaseKeepAlive"): 0.10,
        }
        for (svc, method), weight in weights.items():
            ok = int(ops * weight)
            self.grpc_handled[(svc, method, "OK")].tick(rng, drift=ok)
            if failure_rate > 0 or not has_leader:
                err_n = max(0, int(ok * max(failure_rate, 0.02)))
                if err_n:
                    code = rng.choice(GRPC_ERROR_CODES)
                    self.grpc_handled[(svc, method, code)].tick(rng, drift=err_n)

        self.mvcc_range.tick(rng, drift=int(ops * 0.55))
        self.mvcc_put.tick(rng, drift=int(ops * 0.25))
        self.mvcc_delete.tick(rng, drift=int(ops * 0.08))
        self.mvcc_txn.tick(rng, drift=int(ops * 0.12))

        self.lease_granted.tick(rng, drift=rng.randint(0, max(1, int(3 * load))))
        self.lease_renewed.tick(rng, drift=rng.randint(5, max(6, int(40 * load))))
        self.lease_revoked.tick(rng, drift=rng.randint(0, 2))
        self.lease_expired.tick(rng, drift=1 if failure_rate > 0.2 and rng.random() < 0.3 else 0)

        self.keys = max(100, self.keys + rng.randint(-50, int(80 * load)))
        self.revision += max(1, int(ops * 0.8))
        self.watchers = max(1, self.watchers + rng.randint(-3, 4))
        self.watch_streams = max(1, min(self.watchers, self.watch_streams + rng.randint(-2, 3)))
        # DB size drifts slowly; chaos can push toward quota.
        # Cap healthy usage well under the OOTB alert threshold (usage_ratio > 0.9).
        growth = rng.randint(-2, 8) * 1024 * 1024
        if failure_rate > 0.5:
            growth = rng.randint(20, 80) * 1024 * 1024
        self.db_size = max(50 * 1024 * 1024, min(int(self.quota * 0.85), self.db_size + growth))
        self.db_size_in_use = max(
            40 * 1024 * 1024,
            min(self.db_size, int(self.db_size * rng.uniform(0.7, 0.95))),
        )



from generators.etcd_otel import SPEC as _ALERT_SPEC
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

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("etcd")
    member_count: int = max(1, int(cfg.get("member_count", 3)))
    cloud = cfg.get("cloud") or _default_cloud()
    host_base = cfg.get("host_name") or ScenarioIdentity(ns).label("etcd")

    impact_keys = SPEC.impact_keys if SPEC is not None else {
        "has_leader": {"type": "bool", "default": True},
        "proposal_failure_rate": {"type": "float", "default": 0.0},
    }

    member_ids = [str(i) for i in range(1, member_count + 1)]
    members = MultiEntityState(member_ids, _MemberState, rng)
    scope = {"name": ETCD_SCOPE, "version": SCOPE_VERSION}

    try:
        es_bulk = ESBulkClient()
    except Exception as exc:  # pragma: no cover - missing http2/deps in some host envs
        logger.warning("ESBulkClient unavailable (%s); co-located docs disabled", exc)
        es_bulk = None

    _reset_proposal_counter_mapping()

    logger.info("etcd generator started (namespace=%s, members=%d)", ns, member_count)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="etcd_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        c = active_chaos or {}
        has_leader = float(c.get("no_leader", 0.0)) <= 0
        db_quota_ratio = float(c.get("database_size_approaching_quota", 0.0))
        fd_pressure = float(c.get("file_descriptor_exhaustion", 0.0))
        leader_changes_key = float(c.get("frequent_leader_elections", 0.0))
        proposals_pending_key = float(c.get("pending_proposals_backlog", 0.0))
        slow_applies_key = float(c.get("slow_apply_operations", 0.0))
        scrape_down = float(c.get("scrape_target_down", 0.0)) > 0
        proposal_failures_key = float(c.get("proposal_failures", 0.0))
        heartbeat_failures_key = float(c.get("heartbeat_send_failures", 0.0))
        grpc_errors_key = float(c.get("grpc_errors", 0.0))
        load = business_load()
        bulk_docs: list[dict[str, Any]] = []
        ts_ms = int(time.time() * 1000)

        # Member 1 is the Raft leader when the cluster has a leader.
        leader_id = member_ids[0] if has_leader else None

        for member_id, state in members.items():
            state.tick(rng, load=load, has_leader=has_leader, failure_rate=0.0)
            if leader_changes_key > 0:
                state.leader_changes.tick(rng, drift=int(leader_changes_key))
            if slow_applies_key > 0:
                state.slow_apply.tick(rng, drift=int(slow_applies_key))
            if proposal_failures_key > 0:
                state.proposals_failed.tick(rng, drift=int(proposal_failures_key))
            if heartbeat_failures_key > 0:
                state.heartbeat_failures.tick(rng, drift=int(heartbeat_failures_key))
            if grpc_errors_key > 0:
                for (svc, method, code), counter in state.grpc_handled.items():
                    if code != "OK":
                        counter.tick(rng, drift=max(1, int(grpc_errors_key)))
            if db_quota_ratio > 0:
                state.db_size = int(state.quota * min(1.0, db_quota_ratio))
                state.db_size_in_use = int(state.db_size * 0.9)
            else:
                # Snap back under alert threshold after remediate (tick alone
                # would leave a post-chaos tip at the old high until growth
                # drifts — and the prior 0.95 healthy cap never cleared >0.9).
                healthy_cap = int(state.quota * 0.85)
                if state.db_size > healthy_cap:
                    state.db_size = int(state.quota * rng.uniform(0.35, 0.55))
                    state.db_size_in_use = int(state.db_size * 0.85)
            is_leader = member_id == leader_id
            # Healthy clusters keep pending at zero; chaos sets an explicit backlog.
            pending = 0 if proposals_pending_key <= 0 else int(proposals_pending_key)

            max_fds = 65536
            if fd_pressure > 0:
                # GT may send percent (85.0) or fraction (0.95). OOTB alert is
                # fd_ratio > 0.9 — force a clear breach when the channel is on.
                ratio = fd_pressure / 100.0 if fd_pressure > 1.0 else fd_pressure
                if ratio <= 0.9:
                    ratio = 0.95
                open_fds = int(max_fds * min(1.0, ratio))
            else:
                open_fds = rng.randint(40, 120)

            metrics: list[dict] = [
                build_gauge("etcd_server_has_leader", "1", 1 if has_leader else 0, {}),
                build_gauge("up", "1", 0 if scrape_down else 1, {}),
                build_gauge("etcd_server_is_leader", "1", 1 if is_leader else 0, {}),
                _sum("etcd_server_leader_changes_seen_total", "{changes}", state.leader_changes),
                # Gauges (etcd docs): Raft DELTA() panels reject counter_long.
                build_gauge("etcd_server_proposals_committed_total", "{proposals}",
                            state.proposals_committed.value, {}),
                build_gauge("etcd_server_proposals_applied_total", "{proposals}",
                            state.proposals_applied.value, {}),
                build_gauge("etcd_server_proposals_pending", "{proposals}", pending, {}),
                _sum("etcd_server_proposals_failed_total", "{proposals}", state.proposals_failed),
                _sum("etcd_server_heartbeat_send_failures_total", "{failures}",
                     state.heartbeat_failures),
                _sum("etcd_server_slow_apply_total", "{applies}", state.slow_apply),
                _sum("etcd_server_read_indexes_failed_total", "{failures}",
                     state.read_index_failures),
                build_gauge("etcd_server_quota_backend_bytes", "By", state.quota, {}),
                _sum("etcd_network_client_grpc_received_bytes_total", "By", state.grpc_rx_bytes),
                _sum("etcd_network_client_grpc_sent_bytes_total", "By", state.grpc_tx_bytes),
                build_gauge("etcd_mvcc_db_total_size_in_bytes", "By", state.db_size, {}),
                build_gauge("etcd_mvcc_db_total_size_in_use_in_bytes", "By",
                            state.db_size_in_use, {}),
                _sum("etcd_mvcc_put_total", "{ops}", state.mvcc_put),
                _sum("etcd_mvcc_range_total", "{ops}", state.mvcc_range),
                _sum("etcd_mvcc_delete_total", "{ops}", state.mvcc_delete),
                _sum("etcd_mvcc_txn_total", "{ops}", state.mvcc_txn),
                build_gauge("etcd_debugging_mvcc_keys_total", "{keys}", state.keys, {}),
                build_gauge("etcd_debugging_mvcc_current_revision", "{revision}",
                            state.revision, {}),
                build_gauge("etcd_debugging_mvcc_watcher_total", "{watchers}",
                            state.watchers, {}),
                build_gauge("etcd_debugging_mvcc_watch_stream_total", "{streams}",
                            state.watch_streams, {}),
                _sum("etcd_debugging_lease_granted_total", "{leases}", state.lease_granted),
                _sum("etcd_debugging_lease_renewed_total", "{leases}", state.lease_renewed),
                _sum("etcd_debugging_lease_revoked_total", "{leases}", state.lease_revoked),
                _sum("etcd_debugging_server_lease_expired_total", "{leases}",
                     state.lease_expired),
                build_gauge("go_goroutines", "1", rng.randint(60, 200), {}),
                build_gauge("process_open_fds", "1", open_fds, {}),
                build_gauge("process_max_fds", "1", max_fds, {}),
                build_gauge("process_resident_memory_bytes", "By",
                            rng.randint(50 * 1024 * 1024, 200 * 1024 * 1024), {}),
            ]

            for req_type, counter in state.client_requests.items():
                metrics.append(_sum(
                    "etcd_server_client_requests_total", "{requests}", counter,
                    {"type": req_type},
                ))

            for (svc, method, code), counter in state.grpc_handled.items():
                # Skip empty error series to keep payload small.
                if code != "OK" and counter.value == 0:
                    continue
                metrics.append(_sum(
                    "grpc_server_handled_total", "{requests}", counter,
                    {
                        "grpc_service": svc,
                        "grpc_method": method,
                        "grpc_code": code,
                        "grpc_type": "unary",
                    },
                ))

            resource = build_signal_resource(
                ns=ns, signal_type="metrics", dataset=otel_dataset("etcd"),
                service_name=service_name, service_version="3.5.12",
                service_instance_id=f"{service_name}-{member_id}",
                host_name=f"{host_base}-{member_id}", cloud=cloud,
            )
            client.send_metrics_scoped(resource, scope, metrics)

            bulk_docs.append(build_quota_utilization_doc(
                resource,
                db_size=state.db_size,
                quota=state.quota,
                timestamp_ms=ts_ms,
            ))
            bulk_docs.append(build_fd_utilization_doc(
                resource,
                open_fds=open_fds,
                max_fds=max_fds,
                timestamp_ms=ts_ms,
            ))

        if es_bulk is not None and es_bulk.configured and bulk_docs:
            es_bulk.send_bulk(
                "metrics-etcd.otel-default",
                bulk_docs,
                dynamic_templates={
                    "metrics.etcd_mvcc_db_total_size_in_bytes": "gauge_long",
                    "metrics.etcd_server_quota_backend_bytes": "gauge_long",
                    "metrics.process_open_fds": "gauge_long",
                    "metrics.process_max_fds": "gauge_long",
                },
            )

    run_tick_loop(stop_event, _emit_tick, interval=lambda: EMIT_INTERVAL_S)

    if es_bulk is not None:
        es_bulk.close()
    logger.info("etcd generator stopped.")


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
