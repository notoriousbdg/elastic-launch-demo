"""etcd tests for impact key apply + healthy baseline."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from generators.etcd_otel import SPEC
from generators.etcd_otel import generator as mod


class _FakeChaos:
    def get_active_channels(self):
        return ["ch1"]


def _run(impact: dict | None = None) -> list[dict]:
    client = MagicMock()
    stop = MagicMock()
    stop.is_set.side_effect = [False, True]
    stop.wait.return_value = None
    registry = {}
    controller = None
    if impact is not None:
        controller = _FakeChaos()
        registry = {"ch1": {"generator_impact": {"etcd_otel": impact}}}
    with patch.object(mod, "_reset_proposal_counter_mapping"):
        with patch.object(mod, "ESBulkClient", side_effect=Exception("skip")):
            mod.run(client, stop, scenario_data={
                "namespace": "testns",
                "generator_config": {"chaos": "explicit"},
                "channel_registry": registry,
            }, chaos_controller=controller)
    metrics: list[dict] = []
    for call in client.send_metrics_scoped.call_args_list:
        metrics.extend(call.args[2])
    return metrics


def _gauge(metrics, name) -> float:
    for m in metrics:
        if m["name"] == name and "gauge" in m:
            dp = m["gauge"]["dataPoints"][0]
            return float(dp.get("asDouble", dp.get("asInt")))
    raise AssertionError(f"missing {name}")


def test_healthy_baseline_no_chaos():
    m = _run(None)
    assert _gauge(m, "etcd_server_has_leader") == 1
    assert _gauge(m, "up") == 1
    assert _gauge(m, "etcd_server_proposals_pending") <= 10
    db = _gauge(m, "etcd_mvcc_db_total_size_in_bytes")
    quota = _gauge(m, "etcd_server_quota_backend_bytes")
    assert db / quota <= 0.85
    assert _gauge(m, "process_open_fds") / _gauge(m, "process_max_fds") <= 0.9


def test_leader_changes_and_grpc_errors_flat_without_chaos():
    """OOTB alerts fire on INCREASE > 0 — must start at 0 and stay flat."""
    import random as _random

    rng = _random.Random(0)
    state = mod._MemberState(rng)
    assert state.leader_changes.value == 0
    for (_svc, _method, code), counter in state.grpc_handled.items():
        if code != "OK":
            assert counter.value == 0
    for _ in range(20):
        state.tick(rng, load=1.0, has_leader=True, failure_rate=0.0)
    assert state.leader_changes.value == 0
    for (_svc, _method, code), counter in state.grpc_handled.items():
        if code != "OK":
            assert counter.value == 0


def test_no_leader_and_scrape_down():
    assert _gauge(_run({"no_leader": 1.0}), "etcd_server_has_leader") == 0
    assert _gauge(_run({"scrape_target_down": 1.0}), "up") == 0


def test_quota_fd_pending():
    m = _run({"database_size_approaching_quota": 0.95})
    assert _gauge(m, "etcd_mvcc_db_total_size_in_bytes") / _gauge(m, "etcd_server_quota_backend_bytes") > 0.9
    m = _run({"file_descriptor_exhaustion": 0.95})
    assert _gauge(m, "process_open_fds") / _gauge(m, "process_max_fds") > 0.9
    # GT Impact Key Coverage often sends percent 85.0 (< OOTB 90% threshold)
    m = _run({"file_descriptor_exhaustion": 85.0})
    assert _gauge(m, "process_open_fds") / _gauge(m, "process_max_fds") > 0.9
    assert _gauge(_run({"pending_proposals_backlog": 20}), "etcd_server_proposals_pending") > 10

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.etcd_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['database_size_approaching_quota', 'file_descriptor_exhaustion', 'frequent_leader_elections', 'grpc_errors', 'heartbeat_send_failures', 'no_leader', 'pending_proposals_backlog', 'proposal_failures', 'scrape_target_down', 'slow_apply_operations']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
