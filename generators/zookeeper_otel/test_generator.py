"""ZooKeeper-specific tests for the zookeeper_otel generator."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

from generators.zookeeper_otel import SPEC
from generators.zookeeper_otel.generator import METRIC_NAMES, run


def _metric_names(calls) -> set[str]:
    names: set[str] = set()
    for call in calls:
        metrics = call.args[2] if len(call.args) >= 3 else call.kwargs.get("metrics", [])
        for m in metrics:
            names.add(m["name"])
    return names


def test_dataset_is_generic() -> None:
    assert SPEC.datasets == ("generic.otel",), (
        "zookeeper_otel routes to metrics-generic.otel-default; "
        "do not change the dataset to 'zookeeperreceiver.otel'"
    )


def test_emits_all_dashboard_metrics() -> None:
    client = MagicMock()
    stop = threading.Event()
    sends = {"n": 0}

    def _after_send(*_a, **_k):
        sends["n"] += 1
        if sends["n"] >= 3:
            stop.set()

    client.send_metrics_scoped.side_effect = _after_send
    run(client, stop, scenario_data={
        "namespace": "testns",
        "generator_config": {"service_name": "zookeeper"},
        "channel_registry": {},
    })

    assert client.send_metrics_scoped.call_count >= 3
    names = _metric_names(client.send_metrics_scoped.call_args_list)
    missing = set(METRIC_NAMES) - names
    assert not missing, f"missing metrics: {sorted(missing)}"
    assert "zookeeper.connections" not in names
    assert "zookeeper.data_tree.node.count" not in names


def test_leader_emits_sync_and_follower_metrics() -> None:
    client = MagicMock()
    stop = threading.Event()
    client.send_metrics_scoped.side_effect = lambda *_a, **_k: stop.set()

    run(client, stop, scenario_data={
        "namespace": "testns",
        "generator_config": {
            "service_name": "zookeeper",
            "node_names": ["zk-1", "zk-2", "zk-3"],
        },
        "channel_registry": {},
    })

    leader_metrics = client.send_metrics_scoped.call_args_list[0].args[2]
    leader_names = {m["name"] for m in leader_metrics}
    assert "zookeeper.sync.pending" in leader_names
    assert "zookeeper.follower.count" in leader_names

    follower_states = set()
    for m in leader_metrics:
        if m["name"] != "zookeeper.follower.count":
            continue
        dps = m["sum"]["dataPoints"]
        for a in dps[0].get("attributes", []):
            if a["key"] == "state":
                follower_states.add(next(iter(a["value"].values())))
    assert follower_states == {"synced", "unsynced"}

# --- 1:1 alert impact_keys coverage (auto) ---
from generators.zookeeper_otel import SPEC as _SPEC_FOR_ALERT_KEYS


def test_impact_keys_one_per_package_alert():
    """SPEC.impact_keys must be exactly 1:1 with package alert rules."""
    expected = ['data_tree_size_excessive', 'excessive_watch_count', 'excessive_znode_count', 'file_descriptor_utilization_high', 'high_request_latency', 'node_health_check_failed', 'outstanding_requests_saturation', 'pending_syncs_buildup_on_leader', 'unsynced_followers_detected']
    assert sorted(_SPEC_FOR_ALERT_KEYS.impact_keys) == expected
    assert _SPEC_FOR_ALERT_KEYS.wants_chaos is True
