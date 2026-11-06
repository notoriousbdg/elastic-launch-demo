"""Contracts for the Chaos UI infrastructure spikes: controller keys, persistence, reset.

The channel augmentation keys (node disk fill, NotReady, CrashLoop, probes, HPA cap, PVC fill,
DaemonSet, APM failed-transaction rate) are controller-owned. They must round-trip through the
SQLite store without a column per key and clear when the last channel resolves.
"""
import json
import sqlite3

from app.chaos.controller import INFRA_SPIKE_DEFAULTS, ChaosController
from app.store import ChaosStore

AUGMENTATION_KEYS = {
    "node_disk_fill_pct", "k8s_node_not_ready", "k8s_pvc_fill_pct", "k8s_crashloop",
    "k8s_readiness_fail", "k8s_liveness_fail", "k8s_hpa_capped",
    "k8s_daemonset_misscheduled", "apm_error_rate",
}


def _registry():
    return {1: {
        "name": "fuel", "subsystem": "propulsion", "affected_services": ["engine"],
        "cascade_services": [], "error_type": "Fault", "sensor_type": "pressure",
        "vehicle_section": "booster", "description": "fuel", "infra_impact": {},
    }}


def test_augmentation_keys_are_controller_defaults_and_off():
    assert AUGMENTATION_KEYS <= set(INFRA_SPIKE_DEFAULTS)
    assert all(INFRA_SPIKE_DEFAULTS[k] == 0 for k in AUGMENTATION_KEYS)


def test_augmentation_spikes_persist_and_survive_a_restart(tmp_path):
    db = str(tmp_path / "chaos.db")
    controller = ChaosController(_registry(), chaos_store=ChaosStore(db), deployment_id="dep")
    controller.trigger(1, session_id="s")
    controller.set_infra_spikes({"k8s_crashloop": 1, "node_disk_fill_pct": 90, "cpu_pct": 40})

    restored = ChaosController(_registry(), chaos_store=ChaosStore(db), deployment_id="dep")
    spikes = restored.get_infra_spikes()
    assert spikes["k8s_crashloop"] == 1
    assert spikes["node_disk_fill_pct"] == 90
    assert spikes["cpu_pct"] == 40  # legacy column still round-trips


def test_unknown_spike_keys_are_ignored(tmp_path):
    controller = ChaosController(_registry(), chaos_store=ChaosStore(str(tmp_path / "c.db")), deployment_id="d")
    controller.set_infra_spikes({"not_a_spike": 5})
    assert "not_a_spike" not in controller.get_infra_spikes()


def test_augmentations_reset_when_the_last_channel_resolves(tmp_path):
    controller = ChaosController(_registry(), chaos_store=ChaosStore(str(tmp_path / "c.db")), deployment_id="d")
    controller.trigger(1, session_id="s")
    controller.set_infra_spikes({"k8s_node_not_ready": 1, "apm_error_rate": 25, "k8s_hpa_capped": 1})
    controller.resolve(1, session_id="s")
    spikes = controller.get_infra_spikes()
    assert all(spikes[k] == 0 for k in AUGMENTATION_KEYS)
    assert spikes["latency_multiplier"] == 1.0


def test_store_migrates_a_db_created_before_extra_json(tmp_path):
    db = str(tmp_path / "old.db")
    conn = sqlite3.connect(db)
    conn.execute(
        """CREATE TABLE chaos_spikes (
            deployment_id TEXT PRIMARY KEY, cpu_pct REAL NOT NULL DEFAULT 0,
            memory_pct REAL NOT NULL DEFAULT 0, k8s_oom_intensity REAL NOT NULL DEFAULT 0,
            k8s_pod_cpu_intensity REAL NOT NULL DEFAULT 0,
            k8s_pod_memory_intensity REAL NOT NULL DEFAULT 0,
            latency_multiplier REAL NOT NULL DEFAULT 1.0)"""
    )
    conn.execute("INSERT INTO chaos_spikes (deployment_id, cpu_pct) VALUES ('dep', 55)")
    conn.commit()
    conn.close()

    store = ChaosStore(db)
    assert store.get_spikes("dep")["cpu_pct"] == 55
    store.upsert_spikes("dep", {"cpu_pct": 55, "k8s_crashloop": 1, "apm_error_rate": 12.5})
    got = store.get_spikes("dep")
    assert got["k8s_crashloop"] == 1 and got["apm_error_rate"] == 12.5
    assert "extra_json" not in got
    raw = sqlite3.connect(db).execute("SELECT extra_json FROM chaos_spikes").fetchone()[0]
    assert json.loads(raw) == {"k8s_crashloop": 1, "apm_error_rate": 12.5}
