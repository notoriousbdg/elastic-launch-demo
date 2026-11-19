"""Azure retrieval-service / embedding-service channels (05, 06, 11, 13, 20).

Each signal metric sits in its baseline band before the fault, deviates during it while the
status log says ANOMALY (with the bad configuration visible), and converges back after the
opsctl fix with the corrected configuration. A metric only moves for the channel that causes
it (the shared retrieval-service metrics stay at baseline for the other channels).
"""
from __future__ import annotations

from pathlib import Path

import pytest

import yaml

from app import ops
from tests._azure_harness import (
    OPS_ACTOR, PROFILE, REG, attr_values, phase, records, values,
)

RET, EMB = "retrieval-service", "embedding-service"
CHANNEL_DIR = Path(__file__).resolve().parent.parent / "scenarios" / "azure" / "channels"


def _params(ch: int) -> dict:
    """fault_params of channel *ch* straight from its YAML (the registry does not carry them)."""
    (path,) = CHANNEL_DIR.glob(f"{ch:02d}-*.yaml")
    return yaml.safe_load(path.read_text())["fault_params"]


def _within(ch: int) -> float:
    return max(v["within_seconds"] for v in REG[ch]["remediation_effects"]["verification"])


def _sig(ch, which, svc, metric, cycles=6, **kw):
    logs, metrics = records(phase(ch, which, **kw), cycles=cycles)
    vals = values(metrics, svc, metric)
    assert vals, f"{svc} emitted no {metric}"
    return logs, vals


def _status(logs, svc, op, key):
    return {r["attrs"].get(key) for r in logs
            if r["service"] == svc and r["attrs"].get("operation") == op}


def _cmd_lines(ch):
    return [ln.strip() for ln in REG[ch]["remediation_command"].splitlines() if ln.strip()]


def _assert_cmd_lines(logs, ch):
    for line in _cmd_lines(ch):
        hits = [r["body"] for r in logs
                if line in r["body"] and f"by={OPS_ACTOR}" in r["body"]
                and r["body"].startswith("config-service:")]
        assert len(hits) == 1, (line, hits)


def _cause_precedes(ch, svc):
    ev = REG[ch]["infrastructure_events"][0]
    assert ev["offset_seconds"] < 0 and ev["service"] == svc


# -- ch05: indexer credentials expired -> index freshness ----------------------

def test_ch05_index_freshness_phases():
    m = "retrieval.index_freshness_minutes"
    logs, v = _sig(5, "before", RET, m)
    assert all(2 <= x <= 18 for x in v), v
    assert _status(logs, RET, "index_status", "search.status") == {"NOMINAL"}
    assert attr_values(logs, RET, "search.indexer_status") == {"success"}

    logs, v = _sig(5, "during", RET, m)
    assert all(48 * 60 <= x <= 240 * 60 for x in v), v
    assert _status(logs, RET, "index_status", "search.status") == {"ANOMALY"}
    assert attr_values(logs, RET, "search.indexer_status") == {"error"}
    assert "Data source credentials expired" in attr_values(logs, RET, "search.indexer_error")

    logs, v = _sig(5, "after", RET, m, since=_within(5))
    assert all(x < 6 * 60 for x in v) and all(x <= 18 for x in v), v
    assert _status(logs, RET, "index_status", "search.status") == {"NOMINAL"}
    assert attr_values(logs, RET, "search.indexer_status") == {"success"}
    _assert_cmd_lines(logs, 5)


def test_ch05_converges_rather_than_snaps():
    _, v = _sig(5, "after", RET, "retrieval.index_freshness_minutes", since=10)
    assert min(v) > 6 * 60, v


def test_ch05_cause_event_precedes_symptoms():
    _cause_precedes(5, RET)
    assert REG[5]["infrastructure_events"][0]["attributes"]["search.indexer_status"] == "error"


# -- ch06: dimension mismatch -> vector_dimension, hits_returned ---------------

def test_ch06_dimension_and_hits_phases():
    d, h = "embedding_service.vector_dimension", "retrieval.hits_returned"
    logs, v = _sig(6, "before", EMB, d)
    assert set(v) == {1536}, v
    assert attr_values(logs, EMB, "embedding.dimensions") == {"1536"}
    assert _status(logs, EMB, "embedding_config", "embedding.status") == {"NOMINAL"}
    _, hits = _sig(6, "before", RET, h, cycles=40)
    assert all(5 <= x <= 8 for x in hits), hits

    logs, v = _sig(6, "during", EMB, d)
    assert set(v) == {_params(6)["model_dimensions"]}, v
    assert attr_values(logs, EMB, "embedding.dimensions") == {"3072"}
    assert _status(logs, EMB, "embedding_config", "embedding.status") == {"ANOMALY"}
    logs_r, hits = _sig(6, "during", RET, h, cycles=60)
    zero = sum(1 for x in hits if x == 0) / len(hits)
    assert 0.2 < zero < 0.97 and all(x == 0 or 5 <= x <= 8 for x in hits), (zero, hits)
    pct = attr_values(logs_r, RET, "search.empty_result_pct")
    assert pct and all(35.0 <= p <= 90.0 for p in pct), pct
    assert _status(logs_r, RET, "vector_query", "search.status") == {"ANOMALY"}

    logs, v = _sig(6, "after", EMB, d, since=_within(6))
    assert set(v) == {1536}, v
    assert attr_values(logs, EMB, "embedding.dimensions") == {"1536"}
    assert _status(logs, EMB, "embedding_config", "embedding.status") == {"NOMINAL"}
    _, hits = _sig(6, "after", RET, h, cycles=40, since=_within(6))
    assert all(5 <= x <= 8 for x in hits), hits
    _assert_cmd_lines(logs, 6)


def test_ch06_converges_rather_than_snaps():
    _, v = _sig(6, "after", EMB, "embedding_service.vector_dimension", cycles=40, since=10)
    assert 3072 in v, v
    _, hits = _sig(6, "after", RET, "retrieval.hits_returned", cycles=60, since=10)
    assert 0 in hits, hits


def test_ch06_cause_event_precedes_symptoms():
    _cause_precedes(6, EMB)
    assert REG[6]["infrastructure_events"][0]["attributes"]["embedding.dimensions"] == 3072


# -- ch11: ranker config -> search latency -------------------------------------

def test_ch11_search_latency_phases():
    m = "retrieval.search_latency_ms"
    logs, v = _sig(11, "before", RET, m)
    assert all(45.0 <= x <= 160.0 for x in v), v
    assert _status(logs, RET, "search_health", "search.status") == {"NOMINAL"}
    assert attr_values(logs, RET, "search.ranker_config") == {"semantic-ranker-v3-rerank50"}

    p95 = _params(11)["query_p95_ms"]["randint"]
    logs, v = _sig(11, "during", RET, m)
    assert all(p95[0] <= x <= p95[1] for x in v), v
    assert _status(logs, RET, "search_health", "search.status") == {"ANOMALY"}
    assert attr_values(logs, RET, "search.ranker_config") == {_params(11)["ranker_config"]}
    # the semantic ranker itself is not the cost
    _, sem = _sig(11, "during", RET, "retrieval.semantic_ranker_ms")
    assert all(x <= 110.0 for x in sem), sem

    logs, v = _sig(11, "after", RET, m, since=_within(11))
    assert all(x <= _params(11)["baseline_p95_ms"] for x in v), v
    assert _status(logs, RET, "search_health", "search.status") == {"NOMINAL"}
    assert attr_values(logs, RET, "search.ranker_config") == {"semantic-ranker-v3-rerank50"}
    _assert_cmd_lines(logs, 11)


def test_ch11_converges_rather_than_snaps():
    _, v = _sig(11, "after", RET, "retrieval.search_latency_ms", since=10)
    assert min(v) > 1000.0, v


def test_ch11_cause_event_precedes_symptoms():
    _cause_precedes(11, RET)
    assert "rerank200" in REG[11]["infrastructure_events"][0]["attributes"]["search.ranker_config"]


# -- ch13: worker pool cap -> queue depth --------------------------------------

def test_ch13_queue_depth_phases():
    m = "embedding_service.queue_depth"
    logs, v = _sig(13, "before", EMB, m)
    assert all(0 <= x <= 60 for x in v), v
    assert _status(logs, EMB, "queue_health", "embed.status") == {"NOMINAL"}
    assert attr_values(logs, EMB, "batch.max_workers") == {"4"}

    rng = _params(13)["queue_depth"]["randint"]
    logs, v = _sig(13, "during", EMB, m)
    assert all(rng[0] <= x <= rng[1] for x in v), v
    assert _status(logs, EMB, "queue_health", "embed.status") == {"ANOMALY"}
    assert attr_values(logs, EMB, "batch.max_workers") == {str(_params(13)["max_workers"])}

    logs, v = _sig(13, "after", EMB, m, since=_within(13))
    assert all(x <= 100 for x in v), v
    assert _status(logs, EMB, "queue_health", "embed.status") == {"NOMINAL"}
    assert attr_values(logs, EMB, "batch.max_workers") == {"16"}
    _assert_cmd_lines(logs, 13)


def test_ch13_drains_over_minutes():
    _, early = _sig(13, "after", EMB, "embedding_service.queue_depth", since=10)
    assert min(early) > 4000, early
    _, mid = _sig(13, "after", EMB, "embedding_service.queue_depth", since=75)
    assert 1000 < min(mid) and max(mid) < 30000, mid


def test_ch13_cause_event_precedes_symptoms():
    _cause_precedes(13, EMB)
    assert REG[13]["infrastructure_events"][0]["attributes"]["batch.max_workers"] == 4


# -- ch20: cache not invalidated -> stale hits ---------------------------------

def test_ch20_cache_phases():
    st, hit = "retrieval.cache_stale_hit_pct", "retrieval.cache_hit_pct"
    logs, v = _sig(20, "before", RET, st)
    assert set(v) == {0.0}, v
    _, h = _sig(20, "before", RET, hit)
    assert all(38.0 <= x <= 66.0 for x in h), h
    assert _status(logs, RET, "cache_status", "cache.status") == {"NOMINAL"}
    assert attr_values(logs, RET, "cache.config") == {"invalidate_on_index_refresh=true"}

    logs, v = _sig(20, "during", RET, st)
    assert all(30.0 <= x <= 85.0 for x in v), v
    _, h = _sig(20, "during", RET, hit)
    assert all(72.0 <= x <= 92.0 for x in h), h
    assert _status(logs, RET, "cache_status", "cache.status") == {"ANOMALY"}
    assert attr_values(logs, RET, "cache.config") == {"invalidate_on_index_refresh=false"}

    logs, v = _sig(20, "after", RET, st, since=_within(20) - 240)
    assert set(v) == {0.0}, v
    assert _status(logs, RET, "cache_status", "cache.status") == {"NOMINAL"}
    assert attr_values(logs, RET, "cache.config") == {"invalidate_on_index_refresh=true"}
    _assert_cmd_lines(logs, 20)
    _, h = _sig(20, "after", RET, hit, since=_within(20))
    assert all(38.0 <= x <= 66.0 for x in h), h


def test_ch20_stale_converges_and_cache_refills():
    _, v = _sig(20, "after", RET, "retrieval.cache_stale_hit_pct", since=10)
    assert min(v) > 0.0, v
    _, h = _sig(20, "after", RET, "retrieval.cache_hit_pct", since=5)
    assert max(h) < 15.0, h  # emptied by the delete
    _, h = _sig(20, "after", RET, "retrieval.cache_hit_pct", since=120)
    assert 15.0 < min(h) and max(h) < 66.0, h  # refilling


def test_ch20_cause_event_precedes_symptoms():
    _cause_precedes(20, RET)
    assert REG[20]["infrastructure_events"][0]["attributes"]["cache.config"] == "invalidate_on_index_refresh=false"


# -- isolation: each shared metric only moves for its own channel --------------

@pytest.mark.parametrize("ch", [5, 6, 11, 13, 20])
def test_other_channels_leave_signals_at_baseline(ch):
    own = {
        "retrieval.index_freshness_minutes": {5, 13}, "retrieval.hits_returned": {6},
        "retrieval.search_latency_ms": {11}, "retrieval.cache_stale_hit_pct": {20},
        "retrieval.cache_hit_pct": {20},
        "embedding_service.vector_dimension": {6}, "embedding_service.queue_depth": {13},
    }
    base = {
        "retrieval.index_freshness_minutes": lambda x: x <= 18,
        "retrieval.hits_returned": lambda x: 5 <= x <= 8,
        "retrieval.search_latency_ms": lambda x: x <= 160,
        "retrieval.cache_stale_hit_pct": lambda x: x == 0,
        "retrieval.cache_hit_pct": lambda x: 38 <= x <= 66,
        "embedding_service.vector_dimension": lambda x: x == 1536,
        "embedding_service.queue_depth": lambda x: x <= 60,
    }
    _, metrics = records(phase(ch, "during"), cycles=10)
    for name, owners in own.items():
        if ch in owners:
            continue
        svc = EMB if name.startswith("embedding") else RET
        vals = values(metrics, svc, name)
        assert vals and all(base[name](x) for x in vals), (ch, name, vals)


# -- shared: commands ----------------------------------------------------------

@pytest.mark.parametrize("ch", [5, 6, 11, 13, 20])
def test_expected_commands_parse(ch):
    assert len(ops.expected_commands(REG[ch]["remediation_command"], PROFILE)) == len(_cmd_lines(ch))
    for line in _cmd_lines(ch):
        ops.parse_command(line, PROFILE)
