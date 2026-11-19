"""Tests for the gcp_cloudstorage_otel generator."""

from __future__ import annotations

import random

from generator_engine.identity import ScenarioIdentity
from generators.gcp_cloudstorage_otel import SPEC
from generators.gcp_cloudstorage_otel.generator import (
    CLIENT_FAULT_CODES,
    METRIC_NAMES,
    SERVICE_FAULT_CODES,
    BucketState,
    build_buckets,
    generate_bucket_metrics,
)

_P = "storage.googleapis.com/"


def _attrs(dp: dict) -> dict:
    return {a["key"]: next(iter(a["value"].values())) for a in dp.get("attributes", [])}


def _buckets():
    ident = ScenarioIdentity("gtgcpcloudstorage")
    return build_buckets(ident, ["data-lake", "media-assets", "backups"],
                         ident.gcp_project_id, "us-central1")


def _emit(chaos=None, seed=11, only_target=True):
    rng = random.Random(seed)
    out = []
    for b in _buckets():
        if only_target and b["index"] != 0:
            continue
        out.extend(generate_bucket_metrics(b, BucketState(rng), rng, chaos))
    return out


def _points(metrics, suffix):
    out = []
    for m in metrics:
        if m["name"] == _P + suffix:
            for dp in m["gauge"]["dataPoints"]:
                v = float(dp["asDouble"]) if "asDouble" in dp else float(dp["asInt"])
                out.append((_attrs(dp), v))
    return out


def _requests(metrics):
    return _points(metrics, "api/request_count")


def _share(metrics, codes):
    pts = _requests(metrics)
    total = sum(v for _, v in pts)
    return sum(v for a, v in pts if a["response_code"] in codes) / total


def test_spec_shape():
    assert SPEC.client == "otlp"
    assert SPEC.datasets == ("gcp.cloudstorage.otel",)
    assert SPEC.wants_chaos is True and SPEC.chaos_mode == "explicit"
    assert len(SPEC.impact_keys) >= 3
    assert all(not f["required"] for f in SPEC.config_fields.values())


def test_buckets_identity():
    buckets = _buckets()
    assert [b["name"] for b in buckets] == [
        "gtgcpcloudstorage-data-lake", "gtgcpcloudstorage-media-assets",
        "gtgcpcloudstorage-backups",
    ]
    assert buckets[1]["location"] == "us"
    assert buckets[0]["location"] == "us-central1"


def test_every_dashboard_metric_is_emitted():
    metrics = _emit(only_target=False)
    assert {m["name"] for m in metrics} == set(METRIC_NAMES)
    assert all("gauge" in m for m in metrics)
    methods = {a["method"] for a, _ in _requests(metrics)}
    assert {"ReadObject", "WriteObject"} <= methods
    types = {a["type"] for a, _ in _points(metrics, "storage/v2/total_bytes")}
    assert "live-object" in types and len(types) == 4
    reads = [a for a, _ in _points(metrics, "network/sent_bytes_count")
             if a["method"] == "ReadObject"]
    assert reads


def test_healthy_baseline():
    metrics = _emit(only_target=False)
    assert _share(metrics, {"OK"}) > 0.99
    assert _share(metrics, set(SERVICE_FAULT_CODES)) < 0.005
    total = sum(v for _, v in _points(metrics, "storage/total_bytes"))
    assert total > 100 * 1024 ** 4


def test_error_4xx_rate():
    metrics = _emit({"error_4xx_rate": 0.4})
    assert _share(metrics, {"PERMISSION_DENIED"}) > 0.35
    assert _share(metrics, set(CLIENT_FAULT_CODES)) > 0.35


def test_error_5xx_rate():
    metrics = _emit({"error_5xx_rate": 0.2})
    assert 0.15 < _share(metrics, set(SERVICE_FAULT_CODES)) < 0.25
    codes = {a["response_code"] for a, _ in _requests(metrics)}
    assert {"UNAVAILABLE", "INTERNAL", "DEADLINE_EXCEEDED"} <= codes


def test_request_spike():
    healthy = sum(v for _, v in _requests(_emit()))
    spiked = sum(v for _, v in _requests(_emit({"request_spike": 10.0})))
    assert spiked > 7 * healthy
    egress_h = sum(v for _, v in _points(_emit(), "network/sent_bytes_count"))
    egress_s = sum(v for _, v in _points(_emit({"request_spike": 10.0}), "network/sent_bytes_count"))
    assert egress_s > 5 * egress_h


def test_non_target_bucket_unaffected():
    rng = random.Random(2)
    bucket = _buckets()[1]
    metrics = generate_bucket_metrics(bucket, BucketState(rng), rng, {"error_4xx_rate": 0.9})
    assert _share(metrics, {"PERMISSION_DENIED"}) < 0.01


# ── Daily storage history backfill ────────────────────────────────────────────

import datetime  # noqa: E402

import httpx  # noqa: E402

from generators.gcp_cloudstorage_otel import generator as gen  # noqa: E402

_NOW = datetime.datetime(2026, 10, 21, 17, 40, tzinfo=datetime.timezone.utc)


def test_backfill_days_are_noon_utc_inside_the_look_back():
    days = gen.backfill_days(_NOW)
    assert [d for d, _ in days] == [6, 5, 4, 3, 2, 1]
    stamps = [datetime.datetime.fromtimestamp(ts / 1e9, datetime.timezone.utc) for _, ts in days]
    assert all(s.hour == 12 and s.minute == 0 for s in stamps)
    assert stamps == sorted(stamps) and stamps[-1].date() == datetime.date(2026, 10, 20)
    assert _NOW - stamps[0] < datetime.timedelta(days=7)


def test_daily_history_rises_toward_today():
    rng = random.Random(3)
    bucket = _buckets()[0]
    state = BucketState(rng)
    state.seed(bucket["classes"], rng)
    today = _points(gen.daily_storage_metrics(bucket, state, 0), "storage/total_bytes")
    week = _points(gen.daily_storage_metrics(bucket, state, 6), "storage/total_bytes")
    assert len(today) == len(week) == len(bucket["classes"])
    for (t_attrs, t_bytes), (w_attrs, w_bytes) in zip(today, week):
        assert t_attrs == w_attrs
        assert w_bytes < t_bytes


def _es(stream_start: str | None, template: dict | None = None):
    """A fake Elasticsearch: records calls; ``stream_start`` None means no stream."""
    calls: list[tuple[str, str, dict | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = None
        if request.content:
            import json
            body = json.loads(request.content)
        calls.append((request.method, request.url.path, body))
        path = request.url.path
        if path == "/_component_template/metrics@custom":
            if request.method == "GET":
                if template is None:
                    return httpx.Response(404, json={})
                return httpx.Response(200, json={"component_templates": [
                    {"name": "metrics@custom", "component_template": {"template": template}}]})
            return httpx.Response(200, json={"acknowledged": True})
        if path == f"/_data_stream/{gen.DATA_STREAM}":
            if request.method == "DELETE":
                return httpx.Response(200, json={"acknowledged": True})
            if stream_start is None:
                return httpx.Response(404, json={})
            return httpx.Response(200, json={"data_streams": [
                {"indices": [{"index_name": ".ds-x-000001"}]}]})
        if path.startswith("/.ds-x-000001/_settings"):
            return httpx.Response(200, json={".ds-x-000001": {"settings": {
                "index": {"time_series": {"start_time": stream_start}}}}})
        return httpx.Response(404, json={})

    return httpx.Client(transport=httpx.MockTransport(handler)), calls


_OLDEST = datetime.datetime(2026, 10, 15, 12, tzinfo=datetime.timezone.utc)


def test_prepare_keeps_other_template_settings_and_needs_no_reset_without_a_stream():
    http, calls = _es(None, {"settings": {"index": {"refresh_interval": "5s"}}})
    assert gen.prepare_history_stream(http, "http://es.test", _OLDEST)
    put = next(b for m, p, b in calls if m == "PUT")
    assert put["template"]["settings"]["index"] == {"refresh_interval": "5s", "look_back_time": "7d"}
    assert not any(m == "DELETE" for m, _, _ in calls)


def test_prepare_recreates_a_stream_that_cannot_reach_back():
    http, calls = _es("2026-10-21T15:31:50.000Z", {"settings": {"index": {"look_back_time": "7d"}}})
    assert gen.prepare_history_stream(http, "http://es.test", _OLDEST)
    assert not any(m == "PUT" for m, _, _ in calls)
    assert ("DELETE", f"/_data_stream/{gen.DATA_STREAM}", None) in calls


def test_prepare_keeps_a_stream_that_already_reaches_back():
    http, calls = _es("2026-10-14T17:00:00.000Z", {"settings": {"index": {"look_back_time": "7d"}}})
    assert gen.prepare_history_stream(http, "http://es.test", _OLDEST)
    assert not any(m == "DELETE" for m, _, _ in calls)


class _Recorder:
    def __init__(self, elastic_url="", api_key=""):
        self.elastic_url, self.api_key = elastic_url, api_key
        self.sent: list[list[dict]] = []

    def send_metrics_scoped(self, resource, scope, metrics):
        self.sent.append(metrics)


def _backfill(client, monkeypatch):
    from generator_engine.db_common import MultiEntityState

    for key in ("ELASTIC_URL", "ELASTICSEARCH_URL", "ELASTIC_API_KEY", "ELASTICSEARCH_API_KEY"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(gen, "prepare_history_stream", lambda http, url, oldest: True)
    rng = random.Random(5)
    buckets = _buckets()
    states = MultiEntityState([b["name"] for b in buckets], BucketState, rng)
    resources = {b["name"]: {} for b in buckets}
    return gen.backfill_daily_history(client, resources, buckets, states, {}, rng), buckets


def test_backfill_sends_one_stamp_per_day_per_bucket(monkeypatch):
    client = _Recorder("http://es.example", "key")
    days, buckets = _backfill(client, monkeypatch)
    assert days == gen.BACKFILL_DAYS
    assert len(client.sent) == days * len(buckets)
    stamps = []
    for metrics in client.sent:
        times = {dp["timeUnixNano"] for m in metrics for dp in m["gauge"]["dataPoints"]}
        assert len(times) == 1
        stamps.append(times.pop())
    assert len(set(stamps)) == days


def test_backfill_needs_elasticsearch(monkeypatch):
    client = _Recorder()
    days, _ = _backfill(client, monkeypatch)
    assert days == 0 and client.sent == []
