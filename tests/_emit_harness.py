"""Run a generator for a few ticks and rebuild the documents Elasticsearch would index.

Shared by the tests that check generator output against what package templates
and library workflows read: alert identity, SLO templates, ML modules and
workflow evidence steps. Recorded OTLP sends are expanded the way the intake does
(resource and data-point attributes under ``resource.attributes.*`` /
``attributes.*``, one metrics document per data-point attribute set and time), and
``field`` resolves a query field the way the OTel passthrough mapping does.
"""
from __future__ import annotations

import fnmatch
import functools
import inspect
import re
import sys
import threading
import time
from pathlib import Path

import yaml

from generator_engine.registry import GENERATORS, load_entrypoint
from scenario_engine.yaml_scenario import load_yaml_scenario
from tests.test_generator_contract import (
    _OTLP_SENDS,
    _Chaos,
    _freeze,
    _RecordingES,
    _RecordingOTLP,
    _run_cycle,
    _scenario_data,
)

ROOT = Path(__file__).resolve().parent.parent
TICKS = 6
_NS = 1_000_000_000
_SETUP_HELPERS = re.compile(
    r"^(_reset_\w*mapping\w*|ensure_\w*mapping\w*|_maybe_heal_\w+|backfill_daily_history"
    r"|start_live_rollup_thread)$"
)


@functools.lru_cache(maxsize=None)
def gt_scenario(gid: str) -> Path | None:
    """A ``scenarios_testing/gt-*`` scenario that runs ``gid`` (None if none does).

    Mirrors ``generator_engine.registry.enabled_specs``: a generator runs when the
    scenario lists it (``true`` or a config dict without ``enabled: false``), or when
    it is not listed and the spec is ``default_enabled``. Explicit listings win; among
    default-on matches the gt scenario named after the generator is preferred
    (``gt-nginx`` for ``nginx_otel``).
    """
    spec = GENERATORS.get(gid)
    explicit: list[Path] = []
    default_on: list[Path] = []
    for path in sorted((ROOT / "scenarios_testing").glob("gt-*/scenario.yaml")):
        block = (yaml.safe_load(path.read_text()) or {}).get("generators") or {}
        if gid in block:
            cfg = block[gid]
            if cfg is True or (isinstance(cfg, dict) and cfg.get("enabled", True)):
                explicit.append(path.parent)
        elif spec is not None and spec.default_enabled:
            default_on.append(path.parent)
    if explicit:
        return explicit[0]
    stem = gid.split("_")[0]
    named = [d for d in default_on if d.name.removeprefix("gt-").startswith(stem)]
    return (named or default_on or [None])[0]


@functools.lru_cache(maxsize=None)
def gt_channels() -> dict[tuple[str, str], tuple[Path, int]]:
    """``(generator, impact key) -> (gt scenario dir, channel number)`` for every gt fault."""
    found: dict[tuple[str, str], tuple[Path, int]] = {}
    for path in sorted((ROOT / "scenarios_testing").glob("*/channels/*.yaml")):
        impact = (yaml.safe_load(path.read_text()) or {}).get("generator_impact") or {}
        for gid, keys in impact.items():
            for key in keys or {}:
                found.setdefault((gid, key), (path.parent.parent, int(path.name.split("-", 1)[0])))
    return found


# ── running a generator for several ticks ──

class _Ticks(threading.Event):
    """Stops after ``n`` ticks and moves the fake clock forward by each tick's wait."""

    def __init__(self, n: int, clock: dict):
        super().__init__()
        self._n = n
        self._clock = clock
        self.index = 0

    def wait(self, timeout=None):
        self._clock["ns"] += int((timeout or 30) * _NS)
        self.index += 1
        if self.index >= self._n:
            self.set()
        return self.is_set()


class _TickOTLP(_RecordingOTLP):
    def __init__(self, ticks: _Ticks):
        super().__init__()
        self.ticks = ticks
        self.tags: list[int] = []

    def __getattr__(self, name):
        target = super().__getattr__(name)
        if name not in _OTLP_SENDS:
            return target

        def _tagged(*args, **kwargs):
            self.tags.append(self.ticks.index)
            return target(*args, **kwargs)
        return _tagged

    def _send(self, url, payload, label):
        self.tags.append(self.ticks.index)
        super()._send(url, payload, label)


class _TickES(_RecordingES):
    max_failures_before_backoff = 5

    def __init__(self, ticks: _Ticks):
        super().__init__()
        self.ticks = ticks
        self.tags: list[int] = []

    def send_bulk(self, data_stream, docs, **kwargs):
        self.tags.append(self.ticks.index)
        return super().send_bulk(data_stream, docs, **kwargs)


def run_ticks(
    gid: str, scenario_dir: Path, channel: int | None, monkeypatch, *, ticks: int = TICKS,
) -> list[list[tuple[str, dict]]]:
    """Run ``gid`` for ``ticks`` ticks; return ``(index, doc)`` per tick.

    ``channel`` is the fault channel to hold active, or None for the healthy baseline.
    """
    spec = GENERATORS[gid]
    for key in ("ELASTIC_URL", "ELASTICSEARCH_URL", "ELASTIC_API_KEY", "ELASTICSEARCH_API_KEY"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr("app.config.ELASTIC_URL", "")
    monkeypatch.setattr("app.config.ELASTIC_API_KEY", "")
    fn = load_entrypoint(spec)
    mod = inspect.getmodule(fn)
    _freeze(mod, monkeypatch)
    clock = {"ns": 1_700_000_000 * _NS}

    def _time_ns() -> int:
        clock["ns"] += 1_000_000
        return clock["ns"]

    monkeypatch.setattr(time, "time_ns", _time_ns)
    monkeypatch.setattr(time, "time", lambda: _time_ns() / _NS)
    for name, module in list(sys.modules.items()):
        if name.startswith(("generators.", "generator_engine.")) and hasattr(module, "business_load"):
            monkeypatch.setattr(module, "business_load", lambda now=None: 1.0)
    for name in dir(mod):
        if _SETUP_HELPERS.match(name):
            monkeypatch.setattr(mod, name, lambda *a, **k: None)

    clock_ticks = _Ticks(ticks, clock)
    sidecar = _TickES(clock_ticks)
    sidecar.elastic_url = "http://es.invalid"
    sidecar.api_key = "test"
    if hasattr(mod, "ESBulkClient"):
        monkeypatch.setattr(mod, "ESBulkClient", lambda *a, **k: sidecar)
    client = _TickES(clock_ticks) if spec.client == "es_bulk" else _TickOTLP(clock_ticks)

    scenario = load_yaml_scenario(scenario_dir)
    data, _ = _scenario_data(scenario, spec, chaos=False)
    # As app/services/manager.py does: the scenario's generators: block config.
    config = scenario.get_generator_config(gid)
    if config:
        data["generator_config"] = config
    monkeypatch.setattr(sys.modules[_run_cycle.__module__], "_OneCycle", lambda: clock_ticks)
    _run_cycle(fn, client, data, _Chaos(channel) if channel is not None else None)

    per_tick: list[list[tuple[str, dict]]] = [[] for _ in range(ticks)]
    for recorder in (client, sidecar):
        for tick, (name, args, kwargs) in zip(recorder.tags, recorder.payloads):
            per_tick[min(tick, ticks - 1)].extend(documents(name, args, kwargs))
    return per_tick


# ── rebuilding indexed documents from recorded sends ──

_BUILDER: list = []


def _otlp_payloads(name: str, args: tuple, kwargs: dict) -> list[dict]:
    if name == "_send":
        return [args[1] if len(args) > 1 else kwargs.get("payload") or {}]
    if not _BUILDER:
        from app.telemetry import OTLPClient

        _BUILDER.append(OTLPClient(endpoint="http://127.0.0.1:9", api_key=""))
    builder = _BUILDER[0]
    captured: list[dict] = []
    builder._send = lambda _url, payload, _label: captured.append(payload)
    getattr(builder, name)(*args, **kwargs)
    return captured


def _value(value):
    if not isinstance(value, dict):
        return value
    for key in ("stringValue", "boolValue", "doubleValue"):
        if key in value:
            return value[key]
    if "intValue" in value:
        return int(value["intValue"])
    return None


def _attrs(items, prefix: str) -> dict:
    return {f"{prefix}{a['key']}": _value(a.get("value")) for a in items or [] if "key" in a}


def _flatten(obj, prefix: str = "", out: dict | None = None) -> dict:
    out = {} if out is None else out
    for key, value in obj.items():
        name = f"{prefix}{key}"
        if isinstance(value, dict):
            _flatten(value, f"{name}.", out)
        else:
            out[name] = value
    return out


def _histogram(dp: dict) -> dict:
    """An OTLP histogram point as the ``histogram`` field ES stores: bucket values + counts.

    Explicit-bound buckets are represented by their midpoint; the open first and last
    buckets by their one finite bound. Exponential histograms keep only the totals.
    """
    counts = [int(c) for c in dp.get("bucketCounts") or []]
    bounds = [float(b) for b in dp.get("explicitBounds") or []]
    values: list[float] = []
    for i, _count in enumerate(counts):
        if not bounds:
            values.append(float(dp.get("sum") or 0) / max(1, int(dp.get("count") or 1)))
        elif i == 0:
            values.append(bounds[0])
        elif i >= len(bounds):
            values.append(bounds[-1])
        else:
            values.append((bounds[i - 1] + bounds[i]) / 2)
    return {"values": values, "counts": counts, "count": int(dp.get("count") or sum(counts))}


def _index(signal: str, resource: dict) -> str:
    dataset = resource.get("resource.attributes.data_stream.dataset") or "generic"
    if not dataset.endswith(".otel"):
        dataset += ".otel"
    namespace = resource.get("resource.attributes.data_stream.namespace") or "default"
    return f"{signal}-{dataset}-{namespace}"


def documents(name: str, args: tuple, kwargs: dict) -> list[tuple[str, dict]]:
    if name == "send_bulk":
        stream, docs = args[0], args[1]
        return [(stream, _flatten(doc)) for doc in docs or []]
    docs: list[tuple[str, dict]] = []
    for payload in _otlp_payloads(name, args, kwargs):
        for rl in payload.get("resourceLogs") or []:
            res = _attrs((rl.get("resource") or {}).get("attributes"), "resource.attributes.")
            for sl in rl.get("scopeLogs") or []:
                for rec in sl.get("logRecords") or []:
                    doc = {**res, **_attrs(rec.get("attributes"), "attributes.")}
                    doc["event_name"] = rec.get("eventName")
                    doc["body.text"] = _value(rec.get("body"))
                    doc["severity_text"] = rec.get("severityText")
                    docs.append((_index("logs", res), doc))
        for rm in payload.get("resourceMetrics") or []:
            res = _attrs((rm.get("resource") or {}).get("attributes"), "resource.attributes.")
            for sm in rm.get("scopeMetrics") or []:
                grouped: dict[tuple, dict] = {}
                for metric in sm.get("metrics") or []:
                    for kind in ("gauge", "sum", "histogram", "exponentialHistogram"):
                        for dp in (metric.get(kind) or {}).get("dataPoints") or []:
                            point = _attrs(dp.get("attributes"), "attributes.")
                            key = (tuple(sorted(point.items(), key=str)), dp.get("timeUnixNano"))
                            doc = grouped.setdefault(key, {**res, **point})
                            if kind in ("gauge", "sum"):
                                value = dp.get("asDouble", dp.get("asInt"))
                            else:
                                value = _histogram(dp)
                            doc[f"metrics.{metric.get('name')}"] = value
                docs.extend((_index("metrics", res), doc) for doc in grouped.values())
        for rs in payload.get("resourceSpans") or []:
            res = _attrs((rs.get("resource") or {}).get("attributes"), "resource.attributes.")
            for ss in rs.get("scopeSpans") or []:
                for span in ss.get("spans") or []:
                    doc = {**res, **_attrs(span.get("attributes"), "attributes."), "span.name": span.get("name")}
                    docs.append((_index("traces", res), doc))
    return docs


def field(doc: dict, name: str):
    name = name.strip("`")
    for candidate in (name, f"metrics.{name}", f"attributes.{name}", f"resource.attributes.{name}"):
        if doc.get(candidate) is not None:
            return doc[candidate]
    for prefix in ("resource.attributes.", "attributes.", "metrics."):
        if name.startswith(prefix) and doc.get(name[len(prefix):]) is not None:
            return doc[name[len(prefix):]]
    return None


def matches_from(index: str, patterns: list[str]) -> bool:
    return not patterns or any(fnmatch.fnmatchcase(index, p) for p in patterns)


def es_view(index: str, doc: dict) -> dict:
    """``doc`` plus the ``data_stream.*`` fields Elasticsearch derives from its index name."""
    kind, _, rest = index.partition("-")
    dataset, _, namespace = rest.rpartition("-")
    return {
        **doc,
        "data_stream.type": kind,
        "data_stream.dataset": dataset or rest,
        "data_stream.namespace": namespace if dataset else "default",
    }


def index_matches(index: str, patterns: str | list[str]) -> bool:
    """True when ``index`` matches one of the comma-separated / listed index patterns."""
    if isinstance(patterns, str):
        patterns = [p.strip() for p in patterns.split(",") if p.strip()]
    return any(fnmatch.fnmatchcase(index, p) for p in patterns)


_RUNS: dict[tuple[str, int | None, int, str], list[list[tuple[str, dict]]]] = {}


def cached_run(gid: str, channel: int | None, monkeypatch, *, scenario_dir: Path | None = None,
               ticks: int = TICKS) -> list[list[tuple[str, dict]]]:
    """``run_ticks`` once per (generator, channel, ticks, scenario) per test session."""
    scenario_dir = scenario_dir or gt_scenario(gid)
    if scenario_dir is None:
        raise LookupError(f"no scenarios_testing/gt-* scenario enables {gid}")
    key = (gid, channel, ticks, str(scenario_dir))
    if key not in _RUNS:
        _RUNS[key] = run_ticks(gid, scenario_dir, channel, monkeypatch, ticks=ticks)
    return _RUNS[key]

