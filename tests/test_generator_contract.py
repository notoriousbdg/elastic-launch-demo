"""Contract for every registered generator.

Static checks catch a missing ``run`` or an impact key the source never mentions.
Runtime checks load ``run`` and execute one cycle against a fake client, with
chaos off and on, so a generator cannot drift from ``GeneratorSpec`` or from the
``ServiceManager`` call shape without failing.
"""
import copy
import inspect
import random
import re
import threading
from pathlib import Path

import pytest

from generator_engine.registry import GENERATORS, load_entrypoint

ROOT = Path(__file__).resolve().parent.parent
_Random = random.Random

_OTLP_SENDS = {
    "_send",
    "send_logs",
    "send_logs_scoped",
    "send_metrics",
    "send_metrics_scoped",
    "send_traces",
    "send_traces_multi",
}
_ES_SENDS = {"send_bulk"}


def _source(spec) -> str:
    return (ROOT / (spec.module.replace(".", "/") + ".py")).read_text()


class _OneCycle(threading.Event):
    """Lets a generator finish a single loop, then stop on the interval wait."""

    def wait(self, timeout=None):
        self.set()
        return True


class _Chaos:
    def __init__(self, channel_id: int):
        self._channel_id = channel_id

    def get_active_channels(self):
        return [self._channel_id]

    def get_infra_spikes(self):
        return {"latency_multiplier": 8.0, "error_rate": 0.95}


class _RecordingOTLP:
    def __init__(self):
        from app.telemetry import OTLPClient

        self._client = OTLPClient(endpoint="http://127.0.0.1:9", api_key="")
        self.endpoint = self._client.endpoint
        self.elastic_url = ""
        self.api_key = ""
        self.payloads: list = []

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        target = getattr(self._client, name)
        if name in _OTLP_SENDS:
            def _record(*args, **kwargs):
                self.payloads.append((name, args, kwargs))
                return None
            return _record
        return target

    def _send(self, url, payload, label):
        self.payloads.append(("_send", (url, payload, label), {}))


class _RecordingES:
    def __init__(self):
        self.elastic_url = ""
        self.api_key = ""
        self.configured = True
        self.consecutive_failures = 0
        self.payloads: list = []

    def send_bulk(self, data_stream, docs, **kwargs):
        self.payloads.append(("send_bulk", (data_stream, docs), kwargs))
        return len(docs or [])

    def close(self):
        return None


def _extreme(meta: dict):
    span = meta.get("range") or []
    if len(span) == 2:
        return span[1]
    kind = meta.get("type")
    if kind == "int":
        return int(meta.get("default") or 0) + 50
    if kind == "float":
        return float(meta.get("default") or 0) + 10.0
    if kind == "bool":
        return not bool(meta.get("default"))
    return "contract-chaos"


def _scenario_data(scenario, spec, *, chaos: bool):
    data = {
        "services": copy.deepcopy(scenario.services),
        "channel_registry": copy.deepcopy(scenario.channel_registry),
        "namespace": scenario.namespace,
        "hosts": copy.deepcopy(scenario.hosts),
        "k8s_clusters": copy.deepcopy(scenario.k8s_clusters),
        "service_topology": copy.deepcopy(scenario.service_topology),
        "entry_endpoints": copy.deepcopy(scenario.entry_endpoints),
        "db_operations": copy.deepcopy(scenario.db_operations),
        "scenario": scenario,
    }
    channel_id = sorted(data["channel_registry"])[0]
    if chaos:
        channel = data["channel_registry"][channel_id]
        impact = {key: _extreme(meta) for key, meta in spec.impact_keys.items()}
        channel.setdefault("generator_impact", {})[spec.id] = impact
    return data, channel_id


def _freeze(mod, monkeypatch) -> None:
    """Make one cycle deterministic so chaos-on and chaos-off can be compared."""
    rnd = _Random(0)

    def _seeded(seed=None):
        return _Random(0 if seed is None else seed)

    monkeypatch.setattr(mod.random, "Random", _seeded)
    for name in ("random", "randint", "uniform", "choice", "choices", "sample", "randrange"):
        monkeypatch.setattr(mod.random, name, getattr(rnd, name))
    if hasattr(mod, "time"):
        monkeypatch.setattr(mod.time, "time", lambda: 1_700_000_000.0)


def _run_cycle(fn, client, scenario_data, chaos):
    stop = _OneCycle()
    errors: list[BaseException] = []

    def _target():
        try:
            kwargs = {"scenario_data": scenario_data}
            if chaos is not None:
                kwargs["chaos_controller"] = chaos
            fn(client, stop, **kwargs)
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=_target, daemon=True)
    thread.start()
    thread.join(25)
    if thread.is_alive():
        stop.set()
        thread.join(2)
        raise AssertionError("run() did not finish a single cycle")
    if errors:
        raise errors[0]


def _call_kwargs(fn, args, kwargs) -> dict:
    bound = inspect.signature(fn).bind(*args, **kwargs)
    bound.apply_defaults()
    return bound.arguments


@pytest.fixture(scope="module")
def scenario():
    from scenario_engine import get_scenario

    return get_scenario("space")


@pytest.mark.parametrize("gid", sorted(GENERATORS))
def test_generator_module_defines_run(gid):
    spec = GENERATORS[gid]
    assert (ROOT / (spec.module.replace(".", "/") + ".py")).is_file()
    assert re.search(r"^def run\(", _source(spec), re.M), f"{spec.module} has no run()"


@pytest.mark.parametrize("gid", sorted(GENERATORS))
def test_generator_reads_chaos_through_the_engine(gid):
    assert "resolve_active_chaos" in _source(GENERATORS[gid])


@pytest.mark.parametrize("gid", sorted(GENERATORS))
def test_every_declared_impact_key_is_read_in_code(gid):
    spec = GENERATORS[gid]
    src = _source(spec)
    unread = [k for k in spec.impact_keys if not re.search(rf"""['"]{re.escape(k)}['"]""", src)]
    assert not unread, f"{gid} declares impact keys it never reads: {unread}"


@pytest.mark.parametrize("gid", sorted(GENERATORS))
def test_run_matches_manager_signature(gid):
    """ServiceManager calls run(client, stop_event, *, scenario_data, chaos_controller)."""
    spec = GENERATORS[gid]
    fn = load_entrypoint(spec)
    sig = inspect.signature(fn)
    params = list(sig.parameters.values())
    assert [p.name for p in params[:2]] == ["client", "stop_event"]
    assert "scenario_data" in sig.parameters
    names = [p.name for p in params]
    if "chaos_controller" in names and "scenario_data" in names:
        assert names.index("scenario_data") < names.index("chaos_controller"), (
            f"{gid} takes chaos_controller before scenario_data"
        )
    if spec.wants_chaos:
        assert "chaos_controller" in sig.parameters
    assert spec.client in ("otlp", "es_bulk")
    extra = {"scenario_data": {}}
    if spec.wants_chaos:
        extra["chaos_controller"] = object()
    sig.bind(object(), threading.Event(), **extra)


@pytest.mark.parametrize("gid", sorted(GENERATORS))
def test_one_cycle_emits_for_declared_client_with_and_without_chaos(gid, scenario, monkeypatch):
    spec = GENERATORS[gid]
    for key in ("ELASTIC_URL", "ELASTICSEARCH_URL", "ELASTIC_API_KEY", "ELASTICSEARCH_API_KEY"):
        monkeypatch.delenv(key, raising=False)

    monkeypatch.setattr("app.telemetry.time.time", lambda: 1_700_000_000.0)

    fn = load_entrypoint(spec)
    mod = inspect.getmodule(fn)
    targets = [
        name for name, value in vars(mod).items()
        if callable(value) and getattr(value, "__name__", "") == "resolve_active_chaos"
    ]
    assert targets, f"{gid} does not import resolve_active_chaos"
    real = getattr(mod, targets[0])
    seen: list[dict] = []

    def _spy(*args, **kwargs):
        seen.append(_call_kwargs(real, args, kwargs))
        return real(*args, **kwargs)

    for name in targets:
        monkeypatch.setattr(mod, name, _spy)
    _freeze(mod, monkeypatch)

    quiet, _ = _scenario_data(scenario, spec, chaos=False)
    client_off = _RecordingES() if spec.client == "es_bulk" else _RecordingOTLP()
    _run_cycle(fn, client_off, quiet, None)
    assert client_off.payloads, f"{gid} emitted nothing with chaos off"
    send_names = _OTLP_SENDS if spec.client == "otlp" else _ES_SENDS
    assert all(item[0] in send_names for item in client_off.payloads)

    seen.clear()
    _freeze(mod, monkeypatch)
    hot, channel_id = _scenario_data(scenario, spec, chaos=True)
    client_on = _RecordingES() if spec.client == "es_bulk" else _RecordingOTLP()
    _run_cycle(fn, client_on, hot, _Chaos(channel_id) if spec.wants_chaos else None)
    assert client_on.payloads, f"{gid} emitted nothing with chaos on"

    if not spec.wants_chaos:
        return

    assert seen, f"{gid} never called resolve_active_chaos"
    for call in seen:
        assert call.get("generator_id") == spec.id
        assert call.get("chaos_mode") == spec.chaos_mode
        passed = call.get("impact_keys") or {}
        assert set(passed) == set(spec.impact_keys), (
            f"{gid} passed impact keys {sorted(passed)} but SPEC declares {sorted(spec.impact_keys)}"
        )
        assert call.get("chaos_controller") is not None

    if spec.impact_keys:
        assert client_off.payloads != client_on.payloads, (
            f"{gid} chaos-on emit matched chaos-off; declared impact keys were not applied"
        )


# ── a generator may only read chaos keys its SPEC declares ────────────────────
# The reverse of test_every_declared_impact_key_is_read_in_code. A channel can only set
# declared keys (verify_yaml_scenarios rejects the rest), so a read of an undeclared key
# is dead in practice, the drift that left the aws_rds tests on legacy aliases.

_CHANNEL_METADATA = {
    "channel_id", "channel_ids", "name", "subsystem", "error_type", "affected_services",
}
_CHAOS_NAMES = ("chaos", "active_chaos", "_host_chaos", "_active_chaos", "_trace_chaos")
_CHAOS_ALIAS = re.compile(r"\b(\w+)\s*=\s*\(?\s*\w*chaos\w*\s+or\s+\{\}\s*\)?")

# Known leftovers, each a candidate for a follow-up (map the read to a declared key, or
# declare it). Empty today; it must never gain an entry.
_ALLOWED_UNDECLARED: dict[str, set[str]] = {}

# Keys from chaos_controller.get_infra_spikes() (kubernetes_otel, host_metrics, traces)
# belong to the controller and the Chaos UI sliders, not to SPEC.impact_keys, and are
# out of scope here.


def _chaos_keys_read(src: str) -> set[str]:
    names = set(_CHAOS_NAMES) | set(_CHAOS_ALIAS.findall(src))
    keys = set(re.findall(r"""impact_value\(\s*[\w.\[\]"']+\s*,\s*"(\w+)\"""", src))
    for name in names:
        keys |= set(re.findall(rf"""\b{name}\.get\(\s*"(\w+)\"""", src))
        keys |= set(re.findall(rf"""\(\s*{name}\s+or\s+\{{\}}\s*\)\.get\(\s*"(\w+)\"""", src))
    return keys


@pytest.mark.parametrize("gid", sorted(GENERATORS))
def test_generator_reads_only_declared_chaos_keys(gid):
    spec = GENERATORS[gid]
    undeclared = _chaos_keys_read(_source(spec)) - set(spec.impact_keys) - _CHANNEL_METADATA
    unexpected = undeclared - _ALLOWED_UNDECLARED.get(gid, set())
    assert not unexpected, (
        f"{gid} reads chaos keys its SPEC does not declare: {sorted(unexpected)}; "
        "declare them in SPEC.impact_keys or read the declared key instead"
    )


def test_the_undeclared_key_allow_list_has_no_stale_entries():
    for gid, allowed in _ALLOWED_UNDECLARED.items():
        spec = GENERATORS[gid]
        still = _chaos_keys_read(_source(spec)) - set(spec.impact_keys) - _CHANNEL_METADATA
        assert allowed <= still, f"{gid}: remove fixed keys from _ALLOWED_UNDECLARED: {sorted(allowed - still)}"


# ── infra-spike keys belong to the controller ─────────────────────────────────

@pytest.mark.parametrize("gid", sorted(GENERATORS))
def test_infra_spike_reads_use_keys_the_controller_defines(gid):
    from app.chaos.controller import INFRA_SPIKE_DEFAULTS

    src = _source(GENERATORS[gid])
    names = set(re.findall(r"(\w+)\s*=\s*\(?\s*chaos_controller\.get_infra_spikes\(\)", src))
    read = set()
    for name in names:
        read |= set(re.findall(rf"\b{name}\.get\(\s*\"(\w+)\"", src))
    assert read <= set(INFRA_SPIKE_DEFAULTS), (
        f"{gid} reads infra spike keys the controller does not define: "
        f"{sorted(read - set(INFRA_SPIKE_DEFAULTS))}"
    )
    if gid in {"kubernetes_otel", "host_metrics", "traces"}:
        assert read, f"{gid}: expected to find infra spike reads (scan pattern drifted?)"

