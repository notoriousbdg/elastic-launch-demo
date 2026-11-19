"""Two scenarios must never share an identity value a dashboard can filter on.

Each generator runs one cycle for two scenarios and the values of its identity
fields are compared. A generator that hardcodes a host, service, account,
subscription, tenant, domain or endpoint name emits the same value for both and
fails here, however the literal is spelled. ``generator_engine/identity.py``
holds the helpers that produce scenario-unique values.
"""
import ast
import inspect
import ipaddress
from pathlib import Path

import pytest

from generator_engine.registry import GENERATORS, load_entrypoint
from tests.test_generator_contract import (
    _freeze,
    _RecordingES,
    _RecordingOTLP,
    _run_cycle,
    _scenario_data,
)

# Fields watched for every generator, on top of its ``SPEC.identity_fields``.
_ANCHOR_FIELDS = frozenset({
    "host.name",
    "host.id",
    "cloud.account.id",
    "cloud.account.name",
    "cloud.instance.id",
    "cloud.project.id",
    "azure.subscription_id",
    "azure.tenant_id",
    "url.domain",
    "server.address",
    "k8s.cluster.name",
})

# gid -> {field: reason}. A value shared on purpose goes here with the reason.
_KNOWN_SHARED: dict[str, dict[str, str]] = {
    "cockroachdb_otel": {
        "service.name": (
            "The cockroachdb_otel package's SLO templates and OOTB alert rules all filter "
            "service.name == \"cockroachdb\", the receiver pipeline's fixed name, so a "
            "scenario-scoped name matches none of them. host.name and service.instance.id "
            "stay scenario-unique."
        ),
    },
}

_SCENARIOS = ("space", "banking")


def _is_ip(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return False
    return True


def _scalar(value):
    """String form of an OTLP ``AnyValue`` or a plain scalar, else ``None``."""
    if isinstance(value, dict):
        for key in ("stringValue", "intValue"):
            if key in value:
                return str(value[key])
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    return str(value)


def _collect(obj, fields: frozenset, found: dict[str, set[str]], path: str = "") -> None:
    """Gather values of ``fields`` from OTLP attribute lists and nested or dotted docs."""
    if isinstance(obj, dict):
        key = obj.get("key")
        if isinstance(key, str) and "value" in obj and key in fields:
            value = _scalar(obj["value"])
            if value:
                found.setdefault(key, set()).add(value)
        for name, child in obj.items():
            child_path = f"{path}.{name}" if path else str(name)
            if child_path in fields:
                value = _scalar(child)
                if value:
                    found.setdefault(child_path, set()).add(value)
            _collect(child, fields, found, child_path)
    elif isinstance(obj, (list, tuple)):
        for child in obj:
            _collect(child, fields, found, path)


def _identity_values(gid: str, scenario_id: str, monkeypatch) -> dict[str, set[str]]:
    from scenario_engine import get_scenario

    spec = GENERATORS[gid]
    fields = frozenset(spec.identity_fields) | _ANCHOR_FIELDS
    fn = load_entrypoint(spec)
    _freeze(inspect.getmodule(fn), monkeypatch)

    data, _ = _scenario_data(get_scenario(scenario_id), spec, chaos=False)
    client = _RecordingES() if spec.client == "es_bulk" else _RecordingOTLP()
    _run_cycle(fn, client, data, None)

    found: dict[str, set[str]] = {}
    for _name, args, kwargs in client.payloads:
        _collect((args, kwargs), fields, found)
    return found


@pytest.mark.parametrize("gid", sorted(GENERATORS))
def test_identity_values_differ_between_scenarios(gid, monkeypatch):
    for key in ("ELASTIC_URL", "ELASTICSEARCH_URL", "ELASTIC_API_KEY", "ELASTICSEARCH_API_KEY"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr("app.telemetry.time.time", lambda: 1_700_000_000.0)

    first, second = (_identity_values(gid, sid, monkeypatch) for sid in _SCENARIOS)
    assert first, (
        f"{gid} emitted none of its identity fields {sorted(GENERATORS[gid].identity_fields)}; "
        "set SPEC.identity_fields to the fields its dashboards filter on"
    )

    allowed = _KNOWN_SHARED.get(gid, {})
    shared = {
        field: sorted(v for v in first[field] & second.get(field, set()) if not _is_ip(v))
        for field in first
        if field not in allowed
    }
    shared = {field: values for field, values in shared.items() if values}
    assert not shared, (
        f"{gid} emits the same identity value for {_SCENARIOS[0]} and {_SCENARIOS[1]}: {shared}. "
        "Build it with ScenarioIdentity (generator_engine/identity.py) instead of a literal."
    )


def test_known_shared_entries_name_real_generators():
    assert set(_KNOWN_SHARED) <= set(GENERATORS)


# ── static complement ─────────────────────────────────────────────────────────
# The runtime test only sees branches one cycle executes (the ES bulk path, for
# one, needs a configured client). This scan reads every ``generator.py`` and
# flags a plain string stored under an identity field, whichever branch it is in.

_GENERATORS_DIR = Path(__file__).resolve().parent.parent / "generators"
_LITERAL_FIELDS = _ANCHOR_FIELDS | {"service.name", "service.instance.id"}


def _constant_identity_values(source: str) -> list[tuple[int, str, str]]:
    hits = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Dict):
            continue
        for key, value in zip(node.keys, node.values):
            if (
                isinstance(key, ast.Constant)
                and key.value in _LITERAL_FIELDS
                and isinstance(value, ast.Constant)
                and isinstance(value.value, str)
            ):
                hits.append((value.lineno, key.value, value.value))
    return hits


def test_generators_store_no_constant_identity_values():
    found = [
        f"{path.parent.name}:{line} {field}={value!r}"
        for path in sorted(_GENERATORS_DIR.glob("*/generator.py"))
        for line, field, value in _constant_identity_values(path.read_text())
    ]
    assert not found, (
        "identity fields set to a literal; build them with ScenarioIdentity "
        f"(generator_engine/identity.py): {found}"
    )


def test_constant_identity_scan_flags_a_literal():
    assert _constant_identity_values('r = {"service.name": "nginx-proxy"}') == [
        (1, "service.name", "nginx-proxy")
    ]
    assert _constant_identity_values('r = {"service.name": ident.service("x")}') == []
