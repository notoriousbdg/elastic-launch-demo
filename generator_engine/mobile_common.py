"""Shared helpers for client-side (mobile and browser RUM) generators.

The Android, iOS and RUM content packages read logs and traces the way the
OpenTelemetry client SDKs send them: one resource per device or browser, an
instrumentation scope per signal source (``scope.name`` is queried by the RUM
dashboard), log records carrying ``eventName`` and spans with explicit start and
end times so a session reads in order on the event timeline.

``OTLPClient.send_logs`` / ``send_traces_multi`` pin a single demo scope and
stamp every record with the send time, so these helpers build the OTLP payload
themselves and hand it to ``client._send`` (one POST per signal per tick).

No ``data_stream.*`` resource attributes are set: like a real SDK, the data
lands in ``logs-generic.otel-default`` / ``traces-generic.otel-default``, which
is what the dashboards query.
"""

from __future__ import annotations

import random
from typing import Any, Sequence

from app.telemetry import SCHEMA_URL, _format_attributes

SPAN_KIND_INTERNAL = 1
SPAN_KIND_SERVER = 2
SPAN_KIND_CLIENT = 3

STATUS_UNSET = 0
STATUS_OK = 1
STATUS_ERROR = 2

_SEVERITY_NUMBER = {"TRACE": 1, "DEBUG": 5, "INFO": 9, "WARN": 13, "ERROR": 17, "FATAL": 21}

MS = 1_000_000  # nanoseconds per millisecond


def hex_id(rng: random.Random, n_bytes: int) -> str:
    """Random lowercase hex id (trace ids are 16 bytes, span ids 8)."""
    return f"{rng.getrandbits(n_bytes * 8):0{n_bytes * 2}x}"


def uuid4(rng: random.Random) -> str:
    """A version-4 shaped UUID drawn from ``rng`` (deterministic under a seed)."""
    h = f"{rng.getrandbits(128):032x}"
    return f"{h[:8]}-{h[8:12]}-4{h[13:16]}-{'89ab'[int(h[16], 16) % 4]}{h[17:20]}-{h[20:32]}"


def weighted_choice(rng: random.Random, items: Sequence[Any], weights: Sequence[float]) -> Any:
    return rng.choices(list(items), weights=list(weights), k=1)[0]


def pairs(raw: Any, default: list) -> list[tuple[Any, float]]:
    """Normalise a ``[[value, weight], ...]`` (or ``[value, ...]``) config list."""
    if not isinstance(raw, list) or not raw:
        raw = default
    out: list[tuple[Any, float]] = []
    for item in raw:
        if isinstance(item, (list, tuple)) and len(item) == 2:
            out.append((item[0], float(item[1])))
        else:
            out.append((item, 1.0))
    return out


def resource(attrs: dict[str, Any]) -> dict[str, Any]:
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


def log_record(
    time_ns: int,
    *,
    body: str = "",
    severity: str = "INFO",
    attributes: dict[str, Any] | None = None,
    event_name: str | None = None,
    trace_id: str | None = None,
    span_id: str | None = None,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "timeUnixNano": str(int(time_ns)),
        "observedTimeUnixNano": str(int(time_ns)),
        "severityText": severity,
        "severityNumber": _SEVERITY_NUMBER.get(severity.upper(), 9),
        "body": {"stringValue": body},
    }
    if event_name:
        record["eventName"] = event_name
    if trace_id:
        record["traceId"] = trace_id
    if span_id:
        record["spanId"] = span_id
    if attributes:
        record["attributes"] = _format_attributes(attributes)
    return record


def span(
    name: str,
    *,
    trace_id: str,
    span_id: str,
    start_ns: int,
    duration_ms: float,
    parent_span_id: str | None = None,
    kind: int = SPAN_KIND_INTERNAL,
    attributes: dict[str, Any] | None = None,
    status_code: int = STATUS_UNSET,
    status_message: str = "",
) -> dict[str, Any]:
    end_ns = int(start_ns) + max(1, int(duration_ms * MS))
    out: dict[str, Any] = {
        "traceId": trace_id,
        "spanId": span_id,
        "name": name,
        "kind": kind,
        "startTimeUnixNano": str(int(start_ns)),
        "endTimeUnixNano": str(end_ns),
        "status": {"code": status_code},
    }
    if status_message:
        out["status"]["message"] = status_message
    if parent_span_id:
        out["parentSpanId"] = parent_span_id
    if attributes:
        out["attributes"] = _format_attributes(attributes)
    return out


class SignalBatch:
    """Collects records per (resource, scope) and sends one OTLP payload."""

    def __init__(self) -> None:
        self._resources: dict[int, dict[str, Any]] = {}
        self._groups: dict[tuple[int, str, str], list[dict[str, Any]]] = {}

    def add(self, res: dict[str, Any], scope: tuple[str, str], record: dict[str, Any]) -> None:
        key = id(res)
        self._resources[key] = res
        self._groups.setdefault((key, scope[0], scope[1]), []).append(record)

    def __len__(self) -> int:
        return sum(len(v) for v in self._groups.values())

    def records(self) -> list[dict[str, Any]]:
        return [r for recs in self._groups.values() for r in recs]

    def grouped(self) -> list[tuple[dict[str, Any], dict[str, str], list[dict[str, Any]]]]:
        return [
            (self._resources[key], {"name": name, "version": version}, recs)
            for (key, name, version), recs in self._groups.items()
        ]

    def _payload(self, top: str, scoped: str, items: str) -> dict[str, Any]:
        by_resource: dict[int, list[dict[str, Any]]] = {}
        for (key, name, version), recs in self._groups.items():
            by_resource.setdefault(key, []).append(
                {"scope": {"name": name, "version": version}, items: recs}
            )
        return {
            top: [
                {"resource": self._resources[key], scoped: scopes}
                for key, scopes in by_resource.items()
            ]
        }

    def logs_payload(self) -> dict[str, Any]:
        return self._payload("resourceLogs", "scopeLogs", "logRecords")

    def traces_payload(self) -> dict[str, Any]:
        return self._payload("resourceSpans", "scopeSpans", "spans")


def send_logs(client: Any, batch: SignalBatch, label: str) -> None:
    if len(batch):
        client._send(f"{client.endpoint}/v1/logs", batch.logs_payload(), label)


def send_traces(client: Any, batch: SignalBatch, label: str) -> None:
    if len(batch):
        client._send(f"{client.endpoint}/v1/traces", batch.traces_payload(), label)


def attr_map(record: dict[str, Any]) -> dict[str, Any]:
    """Decode an OTLP attribute list back to a dict (tests and dry-runs)."""
    out: dict[str, Any] = {}
    for item in record.get("attributes", []):
        value = item["value"]
        if "intValue" in value:
            out[item["key"]] = int(value["intValue"])
        elif "doubleValue" in value:
            out[item["key"]] = float(value["doubleValue"])
        elif "boolValue" in value:
            out[item["key"]] = bool(value["boolValue"])
        else:
            out[item["key"]] = value.get("stringValue")
    return out


# ── Mobile fleet model ───────────────────────────────────────────────────────


class Installation:
    """One app install on one device. Resource attributes are fixed for its life."""

    __slots__ = ("app", "version", "build_id", "installation_id", "device", "os_version", "resource", "attrs")

    def __init__(self, *, app: str, version: str, build_id: str, installation_id: str,
                 device: dict[str, Any], os_version: str, attrs: dict[str, Any]):
        self.app = app
        self.version = version
        self.build_id = build_id
        self.installation_id = installation_id
        self.device = device
        self.os_version = os_version
        self.attrs = attrs
        self.resource = resource(attrs)


class Session:
    __slots__ = ("session_id", "install", "ticks_left", "screen", "new", "ticks")

    def __init__(self, session_id: str, install: Installation, ticks_left: int):
        self.session_id = session_id
        self.install = install
        self.ticks_left = ticks_left
        self.screen: str | None = None
        self.new = True
        self.ticks = 0


class MobileFleet:
    """Installations per app and the sessions currently open on them.

    Each tick ``advance`` tops the open sessions up to ``target_active`` (new
    sessions are marked ``new`` so the caller emits an app start) and returns
    the open sessions. ``end`` closes one (session end or crash).
    """

    def __init__(self, installs: list[Installation], *, target_active: int,
                 min_ticks: int = 3, max_ticks: int = 18):
        self.installs = installs
        self.target_active = max(1, int(target_active))
        self.min_ticks = min_ticks
        self.max_ticks = max_ticks
        self.sessions: dict[str, Session] = {}

    def advance(self, rng: random.Random) -> list[Session]:
        for sess in self.sessions.values():
            sess.new = False
            sess.ticks += 1
        busy = {s.install.installation_id for s in self.sessions.values()}
        idle = [i for i in self.installs if i.installation_id not in busy]
        rng.shuffle(idle)
        want = self.target_active + rng.randint(-2, 2)
        while len(self.sessions) < want and idle:
            inst = idle.pop()
            sess = Session(uuid4(rng), inst, rng.randint(self.min_ticks, self.max_ticks))
            self.sessions[sess.session_id] = sess
        return list(self.sessions.values())

    def end(self, sess: Session) -> None:
        self.sessions.pop(sess.session_id, None)

    def expire(self) -> list[Session]:
        """Count down session lifetimes and return the ones that just ended."""
        done = []
        for sess in list(self.sessions.values()):
            sess.ticks_left -= 1
            if sess.ticks_left <= 0:
                done.append(sess)
                self.end(sess)
        return done


def build_installations(
    rng: random.Random,
    *,
    apps: list[str],
    versions: list[tuple[str, float]],
    devices: list[dict[str, Any]],
    os_versions: list[tuple[str, float]],
    per_app: int,
    base_attrs: dict[str, Any],
    device_attrs: Any,
    build_id: Any,
) -> list[Installation]:
    """Create ``per_app`` installs per app with a weighted device / OS / version mix.

    ``device_attrs(device, os_version)`` returns the platform resource attributes;
    ``build_id(app, version)`` the stable build id for an app release.
    """
    installs: list[Installation] = []
    dev_weights = [float(d.get("weight", 1.0)) for d in devices]
    for app in apps:
        for _ in range(max(1, int(per_app))):
            version = weighted_choice(rng, [v for v, _ in versions], [w for _, w in versions])
            device = weighted_choice(rng, devices, dev_weights)
            os_version = weighted_choice(rng, [v for v, _ in os_versions], [w for _, w in os_versions])
            inst_id = uuid4(rng)
            bid = build_id(app, version)
            attrs = dict(base_attrs)
            attrs.update({
                "service.name": app,
                "service.version": version,
                "app.installation.id": inst_id,
                "app.build_id": bid,
            })
            attrs.update(device_attrs(device, os_version))
            installs.append(Installation(
                app=app, version=version, build_id=bid, installation_id=inst_id,
                device=device, os_version=os_version, attrs=attrs,
            ))
    return installs
