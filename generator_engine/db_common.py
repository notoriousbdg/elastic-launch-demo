"""Shared plumbing for database and infrastructure telemetry generators.

All functions that mysql_log_generator.py kept private are extracted here so
every new generator can reuse them.  The MySQL module imports from this module
instead of duplicating the code.
"""

from __future__ import annotations

import datetime
import os
import random
import time
from typing import Any

from app.chaos.protocol import ChaosSource
from app.telemetry import _format_attributes, _now_ns, SCHEMA_URL, SEVERITY_MAP
from generator_engine.identity import ScenarioIdentity
from generator_engine.scrape_clock import scrape_now_ns, scrape_start_ns


# ── Stable counter start times ────────────────────────────────────────────────
# OTLP cumulative counters identify a time series partly by startTimeUnixNano.
# A new start_ns on every process restart creates overlapping series in the
# alert window; ES|QL SUM(INCREASE(...)) then returns phantom >0 deltas even
# when MAX/MIN of the metric are both 0. Persist one start_ns for the PVC life.

_STABLE_START_NS: str | None = None
_STABLE_START_PATH = os.environ.get(
    "OTEL_COUNTER_START_NS_FILE", "/app/generators/.otel_counter_start_ns"
)


def stable_counter_start_ns() -> str:
    """Return a process/PVC-stable startTimeUnixNano string for cumulative sums."""
    global _STABLE_START_NS
    if _STABLE_START_NS:
        return _STABLE_START_NS
    try:
        with open(_STABLE_START_PATH, encoding="utf-8") as fh:
            raw = fh.read().strip()
        if raw.isdigit():
            _STABLE_START_NS = raw
            return _STABLE_START_NS
    except OSError:
        pass
    _STABLE_START_NS = str(int(time.time() * 1_000_000_000) - 60_000_000_000)
    try:
        os.makedirs(os.path.dirname(_STABLE_START_PATH) or ".", exist_ok=True)
        with open(_STABLE_START_PATH, "w", encoding="utf-8") as fh:
            fh.write(_STABLE_START_NS)
    except OSError:
        pass
    return _STABLE_START_NS


# ── Resource builder ──────────────────────────────────────────────────────────

def build_signal_resource(
    ns: str = "",
    signal_type: str = "metrics",
    dataset: str = "",
    service_name: str = "",
    service_version: str = "1.0.0",
    service_instance_id: str = "",
    host_name: str = "",
    cloud: dict[str, str] | None = None,
    extra_attrs: dict[str, Any] | None = None,
    *,
    identity: ScenarioIdentity | None = None,
    role: str | None = None,
    host_role: str | None = None,
    host_index: int | None = None,
    instance_index: int = 1,
) -> dict[str, Any]:
    """Build an OTLP resource for a generator signal.

    ``host.name``, ``service.name``, and ``service.instance.id`` always come
    from :class:`ScenarioIdentity`. Pass ``identity`` and ``role`` so the
    caller does not format those strings. Legacy keyword arguments are
    qualified through the same helper.

    Parameters
    ----------
    ns:
        Scenario namespace, for example ``"nova7"``. Ignored when ``identity``
        is set.
    signal_type:
        ``"logs"``, ``"metrics"``, or ``"traces"``.
    dataset:
        Data-stream dataset, for example ``"mysqlreceiver.otel"``.
    service_name:
        Role or already-qualified ``service.name``. Qualified when ``role``
        is omitted.
    service_version:
        ``service.version`` attribute value.
    service_instance_id:
        Already-qualified instance id. Derived from ``role`` when ``role`` is set.
    host_name:
        Role or already-qualified ``host.name``. Derived from ``role`` when
        ``role`` is set.
    cloud:
        Dict with keys ``provider``, ``platform``, ``region``,
        ``availability_zone``.
    extra_attrs:
        Any additional attributes to merge in, for example
        ``{"mysql.instance.endpoint": "host:3306"}``.
    identity:
        Scenario identity. Built from ``ns`` when omitted.
    role:
        Infrastructure role. When set, host, service, and instance id are
        taken from ``identity`` and the three name arguments are ignored.
    """
    if identity is None:
        identity = ScenarioIdentity(ns or "default")
    ns = identity.namespace
    cloud = cloud or {}
    if role:
        service_name = identity.service(role)
        host_name = identity.host(host_role or role, host_index)
        service_instance_id = identity.instance(role, instance_index)
    else:
        if service_name:
            service_name = identity.qualify(service_name)
        if host_name:
            host_name = identity.qualify(host_name)
        if service_instance_id:
            service_instance_id = identity.qualify(service_instance_id)
        elif service_name:
            service_instance_id = identity.instance(service_name)
    attrs: dict[str, Any] = {
        "service.name": service_name,
        "service.namespace": ns,
        "service.version": service_version,
        "service.instance.id": service_instance_id,
        "telemetry.sdk.language": "python",
        "telemetry.sdk.name": "opentelemetry",
        "telemetry.sdk.version": "1.24.0",
        "cloud.provider": cloud.get("provider", "gcp"),
        "cloud.platform": cloud.get("platform", "gcp_compute_engine"),
        "cloud.region": cloud.get("region", "us-central1"),
        "cloud.availability_zone": cloud.get("availability_zone", "us-central1-b"),
        "deployment.environment": f"production-{ns}",
        "host.name": host_name,
        "host.architecture": "amd64",
        "os.type": "linux",
        "data_stream.type": signal_type,
        "data_stream.dataset": dataset,
        "data_stream.namespace": "default",
    }
    if extra_attrs:
        attrs.update(extra_attrs)
    return {"attributes": _format_attributes(attrs), "schemaUrl": SCHEMA_URL}


# ── Metric point builders ─────────────────────────────────────────────────────

def build_cumulative_sum(
    name: str,
    unit: str,
    value: int | float,
    start_time_ns: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an OTLP cumulative monotonic sum data point.

    ``start_time_ns`` defaults to 60 seconds before the scrape so each call
    produces a valid (start < end) time range without requiring callers to
    manage a start timestamp. Every point of a scrape shares the scrape's
    timestamp (see :mod:`generator_engine.scrape_clock`).  Use :class:`CounterState` when you need a single
    consistent start timestamp across ticks.

    Integer values are emitted as ``asInt``; floats as ``asDouble`` so
    metrics like ``sqlserver.database.latency`` (value_type: double) are not
    truncated to zero by ``int(value)``.
    """
    now = scrape_now_ns()
    start = start_time_ns or scrape_start_ns()
    dp: dict[str, Any] = {
        "startTimeUnixNano": start,
        "timeUnixNano": now,
    }
    if isinstance(value, float):
        dp["asDouble"] = float(value)
    else:
        dp["asInt"] = str(int(value))
    if attributes:
        dp["attributes"] = _format_attributes(attributes)
    return {
        "name": name,
        "unit": unit,
        "sum": {"dataPoints": [dp], "aggregationTemporality": 2, "isMonotonic": True},
    }


def build_non_monotonic_sum(
    name: str,
    unit: str,
    value: int | float,
    start_time_ns: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an OTLP cumulative non-monotonic sum (UpDownCounter) data point.

    Use for receiver metrics declared as ``sum`` with ``monotonic: false``
    (for example open connection / file-descriptor counts). Dashboards that
    call ``LAST_OVER_TIME`` / ``MAX`` on these fields expect a sum mapping,
    not a gauge.
    """
    now = scrape_now_ns()
    start = start_time_ns or scrape_start_ns()
    dp: dict[str, Any] = {
        "startTimeUnixNano": start,
        "timeUnixNano": now,
    }
    if isinstance(value, float):
        dp["asDouble"] = float(value)
    else:
        dp["asInt"] = str(int(value))
    if attributes:
        dp["attributes"] = _format_attributes(attributes)
    return {
        "name": name,
        "unit": unit,
        "sum": {"dataPoints": [dp], "aggregationTemporality": 2, "isMonotonic": False},
    }


def build_gauge(
    name: str,
    unit: str,
    value: int | float,
    attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an OTLP gauge data point stamped with the current scrape's time."""
    now = scrape_now_ns()
    dp: dict[str, Any] = {"timeUnixNano": now}
    if isinstance(value, int):
        dp["asInt"] = str(value)
    else:
        dp["asDouble"] = float(value)
    if attributes:
        dp["attributes"] = _format_attributes(attributes)
    return {"name": name, "unit": unit, "gauge": {"dataPoints": [dp]}}


# ── Counter state ─────────────────────────────────────────────────────────────

class CounterState:
    """Base class for cumulative counter state.

    Holds the start timestamp so cumulative sums are valid across ticks.
    Subclasses call ``super().__init__(rng)`` and define their own counters.

    Simple one-counter usage (Prometheus-style generators)::

        counter = CounterState(rng)
        counter.tick(rng, drift=50)
        build_cumulative_sum("requests_total", "{req}", counter.value,
                             counter.start_time_ns)

    Named-counter subclass usage::

        class MySQLMetricState(CounterState):
            def __init__(self, rng):
                super().__init__(rng)
                self.query_count = rng.randint(500_000, 2_000_000)

            def tick(self, rng):
                self.query_count += rng.randint(20, 200)

    Pass ``state.start_time_ns`` as the ``start_time_ns`` argument of
    :func:`build_cumulative_sum` to get a consistent start timestamp.
    """

    def __init__(self, rng: random.Random, initial: int | None = None):
        self.start_time_ns: str = stable_counter_start_ns()
        # Used by generators that treat CounterState as a single cumulative counter.
        self.value: int = int(initial) if initial is not None else rng.randint(50_000, 5_000_000)

    def tick(self, rng: random.Random, drift: int = 0) -> None:
        """Advance the simple ``value`` counter by ``drift``.

        Subclasses that manage named counters should override this method.
        """
        if drift:
            self.value += max(0, int(drift))


# ── Diurnal load curve ────────────────────────────────────────────────────────

def business_load(now: datetime.datetime | None = None) -> float:
    """Return a load multiplier from 0.15 to 1.0 based on the time of day.

    Peaks on weekdays 09:00 to 17:00 UTC.  Weekends and nights have a floor
    of 0.15 (15% of peak) to simulate reduced but non-zero background activity.
    Multiply your baseline metric values by this factor for a realistic curve.
    """
    t = now or datetime.datetime.utcnow()
    hour = t.hour + t.minute / 60.0
    is_weekday = t.weekday() < 5  # Monday = 0, Friday = 4

    if not is_weekday:
        return 0.15

    if 9.0 <= hour < 17.0:
        # Full business hours: sine wave from 0.6 to 1.0
        phase = (hour - 9.0) / 8.0 * 3.14159
        return 0.6 + 0.4 * abs(1.0 - phase / 3.14159)
    elif 7.0 <= hour < 9.0:
        # Ramp up
        return 0.15 + 0.45 * (hour - 7.0) / 2.0
    elif 17.0 <= hour < 19.0:
        # Ramp down
        return 0.6 - 0.45 * (hour - 17.0) / 2.0
    else:
        return 0.15


# ── Chaos resolver ────────────────────────────────────────────────────────────

def resolve_active_chaos(
    chaos_controller: ChaosSource | None,
    channel_registry: dict,
    generator_id: str,
    chaos_mode: str = "any",
    impact_keys: dict | None = None,
) -> dict | None:
    """Return a resolved chaos dict for the generator, or None.

    Parameters
    ----------
    chaos_controller:
        The ChaosController instance from the demo.
    channel_registry:
        Dict of channel_id to channel data.
    generator_id:
        The generator id, for example ``"postgresql"``.
    chaos_mode:
        ``"any"`` (react to every active channel) or ``"explicit"`` (react
        only when the channel declares ``generator_impact.<generator_id>``).
    impact_keys:
        The impact_keys dict from the GeneratorSpec.  Used to apply defaults
        when a channel does not supply a specific key.

    Returns
    -------
    dict or None
        Resolved chaos parameters if a relevant channel is active, otherwise
        None.  Always includes ``"channel_id"``, ``"name"``, ``"subsystem"``,
        ``"error_type"``, and ``"affected_services"``.  Also includes any
        impact key values resolved from the channel or defaulted from the spec.
    """
    if not chaos_controller:
        return None

    # Merge generator_impact from *all* active channels for this generator.
    # First-match-only incorrectly zeroed sibling impact_keys when multiple
    # GT / multi-fault channels were active together.
    matched: list[tuple[int, dict, dict]] = []
    for ch_id in chaos_controller.get_active_channels():
        ch = channel_registry.get(ch_id)
        if not ch:
            continue

        gen_impact = ch.get("generator_impact", {}).get(generator_id)

        if chaos_mode == "explicit" and gen_impact is None:
            continue

        matched.append((ch_id, ch, gen_impact or {}))

    if not matched:
        return None

    primary_id, primary_ch, _ = matched[0]
    result: dict[str, Any] = {
        "channel_id": primary_id,
        "name": primary_ch.get("name"),
        "subsystem": primary_ch.get("subsystem"),
        "error_type": primary_ch.get("error_type"),
        "affected_services": primary_ch.get("affected_services", []),
        "channel_ids": [ch_id for ch_id, _, _ in matched],
    }

    raw_impact: dict[str, Any] = {}
    for _, _, impact in matched:
        raw_impact.update(impact)

    # Only apply SPEC defaults when this generator was explicitly targeted
    # (non-empty generator_impact).  Under chaos_mode="any", a foreign channel
    # matches with raw_impact={} — injecting every key's default (often 0.0)
    # makes `impact_value` treat healthy baselines as active faults
    # (e.g. ready_fraction=0.0 → Deployment unavailable / Failed pods).
    if impact_keys and raw_impact:
        from scenario_engine.fault_spec import resolve_dict

        defaulted = {
            k: raw_impact.get(k, v["default"])
            for k, v in impact_keys.items()
        }
        try:
            resolved = resolve_dict(defaulted)
            result.update(resolved)
        except Exception:
            result.update(defaulted)

    return result


def impact_value(chaos: dict | None, *keys: str, default=None):
    """Return the first present SPEC impact_key value from a resolved chaos dict."""
    if not chaos:
        return default
    for key in keys:
        if key in chaos and chaos[key] is not None:
            return chaos[key]
    return default


# ── OTel dataset name helper ──────────────────────────────────────────────────

def otel_dataset(name: str) -> str:
    """Return the data_stream.dataset value to pass to build_signal_resource.

    The Elastic OTLP intake endpoint appends ``.otel`` to the dataset name
    automatically when routing to a data stream.  Do NOT include ``.otel`` in
    the string you pass here.  ``GeneratorSpec.datasets`` continues to hold the
    final name with ``.otel`` because the deployer uses it to build index
    patterns.

    Example::

        resource = build_signal_resource(
            ...
            dataset=otel_dataset("mysqlreceiver"),
            ...
        )
        # routes to  metrics-mysqlreceiver.otel-default
    """
    return name


# ── CloudWatch metric builder ─────────────────────────────────────────────────

def build_cloudwatch_metric(
    aws_namespace: str,
    metric: str,
    value: int | float,
    dimensions: dict[str, Any],
    stat: str = "Average",
    cumulative: bool = False,
    unit: str = "{count}",
    start_time_ns: str | None = None,
) -> dict[str, Any]:
    """Build a CloudWatch-style OTLP metric data point.

    Constructs the full CloudWatch metric name
    ``amazonaws.com/AWS/<aws_namespace>/<metric>`` and merges ``dimensions``
    with the ``stat`` key before calling :func:`build_gauge` or
    :func:`build_cumulative_sum`.

    Parameters
    ----------
    aws_namespace:
        AWS service namespace without the ``AWS/`` prefix, for example
        ``"EC2"`` or ``"ApplicationELB"``.
    metric:
        Bare metric name, for example ``"CPUUtilization"``.
    value:
        Metric value.
    dimensions:
        CloudWatch dimension key-value pairs plus ``cloud.region`` and
        ``cloud.account.id``, for example
        ``{"InstanceId": "i-abc", "cloud.region": "us-east-1",
        "cloud.account.id": "123456789012"}``.
    stat:
        CloudWatch statistic to report (``"Average"``, ``"Sum"``,
        ``"Maximum"``, etc.).  Added to the attributes under the ``stat`` key.
    cumulative:
        When ``True`` emits a cumulative monotonic sum; when ``False`` (the
        default) emits a gauge.
    unit:
        OTLP unit string, for example ``"1"`` (dimensionless ratio),
        ``"By"`` (bytes), ``"{request}"`` (requests).
        Defaults to ``"{count}"``.
    start_time_ns:
        Required when ``cumulative=True``.  Timestamp of the counter start in
        nanoseconds as a string.  Use ``CounterState.start_time_ns``.
    """
    full_name = f"amazonaws.com/AWS/{aws_namespace}/{metric}"
    attrs: dict[str, Any] = dict(dimensions)
    # OOTB CloudWatch alert templates filter on Namespace / MetricName columns.
    attrs.setdefault("Namespace", f"AWS/{aws_namespace}")
    attrs.setdefault("MetricName", metric)
    attrs["stat"] = stat
    if cumulative:
        return build_cumulative_sum(full_name, unit, int(value), start_time_ns, attrs)
    if isinstance(value, int):
        return build_gauge(full_name, unit, value, attrs)
    return build_gauge(full_name, unit, float(value), attrs)


# ── Audit log record builder ──────────────────────────────────────────────────

def build_audit_log_record(
    actor: str,
    action: str,
    outcome: str,
    source_address: str,
    event_name: str,
    severity: str = "INFO",
    extra_attrs: dict[str, Any] | None = None,
    jitter_ns: int = 0,
) -> dict[str, Any]:
    """Build an OTLP log record for a cloud audit or security event.

    Produces a camelCase OTLP log record with formatted attributes, ready for
    :meth:`~app.telemetry.OTLPClient.send_logs_scoped`.  Common fields used by
    all Wave C cloud-audit generators are pre-populated; generator-specific
    fields go into ``extra_attrs``.

    Parameters
    ----------
    actor:
        Principal that performed the action, for example a user email, ARN,
        or service account.
    action:
        API method or event type, for example ``"s3:PutObject"`` or
        ``"Microsoft.Network/routeTables/read"``.
    outcome:
        ``"success"``, ``"failure"``, or ``"unknown"``.
    source_address:
        Client IP address or CIDR.
    event_name:
        OTLP event name, for example ``"aws.cloudtrail.event"``.
    severity:
        Log severity string: ``"INFO"``, ``"WARN"``, or ``"ERROR"``.
        ``"WARN"`` is used for authorization failures; ``"ERROR"`` for
        denied actions.
    extra_attrs:
        Additional attributes to merge, for example
        ``{"rpc.service": "s3", "aws.error.code": "AccessDenied"}``.
    jitter_ns:
        Optional negative jitter in nanoseconds so that a batch of records
        spreads across the last N seconds.  The record's ``timeUnixNano`` is
        set to ``now - jitter_ns``.
    """
    severity_upper = severity.upper()
    now = int(_now_ns())
    ts = str(now - jitter_ns)

    attrs: dict[str, Any] = {
        "actor": actor,
        "event.action": action,
        "outcome": outcome,
        "source.address": source_address,
    }
    if extra_attrs:
        attrs.update(extra_attrs)

    return {
        "timeUnixNano": ts,
        "observedTimeUnixNano": str(now),
        "severityNumber": SEVERITY_MAP.get(severity_upper, 9),
        "severityText": severity_upper,
        "eventName": event_name,
        "attributes": _format_attributes(attrs),
    }


# ── Structured body log builder ───────────────────────────────────────────────

def build_structured_body_log(
    fields: dict[str, Any],
    event_name: str,
    severity: str = "INFO",
    jitter_ns: int = 0,
    attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an OTLP log record with a structured (kvlist) body.

    Some content packages query ``body.structured.*`` fields in Elasticsearch
    rather than ``attributes.*``.  The Elastic OTLP intake maps an OTLP
    ``kvlistValue`` body to the ``body.structured`` **flattened** field, so
    Lens cannot terms/sum/filter on subfields.  Generators that need Lens
    aggregations should also put the same keys in ``attributes`` (passthrough
    keyword/long/geo_point).  Passthrough promotes those keys to top-level
    aliases.  For stock panels that still query ``body.structured.*``, remap
    that field from flattened → object via ``logs-otel@custom`` (see
    ``nginx_ingress_controller_otel``); do not edit package dashboards.

    Parameters
    ----------
    fields:
        Key-value pairs to place inside the structured body, for example
        ``{"http.request.method": "GET", "http.response.status_code": 200,
        "log.level": "info"}``.
    event_name:
        OTLP event name for the record.
    severity:
        Log severity string.
    jitter_ns:
        Optional negative jitter in nanoseconds.
    attributes:
        Optional OTLP log attributes (mapped to ``attributes.*``), for example
        ``{"event.name": "nginx_ingress_controller.access",
        "source.address": "203.0.113.10"}``.
    """
    severity_upper = severity.upper()
    now = int(_now_ns())
    ts = str(now - jitter_ns)

    values = []
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, bool):
            val_dict = {"boolValue": value}
        elif isinstance(value, int):
            val_dict = {"intValue": str(value)}
        elif isinstance(value, float):
            val_dict = {"doubleValue": value}
        else:
            val_dict = {"stringValue": str(value)}
        values.append({"key": key, "value": val_dict})

    record: dict[str, Any] = {
        "timeUnixNano": ts,
        "observedTimeUnixNano": str(now),
        "severityNumber": SEVERITY_MAP.get(severity_upper, 9),
        "severityText": severity_upper,
        "eventName": event_name,
        "body": {"kvlistValue": {"values": values}},
    }
    if attributes:
        record["attributes"] = _format_attributes(attributes)
    return record


# ── Multi-entity counter state ────────────────────────────────────────────────

class MultiEntityState:
    """Container that holds one :class:`CounterState` per entity.

    Use when a generator emits metrics for multiple independent entities, such
    as multiple containers, virtual machines, or database shards.  Each entity
    gets its own start timestamp and independent counter values.

    Usage::

        class VMState(CounterState):
            def __init__(self, rng):
                super().__init__(rng)
                self.cpu_usage = rng.uniform(5.0, 80.0)

            def tick(self, rng):
                self.cpu_usage = min(100.0, max(0.5,
                    self.cpu_usage + rng.uniform(-5, 5)))

        entities = MultiEntityState(["vm-1", "vm-2", "vm-3"], VMState, rng)

        for entity_id, state in entities.items():
            state.tick(rng)
            metrics.append(build_gauge(
                "vcenter.vm.cpu.utilization", "1", state.cpu_usage,
                {"vm.id": entity_id},
            ))
    """

    def __init__(
        self,
        entity_ids: list[str],
        state_class: type,
        rng: random.Random,
        **kwargs: Any,
    ) -> None:
        """
        Parameters
        ----------
        entity_ids:
            List of entity identifiers (container IDs, VM names, shard IDs).
        state_class:
            A subclass of :class:`CounterState` to instantiate for each entity.
        rng:
            Random generator used to seed each entity's initial state.
        **kwargs:
            Extra keyword arguments forwarded to the ``state_class``
            constructor after ``rng``.
        """
        self._states: dict[str, Any] = {
            eid: state_class(rng, **kwargs) for eid in entity_ids
        }

    def __iter__(self):
        return iter(self._states)

    def __len__(self) -> int:
        return len(self._states)

    def __getitem__(self, entity_id: str):
        return self._states[entity_id]

    def items(self):
        """Iterate over (entity_id, state) pairs."""
        return self._states.items()

    def values(self):
        """Iterate over states."""
        return self._states.values()

    def tick_all(self, rng: random.Random) -> None:
        """Advance all entity states by one scrape interval."""
        for state in self._states.values():
            state.tick(rng)
