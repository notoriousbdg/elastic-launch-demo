"""Abstract base class for all NOVA-7 simulated services."""

from __future__ import annotations

import logging
import random
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Optional

from app.chaos.protocol import ChaosSource
from app.config import ACTIVE_SCENARIO, CHANNEL_REGISTRY, MISSION_ID, SERVICES
from app.telemetry import OTLPClient
from app.trace_context import _trace_context_store

logger = logging.getLogger("nova7.services")

# Module-level topology cache — built once per scenario_id so all ~37 service
# constructors share the same deterministic host/pod/container identifiers without
# re-running the RNG sequence on each instantiation.
_topology_cache: dict[str, dict] = {}


def _scenario_topology(ctx) -> dict:
    """Return (cached) infra_topology for the given ScenarioContext."""
    topo = _topology_cache.get(ctx.scenario_id)
    if topo is None:
        from generator_engine.infra_topology import build_topology

        topo = build_topology(
            {
                "hosts": ctx.scenario.hosts,
                "k8s_clusters": ctx.scenario.k8s_clusters,
                "k8s_topology": getattr(ctx.scenario, "k8s_topology", None),
                "services": ctx.services,
                "namespace": ctx.namespace,
            }
        )
        _topology_cache[ctx.scenario_id] = topo
    return topo


def _get_scenario():
    from scenario_engine import get_scenario

    return get_scenario(ACTIVE_SCENARIO)


class BaseService(ABC):
    """Base class providing telemetry emission, threading, and fault injection hooks."""

    # Subclasses MUST set this
    SERVICE_NAME: str = ""

    # Class-level context — set by ServiceManager before batch-creating services.
    # Avoids changing 37 subclass constructors.
    _context = None  # ScenarioContext | None
    _context_lock = threading.Lock()

    @classmethod
    def set_context(cls, ctx):
        """Set the scenario context for the next batch of service instantiations."""
        cls._context = ctx

    @classmethod
    def clear_context(cls):
        """Clear the class-level context after batch creation."""
        cls._context = None

    def __init__(self, chaos_controller: ChaosSource, otlp_client: OTLPClient):
        self.chaos_controller = chaos_controller
        self.otlp = otlp_client

        # Capture context at creation time
        ctx = self.__class__._context
        if ctx:
            self._ctx = ctx
            self.service_cfg = ctx.services[self.SERVICE_NAME]
            self._channel_registry = ctx.channel_registry
            self._mission_id = ctx.mission_id
            self._namespace = ctx.namespace
        else:
            # Backward-compatible: module-level globals
            self._ctx = None
            self.service_cfg = SERVICES[self.SERVICE_NAME]
            self._channel_registry = CHANNEL_REGISTRY
            self._mission_id = MISSION_ID
            self._namespace = "nova7"

        # Derive topology-based infra attributes so that logs, per-service metrics,
        # and single-span traces carry the same host.name / container.id / k8s.pod.*
        # join keys as the trace, JVM-metrics, and k8s-metrics generators.  This is
        # what makes APM failed-transaction correlation → Logs → Infrastructure
        # resolve end-to-end without leaving any signal on a synthetic "{svc}-host".
        infra_attrs: dict | None = None
        if self._ctx:
            from generator_engine.infra_topology import get_resource_attrs

            topo = _scenario_topology(self._ctx)
            infra_attrs = dict(get_resource_attrs(topo, self.SERVICE_NAME))
            # Mirror the trace path: for k8s services, set service.instance.id to
            # the pod uid so APM instance grouping is consistent across all signals.
            entry = topo.get(self.SERVICE_NAME) or {}
            if entry.get("_service_instance_id"):
                infra_attrs["service.instance.id"] = entry["_service_instance_id"]

        self.resource = OTLPClient.build_resource(
            self.SERVICE_NAME,
            self.service_cfg,
            namespace=self._namespace,
            infra_attrs=infra_attrs,
        )

        # Derive nominal label from scenario (space="NOMINAL", others="NORMAL")
        if self._ctx:
            self._nominal_label = self._ctx.scenario.nominal_label
        else:
            self._nominal_label = _get_scenario().nominal_label

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._phase = "ACTIVE"
        self._status = self._nominal_label
        self._last_status_change = time.time()

    # ── Lifecycle ──────────────────────────────────────────────────────

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name=f"svc-{self.SERVICE_NAME}", daemon=True
        )
        self._thread.start()
        logger.info("Service %s started", self.SERVICE_NAME)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._thread = None
        logger.info("Service %s stopped", self.SERVICE_NAME)

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.generate_telemetry()
            except Exception:
                logger.exception("Error in %s telemetry loop", self.SERVICE_NAME)
            interval = random.uniform(1.5, 3.0)
            self._stop_event.wait(interval)

    # ── Abstract ───────────────────────────────────────────────────────

    @abstractmethod
    def generate_telemetry(self) -> None:
        """Produce one cycle of logs/metrics/traces. Called every 1.5-3s."""
        ...

    # ── Fault injection hooks ──────────────────────────────────────────

    def is_channel_active(self, channel: int) -> bool:
        """Check if a specific chaos channel is currently active."""
        return self.chaos_controller.is_active(channel)

    def _service_lag(self, ch_def: dict) -> float:
        """This service's extra onset delay for *ch_def* (``service_lag_seconds``), else 0."""
        lag = (ch_def.get("service_lag_seconds") or {}).get(self.SERVICE_NAME, 0)
        try:
            return max(0.0, float(lag or 0))
        except (TypeError, ValueError):
            return 0.0

    def _onset_reached(self, channel: int, ch_def: dict) -> bool:
        """Symptoms start ``onset_delay_seconds`` after the trigger (schema 1.3).

        The change event that caused the fault is emitted right away (backdated by its
        ``offset_seconds``), so the cause always precedes the effect in the telemetry.
        A channel may also set ``service_lag_seconds: {service: seconds}``: that much
        extra delay for the named service only, so a dependent shows its symptoms after
        the component at fault instead of at the same moment.
        """
        delay = float(ch_def.get("onset_delay_seconds", 0) or 0) + self._service_lag(ch_def)
        if delay <= 0:
            return True
        getter = getattr(self.chaos_controller, "get_channel_elapsed", None)
        if getter is None:
            return True
        elapsed = getter(channel)
        return elapsed is not None and elapsed >= delay

    def get_active_channels_for_service(self) -> list[int]:
        """Return list of active channels that affect this service."""
        active = []
        for ch_id, ch_def in self._channel_registry.items():
            if self.SERVICE_NAME in ch_def["affected_services"]:
                if self.is_channel_active(ch_id) and self._onset_reached(ch_id, ch_def):
                    active.append(ch_id)
        return active

    def get_tapering_channels_for_service(self) -> list[int]:
        """Fixed channels whose fault lines are still fading out on this service.

        After a fix that rolls pods one at a time, the old pods keep failing for a
        little while: each cycle a channel is returned with the probability that is
        left of its ``remediation_effects.log_taper_seconds`` window (1 → 0).
        """
        tapering = getattr(self.chaos_controller, "tapering_channels", None)
        if tapering is None:
            return []
        out = []
        for ch_id, frac in (tapering() or {}).items():
            ch_def = self._channel_registry.get(ch_id) or {}
            if self.SERVICE_NAME in ch_def.get("affected_services", []) and random.random() < frac:
                out.append(ch_id)
        return out

    def get_triggered_channels_for_service(self) -> list[int]:
        """Active channels that name this service anywhere, onset reached or not."""
        out = []
        for ch_id, ch_def in self._channel_registry.items():
            named = self.SERVICE_NAME in ch_def.get("affected_services", []) or \
                self.SERVICE_NAME in ch_def.get("cascade_services", []) or any(
                    (evt or {}).get("service") == self.SERVICE_NAME
                    for evt in ch_def.get("infrastructure_events", []) or []
                )
            if named and self.is_channel_active(ch_id):
                out.append(ch_id)
        return out

    def get_cascade_channels_for_service(self) -> list[int]:
        """Return list of active channels where this service is in the cascade."""
        active = []
        for ch_id, ch_def in self._channel_registry.items():
            if self.SERVICE_NAME in ch_def.get("cascade_services", []):
                if self.is_channel_active(ch_id) and self._onset_reached(ch_id, ch_def):
                    active.append(ch_id)
        return active

    # ── Status ─────────────────────────────────────────────────────────

    def get_status(self) -> dict[str, Any]:
        active = self.get_active_channels_for_service()
        cascade = self.get_cascade_channels_for_service()
        if active:
            status = "CRITICAL"
        elif cascade:
            status = "WARNING"
        else:
            status = self._nominal_label
        self._status = status
        return {
            "service": self.SERVICE_NAME,
            "subsystem": self.service_cfg["subsystem"],
            "cloud_provider": self.service_cfg["cloud_provider"],
            "cloud_region": self.service_cfg["cloud_region"],
            "status": status,
            "phase": self._phase,
            "active_faults": active,
            "cascade_faults": cascade,
        }

    # ── Telemetry helpers ──────────────────────────────────────────────

    def _base_log_attrs(self) -> dict[str, Any]:
        """Attributes added to every log record.

        Earlier versions stamped demo bookkeeping here (mission id, phase, status);
        those fields exist in no real component's logs, so they are gone. What remains
        are attributes a fix left behind (``remediation_effects.steady_attributes``).
        """
        return dict(self._steady_attributes())

    def _steady_attributes(self) -> dict[str, Any]:
        getter = getattr(self.chaos_controller, "get_fixed_channels", None)
        if getter is None:
            return {}
        out: dict[str, Any] = {}
        for ch_id in getter():
            effects = (self._channel_registry.get(ch_id) or {}).get("remediation_effects") or {}
            attrs = (effects.get("steady_attributes") or {}).get(self.SERVICE_NAME)
            if isinstance(attrs, dict):
                out.update(attrs)
        return out

    # ── Pinned platform identity (schema 1.3 k8s_topology) ─────────────

    def _platform_model(self):
        """Effective pinned platform (baseline ⊕ fixes ⊕ active fault_state), or None."""
        if not self._ctx:
            return None
        topo = getattr(self._ctx.scenario, "k8s_topology", None)
        if not topo:
            return None
        from generator_engine.platform_topology import effective_platform, get_platform

        base = get_platform(topo, self._namespace)
        if base is None:
            return None
        getter = getattr(self.chaos_controller, "get_platform_state", None)
        state = getter() if getter else None
        return effective_platform(base, state, time.time())

    def _pick_identity(self, scope: dict | None = None) -> dict[str, Any] | None:
        """Resource attributes for one log record from the pinned platform.

        Services mapped to a workload rotate across its pods (restricted to *scope*
        when given); ``log_identity: node`` services (kubelet, kernel) rotate across
        nodes. Returns None when the scenario has no pinned topology.
        """
        platform = self._platform_model()
        if platform is None:
            return None
        from generator_engine.platform_topology import node_resource_attrs, pod_resource_attrs

        spec = getattr(self, "_telemetry_spec", {}) or {}
        mode = spec.get("log_identity", "pod")
        if mode == "cluster":
            # Managed control plane (EKS): its logs carry the cluster, not a host or pod.
            c = platform.cluster((scope or {}).get("cluster"))
            return {
                "k8s.cluster.name": c.name,
                "cloud.provider": c.provider,
                "cloud.platform": c.platform,
                "cloud.region": c.region,
                "cloud.account.id": c.account_id,
            }
        if mode == "node":
            nodes = platform.nodes_in_scope(scope)
            if scope and scope.get("workloads") and not any(
                scope.get(k) for k in ("nodes", "node_pool", "az")
            ):
                # Workload-only scope: the nodes those workloads' pods run on.
                c = platform.cluster(scope.get("cluster"))
                hosting = {p.node for p in c.pods if p.workload in set(scope["workloads"])}
                nodes = [n for n in nodes if n.name in hosting] or nodes
            pools = spec.get("log_identity_pools")
            if not scope and pools:
                nodes = [n for n in nodes if n.pool in set(pools)]
            silent = self._silenced_nodes(platform)
            if silent:
                nodes = [n for n in nodes if n.name not in silent]
            if not nodes:
                return None
            node = random.choice(nodes)
            return node_resource_attrs(platform.cluster(node.cluster), node)
        found = platform.workload_for_service(self.SERVICE_NAME)
        if not found:
            return None
        cluster, wl = found
        pods = platform.pods_in_scope(scope, workload=wl.name) if scope else cluster.pods_of(wl.name)
        if not pods:
            pods = cluster.pods_of(wl.name)
        if not pods:
            return None
        pod = random.choice(pods)
        return pod_resource_attrs(cluster, pod, wl)

    def _silenced_nodes(self, platform) -> set[str]:
        """Nodes whose own logs can't leave them right now.

        A channel whose ``fault_scope`` sets ``node_logs: false`` breaks the log shipper
        on the nodes in its scope (e.g. fluent-bit crash-looping on a new pool), so no
        node-level log line (kubelet, kernel, container runtime) comes from them.
        """
        out: set[str] = set()
        for ch_id, ch_def in self._channel_registry.items():
            scope = ch_def.get("fault_scope") or {}
            if scope.get("node_logs") is False and self.is_channel_active(ch_id):
                out.update(n.name for n in platform.nodes_in_scope(scope))
        return out

    @staticmethod
    def _time_placeholders(ts: float | None = None) -> dict[str, Any]:
        """Timestamps a log body quotes in its component's own format, at *ts*."""
        import datetime as _dt

        t = _dt.datetime.fromtimestamp(ts if ts is not None else time.time(), _dt.timezone.utc)
        return {
            "klog_time": t.strftime("%m%d %H:%M:%S.") + f"{t.microsecond:06d}",
            "nginx_time": t.strftime("%Y/%m/%d %H:%M:%S"),
            "iso_now": t.strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond // 1000:03d}Z",
            "epoch_now": round(t.timestamp(), 3),
        }

    def _runtime_placeholders(self, identity: dict[str, Any] | None, scope: dict | None) -> dict[str, Any]:
        """``self_*`` identity values, live platform counts and timestamps for templates."""
        out: dict[str, Any] = self._time_placeholders()
        platform = self._platform_model()
        if platform is None:
            return out
        from generator_engine.platform_topology import identity_placeholders, platform_placeholders

        out.update(platform_placeholders(platform, (scope or {}).get("cluster")))
        if identity:
            out.update(identity_placeholders(identity))
        spec = getattr(self, "_telemetry_spec", {}) or {}
        if spec.get("log_identity") == "cluster" and scope and scope.get("workloads"):
            # A control-plane record (e.g. the audit record of an Event) is about an
            # object inside the fault scope: {self_pod}/{self_node} name that pod.
            from generator_engine.platform_topology import pod_resource_attrs

            c = platform.cluster(scope.get("cluster"))
            for wl_name in random.sample(list(scope["workloads"]), len(scope["workloads"])):
                wl = c.workloads.get(wl_name)
                pods = platform.pods_in_scope(scope, workload=wl_name) if wl else []
                if pods:
                    subject = identity_placeholders(pod_resource_attrs(c, random.choice(pods), wl))
                    out.update({k: v for k, v in subject.items() if k != "self_cluster"})
                    break
        return out

    def _resource_for(self, identity: dict[str, Any] | None) -> dict:
        if not identity:
            return self.resource
        cache = self.__dict__.setdefault("_identity_resources", {})
        key = (identity.get("k8s.pod.uid"), identity.get("k8s.node.name"),
               identity.get("container.image.tag"))
        res = cache.get(key)
        if res is None:
            infra = dict(identity)
            if identity.get("k8s.pod.uid"):
                infra["service.instance.id"] = identity["k8s.pod.uid"]
            res = OTLPClient.build_resource(
                self.SERVICE_NAME, self.service_cfg, namespace=self._namespace, infra_attrs=infra,
            )
            if len(cache) > 512:
                cache.clear()
            cache[key] = res
        return res

    def emit_log(
        self,
        level: str,
        message: str,
        extra_attrs: dict[str, Any] | None = None,
        event_name: str | None = None,
        channel: int | None = None,
        *,
        identity: dict[str, Any] | None = None,
        timestamp: float | None = None,
    ) -> None:
        attrs = self._base_log_attrs()
        if extra_attrs:
            attrs.update(extra_attrs)
        # Prefer the per-channel error trace context when a channel is supplied
        # (so fault logs link to an actual error trace, not last-seen-per-service).
        trace_id: str | None = None
        span_id: str | None = None
        if channel is not None:
            trace_id, span_id = _trace_context_store.get_for_channel(channel, self.SERVICE_NAME, namespace=self._namespace)
        if not trace_id:
            trace_id, span_id = _trace_context_store.get(self.SERVICE_NAME, namespace=self._namespace)
        record = self.otlp.build_log_record(
            severity=level,
            body=message,
            attributes=attrs,
            event_name=event_name,
            trace_id=trace_id,
            span_id=span_id,
        )
        if timestamp is not None:
            ts = str(int(timestamp * 1e9))
            record["timeUnixNano"] = ts
            record["observedTimeUnixNano"] = ts
        if identity is None:
            identity = self._pick_identity()
        self.otlp.send_logs(self._resource_for(identity), [record])

    def emit_metric(
        self,
        name: str,
        value: float,
        unit: str = "",
        extra_attrs: dict[str, Any] | None = None,
    ) -> None:
        attrs = extra_attrs or {}
        metric = self.otlp.build_gauge(name, value, unit, attrs)
        self.otlp.send_metrics(self.resource, [metric])

    @staticmethod
    def _safe_format(template: str, params: dict) -> str:
        """Format a template string, ignoring missing keys."""
        import string

        class SafeDict(dict):
            def __missing__(self, key):
                return f"{{{key}}}"

        return string.Formatter().vformat(template, (), SafeDict(params))

    def emit_fault_logs(self, channel: int, with_infra_events: bool = True) -> None:
        """Emit error logs matching the channel's exact error signature.

        Each record carries the channel's ``rca_clues`` for this service as log
        attributes (the root-cause evidence lives in logs, not only on spans) and, with
        a pinned topology, the identity of a pod or node inside ``fault_scope``. The
        message may quote that identity through ``{self_node}``, ``{self_pod_ip}``...
        """
        ch = self._channel_registry.get(channel)
        if not ch:
            return

        # Emit one-shot infrastructure events (idempotent — tracked internally)
        if with_infra_events:
            self.emit_infrastructure_events(channel)

        # Schema 1.3 error_services: only the component at fault emits the error
        # signature; other affected services (e.g. the API gateway, whose requests
        # fail as a result) log their own symptom lines from cascade_messages.
        error_services = ch.get("error_services")
        if error_services and self.SERVICE_NAME not in error_services:
            if (ch.get("cascade_messages") or {}).get(self.SERVICE_NAME):
                for _ in range(random.randint(1, 3)):
                    self.emit_cascade_logs(channel)
            return

        scope = ch.get("fault_scope") or None
        rng = random.Random()
        # Generate 2-4 error logs per cycle when channel is active
        for _ in range(random.randint(2, 4)):
            identity = self._pick_identity(scope)
            fault_params = self._generate_fault_params(channel)
            fault_params = {**self._runtime_placeholders(identity, scope), **fault_params}
            msg = self._safe_format(ch["error_message"], fault_params)
            stack = self._safe_format(ch.get("stack_trace") or "", fault_params)

            attrs: dict[str, Any] = {"error.type": ch["error_type"]}
            if stack.strip():
                attrs.update({
                    "exception.type": ch["error_type"],
                    "exception.message": msg,
                    "exception.stacktrace": stack,
                })
            attrs.update(self._rca_clue_attrs(channel, rng))
            self.emit_log("ERROR", msg, attrs, channel=channel, identity=identity)

    def _rca_clue_attrs(self, channel: int, rng: random.Random) -> dict[str, Any]:
        scenario = self._ctx.scenario if self._ctx else None
        if scenario is None or not hasattr(scenario, "get_rca_clues"):
            return {}
        try:
            return dict(scenario.get_rca_clues(channel, self.SERVICE_NAME, rng) or {})
        except Exception:
            logger.debug("rca clue resolution failed", exc_info=True)
            return {}

    # Neutral fallback when a channel has no cascade_messages for this service. It
    # describes the symptom a dependent sees without naming the cause.
    _CASCADE_FALLBACK = (
        "upstream dependency {subsystem} returning errors; retrying with backoff",
        "health check degraded: elevated error rate from {subsystem} dependency",
        "request to {subsystem} dependency failed; serving degraded response",
    )

    def emit_cascade_logs(self, channel: int) -> None:
        """Emit warning logs for cascading effects (not matching the SE query)."""
        ch = self._channel_registry.get(channel)
        if not ch:
            return
        custom = (ch.get("cascade_messages") or {}).get(self.SERVICE_NAME)
        params: dict[str, Any] = {"subsystem": ch.get("subsystem", "upstream")}
        identity = self._pick_identity()
        if custom:
            params.update(self._generate_fault_params(channel))
            params = {**self._runtime_placeholders(identity, ch.get("fault_scope")), **params}
            template = random.choice(list(custom))
            level = "WARN"
            if isinstance(template, dict):
                level = template.get("severity", "WARN")
                template = template.get("message", "")
        else:
            template = random.choice(self._CASCADE_FALLBACK)
            level = "WARN"
        self.emit_log(level, self._safe_format(str(template), params), None,
                      channel=channel, identity=identity)

    def emit_infrastructure_events(self, channel: int) -> None:
        """Emit one-shot infrastructure/audit events when a channel fires.

        Reads optional 'infrastructure_events' list from channel_registry entry.
        Each event: {body, severity, event_name, attributes, service (optional),
        offset_seconds (optional)}. A negative ``offset_seconds`` backdates the record
        relative to the trigger (the GitOps sync that caused the fault happened before
        the first error); a positive one holds it back until that much time has passed.
        Events are only emitted once per channel activation.
        """
        if not hasattr(self, "_infra_events_emitted"):
            self._infra_events_emitted: set[tuple[int, int]] = set()

        ch = self._channel_registry.get(channel)
        if not ch:
            return
        infra_events = ch.get("infrastructure_events", [])
        if not infra_events:
            return

        triggered_at = None
        elapsed = None
        getter = getattr(self.chaos_controller, "get_channel_triggered_at", None)
        if getter is not None:
            triggered_at = getter(channel)
        if triggered_at:
            elapsed = time.time() - triggered_at

        for idx, evt in enumerate(infra_events):
            # Only emit for this service (or if no service filter specified)
            evt_service = evt.get("service")
            if evt_service and evt_service != self.SERVICE_NAME:
                continue

            key = (channel, idx)
            if key in self._infra_events_emitted:
                continue
            offset = float(evt.get("offset_seconds", 0) or 0)
            if offset > 0 and (elapsed is None or elapsed < offset):
                continue
            self._infra_events_emitted.add(key)

            attrs: dict[str, Any] = {}
            if evt.get("attributes"):
                attrs.update(evt["attributes"])
            ts = (triggered_at + offset) if (triggered_at and offset) else None
            # Same pod/node as the fault logs: a containerd timeout for a NotReady node
            # must come from that node, not from a random one in the cluster.
            scope = ch.get("fault_scope") or None
            identity = self._pick_identity(scope)
            params = {**self._runtime_placeholders(identity, scope),
                      **self._time_placeholders(ts)}

            self.emit_log(
                evt.get("severity", "WARN"),
                self._safe_format(evt["body"], params),
                attrs,
                event_name=evt.get("event_name"),
                channel=channel,
                identity=identity,
                timestamp=ts,
            )

    def emit_pending_fix_events(self) -> None:
        """Emit queued one-shot log lines from applied fixes (audit, rollout events)."""
        drain = getattr(self.chaos_controller, "drain_events", None)
        if drain is None:
            return
        for evt in drain(self.SERVICE_NAME):
            params = {**self._runtime_placeholders(None, None),
                      **self._time_placeholders(evt.get("emit_at"))}
            self.emit_log(
                evt.get("severity", "INFO"),
                self._safe_format(evt["body"], params),
                evt.get("attributes") or None,
                timestamp=evt.get("emit_at"),
            )

    def reset_infrastructure_events(self, active: list[int] | None = None) -> None:
        """Forget emitted infrastructure events so they fire again on re-trigger.

        With *active*, only channels not in it are forgotten (call every cycle
        with the currently active channels); with None, everything is cleared.
        """
        emitted = getattr(self, "_infra_events_emitted", None)
        if not emitted:
            return
        if active is None:
            emitted.clear()
        else:
            self._infra_events_emitted = {k for k in emitted if k[0] in active}

    def _generate_fault_params(self, channel: int) -> dict[str, Any]:
        """Generate realistic random parameters for fault messages from scenario."""
        if self._ctx:
            return self._ctx.scenario.get_fault_params(channel)
        return _get_scenario().get_fault_params(channel)
