"""Chaos Controller — manages state for 20 independent fault channels."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any, Callable

from app.config import CHANNEL_REGISTRY, CHANNEL_TIMEOUT

if TYPE_CHECKING:
    from app.store import ChaosStore

logger = logging.getLogger("nova7.chaos")

# Channel states
STANDBY = "STANDBY"
ACTIVE = "ACTIVE"

# Who a fix's log lines name as the actor ({actor} in remediation_effects.log_events).
# Fallbacks only: a scenario's ops profile (ops.yaml `actors`) sets its own, passed to
# ChaosController(actors=...) and read back through default_actor / auto_actor.
DEFAULT_FIX_ACTOR = "opsctl/oncall"  # a person resolving from the Chaos UI
AUTO_REMEDIATOR_ACTOR = "automation/auto-remediator"  # the workflow, approval skipped


def with_actor(value: Any, actor: str) -> Any:
    """*value* with ``{actor}`` replaced (strings only)."""
    return value.replace("{actor}", actor) if isinstance(value, str) else value

# Maximum fault duration (seconds) — override via CHANNEL_TIMEOUT in .env
MAX_FAULT_DURATION = CHANNEL_TIMEOUT

# Infra spikes are controller-owned (Chaos UI sliders and a channel's infra_impact: block),
# separate from the per-generator SPEC.impact_keys. These are the only valid keys.
INFRA_SPIKE_DEFAULTS: dict[str, float] = {
    "cpu_pct": 0,
    "memory_pct": 0,
    "k8s_oom_intensity": 0,
    "k8s_pod_cpu_intensity": 0,
    "k8s_pod_memory_intensity": 0,
    "latency_multiplier": 1.0,
    # Channel augmentations (kubernetes_otel / host_metrics / traces). Toggles are 0/1.
    "node_disk_fill_pct": 0,
    "k8s_node_not_ready": 0,
    "k8s_pvc_fill_pct": 0,
    "k8s_crashloop": 0,
    "k8s_readiness_fail": 0,
    "k8s_liveness_fail": 0,
    "k8s_hpa_capped": 0,
    "k8s_daemonset_misscheduled": 0,
    "apm_error_rate": 0,
}


class ChaosController:
    """Thread-safe chaos channel state management."""

    def __init__(
        self,
        channel_registry: dict[int, dict[str, Any]] | None = None,
        chaos_store: ChaosStore | None = None,
        deployment_id: str = "",
        on_resolve: "Callable[[], None] | None" = None,
        actors: dict[str, str] | None = None,
    ):
        self._lock = threading.RLock()
        actors = actors or {}
        self.default_actor = str(actors.get("default") or DEFAULT_FIX_ACTOR)
        self.auto_actor = str(actors.get("auto") or AUTO_REMEDIATOR_ACTOR)
        # Latest applied opsctl flags per target ("kind/name" -> {flag: value}), for the
        # service DSL's `applied` (a change shows in telemetry whether or not it fixes).
        self._applied: dict[str, dict[str, Any]] = {}
        self._channel_registry = channel_registry or CHANNEL_REGISTRY
        self._store = chaos_store
        self._deployment_id = deployment_id
        # Called best-effort in a background thread when the last active
        # channel resolves (manual or auto-expire). Used to revert the APM
        # ML model to its baseline snapshot so iterative demo cycles always
        # start from the same clean baseline. Errors are swallowed — this is
        # purely a quality-of-life hook for repeat testing.
        self._on_resolve = on_resolve
        self._channels: dict[int, dict[str, Any]] = {}
        for ch_id in self._channel_registry:
            self._channels[ch_id] = {
                "state": STANDBY,
                "se_name": None,
                "session_id": None,
                "triggered_at": None,
                "resolved_at": None,
                "callback_url": "",
                "user_email": "",
            }
        self._infra_spikes: dict[str, float] = dict(INFRA_SPIKE_DEFAULTS)
        # Post-remediation platform state (schema 1.3): one row per channel whose fix was
        # applied — {channel, state, applied_at, transition_seconds}. Generators read the
        # merged view through get_platform_state(); empty for every channel without
        # remediation_effects, so older scenarios behave exactly as before.
        self._platform_rows: list[dict[str, Any]] = []
        # One-shot log lines a fix leaves behind (audit records, rollout events), drained
        # by the owning service once their emit_at time has passed.
        self._pending_events: list[dict[str, Any]] = []
        # channel -> indices of its remediation_effects.log_events that opsctl already
        # emitted as the audit record of a matching command (so they are not doubled).
        self._emitted_fix_events: dict[int, set[int]] = {}
        # channel -> (fixed_at, taper_seconds): fault log lines fade out after a fix
        # instead of stopping on the instant (pods roll one at a time).
        self._tapers: dict[int, tuple[float, float]] = {}
        # Channels whose fault auto-expired (CHANNEL_TIMEOUT) before anyone fixed it.
        # Expiry is not a fix, so a correct fix applied afterwards must still leave its
        # remediation_effects (reload lines, steady state) rather than only an audit line.
        self._expired_unfixed: set[int] = set()
        # Restore persisted state from SQLite
        self._restore_from_store()

    def _restore_from_store(self) -> None:
        """Hydrate in-memory channel state from SQLite on startup."""
        if not self._store or not self._deployment_id:
            return
        try:
            rows = self._store.get_all_channels(self._deployment_id)
            for row in rows:
                ch_id = row["channel"]
                if ch_id not in self._channels:
                    continue
                self._channels[ch_id]["state"] = row["state"]
                self._channels[ch_id]["se_name"] = row["se_name"]
                self._channels[ch_id]["session_id"] = row["session_id"]
                self._channels[ch_id]["triggered_at"] = row["triggered_at"]
                self._channels[ch_id]["resolved_at"] = row["resolved_at"]
                self._channels[ch_id]["callback_url"] = row["callback_url"] or ""
                self._channels[ch_id]["user_email"] = row["user_email"] or ""
            restored = sum(1 for r in rows if r["state"] == ACTIVE)
            if restored:
                logger.info("Restored %d active chaos channels from SQLite", restored)
                # Recalculate infra spikes from restored ACTIVE channels so that
                # CPU/memory/latency effects survive an app crash and restart.
                self._infra_spikes = {
                    k: (1.0 if k == "latency_multiplier" else 0.0)
                    for k in self._infra_spikes
                }
                for ch_id, ch in self._channels.items():
                    if ch["state"] == ACTIVE:
                        impact = self._channel_registry.get(ch_id, {}).get("infra_impact", {})
                        for key, val in impact.items():
                            if key in self._infra_spikes:
                                self._infra_spikes[key] = max(self._infra_spikes[key], float(val))
            self._platform_rows = list(self._store.get_platform_state(self._deployment_id))
            # Overlay any manually-set spike values persisted via the sliders
            persisted_spikes = self._store.get_spikes(self._deployment_id)
            if persisted_spikes:
                for key in self._infra_spikes:
                    if key in persisted_spikes and persisted_spikes[key] is not None:
                        self._infra_spikes[key] = max(
                            self._infra_spikes[key], float(persisted_spikes[key])
                        )
        except Exception:
            logger.exception("Failed to restore chaos channels from SQLite")

    def trigger(
        self,
        channel: int,
        se_name: str = "",
        callback_url: str = "",
        user_email: str = "",
        session_id: str = "",
    ) -> dict[str, Any]:
        if channel not in self._channel_registry:
            return {"error": f"Unknown channel {channel}"}

        with self._lock:
            ch = self._channels[channel]
            if ch["state"] == ACTIVE:
                return {"status": "already_active", "channel": channel}

            ch["state"] = ACTIVE
            ch["se_name"] = se_name
            ch["session_id"] = session_id
            ch["triggered_at"] = time.time()
            ch["resolved_at"] = None
            ch["callback_url"] = callback_url
            ch["user_email"] = user_email
            # Re-triggering a channel undoes its earlier fix, so the demo repeats from
            # the same baseline (e.g. the memory limit goes back to 1Gi).
            targets = _state_targets(self._channel_registry.get(channel) or {})
            dropped = [
                r for r in self._platform_rows
                if r["channel"] == channel
                or (r["channel"] < 0 and _state_touches(r["state"], targets))
            ]
            had_fix = bool(dropped)
            self._platform_rows = [r for r in self._platform_rows if r not in dropped]
            self._pending_events = [e for e in self._pending_events if e.get("channel") != channel]
            self._emitted_fix_events.pop(channel, None)
            self._tapers.pop(channel, None)
            self._expired_unfixed.discard(channel)

        if had_fix and self._store and self._deployment_id:
            for r in dropped:
                self._store.delete_platform_state(self._deployment_id, r["channel"])

        # Write-through to SQLite
        if self._store and self._deployment_id:
            self._store.upsert_channel(
                self._deployment_id,
                channel,
                state=ACTIVE,
                se_name=se_name,
                session_id=session_id,
                triggered_at=ch["triggered_at"],
                callback_url=callback_url,
                user_email=user_email,
            )

        ch_def = self._channel_registry[channel]

        # Auto-apply infrastructure spikes from channel definition
        infra_impact = ch_def.get("infra_impact")
        if infra_impact:
            with self._lock:
                for key, val in infra_impact.items():
                    if key in self._infra_spikes:
                        self._infra_spikes[key] = max(self._infra_spikes[key], float(val))

        logger.info(
            "CHAOS: Channel %d [%s] ACTIVATED (session=%s)",
            channel,
            ch_def["name"],
            session_id[:8] if session_id else "none",
        )
        return {
            "status": "triggered",
            "channel": channel,
            "name": ch_def["name"],
        }

    def resolve(
        self,
        channel: int,
        session_id: str = "",
        force: bool = False,
        user_email: str = "",
        apply_effects: bool = True,
        actor: str = "",
    ) -> dict[str, Any]:
        """Return *channel* to STANDBY.

        Every resolve path except auto-expiry represents the fix being made, so by
        default the channel's ``remediation_effects`` (if any) are applied: the changed
        config values persist in telemetry and the change log lines are queued.
        """
        if channel not in self._channel_registry:
            return {"error": f"Unknown channel {channel}"}

        with self._lock:
            ch = self._channels[channel]
            if ch["state"] == STANDBY:
                if channel not in self._expired_unfixed:
                    return {"status": "already_standby", "channel": channel}
                # Late fix of a fault that already auto-expired: nothing to return to
                # STANDBY, but the fix's effects still have to be recorded.
                self._expired_unfixed.discard(channel)
                late_fix = True
            else:
                late_fix = False
        if late_fix:
            ch_def = self._channel_registry[channel]
            logger.info("CHAOS: Channel %d [%s] fixed after auto-expiry", channel, ch_def["name"])
            if apply_effects:
                self.apply_remediation_effects(channel, actor=actor)
            return {"status": "resolved", "channel": channel, "name": ch_def["name"]}

        with self._lock:
            ch = self._channels[channel]

            # Session ownership check (skip if force=True or channel has no owner)
            if not force and ch["session_id"]:
                session_matches = session_id and ch["session_id"] == session_id
                email_matches = user_email and ch.get("user_email") and ch["user_email"] == user_email
                if not session_matches and not email_matches:
                    return {"error": "session_mismatch", "channel": channel}

            ch["state"] = STANDBY
            ch["session_id"] = None
            ch["resolved_at"] = time.time()
            ch["callback_url"] = ""
            ch["user_email"] = ""

            # Only clear infra spikes when the last active channel resolves
            any_active = any(c["state"] == ACTIVE for c in self._channels.values())
            if not any_active:
                self._infra_spikes = {
                    k: (1.0 if k == "latency_multiplier" else 0) for k in self._infra_spikes
                }

        # Write-through to SQLite
        if self._store and self._deployment_id:
            self._store.resolve_channel(self._deployment_id, channel, ch["resolved_at"])

        ch_def = self._channel_registry[channel]
        logger.info("CHAOS: Channel %d [%s] RESOLVED", channel, ch_def["name"])
        if apply_effects:
            self.apply_remediation_effects(channel, actor=actor)

        # If this was the last active channel, fire the on_resolve hook
        # (e.g. revert the APM ML model to baseline) in a background thread.
        if not any_active:
            self._fire_on_resolve_async()

        return {
            "status": "resolved",
            "channel": channel,
            "name": ch_def["name"],
        }

    def _expire_stale(self) -> None:
        """Auto-resolve channels that have been active longer than MAX_FAULT_DURATION.

        Must be called while holding self._lock.
        """
        now = time.time()
        expired_ids: list[int] = []
        for ch_id, ch in self._channels.items():
            if ch["state"] != ACTIVE:
                continue
            if ch["triggered_at"] and (now - ch["triggered_at"]) >= MAX_FAULT_DURATION:
                ch["state"] = STANDBY
                ch["session_id"] = None
                ch["resolved_at"] = now
                ch["callback_url"] = ""
                ch["user_email"] = ""
                expired_ids.append(ch_id)
                self._expired_unfixed.add(ch_id)
                ch_def = self._channel_registry[ch_id]
                logger.info(
                    "CHAOS: Channel %d [%s] AUTO-EXPIRED after %ds",
                    ch_id,
                    ch_def["name"],
                    MAX_FAULT_DURATION,
                )
        # Persist expiry to SQLite
        if expired_ids and self._store and self._deployment_id:
            self._store.expire_channels(self._deployment_id, MAX_FAULT_DURATION)

        # Mirror the manual-resolve spike-clearing semantics: when an auto-expire
        # leaves no active channels, clear infra spikes back to baseline. Without
        # this, CPU/memory/latency_multiplier stay elevated indefinitely after a
        # silent timeout — confusing for demos where the user expects "back to
        # green" once the fault is over.
        if expired_ids:
            any_active = any(c["state"] == ACTIVE for c in self._channels.values())
            if not any_active:
                self._infra_spikes = {
                    k: (1.0 if k == "latency_multiplier" else 0) for k in self._infra_spikes
                }
                self._fire_on_resolve_async()

    def _fire_on_resolve_async(self) -> None:
        """Run the on_resolve hook in a daemon thread. Best-effort: any
        exception is swallowed and logged. Avoids blocking chaos UI calls on
        an HTTP-bound ML revert."""
        if not self._on_resolve:
            return
        cb = self._on_resolve
        def _run():
            try:
                cb()
            except Exception as exc:
                logger.warning("chaos on_resolve hook failed (non-fatal): %s", exc)
        threading.Thread(target=_run, name="chaos-on-resolve", daemon=True).start()

    def is_active(self, channel: int) -> bool:
        with self._lock:
            self._expire_stale()
            ch = self._channels.get(channel)
            return ch is not None and ch["state"] == ACTIVE

    def get_status(self) -> dict[str, Any]:
        with self._lock:
            self._expire_stale()
            result = {}
            for ch_id, ch_state in self._channels.items():
                ch_def = self._channel_registry[ch_id]
                result[ch_id] = {
                    "name": ch_def["name"],
                    "subsystem": ch_def["subsystem"],
                    "state": ch_state["state"],
                    "triggered_at": ch_state["triggered_at"],
                    "session_id": ch_state["session_id"],
                    "user_email": ch_state.get("user_email", ""),
                    "affected_services": ch_def["affected_services"],
                    "description": ch_def["description"],
                }
            return result

    def get_channel_status(self, channel: int) -> dict[str, Any]:
        if channel not in self._channel_registry:
            return {"error": f"Unknown channel {channel}"}
        with self._lock:
            ch_state = self._channels[channel]
            ch_def = self._channel_registry[channel]
            return {
                "channel": channel,
                "name": ch_def["name"],
                "subsystem": ch_def["subsystem"],
                "state": ch_state["state"],
                "se_name": ch_state["se_name"],
                "triggered_at": ch_state["triggered_at"],
                "resolved_at": ch_state["resolved_at"],
                "callback_url": ch_state.get("callback_url", ""),
                "user_email": ch_state.get("user_email", ""),
                "affected_services": ch_def["affected_services"],
                "cascade_services": ch_def["cascade_services"],
                "error_type": ch_def["error_type"],
                "sensor_type": ch_def["sensor_type"],
                "vehicle_section": ch_def["vehicle_section"],
                "description": ch_def["description"],
            }

    def get_channel_metadata(self, channel: int) -> dict[str, str]:
        """Return callback_url and user_email for a channel."""
        with self._lock:
            ch = self._channels.get(channel)
            if ch is None:
                return {"callback_url": "", "user_email": ""}
            return {
                "callback_url": ch.get("callback_url", ""),
                "user_email": ch.get("user_email", ""),
            }

    def validate_session(self, session_id: str, user_email: str = "") -> list[int]:
        """Return list of channel IDs owned by this session_id or user_email."""
        with self._lock:
            self._expire_stale()
            return [
                ch_id
                for ch_id, ch in self._channels.items()
                if ch["state"] == ACTIVE and (
                    ch["session_id"] == session_id
                    or (user_email and ch.get("user_email") and ch["user_email"] == user_email)
                )
            ]

    def get_unfixed_channels(self) -> list[int]:
        """Active channels plus those whose fault auto-expired without a fix."""
        with self._lock:
            self._expire_stale()
            return [
                ch_id for ch_id, ch in self._channels.items()
                if ch["state"] == ACTIVE or ch_id in self._expired_unfixed
            ]

    def get_active_channels(self) -> list[int]:
        with self._lock:
            return [
                ch_id for ch_id, ch in self._channels.items() if ch["state"] == ACTIVE
            ]

    # ── Timing (schema 1.3 onset_delay_seconds / ramp_seconds) ───────────

    def get_channel_elapsed(self, channel: int) -> float | None:
        """Seconds since *channel* was triggered, or None when it is not active."""
        with self._lock:
            ch = self._channels.get(channel)
            if ch is None or ch["state"] != ACTIVE or not ch["triggered_at"]:
                return None
            return max(0.0, time.time() - ch["triggered_at"])

    def get_channel_triggered_at(self, channel: int) -> float | None:
        with self._lock:
            ch = self._channels.get(channel)
            if ch is None or ch["state"] != ACTIVE:
                return None
            return ch["triggered_at"]

    # ── Post-remediation platform state ──────────────────────────────────

    def apply_remediation_effects(self, channel: int, actor: str = "") -> bool:
        """Record *channel*'s remediation_effects. Returns False when it declares none.

        *actor* is who made the fix; it replaces ``{actor}`` in the fix's log lines
        (``opsctl/claude-code`` for opsctl, the auto-remediator for the workflow).
        Log lines that opsctl already emitted for this fix (see
        :meth:`mark_fix_event_emitted`) are not queued again.
        """
        ch_def = self._channel_registry.get(channel) or {}
        effects = ch_def.get("remediation_effects")
        if not isinstance(effects, dict) or not effects:
            return False
        actor = (actor or self.default_actor).strip() or self.default_actor
        now = time.time()
        state = effects.get("state") or {}
        transition = float(effects.get("transition_seconds", 0) or 0)
        row = {
            "channel": channel,
            "state": state if isinstance(state, dict) else {},
            "applied_at": now,
            "transition_seconds": transition,
            # A fix that plays out over time starts from where the fault left things
            # (e.g. 6 of 24 nodes already upgraded), not from the pre-fault baseline.
            "from_state": dict(ch_def.get("fault_state") or {}),
        }
        with self._lock:
            already = self._emitted_fix_events.pop(channel, set())
        events = []
        # one change id for the fix, on every record the fixer made (the lines naming
        # {actor}: audit, CloudTrail, or a service's own "applied by" line); follow-on
        # records by controllers such as the node manager's evictions are not part of
        # the change. opsctl adds its own id.
        change_id = "chg-" + uuid.uuid4().hex[:8]
        for idx, evt in enumerate(effects.get("log_events") or []):
            if not isinstance(evt, dict) or not evt.get("body") or idx in already:
                continue
            body = with_actor(evt["body"], actor)
            if "{actor}" in evt["body"] and "change_id=" not in body:
                body += f" change_id={change_id}"
            events.append({
                "channel": channel,
                "service": evt.get("service"),
                "severity": evt.get("severity", "INFO"),
                "body": body,
                "attributes": {k: with_actor(v, actor) for k, v in (evt.get("attributes") or {}).items()},
                "emit_at": now + max(0.0, float(evt.get("offset_seconds", 0) or 0)),
            })
        taper = float(effects.get("log_taper_seconds", 0) or 0)
        with self._lock:
            self._platform_rows = [r for r in self._platform_rows if r["channel"] != channel]
            self._platform_rows.append(row)
            self._pending_events.extend(events)
            if taper > 0:
                self._tapers[channel] = (now, taper)
        if self._store and self._deployment_id:
            self._store.upsert_platform_state(
                self._deployment_id, channel, row["state"], now, transition,
            )
        logger.info("CHAOS: Channel %d remediation effects applied", channel)
        return True

    def get_platform_state(self) -> dict[str, Any]:
        """Fix rows plus the fault_state of channels whose onset has passed.

        ``{"fixes": [{channel, state, applied_at, transition_seconds}],
           "faults": [{channel, state, triggered_at, onset_at}]}``
        """
        with self._lock:
            self._expire_stale()
            fixes = [dict(r) for r in self._platform_rows]
            faults = []
            now = time.time()
            for ch_id, ch in self._channels.items():
                if ch["state"] != ACTIVE:
                    continue
                ch_def = self._channel_registry.get(ch_id) or {}
                fstate = ch_def.get("fault_state")
                if not isinstance(fstate, dict) or not fstate:
                    continue
                onset_at = (ch["triggered_at"] or now) + float(ch_def.get("onset_delay_seconds", 0) or 0)
                faults.append({
                    "channel": ch_id,
                    "state": fstate,
                    "triggered_at": ch["triggered_at"],
                    "onset_at": onset_at,
                })
        return {"fixes": fixes, "faults": faults}

    def get_fixed_channels(self) -> list[int]:
        with self._lock:
            return [r["channel"] for r in self._platform_rows if r["channel"] > 0]

    def get_since_fix(self) -> dict[int, float]:
        """Seconds since each fixed channel's fix was applied (the DSL's ``since_fix``)."""
        now = time.time()
        with self._lock:
            return {r["channel"]: max(0.0, now - float(r.get("applied_at") or now))
                    for r in self._platform_rows if r["channel"] > 0}

    def record_applied(self, target: str, verb: str, flags: dict[str, Any]) -> None:
        """Remember an applied opsctl change (the DSL's ``applied``)."""
        entry = {k: v for k, v in (flags or {}).items()}
        entry["_verb"] = verb
        with self._lock:
            self._applied[target] = {**self._applied.get(target, {}), **entry}

    def get_applied(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {t: dict(f) for t, f in self._applied.items()}

    def apply_change_state(self, state: dict[str, Any], transition_seconds: float = 0.0) -> None:
        """Record the effect of one applied platform change (an opsctl patch, scale...).

        A change takes effect whether or not it completes a fix: patching a memory limit
        changes the limit even while the fault continues. Each change is its own row
        (negative id, never a channel) so its rollout keeps its own timing.
        """
        if not state:
            return
        now = time.time()
        with self._lock:
            ids = [r["channel"] for r in self._platform_rows if r["channel"] < 0]
            row = {"channel": min(ids, default=0) - 1, "state": state,
                   "applied_at": now, "transition_seconds": float(transition_seconds)}
            self._platform_rows.append(row)
        if self._store and self._deployment_id:
            self._store.upsert_platform_state(
                self._deployment_id, row["channel"], state, now, row["transition_seconds"],
            )

    def mark_fix_event_emitted(self, channel: int, index: int) -> None:
        """opsctl emitted *channel*'s fix log line *index* as its audit record."""
        with self._lock:
            self._emitted_fix_events.setdefault(channel, set()).add(index)

    def taper_fraction(self, channel: int) -> float:
        """Share (1 → 0) of *channel*'s fault log rate still emitted after its fix."""
        with self._lock:
            entry = self._tapers.get(channel)
            if not entry:
                return 0.0
            fixed_at, span = entry
            left = 1.0 - (time.time() - fixed_at) / span
            if left <= 0:
                self._tapers.pop(channel, None)
                return 0.0
            ch = self._channels.get(channel) or {}
            if ch.get("state") == ACTIVE:  # re-triggered: the fault is back for real
                self._tapers.pop(channel, None)
                return 0.0
            return left

    def tapering_channels(self) -> dict[int, float]:
        """Channels whose fault lines are still fading out after a fix."""
        with self._lock:
            chans = list(self._tapers)
        return {c: f for c in chans if (f := self.taper_fraction(c)) > 0}

    def drain_events(self, service: str) -> list[dict[str, Any]]:
        """Pop queued one-shot fix events for *service* whose emit time has passed."""
        now = time.time()
        out: list[dict[str, Any]] = []
        with self._lock:
            keep = []
            for e in self._pending_events:
                if e.get("service") == service and e["emit_at"] <= now:
                    out.append(e)
                else:
                    keep.append(e)
            self._pending_events = keep
        return out

    def queue_event(
        self,
        service: str,
        body: str,
        severity: str = "INFO",
        attributes: dict[str, Any] | None = None,
        delay_seconds: float = 0,
        channel: int | None = None,
    ) -> None:
        """Queue a one-shot log line for *service*, emitted after *delay_seconds*."""
        evt = {
            "channel": channel,
            "service": service,
            "severity": severity,
            "body": body,
            "attributes": dict(attributes or {}),
            "emit_at": time.time() + max(0.0, float(delay_seconds or 0)),
        }
        with self._lock:
            self._pending_events.append(evt)

    def reset_platform_state(self) -> None:
        """Forget every applied fix (``POST /api/ops/reset``)."""
        with self._lock:
            self._platform_rows = []
            self._pending_events = []
            self._emitted_fix_events = {}
            self._tapers = {}
            self._expired_unfixed = set()
            self._applied = {}
        if self._store and self._deployment_id:
            self._store.delete_platform_state(self._deployment_id)

    def set_infra_spikes(self, spikes: dict[str, float]) -> None:
        with self._lock:
            for key in self._infra_spikes:
                if key in spikes:
                    self._infra_spikes[key] = float(spikes[key])
        if self._store and self._deployment_id:
            self._store.upsert_spikes(self._deployment_id, self._infra_spikes)

    def get_infra_spikes(self) -> dict[str, float]:
        with self._lock:
            return dict(self._infra_spikes)


def _state_targets(ch_def: dict[str, Any]) -> set[tuple[str, str]]:
    """(kind, name) of the workloads / node pools a channel's fault or fix sets."""
    out: set[tuple[str, str]] = set()
    blocks = [ch_def.get("fault_state") or {}, (ch_def.get("remediation_effects") or {}).get("state") or {}]
    for block in blocks:
        for gen_state in block.values():
            if not isinstance(gen_state, dict):
                continue
            out.update(("workloads", w) for w in (gen_state.get("workloads") or {}))
            out.update(("node_pools", p) for p in (gen_state.get("node_pools") or {}))
    return out


def _state_touches(state: dict[str, Any], targets: set[tuple[str, str]]) -> bool:
    for gen_state in (state or {}).values():
        if not isinstance(gen_state, dict):
            continue
        for kind in ("workloads", "node_pools"):
            if any((kind, name) in targets for name in (gen_state.get(kind) or {})):
                return True
    return False
