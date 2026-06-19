"""Generic YAML-driven BaseService: YamlService.

Replaces all bespoke ``generate_telemetry()`` implementations.  One class,
used by every scenario, instantiated by ``ServiceManager`` exactly like the
old per-service Python subclasses.

Usage (via yaml_scenario.get_service_classes)
----------------------------------------------
    spec = load_yaml(services_dir / f"{svc_name}.yaml")
    cls = type(cls_name, (YamlService,), {
        "SERVICE_NAME": svc_name,
        "_telemetry_spec": spec,
    })

Spec schema summary (services/<svc>.yaml)
-----------------------------------------
    service: <svc-key>              # informational (must match SERVICE_NAME)
    emit_fault_logs: true           # default true; false = cascade-only service
    kpi_emitter: false              # default false

    constants:                      # named lists/dicts available in all steps
      SENSORS: {key: {lo, hi, metric, unit}, ...}
      ROUTES:  [[src, dst], ...]

    state:                          # per-instance mutable state
      _count:     {init: 0}         # counter
      _last_ts:   {init: now}       # throttle timestamp (float)
      _idx:       {init: 0}         # round-robin index
      _counters:                    # per-key mutable dict
        init_per_key:
          keys_from: STREAMS        # constant name holding the key list
          spec: {randint: [1000000, 5000000]}

    per_active_channel_steps:       # executed per-active-channel in addition to
      - log: ...                    #   emit_fault_logs (sensor_validator pattern)
          with_remediation_meta: true

    steps:                          # main telemetry body
      - sample: {var: value_spec, ...}
      - metric: {name: str, value: value_spec, unit: str}
      - log:    {level: str, message: str, attrs: {k: value_spec, ...}}
      - for_each: {in: src, as: var | [k, v], steps: [...]}
      - every:  {seconds: n, state_key: k, steps: [...], else: [...]}
      - counter: {state_key: k, by: value_spec}
      - incr_key: {state_key: k, key: value_spec, by: value_spec}

Value spec forms
----------------
Scalar int/float/str/bool/None → pass through
{var: name}                   → env lookup
{expr: "python_expr"}         → safe_eval (supports rand.*, time.time(), arithmetic)
{channel_field: key}          → ch_<key> from per_active_channel_steps env
{if_active: spec, else: spec} → nominal/degraded branch
{choice: list_or_const_name}  → rand.choice (dict name → choice of keys)
{randint: [lo, hi]}           → rand.randint
{uniform: [lo, hi], round: n} → rand.uniform (optional round)
{random: true, round: n}      → rand.random (optional round)
{gauss: [mu, sig], clamp: [lo, hi], round: n} → rand.gauss with optional clamp/round
{sample: {from: const, k: n}} → rand.sample
{format: "tmpl", k: spec}     → str.format (sub-specs resolved first)
"""
from __future__ import annotations

import json
import random
import time
from typing import Any

from app.services.base_service import BaseService
from app.services.expr import _SAFE_TIME, make_safe_rand, safe_eval
from scenario_engine.fault_spec import resolve


class YamlService(BaseService):
    """A :class:`~app.services.base_service.BaseService` driven by a YAML spec.

    Every scenario subclass is created dynamically by
    :meth:`~scenarios.yaml_scenario.YamlScenario.get_service_classes`:

        cls = type(cls_name, (YamlService,), {
            "SERVICE_NAME": svc_name,
            "_telemetry_spec": spec,
        })

    ``_telemetry_spec`` is the parsed ``services/<svc>.yaml`` dict.
    """

    SERVICE_NAME: str = ""
    _telemetry_spec: dict = {}

    def __init__(self, chaos_controller, otlp_client) -> None:
        super().__init__(chaos_controller, otlp_client)
        self._rng = random.Random()
        self._safe_rand = make_safe_rand(self._rng)
        self._state: dict[str, Any] = {}
        self._init_state()

    # ── State initialisation ──────────────────────────────────────────────────

    def _init_state(self) -> None:
        spec = self._telemetry_spec
        constants = spec.get("constants", {})
        for key, init_spec in spec.get("state", {}).items():
            self._state[key] = self._eval_init(init_spec, constants)

    def _eval_init(self, init_spec: Any, constants: dict) -> Any:
        """Evaluate a single ``state:`` init spec."""
        if not isinstance(init_spec, dict):
            return init_spec

        # Simple value: {init: 0} / {init: now}
        if "init" in init_spec:
            v = init_spec["init"]
            if v == "now":
                return time.time()
            return v

        # Per-key counter dict: {init_per_key: {keys_from: CONST, spec: value_spec}}
        if "init_per_key" in init_spec:
            cfg = init_spec["init_per_key"]
            keys_from = cfg["keys_from"]
            key_spec = cfg["spec"]
            keys = constants.get(keys_from, [])
            if isinstance(keys, dict):
                keys = list(keys.keys())
            return {k: resolve(key_spec, self._rng) for k in keys}

        return init_spec

    # ── Main telemetry loop ───────────────────────────────────────────────────

    def generate_telemetry(self) -> None:
        spec = self._telemetry_spec

        active_channels = self.get_active_channels_for_service()
        cascade_channels = self.get_cascade_channels_for_service()

        # Standard fault logs (all affected-service channels)
        if spec.get("emit_fault_logs", True):
            for ch in active_channels:
                self.emit_fault_logs(ch)

        # Cascade logs (always, for cascade-service channels)
        for ch in cascade_channels:
            self.emit_cascade_logs(ch)

        # Per-active-channel custom steps (sensor_validator pattern)
        per_ch_steps = spec.get("per_active_channel_steps", [])
        if per_ch_steps and active_channels:
            for ch in active_channels:
                ch_data = self._channel_registry.get(ch, {})
                ch_env = self._make_channel_env(ch, ch_data, active_channels, cascade_channels)
                self._exec_steps(per_ch_steps, ch_env)

        # Build shared env and execute main steps
        env = self._make_base_env(active_channels, cascade_channels)
        self._exec_steps(spec.get("steps", []), env)

        # KPI emission (implicitly gated by scenario.executive_kpi_emitter_service_name)
        if spec.get("kpi_emitter", False):
            from scenario_engine.yaml_scenario import emit_executive_business_metrics_if_eligible
            emit_executive_business_metrics_if_eligible(self)

    # ── Environment builders ──────────────────────────────────────────────────

    def _make_base_env(self, active_channels: list, cascade_channels: list) -> dict[str, Any]:
        env: dict[str, Any] = {
            # Random + time
            "rand": self._safe_rand,
            "time": _SAFE_TIME,
            # Fault state
            "active": bool(active_channels),
            "active_channels": active_channels,
            "cascade_channels": cascade_channels,
            "cascade_count": len(cascade_channels),
            # Service phase (set externally via set_phase())
            "_phase": self._phase,
        }
        # All constants (lists, dicts, sensor tables, phase message tables, …)
        env.update(self._telemetry_spec.get("constants", {}))
        # All current state (counters, timestamps, per-key dicts, …)
        env.update(self._state)
        return env

    def _make_channel_env(
        self,
        channel: int,
        ch_data: dict,
        active_channels: list,
        cascade_channels: list,
    ) -> dict[str, Any]:
        """Extend base env with per-channel fields for per_active_channel_steps."""
        env = self._make_base_env(active_channels, cascade_channels)
        env["channel"] = channel
        # Expose every channel-registry field as ch_<key>
        for k, v in ch_data.items():
            env[f"ch_{k}"] = v
        return env

    # ── Step executor ─────────────────────────────────────────────────────────

    def _exec_steps(self, steps: list, env: dict) -> None:
        """Dispatch each step in *steps* using the shared *env*."""
        for step in steps:
            if not isinstance(step, dict):
                continue
            if "sample" in step:
                self._step_sample(step["sample"], env)
            elif "metric" in step:
                self._step_metric(step["metric"], env)
            elif "log" in step:
                self._step_log(step["log"], env)
            elif "for_each" in step:
                self._step_for_each(step["for_each"], env)
            elif "every" in step:
                self._step_every(step["every"], env)
            elif "counter" in step:
                self._step_counter(step["counter"], env)
            elif "incr_key" in step:
                self._step_incr_key(step["incr_key"], env)

    # ── Individual step handlers ──────────────────────────────────────────────

    def _step_sample(self, spec: dict, env: dict) -> None:
        """Bind per-cycle variables into *env*.  Variables are resolved in order,
        so later vars may reference earlier ones via ``{var: name}`` or ``{expr: ...}``.
        """
        for var_name, val_spec in spec.items():
            env[var_name] = self._resolve(val_spec, env)

    def _step_metric(self, spec: dict, env: dict) -> None:
        name = self._resolve_str(spec["name"], env)
        value = float(self._resolve(spec["value"], env))
        unit = self._resolve_str(spec.get("unit", ""), env)
        attrs_spec = spec.get("attrs", {})
        attrs = self._resolve_attrs(attrs_spec, env) if attrs_spec else None
        self.emit_metric(name, value, unit, attrs)

    def _step_log(self, spec: dict, env: dict) -> None:
        level = str(spec.get("level", "INFO"))
        message = self._resolve_str(spec["message"], env)
        attrs_spec = spec.get("attrs", {})
        attrs = self._resolve_attrs(attrs_spec, env) if attrs_spec else {}

        channel = env.get("channel")
        with_remed = bool(spec.get("with_remediation_meta", False))

        if with_remed and channel is not None:
            # Mirror the remediation-metadata injection in emit_fault_logs
            meta = self.chaos_controller.get_channel_metadata(channel)
            if meta.get("callback_url"):
                attrs["chaos.callback_url"] = meta["callback_url"]
            if meta.get("user_email"):
                attrs["chaos.user_email"] = meta["user_email"]
            ev_name: str | None = None
            if meta.get("callback_url") or meta.get("user_email"):
                ev_name = json.dumps({
                    "callback_url": meta.get("callback_url", ""),
                    "user_email": meta.get("user_email", ""),
                    "deployment_id": self._ctx.scenario_id if self._ctx else "",
                })
            self.emit_log(level, message, attrs or None, event_name=ev_name, channel=channel)
        else:
            self.emit_log(level, message, attrs or None)

    def _step_for_each(self, spec: dict, env: dict) -> None:
        """Iterate over a list/dict source, binding loop vars into the shared env.

        The env is **not** copied — loop variables are bound into the same dict
        and cleaned up after iteration so that counter/incr_key steps inside
        the loop correctly accumulate into the shared env.
        """
        items = self._resolve_iterable(spec["in"], env)
        as_var = spec["as"]
        sub_steps = spec["steps"]
        is_list_unpack = isinstance(as_var, list)
        loop_var_names: list[str] = as_var if is_list_unpack else [as_var]

        for item in items:
            if is_list_unpack:
                for i, vname in enumerate(as_var):
                    # item may be a tuple, list, or plain value
                    env[vname] = item[i] if hasattr(item, "__getitem__") else item
            else:
                env[as_var] = item
            self._exec_steps(sub_steps, env)

        # Remove loop vars so they don't bleed into subsequent top-level steps
        for vname in loop_var_names:
            env.pop(vname, None)

    def _step_every(self, spec: dict, env: dict) -> None:
        """Execute *steps* at most once every *seconds*; run *else* otherwise.

        The throttle timestamp is stored in ``self._state[state_key]`` and
        mirrored into *env* so subsequent steps can see the refreshed value.
        """
        seconds = float(spec["seconds"])
        state_key = str(spec["state_key"])
        last_ts = self._state.get(state_key, 0.0)
        if time.time() - last_ts > seconds:
            # Refresh env from latest state before executing the throttled block
            env.update(self._state)
            self._exec_steps(spec.get("steps", []), env)
            self._state[state_key] = time.time()
            env[state_key] = self._state[state_key]
        elif "else" in spec:
            self._exec_steps(spec["else"], env)

    def _step_counter(self, spec: dict, env: dict) -> None:
        """Increment ``self._state[state_key]`` and mirror the update into *env*."""
        state_key = str(spec["state_key"])
        by = self._resolve(spec.get("by", 1), env)
        new_val = self._state.get(state_key, 0) + by
        self._state[state_key] = new_val
        env[state_key] = new_val

    def _step_incr_key(self, spec: dict, env: dict) -> None:
        """Increment a per-key counter inside a dict stored in ``self._state``."""
        state_key = str(spec["state_key"])
        key = self._resolve(spec["key"], env)
        by = self._resolve(spec.get("by", 1), env)
        d = self._state.get(state_key, {})
        d[key] = d.get(key, 0) + by
        self._state[state_key] = d
        env[state_key] = d

    # ── Value resolution ──────────────────────────────────────────────────────

    def _resolve(self, spec: Any, env: dict) -> Any:  # noqa: PLR0911,PLR0912
        """Resolve a value spec to a concrete value.

        Scalars and non-DSL dicts pass through unchanged.  DSL dicts are
        dispatched by their first recognised key.
        """
        if not isinstance(spec, dict):
            return spec

        # ── env lookup ─────────────────────────────────────────────────────
        if "var" in spec:
            key = spec["var"]
            if key in env:
                return env[key]
            if key in self._state:
                return self._state[key]
            raise KeyError(f"YamlService: var {key!r} not found in env or state")

        # ── safe expression ────────────────────────────────────────────────
        if "expr" in spec:
            return safe_eval(str(spec["expr"]), env)

        # ── channel field (only valid in per_active_channel_steps) ─────────
        if "channel_field" in spec:
            return env.get(f"ch_{spec['channel_field']}")

        # ── nominal/degraded branch ────────────────────────────────────────
        if "if_active" in spec:
            if env.get("active", False):
                return self._resolve(spec["if_active"], env)
            else_spec = spec.get("else")
            return self._resolve(else_spec, env) if else_spec is not None else None

        # ── gauss with optional clamp + round ──────────────────────────────
        if "gauss" in spec:
            mu, sigma = spec["gauss"]
            val = self._rng.gauss(float(mu), float(sigma))
            if "clamp" in spec:
                lo, hi = spec["clamp"]
                val = max(float(lo), min(float(hi), val))
            if "round" in spec:
                val = round(val, int(spec["round"]))
            return val

        # ── random.sample ──────────────────────────────────────────────────
        if "sample" in spec:
            cfg = spec["sample"]
            from_key = cfg["from"]
            k = int(cfg["k"])
            # Try env first (for sampled vars), then constants
            constants = self._telemetry_spec.get("constants", {})
            src = env.get(from_key, constants.get(from_key, []))
            if isinstance(src, dict):
                src = list(src)
            return self._rng.sample(src, min(k, len(src)))

        # ── choice (with constant-name support for dicts/lists) ────────────
        if "choice" in spec:
            src = spec["choice"]
            if isinstance(src, str):
                # Look up by name: constant first, then env
                constants = self._telemetry_spec.get("constants", {})
                if src in constants:
                    src = constants[src]
                elif src in env:
                    src = env[src]
            if isinstance(src, dict):
                src = list(src.keys())
            return self._rng.choice(src)

        # ── randint ────────────────────────────────────────────────────────
        if "randint" in spec:
            lo, hi = spec["randint"]
            return self._rng.randint(int(lo), int(hi))

        # ── uniform ────────────────────────────────────────────────────────
        if "uniform" in spec:
            lo, hi = spec["uniform"]
            val = self._rng.uniform(float(lo), float(hi))
            if "round" in spec:
                val = round(val, int(spec["round"]))
            return val

        # ── random ─────────────────────────────────────────────────────────
        if "random" in spec:
            val = self._rng.random()
            if "round" in spec:
                val = round(val, int(spec["round"]))
            return val

        # ── format (sub-specs resolved, then str.format) ───────────────────
        if "format" in spec:
            template = str(spec["format"])
            kwargs = {k: self._resolve(v, env) for k, v in spec.items() if k != "format"}
            return template.format(**kwargs)

        # Plain dict with no recognised DSL key — return verbatim (literal dict)
        return spec

    def _resolve_str(self, s: Any, env: dict) -> str:
        """Resolve *s* as a string, interpolating ``{name}`` and ``{dict[key]}``
        patterns from *env* via :meth:`~app.services.base_service.BaseService._safe_format`.
        """
        if isinstance(s, dict):
            return str(self._resolve(s, env))
        s = str(s) if s is not None else ""
        if "{" in s:
            return self._safe_format(s, env)
        return s

    def _resolve_attrs(self, attrs_spec: dict, env: dict) -> dict[str, Any]:
        """Resolve all values in an attrs dict."""
        return {k: self._resolve(v, env) for k, v in attrs_spec.items()}

    def _resolve_iterable(self, in_spec: Any, env: dict) -> list:
        """Resolve a ``for_each.in`` source to a Python list.

        - Dict value spec (``{var: x}``, ``{sample: ...}``) → resolved directly
        - String → look up in *env* first, then constants
        - Dict constant → converted to ``list(src.items())`` for (k, v) iteration
        """
        if isinstance(in_spec, dict):
            src = self._resolve(in_spec, env)
        elif isinstance(in_spec, str):
            constants = self._telemetry_spec.get("constants", {})
            # env takes precedence (sampled variables shadow constants of the same name)
            if in_spec in env:
                src = env[in_spec]
            elif in_spec in constants:
                src = constants[in_spec]
            else:
                return []
        else:
            src = in_spec

        if isinstance(src, dict):
            return list(src.items())
        return list(src) if src is not None else []
