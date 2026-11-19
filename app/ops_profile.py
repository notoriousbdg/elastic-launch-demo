"""Operations profiles: what ``opsctl`` looks like for one scenario.

The ops engine (:mod:`app.ops`) knows nothing about any scenario. Everything a
scenario's operations look like — verbs and their help, resource kinds, the audit
line a change leaves, follow-on records, who the actors are, which channels
remediate themselves, the wording the runbooks use and the telemetry guide shown to
Claude — is data:

1. Presets shipped with the engine (``app/ops_presets/<name>.yaml``), each a
   scenario-neutral fragment: ``neutral`` (the base every profile starts from),
   ``kubernetes``, ``aws-cloudtrail``, ``gcp-audit``, ``azure-activity``.
2. The scenario's own ``scenarios/<id>/ops.yaml``, which names the presets it
   ``extends`` and overrides or adds anything on top.

Because the scenario part lives in the scenario folder, an exported scenario zip
carries its operations with it, like its channels.

Merge rules (presets in ``extends`` order, then ``ops.yaml``):

- mappings merge key by key, recursively; a ``null`` value removes the key;
- ``side_effects`` and ``state_changes`` lists are concatenated;
- any other value (string, number, list) replaces the earlier one.

Templates
---------
Strings in a profile are templates. ``{key}`` is replaced with a value from the
command context; dotted keys walk mappings (``{flags.to-revision}``,
``{kind_info.plural}``). ``{=expr}`` evaluates a :func:`app.services.expr.safe_eval`
expression over the same context. A template that is exactly one placeholder keeps
the value's type (``{status}`` stays an int). Unknown keys render as ``""``.
``{{`` / ``}}`` are literal braces.

Where a profile accepts alternatives (``describe``, ``plan``) it takes either one
template or a list of cases ``{when: <expr>, text: <template>}`` (``lines`` for
``plan``); the first case whose ``when`` is true (or that has none) wins.
"""

from __future__ import annotations

import copy
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from app.services.expr import safe_eval

logger = logging.getLogger("nova7.ops")

PRESETS_DIR = Path(__file__).resolve().parent / "ops_presets"
PROFILE_FILE = "ops.yaml"

# Lists that accumulate across presets instead of replacing.
_CONCAT_KEYS = frozenset({"side_effects", "state_changes"})

# Top-level keys a profile may have (the verifier rejects anything else).
PROFILE_KEYS = frozenset({
    "extends", "banner", "namespaces", "actors", "auto_remediate_channels", "verbs",
    "kinds", "flags", "vars", "audit", "side_effects", "state_changes", "runbook", "guide",
})
VERB_KEYS = frozenset({"summary", "example", "describe", "plan", "audit_verb"})
KIND_KEYS = frozenset({"aliases", "audit", "plural", "group", "display", "cloudtrail", "azure", "gcp",
                       "params"})
FORMAT_KEYS = frozenset({"vars", "body", "attributes", "params", "params_prefix", "match", "severity"})
SIDE_EFFECT_KEYS = frozenset({"when", "verbs", "kinds", "service", "severity", "body", "attributes",
                              "delay_seconds"})
STATE_CHANGE_KEYS = frozenset({"when", "verbs", "kinds", "generator", "state", "transition_seconds"})
RUNBOOK_KEYS = frozenset({"scope_note", "recovery_note", "audit_label"})

# Keys every command context has (templates may use them; the verifier checks names).
CONTEXT_KEYS = frozenset({
    "command", "raw", "verb", "kind", "name", "target", "namespace", "flags", "settings",
    "actor", "change_id", "account_id", "describe", "iso_now", "klog_time", "kind_info",
    "verb_info", "flag", "flag_list", "ref", "params", "params_text", "service",
})

DEFAULT_AUTO_CHANNELS = (16, 17, 18, 19, 20)


# ── Loading and merging ───────────────────────────────────────────────────


def _load(path: Path) -> dict[str, Any]:
    with path.open() as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path}: an ops profile must be a mapping")
    return data


def preset_names() -> list[str]:
    return sorted(p.stem for p in PRESETS_DIR.glob("*.yaml"))


def load_preset(name: str) -> dict[str, Any]:
    path = PRESETS_DIR / f"{name}.yaml"
    if not path.is_file():
        raise ValueError(f"unknown ops preset {name!r} (shipped: {', '.join(preset_names())})")
    return _load(path)


def merge(base: dict[str, Any], over: dict[str, Any]) -> dict[str, Any]:
    """*over* merged onto a copy of *base* (see module docstring for the rules)."""
    out = copy.deepcopy(base)
    for key, val in (over or {}).items():
        if val is None:
            out.pop(key, None)
        elif key in _CONCAT_KEYS and isinstance(val, list):
            out[key] = list(out.get(key) or []) + copy.deepcopy(val)
        elif isinstance(val, dict) and isinstance(out.get(key), dict):
            out[key] = merge(out[key], val)
        else:
            # Re-insert at the end: `vars` are evaluated in order, so an override must
            # come after the vars it may now depend on.
            out.pop(key, None)
            out[key] = copy.deepcopy(val)
    return out


def resolve(raw: dict[str, Any] | None) -> dict[str, Any]:
    """A complete profile: ``neutral``, then each preset in ``extends``, then *raw*."""
    raw = dict(raw or {})
    extends = raw.pop("extends", None) or []
    if isinstance(extends, str):
        extends = [extends]
    profile = load_preset("neutral")
    for name in extends:
        if name != "neutral":
            profile = merge(profile, load_preset(name))
    profile = merge(profile, raw)
    profile["extends"] = ["neutral", *[n for n in extends if n != "neutral"]]
    return profile


def load_profile(scenario_dir: Path | None) -> dict[str, Any]:
    """The resolved profile of the scenario in *scenario_dir* (neutral without ops.yaml)."""
    raw: dict[str, Any] = {}
    if scenario_dir is not None:
        path = Path(scenario_dir) / PROFILE_FILE
        if path.is_file():
            raw = _load(path)
    return resolve(raw)


_NEUTRAL_CACHE: dict[str, Any] = {}


def neutral() -> dict[str, Any]:
    if not _NEUTRAL_CACHE:
        _NEUTRAL_CACHE.update(resolve({}))
    return _NEUTRAL_CACHE


def scenario_of(inst: Any) -> Any:
    return getattr(getattr(inst, "ctx", None), "scenario", None) or getattr(inst, "scenario", None)


def profile_for(inst_or_scenario: Any) -> dict[str, Any]:
    """The profile of an app instance or a scenario object (neutral when it has none)."""
    if inst_or_scenario is None:
        return neutral()
    prof = getattr(inst_or_scenario, "ops_profile", None)
    if not isinstance(prof, dict):
        prof = getattr(scenario_of(inst_or_scenario), "ops_profile", None)
    return prof if isinstance(prof, dict) else neutral()


def auto_remediate_channels(scenario: Any) -> set[int]:
    """Channels whose fix the remediation workflow applies on its own."""
    chans = profile_for(scenario).get("auto_remediate_channels")
    if chans is None:
        chans = DEFAULT_AUTO_CHANNELS
    out = set()
    for c in chans or []:
        try:
            out.add(int(c))
        except (TypeError, ValueError):
            continue
    return out


def channel_ranges(channels: Any) -> str:
    """``[1, 2, 3, 7, 16, 17]`` -> ``"1–3, 7, 16–17"`` (for agent-facing guidance)."""
    nums = sorted({int(c) for c in channels or []})
    parts: list[str] = []
    i = 0
    while i < len(nums):
        j = i
        while j + 1 < len(nums) and nums[j + 1] == nums[j] + 1:
            j += 1
        parts.append(str(nums[i]) if i == j else f"{nums[i]}–{nums[j]}")
        i = j + 1
    return ", ".join(parts) or "none"


def hitl_and_auto_ranges(scenario: Any) -> tuple[str, str]:
    """(HITL channel ranges, auto-remediated channel ranges) of *scenario*."""
    auto = auto_remediate_channels(scenario)
    try:
        chans = set(getattr(scenario, "channel_registry", {}) or {}) or set(range(1, 21))
    except Exception:
        chans = set(range(1, 21))
    return channel_ranges(chans - auto), channel_ranges(auto & chans or auto)


# ── Templates ─────────────────────────────────────────────────────────────


_PLACEHOLDER = re.compile(r"\{\{|\}\}|\{(=[^{}]+|[A-Za-z_][\w.\-]*)\}")


def _lookup(path: str, ctx: dict[str, Any]) -> Any:
    cur: Any = ctx
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
        if cur is None:
            return None
    return cur


def _value(token: str, ctx: dict[str, Any]) -> Any:
    if token.startswith("="):
        try:
            return safe_eval(token[1:].strip(), ctx)
        except ValueError as exc:
            logger.warning("ops: template expression %r failed: %s", token[1:], exc)
            return None
    return _lookup(token, ctx)


def render(template: Any, ctx: dict[str, Any]) -> Any:
    """*template* with its placeholders filled from *ctx* (non-strings pass through)."""
    if not isinstance(template, str):
        return template
    whole = _PLACEHOLDER.fullmatch(template)
    if whole and whole.group(1):
        val = _value(whole.group(1), ctx)
        return "" if val is None else val

    def sub(m: re.Match) -> str:
        if m.group(0) == "{{":
            return "{"
        if m.group(0) == "}}":
            return "}"
        val = _value(m.group(1), ctx)
        if val is None or val is False:
            return ""
        return str(val)

    return _PLACEHOLDER.sub(sub, template)


def placeholders(template: Any) -> list[str]:
    """Placeholder names in *template* (the first path segment; expressions excluded)."""
    if not isinstance(template, str):
        return []
    return [m.group(1).split(".")[0] for m in _PLACEHOLDER.finditer(template)
            if m.group(1) and not m.group(1).startswith("=")]


def render_mapping(mapping: dict[str, Any] | None, ctx: dict[str, Any]) -> dict[str, Any]:
    """Keys and values rendered; entries whose value renders empty are dropped."""
    out: dict[str, Any] = {}
    for key, val in (mapping or {}).items():
        k = str(render(str(key), ctx))
        v = render(val, ctx)
        if v is None or v == "" or not k:
            continue
        out[k] = v
    return out


def truthy(expr: Any, ctx: dict[str, Any]) -> bool:
    if expr is None or expr is True:
        return True
    if expr is False:
        return False
    try:
        return bool(safe_eval(str(expr), ctx))
    except ValueError as exc:
        logger.warning("ops: condition %r failed: %s", expr, exc)
        return False


def eval_vars(spec: dict[str, Any] | None, ctx: dict[str, Any]) -> dict[str, Any]:
    """Evaluate ``vars`` (name -> expression) in order, each seeing the earlier ones."""
    for name, expr in (spec or {}).items():
        if isinstance(expr, str):
            try:
                ctx[name] = safe_eval(expr, ctx)
            except ValueError as exc:
                logger.warning("ops: var %s=%r failed: %s", name, expr, exc)
                ctx[name] = ""
        else:
            ctx[name] = expr
    return ctx


def choose(cases: Any, ctx: dict[str, Any], key: str = "text") -> Any:
    """The first matching case of *cases* (a template, or a list of {when, text})."""
    if cases is None:
        return None
    if not isinstance(cases, list) or (cases and not isinstance(cases[0], dict)):
        return cases
    for case in cases:
        if isinstance(case, dict) and truthy(case.get("when"), ctx):
            return case.get(key)
    return None


# ── Command context ───────────────────────────────────────────────────────


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _klog_time() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("%m%d %H:%M:%S.") + f"{now.microsecond:06d}"


def command_context(profile: dict[str, Any], cmd: Any, **extra: Any) -> dict[str, Any]:
    """The template context for *cmd* (an :class:`app.ops.OpsCommand`)."""
    flags = dict(cmd.flags)

    def flag(name: str, default: str = "") -> str:
        v = flags.get(name)
        return default if v is None or v is True else str(v)

    def flag_list(name: str) -> list[str]:
        return [s.strip() for s in flag(name).split(",") if s.strip()]

    ctx: dict[str, Any] = {
        "command": cmd.display(),
        "raw": cmd.raw,
        "verb": cmd.verb,
        "kind": cmd.kind,
        "name": cmd.name,
        "target": cmd.target,
        "namespace": cmd.namespace,
        "flags": flags,
        "settings": ", ".join(f"{k}={v}" for k, v in flags.items() if v is not True),
        "flag": flag,
        "flag_list": flag_list,
        "kind_info": dict((profile.get("kinds") or {}).get(cmd.kind) or {}),
        "verb_info": dict((profile.get("verbs") or {}).get(cmd.verb) or {}),
        "iso_now": _now_iso(),
        "klog_time": _klog_time(),
        "actor": "",
        "change_id": "",
        "account_id": "",
    }
    ctx.update(extra)
    eval_vars(profile.get("vars"), ctx)
    ctx.setdefault("ref", cmd.target)
    if not ctx.get("ref"):
        ctx["ref"] = cmd.target
    return ctx


# ── Aliases, verbs, help ──────────────────────────────────────────────────


def kind_aliases(profile: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for kind, info in (profile.get("kinds") or {}).items():
        for alias in (info or {}).get("aliases") or []:
            out[str(alias).lower()] = kind
    return out


def verbs(profile: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {v: dict(info or {}) for v, info in (profile.get("verbs") or {}).items() if info is not None}


def grammar(profile: dict[str, Any]) -> str:
    ns = " [-n <namespace>]" if profile.get("namespaces") else ""
    return f"opsctl [plan] <verb> <kind>[/<name>]{ns} [--flag value | --flag=value | --switch]..."


def guide_text(profile: dict[str, Any]) -> str:
    guide = profile.get("guide")
    if isinstance(guide, dict):
        return "\n\n".join(str(v).strip() for v in guide.values() if v)
    return str(guide or "").strip()


def public_view(profile: dict[str, Any]) -> dict[str, Any]:
    """What ``GET /api/ops/profile`` returns (help and guide; no templates)."""
    return {
        "banner": profile.get("banner") or "",
        "usage": grammar(profile),
        "namespaces": bool(profile.get("namespaces")),
        "verbs": [{"verb": v, "summary": i.get("summary", ""), "example": i.get("example", "")}
                  for v, i in verbs(profile).items()],
        "guide": guide_text(profile),
        "auto_remediate_channels": sorted(
            int(c) for c in (profile.get("auto_remediate_channels") or DEFAULT_AUTO_CHANNELS)),
    }
