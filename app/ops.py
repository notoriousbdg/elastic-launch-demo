"""Platform operations API behind ``opsctl`` (``/api/ops/actions``).

An invocation is parsed into a normalized command and compared with the
``remediation_command`` lines (schema 1.3) of every active channel. A channel is
fixed once all of its lines have been matched by invocations made while it was
active; the channel is then resolved, which applies its ``remediation_effects``.

Every applied invocation, right or wrong, leaves an audit record on the service
the scenario's ops profile names (app/ops_profile.py). The response never
says whether the change fixed anything: the caller verifies in telemetry.
"""

from __future__ import annotations

import logging
import re
import shlex
import threading
import uuid
import weakref
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app import ops_profile

logger = logging.getLogger("nova7.ops")

OPS_USER = "opsctl/claude-code"
# Fallback only; the real account comes from the scenario's ScenarioIdentity, so
# CloudTrail lines quote the same account as every other AWS signal.
AWS_ACCOUNT_ID = "000000000000"


def _account_id(inst: Any) -> str:
    ns = getattr(inst, "namespace", None) or getattr(getattr(inst, "ctx", None), "namespace", None)
    if not ns:
        return AWS_ACCOUNT_ID
    try:
        from generator_engine.identity import ScenarioIdentity

        return ScenarioIdentity(str(ns)).aws_account_id
    except Exception:
        return AWS_ACCOUNT_ID

# The neutral verbs (``opsctl --help`` for a scenario without an ops.yaml). The verbs a
# scenario actually accepts come from its ops profile (app/ops_profile.py).
VERBS: dict[str, str] = {v: i.get("summary", "") for v, i in ops_profile.verbs(ops_profile.neutral()).items()}
PLAN_HELP = "Dry run any of the above: show what would change without applying it"

GRAMMAR = ops_profile.grammar(ops_profile.neutral())


class OpsError(ValueError):
    """Bad invocation; the message is shown to the caller as-is."""


@dataclass
class OpsCommand:
    verb: str
    target: str  # normalized "kind/name" or "kind"
    namespace: str = ""
    flags: dict[str, Any] = field(default_factory=dict)  # name -> str | True
    dry_run: bool = False
    raw: str = ""

    @property
    def kind(self) -> str:
        return self.target.split("/", 1)[0]

    @property
    def name(self) -> str:
        return self.target.split("/", 1)[1] if "/" in self.target else ""

    def display(self) -> str:
        parts = [self.verb, self.target]
        if self.namespace:
            parts += ["-n", self.namespace]
        for k, v in self.flags.items():
            parts.append(f"--{k}" if v is True else f"--{k}={shlex.quote(str(v))}")
        return " ".join(parts)


# ── Parsing ───────────────────────────────────────────────────────────────


def _flag_aliases(profile: dict[str, Any]) -> dict[str, str]:
    return {str(k).lower(): str(v).lower()
            for k, v in ((profile.get("flags") or {}).get("aliases") or {}).items()}


def _switches(profile: dict[str, Any]) -> set[str]:
    return {str(s).lower() for s in (profile.get("flags") or {}).get("switches") or []}


def _norm_flag(name: str, profile: dict[str, Any] | None = None) -> str:
    name = name.lstrip("-").strip().lower().replace("_", "-")
    return _flag_aliases(profile or ops_profile.neutral()).get(name, name)


def _norm_target(token: str, profile: dict[str, Any] | None = None) -> str:
    token = token.strip().lower()
    kind, sep, name = token.partition("/")
    kind = ops_profile.kind_aliases(profile or ops_profile.neutral()).get(kind, kind)
    return f"{kind}/{name}" if sep and name else kind


def parse_command(command: str | list[str], profile: dict[str, Any] | None = None) -> OpsCommand:
    """Parse ``"rollback configmap/coredns -n kube-system --to-revision 42"`` (or argv).

    *profile* (the scenario's ops profile) supplies the verbs, kind aliases, flag
    aliases and switches; the neutral profile when omitted.
    """
    profile = profile or ops_profile.neutral()
    known_verbs = ops_profile.verbs(profile)
    switches = _switches(profile)
    if isinstance(command, str):
        try:
            tokens = shlex.split(command)
        except ValueError as exc:
            raise OpsError(f"error: {exc}") from None
    else:
        tokens = [str(t) for t in command]
    raw = " ".join(shlex.quote(t) for t in tokens)
    if tokens and tokens[0].rsplit("/", 1)[-1] == "opsctl":
        tokens = tokens[1:]
    dry_run = False
    if tokens and tokens[0].lower() == "plan":
        dry_run = True
        tokens = tokens[1:]
    if not tokens:
        raise OpsError("error: you must specify a command. See 'opsctl --help'.")
    verb = tokens[0].lower()
    if verb not in known_verbs:
        raise OpsError(f'error: unknown command "{tokens[0]}" for "opsctl". See \'opsctl --help\'.')

    target = ""
    namespace = ""
    flags: dict[str, Any] = {}
    last_flag = ""
    i = 1
    while i < len(tokens):
        tok = tokens[i]
        if tok.startswith("-") and len(tok) > 1:
            name, eq, value = tok.partition("=")
            fname = _norm_flag(name, profile)
            if not eq:
                nxt = tokens[i + 1] if i + 1 < len(tokens) else None
                if fname not in switches and nxt is not None and not nxt.startswith("-"):
                    value = nxt
                    i += 1
                else:
                    value = True
            if fname in ("n", "namespace"):
                if value is True or not str(value).strip():
                    raise OpsError("error: flag needs an argument: 'n' in -n")
                namespace = str(value).strip().lower()
            else:
                flags[fname] = value
                last_flag = fname
        elif not target:
            target = _norm_target(tok, profile)
        elif last_flag and isinstance(flags.get(last_flag), str) and flags[last_flag].endswith(","):
            flags[last_flag] += tok  # "--instance-types a, b" (space after the comma)
        else:
            raise OpsError(f'error: unexpected argument "{tok}" for "opsctl {verb}"')
        i += 1
    if not target:
        raise OpsError(
            f'error: you must specify the resource for "opsctl {verb}", e.g. <kind>/<name>. '
            f"See 'opsctl {verb} --help'."
        )
    return OpsCommand(verb=verb, target=target, namespace=namespace, flags=flags,
                      dry_run=dry_run, raw=raw)


def expected_commands(remediation_command: Any, profile: dict[str, Any] | None = None) -> list[OpsCommand]:
    """The parsed lines of a channel's ``remediation_command`` (str or list)."""
    if not remediation_command:
        return []
    chunks = remediation_command if isinstance(remediation_command, list) else [remediation_command]
    lines: list[str] = []
    for chunk in chunks:
        pending = ""
        for line in str(chunk).splitlines():
            line = line.strip()
            if line.endswith("\\"):
                pending += line[:-1] + " "
                continue
            line = (pending + line).strip()
            pending = ""
            if line and not line.startswith("#"):
                lines.append(line)
        if pending.strip():
            lines.append(pending.strip())
    out = []
    for line in lines:
        try:
            out.append(parse_command(line, profile))
        except OpsError:
            logger.warning("ops: unparseable remediation_command line %r", line)
    return out


# ── Matching ──────────────────────────────────────────────────────────────


def _norm_value(value: Any) -> Any:
    if value is True:
        return True
    s = " ".join(str(value).split()).lower()
    if s in ("true", "yes"):
        return True
    if "," in s:
        return frozenset(p.strip() for p in s.split(",") if p.strip())
    return s


def _values_match(flag: str, expected: Any, actual: Any) -> bool:
    e, a = _norm_value(expected), _norm_value(actual)
    if flag == "to-revision" and "previous" in (e, a) and e is not True and a is not True:
        return True
    return e == a


def matches(invocation: OpsCommand, expected: OpsCommand) -> bool:
    """Same verb, target and namespace; every expected flag present with an equal value."""
    if invocation.verb != expected.verb or invocation.target != expected.target:
        return False
    if (invocation.namespace or "") != (expected.namespace or ""):
        return False
    for flag, value in expected.flags.items():
        if flag not in invocation.flags:
            return False
        if not _values_match(flag, value, invocation.flags[flag]):
            return False
    return True


# ── Per-instance progress ─────────────────────────────────────────────────


class _Progress:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        # channel -> {"triggered_at": float, "matched": set[int]}
        self.channels: dict[int, dict[str, Any]] = {}


_PROGRESS: "weakref.WeakKeyDictionary[Any, _Progress]" = weakref.WeakKeyDictionary()
_PROGRESS_LOCK = threading.Lock()


def _progress_for(controller: Any) -> _Progress:
    with _PROGRESS_LOCK:
        p = _PROGRESS.get(controller)
        if p is None:
            p = _Progress()
            _PROGRESS[controller] = p
        return p


def reset_progress(controller: Any) -> None:
    with _PROGRESS_LOCK:
        _PROGRESS.pop(controller, None)


def _record_and_complete(inst: Any, cmd: OpsCommand) -> list[int]:
    """Record *cmd* against every active channel; return the channels now complete."""
    controller = inst.chaos_controller
    registry = getattr(controller, "_channel_registry", {}) or {}
    profile = ops_profile.profile_for(inst)
    progress = _progress_for(controller)
    completed: list[int] = []
    # Faults that auto-expired before anyone fixed them still need their fix recorded.
    active = controller.get_unfixed_channels()
    with progress.lock:
        for ch in list(progress.channels):
            if ch not in active:
                progress.channels.pop(ch, None)
        for ch in active:
            expected = expected_commands((registry.get(ch) or {}).get("remediation_command"), profile)
            if not expected:
                continue
            triggered_at = controller.get_channel_status(ch).get("triggered_at")
            entry = progress.channels.get(ch)
            if entry is None or entry["triggered_at"] != triggered_at:
                entry = {"triggered_at": triggered_at, "matched": set()}
                progress.channels[ch] = entry
            for idx, exp in enumerate(expected):
                if matches(cmd, exp):
                    entry["matched"].add(idx)
            if len(entry["matched"]) >= len(expected):
                completed.append(ch)
                progress.channels.pop(ch, None)
    return completed


# ── Messages and audit records (all from the scenario's ops profile) ─────


def _ctx(profile: dict[str, Any], cmd: OpsCommand, **extra: Any) -> dict[str, Any]:
    return ops_profile.command_context(profile, cmd, **extra)


def describe(cmd: OpsCommand, change_id: str = "", profile: dict[str, Any] | None = None,
             ctx: dict[str, Any] | None = None) -> str:
    """The result line for an applied command (the verb's ``describe`` template)."""
    profile = profile or ops_profile.neutral()
    ctx = ctx if ctx is not None else _ctx(profile, cmd, change_id=change_id)
    tmpl = ops_profile.choose((ops_profile.verbs(profile).get(cmd.verb) or {}).get("describe"), ctx)
    if not tmpl:
        tmpl = "{ref} " + cmd.verb + " applied"
    return str(ops_profile.render(tmpl, ctx))


def describe_plan(cmd: OpsCommand, profile: dict[str, Any] | None = None) -> list[str]:
    """What a dry run of *cmd* would change (the verb's ``plan`` lines)."""
    profile = profile or ops_profile.neutral()
    ctx = _ctx(profile, cmd)
    changes = [f"{k} -> {v}" for k, v in cmd.flags.items() if v is not True]
    ctx["changes_text"] = (": " + ", ".join(changes)) if changes else ""
    lines = ops_profile.choose((ops_profile.verbs(profile).get(cmd.verb) or {}).get("plan"), ctx,
                               key="lines")
    if not lines:
        lines = ["~ {ref}{where}{changes_text}"]
    if isinstance(lines, str):
        lines = [lines]
    return [str(ops_profile.render(line, ctx)) for line in lines]


def _format_for(profile: dict[str, Any], cmd: OpsCommand) -> tuple[str, dict[str, Any]]:
    audit = profile.get("audit") or {}
    formats = audit.get("formats") or {}
    kind_info = (profile.get("kinds") or {}).get(cmd.kind) or {}
    name = kind_info.get("audit") or audit.get("default") or "command_log"
    fmt = formats.get(name)
    if fmt is None:
        logger.warning("ops: audit format %r not in the profile; using command_log", name)
        name, fmt = "command_log", formats.get("command_log") or ops_profile.neutral()["audit"]["formats"]["command_log"]
    return name, fmt


def _audit_ctx(profile: dict[str, Any], cmd: OpsCommand, change_id: str, account_id: str,
               user: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """(format, context with the format's vars and params evaluated) for *cmd*."""
    ctx = _ctx(profile, cmd, change_id=change_id, account_id=account_id, actor=user)
    ctx["describe"] = describe(cmd, change_id, profile, ctx=dict(ctx))
    _, fmt = _format_for(profile, cmd)
    ops_profile.eval_vars(fmt.get("vars"), ctx)
    params_spec = fmt.get("params") or ctx["kind_info"].get("params") or {}
    params = ops_profile.render_mapping(params_spec, ctx)
    prefix = str(fmt.get("params_prefix") or "")
    ctx["params"] = params
    ctx["params_text"] = " ".join(f"{prefix}{k}={v}" for k, v in params.items())
    return fmt, ctx


def audit_record(cmd: OpsCommand, change_id: str, account_id: str = AWS_ACCOUNT_ID,
                 user: str = OPS_USER, profile: dict[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
    """(body, attributes) of the audit line an applied command leaves behind."""
    profile = profile or ops_profile.neutral()
    fmt, ctx = _audit_ctx(profile, cmd, change_id, account_id, user)
    body = str(ops_profile.render(fmt.get("body") or "", ctx)).strip()
    attrs = ops_profile.render_mapping(fmt.get("attributes"), ctx)
    return body, attrs


def _token_present(token: str, body: str) -> bool:
    return re.search(r"(?<![\w/.\-])" + re.escape(token) + r"(?![\w/.\-])", body, re.IGNORECASE) is not None


def _is_fix_line_for(body: str, fmt: dict[str, Any], ctx: dict[str, Any]) -> bool:
    """Whether a channel's raw log line is *cmd*'s audit record, per the format's ``match``."""
    if "{actor}" not in body:
        return False
    match = fmt.get("match") or {}
    prefix = match.get("prefix")
    if prefix and not body.startswith(str(prefix)):
        return False
    tokens = match.get("tokens") or []
    if not tokens:
        return bool(prefix)
    for tmpl in tokens:
        # A token whose placeholders are empty (no namespace, no name) is not checked.
        if any(ops_profile.render("{" + ph + "}", ctx) in ("", None)
               for ph in ops_profile.placeholders(tmpl)):
            continue
        if not _token_present(str(ops_profile.render(tmpl, ctx)), body):
            return False
    return True


def _channel_fix_line(inst: Any, cmd: OpsCommand) -> tuple[int, int, dict[str, Any]] | None:
    """The active channel's own audit line for *cmd*, when *cmd* is one of its fix steps.

    Scenarios describe the right fix's audit record in ``remediation_effects.log_events``
    (a line naming ``{actor}``) with more detail than the generic record. opsctl emits
    that line as the command's audit record instead of its generic one, so each action
    leaves exactly one record. Which line belongs to which command is decided by the
    audit format's ``match`` (prefix and tokens) in the scenario's ops profile.
    """
    controller = inst.chaos_controller
    registry = getattr(controller, "_channel_registry", {}) or {}
    profile = ops_profile.profile_for(inst)
    fmt, ctx = _audit_ctx(profile, cmd, "", _account_id(inst), OPS_USER)
    for ch in controller.get_unfixed_channels():
        ch_def = registry.get(ch) or {}
        if not any(matches(cmd, exp)
                   for exp in expected_commands(ch_def.get("remediation_command"), profile)):
            continue
        events = (ch_def.get("remediation_effects") or {}).get("log_events") or []
        for idx, evt in enumerate(events):
            body = str((evt or {}).get("body") or "")
            if _is_fix_line_for(body, fmt, ctx):
                return ch, idx, evt
    return None


def _services(inst: Any) -> dict[str, Any]:
    services = getattr(getattr(inst, "ctx", None), "services", None) or {}
    if not services:
        services = getattr(ops_profile.scenario_of(inst), "services", None) or {}
    return services


def _audit_service(inst: Any) -> str | None:
    """The profile's first ``audit.service`` candidate the scenario has, else its first service."""
    services = _services(inst)
    wanted = (ops_profile.profile_for(inst).get("audit") or {}).get("service") or []
    if isinstance(wanted, str):
        wanted = [wanted]
    for name in wanted:
        if name in services:
            return name
    return next(iter(services), None)


def _side_effects(cmd: OpsCommand, change_id: str, user: str = OPS_USER,
                  profile: dict[str, Any] | None = None) -> list[tuple[str, str, dict[str, Any], str | None]]:
    """(severity, body, attrs, service) for the follow-on records of a command."""
    profile = profile or ops_profile.neutral()
    ctx = _ctx(profile, cmd, change_id=change_id, actor=user)
    out = []
    for se in profile.get("side_effects") or []:
        if not isinstance(se, dict):
            continue
        if se.get("verbs") and cmd.verb not in se["verbs"]:
            continue
        if se.get("kinds") and cmd.kind not in se["kinds"]:
            continue
        if not ops_profile.truthy(se.get("when"), ctx):
            continue
        out.append((str(se.get("severity") or "INFO"),
                    str(ops_profile.render(se.get("body") or "", ctx)),
                    ops_profile.render_mapping(se.get("attributes"), ctx),
                    se.get("service")))
    return out


# ── Platform state an applied command changes ─────────────────────────────


def _nest(path: str, value: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    cur = out
    parts = [p for p in path.split(".") if p]
    for p in parts[:-1]:
        cur = cur.setdefault(p, {})
    cur[parts[-1]] = value
    return out


def _rule_state(rule: dict[str, Any], cmd: OpsCommand, ctx: dict[str, Any]) -> tuple[dict[str, Any], float] | None:
    if rule.get("verbs") and cmd.verb not in rule["verbs"]:
        return None
    if rule.get("kinds") and cmd.kind not in rule["kinds"]:
        return None
    if not ops_profile.truthy(rule.get("when"), ctx):
        return None
    gid = rule.get("generator")
    if not gid:
        return None
    state: dict[str, Any] = {}
    for path, tmpl in (rule.get("state") or {}).items():
        val = ops_profile.render(tmpl, ctx)
        if val in ("", None):
            continue
        state = ops_profile.merge(state, _nest(str(ops_profile.render(str(path), ctx)), val))
    if not state:
        return None
    return {gid: state}, float(rule.get("transition_seconds", 0) or 0)


def change_state(inst: Any, cmd: OpsCommand) -> tuple[dict[str, Any], float]:
    """The platform change an applied command makes, as generator state.

    Every applied change takes effect in telemetry on its own, whether or not it
    completes a fix: patching a memory limit rolls the pods to the new limit even while
    the fault continues. Two sources, first hit wins:

    1. the scenario's ``ops.yaml`` ``state_changes`` rules;
    2. each enabled generator's ``SPEC.ops_bindings`` (rules like the above, or a
       callable ``(scenario, cmd) -> (state, transition_seconds)``).

    Returns ({}, 0) when the command changes nothing a generator reports.
    """
    scenario = ops_profile.scenario_of(inst)
    profile = ops_profile.profile_for(inst)
    ctx = _ctx(profile, cmd)
    for rule in profile.get("state_changes") or []:
        hit = _rule_state(rule, cmd, ctx) if isinstance(rule, dict) else None
        if hit:
            return hit
    if scenario is None:
        return {}, 0.0
    try:
        from generator_engine.registry import enabled_specs
        specs = enabled_specs(scenario)
    except Exception:
        specs = []
    for spec in specs:
        bindings = getattr(spec, "ops_bindings", None)
        if callable(bindings):
            try:
                state, transition = bindings(scenario, cmd)
            except Exception:
                logger.exception("ops: %s ops_bindings failed for %s", spec.id, cmd.display())
                continue
            if state:
                return state, float(transition or 0)
            continue
        for rule in bindings or []:
            if isinstance(rule, dict):
                hit = _rule_state({"generator": spec.id, **rule}, cmd, ctx)
                if hit:
                    return hit
    return {}, 0.0


# ── Entry points ──────────────────────────────────────────────────────────


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def list_actions(profile: dict[str, Any] | None = None) -> dict[str, Any]:
    profile = profile or ops_profile.neutral()
    flags = {}
    if profile.get("namespaces"):
        flags["-n, --namespace"] = "Namespace of the target object (omit for cluster or account scoped objects)"
    flags["--flag value | --flag=value"] = "Setting to change; lists are comma-separated"
    switches = sorted(_switches(profile))
    flags["--switch"] = "Boolean option" + (f", e.g. {', '.join('--' + s for s in switches[:3])}" if switches else "")
    return {
        "usage": ops_profile.grammar(profile),
        "verbs": [{"verb": v, "help": i.get("summary", "")} for v, i in ops_profile.verbs(profile).items()]
        + [{"verb": "plan", "help": PLAN_HELP}],
        "flags": flags,
    }


def execute(inst: Any, command: str | list[str], dry_run: bool = False,
            user: str | None = None) -> dict[str, Any]:
    """Run one opsctl invocation against *inst*. Raises OpsError on bad input.

    *user* is the actor recorded in the audit lines (default ``opsctl/claude-code``);
    automation that applies a runbook fix on its own passes its own identity.
    """
    user = (user or OPS_USER).strip()[:128] or OPS_USER
    profile = ops_profile.profile_for(inst)
    cmd = parse_command(command, profile)
    dry_run = dry_run or cmd.dry_run
    base = {
        "action": cmd.verb,
        "target": cmd.target,
        "namespace": cmd.namespace or None,
        "timestamp": _now_iso(),
    }
    if dry_run:
        ref = _ctx(profile, cmd).get("ref") or cmd.target
        return {"status": "planned", **base,
                "message": f"{ref} would be changed (dry run, nothing applied)",
                "changes": describe_plan(cmd, profile)}

    change_id = "chg-" + uuid.uuid4().hex[:8]
    controller = inst.chaos_controller
    service = _audit_service(inst)
    if service:
        own = _channel_fix_line(inst, cmd)
        if own:
            ch, idx, evt = own
            from app.chaos.controller import with_actor
            body = f"{with_actor(str(evt['body']), user)} change_id={change_id}"
            attrs = {k: with_actor(v, user) for k, v in (evt.get("attributes") or {}).items()}
            attrs.update({"user.name": user, "change.id": change_id})
            controller.queue_event(evt.get("service") or service, body,
                                   severity=evt.get("severity", "INFO"), attributes=attrs)
            if hasattr(controller, "mark_fix_event_emitted"):
                controller.mark_fix_event_emitted(ch, idx)
        else:
            body, attrs = audit_record(cmd, change_id, _account_id(inst), user, profile)
            controller.queue_event(service, body, attributes=attrs)
        for i, (sev, sbody, sattrs, sservice) in enumerate(
                _side_effects(cmd, change_id, user, profile), start=1):
            controller.queue_event(sservice or service, sbody, severity=sev, attributes=sattrs,
                                   delay_seconds=i)

    if hasattr(controller, "record_applied"):
        controller.record_applied(cmd.target, cmd.verb, cmd.flags)
    state, transition = change_state(inst, cmd)
    if state and hasattr(controller, "apply_change_state"):
        controller.apply_change_state(state, transition)

    for ch in _record_and_complete(inst, cmd):
        logger.info("ops: change %s completes the fix for channel %d", change_id, ch)
        inst.resolve_channel(ch, force=True, actor=user)

    return {"status": "applied", **base, "message": describe(cmd, change_id, profile),
            "change_id": change_id}


def reset(inst: Any) -> dict[str, Any]:
    controller = inst.chaos_controller
    controller.reset_platform_state()
    reset_progress(controller)
    return {"status": "reset", "timestamp": _now_iso()}
