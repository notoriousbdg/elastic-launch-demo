"""Platform operations API behind ``opsctl`` (``/api/ops/actions``).

An invocation is parsed into a normalized command and compared with the
``remediation_command`` lines (schema 1.3) of every active channel. A channel is
fixed once all of its lines have been matched by invocations made while it was
active; the channel is then resolved, which applies its ``remediation_effects``.

Every applied invocation, right or wrong, leaves an audit record on the
``kube-apiserver`` service (or the scenario's first service). The response never
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

# verb -> one-line help (shown by `opsctl --help` / GET /api/ops/actions)
VERBS: dict[str, str] = {
    "rollback": "Roll a resource back to an earlier revision (--to-revision N|previous)",
    "restore": "Restore a versioned resource to a known-good version (--version)",
    "renew": "Trigger certificate renewal (--all, --not-ready to limit the selection)",
    "rotate": "Rotate a credential or secret and roll out the new value",
    "sync": "Force an immediate refresh of a synced secret or config source",
    "drain": "Cordon a node and evict its pods (--ignore-daemonsets)",
    "replace": "Cordon, drain and terminate a node; the node group launches a replacement",
    "patch": "Change settings on a resource (e.g. --memory-limit, --instance-types, --cron)",
    "apply": "Create or update a policy object (e.g. ratelimit --rps)",
    "expand": "Grow a persistent volume claim (--size)",
    "run": "Start a one-off job from its template",
    "delete": "Delete a resource, or every resource matching --selector",
    "restart": "Rolling restart of a workload",
}
PLAN_HELP = "Dry run any of the above: show what would change without applying it"

GRAMMAR = (
    "opsctl [plan] <verb> <kind>[/<name>] [-n <namespace>] [--flag value | --flag=value | --switch]..."
)

# Kinds that live in the cloud account rather than the cluster API.
AWS_KINDS = {"iam-policy", "iam-role", "iam-role-trust", "nodegroup", "registry-credential"}

_KIND_ALIASES = {
    "cm": "configmap", "configmaps": "configmap",
    "deploy": "deployment", "deployments": "deployment",
    "sts": "statefulset", "statefulsets": "statefulset",
    "ds": "daemonset", "daemonsets": "daemonset",
    "no": "node", "nodes": "node",
    "po": "pod", "pods": "pod",
    "svc": "service", "services": "service",
    "ing": "ingress", "ingresses": "ingress",
    "cert": "certificate", "certificates": "certificate",
    "persistentvolumeclaim": "pvc", "persistentvolumeclaims": "pvc", "pvcs": "pvc",
    "jobs": "job", "secrets": "secret",
    "externalsecrets": "externalsecret", "es": "externalsecret",
    "ratelimits": "ratelimit", "nodegroups": "nodegroup", "ng": "nodegroup",
    "iam-policies": "iam-policy",
    "netpol": "networkpolicy", "networkpolicies": "networkpolicy",
    "poddisruptionbudget": "pdb", "poddisruptionbudgets": "pdb", "pdbs": "pdb",
    "horizontalpodautoscaler": "hpa", "horizontalpodautoscalers": "hpa", "hpas": "hpa",
}

_RESOURCE_PLURAL = {
    "configmap": "configmaps", "deployment": "deployments", "statefulset": "statefulsets",
    "daemonset": "daemonsets", "node": "nodes", "pod": "pods", "service": "services",
    "ingress": "ingresses", "certificate": "certificates", "pvc": "persistentvolumeclaims",
    "job": "jobs", "secret": "secrets", "externalsecret": "externalsecrets",
    "ratelimit": "ratelimits", "harbor": "harborconfigurations",
    "networkpolicy": "networkpolicies", "pdb": "poddisruptionbudgets",
    "hpa": "horizontalpodautoscalers",
}

_RESOURCE_GROUP = {
    "deployment": "apps", "statefulset": "apps", "daemonset": "apps", "job": "batch",
    "ingress": "networking.k8s.io", "certificate": "cert-manager.io",
    "externalsecret": "external-secrets.io", "ratelimit": "gateway.platform.io",
    "harbor": "goharbor.io", "networkpolicy": "networking.k8s.io", "pdb": "policy",
    "hpa": "autoscaling",
}

_FLAG_ALIASES = {"l": "selector", "selector": "selector", "revision": "to-revision"}

# flags that take no value
_SWITCHES = {"all", "not-ready", "ignore-daemonsets", "delete-emptydir-data", "force",
             "wait", "all-namespaces", "a"}


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


def _norm_flag(name: str) -> str:
    name = name.lstrip("-").strip().lower().replace("_", "-")
    return _FLAG_ALIASES.get(name, name)


def _norm_target(token: str) -> str:
    token = token.strip().lower()
    kind, sep, name = token.partition("/")
    kind = _KIND_ALIASES.get(kind, kind)
    return f"{kind}/{name}" if sep and name else kind


def parse_command(command: str | list[str]) -> OpsCommand:
    """Parse ``"rollback configmap/coredns -n kube-system --to-revision 42"`` (or argv)."""
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
    if verb not in VERBS:
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
            fname = _norm_flag(name)
            if not eq:
                nxt = tokens[i + 1] if i + 1 < len(tokens) else None
                if fname not in _SWITCHES and nxt is not None and not nxt.startswith("-"):
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
            target = _norm_target(tok)
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


def expected_commands(remediation_command: Any) -> list[OpsCommand]:
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
            out.append(parse_command(line))
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
    progress = _progress_for(controller)
    completed: list[int] = []
    active = controller.get_active_channels()
    with progress.lock:
        for ch in list(progress.channels):
            if ch not in active:
                progress.channels.pop(ch, None)
        for ch in active:
            expected = expected_commands((registry.get(ch) or {}).get("remediation_command"))
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


# ── Messages and audit records ────────────────────────────────────────────


def _flag(cmd: OpsCommand, name: str) -> str:
    v = cmd.flags.get(name)
    return "" if v is None or v is True else str(v)


def _settings(cmd: OpsCommand, skip: tuple[str, ...] = ()) -> str:
    items = [f"{k}={v}" for k, v in cmd.flags.items() if v is not True and k not in skip]
    return ", ".join(items)


# kubectl's singular resource names where opsctl's kind is a short name
_KIND_DISPLAY = {"pdb": "poddisruptionbudget", "hpa": "horizontalpodautoscaler",
                 "pvc": "persistentvolumeclaim"}


def _ref(cmd: OpsCommand) -> str:
    group = _RESOURCE_GROUP.get(cmd.kind)
    name = _KIND_DISPLAY.get(cmd.kind, cmd.kind)
    kind = f"{name}.{group}" if group else name
    return f"{kind}/{cmd.name}" if cmd.name else kind


def describe(cmd: OpsCommand, change_id: str = "") -> str:
    """kubectl-style result line for an applied command."""
    ref, v = _ref(cmd), cmd.verb
    ns = f" in namespace {cmd.namespace}" if cmd.namespace else ""
    if v == "rollback":
        rev = _flag(cmd, "to-revision")
        if rev and rev.lower() != "previous":
            return f"{ref} rolled back to revision {rev}"
        return f"{ref} rolled back to previous revision"
    if v == "restore":
        ver = _flag(cmd, "version")
        return f"{ref} restored to version {ver}" if ver else f"{ref} restored to last known-good version"
    if v == "renew":
        if cmd.name:
            return f"{ref} renewal triggered"
        scope = "not-ready certificates" if "not-ready" in cmd.flags else "certificates"
        return f"renewal triggered for all {scope}{ns}"
    if v == "rotate":
        return f"{ref} rotated; new version staged and rolled out"
    if v == "sync":
        return f"{ref} force-sync requested; secret refreshed from provider"
    if v == "drain":
        return f"{ref} cordoned\n{ref} drained"
    if v == "replace":
        return (f"{ref} cordoned\n{ref} drained\n{ref} terminated; "
                f"replacement instance requested from node group")
    if v == "patch":
        s = _settings(cmd)
        return f"{ref} patched" + (f" ({s})" if s else "")
    if v == "apply":
        s = _settings(cmd)
        return f"{ref} configured" + (f" ({s})" if s else "")
    if v == "expand":
        size = _flag(cmd, "size")
        return f"{ref} resize requested" + (f" to {size}" if size else "") + "; filesystem expansion pending"
    if v == "run":
        suffix = change_id.split("-")[-1][:5] if change_id else uuid.uuid4().hex[:5]
        return f"job.batch/{cmd.name or cmd.kind}-manual-{suffix} created"
    if v == "delete":
        sel = _flag(cmd, "selector")
        if sel:
            return f"{_RESOURCE_PLURAL.get(cmd.kind, cmd.kind + 's')} matching {sel} deleted{ns}"
        return f"{ref} deleted"
    if v == "restart":
        return f"{ref} restarted"
    return f"{ref} {v} applied"


def describe_plan(cmd: OpsCommand) -> list[str]:
    """What a dry run of *cmd* would change."""
    ref = _ref(cmd)
    where = f" (namespace {cmd.namespace})" if cmd.namespace else ""
    v = cmd.verb
    lines: list[str]
    if v == "rollback":
        rev = _flag(cmd, "to-revision") or "previous"
        lines = [f"~ {ref}{where}: restore data from revision {rev}"]
    elif v == "restore":
        lines = [f"~ {ref}: set default version -> {_flag(cmd, 'version') or 'last known-good'}"]
    elif v == "renew":
        lines = [f"~ {ref}{where}: issue renewal request" +
                 (" for every matching certificate" if "all" in cmd.flags else "")]
    elif v == "rotate":
        lines = [f"~ {ref}{where}: generate new credential, stage it, roll dependent workloads"]
    elif v == "sync":
        lines = [f"~ {ref}{where}: annotate force-sync, refresh target secret"]
    elif v == "drain":
        lines = [f"~ {ref}: cordon (spec.unschedulable=true)", f"- {ref}: evict pods"]
    elif v == "replace":
        lines = [f"~ {ref}: cordon and drain", f"- {ref}: terminate instance",
                 "+ node group: launch replacement instance"]
    elif v == "delete":
        sel = _flag(cmd, "selector")
        lines = [f"- {ref}{where}" + (f": every object matching {sel}" if sel else "")]
    elif v == "run":
        lines = [f"+ job.batch/{cmd.name or cmd.kind}-manual-<id>{where}"]
    elif v == "restart":
        lines = [f"~ {ref}{where}: spec.template.metadata.annotations"
                 f"[kubectl.kubernetes.io/restartedAt] -> <now>"]
    else:
        changes = [f"{k} -> {val}" for k, val in cmd.flags.items() if val is not True]
        lines = [f"~ {ref}{where}" + (f": {', '.join(changes)}" if changes else "")]
    return lines


_K8S_AUDIT_VERB = {
    "rollback": "update", "restore": "update", "renew": "patch", "rotate": "update",
    "sync": "patch", "drain": "create", "replace": "delete", "patch": "patch",
    "apply": "patch", "expand": "patch", "run": "create", "delete": "delete",
    "restart": "patch",
}


def _cloudtrail(cmd: OpsCommand, account_id: str = AWS_ACCOUNT_ID) -> tuple[str, str, dict[str, str]]:
    """(eventSource, eventName, requestParameters) for a cloud-account target."""
    name = cmd.name or cmd.kind
    if cmd.kind == "iam-policy":
        params = {"policyArn": f"arn:aws:iam::{account_id}:policy/{name}"}
        if _flag(cmd, "version"):
            params["versionId"] = _flag(cmd, "version")
        event = "SetDefaultPolicyVersion" if cmd.verb in ("restore", "rollback") else "CreatePolicyVersion"
        return "iam.amazonaws.com", event, params
    if cmd.kind in ("iam-role-trust", "iam-role"):
        event = "UpdateAssumeRolePolicy" if cmd.kind == "iam-role-trust" else "UpdateRole"
        return "iam.amazonaws.com", event, {"roleName": name}
    if cmd.kind == "nodegroup":
        params = {"nodegroupName": name}
        if _flag(cmd, "instance-types"):
            # CloudTrail renders the list the way the scenarios' change events do: [a,b]
            params["instanceTypes"] = "[" + ",".join(
                s.strip() for s in str(_flag(cmd, "instance-types")).split(",") if s.strip()) + "]"
        event = {"patch": "UpdateNodegroupConfig", "apply": "UpdateNodegroupConfig",
                 "rollback": "UpdateNodegroupVersion", "restore": "UpdateNodegroupVersion",
                 "delete": "DeleteNodegroup"}.get(cmd.verb, "UpdateNodegroupConfig")
        return "eks.amazonaws.com", event, params
    if cmd.kind == "registry-credential":
        event = {"rotate": "RotateSecret", "restore": "RestoreSecret",
                 "delete": "DeleteSecret"}.get(cmd.verb, "PutSecretValue")
        return "secretsmanager.amazonaws.com", event, {"secretId": f"{cmd.namespace + '/' if cmd.namespace else ''}{name}"}
    return "eks.amazonaws.com", cmd.verb.capitalize() + cmd.kind.title().replace("-", ""), {"name": name}


def audit_record(cmd: OpsCommand, change_id: str, account_id: str = AWS_ACCOUNT_ID,
                 user: str = OPS_USER) -> tuple[str, dict[str, Any]]:
    """(body, attributes) of the audit line an applied command leaves behind."""
    if cmd.kind in AWS_KINDS:
        source, event, params = _cloudtrail(cmd, account_id)
        rp = " ".join(f"requestParameters.{k}={v}" for k, v in params.items())
        body = (f"cloudtrail: eventSource={source} eventName={event} "
                f"userIdentity={user} {rp} errorCode=none change_id={change_id}")
        attrs = {
            "cloud.provider": "aws",
            "cloud.account.id": account_id,
            "aws.cloudtrail.event_source": source,
            "aws.cloudtrail.event_name": event,
            "user.name": user,
            "change.id": change_id,
            "event.action": event,
        }
        return body, attrs
    verb = _K8S_AUDIT_VERB.get(cmd.verb, "patch")
    resource = _RESOURCE_PLURAL.get(cmd.kind, cmd.kind + "s")
    if cmd.verb == "drain":
        resource = "pods/eviction"
    if cmd.verb == "delete" and not cmd.name:
        verb = "deletecollection"
    status = 201 if verb == "create" else 200
    # Same shape as the audit lines the scenarios' change events use.
    parts = ["audit: level=Metadata stage=ResponseComplete", f"verb={verb}", f"resource={resource}"]
    if cmd.namespace:
        parts.append(f"namespace={cmd.namespace}")
    if cmd.name:
        parts.append(f"name={cmd.name}")
    parts.append(f"user={user}")
    sel = _flag(cmd, "selector")
    if sel:
        parts.append(f'labelSelector="{sel}"')
    detail = describe(cmd).replace('"', "'")
    parts += [f"code={status}", f'change="{detail}"', f"change_id={change_id}"]
    attrs: dict[str, Any] = {
        "k8s.audit.verb": verb,
        "k8s.audit.resource": resource,
        "k8s.audit.response_code": status,
        "user.name": user,
        "change.id": change_id,
        "event.action": cmd.verb,
    }
    if cmd.name:
        attrs["k8s.audit.object_name"] = cmd.name
    if cmd.namespace:
        attrs["k8s.namespace.name"] = cmd.namespace
    return " ".join(parts), attrs


_AUDIT_KV = re.compile(r"\b(resource|namespace|name)=(\S+)")


def _fix_line_key(cmd: OpsCommand) -> tuple[str, ...]:
    """What identifies *cmd*'s audit line: (eventName,) for AWS, else (resource, ns, name)."""
    if cmd.kind in AWS_KINDS:
        return (_cloudtrail(cmd)[1],)
    resource = "pods/eviction" if cmd.verb == "drain" else _RESOURCE_PLURAL.get(cmd.kind, cmd.kind + "s")
    return (resource, cmd.namespace or "", cmd.name or "")


def _channel_fix_line(inst: Any, cmd: OpsCommand) -> tuple[int, int, dict[str, Any]] | None:
    """The active channel's own audit line for *cmd*, when *cmd* is one of its fix steps.

    Scenarios describe the right fix's audit record in ``remediation_effects.log_events``
    with more detail (e.g. ``rollback to revision 11 (job-operator:v0.8.4)``). opsctl
    emits that line as the command's audit record instead of its generic one, so each
    action leaves exactly one record.
    """
    controller = inst.chaos_controller
    registry = getattr(controller, "_channel_registry", {}) or {}
    key = _fix_line_key(cmd)
    for ch in controller.get_active_channels():
        ch_def = registry.get(ch) or {}
        if not any(matches(cmd, exp) for exp in expected_commands(ch_def.get("remediation_command"))):
            continue
        events = (ch_def.get("remediation_effects") or {}).get("log_events") or []
        for idx, evt in enumerate(events):
            body = str((evt or {}).get("body") or "")
            if cmd.kind in AWS_KINDS:
                if body.startswith("cloudtrail:") and f"eventName={key[0]} " in body + " ":
                    return ch, idx, evt
                continue
            if not body.startswith("audit:"):
                continue
            kv = dict(_AUDIT_KV.findall(body))
            if (kv.get("resource"), kv.get("namespace", ""), kv.get("name", "")) == key:
                return ch, idx, evt
    return None


def _audit_service(inst: Any) -> str | None:
    services = getattr(getattr(inst, "ctx", None), "services", None) or {}
    if not services:
        scenario = getattr(inst, "scenario", None)
        services = getattr(scenario, "services", None) or {}
    if "kube-apiserver" in services:
        return "kube-apiserver"
    return next(iter(services), None)


def _side_effects(cmd: OpsCommand, change_id: str,
                  user: str = OPS_USER) -> list[tuple[str, str, dict[str, Any]]]:
    """(severity, body, attrs) for the obvious follow-on records of a command."""
    if cmd.verb == "restart":
        ref = _ref(cmd)
        attrs = {"k8s.namespace.name": cmd.namespace, "change.id": change_id,
                 "user.name": user, "event.action": "rollout-restart"}
        if cmd.name:
            attrs[f"k8s.{cmd.kind}.name"] = cmd.name
        return [("INFO", f"{ref} restarted (kubectl.kubernetes.io/restartedAt annotation set); "
                         f"rolling update started", attrs)]
    return []


# ── Entry points ──────────────────────────────────────────────────────────


# opsctl flag -> workload field it changes in the pinned platform (schema 1.3 k8s_topology).
_WORKLOAD_FLAGS = {
    "memory-limit": "memory_limit", "memory-request": "memory_request",
    "cpu-limit": "cpu_limit", "cpu-request": "cpu_request",
    "image": "image", "replicas": "replicas",
}
_WORKLOAD_KINDS = {"deployment", "daemonset", "statefulset"}
ROLLOUT_SECONDS = 90.0   # a rolling update of a few pods (maxSurge 25%)
PVC_RESIZE_SECONDS = 45.0


def change_state(inst: Any, cmd: OpsCommand) -> tuple[dict[str, Any], float]:
    """The platform change an applied command makes, as ``kubernetes_otel`` state.

    Every applied change takes effect in telemetry on its own, whether or not it
    completes a fix: patching a memory limit rolls the pods to the new limit even while
    the fault continues. Returns ({}, 0) when the command changes nothing the platform
    model reports, or the scenario has no pinned topology.
    """
    scenario = getattr(getattr(inst, "ctx", None), "scenario", None) or getattr(inst, "scenario", None)
    topo = getattr(scenario, "k8s_topology", None)
    if not topo:
        return {}, 0.0
    default = topo.get("default_cluster") or (topo.get("clusters") or [{}])[0].get("name")
    cluster = next((c for c in topo.get("clusters") or [] if c.get("name") == default), {})
    workloads = {w.get("name"): w for w in cluster.get("workloads") or []}
    pools = {p.get("name") for p in cluster.get("node_pools") or []}

    if cmd.kind in _WORKLOAD_KINDS and cmd.name in workloads and cmd.verb in ("patch", "apply", "scale"):
        changes = {}
        for flag, fld in _WORKLOAD_FLAGS.items():
            val = cmd.flags.get(flag)
            if val is None or val is True:
                continue
            changes[fld] = int(val) if fld == "replicas" else val
        if changes:
            return {"kubernetes_otel": {"workloads": {cmd.name: changes}}}, ROLLOUT_SECONDS
    if cmd.kind == "pvc" and cmd.verb == "expand" and isinstance(cmd.flags.get("size"), str):
        owner = next((n for n, w in workloads.items() if (w.get("pvc") or {}).get("name") == cmd.name), None)
        if owner:
            return ({"kubernetes_otel": {"workloads": {owner: {"pvc_capacity": cmd.flags["size"]}}}},
                    PVC_RESIZE_SECONDS)
    if cmd.kind == "nodegroup" and cmd.name in pools and isinstance(cmd.flags.get("instance-types"), str):
        types = [t.strip() for t in cmd.flags["instance-types"].split(",") if t.strip()]
        return {"kubernetes_otel": {"node_pools": {cmd.name: {"instance_types": types}}}}, 0.0
    return {}, 0.0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def list_actions() -> dict[str, Any]:
    return {
        "usage": GRAMMAR,
        "verbs": [{"verb": v, "help": h} for v, h in VERBS.items()]
        + [{"verb": "plan", "help": PLAN_HELP}],
        "flags": {
            "-n, --namespace": "Namespace of the target object (omit for cluster or account scoped objects)",
            "--flag value | --flag=value": "Setting to change; lists are comma-separated",
            "--switch": "Boolean option, e.g. --all, --ignore-daemonsets",
        },
    }


def execute(inst: Any, command: str | list[str], dry_run: bool = False,
            user: str | None = None) -> dict[str, Any]:
    """Run one opsctl invocation against *inst*. Raises OpsError on bad input.

    *user* is the actor recorded in the audit lines (default ``opsctl/claude-code``);
    automation that applies a runbook fix on its own passes its own identity.
    """
    user = (user or OPS_USER).strip()[:128] or OPS_USER
    cmd = parse_command(command)
    dry_run = dry_run or cmd.dry_run
    base = {
        "action": cmd.verb,
        "target": cmd.target,
        "namespace": cmd.namespace or None,
        "timestamp": _now_iso(),
    }
    if dry_run:
        return {"status": "planned", **base,
                "message": f"{_ref(cmd)} would be changed (dry run, nothing applied)",
                "changes": describe_plan(cmd)}

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
            body, attrs = audit_record(cmd, change_id, _account_id(inst), user)
            controller.queue_event(service, body, attributes=attrs)
        for i, (sev, sbody, sattrs) in enumerate(_side_effects(cmd, change_id, user), start=1):
            controller.queue_event(service, sbody, severity=sev, attributes=sattrs,
                                   delay_seconds=i)

    state, transition = change_state(inst, cmd)
    if state and hasattr(controller, "apply_change_state"):
        controller.apply_change_state(state, transition)

    for ch in _record_and_complete(inst, cmd):
        logger.info("ops: change %s completes the fix for channel %d", change_id, ch)
        inst.resolve_channel(ch, force=True, actor=user)

    return {"status": "applied", **base, "message": describe(cmd, change_id),
            "change_id": change_id}


def reset(inst: Any) -> dict[str, Any]:
    controller = inst.chaos_controller
    controller.reset_platform_state()
    reset_progress(controller)
    return {"status": "reset", "timestamp": _now_iso()}
