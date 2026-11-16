---
name: platform-ops
description: Apply changes to the Kubernetes platform and its cloud account with opsctl. Use when carrying out a remediation recommendation or runbook step, such as rolling back a ConfigMap, restoring an IAM policy version, renewing certificates, patching a node group or deployment limits, draining or replacing a node, expanding a PVC, rotating or syncing a secret, applying a rate limit, or running a cleanup job. Covers dry run, apply and verification of recovery in telemetry.
---

# Platform changes with opsctl

All changes to the cluster and the cloud account go through `opsctl`. Each applied
change is written to the audit log (kube-apiserver audit or CloudTrail) with a change id.
Do not use `kubectl`, `aws` or ad-hoc API calls to change the platform.

`opsctl` is on PATH. If it is not found, call it by its full path:
`/root/.claude/skills/platform-ops/bin/opsctl`.

```
opsctl [plan] <verb> <kind>[/<name>] [-n <namespace>] [--flag value | --flag=value | --switch]...
```

Verbs: `rollback`, `restore`, `renew`, `rotate`, `sync`, `drain`, `replace`, `patch`,
`apply`, `expand`, `run`, `delete`, `restart`. `opsctl --help` and
`opsctl <verb> --help` show the details.

## Change procedure

1. **Read the recommendation and its runbook.** Take the remediation commands exactly as
   written: the same target, namespace and values. A runbook can list more than one
   command; apply all of them. If you were given only a command, or no runbook, look the
   runbook up in the Wiki first (see "Finding the runbook") and check whether it lists
   more commands than you were given. Note its **Scope** (cluster, node pool, zone) and the
   signals under "Recovery is confirmed when" with their transition times. Do not
   substitute a restart for the documented fix.
2. **Dry run each command** with `opsctl plan <verb> ...`. Check that the target and the
   changes are what the runbook describes.
3. **Apply each command** in the order given. Record the change id that each one prints.
   If a command fails, stop and report the error. Do not improvise a different change.
4. **Verify recovery in telemetry.** A command that succeeds tells you only that the change
   was applied, not that the problem is gone. Wait for the stated transition time, then
   query the signals from "Recovery is confirmed when" (see "Where the telemetry is").
   Always filter to the cluster in the runbook's Scope: the other clusters were not
   changed and can legitimately still show the old values. Compare the window after the
   change with the window before it, and find your change in the audit trail by its
   `change.id`. If the signals have not recovered after the transition time, report that.
   Do not apply more changes than the runbook lists.
5. **Report** what changed (each command and its change id), when it was applied, and
   the telemetry that confirms recovery, or the telemetry that shows it has not recovered.

## Where the telemetry is

The data is OpenTelemetry, not ECS: there are no `kubernetes.*`, `service.name` or
`log.level` top-level fields. Run ES|QL with curl (or the Elastic MCP tools):

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" -H "Content-Type: application/json" \
  "$ELASTICSEARCH_URL/_query?format=txt" -d '{"query": "<ES|QL>"}'
```

- **Logs:** `FROM logs.otel.*`. Message in `body.text`, level in `severity_text`, emitter in
  `resource.attributes.service.name`, placement in `resource.attributes.k8s.cluster.name`,
  `...k8s.namespace.name`, `...k8s.pod.name`, `...k8s.node.name`. Root-cause and change
  details are in `attributes.*` (for example `attributes.change.id`).
- **Kubernetes object state** (limits and requests, restarts, readiness, replicas, HPA,
  DaemonSets, node conditions, pod phase): `FROM metrics-k8sclusterreceiver.otel-*`, for
  example `metrics.k8s.container.memory_limit`, `metrics.k8s.container.restarts`,
  `metrics.k8s.hpa.current_replicas`, `metrics.k8s.node.condition_ready`.
- **Usage** (CPU, memory, filesystem, volumes): `FROM metrics-kubeletstatsreceiver.otel-*`, for
  example `metrics.k8s.container.memory_limit_utilization`, `metrics.k8s.volume.capacity`.
- **Service metrics** (CoreDNS, ingress-nginx, cert-manager, the autoscaler, the API server,
  tenant-gateway): `FROM metrics-generic.otel-default`, keyed by
  `resource.attributes.service.name`, for example `metrics.coredns.dns.responses_per_s` split by
  `attributes.rcode`. They are not in the Kubernetes metric indices above. Before you conclude
  that a metric doesn't exist, look here.
- Object names are resource attributes: `resource.attributes.k8s.deployment.name`,
  `...k8s.daemonset.name`, `...k8s.hpa.name`, `...k8s.persistentvolumeclaim.name`,
  ``...`k8s.node.label.eks.amazonaws.com/nodegroup` `` (backtick-quote it in ES|QL because of the `/`).

Example: the memory limit each controller pod reports now, in one cluster:

```
FROM metrics-k8sclusterreceiver.otel-*
| WHERE @timestamp > NOW() - 5 minutes
  AND resource.attributes.k8s.cluster.name == "prod-use1"
  AND resource.attributes.k8s.deployment.name == "ingress-nginx-controller"
  AND metrics.k8s.container.memory_limit IS NOT NULL
| STATS last_seen = MAX(@timestamp), limit = MAX(metrics.k8s.container.memory_limit)
    BY pod = resource.attributes.k8s.pod.name
| SORT last_seen DESC
```

Changing a Deployment's pod template (image, requests, limits) rolls it out like
Kubernetes does: a new ReplicaSet with new pod names replaces the old pods one by one.
During the rollout old pods (old values) and new pods (new values) both report; afterwards
the old pod names stop reporting. Judge by the pods seen in the last minute or two, not by
every pod in a longer window.

## Finding the runbook

Runbooks live in the Wiki (the Nightshift knowledge store). List the pages and pick the
runbook (`entity_type` `runbook`) for the failing component or error signature:

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" -H "kbn-xsrf: true" \
  -H "x-elastic-internal-origin: kibana" "$KIBANA_URL/internal/nightshift/wiki/pages" \
  | jq -r '.. | objects | select(.entity_type? == "runbook") | "\(.id)\t\(.title)"'
```

Fetch one page with `.../internal/nightshift/wiki/pages/<id>`. Its "Established facts"
section has the Scope, the remediation commands and "Recovery is confirmed when".
