#!/usr/bin/env python3
"""Azure Activity Log Generator — sends Azure control-plane audit logs via OTLP.

Emits data matching the azure_activity_otel content package
(discovery.datasets: azure.activity.otel), which populates the bundled
Azure Activity Log dashboards (Overview, Administrative Operations,
Platform Health).

Field names verified against:
  - elastic/integrations/packages/azure_activity_otel/kibana/dashboard/*.json
  - opentelemetry-collector-contrib azureencodingextension activity log mapping
    (azure.result.type, azure.operation.duration, network.peer.address,
     cloud.resource_id, identity/principal fields, ServiceHealth /
     ResourceHealth / Recommendation / Alert category attributes)

Operation names are UPPERCASE (e.g. MICROSOFT.COMPUTE/VIRTUALMACHINES/WRITE)
because dashboard ES|QL uses case-sensitive LIKE/equality against that form.

Usage (standalone):
    python3 -m generators.azure_activity_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/azure_activity_otel@0.2.0"

import logging
import math
import os
import random
import signal
import threading
import uuid

from app.telemetry import OTLPClient
from app.config import NAMESPACE
from generator_engine.db_common import (
    build_audit_log_record,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

AZURE_ACTIVITY_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/"
    "extension/encoding/azureencodingextension"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("azure-activity-generator")
BATCH_INTERVAL_MIN = 5
BATCH_INTERVAL_MAX = 15

# Dashboard-queried log attributes (azure_activity_otel Lens panels).
LOG_ATTRIBUTES = (
    "azure.category",
    "azure.correlation_id",
    "azure.duration",
    "azure.identity.authorization.evidence.principal.id",
    "azure.identity.authorization.evidence.principal.type",
    "azure.operation.duration",
    "azure.operation.name",
    "azure.recommendation.category",
    "azure.recommendation.impact",
    "azure.recommendation.name",
    "azure.resourcehealth.cause",
    "azure.resourcehealth.previous_state",
    "azure.resourcehealth.state",
    "azure.resourcehealth.title",
    "azure.result.description",
    "azure.result.type",
    "azure.servicehealth.incident.type",
    "azure.servicehealth.region",
    "azure.servicehealth.service",
    "azure.servicehealth.state",
    "azure.servicehealth.title",
    "cloud.region",
    "cloud.resource_id",
    "network.peer.address",
    "user.id",
)

# Administrative operations — UPPERCASE to match Azure Activity Log export +
# dashboard ES|QL (LIKE "*DELETE*", == "MICROSOFT.RESOURCES/DEPLOYMENTS/WRITE").
_ADMIN_OPERATIONS = [
    ("MICROSOFT.COMPUTE", "MICROSOFT.COMPUTE/VIRTUALMACHINES/WRITE"),
    ("MICROSOFT.COMPUTE", "MICROSOFT.COMPUTE/VIRTUALMACHINES/DELETE"),
    ("MICROSOFT.COMPUTE", "MICROSOFT.COMPUTE/VIRTUALMACHINES/DEALLOCATE/ACTION"),
    ("MICROSOFT.COMPUTE", "MICROSOFT.COMPUTE/VIRTUALMACHINES/READ"),
    ("MICROSOFT.STORAGE", "MICROSOFT.STORAGE/STORAGEACCOUNTS/WRITE"),
    ("MICROSOFT.NETWORK", "MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/WRITE"),
    ("MICROSOFT.NETWORK", "MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/DELETE"),
    ("MICROSOFT.NETWORK", "MICROSOFT.NETWORK/ROUTETABLES/READ"),
    ("MICROSOFT.KEYVAULT", "MICROSOFT.KEYVAULT/VAULTS/READ"),
    ("MICROSOFT.AUTHORIZATION", "MICROSOFT.AUTHORIZATION/DENYASSIGNMENTS/WRITE"),
    ("MICROSOFT.RESOURCES", "MICROSOFT.RESOURCES/DEPLOYMENTS/WRITE"),
    ("MICROSOFT.CONTAINERSERVICE", "MICROSOFT.CONTAINERSERVICE/MANAGEDCLUSTERS/READ"),
    ("MICROSOFT.SQL", "MICROSOFT.SQL/SERVERS/DATABASES/READ"),
]

# Sensitive-data-access operations — excluded from the healthy baseline pool
# above; only emitted when the sensitive_key_access impact key is active
# (LISTKEYS / KeyVault secret reads breach the OOTB alert on any occurrence).
_SENSITIVE_OPERATIONS = [
    ("MICROSOFT.KEYVAULT", "MICROSOFT.KEYVAULT/VAULTS/SECRETS/READ"),
    ("MICROSOFT.STORAGE", "MICROSOFT.STORAGE/STORAGEACCOUNTS/LISTKEYS/ACTION"),
]

_AZURE_REGIONS = [
    "eastus", "eastus2", "westus", "westus2", "centralus",
    "northeurope", "westeurope", "southeastasia",
]

_PRINCIPAL_TYPES = ("User", "ServicePrincipal", "ManagedIdentity")

_RESOURCE_TYPES = [
    ("MICROSOFT.COMPUTE", "VIRTUALMACHINES", "vm"),
    ("MICROSOFT.STORAGE", "STORAGEACCOUNTS", "sa"),
    ("MICROSOFT.NETWORK", "NETWORKSECURITYGROUPS", "nsg"),
    ("MICROSOFT.KEYVAULT", "VAULTS", "kv"),
    ("MICROSOFT.SQL", "SERVERS", "sql"),
    ("MICROSOFT.CONTAINERSERVICE", "MANAGEDCLUSTERS", "aks"),
    ("MICROSOFT.WEB", "SITES", "app"),
]

_SERVICE_HEALTH_SERVICES = [
    ("App Service", "East US"),
    ("Virtual Machines", "West Europe"),
    ("Azure Kubernetes Service", "East US 2"),
    ("Storage", "Southeast Asia"),
    ("SQL Database", "Central US"),
]

_RECOMMENDATIONS = [
    ("Enable soft delete for blobs", "Security", "High"),
    ("Consider having at least two origins", "HighAvailability", "High"),
    ("Right-size underutilized virtual machines", "Cost", "Medium"),
    ("Enable Azure Defender for Storage", "Security", "Medium"),
    ("Use availability zones for AKS", "HighAvailability", "Low"),
]

# Category mix: Administrative dominates; platform-health categories fill those dashboards.
_CATEGORY_WEIGHTS = [
    ("Administrative", 55),
    ("ServiceHealth", 15),
    ("ResourceHealth", 15),
    ("Recommendation", 8),
    ("Alert", 7),
]


def _default_cloud() -> dict:
    return {"provider": "azure", "platform": "azure_vm",
            "region": "eastus", "availability_zone": "1"}


def _synthetic_uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128)))


def _resource_id(
    subscription_id: str,
    resource_group: str,
    provider: str,
    resource_type: str,
    name: str,
) -> str:
    return (
        f"/SUBSCRIPTIONS/{subscription_id.upper()}"
        f"/RESOURCEGROUPS/{resource_group.upper()}"
        f"/PROVIDERS/{provider}/{resource_type}/{name.upper()}"
    )


def _pick_category(rng: random.Random) -> str:
    total = sum(w for _, w in _CATEGORY_WEIGHTS)
    roll = rng.randint(1, total)
    cumulative = 0
    for name, weight in _CATEGORY_WEIGHTS:
        cumulative += weight
        if roll <= cumulative:
            return name
    return "Administrative"


def _principal_meta(principal: str, rng: random.Random) -> tuple[str, str]:
    """Return (principal_type, user.id) for a synthetic Azure AD principal."""
    if principal.startswith("ci-") or "service" in principal.lower():
        ptype = "ServicePrincipal"
    elif rng.random() < 0.15:
        ptype = "ManagedIdentity"
    else:
        ptype = rng.choice(("User", "User", "ServicePrincipal"))
    return ptype, _synthetic_uuid(rng)


def _peer_address(rng: random.Random) -> str:
    return f"203.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"


def _fault_actor(
    principals: list[str],
    rng: random.Random,
    op_pool: list[tuple[str, str]] | None = None,
) -> dict:
    """One user, address and target held for the life of a fault.

    The OOTB rules build each alert id from caller, principal type, peer address
    and resource, so a fault that draws them per record opens new alerts and
    recovers the old ones on every rule run.
    """
    users = [p for p in principals if not (p.startswith("ci-") or "service" in p.lower())]
    provider, op_name = rng.choice(op_pool or _ADMIN_OPERATIONS)
    resource_type = op_name.split("/")[1]
    return {
        "principal": rng.choice(users or principals),
        "principal_id": _synthetic_uuid(rng),
        "peer": _peer_address(rng),
        "op": (provider, op_name),
        "resource_name": f"{resource_type.lower()}-{rng.randint(1, 99):02d}",
    }


def _identity_attrs(
    caller: str,
    principal_type: str,
    peer: str,
    rng: random.Random,
    *,
    email: str | None = None,
    principal_id: str | None = None,
) -> dict:
    """Fields for Recent Activity / Top Callers columns on every category.

    Overview ``Recent Activity Log Events`` KEEP includes principal.type,
    user.id, and network.peer.address for all azure.category values — not only
    Administrative. Platform categories must still populate them so table rows
    are not sparse nulls.
    """
    pid = principal_id or _synthetic_uuid(rng)
    attrs = {
        "azure.identity.authorization.evidence.principal.type": principal_type,
        "azure.identity.authorization.evidence.principal.id": pid,
        # Caller column is user.id (alerts COALESCE with principal.id).
        "user.id": caller,
        "network.peer.address": peer,
    }
    if email:
        attrs["user.email"] = email
    return attrs


_ROLE_NAMES = (
    "Owner",
    "Contributor",
    "Reader",
    "User Access Administrator",
    "Virtual Machine Contributor",
)


def _duration_attrs(duration_ms: int) -> dict[str, int]:
    """Operation duration (ms) under both names consumers read.

    ``azure.operation.duration`` feeds the package dashboards; the package SLO
    "Administrative operation latency" averages ``azure.duration`` (< 3000 ms).
    """
    return {"azure.operation.duration": duration_ms, "azure.duration": duration_ms}


def _role_assignment_write_record(
    principals: list[str],
    subscription_id: str,
    resource_group: str,
    region: str,
    rng: random.Random,
    jitter: int,
    actor: dict | None = None,
) -> dict:
    """Administrative ROLEASSIGNMENTS/WRITE with evidence.role for OOTB alerts.

    ``actor`` (from ``_fault_actor``) pins the alert's caller, principal type and
    peer address while the fault is active.
    """
    provider = "MICROSOFT.AUTHORIZATION"
    op_name = "MICROSOFT.AUTHORIZATION/ROLEASSIGNMENTS/WRITE"
    resource_type = "ROLEASSIGNMENTS"
    resource_name = f"roleassignment-{rng.randint(1, 99):02d}"
    resource_id = _resource_id(
        subscription_id, resource_group, provider, resource_type, resource_name,
    )
    if actor:
        principal, ptype, principal_id, peer = (
            actor["principal"], "User", actor["principal_id"], actor["peer"],
        )
    else:
        principal = rng.choice(principals)
        ptype, principal_id = _principal_meta(principal, rng)
        peer = _peer_address(rng)
    caller = principal if ptype == "User" else principal_id
    extra = {
        "azure.category": "Administrative",
        "azure.correlation_id": _synthetic_uuid(rng),
        "azure.operation.name": op_name,
        **_duration_attrs(rng.randint(20, 3000)),
        "azure.result.type": "Succeeded",
        "azure.result.description": "The operation completed successfully.",
        "azure.identity.authorization.evidence.role": rng.choice(_ROLE_NAMES),
        "cloud.region": region,
        "cloud.resource_id": resource_id,
        **_identity_attrs(
            caller, ptype, peer, rng, email=principal, principal_id=principal_id,
        ),
    }
    return build_audit_log_record(
        actor=principal,
        action=op_name,
        outcome="success",
        source_address=peer,
        event_name="azure.resource.log",
        severity="INFO",
        extra_attrs=extra,
        jitter_ns=jitter,
    )


def _admin_record(
    principals: list[str],
    subscription_id: str,
    resource_group: str,
    region: str,
    rng: random.Random,
    error_rate: float,
    jitter: int,
    op_pool: list[tuple[str, str]] | None = None,
    actor: dict | None = None,
) -> dict:
    """An Administrative record; ``actor`` (from ``_fault_actor``) pins who did what to which resource."""
    if actor:
        principal = actor["principal"]
        provider, op_name = actor["op"] if not op_pool else rng.choice(op_pool)
        resource_name = actor["resource_name"]
    else:
        principal = rng.choice(principals)
        provider, op_name = rng.choice(op_pool or _ADMIN_OPERATIONS)
        resource_name = None
    resource_type = op_name.split("/")[1]
    resource_name = resource_name or f"{resource_type.lower()}-{rng.randint(1, 99):02d}"
    resource_id = _resource_id(
        subscription_id, resource_group, provider, resource_type, resource_name,
    )
    if actor:
        ptype, principal_id, peer = "User", actor["principal_id"], actor["peer"]
    else:
        ptype, principal_id = _principal_meta(principal, rng)
        peer = _peer_address(rng)
    # user.id: email for Users; object id for SP / ManagedIdentity.
    caller = principal if ptype == "User" else principal_id

    # Mix Start (in-progress), Succeeded, and Failed — dashboards exclude Start
    # from failure-rate denominators and match Failed/Failure for failures.
    roll = rng.random()
    if roll < max(error_rate, 0.02):
        result_type = rng.choice(("Failed", "Failure"))
        result_desc = rng.choice((
            "The resource operation failed.",
            "Authorization failed.",
            "Conflict: resource already exists.",
            "BadRequest: invalid parameter.",
        ))
        outcome, severity = "failure", "ERROR"
        duration_ms = rng.randint(50, 8000)
    elif roll < max(error_rate, 0.02) + 0.08:
        result_type = "Start"
        result_desc = "Started."
        outcome, severity = "unknown", "INFO"
        duration_ms = 0
    else:
        result_type = "Succeeded"
        result_desc = "The operation completed successfully."
        outcome, severity = "success", "INFO"
        duration_ms = rng.randint(20, 3000)

    extra = {
        "azure.category": "Administrative",
        "azure.correlation_id": _synthetic_uuid(rng),
        "azure.operation.name": op_name,
        **_duration_attrs(duration_ms),
        "azure.result.type": result_type,
        "azure.result.description": result_desc,
        "cloud.region": region,
        "cloud.resource_id": resource_id,
        **_identity_attrs(
            caller, ptype, peer, rng, email=principal, principal_id=principal_id,
        ),
    }
    # OOTB privilege-escalation alert STATS VALUES(...evidence.role).
    if "ROLEASSIGNMENT" in op_name.upper():
        extra["azure.identity.authorization.evidence.role"] = rng.choice(_ROLE_NAMES)
    return build_audit_log_record(
        actor=principal,
        action=op_name,
        outcome=outcome,
        source_address=peer,
        event_name="azure.resource.log",
        severity=severity,
        extra_attrs=extra,
        jitter_ns=jitter,
    )


def _service_health_record(
    subscription_id: str,
    region: str,
    rng: random.Random,
    jitter: int,
    *,
    degraded: bool = False,
    incident: dict | None = None,
) -> dict:
    """A ServiceHealth record; ``incident`` pins one active outage for the life of a fault.

    The OOTB rule groups by tracking id and title, so a fault must keep
    reporting the same incident rather than a new one per record.
    """
    if incident:
        service, svc_region = incident["service"]
        incident_type = "Incident"
    else:
        service, svc_region = rng.choice(_SERVICE_HEALTH_SERVICES)
        incident_type = rng.choice(("Incident", "Incident", "Maintenance"))
    if incident_type == "Incident":
        # Healthy baseline never leaves an incident Active (0=healthy alert).
        state = "Active" if degraded else "Resolved"
        title = f"{service} service issue in {svc_region}"
        result_type = "Active" if state == "Active" else "Resolved"
    else:
        state = "Active" if degraded else rng.choice(("Resolved", "Complete"))
        title = f"Routine Planned Maintenance for {service} in {svc_region}"
        result_type = "Resolved" if state in ("Resolved", "Complete") else "Active"

    actor = "AcmClient@microsoft.com"
    peer = _peer_address(rng)
    extra = {
        "azure.category": "ServiceHealth",
        "azure.correlation_id": _synthetic_uuid(rng),
        "azure.operation.name": (
            "MICROSOFT.SERVICEHEALTH/INCIDENT/ACTION"
            if incident_type == "Incident"
            else "MICROSOFT.SERVICEHEALTH/MAINTENANCE/ACTION"
        ),
        "azure.result.type": result_type,
        "azure.result.description": title,
        "azure.servicehealth.title": title,
        "azure.servicehealth.service": service,
        "azure.servicehealth.region": svc_region,
        "azure.servicehealth.incident.type": incident_type,
        "azure.servicehealth.state": state,
        # OOTB service-health alert filters is_synthetic and groups by tracking.id.
        "azure.servicehealth.is_synthetic": False,
        "azure.servicehealth.tracking.id": (
            incident["tracking_id"] if incident else _synthetic_uuid(rng)
        ),
        "cloud.region": region,
        "cloud.resource_id": f"/SUBSCRIPTIONS/{subscription_id.upper()}",
        **_identity_attrs(actor, "ServicePrincipal", peer, rng, email=actor),
    }
    return build_audit_log_record(
        actor=actor,
        action=extra["azure.operation.name"],
        outcome="success",
        source_address=peer,
        event_name="azure.resource.log",
        severity="WARN" if state == "Active" and incident_type == "Incident" else "INFO",
        extra_attrs=extra,
        jitter_ns=jitter,
    )


def _resource_health_record(
    subscription_id: str,
    resource_group: str,
    region: str,
    rng: random.Random,
    jitter: int,
    *,
    degraded: bool = False,
    outage: dict | None = None,
) -> dict:
    """A ResourceHealth record; ``outage`` pins one unhealthy resource for the life of a fault.

    The OOTB rule groups by resource id and title (which carries the state).
    """
    if outage:
        provider, rtype, prefix = outage["type"]
        name = outage["name"]
    else:
        provider, rtype, prefix = rng.choice(_RESOURCE_TYPES)
        name = f"{prefix}-{rng.randint(1, 40):02d}"
    resource_id = _resource_id(subscription_id, resource_group, provider, rtype, name)
    # Healthy baseline never reports Unavailable/Degraded (0=healthy alert).
    if outage:
        state = outage["state"]
    elif degraded:
        state = rng.choice(("Unavailable", "Degraded"))
    else:
        state = rng.choices(("Available", "Unknown"), weights=(85, 15))[0]
    previous = rng.choice(("Available", "Unavailable", "Degraded", "Unknown"))
    if previous == state:
        previous = "Available" if state != "Available" else "Unknown"
    cause = rng.choice((
        "PlatformInitiated", "UserInitiated", "Unknown",
        "Payload", "Downtime",
    ))
    title = f"Resource health status changed to {state}"
    actor = "Microsoft.ResourceHealth"
    peer = _peer_address(rng)

    extra = {
        "azure.category": "ResourceHealth",
        "azure.correlation_id": _synthetic_uuid(rng),
        "azure.operation.name": "MICROSOFT.RESOURCEHEALTH/HEALTHEVENT/ACTIVATED/ACTION",
        "azure.result.type": "Active" if state in ("Unavailable", "Degraded") else "Resolved",
        "azure.resourcehealth.title": title,
        "azure.resourcehealth.state": state,
        "azure.resourcehealth.previous_state": previous,
        "azure.resourcehealth.cause": cause,
        # OOTB resource-health alert STATS VALUES(azure.resourcehealth.type).
        "azure.resourcehealth.type": cause if cause in (
            "PlatformInitiated", "UserInitiated", "Unknown",
        ) else "Downtime",
        "cloud.region": region,
        "cloud.resource_id": resource_id,
        **_identity_attrs(actor, "ServicePrincipal", peer, rng),
    }
    return build_audit_log_record(
        actor=actor,
        action=extra["azure.operation.name"],
        outcome="failure" if state in ("Unavailable", "Degraded") else "success",
        source_address=peer,
        event_name="azure.resource.log",
        severity="ERROR" if state in ("Unavailable", "Degraded") else "INFO",
        extra_attrs=extra,
        jitter_ns=jitter,
    )


def _resource_outage(rng: random.Random) -> dict:
    """One resource that stays Unavailable or Degraded for the life of a fault."""
    resource_type = rng.choice(_RESOURCE_TYPES)
    return {
        "type": resource_type,
        "name": f"{resource_type[2]}-{rng.randint(1, 40):02d}",
        "state": rng.choice(("Unavailable", "Degraded")),
    }


def _recommendation_record(
    subscription_id: str,
    resource_group: str,
    region: str,
    rng: random.Random,
    jitter: int,
) -> dict:
    name, category, impact = rng.choice(_RECOMMENDATIONS)
    provider, rtype, prefix = rng.choice(_RESOURCE_TYPES)
    rname = f"{prefix}-{rng.randint(1, 40):02d}"
    resource_id = _resource_id(subscription_id, resource_group, provider, rtype, rname)

    actor = "Microsoft.Advisor"
    peer = _peer_address(rng)
    extra = {
        "azure.category": "Recommendation",
        "azure.correlation_id": _synthetic_uuid(rng),
        "azure.operation.name": "MICROSOFT.ADVISOR/RECOMMENDATIONS/AVAILABLE/ACTION",
        "azure.result.type": "Active",
        "azure.result.description": "A new recommendation is available.",
        "azure.recommendation.name": name,
        "azure.recommendation.category": category,
        "azure.recommendation.impact": impact,
        "cloud.region": region,
        "cloud.resource_id": resource_id,
        **_identity_attrs(actor, "ServicePrincipal", peer, rng),
    }
    return build_audit_log_record(
        actor=actor,
        action=extra["azure.operation.name"],
        outcome="success",
        source_address=peer,
        event_name="azure.resource.log",
        severity="INFO",
        extra_attrs=extra,
        jitter_ns=jitter,
    )


def _failed_admin_burst(
    subscription_id: str,
    resource_group: str,
    region: str,
    rng: random.Random,
    count: int,
) -> list[dict]:
    """Emit ``count`` identical Administrative failures for the OOTB alert.

    ``[Azure Activity OTel] Failed administrative operations`` groups
    ``BY azure.operation.name, caller, network.peer.address`` and requires
    ``failed_count >= 3``. Randomizing those dimensions under chaos spreads
    failures so no group reaches 3 — pin one identity and repeat.
    """
    provider, op_name = ("MICROSOFT.COMPUTE", "MICROSOFT.COMPUTE/VIRTUALMACHINES/WRITE")
    resource_type = "VIRTUALMACHINES"
    resource_name = "vm-chaos-01"
    resource_id = _resource_id(
        subscription_id, resource_group, provider, resource_type, resource_name,
    )
    # Stable caller / peer so every doc lands in the same STATS bucket.
    caller = "chaos-admin@gt-azureactivity.onmicrosoft.com"
    principal_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    peer = "203.0.113.50"
    records: list[dict] = []
    for i in range(count):
        extra = {
            "azure.category": "Administrative",
            "azure.correlation_id": _synthetic_uuid(rng),
            "azure.operation.name": op_name,
            **_duration_attrs(rng.randint(50, 8000)),
            "azure.result.type": "Failed" if i % 2 == 0 else "Failure",
            "azure.result.description": "The resource operation failed.",
            "cloud.region": region,
            "cloud.resource_id": resource_id,
            **_identity_attrs(
                caller, "User", peer, rng,
                email=caller, principal_id=principal_id,
            ),
        }
        records.append(build_audit_log_record(
            actor=caller,
            action=op_name,
            outcome="failure",
            source_address=peer,
            event_name="azure.resource.log",
            severity="ERROR",
            extra_attrs=extra,
            jitter_ns=rng.randint(0, int(BATCH_INTERVAL_MAX * 1e9)),
        ))
    return records


def _security_record(
    subscription_id: str,
    resource_group: str,
    region: str,
    rng: random.Random,
    jitter: int,
) -> dict:
    """Security-category Activity Log — OOTB alert filters ``azure.category == "Security"``."""
    provider, rtype, prefix = rng.choice(_RESOURCE_TYPES)
    rname = f"{prefix}-{rng.randint(1, 40):02d}"
    resource_id = _resource_id(subscription_id, resource_group, provider, rtype, rname)
    op_name = "MICROSOFT.SECURITY/LOCATIONS/ALERTS/ACTIVATE/ACTION"
    caller = "security-center@microsoft.com"
    peer = _peer_address(rng)
    extra = {
        "azure.category": "Security",
        "azure.correlation_id": _synthetic_uuid(rng),
        "azure.operation.name": op_name,
        "azure.result.type": "Active",
        "azure.result.description": "Security alert activated.",
        "cloud.region": region,
        "cloud.resource_id": resource_id,
        **_identity_attrs(caller, "ServicePrincipal", peer, rng, email=caller),
    }
    return build_audit_log_record(
        actor=caller,
        action=op_name,
        outcome="success",
        source_address=peer,
        event_name="azure.resource.log",
        severity="WARN",
        extra_attrs=extra,
        jitter_ns=jitter,
    )


def _alert_record(
    subscription_id: str,
    resource_group: str,
    region: str,
    rng: random.Random,
    jitter: int,
) -> dict:
    provider, rtype, prefix = rng.choice(_RESOURCE_TYPES)
    rname = f"{prefix}-{rng.randint(1, 40):02d}"
    resource_id = _resource_id(subscription_id, resource_group, provider, rtype, rname)
    result_type = rng.choice(("Activated", "Active", "Resolved", "Activated"))
    actor = "Microsoft.Insights"
    peer = _peer_address(rng)

    extra = {
        "azure.category": "Alert",
        "azure.correlation_id": _synthetic_uuid(rng),
        "azure.operation.name": f"MICROSOFT.INSIGHTS/ALERTRULES/{result_type.upper()}/ACTION",
        "azure.result.type": result_type,
        "azure.result.description": f"Alert rule {result_type.lower()}.",
        "azure.alert.rule.name": f"cpu-high-{rng.randint(1, 20)}",
        "cloud.region": region,
        "cloud.resource_id": resource_id,
        **_identity_attrs(actor, "ServicePrincipal", peer, rng),
    }
    return build_audit_log_record(
        actor=actor,
        action=extra["azure.operation.name"],
        outcome="success",
        source_address=peer,
        event_name="azure.resource.log",
        severity="WARN" if result_type in ("Activated", "Active") else "INFO",
        extra_attrs=extra,
        jitter_ns=jitter,
    )


def _generate_records(
    principals: list[str],
    subscription_id: str,
    resource_group: str,
    region: str,
    rng: random.Random,
    count: int,
    error_rate: float = 0.0,
) -> list[dict]:
    records = []
    for _ in range(count):
        jitter = rng.randint(0, int(BATCH_INTERVAL_MAX * 1e9))
        category = _pick_category(rng)
        if category == "Administrative":
            records.append(_admin_record(
                principals, subscription_id, resource_group, region,
                rng, error_rate, jitter,
            ))
        elif category == "ServiceHealth":
            records.append(_service_health_record(
                subscription_id, region, rng, jitter,
            ))
        elif category == "ResourceHealth":
            records.append(_resource_health_record(
                subscription_id, resource_group, region, rng, jitter,
            ))
        elif category == "Recommendation":
            records.append(_recommendation_record(
                subscription_id, resource_group, region, rng, jitter,
            ))
        else:
            records.append(_alert_record(
                subscription_id, resource_group, region, rng, jitter,
            ))
    return records



from generators.azure_activity_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop

def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    rng = random.Random()
    ns = scenario_data["namespace"] if scenario_data else NAMESPACE
    cfg = scenario_data.get("generator_config", {}) if scenario_data else {}
    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("azure-activity")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("azure-activity")
    cloud = cfg.get("cloud") or _default_cloud()
    region = cloud.get("region", "eastus")
    ident = ScenarioIdentity.from_scenario(scenario_data, ns)
    subscription_id = ident.azure_subscription_id
    resource_group = ScenarioIdentity(ns).label("rg")
    records_per_cycle: int = cfg.get("records_per_cycle", 10)

    default_principals = [
        f"{role}@{ScenarioIdentity(ns).domain()}"
        for role in ("admin", "developer", "devops", "readonly", "ci-service")
    ]
    principals: list[str] = cfg.get("principals") or default_principals


    resource = build_signal_resource(
        ns=ns, signal_type="logs", dataset=otel_dataset("azure.activity"),
        service_name=service_name, service_version="1.0.0",
        service_instance_id=ScenarioIdentity.from_scenario(scenario_data, ns).instance(service_name),
        host_name=host_name, cloud=cloud,
        extra_attrs={
            "azure.subscription_id": subscription_id,
            "azure.resource_group": resource_group,
            "azure.tenant.id": ident.azure_tenant_id,
        },
    )
    scope = {"name": AZURE_ACTIVITY_SCOPE, "version": SCOPE_VERSION}
    logger.info("Azure Activity log generator started (namespace=%s)", ns)
    mutations = [op for op in _ADMIN_OPERATIONS if not op[1].endswith("/READ")]
    listkeys = [op for op in _SENSITIVE_OPERATIONS if "LISTKEYS" in op[1]]
    pin_factories = {
        "change_storm_high_mutation_rate": lambda: _fault_actor(principals, rng, mutations),
        "role_assignment_write_privilege_escalation": lambda: _fault_actor(principals, rng),
        "sensitive_key_access": lambda: _fault_actor(principals, rng, listkeys),
        "service_health_incident": lambda: {
            "service": rng.choice(_SERVICE_HEALTH_SERVICES),
            "tracking_id": _synthetic_uuid(rng),
        },
        "resource_health_degradation": lambda: _resource_outage(rng),
    }
    pinned: dict[str, dict] = {}

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller, channel_registry=_channel_registry,
            generator_id="azure_activity_otel", chaos_mode=chaos_mode, impact_keys=_ALERT_SPEC.impact_keys,
        )
        c = active_chaos or {}
        failed_admin = float(c.get("failed_administrative_operations", 0.0))
        change_storm = float(c.get("change_storm_high_mutation_rate", 0.0))
        resource_health = float(c.get("resource_health_degradation", 0.0))
        role_escalation = float(c.get("role_assignment_write_privilege_escalation", 0.0))
        security_events = float(c.get("security_events", 0.0))
        sensitive_key = float(c.get("sensitive_key_access", 0.0))
        service_health = float(c.get("service_health_incident", 0.0))
        for key, factory in pin_factories.items():
            if float(c.get(key, 0.0)) <= 0:
                pinned.pop(key, None)
            elif key not in pinned:
                pinned[key] = factory()
        # Do not scatter failures via error_rate — OOTB failed-admin alert needs
        # >=3 docs in one (operation, caller, peer) bucket (see burst helper).
        error_rate = 0.0
        n = max(1, int(records_per_cycle * business_load()))

        records = _generate_records(principals, subscription_id, resource_group,
                                    region, rng, n, error_rate)
        if change_storm > 0:
            # OOTB: >= threshold mutations per caller in the rule window. Ticks are
            # at most BATCH_INTERVAL_MAX apart, so this many per tick clears the
            # threshold within 5 minutes from one caller, whatever the load.
            per_tick = math.ceil(change_storm * BATCH_INTERVAL_MAX / 300) + 1
            records.extend(
                _admin_record(principals, subscription_id, resource_group, region, rng, 0.0,
                              rng.randint(0, int(BATCH_INTERVAL_MAX * 1e9)),
                              actor=pinned["change_storm_high_mutation_rate"])
                for _ in range(per_tick)
            )
        if failed_admin > 0:
            # GT sets e.g. 50.0; alert threshold is failed_count >= 3.
            records.extend(_failed_admin_burst(
                subscription_id, resource_group, region, rng,
                count=max(3, int(failed_admin)),
            ))
        if service_health > 0:
            records.append(_service_health_record(
                subscription_id, region, rng, 0, degraded=True,
                incident=pinned["service_health_incident"],
            ))
        if resource_health > 0:
            records.append(_resource_health_record(
                subscription_id, resource_group, region, rng, 0, degraded=True,
                outage=pinned["resource_health_degradation"],
            ))
        if role_escalation > 0:
            records.append(_role_assignment_write_record(
                principals, subscription_id, resource_group, region, rng, 0,
                actor=pinned["role_assignment_write_privilege_escalation"],
            ))
        if sensitive_key > 0:
            records.append(_admin_record(
                principals, subscription_id, resource_group, region, rng, 0.0, 0,
                actor=pinned["sensitive_key_access"],
            ))
        if security_events > 0:
            # Must be azure.category == "Security" (Alert category will not fire).
            records.append(_security_record(
                subscription_id, resource_group, region, rng, 0,
            ))
        client.send_logs_scoped(resource, scope, records)

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("Azure Activity log generator stopped.")


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "60"))
    def _stop(sig, frame): stop_event.set()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    client = OTLPClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()

if __name__ == "__main__":
    main()
