"""Scenario-scoped identity for telemetry generators.

Every host, instance, database, URL, and cloud account that a dashboard can
filter on is produced here. Generators call these methods instead of formatting
``{namespace}-…`` themselves. The same namespace always yields the same ids, and
two namespaces never share an AWS account, Azure subscription, Azure tenant, or
GCP project.
"""

from __future__ import annotations

import hashlib
import random
import uuid
import zlib
from typing import Any

_AWS_PREFIXES = {
    "instance": "i-",
    "eni": "eni-",
    "volume": "vol-",
    "vol": "vol-",
    "ami": "ami-",
    "vpc": "vpc-",
    "subnet": "subnet-",
    "sg": "sg-",
}

# Well-known literals that used to be copied into every scenario.
_FORBIDDEN_AWS_ACCOUNTS = {"000000000000", "123456789012"}


def _digest(namespace: str, kind: str) -> bytes:
    return hashlib.sha256(f"{namespace}:{kind}".encode()).digest()


class ScenarioIdentity:
    """Format scenario-unique names and shared cloud anchors."""

    def __init__(self, namespace: str, config: dict[str, Any] | None = None):
        self.namespace = namespace or "default"
        self.config = config or {}

    @classmethod
    def from_scenario(
        cls,
        scenario_data: dict | None,
        fallback_ns: str = "default",
    ) -> ScenarioIdentity:
        if not scenario_data:
            return cls(fallback_ns)
        ns = scenario_data.get("namespace") or fallback_ns
        cfg = scenario_data.get("generator_config") or {}
        if not isinstance(cfg, dict):
            cfg = {}
        return cls(str(ns), cfg)

    def qualify(self, value: str) -> str:
        """Prefix ``value`` with the namespace unless it already carries it."""
        text = str(value).strip()
        ns = self.namespace
        if not text:
            return ns
        if (
            text == ns
            or text.startswith(f"{ns}-")
            or text.startswith(f"{ns}_")
            or text.startswith(f"{ns}.")
        ):
            return text
        ns_us = ns.replace("-", "_")
        if ns_us != ns and (
            text.startswith(f"{ns_us}_")
            or text.startswith(f"{ns_us}-")
            or text.startswith(f"{ns_us}.")
        ):
            return text
        return f"{ns}-{text}"

    def _cfg_str(self, key: str) -> str | None:
        raw = self.config.get(key)
        if raw is None or raw == "":
            return None
        return str(raw)

    def host(self, role: str, index: int | None = None, *, key: str = "host_name") -> str:
        override = self._cfg_str(key)
        base = self.qualify(override) if override is not None else self.qualify(role)
        if index is None:
            return base
        suffix = f"-{index:02d}"
        if base.endswith(suffix):
            return base
        return f"{base}{suffix}"

    def service(self, role: str, *, key: str = "service_name") -> str:
        override = self._cfg_str(key)
        if override is not None:
            return self.qualify(override)
        return self.qualify(role)

    def instance(self, role: str, index: int = 1) -> str:
        return f"{self.service(role)}-{index:02d}"

    def database(self, name: str) -> str:
        ns = self.namespace.replace("-", "_")
        text = str(name)
        if text.startswith(f"{ns}_"):
            return text
        return f"{ns}_{text}"

    def databases(self, names: list[str], *, key: str = "databases") -> list[str]:
        raw = self.config.get(key)
        if isinstance(raw, list) and raw:
            names = [str(item) for item in raw]
        return [self.database(name) for name in names]

    def domain(self, name: str | None = None) -> str:
        if not name:
            return f"{self.namespace}.internal"
        text = str(name)
        suffix = f".{self.namespace}.internal"
        if text == f"{self.namespace}.internal" or text.endswith(suffix):
            return text
        return f"{text}.{self.namespace}.internal"

    def label(self, role: str, index: int | None = None) -> str:
        base = self.qualify(str(role))
        if index is None:
            return base
        suffix = f"-{index:02d}"
        if base.endswith(suffix):
            return base
        return f"{base}{suffix}"

    def app_service(self, name: str) -> str:
        """Scenario application service name. Join key for traces, JVM, and Kubernetes."""
        return str(name)

    @property
    def aws_account_id(self) -> str:
        """12-digit account id, stable per namespace, never the shared demo literal."""
        span = 900_000_000_000
        base = 100_000_000_000
        n = base + (int.from_bytes(_digest(self.namespace, "aws-account")[:8], "big") % span)
        account = f"{n:012d}"
        if account in _FORBIDDEN_AWS_ACCOUNTS:
            n = base + ((n - base + 1) % span)
            account = f"{n:012d}"
        return account

    @property
    def azure_subscription_id(self) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{self.namespace}:azure-subscription"))

    @property
    def azure_tenant_id(self) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{self.namespace}:azure-tenant"))

    @property
    def gcp_project_id(self) -> str:
        return f"{self.namespace}-project"

    def gcp_project(self, override: str | None = None) -> str:
        """GCP project id: the namespace's own project, or a configured override scoped to it."""
        if override:
            return self.qualify(override)
        return self.gcp_project_id

    def gcp_instance_id(self, index: int = 1) -> str:
        """Stable decimal GCE instance id, unique per namespace and host index."""
        raw = int.from_bytes(
            hashlib.sha256(f"{self.namespace}:gcp-instance:{index}".encode()).digest()[:8],
            "big",
        )
        return str(1_000_000_000_000_000 + (raw % 9_000_000_000_000_000))

    def aws_id(self, kind: str, role: str, index: int = 1) -> str:
        """AWS resource id: prefix plus 17 hex digits from the namespace, kind, role, and index."""
        prefix = _AWS_PREFIXES[kind]
        digest = hashlib.sha256(
            f"{self.namespace}:{kind}:{role}:{index}".encode()
        ).hexdigest()
        return f"{prefix}{digest[:17]}"

    def k8s_node(self, region: str, index: int, rng: random.Random) -> str:
        """Node name shared by Kubernetes metrics and the infra topology join.

        ``index`` selects the call order. The two ``randint`` draws must stay in
        the same sequence as the historical node-name loop so pod ids that follow
        them do not shift.
        """
        del index  # call order is the RNG sequence; the name does not embed the index
        ns_octet = zlib.crc32(self.namespace.encode()) % 256
        left = rng.randint(10, 200)
        right = rng.randint(10, 200)
        return f"ip-10-{ns_octet}-{left}-{right}.{region}.compute.internal"


def apply_host_identity(hosts: list[dict], namespace: str) -> list[dict]:
    """Return host dicts whose account and instance ids come from ``ScenarioIdentity``.

    Public image names are left unchanged. AWS, GCP, and Azure hosts in one
    scenario share that cloud's anchor and get distinct instance ids.
    """
    ident = ScenarioIdentity(namespace)
    aws_i = gcp_i = azure_i = 0
    updated: list[dict] = []
    for host in hosts:
        item = dict(host)
        provider = item.get("cloud.provider")
        if provider == "aws":
            aws_i += 1
            instance = ident.aws_id("instance", "host", aws_i)
            item["cloud.account.id"] = ident.aws_account_id
            item["host.id"] = instance
            item["cloud.instance.id"] = instance
        elif provider == "gcp":
            gcp_i += 1
            instance = ident.gcp_instance_id(gcp_i)
            item["cloud.account.id"] = ident.gcp_project_id
            item["host.id"] = instance
            item["cloud.instance.id"] = instance
        elif provider == "azure":
            azure_i += 1
            vm = ident.label("vm", azure_i)
            sub = ident.azure_subscription_id
            item["cloud.account.id"] = sub
            item["cloud.instance.id"] = vm
            item["host.id"] = (
                f"/subscriptions/{sub}/resourceGroups/{namespace}-rg/"
                f"providers/Microsoft.Compute/virtualMachines/{vm}"
            )
        updated.append(item)
    return updated
