"""ScenarioIdentity: scoped names and cloud anchors."""

from __future__ import annotations

import random
import re
import uuid

from generator_engine.identity import ScenarioIdentity, apply_host_identity


def test_text_formatters_prefix_namespace():
    ident = ScenarioIdentity("healthcare")
    assert ident.host("postgresql") == "healthcare-postgresql"
    assert ident.host("nginx", 1) == "healthcare-nginx-01"
    assert ident.service("redis-cache") == "healthcare-redis-cache"
    assert ident.instance("redis-cache") == "healthcare-redis-cache-01"
    assert ident.instance("redis-cache", 2) == "healthcare-redis-cache-02"
    assert ident.database("app") == "healthcare_app"
    assert ident.domain() == "healthcare.internal"
    assert ident.domain("api") == "api.healthcare.internal"
    assert ident.label("events") == "healthcare-events"
    assert ident.app_service("scheduling-api") == "scheduling-api"


def test_qualify_leaves_existing_prefix():
    ident = ScenarioIdentity("healthcare")
    assert ident.qualify("healthcare-postgresql-host") == "healthcare-postgresql-host"
    assert ident.qualify("healthcare_app") == "healthcare_app"
    assert ident.qualify("healthcare.internal") == "healthcare.internal"
    assert ident.database("healthcare_app") == "healthcare_app"
    assert ident.domain("api.healthcare.internal") == "api.healthcare.internal"


def test_config_override_is_still_scoped():
    ident = ScenarioIdentity("healthcare", {"host_name": "postgresql-host"})
    assert ident.host("ignored") == "healthcare-postgresql-host"
    ident_db = ScenarioIdentity("healthcare", {"databases": ["ehr", "healthcare_claims"]})
    assert ident_db.databases(["app"]) == ["healthcare_ehr", "healthcare_claims"]


def test_cloud_anchors_are_stable_and_disjoint():
    health = ScenarioIdentity("healthcare")
    bank = ScenarioIdentity("banking")
    assert health.aws_account_id == ScenarioIdentity("healthcare").aws_account_id
    assert bank.aws_account_id == ScenarioIdentity("banking").aws_account_id
    assert health.aws_account_id != bank.aws_account_id
    assert re.fullmatch(r"\d{12}", health.aws_account_id)
    assert health.aws_account_id not in {"123456789012", "000000000000"}
    assert health.azure_subscription_id != bank.azure_subscription_id
    assert health.azure_tenant_id != bank.azure_tenant_id
    assert health.azure_subscription_id != health.azure_tenant_id
    uuid.UUID(health.azure_subscription_id)
    uuid.UUID(health.azure_tenant_id)
    assert health.gcp_project_id == "healthcare-project"
    assert bank.gcp_project_id == "banking-project"
    assert health.gcp_project_id != bank.gcp_project_id


def test_aws_resource_ids_are_hex_and_stable():
    ident = ScenarioIdentity("healthcare")
    other = ScenarioIdentity("banking")
    instance = ident.aws_id("instance", "host", 1)
    eni = ident.aws_id("eni", "flow", 3)
    assert re.fullmatch(r"i-[0-9a-f]{17}", instance)
    assert re.fullmatch(r"eni-[0-9a-f]{17}", eni)
    assert instance == ScenarioIdentity("healthcare").aws_id("instance", "host", 1)
    assert instance != ident.aws_id("instance", "host", 2)
    assert instance != other.aws_id("instance", "host", 1)
    assert eni != other.aws_id("eni", "flow", 3)


def test_two_namespaces_disjoint_text_identity():
    health = ScenarioIdentity("healthcare")
    bank = ScenarioIdentity("banking")
    assert health.host("redis-host") != bank.host("redis-host")
    assert health.service("redis-cache") != bank.service("redis-cache")
    assert health.instance("redis-cache") != bank.instance("redis-cache")
    assert health.database("app") != bank.database("app")
    assert health.domain() != bank.domain()


def test_k8s_node_names_match_historical_rng_sequence():
    ident = ScenarioIdentity("healthcare")
    rng = random.Random("healthcare:0")
    names = [ident.k8s_node("us-east-1", i, rng) for i in range(3)]
    assert len(set(names)) == 3
    assert all(name.startswith("ip-10-") and name.endswith(".us-east-1.compute.internal") for name in names)
    again = ScenarioIdentity("healthcare")
    rng2 = random.Random("healthcare:0")
    assert [again.k8s_node("us-east-1", i, rng2) for i in range(3)] == names


def test_apply_host_identity_rewrites_each_cloud():
    hosts = apply_host_identity(
        [
            {"cloud.provider": "aws", "host.name": "healthcare-aws-host-01", "host.image.id": "ami-public"},
            {"cloud.provider": "gcp", "host.name": "healthcare-gcp-host-01"},
            {"cloud.provider": "azure", "host.name": "healthcare-azure-host-01"},
        ],
        "healthcare",
    )
    ident = ScenarioIdentity("healthcare")
    assert hosts[0]["cloud.account.id"] == ident.aws_account_id
    assert hosts[0]["host.id"] == ident.aws_id("instance", "host", 1)
    assert hosts[0]["cloud.instance.id"] == hosts[0]["host.id"]
    assert hosts[0]["host.image.id"] == "ami-public"
    assert hosts[1]["cloud.account.id"] == ident.gcp_project_id
    assert hosts[1]["host.id"] == ident.gcp_instance_id(1)
    assert hosts[2]["cloud.account.id"] == ident.azure_subscription_id
    assert hosts[2]["cloud.instance.id"] == ident.label("vm", 1)
    assert ident.azure_subscription_id in hosts[2]["host.id"]
