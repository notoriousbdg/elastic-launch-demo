"""CloudTrail-specific tests for the cloudtrail generator."""

import random
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "tests"))

from generators.aws_cloudtrail_otel.generator import _generate_records


def _attr_map(record: dict) -> dict:
    return {a["key"]: next(iter(a["value"].values())) for a in record["attributes"]}


def test_cloudtrail_records_include_user_agent_original():
    """Logs by user agent panel filters on user_agent.original."""
    records = _generate_records(
        principals=["arn:aws:iam::123:user/demo"],
        account_id="123456789012",
        region="us-east-1",
        rng=random.Random(1),
        count=5,
    )
    assert records
    for rec in records:
        attrs = _attr_map(rec)
        assert attrs.get("user_agent.original")
        assert "rpc.service" in attrs
