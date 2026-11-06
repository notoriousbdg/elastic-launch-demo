"""Technology-specific tests for the ``aws_cloudwatch_logs`` generator.

Standard emit/registry/identity tests live in tests/; the Template Library RDS
investigation workflow's log steps are run against this generator in
tests/test_workflow_template_emit.py.
"""
from __future__ import annotations

import random
import re

from generators.aws_cloudwatch_logs import SPEC
from generators.aws_cloudwatch_logs.generator import (
    access_records,
    data_stream_namespace,
    rds_log_group,
    rds_records,
)

# The workflow's DISSECT: %{} "%{method} %{path} %{proto}" %{status} %{}
_DISSECT = re.compile(r'^.*? "(?P<method>\S+) (?P<path>\S+) (?P<proto>\S+)" (?P<status>\d{3}) ')


def test_rds_log_holds_one_fatal_line_per_refusal():
    healthy = rds_records(random.Random(1), 1_700_000_000.0, 30.0, 0, "demo-rds-01", "demo_app", "demo_orders")
    flood = rds_records(random.Random(1), 1_700_000_000.0, 30.0, 120, "demo-rds-01", "demo_app", "demo_orders")
    assert not [r for r in healthy if "FATAL" in r[2]]
    refusals = [r for r in flood if "FATAL" in r[2]]
    assert len(refusals) == 120
    assert all("remaining connection slots are reserved" in line for _ts, _lvl, line in refusals)
    assert all(level == "fatal" for _ts, level, _line in refusals)


def test_access_log_lines_dissect_like_the_workflow():
    for _ts, _level, line in access_records(random.Random(2), 1_700_000_000.0, 30.0, 0.0, ["/api/v1/orders"]):
        assert "HTTP/1." in line
        match = _DISSECT.match(line)
        assert match and match["path"] == "/api/v1/orders" and int(match["status"]) < 500


def test_gateway_error_fraction_sets_the_5xx_share():
    rng = random.Random(3)
    lines = [line for _ in range(20)
             for _t, _l, line in access_records(rng, 1_700_000_000.0, 30.0, 0.4, ["/x"])]
    errors = sum(int(_DISSECT.match(line)["status"]) >= 500 for line in lines)
    assert 0.3 < errors / len(lines) < 0.5


def test_log_group_and_data_stream_names():
    assert rds_log_group("awsorders-orders-db") == "/aws/rds/instance/awsorders-orders-db/postgresql"
    assert data_stream_namespace("Demo-Ns") == "demo_ns"
    assert SPEC.datasets == ("aws_logs.rds", "aws_logs.ecs")
    assert set(SPEC.impact_keys) == {"connection_refusals", "gateway_error_fraction"}
