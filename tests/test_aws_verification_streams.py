"""A verification entry can read the stock AWS integration streams (metrics-aws.<package>.otel-*)."""


def test_recovery_query_reads_backticked_cloudwatch_metric_as_a_trend():
    from elastic_config.deployer_kb import recovery_query

    q = recovery_query({
        "signal": "Lambda duration",
        "from": "metrics-aws.lambda.otel-*",
        "where": 'Namespace == "AWS/Lambda" AND MetricName == "Duration" AND stat == "Average"',
        "field": "`metrics.amazonaws.com/AWS/Lambda/Duration`",
    })
    assert q.startswith("FROM metrics-aws.lambda.otel-* | WHERE @timestamp > NOW() - 15 minutes AND ")
    assert "STATS min = MIN(`metrics.amazonaws.com/AWS/Lambda/Duration`), max = MAX(" in q
    assert "BUCKET(@timestamp, 1 minute)" in q
