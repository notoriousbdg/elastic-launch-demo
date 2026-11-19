"""GeneratorSpec for the ``temporal_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="temporal_otel",
    thread_name="gen-temporal-otel",
    module="generators.temporal_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("temporal.cloud_metrics.otel",),
    integrations=("temporal_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "temporal-cloud",
            "doc": "service.name of the prometheus scrape job for the Temporal Cloud endpoint.",
        },
        "host_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "host.name of the scraping collector.  Defaults to '<ns>-otel-collector'.",
        },
        "account_id": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "Temporal Cloud account id (temporal_account).  Defaults to a stable "
                   "5-character id derived from the scenario namespace.",
        },
        "namespaces": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "Temporal namespaces: list of {name, region, action_limit, task_queues: "
                   "{queue: [workflow types]}}.  Defaults to a payments and an orders "
                   "namespace with three task queues each.",
        },
        "cloud": {
            "type": "dict",
            "required": False,
            "default": {
                "provider": "aws",
                "platform": "aws_ec2",
                "region": "us-east-1",
                "availability_zone": "us-east-1a",
            },
            "doc": "Cloud provider/platform/region/az of the scraping collector.",
        },
    },
    impact_keys={
        "namespace_approaching_action_rate_limit": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 150.0],
            "doc": "Drive alert \"[Temporal OTel] Namespace Approaching Action Rate Limit\": "
                   "set above 80.0 (temporal_cloud_v1_total_action_count) to breach; 0=healthy.",
        },
        "activity_task_timeout_rate_elevated": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 5.0],
            "doc": "Drive alert \"[Temporal OTel] Activity Task Timeout Rate Elevated\": "
                   "set above 0.05 (temporal_cloud_v1_activity_task_timeout_count) to breach; 0=healthy.",
        },
        "high_activity_task_failure_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Temporal OTel] High Activity Task Failure Rate\": "
                   "set above 10.0 (temporal_cloud_v1_activity_task_fail_count) to breach; 0=healthy.",
        },
        "no_pollers_on_task_queue": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10.0],
            "doc": "Drive alert \"[Temporal OTel] No Pollers on Task Queue\": "
                   "set above 0.0 (temporal_cloud_v1_no_poller_tasks_count) to breach; 0=healthy.",
        },
        "namespace_open_workflow_count_anomaly": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100000.0],
            "doc": "Drive alert \"[Temporal OTel] Namespace Open Workflow Count Anomaly\": "
                   "set above 10000.0 (temporal_cloud_v1_namespace_open_workflows) to breach; 0=healthy.",
        },
        "task_queue_backlog_growing": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10000.0],
            "doc": "Drive alert \"[Temporal OTel] Task Queue Backlog Growing\": "
                   "set above 500.0 (temporal_cloud_v1_approximate_backlog_count) to breach; 0=healthy.",
        },
        "workflow_timeout_rate_elevated": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10.0],
            "doc": "Drive alert \"[Temporal OTel] Workflow Timeout Rate Elevated\": "
                   "set above 0.01 (temporal_cloud_v1_workflow_timeout_count) to breach; 0=healthy.",
        },
    },
    chaos_mode="explicit",
    family="platform",
    schema_version="1.0",
    # Dashboard controls pick Namespace / Account; both are namespace-qualified.
    identity_fields=(
        "service.name",
        "service.instance.id",
        "attributes.temporal_namespace",
        "attributes.temporal_account",
    ),
)
