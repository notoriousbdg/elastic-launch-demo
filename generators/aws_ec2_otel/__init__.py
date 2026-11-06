"""GeneratorSpec for the ``aws_ec2`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="aws_ec2_otel",
    thread_name="gen-aws-ec2-otel",
    module="generators.aws_ec2_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("aws.ec2.otel",),
    integrations=("aws_ec2_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="cloud_compute",
        impact_keys={
        "cpu_surplus_credits_charged": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS EC2 OTel] CPU surplus credits charged\": set above 0.0 (total_surplus_charged) to breach; 0=healthy.",
        },
        "ebs_attached_volume_status_check_failed": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS EC2 OTel] EBS attached volume status check failed\": set above 1.0 (max_failed) to breach; 0=healthy.",
        },
        "high_cpu_utilization": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 900.0],
            "doc": "Drive alert \"[AWS EC2 OTel] High CPU utilization\": set above 90.0 (avg_cpu_pct) to breach; 0=healthy.",
        },
        "high_ebs_read_throughput": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 5368709120.0],
            "doc": "Drive alert \"[AWS EC2 OTel] High EBS read throughput\": set above 536870912.0 (total_read_bytes) to breach; 0=healthy.",
        },
        "high_ebs_write_throughput": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 5368709120.0],
            "doc": "Drive alert \"[AWS EC2 OTel] High EBS write throughput\": set above 536870912.0 (total_write_bytes) to breach; 0=healthy.",
        },
        "instance_status_check_failed": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS EC2 OTel] Instance status check failed\": set above 1.0 (max_failed) to breach; 0=healthy.",
        },
        "low_cpu_credit_balance": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS EC2 OTel] Low CPU credit balance\": set below 10.0 (min_credit_balance) to breach; default 100.0=healthy.",
        },
        "system_status_check_failed": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS EC2 OTel] System status check failed\": set above 1.0 (max_failed) to breach; 0=healthy.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "ec2-metrics",
            "doc": "Service name reported to the OTLP endpoint.",
        },
        "host_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "Host name override.  Defaults to <namespace>-ec2.",
        },
        "cloud": {
            "type": "dict",
            "required": False,
            "default": None,
            "doc": "Cloud configuration dict (provider, platform, region, availability_zone).",
        },
        "account_id": {
            "type": "str",
            "required": False,
            "default": "123456789012",
            "doc": "AWS account ID embedded in dimension attributes.",
        },
        "instance_ids": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "List of EC2 instance ID strings to simulate.  Defaults to 3 synthetic ids.",
        },
    },
    schema_version="1.0"
)
