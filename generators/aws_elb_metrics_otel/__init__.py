"""GeneratorSpec for the ``aws_elb_metrics_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="aws_elb_metrics_otel",
    thread_name="gen-aws-elb-metrics-otel",
    module="generators.aws_elb_metrics_otel.generator",
    client='otlp',
    signals=('metrics',),
    datasets=('aws.elb.otel', 'aws.elb_network.otel', 'aws.elb_gateway.otel'),
    integrations=('aws_elb_metrics_otel',),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={'service_name': {'type': 'str', 'required': False, 'default': 'alb-metrics', 'doc': 'service.name for the ALB metrics source.'}, 'host_name': {'type': 'str', 'required': False, 'default': None, 'doc': "host.name.  Defaults to '<ns>-alb'."}, 'lb_name': {'type': 'str', 'required': False, 'default': None, 'doc': "LoadBalancer attribute value (e.g. 'app/my-alb/<id>').  Auto-generated if absent."}, 'target_groups': {'type': 'list', 'required': False, 'default': None, 'doc': 'TargetGroup attribute values.  Auto-generated if absent.'}, 'healthy_hosts': {'type': 'int', 'required': False, 'default': 4, 'doc': 'Number of healthy hosts per target group.'}, 'nlb_name': {'type': 'str', 'required': False, 'default': None, 'doc': "Network load balancer name (e.g. 'net/my-nlb/<id>').  Auto-generated if absent."}, 'gwlb_name': {'type': 'str', 'required': False, 'default': None, 'doc': "Gateway load balancer name (e.g. 'gateway/my-gwlb/<id>').  Auto-generated if absent."}, 'emit_network_and_gateway': {'type': 'bool', 'required': False, 'default': True, 'doc': 'Also emit Network and Gateway load balancer metrics (the 0.12.1 overview and NLB/GWLB dashboards).'}, 'account_id': {'type': 'str', 'required': False, 'default': '123456789012', 'doc': 'AWS account ID for cloud.account.id.'}, 'cloud': {'type': 'dict', 'required': False, 'default': {'provider': 'aws', 'platform': 'aws_elb', 'region': 'us-east-1', 'availability_zone': 'us-east-1a'}, 'doc': 'Cloud provider/platform/region/az for resource attributes.'}},
        impact_keys={
        "high_application_load_balancer_5xx_error_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1000.0],
            "doc": "Drive alert \"[AWS ELB OTel] High Application Load Balancer 5XX error rate\": set above 100.0 (requests) to breach; 0=healthy.",
        },
        "application_load_balancer_rejected_connections_detected": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS ELB OTel] Application Load Balancer rejected connections detected\": set above 0.0 (rejected) to breach; 0=healthy.",
        },
        "high_application_load_balancer_target_5xx_error_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1000.0],
            "doc": "Drive alert \"[AWS ELB OTel] High Application Load Balancer target 5XX error rate\": set above 100.0 (requests) to breach; 0=healthy.",
        },
        "high_application_load_balancer_target_response_time_average": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS ELB OTel] High Application Load Balancer target response time (average)\": set above 2.0 (avg_response_time) to breach; 0=healthy.",
        },
        "high_application_load_balancer_target_response_time_tail": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS ELB OTel] High Application Load Balancer target response time (tail)\": set above 5.0 (max_response_time) to breach; 0=healthy.",
        },
        "application_load_balancer_unhealthy_targets_detected": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS ELB OTel] Application Load Balancer unhealthy targets detected\": set above 0.0 (UnHealthyHostCount) to breach; 0=healthy.",
        },
    },
    chaos_mode='explicit',
    family='cloud_compute',
    schema_version="1.0",
)
