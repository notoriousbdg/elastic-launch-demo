"""GeneratorSpec for the ``aws_elb_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="aws_elb_otel",
    thread_name="gen-aws-elb-otel",
    module="generators.aws_elb_otel.generator",
    client='otlp',
    signals=('logs',),
    datasets=('aws.elbaccess.otel',),
    integrations=('aws_elb_otel',),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={'service_name': {'type': 'str', 'required': False, 'default': 'alb-frontend', 'doc': 'service.name for the ALB.'}, 'host_name': {'type': 'str', 'required': False, 'default': None, 'doc': "host.name / ALB hostname.  Defaults to '<ns>-alb'."}, 'lb_resource_id': {'type': 'str', 'required': False, 'default': None, 'doc': "cloud.resource_id value (e.g. 'app/my-alb/<id>').  Auto-generated if absent."}, 'endpoints': {'type': 'list', 'required': False, 'default': None, 'doc': 'List of {url, path, method} dicts for the access log URL pool.'}, 'client_ips': {'type': 'list', 'required': False, 'default': None, 'doc': 'List of client IP strings.  Auto-generated if absent.'}, 'records_per_cycle': {'type': 'int', 'required': False, 'default': 20, 'doc': 'Number of access log records to emit per sleep cycle.'}, 'cloud': {'type': 'dict', 'required': False, 'default': {'provider': 'aws', 'platform': 'aws_elb', 'region': 'us-east-1', 'availability_zone': 'us-east-1a'}, 'doc': 'Cloud provider/platform/region/az for resource attributes.'}},
    impact_keys={'error_fraction': {'type': 'float', 'default': 0.0, 'range': [0.0, 1.0], 'doc': 'Fraction of requests that return 5xx during fault.'}, 'records_per_cycle': {'type': 'int', 'default': 20, 'range': [1, 1000], 'doc': 'Number of access log records to emit per cycle during fault.'}},
    chaos_mode='explicit',
    family='cloud_compute',
    schema_version="1.0",
)
