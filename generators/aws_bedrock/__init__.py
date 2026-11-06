"""GeneratorSpec for the ``aws_bedrock`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="aws_bedrock",
    thread_name="gen-aws-bedrock",
    module="generators.aws_bedrock.generator",
    client='es_bulk',
    signals=('logs', 'metrics'),
    datasets=('aws_bedrock.invocation', 'aws_bedrock.runtime', 'aws_bedrock.guardrails'),
    integrations=('aws_bedrock',),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={'aws_region': {'type': 'str', 'required': False, 'default': 'us-east-1', 'doc': 'AWS region for the Bedrock service.'}, 'bedrock_models': {'type': 'list', 'required': False, 'default': ['us.anthropic.claude-haiku-4-5-20251001-v1:0', 'us.anthropic.claude-sonnet-5'], 'doc': 'List of Bedrock model IDs to simulate.'}},
    impact_keys={'throttle_multiplier': {'type': 'float', 'default': 1.0, 'range': [1.0, 20.0], 'doc': 'Multiply ThrottlingException rate and reduce successful calls during fault.'}, 'guardrail_intervention_rate': {'type': 'float', 'default': 0.0, 'range': [0.0, 1.0], 'doc': 'Fraction of Bedrock calls that result in a guardrail intervention.'}, 'latency_multiplier': {'type': 'float', 'default': 1.0, 'range': [1.0, 5.0], 'doc': 'Multiply Bedrock invocation latency during fault.'}},
    chaos_mode='explicit',
    family='ai',
    schema_version="1.0",
)
