"""GeneratorSpec for the ``aws_bedrock_agentcore`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="aws_bedrock_agentcore",
    thread_name="gen-aws-bedrock-agentcore",
    module="generators.aws_bedrock_agentcore.generator",
    client="es_bulk",
    signals=("metrics", "logs"),
    datasets=(
        "aws_bedrock_agentcore.metrics",
        "aws_bedrock_agentcore.runtime_application_logs",
        "aws_bedrock_agentcore.gateway_application_logs",
        "aws_bedrock_agentcore.memory_application_logs",
    ),
    integrations=("aws_bedrock_agentcore",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        "aws_region": {
            "type": "str",
            "required": False,
            "default": "us-east-1",
            "doc": "AWS region for Bedrock AgentCore.",
        },
        "agent_name": {
            "type": "str",
            "required": False,
            "default": "customersupport",
            "doc": "Agent endpoint name (aws.dimensions.Name).",
        },
        "gateway_resource": {
            "type": "str",
            "required": False,
            "default": "agentcore-gateway-prod",
            "doc": "Gateway resource id (aws.dimensions.Resource).",
        },
    },
        impact_keys={
        "agent_runtime_high_latency": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100000.0],
            "doc": "Drive alert \"[AWS Bedrock AgentCore] Agent runtime high latency\": set above 10000.0 (avg_latency_ms) to breach; 0=healthy.",
        },
        "agent_runtime_system_errors": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS Bedrock AgentCore] Agent runtime system errors\": set above 5.0 (total_system_errors) to breach; 0=healthy.",
        },
        "agent_runtime_user_errors": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS Bedrock AgentCore] Agent runtime user errors\": set above 10.0 (total_user_errors) to breach; 0=healthy.",
        },
        "browser_errors": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS Bedrock AgentCore] Browser errors\": set above 5.0 (total_errors) to breach; 0=healthy.",
        },
        "browser_session_throttles": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS Bedrock AgentCore] Browser session throttles\": set above 10.0 (total_throttles) to breach; 0=healthy.",
        },
        "code_interpreter_errors": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS Bedrock AgentCore] Code interpreter errors\": set above 5.0 (total_errors) to breach; 0=healthy.",
        },
        "code_interpreter_high_latency": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 300000.0],
            "doc": "Drive alert \"[AWS Bedrock AgentCore] Code interpreter high latency\": set above 30000.0 (avg_duration_ms) to breach; 0=healthy.",
        },
        "code_interpreter_throttles": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS Bedrock AgentCore] Code interpreter throttles\": set above 10.0 (total_throttles) to breach; 0=healthy.",
        },
        "gateway_errors": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS Bedrock AgentCore] Gateway errors\": set above 5.0 (total_errors) to breach; 0=healthy.",
        },
        "gateway_high_latency": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 50000.0],
            "doc": "Drive alert \"[AWS Bedrock AgentCore] Gateway high latency\": set above 5000.0 (avg_latency_ms) to breach; 0=healthy.",
        },
        "gateway_throttles": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS Bedrock AgentCore] Gateway throttles\": set above 10.0 (total_throttles) to breach; 0=healthy.",
        },
        "identity_throttles": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS Bedrock AgentCore] Identity throttles\": set above 10.0 (total_throttles) to breach; 0=healthy.",
        },
        "identity_token_fetch_failures": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS Bedrock AgentCore] Identity token fetch failures\": set above 5.0 (total_failures) to breach; 0=healthy.",
        },
        "memory_errors": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[AWS Bedrock AgentCore] Memory errors\": set above 5.0 (total_errors) to breach; 0=healthy.",
        },
        "memory_high_latency": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 30000.0],
            "doc": "Drive alert \"[AWS Bedrock AgentCore] Memory high latency\": set above 3000.0 (avg_latency_ms) to breach; 0=healthy.",
        },
    },
    chaos_mode="explicit",
    family="ai",
    schema_version="1.0",
)
