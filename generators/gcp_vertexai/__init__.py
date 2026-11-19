"""GeneratorSpec for the ``gcp_vertexai`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="gcp_vertexai",
    thread_name="gen-gcp-vertexai",
    module="generators.gcp_vertexai.generator",
    client='es_bulk',
    signals=('logs', 'metrics'),
    datasets=('gcp_vertexai.metrics', 'gcp_vertexai.auditlogs', 'gcp_vertexai.prompt_response_logs'),
    integrations=('gcp_vertexai',),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    config_fields={
        'gcp_region': {'type': 'str', 'required': False, 'default': 'us-central1', 'doc': 'Primary GCP region for the Vertex AI endpoint (also included in the multi-region mix).'},
        'gcp_regions': {'type': 'list', 'required': False, 'default': [
            'us-central1', 'us-east1', 'us-west1', 'northamerica-northeast1',
            'europe-west1', 'europe-west2', 'europe-west3', 'europe-west4',
            'asia-southeast1', 'asia-northeast1', 'asia-south1',
            'australia-southeast1', 'southamerica-east1', 'me-central1',
        ], 'doc': 'GCP regions to rotate across for Vertex AI metrics and audit logs. Thin profiles are auto-expanded to this global set.'},
        'vertex_models': {'type': 'list', 'required': False, 'default': ['gemini-2.5-flash', 'gemini-2.5-pro'], 'doc': 'List of Vertex AI model identifiers.'},
        'vertex_endpoints': {'type': 'list', 'required': False, 'default': ['endpoint-default'], 'doc': 'Vertex AI endpoint IDs used as gcp.labels.resource.endpoint_id.'},
    },
    impact_keys={
        'latency_multiplier': {'type': 'float', 'default': 1.0, 'range': [1.0, 5.0], 'doc': 'Multiply Vertex AI latency histograms during fault.'},
        'error_rate': {'type': 'float', 'default': 0.0, 'range': [0.0, 1.0], 'doc': 'Fraction of prediction responses that return 5xx and raise error_count during fault.'},
        'throttle_rate': {'type': 'float', 'default': 0.0, 'range': [0.0, 1.0], 'doc': 'Fraction of prediction responses that return 429 during fault.'},
    },
    chaos_mode='explicit',
    family='ai',
    schema_version="1.0",
)
