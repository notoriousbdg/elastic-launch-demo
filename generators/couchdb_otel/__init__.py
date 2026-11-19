"""GeneratorSpec for the ``couchdb_otel`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="couchdb_otel",
    thread_name="gen-couchdb-otel",
    module="generators.couchdb_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("couchdbreceiver.otel",),
    integrations=("couchdb_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="database",
        impact_keys={
        "conflict_storm_elevated_409_responses": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[CouchDB OTel] Conflict storm (elevated 409 responses)\": set above 10.0 (conflict_rate) to breach; 0=healthy.",
        },
        "file_descriptor_exhaustion_risk": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10000.0],
            "doc": "Drive alert \"[CouchDB OTel] File descriptor exhaustion risk\": set above 1000.0 (open_fds) to breach; 0=healthy.",
        },
        "high_5xx_server_error_rate": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[CouchDB OTel] High 5xx server error rate\": set above 0.01 (total_rate) to breach; 0=healthy.",
        },
        "high_average_request_time_latency": {
            "type": "float",
            "default": 1000.0,
            "range": [0.0, 1000.0],
            "doc": "Drive alert \"[CouchDB OTel] High average request time (latency)\": set below 100.0 (target) to breach; default 1000.0=healthy.",
        },
        "high_error_rate_4xx_and_5xx": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[CouchDB OTel] High error rate (4xx and 5xx)\": set above 0.01 (total_rate) to breach; 0=healthy.",
        },
        "open_databases_count_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1000.0],
            "doc": "Drive alert \"[CouchDB OTel] Open databases count high\": set above 100.0 (open_databases) to breach; 0=healthy.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "couchdb",
            "doc": "service.name for the CouchDB instance.",
        },
        "host_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "host.name override.",
        },
        "node_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "CouchDB node name (resource attr couchdb.node.name). Defaults to couchdb@<host_name>.",
        },
        "cloud": {
            "type": "dict",
            "required": False,
            "default": None,
            "doc": "Cloud configuration dict.",
        },
    },
    schema_version="1.0"
)
