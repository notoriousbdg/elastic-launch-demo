"""GeneratorSpec for the ``cassandra_otel`` telemetry generator."""

from generator_engine.spec import GeneratorSpec


SPEC = GeneratorSpec(
    id="cassandra_otel",
    thread_name="gen-cassandra-otel",
    module="generators.cassandra_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("cassandra.otel",),
    integrations=("cassandra_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    family="database",
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "cassandra-node",
            "doc": "service.name for the Cassandra cluster.",
        },
        "host_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": (
                "Single-node host.name / server.address override. "
                "Ignored when node_names is set. Defaults to a 3-node "
                "'<ns>-cassandra-{1,2,3}' cluster."
            ),
        },
        "node_names": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": (
                "Cassandra node hostnames used as server.address / host.name. "
                "Defaults to ['<ns>-cassandra-1', '<ns>-cassandra-2', '<ns>-cassandra-3']."
            ),
        },
        "storage_bytes": {
            "type": "int",
            "required": False,
            "default": 53687091200,
            "doc": "Approximate on-disk data size in bytes (cassandra.storage.load.count baseline).",
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
            "doc": "Cloud provider/platform/region/az for resource attributes.",
        },
    },
        impact_keys={
        "compaction_falling_behind_pending_tasks": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Cassandra OTel] Compaction falling behind (pending tasks)\": set above 0.0 (cassandra.compaction.tasks.pending) to breach; 0=healthy.",
        },
        "high_error_rate_by_node": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Cassandra OTel] High error rate by node\": set above 0.05 (request.count) to breach; 0=healthy.",
        },
        "high_range_slice_latency_p99": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 5000000.0],
            "doc": "Drive alert \"[Cassandra OTel] High range slice latency (p99)\": set above 500000.0 (cassandra.client.request.range_slice.latency.99p) to breach; 0=healthy.",
        },
        "high_read_latency_p99": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1000000.0],
            "doc": "Drive alert \"[Cassandra OTel] High read latency (p99)\": set above 100000.0 (cassandra.client.request.read.latency.99p) to breach; 0=healthy.",
        },
        "high_write_latency_p99": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 500000.0],
            "doc": "Drive alert \"[Cassandra OTel] High write latency (p99)\": set above 50000.0 (cassandra.client.request.write.latency.99p) to breach; 0=healthy.",
        },
        "hints_in_progress_replicas_unreachable": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Cassandra OTel] Hints in progress (replicas unreachable)\": set above 0.0 (cassandra.storage.total_hints.in_progress.count) to breach; 0=healthy.",
        },
        "request_errors_timeout_unavailable_failure": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Cassandra OTel] Request errors (Timeout, Unavailable, Failure)\": set above 0.0 (cassandra.client.request.error.count) to breach; 0=healthy.",
        },
    },
    chaos_mode="explicit",
    schema_version="1.0"
)
