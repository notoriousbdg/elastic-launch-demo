"""GeneratorSpec for the ``etcd`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="etcd_otel",
    thread_name="gen-etcd-otel",
    module="generators.etcd_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("etcd.otel",),
    integrations=("etcd_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="platform",
        impact_keys={
        "database_size_approaching_quota": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[etcd OTel] Database size approaching quota\": set above 0.9 (etcd_server_quota_backend_bytes) to breach; 0=healthy.",
        },
        "file_descriptor_exhaustion": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[etcd OTel] File descriptor exhaustion\": set above 0.9 (process_max_fds) to breach; 0=healthy.",
        },
        "grpc_errors": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[etcd OTel] gRPC errors\": set above 0.0 (errors) to breach; 0=healthy.",
        },
        "heartbeat_send_failures": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[etcd OTel] Heartbeat send failures\": set above 0.0 (increase) to breach; 0=healthy.",
        },
        "frequent_leader_elections": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[etcd OTel] Frequent leader elections\": set above 1.0 (changes) to breach; 0=healthy.",
        },
        "no_leader": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[etcd OTel] No leader\": >0 activates breach emit for this rule; 0=healthy.",
        },
        "proposal_failures": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[etcd OTel] Proposal failures\": set above 0.0 (increase) to breach; 0=healthy.",
        },
        "pending_proposals_backlog": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[etcd OTel] Pending proposals backlog\": set above 10.0 (etcd_server_proposals_pending) to breach; 0=healthy.",
        },
        "slow_apply_operations": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1000.0],
            "doc": "Drive alert \"[etcd OTel] Slow apply operations\": set above 100.0 (takes) to breach; 0=healthy.",
        },
        "scrape_target_down": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[etcd OTel] Scrape target down\": >0 activates breach emit for this rule; 0=healthy.",
        },
    },
    config_fields={
        "service_name": {"type": "str", "required": False, "default": "etcd",
                         "doc": "service.name for the etcd Prometheus scrape target."},
        "member_count": {"type": "int", "required": False, "default": 3,
                         "doc": "Number of etcd cluster members to model."},
    },
    schema_version="1.0",
)
