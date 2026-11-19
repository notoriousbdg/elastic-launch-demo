"""GeneratorSpec for the ``zookeeper`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="zookeeper_otel",
    thread_name="gen-zookeeper-otel",
    module="generators.zookeeper_otel.generator",
    client="otlp",
    signals=("metrics",),
    # Note: zookeeper_otel manifest.yml declares an empty dataset list.
    # The dashboards read from metrics-generic.otel-default.
    # Pass dataset="generic" to build_signal_resource so the OTLP endpoint
    # routes to metrics-generic.otel-default, matching what zookeeper_otel expects.
    datasets=("generic.otel",),
    integrations=("zookeeper_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="platform",
        impact_keys={
        "data_tree_size_excessive": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 3221225472.0],
            "doc": "Drive alert \"[ZooKeeper OTel] Data tree size excessive\": set above 1073741824 (zookeeper.data_tree.size bytes) to breach; values 0<x<=1GiB are promoted to 1.5GiB; 0=healthy.",
        },
        "excessive_watch_count": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100000.0],
            "doc": "Drive alert \"[ZooKeeper OTel] Excessive watch count\": set above 10000.0 (max_watches) to breach; 0=healthy.",
        },
        "excessive_znode_count": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 10000000.0],
            "doc": "Drive alert \"[ZooKeeper OTel] Excessive znode count\": set above 1000000.0 (max_znodes) to breach; 0=healthy.",
        },
        "file_descriptor_utilization_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 800.0],
            "doc": "Drive alert \"[ZooKeeper OTel] File descriptor utilization high\": percent or fraction; values >80% (or >0.8) breach via co-located open+limit docs; 0=healthy.",
        },
        "high_request_latency": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 5000.0],
            "doc": "Drive alert \"[ZooKeeper OTel] High request latency\": set above 500.0 (avg_peak_latency) to breach; 0=healthy.",
        },
        "node_health_check_failed": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 1.0],
            "doc": "Drive alert \"[ZooKeeper OTel] Node health check failed\": any value >0 emits ruok=0 (failed); 0=healthy (ruok=1).",
        },
        "outstanding_requests_saturation": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[ZooKeeper OTel] Outstanding requests saturation\": set above 10.0 (zookeeper.request.active) to breach; 0=healthy.",
        },
        "pending_syncs_buildup_on_leader": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[ZooKeeper OTel] Pending syncs buildup on leader\": set above 10.0 (sync.pending) to breach; 0=healthy.",
        },
        "unsynced_followers_detected": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[ZooKeeper OTel] Unsynced followers detected\": set above 0.0 (max_unsynced) to breach; 0=healthy.",
        },
    },
    config_fields={
        "service_name": {
            "type": "str",
            "required": False,
            "default": "zookeeper",
            "doc": "service.name for the ZooKeeper ensemble.",
        },
        "host_name": {
            "type": "str",
            "required": False,
            "default": None,
            "doc": "host.name override.",
        },
        "cloud": {
            "type": "dict",
            "required": False,
            "default": None,
            "doc": "Cloud configuration dict.",
        },
        "node_names": {
            "type": "list",
            "required": False,
            "default": None,
            "doc": "ZooKeeper node hostnames.  Defaults to 3 nodes.",
        },
    },
    schema_version="1.0"
)
