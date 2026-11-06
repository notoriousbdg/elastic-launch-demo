"""GeneratorSpec for the ``airflow`` telemetry generator."""
from generator_engine.spec import GeneratorSpec

SPEC = GeneratorSpec(
    id="airflow_otel",
    thread_name="gen-airflow-otel",
    module="generators.airflow_otel.generator",
    client="otlp",
    signals=("metrics",),
    datasets=("airflow.otel",),
    integrations=("airflow_otel",),
    always_install=False,
    data_views=(),
    wants_chaos=True,
    default_enabled=False,
    core=False,
    chaos_mode="explicit",
    family="platform",
        impact_keys={
        "dag_file_processing_queue_large": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 200.0],
            "doc": "Drive alert \"[Airflow OTel] DAG file processing queue large\": set above 20.0 (airflow.dag_processing.file_path_queue_size) to breach; 0=healthy.",
        },
        "dag_import_errors_present": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Airflow OTel] DAG import errors present\": set above 0.0 (airflow.dag_processing.import_errors) to breach; 0=healthy.",
        },
        "dag_processor_timeouts": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Airflow OTel] DAG processor timeouts\": set above 0.0 (timeout_count) to breach; 0=healthy.",
        },
        "executor_open_slots_exhausted": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Airflow OTel] Executor open slots exhausted\": set to 0.0 (airflow.executor.open_slots) to breach MIN<=0; default 100.0=healthy.",
        },
        "scheduler_job_heartbeat_failures": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Airflow OTel] Scheduler job heartbeat failures\": set above 0.0 (airflow.job_heartbeat_failure) to breach; 0=healthy.",
        },
        "dag_run_schedule_delay_high": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 3000.0],
            "doc": "Drive alert \"[Airflow OTel] DAG run schedule delay high\": set above 300.0 (airflow.dagrun.schedule_delay) to breach; 0=healthy.",
        },
        "scheduler_heartbeats_critically_low": {
            "type": "float",
            "default": 100.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Airflow OTel] Scheduler heartbeats critically low\": set below 5.0 to suppress airflow.scheduler_heartbeat emission (alert uses COUNT(field)<5); default 100.0=healthy.",
        },
        "scheduler_loop_duration_excessive": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 600.0],
            "doc": "Drive alert \"[Airflow OTel] Scheduler loop duration excessive\": set above 60.0 (airflow.scheduler.scheduler_loop_duration) to breach; 0=healthy.",
        },
        "tasks_starving_due_to_pool_limits": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Airflow OTel] Tasks starving due to pool limits\": set above 5.0 (airflow.scheduler.tasks.starving) to breach; 0=healthy.",
        },
        "task_failures_above_threshold": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Drive alert \"[Airflow OTel] Task failures above threshold\": set above 10.0 (airflow.ti_failures) to breach; 0=healthy.",
        },
        "tasks_killed_externally": {
            "type": "float",
            "default": 0.0,
            "range": [0.0, 100.0],
            "doc": "Increase in killed_externally; values >0 fire killed-externally alert. Alert: [Airflow OTel] Tasks killed externally.",
        },
    },
    config_fields={
        "service_name": {"type": "str", "required": False, "default": "airflow",
                         "doc": "service.name for the Airflow StatsD exporter."},
        "dag_count": {"type": "int", "required": False, "default": 5,
                      "doc": "Number of synthetic DAGs to model."},
    },
    schema_version="1.0",
)
