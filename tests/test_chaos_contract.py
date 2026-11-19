"""ChaosSource interoperability: controller, YamlService, and a generator."""

from __future__ import annotations

import random
import threading

from app.chaos.controller import ChaosController
from app.chaos.protocol import ChaosSource
from app.context import ScenarioContext
from app.services.base_service import BaseService
from app.services.telemetry_dsl import YamlService
from generator_engine.db_common import resolve_active_chaos
from generators.aws_elb_otel import SPEC as ELB_SPEC
from generators.aws_elb_otel.generator import run as elb_run


def _channel(service: str, *, error_fraction: float = 0.0, records: int = 4) -> dict:
    return {
        "name": "edge saturation",
        "subsystem": "edge",
        "affected_services": [service],
        "cascade_services": [],
        "generator_impact": {
            "aws_elb_otel": {
                "error_fraction": error_fraction,
                "records_per_cycle": records,
            }
        },
    }


def _controller(service: str = "checkout", **impact) -> ChaosController:
    registry = {1: _channel(service, **impact)}
    return ChaosController(channel_registry=registry)


class _Scenario:
    nominal_label = "NORMAL"
    hosts: list = []
    k8s_clusters: list = []

    def get_fault_params(self, channel: int) -> dict:
        return {}


class _Metrics:
    def __init__(self) -> None:
        self.gauges: list[tuple[str, float]] = []

    def build_gauge(self, name, value, unit="", attributes=None):
        return {"name": name, "value": value}

    def send_metrics(self, resource, metrics):
        for metric in metrics:
            self.gauges.append((metric["name"], metric["value"]))

    def build_resource(self, *args, **kwargs):
        return {}


def _service(controller: ChaosController, otlp: _Metrics) -> YamlService:
    service_name = "checkout"
    ctx = ScenarioContext(
        scenario=_Scenario(),  # type: ignore[arg-type]
        scenario_id="chaos-contract",
        namespace="contract",
        mission_id="CONTRACT",
        services={
            service_name: {
                "subsystem": "edge",
                "cloud_provider": "aws",
                "cloud_platform": "aws_ec2",
                "cloud_region": "us-east-1",
                "cloud_availability_zone": "us-east-1a",
                "language": "python",
            }
        },
        channel_registry=controller._channel_registry,
    )
    spec = {
        "emit_fault_logs": False,
        "steps": [
            {
                "metric": {
                    "name": "demo.health",
                    "value": {"if_active": 0, "else": 1},
                    "unit": "1",
                }
            }
        ],
    }

    class Checkout(YamlService):
        SERVICE_NAME = service_name
        _telemetry_spec = spec

    BaseService.set_context(ctx)
    try:
        return Checkout(controller, otlp)  # type: ignore[arg-type]
    finally:
        BaseService.clear_context()


def test_controller_satisfies_chaos_source():
    controller = _controller()
    assert isinstance(controller, ChaosSource)
    assert controller.is_active(1) is False
    assert controller.get_active_channels() == []

    result = controller.trigger(1)
    assert result["status"] == "triggered"
    assert controller.is_active(1) is True
    assert controller.get_active_channels() == [1]
    assert "callback_url" in controller.get_channel_metadata(1)


def test_yaml_service_follows_trigger():
    controller = _controller()
    idle = _Metrics()
    _service(controller, idle).generate_telemetry()
    assert idle.gauges == [("demo.health", 1.0)]

    controller.trigger(1)
    faulted = _Metrics()
    _service(controller, faulted).generate_telemetry()
    assert faulted.gauges == [("demo.health", 0.0)]


def test_generator_impact_follows_trigger():
    controller = _controller(error_fraction=1.0, records=6)
    registry = controller._channel_registry

    assert (
        resolve_active_chaos(
            None,
            registry,
            "aws_elb_otel",
            chaos_mode="explicit",
            impact_keys=ELB_SPEC.impact_keys,
        )
        is None
    )

    controller.trigger(1)
    active = resolve_active_chaos(
        controller,
        registry,
        "aws_elb_otel",
        chaos_mode="explicit",
        impact_keys=ELB_SPEC.impact_keys,
    )
    assert active is not None
    assert active["channel_id"] == 1
    assert active["error_fraction"] == 1.0
    assert active["records_per_cycle"] == 6

    class _Logs:
        def __init__(self, stop: threading.Event) -> None:
            self.stop = stop
            self.logs: list[dict] = []

        def send_logs_scoped(self, resource, scope, records):
            self.logs.extend(records)
            self.stop.set()

    import generators.aws_elb_otel.generator as elb

    elb.BATCH_INTERVAL_MIN = 0
    elb.BATCH_INTERVAL_MAX = 0
    random.seed(1)
    stop = threading.Event()
    client = _Logs(stop)
    elb_run(
        client,  # type: ignore[arg-type]
        stop,
        scenario_data={
            "namespace": "contract",
            "channel_registry": registry,
            "generator_config": {"records_per_cycle": 4, "chaos": "explicit"},
        },
        chaos_controller=controller,
    )
    assert client.logs
    statuses = []
    for record in client.logs:
        for attr in record["attributes"]:
            if attr["key"] == "aws.elb.status.code":
                statuses.append(int(attr["value"]["intValue"]))
    assert statuses
    assert any(code >= 500 for code in statuses)
