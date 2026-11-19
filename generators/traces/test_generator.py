"""Technology-specific tests for the ``traces`` generator."""
from __future__ import annotations

import random

from generators.traces import SPEC
from generators.traces.generator import (
    HPA_CAPPED_LATENCY_MULTIPLIER,
    STATUS_ERROR,
    _generate_trace,
    apply_apm_augmentations,
    rollup_error_rate,
)


class _FakeClient:
    def build_span(self, **kwargs):
        return {"statusCode": kwargs["status_code"], **kwargs}

    def build_exception_event(self, exc_type, exc_msg, stacktrace=""):
        return {"exception.type": exc_type, "exception.message": exc_msg}

    def build_log_record(self, **kwargs):
        return kwargs


def _error_span_count(spans_by_service: dict) -> int:
    return sum(
        1
        for spans in spans_by_service.values()
        for span in spans
        if span.get("statusCode") == STATUS_ERROR
    )


def _minimal_trace_inputs(rng: random.Random):
    services = {
        "gateway": {
            "language": "java",
            "cloud_provider": "aws",
            "cloud_platform": "aws_ec2",
            "cloud_region": "us-east-1",
            "cloud_availability_zone": "us-east-1a",
        },
        "worker": {
            "language": "python",
            "cloud_provider": "aws",
            "cloud_platform": "aws_ec2",
            "cloud_region": "us-east-1",
            "cloud_availability_zone": "us-east-1a",
        },
    }
    topology = {"gateway": [("worker", "/work", "POST")]}
    endpoints = {"gateway": [("/api", "GET")]}
    resources = {"gateway": {}, "worker": {}}
    return services, topology, endpoints, resources


def test_healthy_baseline_low_error_rate():
    rng = random.Random(10)
    services, topology, endpoints, resources = _minimal_trace_inputs(rng)
    errors = 0
    trials = 80
    for _ in range(trials):
        spans, _, _logs = _generate_trace(
            _FakeClient(),
            resources,
            rng,
            None,
            services=services,
            namespace="demo",
            service_topology=topology,
            entry_endpoints=endpoints,
            db_operations={},
            error_rate=0.0,
        )
        errors += _error_span_count(spans)
    assert errors / trials < 5


def test_error_rate_elevates_faulted_service_errors():
    rng = random.Random(11)
    services, topology, endpoints, resources = _minimal_trace_inputs(rng)
    errors = 0
    trials = 80
    for _ in range(trials):
        spans, _, _logs = _generate_trace(
            _FakeClient(),
            resources,
            rng,
            {"gateway"},
            services=services,
            namespace="demo",
            service_topology=topology,
            entry_endpoints=endpoints,
            db_operations={},
            error_rate=0.85,
        )
        errors += _error_span_count(spans)
    assert errors / trials > 0.2


def test_error_spans_emit_exception_logs_with_body():
    """system_otel Host Details Logs uses message (=body.text); must be set."""
    rng = random.Random(12)
    services, topology, endpoints, resources = _minimal_trace_inputs(rng)
    found_log = False
    for _ in range(100):
        spans, _, logs = _generate_trace(
            _FakeClient(),
            resources,
            rng,
            {"gateway"},
            services=services,
            namespace="demo",
            service_topology=topology,
            entry_endpoints=endpoints,
            db_operations={},
            error_rate=1.0,
        )
        if _error_span_count(spans) == 0:
            continue
        assert logs, "error spans must emit exception logs with body"
        for svc_logs in logs.values():
            for rec in svc_logs:
                assert rec.get("body"), rec
                assert rec.get("severity") == "ERROR"
                assert rec.get("event_name") == "exception"
                assert rec["attributes"]["exception.message"] == rec["body"]
                found_log = True
        if found_log:
            break
    assert found_log


def test_impact_keys_declared():
    assert sorted(SPEC.impact_keys) == ["error_rate", "latency_multiplier"]
    assert "logs" in SPEC.signals


def test_apm_failed_transaction_slider_replaces_the_channel_error_rate_both_ways():
    # channel default 0.7: the slider can dial it down to 5% or up to 100%
    assert apply_apm_augmentations(0.7, 1.0, {"apm_error_rate": 5}, True)[0] == 0.05
    assert apply_apm_augmentations(0.7, 1.0, {"apm_error_rate": 100}, True)[0] == 1.0
    assert apply_apm_augmentations(0.7, 1.0, {"apm_error_rate": 0}, True)[0] == 0.7  # OFF = channel value


def test_apm_augmentations_need_an_active_channel():
    assert apply_apm_augmentations(0.0, 1.0, {"apm_error_rate": 50, "k8s_hpa_capped": 1}, False) == (0.0, 1.0)


def test_hpa_cap_slows_requests_without_lowering_a_higher_latency_slider():
    assert apply_apm_augmentations(0.7, 1.0, {"k8s_hpa_capped": 1}, True)[1] == HPA_CAPPED_LATENCY_MULTIPLIER
    assert apply_apm_augmentations(0.7, 6.0, {"k8s_hpa_capped": 1}, True)[1] == 6.0


def test_rollup_error_rate_follows_the_slider_only_while_a_channel_is_active():
    assert rollup_error_rate({"apm_error_rate": 40}, True) == 0.4
    assert rollup_error_rate({"apm_error_rate": 40}, False) == 0.01
    assert rollup_error_rate({"apm_error_rate": 0}, True) == 0.01
    assert rollup_error_rate({"apm_error_rate": 0.5}, True) == 0.01  # never below the baseline


def test_lowered_error_rate_produces_fewer_errors_than_the_channel_default():
    def errors(rate: float) -> int:
        rng = random.Random(12)
        services, topology, endpoints, resources = _minimal_trace_inputs(rng)
        total = 0
        for _ in range(200):
            spans, _, _logs = _generate_trace(
                _FakeClient(), resources, rng, {"gateway"},
                services=services, namespace="demo", service_topology=topology,
                entry_endpoints=endpoints, db_operations={}, error_rate=rate,
            )
            total += _error_span_count(spans)
        return total

    assert errors(0.05) < errors(0.7)
