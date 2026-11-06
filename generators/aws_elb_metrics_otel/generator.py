#!/usr/bin/env python3
"""ELB Metrics Generator — sends ALB, NLB and GWLB CloudWatch metrics via OTLP.

Emits data matching the aws_elb_metrics_otel content package. Since 0.12.1 the
package reads one data stream per load balancer type, so this emits three resources:
  aws.elb.otel          ApplicationELB   load balancers named ``app/...``
  aws.elb_network.otel  NetworkELB       ``net/...``
  aws.elb_gateway.otel  GatewayELB       ``gateway/...``

Signal types:
  - Metrics: CloudWatch metrics in the ApplicationELB / NetworkELB / GatewayELB namespaces

Metric names use the CloudWatch path format:
  amazonaws.com/AWS/ApplicationELB/<MetricName>
Elasticsearch stores each as metrics.amazonaws.com/AWS/ApplicationELB/<MetricName>
via the passthrough field mapping.

CloudWatch period values (Sum / Average / Maximum) must be emitted as
**gauges** with a ``stat`` attribute — not OTel cumulative counters.
Dashboard TS panels call ``sum_over_time`` / ``avg_over_time`` /
``max_over_time``, which reject ``counter`` field types.

Attribute keys verified against:
  - elastic/integrations/packages/aws_elb_metrics_otel/kibana/dashboard/*.json

Key attributes: LoadBalancer, TargetGroup, AvailabilityZone, stat,
cloud.region, cloud.account.id

Panels at load balancer level filter WHERE TargetGroup IS NULL AND AvailabilityZone IS NULL.
Panels at target group level filter WHERE TargetGroup IS NOT NULL.
Health / latency panels require ``stat == "Maximum"`` (and Average for
TargetResponseTime) at the load-balancer level.

Usage (standalone):
    python3 -m generators.aws_elb_metrics_otel.generator
"""

from __future__ import annotations

from generator_engine.identity import ScenarioIdentity

SOURCE_INTEGRATION = "elastic/integrations packages/aws_elb_metrics_otel@0.12.1"

import logging
import os
import random
import signal
import threading

from app.telemetry import OTLPClient
from app.config import NAMESPACE
from generator_engine.db_common import (
    impact_value,
    build_gauge,
    build_signal_resource,
    business_load,
    otel_dataset,
    resolve_active_chaos,
)

# ── Grounding constants — do not change names ─────────────────────────────────
# Metric names use the bare path; ES stores them under metrics.<name>.

METRIC_NAMES = (
    "amazonaws.com/AWS/ApplicationELB/ActiveConnectionCount",
    "amazonaws.com/AWS/ApplicationELB/NewConnectionCount",
    "amazonaws.com/AWS/ApplicationELB/HTTPCode_ELB_5XX_Count",
    "amazonaws.com/AWS/ApplicationELB/HTTPCode_Target_2XX_Count",
    "amazonaws.com/AWS/ApplicationELB/HTTPCode_Target_4XX_Count",
    "amazonaws.com/AWS/ApplicationELB/HTTPCode_Target_5XX_Count",
    "amazonaws.com/AWS/ApplicationELB/HealthyHostCount",
    "amazonaws.com/AWS/ApplicationELB/ProcessedBytes",
    "amazonaws.com/AWS/ApplicationELB/RejectedConnectionCount",
    "amazonaws.com/AWS/ApplicationELB/RequestCount",
    "amazonaws.com/AWS/ApplicationELB/TargetResponseTime",
    "amazonaws.com/AWS/ApplicationELB/UnHealthyHostCount",
)

# Network and Gateway load balancers: (metric, unit, kind, stats). "level" metrics are
# gauges (Average = value, Maximum slightly above, Sum = value); "total" metrics are
# period sums (Maximum is the busiest sub-period). Stats are exactly what the
# aws_elb_metrics_otel@0.12.1 NLB and GWLB detail dashboards read.
_NETWORK_SERIES = (
    ("ActiveFlowCount", "{flow}", "level", ("Average",)),
    ("NewFlowCount", "{flow}", "total", ("Maximum", "Sum")),
    ("ProcessedBytes", "By", "total", ("Maximum", "Sum")),
    ("ConsumedLCUs", "1", "level", ("Average",)),
    ("HealthyHostCount", "{host}", "level", ("Average", "Maximum", "Sum")),
    ("UnHealthyHostCount", "{host}", "level", ("Average", "Maximum", "Sum")),
    ("TCP_Client_Reset_Count", "{count}", "total", ("Maximum", "Sum")),
    ("TCP_ELB_Reset_Count", "{count}", "total", ("Maximum", "Sum")),
    ("TCP_Target_Reset_Count", "{count}", "total", ("Sum",)),
)
_GATEWAY_SERIES = (
    ("ActiveFlowCount", "{flow}", "level", ("Average", "Maximum", "Sum")),
    ("NewFlowCount", "{flow}", "total", ("Maximum", "Sum")),
    ("ProcessedBytes", "By", "total", ("Sum",)),
    ("BytesProcessed", "By", "total", ("Maximum", "Sum")),
    ("ConsumedLCUs", "1", "level", ("Average",)),
    ("HealthyHostCount", "{host}", "level", ("Average", "Maximum", "Sum")),
    ("UnHealthyHostCount", "{host}", "level", ("Average", "Maximum", "Sum")),
)

# Package alert thresholds (aws_elb_metrics_otel@0.12.1): response time average > 2 s,
# tail (Maximum) > 5 s. Maximum can never be below Average, so the average fault is
# capped under the tail threshold and the tail fault raises only Maximum.
_AVG_RT_FAULT_CAP_S = 4.0
_TAIL_RT_QUIET_CAP_S = 4.9
# The 5XX rate alerts require > 100 requests; keep fault cycles above that.
_FAULT_MIN_REQUESTS = 400

ELB_METRICS_SCOPE = (
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscloudwatchreceiver"
)
SCOPE_VERSION = "0.115.0"

logger = logging.getLogger("elb-metrics-generator")

BATCH_INTERVAL_MIN = 30
BATCH_INTERVAL_MAX = 60


def _default_cloud() -> dict:
    return {
        "provider": "aws",
        "platform": "aws_elb",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    }


def _cw_attrs(metric_name: str, base: dict, namespace: str = "AWS/ApplicationELB") -> dict:
    """Attach CloudWatch Namespace/MetricName columns used by OOTB alerts."""
    return {
        **base,
        "Namespace": namespace,
        "MetricName": metric_name,
    }


def _lb_dims(lb_name: str, region: str, account_id: str) -> dict:
    """Dimensions for load-balancer-level metrics (TargetGroup/AZ omitted)."""
    return {
        "LoadBalancer": lb_name,
        "cloud.region": region,
        "cloud.account.id": account_id,
    }


def _lb_attrs(lb_name: str, region: str, account_id: str, stat: str) -> dict:
    """Load-balancer-level dimensions plus the CloudWatch ``stat``."""
    return {**_lb_dims(lb_name, region, account_id), "stat": stat}


def _tg_dims(lb_name: str, tg_name: str, az: str | None, region: str, account_id: str) -> dict:
    """Dimensions for target-group-level metrics (``az=None``: the TargetGroup-only level)."""
    dims = {
        "LoadBalancer": lb_name,
        "TargetGroup": tg_name,
        "cloud.region": region,
        "cloud.account.id": account_id,
    }
    if az:
        dims["AvailabilityZone"] = az
    return dims


# Share of ELB-generated 5xx per status code. With no healthy target to route to the
# load balancer answers 503; otherwise bad gateways and timeouts dominate.
_ELB_5XX_NO_TARGETS = (("503", 0.80), ("502", 0.12), ("504", 0.05), ("500", 0.03))
_ELB_5XX_DEFAULT = (("502", 0.55), ("504", 0.25), ("500", 0.15), ("503", 0.05))


def _split_elb_5xx(total: int, no_healthy_targets: bool) -> dict[str, int]:
    """Split ``HTTPCode_ELB_5XX_Count`` into the per-code counts (sums back to ``total``)."""
    shares = _ELB_5XX_NO_TARGETS if no_healthy_targets else _ELB_5XX_DEFAULT
    counts = {code: int(total * share) for code, share in shares}
    counts[shares[0][0]] += total - sum(counts.values())
    return counts


def _alb_stats(
    metrics: list[dict],
    metric: str,
    unit: str,
    stats: dict[str, int | float],
    dims: dict,
) -> None:
    """Emit one ApplicationELB period gauge per ``{stat: value}`` entry."""
    for stat, value in stats.items():
        metrics.append(build_gauge(
            f"amazonaws.com/AWS/ApplicationELB/{metric}", unit, value,
            _cw_attrs(metric, {**dims, "stat": stat}),
        ))


def _generate_metrics(
    lb_name: str,
    target_groups: list[str],
    az: str,
    region: str,
    account_id: str,
    healthy_hosts: int,
    rng: random.Random,
    active_chaos: dict | None = None,
) -> list[dict]:
    metrics: list[dict] = []
    load = business_load()

    c = active_chaos or {}
    alb_5xx = float(c.get("high_application_load_balancer_5xx_error_rate", 0.0))
    target_5xx = float(c.get("high_application_load_balancer_target_5xx_error_rate", 0.0))
    rt_avg_key = float(c.get("high_application_load_balancer_target_response_time_average", 1.0))
    rt_tail_key = float(c.get("high_application_load_balancer_target_response_time_tail", 1.0))
    rejected_key = float(c.get("application_load_balancer_rejected_connections_detected", 0.0))
    unhealthy_override = c.get("application_load_balancer_unhealthy_targets_detected")
    # Each fault drives only its own alert; 2xx only shrinks to leave room for errors.
    error_rate = min(0.9, max(alb_5xx, target_5xx))

    req_delta = int(rng.randint(100, 5_000) * load)
    if alb_5xx > 0 or target_5xx > 0:
        req_delta = max(req_delta, _FAULT_MIN_REQUESTS)
    processed_bytes = int(req_delta * rng.randint(500, 10_000))
    rejected = int(rejected_key) if rejected_key > 0 else 0
    http_2xx = int(req_delta * max(0.1, 1.0 - error_rate - 0.05))
    http_4xx = int(req_delta * 0.02)
    # ELB-generated and target-generated 5xx are separate alerts, so each has its own key.
    # Healthy baseline emits 0 to keep lb_state == "healthy" (dashboard state =
    # "erroring" when be_5xx > 0); only inject errors when chaos is active.
    http_target_5xx = int(req_delta * target_5xx)
    http_elb_5xx = int(req_delta * alb_5xx)

    active_conns = int(rng.randint(100, 5_000) * load)
    new_conns = int(active_conns * rng.uniform(0.05, 0.30))

    # Target health is per target group; CloudWatch publishes it at the
    # (LoadBalancer, TargetGroup) and (…, AvailabilityZone) levels. The load-balancer
    # level copy the overview reads carries the same per-target-group values, so every
    # dashboard panel and the investigation workflow see one consistent count.
    tg_unhealthy = 0
    if unhealthy_override is not None and float(unhealthy_override or 0) > 0:
        tg_unhealthy = max(0, min(healthy_hosts, int(unhealthy_override)))
    tg_healthy = max(0, healthy_hosts - tg_unhealthy)
    elb_5xx_codes = _split_elb_5xx(http_elb_5xx, no_healthy_targets=tg_unhealthy > 0)
    # Load balancer could not open a connection to a target (sparse; 0 when healthy).
    target_conn_errors = int(http_elb_5xx * 0.5) if tg_unhealthy > 0 else 0

    base_rt = rng.uniform(0.010, 0.200)
    if rt_avg_key > 1.0:
        base_rt = max(base_rt, min(rt_avg_key, _AVG_RT_FAULT_CAP_S))
    max_rt = base_rt * rng.uniform(1.2, 2.5)
    if rt_tail_key > 1.0:
        max_rt = max(max_rt, rt_tail_key)
    elif rt_avg_key > 1.0:
        max_rt = max(base_rt, min(max_rt, _TAIL_RT_QUIET_CAP_S))

    # ── Load-balancer-level metrics (TargetGroup/AZ null) ──
    lb = _lb_dims(lb_name, region, account_id)
    # The overview "Active connections over time" panel reads stat == "Average".
    _alb_stats(metrics, "ActiveConnectionCount", "{connection}",
               {"Sum": active_conns, "Average": active_conns}, lb)
    _alb_stats(metrics, "NewConnectionCount", "{connection}", {"Sum": new_conns}, lb)
    _alb_stats(metrics, "RequestCount", "{request}", {"Sum": req_delta}, lb)
    _alb_stats(metrics, "ProcessedBytes", "By", {"Sum": processed_bytes}, lb)
    _alb_stats(metrics, "RejectedConnectionCount", "{connection}", {"Sum": rejected}, lb)
    _alb_stats(metrics, "HTTPCode_ELB_5XX_Count", "{request}", {"Sum": http_elb_5xx}, lb)
    for code, count in elb_5xx_codes.items():
        _alb_stats(metrics, f"HTTPCode_ELB_{code}_Count", "{request}", {"Sum": count}, lb)
    _alb_stats(metrics, "TargetConnectionErrorCount", "{connection}", {"Sum": target_conn_errors}, lb)
    _alb_stats(metrics, "TargetTLSNegotiationErrorCount", "{connection}", {"Sum": 0}, lb)
    _alb_stats(metrics, "HTTPCode_Target_2XX_Count", "{request}", {"Sum": http_2xx}, lb)
    _alb_stats(metrics, "HTTPCode_Target_4XX_Count", "{request}", {"Sum": http_4xx}, lb)
    _alb_stats(metrics, "HTTPCode_Target_5XX_Count", "{request}", {"Sum": http_target_5xx}, lb)

    # Health + latency at LB level — dashboard filters TargetGroup IS NULL and uses
    # stat Maximum (hosts); ALB detail "Healthy hosts" and the unhealthy-hosts alert
    # read Average. Response time alerts read Average and Maximum.
    _alb_stats(metrics, "HealthyHostCount", "{host}",
               {"Maximum": tg_healthy, "Average": tg_healthy}, lb)
    _alb_stats(metrics, "UnHealthyHostCount", "{host}",
               {"Maximum": tg_unhealthy, "Average": tg_unhealthy}, lb)
    _alb_stats(metrics, "TargetResponseTime", "s",
               {"Average": round(base_rt, 4), "Maximum": round(max_rt, 4)}, lb)

    # ── Target-group-level metrics ──
    tg_count = max(1, len(target_groups))
    for tg in target_groups:
        unhealthy, healthy = tg_unhealthy, tg_healthy
        tg_rt = base_rt * rng.uniform(0.8, 1.2)
        tg_rt_max = tg_rt * rng.uniform(1.1, 2.0)
        if rt_tail_key > 1.0:
            tg_rt_max = max(tg_rt_max, rt_tail_key)
        elif rt_avg_key > 1.0:
            tg_rt_max = max(tg_rt, min(tg_rt_max, _TAIL_RT_QUIET_CAP_S))
        tg_reqs = max(1, req_delta // tg_count)
        tg_5xx = max(0, http_target_5xx // tg_count)

        tgd = _tg_dims(lb_name, tg, az, region, account_id)
        # Target-scoped RequestCount / 5xx for target-5xx-rate OOTB alert.
        _alb_stats(metrics, "RequestCount", "{request}", {"Sum": tg_reqs}, tgd)
        _alb_stats(metrics, "HTTPCode_Target_5XX_Count", "{request}", {"Sum": tg_5xx}, tgd)
        tg_only = _tg_dims(lb_name, tg, None, region, account_id)
        for dims in (tgd, tg_only):
            _alb_stats(metrics, "HealthyHostCount", "{host}",
                       {"Average": healthy, "Maximum": healthy}, dims)
            _alb_stats(metrics, "UnHealthyHostCount", "{host}",
                       {"Average": unhealthy, "Maximum": unhealthy}, dims)
        _alb_stats(metrics, "TargetResponseTime", "s",
                   {"Average": round(tg_rt, 4), "Maximum": round(tg_rt_max, 4)}, tgd)

    return metrics


def _generate_lb_type_metrics(
    series: tuple,
    namespace: str,
    lb_name: str,
    region: str,
    account_id: str,
    healthy_hosts: int,
    rng: random.Random,
) -> list[dict]:
    """Load-balancer-level metrics for a Network or Gateway load balancer.

    The detail dashboards filter ``TargetGroup IS NULL AND AvailabilityZone IS NULL``, so
    every series is emitted at load-balancer level with ``Namespace``/``MetricName``.
    """
    load = business_load()
    active = max(1, int(rng.randint(50, 3_000) * load))
    new_flows = max(1, int(active * rng.uniform(0.05, 0.30)))
    processed = new_flows * rng.randint(500, 10_000)
    base = {
        "ActiveFlowCount": active,
        "NewFlowCount": new_flows,
        "ProcessedBytes": processed,
        "BytesProcessed": processed,
        "ConsumedLCUs": round(rng.uniform(0.1, 3.0) * max(0.2, load), 3),
        "HealthyHostCount": healthy_hosts,
        "UnHealthyHostCount": 0,
        "TCP_Client_Reset_Count": rng.randint(0, 5),
        "TCP_ELB_Reset_Count": rng.randint(0, 3),
        "TCP_Target_Reset_Count": rng.randint(0, 5),
    }
    metrics: list[dict] = []
    for metric, unit, kind, stats in series:
        value = base[metric]
        for stat in stats:
            if kind == "level":
                emitted = value if stat in ("Average", "Sum") else (
                    value * rng.uniform(1.0, 1.2) if isinstance(value, float)
                    else int(value * rng.uniform(1.0, 1.2))
                )
            else:  # period total: Sum is the total, Maximum the busiest sub-period
                emitted = value if stat == "Sum" else int(value * rng.uniform(0.2, 0.5))
            metrics.append(build_gauge(
                f"amazonaws.com/{namespace}/{metric}", unit, emitted,
                _cw_attrs(metric, _lb_attrs(lb_name, region, account_id, stat), namespace),
            ))
    return metrics


def _generate_network_metrics(
    lb_name: str, region: str, account_id: str, healthy_hosts: int, rng: random.Random,
) -> list[dict]:
    return _generate_lb_type_metrics(
        _NETWORK_SERIES, "AWS/NetworkELB", lb_name, region, account_id, healthy_hosts, rng,
    )


def _generate_gateway_metrics(
    lb_name: str, region: str, account_id: str, healthy_hosts: int, rng: random.Random,
) -> list[dict]:
    return _generate_lb_type_metrics(
        _GATEWAY_SERIES, "AWS/GatewayELB", lb_name, region, account_id, healthy_hosts, rng,
    )


from generators.aws_elb_metrics_otel import SPEC as _ALERT_SPEC
from generator_engine.tick import run_tick_loop


def _alb_name(ident: ScenarioIdentity, cfg: dict) -> str:
    """The ApplicationELB ``LoadBalancer`` dimension (``app/<name>/<id>``)."""
    return cfg.get("lb_name") or f"app/{ident.label('alb')}/{ident.aws_id('instance', 'alb', 1)[2:]}"


def workflow_input_values(scenario_data: dict | None) -> dict[str, str]:
    """Values the library investigation workflows take as inputs.

    The deployer fills matching manual-trigger inputs with them (and the literal
    fallbacks alert-triggered runs use); see ``elastic_config/workflow_templates.py``.
    """
    ns = (scenario_data or {}).get("namespace") or NAMESPACE
    cfg = (scenario_data or {}).get("generator_config") or {}
    return {"load_balancer": _alb_name(ScenarioIdentity.from_scenario(scenario_data, ns), cfg)}


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    """Run ELB metrics generator loop until stop_event is set."""
    rng = random.Random()

    if scenario_data:
        ns = scenario_data["namespace"]
        cfg = scenario_data.get("generator_config", {})
    else:
        ns = NAMESPACE
        cfg = {}

    _channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")

    service_name = ScenarioIdentity.from_scenario(scenario_data, ns).service("alb-metrics")
    host_name = ScenarioIdentity.from_scenario(scenario_data, ns).host("alb")
    cloud = cfg.get("cloud") or _default_cloud()
    region = cloud.get("region", "us-east-1")
    az = cloud.get("availability_zone", "us-east-1a")
    account_id = ScenarioIdentity.from_scenario(scenario_data, ns).aws_account_id
    healthy_hosts: int = cfg.get("healthy_hosts", 4)

    _ident = ScenarioIdentity.from_scenario(scenario_data, ns)
    lb_name = _alb_name(_ident, cfg)

    default_tgs = [
        f"targetgroup/{_ident.label('api')}/{_ident.aws_id('instance', 'tg-api', 1)[2:]}",
        f"targetgroup/{_ident.label('web')}/{_ident.aws_id('instance', 'tg-web', 1)[2:]}",
    ]
    target_groups: list[str] = cfg.get("target_groups") or default_tgs


    def _resource(dataset: str, lb: str, host: str) -> dict:
        return build_signal_resource(
            ns=ns,
            signal_type="metrics",
            dataset=otel_dataset(dataset),
            service_name=service_name,
            service_version="1.0.0",
            service_instance_id=_ident.instance(service_name),
            host_name=_ident.qualify(host),
            cloud=cloud,
            extra_attrs={"cloud.account.id": account_id, "LoadBalancer": lb},
        )

    resource = _resource("aws.elb", lb_name, host_name)

    # The 0.12.1 overview and the NLB/GWLB detail dashboards read their own streams.
    emit_other_types = cfg.get("emit_network_and_gateway", True)
    nlb_name = cfg.get("nlb_name") or f"net/{_ident.label('nlb')}/{_ident.aws_id('instance', 'nlb', 1)[2:]}"
    gwlb_name = cfg.get("gwlb_name") or f"gateway/{_ident.label('gwlb')}/{_ident.aws_id('instance', 'gwlb', 1)[2:]}"
    nlb_resource = _resource("aws.elb_network", nlb_name, _ident.host("nlb"))
    gwlb_resource = _resource("aws.elb_gateway", gwlb_name, _ident.host("gwlb"))

    scope = {"name": ELB_METRICS_SCOPE, "version": SCOPE_VERSION}

    logger.info("ELB metrics generator started (namespace=%s, alb=%s)", ns, lb_name)

    def _emit_tick() -> None:
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=_channel_registry,
            generator_id="aws_elb_metrics_otel",
            chaos_mode=chaos_mode,
            impact_keys=_ALERT_SPEC.impact_keys,
        )
        metrics = _generate_metrics(
            lb_name, target_groups, az, region, account_id,
            healthy_hosts, rng, active_chaos,
        )
        client.send_metrics_scoped(resource, scope, metrics)
        if emit_other_types:
            client.send_metrics_scoped(
                nlb_resource, scope,
                _generate_network_metrics(nlb_name, region, account_id, healthy_hosts, rng),
            )
            client.send_metrics_scoped(
                gwlb_resource, scope,
                _generate_gateway_metrics(gwlb_name, region, account_id, healthy_hosts, rng),
            )

    run_tick_loop(stop_event, _emit_tick, interval=lambda: rng.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    logger.info("ELB metrics generator stopped.")


def main() -> None:  # pragma: no cover
    """Standalone entry point for local testing."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    stop_event = threading.Event()
    duration = int(os.environ.get("RUN_DURATION", "60"))

    def _stop(sig, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    client = OTLPClient()
    threading.Timer(duration, stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
