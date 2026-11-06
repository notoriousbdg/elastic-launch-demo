#!/usr/bin/env python3
"""Profiling metrics generator — output of the EDOT profilingmetrics connector.

The connector turns continuous-profiling samples into per-interval sample
counts, routed to ``metrics-profilingmetricsconnector.otel-*``. The package
dashboard (``Profilingmetrics``) reads ``metrics-*`` filtered on that dataset:

* ``samples.user.count`` / ``samples.kernel.count`` — User / Kernel CPU panels
* ``samples.<frame type>.count`` for jvm, cpython, go, v8js, dotnet, rust and
  native — User CPU by frame type (the panel's ``*_zero`` runtime fields read
  these as longs, so data points are integer)
* ``samples.native.count`` with ``shlib_name`` — User CPU by shared library
* ``samples.kernel.count`` with ``syscall_name``, ``kernel_area``
  (synchronization / disk / network / memory / ipc), ``kernel_io``
  (read / write) and ``kernel_proto`` (tcp / udp / other) — the kernel panels

One resource per profiled process (``host.name``, ``process.executable.name``,
``service.name``). Scenario services become processes on the scenario host of
their cloud, with the frame type of their language.

Counts are emitted as integer gauges of the samples taken in the interval: the
dashboard ``sum()``s them, which a TSDS counter would not allow. The demo's
OTLPClient has no profiles signal; only the connector's metrics are modelled.

Usage (standalone):
    python3 -m generators.profilingmetrics_otel.generator
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/profilingmetrics_otel@0.1.0"

import logging
import os
import random
import signal
import threading
from typing import Any

from app.config import NAMESPACE
from app.telemetry import OTLPClient, SCHEMA_URL, _format_attributes
from generator_engine.db_common import impact_value, resolve_active_chaos
from generator_engine.identity import ScenarioIdentity
from generator_engine.scrape_clock import scrape_now_ns
from generator_engine.tick import run_tick_loop
from generators.profilingmetrics_otel import SPEC

logger = logging.getLogger("profilingmetrics-generator")

INTERVAL = int(os.getenv("PROFILINGMETRICS_INTERVAL", "10"))

SCOPE = {
    "name": "github.com/elastic/opentelemetry-collector-components/connector/profilingmetricsconnector",
    "version": "0.25.0",
}
DATASET = "profilingmetricsconnector"

# language → (runtime frame type, share of user samples in runtime frames, executable or None)
LANGUAGE_RUNTIME = {
    "java": ("jvm", 0.66, "java"),
    "python": ("cpython", 0.55, "python3.12"),
    "go": ("go", 0.90, None),
    "dotnet": ("dotnet", 0.58, "dotnet"),
    "rust": ("rust", 0.84, None),
    "cpp": ("native", 0.0, None),
    "nodejs": ("v8js", 0.52, "node"),
}
RUNTIME_LANGUAGE = {rt: lang for lang, (rt, _s, _e) in LANGUAGE_RUNTIME.items()}

# Native-frame shared libraries per runtime (None = the executable itself → "Native").
SHLIBS: dict[str, list[tuple[str | None, float]]] = {
    "jvm": [("libjvm.so", 0.55), ("libc.so.6", 0.22), ("libz.so.1", 0.08), ("libcrypto.so.3", 0.07),
            ("libssl.so.3", 0.03), ("libpthread.so.0", 0.03), (None, 0.02)],
    "cpython": [("libpython3.12.so.1.0", 0.58), ("libc.so.6", 0.2), ("libcrypto.so.3", 0.08),
                ("libssl.so.3", 0.04), ("_json.cpython-312-x86_64-linux-gnu.so", 0.05), (None, 0.05)],
    "go": [(None, 0.72), ("libc.so.6", 0.16), ("linux-vdso.so.1", 0.12)],
    "v8js": [(None, 0.55), ("libc.so.6", 0.18), ("libcrypto.so.3", 0.12), ("libssl.so.3", 0.05),
             ("libstdc++.so.6", 0.1)],
    "dotnet": [("libcoreclr.so", 0.5), ("libclrjit.so", 0.2), ("libc.so.6", 0.14),
               ("libSystem.Native.so", 0.08), ("libssl.so.3", 0.04), ("libcrypto.so.3", 0.04)],
    "rust": [(None, 0.7), ("libc.so.6", 0.2), ("libcrypto.so.3", 0.06), ("libssl.so.3", 0.04)],
    "native": [(None, 0.6), ("libc.so.6", 0.15), ("libstdc++.so.6", 0.12), ("libcrypto.so.3", 0.09),
               ("libssl.so.3", 0.04)],
}
TLS_LIBS = {"libcrypto.so.3", "libssl.so.3"}

# (syscall_name, kernel_area, kernel_io, kernel_proto, weight) — None means "not set".
KERNEL_MIX: list[tuple[str | None, str | None, str | None, str | None, float]] = [
    ("futex", "synchronization", None, None, 16),
    ("read", "disk", "read", None, 6),
    ("pread64", "disk", "read", None, 5),
    ("write", "disk", "write", None, 5),
    ("fsync", "disk", "write", None, 3),
    ("recvfrom", "network", "read", "tcp", 9),
    ("sendto", "network", "write", "tcp", 9),
    ("recvmsg", "network", "read", "udp", 3),
    ("sendmsg", "network", "write", "udp", 3),
    ("recvmsg", "network", "read", "other", 1.5),
    ("sendmsg", "network", "write", "other", 1.5),
    ("mmap", "memory", None, None, 4),
    ("munmap", "memory", None, None, 2),
    (None, "memory", None, None, 6),
    ("read", "ipc", "read", None, 2),
    ("write", "ipc", "write", None, 2),
    ("epoll_pwait", None, None, None, 6),
    ("clock_nanosleep", None, None, None, 3),
    (None, None, None, None, 8),
]
_SPIKE_DIMS = {("futex", "synchronization"), ("fsync", "disk"), ("write", "disk")}

DEFAULT_EXTRA_PROCESSES = [
    {"executable": "containerd", "service": "containerd", "runtime": "go", "cpu": 0.12},
    {"executable": "envoy", "service": "edge-proxy", "runtime": "native", "cpu": 0.35},
    {"executable": "node", "service": "web-frontend", "runtime": "v8js", "cpu": 0.45},
]
_FALLBACK_SERVICES = [("checkout", "java"), ("recommendation", "python"), ("payment", "go")]
_CPU_BY_LANGUAGE = {"java": 1.1, "python": 0.7, "go": 0.5, "dotnet": 0.8, "rust": 0.4, "cpp": 0.6}


class Process:
    __slots__ = ("service", "executable", "runtime", "cpu", "resource", "hotspot")

    def __init__(self, *, service: str, executable: str, runtime: str, cpu: float,
                 resource_attrs: dict[str, Any], hotspot: bool = False):
        self.service = service
        self.executable = executable
        self.runtime = runtime
        self.cpu = cpu
        self.hotspot = hotspot
        self.resource = {"attributes": _format_attributes(resource_attrs), "schemaUrl": SCHEMA_URL}


def _hosts(scenario_data: dict | None, ident: ScenarioIdentity) -> list[dict[str, Any]]:
    hosts = [h for h in (scenario_data or {}).get("hosts") or [] if isinstance(h, dict) and h.get("host.name")]
    if hosts:
        return hosts
    return [{"host.name": ident.host("app-host", i), "cloud.provider": "aws", "cloud.region": "us-east-1"}
            for i in range(1, 4)]


def build_processes(scenario_data: dict | None, ident: ScenarioIdentity, cfg: dict,
                    rng: random.Random) -> list[Process]:
    hosts = _hosts(scenario_data, ident)
    services = (scenario_data or {}).get("services") or {}
    entries: list[tuple[str, str, dict[str, Any]]] = []  # (service, language, host)
    if isinstance(services, dict) and services:
        for i, (name, svc) in enumerate(services.items()):
            provider = (svc or {}).get("cloud_provider")
            same = [h for h in hosts if h.get("cloud.provider") == provider] or hosts
            entries.append((ident.app_service(name), str((svc or {}).get("language") or "go"),
                            same[i % len(same)]))
    else:
        for i, (role, lang) in enumerate(_FALLBACK_SERVICES):
            entries.append((ident.service(role), lang, hosts[i % len(hosts)]))

    hot_name = cfg.get("hotspot_service")
    if not hot_name:
        jvm = [s for s, lang, _h in entries if lang == "java"]
        hot_name = jvm[0] if jvm else (entries[0][0] if entries else "")

    def _attrs(host: dict[str, Any], service: str, exe: str, runtime: str) -> dict[str, Any]:
        attrs: dict[str, Any] = {
            "host.name": host["host.name"],
            "host.id": host.get("host.id"),
            "host.arch": host.get("host.arch", "amd64"),
            "os.type": "linux",
            "cloud.provider": host.get("cloud.provider"),
            "cloud.region": host.get("cloud.region"),
            "service.name": service,
            "service.namespace": ident.namespace,
            "process.executable.name": exe,
            "process.pid": rng.randint(1200, 65000),
            "data_stream.type": "metrics",
            "data_stream.dataset": DATASET,
            "data_stream.namespace": "default",
        }
        return attrs

    procs: list[Process] = []
    for service, lang, host in entries:
        runtime, _share, exe = LANGUAGE_RUNTIME.get(lang, LANGUAGE_RUNTIME["go"])
        exe = exe or service.split("/")[-1]
        procs.append(Process(
            service=service, executable=exe, runtime=runtime,
            cpu=_CPU_BY_LANGUAGE.get(lang, 0.6) * rng.uniform(0.8, 1.2),
            resource_attrs=_attrs(host, service, exe, runtime), hotspot=(service == hot_name)))

    extras = cfg.get("extra_processes")
    if not isinstance(extras, list):
        extras = DEFAULT_EXTRA_PROCESSES
    for host in hosts:
        for extra in extras:
            runtime = str(extra.get("runtime", "native"))
            service = ident.qualify(str(extra.get("service") or extra.get("executable")))
            exe = str(extra.get("executable") or service)
            procs.append(Process(
                service=service, executable=exe, runtime=runtime,
                cpu=float(extra.get("cpu", 0.3)) * rng.uniform(0.8, 1.2),
                resource_attrs=_attrs(host, service, exe, runtime)))
    return procs


def _split(rng: random.Random, total: float, weights: list[float]) -> list[int]:
    wsum = sum(weights) or 1.0
    return [max(0, int(round(total * w / wsum * rng.uniform(0.85, 1.15)))) for w in weights]


def process_samples(
    proc: Process,
    rng: random.Random,
    *,
    interval_s: float,
    hz: int,
    cpu_hotspot_multiplier: float = 1.0,
    kernel_time_spike: float = 1.0,
    native_spike: float = 1.0,
) -> dict[str, list[tuple[dict[str, Any], int]]]:
    """Sample counts for one process and interval: {metric name: [(attrs, count)]}."""
    busy = proc.cpu * rng.uniform(0.85, 1.15)
    total = busy * hz * interval_s
    kernel_share = rng.uniform(0.10, 0.20)
    user_total = total * (1 - kernel_share)
    if proc.hotspot:
        user_total *= max(1.0, cpu_hotspot_multiplier)
    kernel_total = total * kernel_share

    out: dict[str, list[tuple[dict[str, Any], int]]] = {}
    _rt, runtime_share, _exe = LANGUAGE_RUNTIME.get(RUNTIME_LANGUAGE.get(proc.runtime, ""), (proc.runtime, 0.0, None))
    runtime_samples = int(round(user_total * runtime_share))
    if proc.runtime != "native" and runtime_samples > 0:
        out[f"samples.{proc.runtime}.count"] = [({}, runtime_samples)]

    shlibs = SHLIBS.get(proc.runtime, SHLIBS["native"])
    native_counts = _split(rng, user_total - runtime_samples, [w for _l, w in shlibs])
    native: list[tuple[dict[str, Any], int]] = []
    for (lib, _w), count in zip(shlibs, native_counts):
        if lib in TLS_LIBS:
            count = int(round(count * max(1.0, native_spike)))
        if count > 0:
            native.append(({"shlib_name": lib} if lib else {}, count))
    if native:
        out["samples.native.count"] = native

    runtime_points = out.get(f"samples.{proc.runtime}.count", []) if proc.runtime != "native" else []
    user = sum(c for _a, c in runtime_points) + sum(c for _a, c in native)
    out["samples.user.count"] = [({}, user)]

    spike = max(1.0, kernel_time_spike)
    weights = []
    for syscall, area, _io, _proto, w in KERNEL_MIX:
        factor = spike if (syscall, area) in _SPIKE_DIMS else 1.0 + (spike - 1.0) * 0.25
        weights.append(w * factor)
    base_weight = sum(w for *_x, w in KERNEL_MIX)
    kernel_counts = _split(rng, kernel_total * sum(weights) / base_weight, weights)
    kernel: list[tuple[dict[str, Any], int]] = []
    for (syscall, area, io, proto, _w), count in zip(KERNEL_MIX, kernel_counts):
        if count <= 0:
            continue
        attrs = {k: v for k, v in (("syscall_name", syscall), ("kernel_area", area),
                                     ("kernel_io", io), ("kernel_proto", proto)) if v is not None}
        kernel.append((attrs, count))
    if kernel:
        out["samples.kernel.count"] = kernel
    return out


def build_metrics(samples: dict[str, list[tuple[dict[str, Any], int]]], now_ns: int,
                  interval_s: float) -> list[dict[str, Any]]:
    start = str(now_ns - int(interval_s * 1e9))
    metrics = []
    for name, points in samples.items():
        dps = []
        for attrs, count in points:
            dp: dict[str, Any] = {"startTimeUnixNano": start, "timeUnixNano": str(now_ns), "asInt": str(int(count))}
            if attrs:
                dp["attributes"] = _format_attributes(attrs)
            dps.append(dp)
        metrics.append({"name": name, "unit": "1", "gauge": {"dataPoints": dps}})
    return metrics


def run(
    client: OTLPClient,
    stop_event: threading.Event,
    *,
    scenario_data: dict | None = None,
    chaos_controller=None,
) -> None:
    rng = random.Random()
    ns = scenario_data["namespace"] if scenario_data else NAMESPACE
    ident = ScenarioIdentity.from_scenario(scenario_data, ns)
    cfg = scenario_data.get("generator_config", {}) if scenario_data else {}
    channel_registry: dict = scenario_data.get("channel_registry", {}) if scenario_data else {}
    chaos_mode = cfg.get("chaos", "explicit")
    hz = int(cfg.get("sample_frequency_hz") or 20)
    procs = build_processes(scenario_data, ident, cfg, rng)
    logger.info("Profiling metrics generator started (%d processes, interval=%ds)", len(procs), INTERVAL)
    ticks = 0

    def _tick() -> None:
        nonlocal ticks
        active_chaos = resolve_active_chaos(
            chaos_controller=chaos_controller,
            channel_registry=channel_registry,
            generator_id=SPEC.id,
            chaos_mode=chaos_mode,
            impact_keys=SPEC.impact_keys,
        )
        cpu_mult = float(impact_value(active_chaos, "cpu_hotspot_multiplier", default=1.0))
        kernel_mult = float(impact_value(active_chaos, "kernel_time_spike", default=1.0))
        native_mult = float(impact_value(active_chaos, "native_spike", default=1.0))
        now_ns = int(scrape_now_ns())
        resource_metrics = []
        for proc in procs:
            samples = process_samples(proc, rng, interval_s=INTERVAL, hz=hz,
                                      cpu_hotspot_multiplier=cpu_mult,
                                      kernel_time_spike=kernel_mult, native_spike=native_mult)
            resource_metrics.append({
                "resource": proc.resource,
                "scopeMetrics": [{"scope": SCOPE, "metrics": build_metrics(samples, now_ns, INTERVAL)}],
            })
        client._send(f"{client.endpoint}/v1/metrics", {"resourceMetrics": resource_metrics}, "profilingmetrics")
        ticks += 1
        if ticks % 30 == 0:
            logger.info("Profiling metrics tick %d: %d processes", ticks, len(procs))

    run_tick_loop(stop_event, _tick, interval=lambda: INTERVAL)
    logger.info("Profiling metrics generator stopped after %d ticks", ticks)


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = OTLPClient()
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    threading.Timer(int(os.environ.get("RUN_DURATION", "60")), stop_event.set).start()
    run(client, stop_event)
    client.close()


if __name__ == "__main__":
    main()
