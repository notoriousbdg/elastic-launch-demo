#!/usr/bin/env python3
"""iOS OpenTelemetry generator — EDOT iOS shaped telemetry.

Feeds the ``[iOS OTel] Application Overview`` and ``[iOS OTel] Exception
Details`` dashboards (``logs-generic.otel*`` / ``traces-generic.otel*``,
``os.name: "iOS"``):

* resource per installation: ``service.name`` / ``service.version``,
  ``app.installation.id``, ``app.build_id``, ``device.model.*``, ``os.name`` /
  ``os.version``
* sessions (``session.id`` on every span and log)
* spans: ``AppLaunch`` (cold / warm), ``<ViewController> - View appearing``
  screen spans, URLSession ``GET`` / ``POST`` client spans (``status.code``
  Error on 5xx and timeouts)
* span ``duration`` (All spans / p95 panels) is derived by Elasticsearch from the
  explicit span start and end times set here
* ``app.crash`` events whose ``exception.stacktrace`` is an Apple crash report
  (``Thread N Crashed:`` / ``Last Exception Backtrace:`` + ``Binary Images:``)
  — the layout the dashboard's ``_stacktrace.group_id`` runtime field parses
  to group crashes by the first frame in the app's own binary
* ``app.hang`` events (main-thread backtrace) for the hang fault

Usage (standalone):
    python3 -m generators.otel_ios_dashboards.generator
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/otel_ios_dashboards@0.1.0"

import hashlib
import logging
import os
import random
import signal
import threading
import time
from typing import Any

from app.config import NAMESPACE
from app.telemetry import OTLPClient
from generator_engine import mobile_common as mc
from generator_engine.db_common import impact_value, resolve_active_chaos
from generator_engine.identity import ScenarioIdentity
from generator_engine.tick import run_tick_loop
from generators.otel_ios_dashboards import SPEC

logger = logging.getLogger("otel-ios-generator")

INTERVAL = int(os.getenv("OTEL_IOS_INTERVAL", "10"))

SDK_VERSION = "1.17.1"
DISTRO_VERSION = "2.2.0"

SCOPE_LAUNCH = ("co.elastic.ios.launch", DISTRO_VERSION)
SCOPE_VIEW = ("co.elastic.ios.view", DISTRO_VERSION)
SCOPE_URLSESSION = ("NSURLSession", SDK_VERSION)
SCOPE_CRASH = ("co.elastic.ios.crash", DISTRO_VERSION)
SCOPE_HANG = ("co.elastic.ios.hang", DISTRO_VERSION)
SCOPE_SESSION = ("co.elastic.ios.session", DISTRO_VERSION)

DEFAULT_DEVICES: list[dict[str, Any]] = [
    {"model_identifier": "iPhone17,1", "model_name": "iPhone 16 Pro", "weight": 14},
    {"model_identifier": "iPhone16,1", "model_name": "iPhone 15 Pro", "weight": 18},
    {"model_identifier": "iPhone15,4", "model_name": "iPhone 15", "weight": 16},
    {"model_identifier": "iPhone15,2", "model_name": "iPhone 14 Pro", "weight": 14},
    {"model_identifier": "iPhone14,5", "model_name": "iPhone 13", "weight": 15},
    {"model_identifier": "iPhone13,2", "model_name": "iPhone 12", "weight": 10},
    {"model_identifier": "iPad14,1", "model_name": "iPad mini (6th generation)", "weight": 5},
]
_OS_BUILD = {"18.1": "22B83", "18.0.1": "22A3370", "17.6.1": "21G93", "17.5": "21F79", "16.7.10": "20H350"}

SCREENS = [
    "HomeViewController",
    "ProductListViewController",
    "ProductDetailViewController",
    "CartViewController",
    "CheckoutViewController",
    "OrderTrackingViewController",
    "SettingsViewController",
]
ENDPOINTS = [
    ("GET", "/v2/catalog/products", 0.30),
    ("GET", "/v2/catalog/products/{id}", 0.22),
    ("GET", "/v2/cart", 0.14),
    ("POST", "/v2/cart/items", 0.12),
    ("POST", "/v2/checkout", 0.06),
    ("GET", "/v2/orders/{id}/tracking", 0.10),
    ("POST", "/v1/auth/refresh", 0.06),
]

BASELINE_CRASH = 0.003
BASELINE_HANG = 0.004
NETWORK_ERROR_RATE = 0.015

# System images: (name, arch, path, base load address). Paths under /System/Library or
# /usr/lib mark an image as "system" for the dashboard's grouping script.
_SYSTEM_IMAGES = {
    "libsystem_kernel.dylib": ("arm64e", "/usr/lib/system/libsystem_kernel.dylib", 0x1E0A4C000),
    "libsystem_pthread.dylib": ("arm64e", "/usr/lib/system/libsystem_pthread.dylib", 0x1F1B80000),
    "libswiftCore.dylib": ("arm64e", "/usr/lib/swift/libswiftCore.dylib", 0x19C3B8000),
    "libobjc.A.dylib": ("arm64e", "/usr/lib/libobjc.A.dylib", 0x18F1A4000),
    "libdispatch.dylib": ("arm64e", "/usr/lib/system/libdispatch.dylib", 0x199D58000),
    "CoreFoundation": ("arm64e", "/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation", 0x1A1A8C000),
    "UIKitCore": ("arm64e", "/System/Library/PrivateFrameworks/UIKitCore.framework/UIKitCore", 0x1855C4000),
    "SwiftUI": ("arm64e", "/System/Library/Frameworks/SwiftUI.framework/SwiftUI", 0x1A0B40000),
    "GraphicsServices": ("arm64e", "/System/Library/PrivateFrameworks/GraphicsServices.framework/GraphicsServices", 0x1DC870000),
    "Foundation": ("arm64e", "/System/Library/Frameworks/Foundation.framework/Foundation", 0x18E2F8000),
    "dyld": ("arm64e", "/usr/lib/dyld", 0x1C7D10000),
}

APP = "<app>"  # placeholder image name, replaced by the app's executable name

# Each signature: (exception.type, exception.message, codes, section, frames)
# frames: (image, offset) — the first APP frame decides the crash group.
_CRASHES: list[tuple[str, str, str, str, list[tuple[str, int]], float]] = [
    ("SIGTRAP", "Fatal error: Unexpectedly found nil while unwrapping an Optional value",
     "0x0000000000000001, 0x000000019c4f0b2c", "thread",
     [("libswiftCore.dylib", 1248044), (APP, 194756), (APP, 182212), ("SwiftUI", 3301340),
      ("UIKitCore", 1043468), ("UIKitCore", 990164), ("GraphicsServices", 4900), ("dyld", 21220)], 5.0),
    ("SIGSEGV", "KERN_INVALID_ADDRESS at 0x0000000000000010",
     "KERN_INVALID_ADDRESS at 0x0000000000000010", "thread",
     [("libobjc.A.dylib", 8480), (APP, 412308), (APP, 409912), ("libdispatch.dylib", 15100),
      ("libdispatch.dylib", 71236), ("libsystem_pthread.dylib", 5340)], 3.0),
    ("NSInvalidArgumentException", "-[__NSCFNumber length]: unrecognized selector sent to instance 0x8000000000000000",
     "0x0000000000000000, 0x0000000000000000", "exception",
     [("CoreFoundation", 680916), ("libobjc.A.dylib", 98304), ("CoreFoundation", 913456),
      ("CoreFoundation", 70288), (APP, 233412), (APP, 229876), ("UIKitCore", 1043468)], 1.5),
]
_BAD_RELEASE = (
    "SIGTRAP", "Fatal error: Index out of range",
    "0x0000000000000001, 0x000000019c4f0b2c", "thread",
    [("libswiftCore.dylib", 1248044), ("libswiftCore.dylib", 1302212), (APP, 518860), (APP, 517004),
     (APP, 498224), ("SwiftUI", 3301340), ("UIKitCore", 1043468), ("GraphicsServices", 4900)],
)
_HANGS = [
    ("Main thread unresponsive for 3.2 seconds (synchronous Keychain read)",
     [("libsystem_kernel.dylib", 4500), ("Security", 0), (APP, 301444), (APP, 300120), ("UIKitCore", 1043468)]),
    ("Main thread unresponsive for 4.8 seconds (Core Data fetch on main queue)",
     [("libsystem_kernel.dylib", 7212), ("CoreFoundation", 70288), (APP, 455320), (APP, 452008), ("UIKitCore", 990164)]),
]


def _executable(app: str) -> str:
    base = app.split("-")[-2] if "-" in app else app
    return base[:1].upper() + base[1:]


def _apple_report(
    rng: random.Random,
    *,
    exc_type: str,
    codes: str,
    section: str,
    frames: list[tuple[str, int]],
    executable: str,
    bundle_id: str,
    version: str,
    model_identifier: str,
    os_version: str,
    crashed: bool = True,
) -> str:
    """Apple crash report text (header, crashed thread / last exception, binary images)."""
    container = mc.uuid4(rng).upper()
    app_path = f"/private/var/containers/Bundle/Application/{container}/{executable}.app/{executable}"
    app_load = 0x100000000 + rng.randrange(0x400, 0x4000) * 0x4000
    loads = {executable: app_load}
    for name, (_arch, _path, base) in _SYSTEM_IMAGES.items():
        loads[name] = base

    def frame_line(i: int, image: str, offset: int) -> str:
        name = executable if image == APP else image
        load = loads.get(name, 0x1A2000000)
        return f"{i:<4}{name:<36}0x{load + offset:016x} 0x{load:x} + {offset}"

    build = _OS_BUILD.get(os_version, "22B83")
    out = [
        f"Incident Identifier: {mc.uuid4(rng).upper()}",
        f"Hardware Model:      {model_identifier}",
        f"Process:             {executable} [{rng.randint(300, 9000)}]",
        f"Path:                {app_path}",
        f"Identifier:          {bundle_id}",
        f"Version:             {version} ({version.replace('.', '')}0)",
        "Code Type:           ARM-64 (Native)",
        "Parent Process:      launchd [1]",
        "",
        f"OS Version:          iPhone OS {os_version} ({build})",
        "Report Version:      104",
        "",
        f"Exception Type:  {exc_type}",
        f"Exception Codes: {codes}",
        "Crashed Thread:  0" if crashed else "Hang Thread:     0",
        "",
    ]
    if section == "exception":
        out.append("Last Exception Backtrace:")
        out += [frame_line(i, img, off) for i, (img, off) in enumerate(frames)]
        out.append("")
        out.append("Thread 0 Crashed:")
        out += [frame_line(0, "libsystem_kernel.dylib", 8432), frame_line(1, "libsystem_pthread.dylib", 29800)]
    else:
        out.append("Thread 0 Crashed:" if crashed else "Thread 0:")
        out += [frame_line(i, img, off) for i, (img, off) in enumerate(frames)]
    out += ["", "Thread 1:", frame_line(0, "libsystem_kernel.dylib", 4500),
            frame_line(1, "libsystem_pthread.dylib", 5340), "", "Binary Images:"]
    used = [executable] + sorted({img for img, _ in frames if img != APP and img in _SYSTEM_IMAGES}
                                 | {"libsystem_kernel.dylib", "libsystem_pthread.dylib"})
    for name in used:
        if name == executable:
            arch, path, load, plus = "arm64", app_path, app_load, "+"
        else:
            arch, path, load = _SYSTEM_IMAGES[name]
            plus = " "
        end = load + 0x3FFFFF
        out.append(f"{load:#18x} - {end:#18x} {plus}{name} {arch}  <{mc.hex_id(rng, 16)}> {path}")
    return "\n".join(out)


def _build_id(app: str, version: str) -> str:
    digest = hashlib.sha256(f"{app}:{version}:ios".encode()).hexdigest()
    return f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}".upper()


def _device_attrs(device: dict[str, Any], os_version: str) -> dict[str, Any]:
    return {
        "device.manufacturer": "Apple",
        "device.model.identifier": device["model_identifier"],
        "device.model.name": device["model_name"],
        "os.name": "iOS",
        "os.type": "darwin",
        "os.version": str(os_version),
        "os.description": f"iOS {os_version} ({_OS_BUILD.get(str(os_version), '22B83')})",
    }


class IOSState:
    """Installations, open sessions and per-app binary identity for one scenario."""

    def __init__(self, ident: ScenarioIdentity, cfg: dict, rng: random.Random):
        self.ident = ident
        raw_apps = cfg.get("apps") or SPEC.config_fields["apps"]["default"]
        self.apps = [ident.qualify(str(a)) for a in raw_apps]
        self.executable = {app: _executable(str(raw)) for app, raw in zip(self.apps, raw_apps)}
        self.bundle = {app: f"co.elastic.demo.{self.executable[app].lower()}" for app in self.apps}
        self.versions = mc.pairs(cfg.get("app_versions"), SPEC.config_fields["app_versions"]["default"])
        self.newest = self.versions[-1][0]
        devices = cfg.get("devices") if isinstance(cfg.get("devices"), list) and cfg.get("devices") else DEFAULT_DEVICES
        os_versions = mc.pairs(cfg.get("os_versions"), SPEC.config_fields["os_versions"]["default"])
        per_app = int(cfg.get("installations_per_app") or 60)
        active = int(cfg.get("active_sessions_per_app") or 16)
        self.api_host = ident.domain("api")
        base = {
            "service.namespace": ident.namespace,
            "deployment.environment.name": "production",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.language": "swift",
            "telemetry.sdk.version": SDK_VERSION,
            "telemetry.distro.name": "elastic",
            "telemetry.distro.version": DISTRO_VERSION,
        }
        self.fleets: dict[str, mc.MobileFleet] = {}
        for app in self.apps:
            installs = mc.build_installations(
                rng, apps=[app], versions=self.versions, devices=devices, os_versions=os_versions,
                per_app=per_app, base_attrs=base, device_attrs=_device_attrs, build_id=_build_id,
            )
            self.fleets[app] = mc.MobileFleet(installs, target_active=active)


def _view_span(rng, res_attrs_base, trace_id, start, dur, vc, parent=None):
    return mc.span(f"{vc} - View appearing", trace_id=trace_id, span_id=mc.hex_id(rng, 8),
                   parent_span_id=parent, start_ns=start, duration_ms=dur,
                   attributes={**res_attrs_base, "screen.name": vc.replace("ViewController", "")})


def simulate_tick(
    state: IOSState,
    rng: random.Random,
    now_ns: int,
    *,
    crash_rate: float = BASELINE_CRASH,
    hang_rate: float = BASELINE_HANG,
    slow_start: float = 1.0,
    interval_s: float = INTERVAL,
) -> tuple[mc.SignalBatch, mc.SignalBatch]:
    """One tick of every open session. Returns (logs, spans)."""
    logs = mc.SignalBatch()
    spans = mc.SignalBatch()
    window_ns = int(interval_s * 1e9)
    for app, fleet in state.fleets.items():
        exe, bundle = state.executable[app], state.bundle[app]
        for sess in fleet.advance(rng):
            inst = sess.install
            res = inst.resource
            t = now_ns - window_ns + int(rng.uniform(0, 0.4) * window_ns)
            base = {"session.id": sess.session_id}

            if sess.new:
                logs.add(res, SCOPE_SESSION, mc.log_record(t, event_name="session.start", attributes=dict(base)))
                cold = rng.random() < 0.55
                dur = (rng.uniform(600, 1300) if cold else rng.uniform(200, 500)) * max(1.0, slow_start)
                trace_id, root_id = mc.hex_id(rng, 16), mc.hex_id(rng, 8)
                spans.add(res, SCOPE_LAUNCH, mc.span(
                    "AppLaunch", trace_id=trace_id, span_id=root_id, start_ns=t, duration_ms=dur,
                    attributes={**base, "start.type": "cold" if cold else "warm"}))
                spans.add(res, SCOPE_VIEW, _view_span(
                    rng, base, trace_id, t + int(dur * 0.7 * mc.MS), dur * rng.uniform(0.2, 0.3),
                    SCREENS[0], parent=root_id))
                sess.screen = SCREENS[0]
                t += int((dur + rng.uniform(300, 1500)) * mc.MS)
            elif rng.random() < 0.6:
                vc = rng.choice(SCREENS[1:])
                spans.add(res, SCOPE_VIEW, _view_span(
                    rng, base, mc.hex_id(rng, 16), t, rng.uniform(60, 320), vc))
                sess.screen = vc
                t += int(rng.uniform(200, 600) * mc.MS)

            for _ in range(rng.randint(0, 3)):
                method, route, _w = mc.weighted_choice(rng, ENDPOINTS, [e[2] for e in ENDPOINTS])
                path = route.replace("{id}", str(rng.randint(10_000, 99_999)))
                failed = rng.random() < NETWORK_ERROR_RATE
                attrs = {
                    **base,
                    "http.request.method": method,
                    "url.full": f"https://{state.api_host}{path}",
                    "server.address": state.api_host,
                    "server.port": 443,
                    "network.connection.type": rng.choice(["wifi", "wifi", "cell"]),
                }
                if failed and rng.random() < 0.5:
                    attrs["error.type"] = "NSURLErrorDomain -1001"
                    dur = rng.uniform(10_000, 15_000)
                    msg = "The request timed out."
                else:
                    code = rng.choice([502, 503, 504]) if failed else rng.choice([200] * 9 + [201, 304])
                    attrs["http.response.status_code"] = code
                    dur = rng.uniform(600, 2500) if failed else rng.uniform(50, 380)
                    msg = ""
                t += int(rng.uniform(20, 400) * mc.MS)
                spans.add(res, SCOPE_URLSESSION, mc.span(
                    method, trace_id=mc.hex_id(rng, 16), span_id=mc.hex_id(rng, 8), start_ns=t,
                    duration_ms=dur, kind=mc.SPAN_KIND_CLIENT, attributes=attrs,
                    status_code=mc.STATUS_ERROR if failed else mc.STATUS_UNSET, status_message=msg))

            if rng.random() < hang_rate:
                hang_msg, frames = rng.choice(_HANGS)
                vc = sess.screen or SCREENS[0]
                spans.add(res, SCOPE_VIEW, _view_span(
                    rng, base, mc.hex_id(rng, 16), t, rng.uniform(2_500, 8_000), vc))
                t += int(2_500 * mc.MS)
                report = _apple_report(
                    rng, exc_type="AppHang", codes="0x0000000000000000", section="thread",
                    frames=[(img, off) for img, off in frames if img == APP or img in _SYSTEM_IMAGES],
                    executable=exe, bundle_id=bundle, version=inst.version,
                    model_identifier=inst.device["model_identifier"], os_version=inst.os_version,
                    crashed=False)
                logs.add(res, SCOPE_HANG, mc.log_record(
                    t, severity="WARN", body=hang_msg, event_name="app.hang",
                    attributes={**base, "exception.type": "AppHang", "exception.message": hang_msg,
                                "exception.stacktrace": report, "thread.name": "main"}))

            on_newest = inst.version == state.newest
            p_crash = crash_rate if on_newest else min(crash_rate, BASELINE_CRASH)
            if rng.random() < p_crash:
                if on_newest and crash_rate > BASELINE_CRASH and rng.random() < 0.85:
                    exc_type, msg, codes, section, frames = _BAD_RELEASE
                else:
                    exc_type, msg, codes, section, frames, _w = mc.weighted_choice(
                        rng, _CRASHES, [c[5] for c in _CRASHES])
                report = _apple_report(
                    rng, exc_type=exc_type, codes=codes, section=section, frames=frames,
                    executable=exe, bundle_id=bundle, version=inst.version,
                    model_identifier=inst.device["model_identifier"], os_version=inst.os_version)
                t += int(rng.uniform(50, 900) * mc.MS)
                logs.add(res, SCOPE_CRASH, mc.log_record(
                    t, severity="ERROR", body=f"{exc_type}: {msg}", event_name="app.crash",
                    attributes={**base, "exception.type": exc_type, "exception.message": msg,
                                "exception.stacktrace": report}))
                fleet.end(sess)
                continue

        for sess in fleet.expire():
            logs.add(sess.install.resource, SCOPE_SESSION, mc.log_record(
                now_ns - int(rng.uniform(0, 0.2) * window_ns), event_name="session.end",
                attributes={"session.id": sess.session_id,
                            "session.duration": float(sess.ticks * interval_s)}))
    return logs, spans


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
    state = IOSState(ident, cfg, rng)
    logger.info("iOS OTel generator started (apps=%s, interval=%ds)", state.apps, INTERVAL)
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
        crash_rate = float(impact_value(active_chaos, "crash_rate", default=BASELINE_CRASH))
        hang_rate = float(impact_value(active_chaos, "hang_rate", default=BASELINE_HANG))
        slow_start = float(impact_value(active_chaos, "slow_start", default=1.0))
        now_ns = int(time.time() * 1e9)
        logs, spans = simulate_tick(state, rng, now_ns, crash_rate=crash_rate,
                                    hang_rate=hang_rate, slow_start=slow_start)
        mc.send_traces(client, spans, "otel-ios-traces")
        mc.send_logs(client, logs, "otel-ios-logs")
        ticks += 1
        if ticks % 30 == 0:
            logger.info("iOS tick %d: %d spans, %d logs", ticks, len(spans), len(logs))

    run_tick_loop(stop_event, _tick, interval=lambda: INTERVAL)
    logger.info("iOS OTel generator stopped after %d ticks", ticks)


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
