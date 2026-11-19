#!/usr/bin/env python3
"""Android OpenTelemetry generator — EDOT / opentelemetry-android shaped telemetry.

Feeds the ``[Android OTel] Application Overview`` and ``[Android OTel] Exception
Details`` dashboards (``logs-generic.otel*`` / ``traces-generic.otel*``,
``os.name: "Android"``):

* resource per installation: ``service.name`` / ``service.version`` (app and
  release), ``app.installation.id``, ``app.build_id``, ``device.manufacturer`` /
  ``device.model.*``, ``os.name`` / ``os.version``
* sessions (``session.id`` on every span and log), ``session.start`` /
  ``session.end`` events
* spans: ``AppStart`` (cold / warm), activity lifecycle ``Created`` / ``Resumed``
  / ``Paused`` with ``screen.name``, OkHttp ``GET`` / ``POST`` client spans
  (``status.code`` Error on 5xx and timeouts)
* span ``duration`` (All spans / p95 panels) is derived by Elasticsearch from the
  explicit span start and end times set here
* ``device.crash`` events and ``device.anr`` events with Java-format
  ``exception.stacktrace`` + ``exception.type`` (the dashboard's
  ``_stacktrace.group_id`` runtime field needs both)

Usage (standalone):
    python3 -m generators.otel_android_dashboards.generator
"""

from __future__ import annotations

SOURCE_INTEGRATION = "elastic/integrations packages/otel_android_dashboards@1.1.0"

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
from generators.otel_android_dashboards import SPEC

logger = logging.getLogger("otel-android-generator")

INTERVAL = int(os.getenv("OTEL_ANDROID_INTERVAL", "10"))

SDK_VERSION = "1.51.0"
DISTRO_VERSION = "1.1.0"

SCOPE_LIFECYCLE = ("io.opentelemetry.lifecycle", SDK_VERSION)
SCOPE_STARTUP = ("io.opentelemetry.startup", SDK_VERSION)
SCOPE_OKHTTP = ("io.opentelemetry.okhttp-3.0", "2.17.0-alpha")
SCOPE_CRASH = ("io.opentelemetry.crash", SDK_VERSION)
SCOPE_ANR = ("io.opentelemetry.anr", SDK_VERSION)
SCOPE_SESSION = ("io.opentelemetry.session", SDK_VERSION)

DEFAULT_DEVICES: list[dict[str, Any]] = [
    {"manufacturer": "samsung", "model_identifier": "SM-S918B", "model_name": "Galaxy S23 Ultra", "weight": 18},
    {"manufacturer": "samsung", "model_identifier": "SM-A546B", "model_name": "Galaxy A54 5G", "weight": 16},
    {"manufacturer": "Google", "model_identifier": "shiba", "model_name": "Pixel 8", "weight": 14},
    {"manufacturer": "Google", "model_identifier": "lynx", "model_name": "Pixel 7a", "weight": 9},
    {"manufacturer": "Xiaomi", "model_identifier": "23021RAA2Y", "model_name": "Redmi Note 12", "weight": 13},
    {"manufacturer": "OnePlus", "model_identifier": "CPH2449", "model_name": "OnePlus 11", "weight": 8},
    {"manufacturer": "motorola", "model_identifier": "moto g54 5G", "model_name": "moto g54 5G", "weight": 7},
]
_API_LEVEL = {"12": 31, "13": 33, "14": 34, "15": 35}
_BUILD_TAG = {"12": "SP1A.210812.016", "13": "TQ3A.230901.001", "14": "AP2A.240905.003", "15": "AP4A.241205.013"}

# (activity class, screen.name)
SCREENS = [
    ("MainActivity", "Home"),
    ("ProductListActivity", "ProductList"),
    ("ProductDetailActivity", "ProductDetail"),
    ("CartActivity", "Cart"),
    ("CheckoutActivity", "Checkout"),
    ("OrderTrackingActivity", "OrderTracking"),
    ("SettingsActivity", "Settings"),
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
BASELINE_ANR = 0.004
NETWORK_ERROR_RATE = 0.015


def _pkg(app: str) -> str:
    """Java package for an app; the stack frames read like the app's own code."""
    base = app.split("-")[-2] if "-" in app else app
    return f"co.elastic.demo.{base.replace('_', '')}"


def _crash_signatures(pkg: str) -> list[tuple[str, str, list[str], float]]:
    """Healthy-baseline crash mix: (exception.type, message, frames, weight)."""
    return [
        ("java.lang.NullPointerException",
         "Attempt to invoke interface method 'int java.util.List.size()' on a null object reference",
         [f"{pkg}.cart.CartAdapter.getItemCount(CartAdapter.kt:58)",
          "androidx.recyclerview.widget.RecyclerView.dispatchLayoutStep1(RecyclerView.java:4578)",
          "androidx.recyclerview.widget.RecyclerView.dispatchLayout(RecyclerView.java:4386)",
          "androidx.recyclerview.widget.RecyclerView.onLayout(RecyclerView.java:4924)",
          "android.view.View.layout(View.java:25033)",
          "android.os.Handler.dispatchMessage(Handler.java:106)",
          "android.os.Looper.loop(Looper.java:288)",
          "android.app.ActivityThread.main(ActivityThread.java:8919)"], 5.0),
        ("java.lang.IllegalStateException",
         "Fragment ProductDetailFragment{8c1e2f4} not attached to a context.",
         ["androidx.fragment.app.Fragment.requireContext(Fragment.java:972)",
          f"{pkg}.catalog.ProductDetailFragment.renderReviews(ProductDetailFragment.kt:211)",
          f"{pkg}.catalog.ProductDetailFragment.access$renderReviews(ProductDetailFragment.kt:44)",
          f"{pkg}.catalog.ProductDetailFragment$onViewCreated$2.invokeSuspend(ProductDetailFragment.kt:97)",
          "kotlin.coroutines.jvm.internal.BaseContinuationImpl.resumeWith(ContinuationImpl.kt:33)",
          "kotlinx.coroutines.DispatchedTask.run(DispatchedTask.kt:108)",
          "android.os.Handler.handleCallback(Handler.java:958)",
          "android.os.Looper.loop(Looper.java:288)"], 3.0),
        ("java.lang.OutOfMemoryError",
         "Failed to allocate a 26214416 byte allocation with 8388608 free bytes and 12MB until OOM",
         ["android.graphics.BitmapFactory.nativeDecodeStream(Native Method)",
          "android.graphics.BitmapFactory.decodeStream(BitmapFactory.java:749)",
          f"{pkg}.media.ImageDecoder.decodeFull(ImageDecoder.kt:72)",
          f"{pkg}.catalog.GalleryPagerAdapter.onBindViewHolder(GalleryPagerAdapter.kt:39)",
          "androidx.recyclerview.widget.RecyclerView$Adapter.bindViewHolder(RecyclerView.java:7254)",
          "android.os.Looper.loop(Looper.java:288)"], 1.5),
    ]


def _bad_release_crash(pkg: str) -> tuple[str, str, list[str]]:
    """The regression the crash_rate fault ships in the newest version."""
    return (
        "java.lang.IndexOutOfBoundsException",
        "Index 0 out of bounds for length 0",
        ["java.util.ArrayList.get(ArrayList.java:427)",
         f"{pkg}.checkout.CheckoutViewModel.selectDefaultPayment(CheckoutViewModel.kt:142)",
         f"{pkg}.checkout.CheckoutViewModel.onPaymentMethodsLoaded(CheckoutViewModel.kt:118)",
         f"{pkg}.checkout.CheckoutViewModel$loadPaymentMethods$1.invokeSuspend(CheckoutViewModel.kt:96)",
         "kotlin.coroutines.jvm.internal.BaseContinuationImpl.resumeWith(ContinuationImpl.kt:33)",
         "kotlinx.coroutines.DispatchedTask.run(DispatchedTask.kt:108)",
         "android.os.Handler.handleCallback(Handler.java:958)",
         "android.os.Looper.loop(Looper.java:288)",
         "android.app.ActivityThread.main(ActivityThread.java:8919)"],
    )


def _anr_signatures(pkg: str) -> list[tuple[str, list[str]]]:
    return [
        ("Application Not Responding for at least 5000 ms (main thread waiting on SharedPreferences)",
         ["java.lang.Object.wait(Native Method)",
          "android.app.SharedPreferencesImpl.awaitLoadedLocked(SharedPreferencesImpl.java:282)",
          "android.app.SharedPreferencesImpl.getString(SharedPreferencesImpl.java:298)",
          f"{pkg}.session.TokenStore.currentToken(TokenStore.kt:31)",
          f"{pkg}.MainActivity.onResume(MainActivity.kt:88)",
          "android.app.Activity.performResume(Activity.java:8786)",
          "android.os.Looper.loop(Looper.java:288)"]),
        ("Application Not Responding for at least 5000 ms (main thread in SQLite query)",
         ["android.database.sqlite.SQLiteConnection.nativeExecuteForCursorWindow(Native Method)",
          "android.database.sqlite.SQLiteConnection.executeForCursorWindow(SQLiteConnection.java:1001)",
          "androidx.room.RoomDatabase.query(RoomDatabase.kt:476)",
          f"{pkg}.orders.OrderDao_Impl.recentOrders(OrderDao_Impl.java:203)",
          f"{pkg}.orders.OrderTrackingActivity.onCreate(OrderTrackingActivity.kt:57)",
          "android.app.Activity.performCreate(Activity.java:8595)",
          "android.os.Looper.loop(Looper.java:288)"]),
    ]


def _stacktrace(exc_type: str, message: str, frames: list[str]) -> str:
    return f"{exc_type}: {message}\n" + "\n".join(f"\tat {f}" for f in frames)


def _build_id(app: str, version: str) -> str:
    digest = hashlib.sha256(f"{app}:{version}:android".encode()).hexdigest()
    return f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"


def _device_attrs(device: dict[str, Any], os_version: str) -> dict[str, Any]:
    api = _API_LEVEL.get(str(os_version), 34)
    return {
        "device.manufacturer": device["manufacturer"],
        "device.model.identifier": device["model_identifier"],
        "device.model.name": device["model_name"],
        "os.name": "Android",
        "os.type": "linux",
        "os.version": str(os_version),
        "os.description": f"Android Version {os_version} (Build {_BUILD_TAG.get(str(os_version), 'AP2A.240905.003')} API level {api})",
        "android.os.api_level": str(api),
    }


class AndroidState:
    """Installations, open sessions and per-app code identity for one scenario."""

    def __init__(self, ident: ScenarioIdentity, cfg: dict, rng: random.Random):
        self.ident = ident
        raw_apps = cfg.get("apps") or SPEC.config_fields["apps"]["default"]
        self.apps = [ident.qualify(str(a)) for a in raw_apps]
        self.pkg = {app: _pkg(str(raw)) for app, raw in zip(self.apps, raw_apps)}
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
            "telemetry.sdk.language": "java",
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


def simulate_tick(
    state: AndroidState,
    rng: random.Random,
    now_ns: int,
    *,
    crash_rate: float = BASELINE_CRASH,
    anr_rate: float = BASELINE_ANR,
    slow_start: float = 1.0,
    interval_s: float = INTERVAL,
) -> tuple[mc.SignalBatch, mc.SignalBatch]:
    """One tick of every open session. Returns (logs, spans)."""
    logs = mc.SignalBatch()
    spans = mc.SignalBatch()
    window_ns = int(interval_s * 1e9)
    for app, fleet in state.fleets.items():
        pkg = state.pkg[app]
        crash_sigs = _crash_signatures(pkg)
        anr_sigs = _anr_signatures(pkg)
        for sess in fleet.advance(rng):
            inst = sess.install
            res = inst.resource
            t = now_ns - window_ns + int(rng.uniform(0, 0.4) * window_ns)
            base = {"session.id": sess.session_id}

            if sess.new:
                logs.add(res, SCOPE_SESSION, mc.log_record(
                    t, event_name="session.start",
                    attributes=dict(base)))
                cold = rng.random() < 0.6
                dur = (rng.uniform(700, 1400) if cold else rng.uniform(250, 600)) * max(1.0, slow_start)
                trace_id, root_id = mc.hex_id(rng, 16), mc.hex_id(rng, 8)
                spans.add(res, SCOPE_STARTUP, mc.span(
                    "AppStart", trace_id=trace_id, span_id=root_id, start_ns=t, duration_ms=dur,
                    attributes={**base, "start.type": "cold" if cold else "warm",
                                "screen.name": SCREENS[0][1]}))
                activity, screen = SCREENS[0]
                created = dur * rng.uniform(0.35, 0.55)
                spans.add(res, SCOPE_LIFECYCLE, mc.span(
                    "Created", trace_id=trace_id, span_id=mc.hex_id(rng, 8), parent_span_id=root_id,
                    start_ns=t + int(dur * 0.4 * mc.MS), duration_ms=created,
                    attributes={**base, "activity.name": activity, "screen.name": screen}))
                spans.add(res, SCOPE_LIFECYCLE, mc.span(
                    "Resumed", trace_id=trace_id, span_id=mc.hex_id(rng, 8), parent_span_id=root_id,
                    start_ns=t + int(dur * 0.92 * mc.MS), duration_ms=rng.uniform(8, 45),
                    attributes={**base, "activity.name": activity, "screen.name": screen}))
                sess.screen = screen
                t += int((dur + rng.uniform(300, 1500)) * mc.MS)
            elif rng.random() < 0.6:
                activity, screen = rng.choice(SCREENS[1:])
                trace_id = mc.hex_id(rng, 16)
                if sess.screen:
                    spans.add(res, SCOPE_LIFECYCLE, mc.span(
                        "Paused", trace_id=mc.hex_id(rng, 16), span_id=mc.hex_id(rng, 8),
                        start_ns=t, duration_ms=rng.uniform(2, 12),
                        attributes={**base, "screen.name": sess.screen}))
                t += int(rng.uniform(5, 40) * mc.MS)
                spans.add(res, SCOPE_LIFECYCLE, mc.span(
                    "Created", trace_id=trace_id, span_id=mc.hex_id(rng, 8), start_ns=t,
                    duration_ms=rng.uniform(40, 180),
                    attributes={**base, "activity.name": activity, "screen.name": screen}))
                t += int(rng.uniform(150, 260) * mc.MS)
                spans.add(res, SCOPE_LIFECYCLE, mc.span(
                    "Resumed", trace_id=trace_id, span_id=mc.hex_id(rng, 8), start_ns=t,
                    duration_ms=rng.uniform(8, 60),
                    attributes={**base, "activity.name": activity, "screen.name": screen}))
                sess.screen = screen

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
                    "screen.name": sess.screen or SCREENS[0][1],
                }
                if failed and rng.random() < 0.5:
                    attrs["error.type"] = "java.net.SocketTimeoutException"
                    dur = rng.uniform(10_000, 15_000)
                    msg = "timeout"
                else:
                    code = rng.choice([502, 503, 504]) if failed else rng.choice([200] * 9 + [201, 304])
                    attrs["http.response.status_code"] = code
                    dur = rng.uniform(600, 2500) if failed else rng.uniform(60, 420)
                    msg = ""
                t += int(rng.uniform(20, 400) * mc.MS)
                spans.add(res, SCOPE_OKHTTP, mc.span(
                    method, trace_id=mc.hex_id(rng, 16), span_id=mc.hex_id(rng, 8), start_ns=t,
                    duration_ms=dur, kind=mc.SPAN_KIND_CLIENT, attributes=attrs,
                    status_code=mc.STATUS_ERROR if failed else mc.STATUS_UNSET, status_message=msg))

            if rng.random() < anr_rate:
                anr_msg, frames = rng.choice(anr_sigs)
                block_ms = rng.uniform(5_200, 9_000)
                spans.add(res, SCOPE_LIFECYCLE, mc.span(
                    "Resumed", trace_id=mc.hex_id(rng, 16), span_id=mc.hex_id(rng, 8), start_ns=t,
                    duration_ms=block_ms,
                    attributes={**base, "screen.name": sess.screen or SCREENS[0][1]}))
                t += int(5_000 * mc.MS)
                logs.add(res, SCOPE_ANR, mc.log_record(
                    t, severity="WARN", body=anr_msg, event_name="device.anr",
                    attributes={**base, "exception.type": "ApplicationNotResponding",
                                "exception.message": anr_msg,
                                "exception.stacktrace": _stacktrace("ApplicationNotResponding", anr_msg, frames),
                                "thread.name": "main", "thread.id": 2}))

            on_newest = inst.version == state.newest
            p_crash = crash_rate if on_newest else min(crash_rate, BASELINE_CRASH)
            if rng.random() < p_crash:
                if on_newest and crash_rate > BASELINE_CRASH and rng.random() < 0.85:
                    exc_type, msg, frames = _bad_release_crash(pkg)
                else:
                    exc_type, msg, frames, _w = mc.weighted_choice(rng, crash_sigs, [c[3] for c in crash_sigs])
                t += int(rng.uniform(50, 900) * mc.MS)
                logs.add(res, SCOPE_CRASH, mc.log_record(
                    t, severity="ERROR", body=f"{exc_type}: {msg}", event_name="device.crash",
                    attributes={**base, "exception.type": exc_type, "exception.message": msg,
                                "exception.stacktrace": _stacktrace(exc_type, msg, frames),
                                "exception.escaped": True, "thread.name": "main", "thread.id": 2}))
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
    state = AndroidState(ident, cfg, rng)
    logger.info("Android OTel generator started (apps=%s, interval=%ds)", state.apps, INTERVAL)
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
        anr_rate = float(impact_value(active_chaos, "anr_rate", default=BASELINE_ANR))
        slow_start = float(impact_value(active_chaos, "slow_start", default=1.0))
        now_ns = int(time.time() * 1e9)
        logs, spans = simulate_tick(state, rng, now_ns, crash_rate=crash_rate,
                                    anr_rate=anr_rate, slow_start=slow_start)
        mc.send_traces(client, spans, "otel-android-traces")
        mc.send_logs(client, logs, "otel-android-logs")
        ticks += 1
        if ticks % 30 == 0:
            logger.info("Android tick %d: %d spans, %d logs", ticks, len(spans), len(logs))

    run_tick_loop(stop_event, _tick, interval=lambda: INTERVAL)
    logger.info("Android OTel generator stopped after %d ticks", ticks)


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
