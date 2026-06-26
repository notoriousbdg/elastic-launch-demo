"""IntegrationsMixin — install Elastic Fleet/EPM integrations used by the demo."""

from __future__ import annotations

import logging
import time

import httpx

from elastic_config.deployer_base import (
    DeployStep,
    _kibana_headers,
    _retry_http,
    ProgressCallback,
)

logger = logging.getLogger("deployer")

# Latest version is resolved at install time — do not hard-code versions here.
INTEGRATIONS = [
    "kubernetes_otel",
    "aws_vpcflow_otel",
    "gcp_vpcflow_otel",
    "nginx_otel",
    "mysql_otel",
]

_EPM_QUERY = {"prerelease": "true"}

# Serverless Fleet/EPM can take several minutes on a cold project.
_FLEET_READY_ROUNDS = 32
_FLEET_READY_DELAY = 15.0  # up to ~8 minutes (32 × 15s)

# Per-package EPM lookup/install retries after the global wait.
_EPM_RETRY_ROUNDS = 8
_EPM_RETRY_DELAY = 15.0  # up to ~2 extra minutes per package

# After this many wait rounds, accept setup + EPM catalog even if isReady is false
# (Observability Serverless may not enroll agents but still serves packages).
_FLEET_EPM_FALLBACK_ROUND = 8

_FLEET_NOT_READY_STATUSES = {404, 408, 429, 500, 502, 503, 504}


class IntegrationsMixin:

    def _fleet_agents_setup_status(
        self, client: httpx.Client, headers: dict[str, str]
    ) -> tuple[bool, str, int | None]:
        """Return (is_ready, detail, http_status)."""
        try:
            resp = client.get(
                f"{self.kibana_url}/api/fleet/agents/setup",
                headers=headers,
                timeout=30.0,
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            return False, str(exc), None

        if resp.status_code != 200:
            return False, f"agents/setup HTTP {resp.status_code}", resp.status_code

        body = resp.json()
        if body.get("isReady"):
            return True, "agents ready", 200

        missing = body.get("missing_requirements") or []
        optional = body.get("missing_optional_features") or []
        parts = []
        if missing:
            parts.append("missing: " + ", ".join(missing))
        if optional:
            parts.append("optional: " + ", ".join(optional))
        return False, parts[0] if parts else "agents/setup not ready", 200

    def _ensure_fleet_setup(self, client: httpx.Client, headers: dict[str, str]) -> str | None:
        """Initialize Fleet (idempotent). Returns an error string on failure."""
        try:
            resp = _retry_http(
                lambda: client.post(
                    f"{self.kibana_url}/api/fleet/setup",
                    headers=headers,
                    json={},
                    timeout=60.0,
                ),
                label="fleet setup",
            )
            if resp is None or resp.status_code >= 300:
                code = resp.status_code if resp is not None else "no-response"
                return f"fleet setup (HTTP {code})"
            body = resp.json()
            if not body.get("isInitialized", True):
                return "fleet setup did not initialize"
            for err in body.get("nonFatalErrors") or []:
                logger.warning("Fleet setup non-fatal: %s", err)
        except Exception as exc:
            return f"fleet setup ({exc})"
        return None

    def _wait_for_fleet_ready(
        self,
        client: httpx.Client,
        headers: dict[str, str],
        step: DeployStep,
        notify: ProgressCallback,
    ) -> str | None:
        """Poll Fleet until agents/setup isReady or EPM catalog is usable.

        Returns an error string if the wait window is exhausted.
        """
        last_detail = "starting"

        for round_idx in range(_FLEET_READY_ROUNDS):
            if round_idx > 0:
                time.sleep(_FLEET_READY_DELAY)

            agents_ready, agents_detail, _agents_http = self._fleet_agents_setup_status(
                client, headers
            )
            setup_err = self._ensure_fleet_setup(client, headers)
            setup_ok = setup_err is None
            epm_ok = self._epm_catalog_reachable(client, headers)

            if agents_ready and setup_ok and epm_ok:
                logger.info("Fleet ready (agents isReady, setup initialized, EPM catalog up)")
                return None

            if (
                round_idx >= _FLEET_EPM_FALLBACK_ROUND
                and setup_ok
                and epm_ok
            ):
                logger.info(
                    "Fleet EPM ready without agents isReady (round %d): %s",
                    round_idx + 1,
                    agents_detail,
                )
                return None

            parts = [f"round {round_idx + 1}/{_FLEET_READY_ROUNDS}"]
            if not agents_ready:
                parts.append(f"agents: {agents_detail}")
            if not setup_ok:
                parts.append(setup_err or "setup pending")
            if not epm_ok:
                parts.append("EPM catalog not up")
            last_detail = "; ".join(parts)
            logger.info("Waiting for Fleet: %s", last_detail)

            step.detail = f"Waiting for Fleet ({last_detail})"
            notify(self.progress)

        return f"Fleet not ready within {_FLEET_READY_ROUNDS * _FLEET_READY_DELAY:.0f}s ({last_detail})"

    def _lookup_package_once(
        self, client: httpx.Client, pkg: str, headers: dict[str, str], *, prerelease: bool
    ) -> tuple[dict | None, int | None]:
        """Return (package item, http status). item is None when not found."""
        params = _EPM_QUERY if prerelease else None
        resp = client.get(
            f"{self.kibana_url}/api/fleet/epm/packages/{pkg}",
            headers=headers,
            params=params,
            timeout=60.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            item = data.get("item") or data.get("response") or {}
            return item, 200
        return None, resp.status_code

    def _lookup_package(
        self, client: httpx.Client, pkg: str, headers: dict[str, str]
    ) -> tuple[dict | None, int | None]:
        """Lookup with retries on transient / not-yet-provisioned responses."""
        last_status: int | None = None
        for round_idx in range(_EPM_RETRY_ROUNDS):
            if round_idx > 0:
                time.sleep(_EPM_RETRY_DELAY)

            item, status = self._lookup_package_once(client, pkg, headers, prerelease=True)
            last_status = status
            if item is not None:
                return item, status

            if status not in _FLEET_NOT_READY_STATUSES:
                break

            item, status = self._lookup_package_once(client, pkg, headers, prerelease=False)
            last_status = status
            if item is not None:
                return item, status

            if status not in _FLEET_NOT_READY_STATUSES:
                break

            logger.info(
                "Package %s lookup HTTP %s (retry %d/%d)",
                pkg,
                status,
                round_idx + 1,
                _EPM_RETRY_ROUNDS,
            )

        return None, last_status

    def _install_package(
        self,
        client: httpx.Client,
        pkg: str,
        version: str | None,
        headers: dict[str, str],
    ) -> httpx.Response | None:
        if version:
            url = f"{self.kibana_url}/api/fleet/epm/packages/{pkg}/{version}"
        else:
            url = f"{self.kibana_url}/api/fleet/epm/packages/{pkg}"
        return _retry_http(
            lambda: client.post(
                url,
                headers=headers,
                params=_EPM_QUERY,
                json={"force": True},
                timeout=120.0,
            ),
            label=f"install integration {pkg}",
            attempts=6,
            base_delay=2.0,
        )

    def _install_integrations(self, client: httpx.Client, notify: ProgressCallback):
        step = self._step(19)
        step.status = "running"
        step.items_total = len(INTEGRATIONS)
        step.items_done = 0
        step.detail = "Waiting for Fleet to become ready…"
        notify(self.progress)

        headers = _kibana_headers(self.api_key)
        installed, skipped, errors = [], [], []

        ready_err = self._wait_for_fleet_ready(client, headers, step, notify)
        if ready_err:
            logger.warning("%s", ready_err)

        setup_err = self._ensure_fleet_setup(client, headers)
        if setup_err:
            logger.warning("Fleet setup after readiness wait: %s", setup_err)

        for pkg in INTEGRATIONS:
            try:
                item, status = self._lookup_package(client, pkg, headers)

                if item is None:
                    if status and status >= 300:
                        errors.append(f"{pkg} (lookup HTTP {status})")
                    else:
                        errors.append(f"{pkg} (not in package registry)")
                    step.items_done += 1
                    notify(self.progress)
                    continue

                latest = item.get("latestVersion")
                current = item.get("version") if item.get("status") == "installed" else None
                if not latest and item.get("version"):
                    latest = item.get("version")
                if not latest:
                    errors.append(f"{pkg} (no version in registry response)")
                    step.items_done += 1
                    notify(self.progress)
                    continue

                if current == latest:
                    skipped.append(f"{pkg}@{latest}")
                else:
                    resp = self._install_package(client, pkg, latest, headers)
                    if resp is not None and resp.status_code < 300:
                        installed.append(f"{pkg}@{latest}")
                    else:
                        resp = self._install_package(client, pkg, None, headers)
                        if resp is not None and resp.status_code < 300:
                            installed.append(f"{pkg}@latest")
                        else:
                            code = resp.status_code if resp is not None else "no-response"
                            errors.append(f"{pkg} (install HTTP {code})")
            except Exception as exc:
                errors.append(f"{pkg} ({exc})")

            step.items_done += 1
            notify(self.progress)

        parts = []
        if ready_err:
            parts.append(f"fleet wait: {ready_err}")
        if installed:
            parts.append(f"installed {len(installed)}: {', '.join(installed)}")
        if skipped:
            parts.append(f"already current: {', '.join(skipped)}")
        if errors:
            parts.append(f"failed: {', '.join(errors)}")
        if setup_err and not (installed or skipped):
            parts.append(f"fleet setup: {setup_err}")

        step.detail = "; ".join(parts) or "no integrations configured"
        if installed or skipped:
            step.status = "ok"
        elif errors and self._epm_catalog_reachable(client, headers):
            step.status = "skipped"
            step.detail = (
                (step.detail + "; " if step.detail else "")
                + "OTel content packages unavailable on this project "
                "(dashboards may auto-install when matching OTLP data arrives)"
            )
        else:
            step.status = "failed"
        notify(self.progress)

    def _epm_catalog_reachable(self, client: httpx.Client, headers: dict[str, str]) -> bool:
        try:
            resp = client.get(
                f"{self.kibana_url}/api/fleet/epm/packages",
                headers=headers,
                params={"category": "opentelemetry"},
                timeout=30.0,
            )
            return resp.status_code == 200
        except Exception:
            return False
