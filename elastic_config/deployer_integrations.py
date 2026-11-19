"""IntegrationsMixin — install Elastic Fleet/EPM integrations used by the demo."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from typing import Any, Callable

import httpx

from elastic_config.deployer_package_templates import package_datasets
from elastic_config.deployer_base import (
    DeployStep,
    StepIdx,
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

# Dynamically generated Idle data streams templates — skip for OOTB rule materialization.
_IDLE_TEMPLATE_RE = re.compile(r"^fleet-.+-inactivity-monitoring")

_ESQL_SOURCE_RE = re.compile(r"^\s*(?:FROM|TS)\s+([^\s|,]+(?:\s*,\s*[^\s|,]+)*)", re.IGNORECASE)


def esql_control_data_view_id(pattern: str) -> str:
    """Id Kibana gives the ad-hoc data view behind an ES|QL-backed control.

    Kibana 9.6 looks the view up by this id and shows "Could not locate that
    data view (id: …), click here to re-create it" when it is missing.
    """
    return hashlib.sha256(f"esql-{pattern}-@timestamp".encode()).hexdigest()


def esql_control_patterns(dashboard_attributes: dict[str, Any]) -> set[str]:
    """Index patterns of a dashboard's ES|QL-backed options list controls."""
    pinned = dashboard_attributes.get("pinned_panels") or {}
    if isinstance(pinned, str):
        try:
            pinned = json.loads(pinned)
        except ValueError:
            return set()
    patterns: set[str] = set()
    for panel in (pinned.get("panels") or {}).values():
        config = panel.get("config") or {}
        if panel.get("type") != "options_list_control" or config.get("values_source") != "esql":
            continue
        match = _ESQL_SOURCE_RE.match(config.get("esql_query") or "")
        if match:
            patterns.add(re.sub(r"\s+", "", match.group(1)))
    return patterns


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

    def _packages_to_install(self) -> list[str]:
        """Resolve integration packages from the active scenario generators block.

        Falls back to the static INTEGRATIONS baseline when no scenario is set
        or registry resolution fails.
        """
        scenario = getattr(self, "scenario", None)
        if scenario is None:
            return list(INTEGRATIONS)
        try:
            from generator_engine.registry import integrations_for_scenario

            pkgs = integrations_for_scenario(scenario)
            return pkgs if pkgs else list(INTEGRATIONS)
        except Exception as exc:
            logger.warning(
                "integrations_for_scenario failed (%s); using baseline INTEGRATIONS",
                exc,
            )
            return list(INTEGRATIONS)

    def _install_integrations(self, client: httpx.Client, notify: ProgressCallback):
        packages = self._packages_to_install()
        step = self._step(StepIdx.INTEGRATIONS)
        step.status = "running"
        step.items_total = len(packages)
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

        try:
            from generator_engine.registry import package_versions

            pins = package_versions()
        except Exception as exc:
            logger.warning("Package version pins unavailable (%s); installing latest", exc)
            pins = {}
        unpinned: list[str] = []

        for pkg in packages:
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
                # Install the version the generator was built against (its
                # SOURCE_INTEGRATION), not whatever is newest — a newer package can
                # read fields the generator does not emit yet. ``force`` lets Fleet
                # move an already-installed newer version back to the pin.
                target = pins.get(pkg) or latest
                if not target:
                    errors.append(f"{pkg} (no version in registry response)")
                    step.items_done += 1
                    notify(self.progress)
                    continue

                if current == target:
                    skipped.append(f"{pkg}@{target}")
                else:
                    resp = self._install_package(client, pkg, target, headers)
                    if resp is not None and resp.status_code < 300:
                        installed.append(f"{pkg}@{target}")
                    else:
                        resp = self._install_package(client, pkg, None, headers)
                        if resp is not None and resp.status_code < 300:
                            installed.append(f"{pkg}@latest")
                            if pins.get(pkg):
                                unpinned.append(f"{pkg} (pinned {target} unavailable)")
                                logger.warning(
                                    "%s@%s could not be installed; fell back to latest", pkg, target,
                                )
                        else:
                            code = resp.status_code if resp is not None else "no-response"
                            errors.append(f"{pkg} (install HTTP {code})")
            except Exception as exc:
                errors.append(f"{pkg} ({exc})")

            step.items_done += 1
            notify(self.progress)

        control_views = self._ensure_esql_control_data_views(
            client, headers, [p.split("@")[0] for p in installed + skipped]
        )

        parts = []
        if ready_err:
            parts.append(f"fleet wait: {ready_err}")
        if control_views:
            parts.append(f"{control_views} ES|QL control data view(s)")
        if installed:
            parts.append(f"installed {len(installed)}: {', '.join(installed)}")
        if skipped:
            parts.append(f"already current: {', '.join(skipped)}")
        if unpinned:
            parts.append(f"NOT at the generator's version: {', '.join(unpinned)}")
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

    def _ensure_esql_control_data_views(
        self, client: httpx.Client, headers: dict[str, str], packages: list[str]
    ) -> int:
        """Create the data views that installed dashboards' ES|QL controls look up.

        Package dashboards built for Kibana 9.6 (the GCP OTel packs, for example)
        filter with ES|QL-backed options list controls. Kibana resolves each one
        through an ad-hoc data view it never saves, so every control shows a
        "Could not locate that data view" banner. Saving a data view with the
        same id (see :func:`esql_control_data_view_id`) — what the banner's
        "re-create it" link does — clears it. The package dashboards are not
        edited. Returns how many data views were created or updated.
        """
        patterns: set[str] = set()
        for pkg in packages:
            try:
                item, _ = self._lookup_package_once(client, pkg, headers, prerelease=True)
                assets = ((item or {}).get("installationInfo") or {}).get("installed_kibana") or []
                ids = [a["id"] for a in assets if a.get("type") == "dashboard"]
                if not ids:
                    continue
                resp = client.post(
                    f"{self.kibana_url}/api/saved_objects/_export",
                    headers=headers,
                    json={"objects": [{"type": "dashboard", "id": i} for i in ids],
                          "includeReferencesDeep": False},
                    timeout=60.0,
                )
                if resp.status_code >= 300:
                    logger.warning("Dashboard export for %s: HTTP %s", pkg, resp.status_code)
                    continue
                for line in resp.text.splitlines():
                    obj = json.loads(line) if line.strip() else {}
                    if obj.get("type") == "dashboard":
                        patterns |= esql_control_patterns(obj.get("attributes") or {})
            except Exception as exc:
                logger.warning("ES|QL control data views for %s skipped: %s", pkg, exc)

        created = 0
        for pattern in sorted(patterns):
            view_id = esql_control_data_view_id(pattern)
            try:
                resp = client.post(
                    f"{self.kibana_url}/api/data_views/data_view",
                    headers=headers,
                    json={"data_view": {"id": view_id, "title": pattern, "name": pattern,
                                        "timeFieldName": "@timestamp"},
                          "override": True},
                    timeout=30.0,
                )
                if resp.status_code < 300:
                    created += 1
                else:
                    logger.warning("ES|QL control data view %s: HTTP %s %s",
                                   pattern, resp.status_code, resp.text[:200])
            except Exception as exc:
                logger.warning("ES|QL control data view %s failed: %s", pattern, exc)
        return created

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

    # ── OOTB alert rules from integration templates ───────────────────

    @staticmethod
    def _is_idle_template_id(template_id: str) -> bool:
        return bool(_IDLE_TEMPLATE_RE.match(template_id or ""))

    @staticmethod
    def _snake_key(key: str) -> str:
        """Convert camelCase / PascalCase keys to snake_case."""
        s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", key)
        return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    def _normalize_api_value(self, value: Any) -> Any:
        """Recursively normalize dict keys from camelCase to snake_case."""
        if isinstance(value, dict):
            return {
                self._snake_key(k): self._normalize_api_value(v)
                for k, v in value.items()
            }
        if isinstance(value, list):
            return [self._normalize_api_value(v) for v in value]
        return value

    def _package_version_for_templates(
        self, client: httpx.Client, pkg: str, headers: dict[str, str]
    ) -> str | None:
        """Return an installed or latest package version suitable for asset lookup."""
        item, _status = self._lookup_package(client, pkg, headers)
        if not item:
            return None
        if item.get("status") == "installed" and item.get("version"):
            return item["version"]
        return item.get("latestVersion") or item.get("version")

    def _reinstall_rule_assets(
        self,
        client: httpx.Client,
        pkg: str,
        version: str,
        headers: dict[str, str],
    ) -> None:
        """Refresh package alerting assets when the template list is empty."""
        resp = _retry_http(
            lambda: client.post(
                f"{self.kibana_url}/api/fleet/epm/packages/{pkg}/{version}/rule_assets",
                headers=headers,
                json={"force": True},
                timeout=120.0,
            ),
            label=f"reinstall rule assets {pkg}",
            attempts=3,
            base_delay=1.5,
        )
        if resp is None or resp.status_code >= 300:
            code = resp.status_code if resp is not None else "no-response"
            logger.warning("rule_assets refresh for %s@%s failed: HTTP %s", pkg, version, code)

    def _package_info(
        self,
        client: httpx.Client,
        pkg: str,
        version: str,
        headers: dict[str, str],
    ) -> dict[str, Any]:
        """Return the Fleet EPM package-info ``item`` for ``pkg@version`` ({} on failure)."""
        resp = _retry_http(
            lambda: client.get(
                f"{self.kibana_url}/api/fleet/epm/packages/{pkg}/{version}",
                headers=headers,
                params=_EPM_QUERY,
                timeout=60.0,
            ),
            label=f"package info {pkg}",
        )
        if resp is None or resp.status_code >= 300:
            return {}
        try:
            body = resp.json()
        except Exception:
            return {}
        return body.get("item") or body.get("response") or {}

    @staticmethod
    def _asset_ids(
        item: dict[str, Any],
        asset_type: str,
        *,
        installed_type: str | None = None,
        keep: Callable[[str], bool] = lambda _tid: True,
    ) -> list[str]:
        """Asset ids of one Kibana asset type in a package-info ``item``.

        Reads ``installationInfo.installed_kibana`` (entries of ``installed_type``,
        which defaults to ``asset_type``) first, then falls back to the package
        file list ``assets.kibana.<asset_type>``, where EPM often returns only a
        path (``…/<asset_type>/<id>.json``). ``keep`` filters ids before the
        fallback decision. Order is preserved and ids are deduped.
        """
        ids: list[str] = []
        installed = (item.get("installationInfo") or {}).get("installed_kibana") or []
        for asset in installed:
            if isinstance(asset, dict) and asset.get("type") == (installed_type or asset_type):
                tid = asset.get("originId") or asset.get("id")
                if tid and keep(tid):
                    ids.append(tid)

        if not ids:
            assets = ((item.get("assets") or {}).get("kibana") or {}).get(asset_type) or []
            for asset in assets:
                if isinstance(asset, dict):
                    tid = asset.get("id") or asset.get("originId")
                    if not tid:
                        path = asset.get("path") or asset.get("name") or ""
                        tid = path.rsplit("/", 1)[-1].removesuffix(".json") or None
                elif isinstance(asset, str):
                    tid = asset.rsplit("/", 1)[-1].removesuffix(".json")
                else:
                    tid = None
                if tid and keep(tid):
                    ids.append(tid)

        return list(dict.fromkeys(ids))

    def _list_package_assets(
        self,
        client: httpx.Client,
        pkg: str,
        version: str,
        asset_type: str,
        headers: dict[str, str],
        *,
        installed_type: str | None = None,
    ) -> list[str]:
        """Return the ids of every ``asset_type`` asset (e.g. ``slo_template``) a package ships."""
        item = self._package_info(client, pkg, version, headers)
        return self._asset_ids(item, asset_type, installed_type=installed_type)

    def _get_package_asset(
        self,
        client: httpx.Client,
        pkg: str,
        version: str,
        asset_type: str,
        asset_id: str,
        headers: dict[str, str],
    ) -> dict[str, Any] | None:
        """Fetch one Kibana asset file from a package through the Fleet EPM file API."""
        file_name = asset_id if asset_id.endswith(".json") else f"{asset_id}.json"
        resp = _retry_http(
            lambda: client.get(
                f"{self.kibana_url}/api/fleet/epm/packages/{pkg}/{version}"
                f"/kibana/{asset_type}/{file_name}",
                headers=headers,
                params=_EPM_QUERY,
                timeout=60.0,
            ),
            label=f"get Fleet {asset_type} {pkg}/{file_name}",
            attempts=3,
            base_delay=1.0,
        )
        if resp is None or resp.status_code >= 300:
            code = resp.status_code if resp is not None else "no-response"
            logger.warning(
                "Fleet %s %s@%s/%s failed: HTTP %s %s",
                asset_type,
                pkg,
                version,
                file_name,
                code,
                (resp.text[:200] if resp is not None else ""),
            )
            return None
        try:
            payload = resp.json()
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    def _list_package_alert_templates(
        self,
        client: httpx.Client,
        pkg: str,
        version: str,
        headers: dict[str, str],
        *,
        allow_refresh: bool = True,
    ) -> list[str]:
        """Return non-Idle alerting_rule_template ids for an installed package."""
        item = self._package_info(client, pkg, version, headers)
        if not item:
            return []
        unique = self._asset_ids(
            item,
            "alerting_rule_template",
            keep=lambda tid: not self._is_idle_template_id(tid),
        )

        if not unique and allow_refresh:
            self._reinstall_rule_assets(client, pkg, version, headers)
            return self._list_package_alert_templates(
                client, pkg, version, headers, allow_refresh=False
            )

        return unique

    def _normalize_fleet_alert_template(
        self, template_id: str, payload: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Flatten Fleet EPM ``alerting_rule_template`` saved-object shape."""
        data = payload
        # Fleet returns {id, type, attributes:{name, ruleTypeId, params, …}}.
        attrs = data.get("attributes") if isinstance(data, dict) else None
        if isinstance(attrs, dict) and (
            "ruleTypeId" in attrs or "rule_type_id" in attrs or "params" in attrs
        ):
            flat = dict(attrs)
            flat["id"] = data.get("id") or attrs.get("id") or template_id
            return flat
        if isinstance(data, dict) and "rule_type_id" not in data and "ruleTypeId" not in data:
            nested = data.get("template") or data.get("data") or data
            if isinstance(nested, dict):
                data = nested
        if not isinstance(data, dict):
            return None
        data.setdefault("id", template_id)
        return data

    def _get_alert_template_from_fleet(
        self,
        client: httpx.Client,
        pkg: str,
        version: str,
        template_id: str,
        headers: dict[str, str],
    ) -> dict[str, Any] | None:
        """Fetch template JSON from the Fleet package asset API.

        Used when ``/internal/alerting/rule_template/{id}`` is unavailable
        (common on some hosted/serverless configs — HTTP 400 "not available
        with the current configuration"). ALERTS #15/#20.
        """
        payload = self._get_package_asset(
            client, pkg, version, "alerting_rule_template", template_id, headers
        )
        if payload is None:
            return None
        return self._normalize_fleet_alert_template(template_id, payload)

    def _get_alert_template(
        self,
        client: httpx.Client,
        template_id: str,
        headers: dict[str, str],
        *,
        pkg: str | None = None,
        version: str | None = None,
    ) -> dict[str, Any] | None:
        """Fetch a full alerting rule template (internal API, then Fleet EPM)."""
        resp = _retry_http(
            lambda: client.get(
                f"{self.kibana_url}/internal/alerting/rule_template/{template_id}",
                headers=headers,
                timeout=30.0,
            ),
            label=f"get alert template {template_id}",
            attempts=2,
            base_delay=1.0,
        )
        if resp is not None and resp.status_code < 300:
            try:
                data = resp.json()
            except Exception:
                data = None
            if isinstance(data, dict):
                normalized = self._normalize_fleet_alert_template(template_id, data)
                if normalized is not None:
                    return normalized

        # Internal API missing/disabled — fall back to Fleet package assets.
        if pkg and version:
            return self._get_alert_template_from_fleet(
                client, pkg, version, template_id, headers
            )

        if resp is not None and resp.status_code == 404:
            logger.warning("Alert template %s not found", template_id)
        elif resp is not None:
            logger.warning(
                "Alert template %s fetch failed: HTTP %s %s",
                template_id,
                resp.status_code,
                resp.text[:200],
            )
        return None

    def _rule_body_from_template(self, template: dict[str, Any]) -> dict[str, Any] | None:
        """Map an alerting_rule_template payload to a public create/update rule body."""
        rule_type_id = template.get("rule_type_id") or template.get("ruleTypeId")
        name = template.get("name")
        params = template.get("params")
        schedule = template.get("schedule")
        if not rule_type_id or not name or params is None or not schedule:
            logger.warning(
                "Alert template %s missing required fields (rule_type_id/name/params/schedule)",
                template.get("id"),
            )
            return None

        # params stay camelCase — that matches the .es-query public API schema
        # (searchType, esqlQuery, timeWindowSize, …). Only top-level rule fields
        # and flapping use snake_case on the public Alerting API.
        #
        # Do NOT send template-only fields like description/artifacts — the
        # public create/update rule API rejects them (HTTP 400).
        params = dict(params)
        # Required .es-query defaults — Fleet templates often omit these and
        # create fails or evaluates oddly (ALERTS #58).
        params.setdefault("size", 100)
        params.setdefault("threshold", [0])
        params.setdefault("thresholdComparator", ">")
        params.setdefault("timeField", "@timestamp")
        params.setdefault("excludeHitsFromPreviousRun", False)
        params.setdefault("aggType", "count")

        body: dict[str, Any] = {
            "name": name,
            "tags": list(template.get("tags") or []),
            "rule_type_id": rule_type_id,
            "consumer": "alerts",
            "enabled": True,
            "schedule": schedule if isinstance(schedule, dict) else {"interval": schedule},
            "params": params,
            # A library investigation workflow wired to this rule (step 6 deployed
            # it) runs once per new alert; see deployer_workflow_templates.py.
            "actions": self._ootb_rule_actions(template) if hasattr(self, "_ootb_rule_actions") else [],
        }

        alert_delay = template.get("alert_delay") or template.get("alertDelay")
        if alert_delay is not None:
            body["alert_delay"] = self._normalize_api_value(alert_delay)

        flapping = template.get("flapping")
        if flapping is not None:
            body["flapping"] = self._normalize_api_value(flapping)

        return body

    def _upsert_rule_from_template(
        self, client: httpx.Client, template: dict[str, Any], headers: dict[str, str]
    ) -> bool:
        """Create or update a rule using the template id as the stable rule id."""
        rule_id = template.get("id")
        if not rule_id:
            return False

        body = self._rule_body_from_template(template)
        if body is None:
            return False

        get_resp = _retry_http(
            lambda: client.get(
                f"{self.kibana_url}/api/alerting/rule/{rule_id}",
                headers=headers,
                timeout=30.0,
            ),
            label=f"get alert rule {rule_id}",
            attempts=3,
            base_delay=1.0,
        )

        exists = get_resp is not None and get_resp.status_code == 200

        if not exists:
            create_body = dict(body)
            resp = _retry_http(
                lambda: client.post(
                    f"{self.kibana_url}/api/alerting/rule/{rule_id}",
                    headers=headers,
                    json=create_body,
                    timeout=60.0,
                ),
                label=f"create OOTB alert rule {rule_id}",
            )
            if resp is not None and resp.status_code < 300:
                return True
            if resp is None or resp.status_code != 409:
                if resp is not None:
                    logger.warning(
                        "Create OOTB alert rule %s failed: HTTP %s %s",
                        rule_id,
                        resp.status_code,
                        resp.text[:200],
                    )
                return False
            # 409 Conflict — rule appeared concurrently; fall through to update.

        # Update path — omit immutable fields and create-only fields.
        # Public PUT rejects `enabled` (use _enable/_disable endpoints instead).
        update_body = {
            k: v
            for k, v in body.items()
            if k not in {"rule_type_id", "consumer", "enabled"}
        }
        put_resp = _retry_http(
            lambda: client.put(
                f"{self.kibana_url}/api/alerting/rule/{rule_id}",
                headers=headers,
                json=update_body,
                timeout=60.0,
            ),
            label=f"update OOTB alert rule {rule_id}",
        )
        if put_resp is not None and put_resp.status_code < 300:
            return True

        # Immutable-field conflict or other update failure → replace.
        logger.warning(
            "Update OOTB alert rule %s failed (HTTP %s); replacing",
            rule_id,
            put_resp.status_code if put_resp is not None else "no-response",
        )
        _retry_http(
            lambda: client.delete(
                f"{self.kibana_url}/api/alerting/rule/{rule_id}",
                headers=headers,
                timeout=30.0,
            ),
            label=f"delete OOTB alert rule {rule_id}",
            attempts=3,
            base_delay=1.0,
        )
        create_body = dict(body)
        recreate = _retry_http(
            lambda: client.post(
                f"{self.kibana_url}/api/alerting/rule/{rule_id}",
                headers=headers,
                json=create_body,
                timeout=60.0,
            ),
            label=f"recreate OOTB alert rule {rule_id}",
        )
        if recreate is not None and recreate.status_code < 300:
            return True
        if recreate is not None:
            logger.warning(
                "Recreate OOTB alert rule %s failed: HTTP %s %s",
                rule_id,
                recreate.status_code,
                recreate.text[:200],
            )
        return False

    def _disable_orphaned_ootb_alerts(
        self, client: httpx.Client, headers: dict[str, str], active_packages: list[str],
    ) -> int:
        """Disable OOTB integration rules whose package is not in the active scenario.

        Switching scenarios leaves prior package alert rules enabled. Without
        matching generator data those rules fail every run with Unknown index /
        Unknown column verification_exception noise.
        """
        active = set(active_packages)
        disabled = 0
        page = 1
        while True:
            resp = _retry_http(
                lambda p=page: client.get(
                    f"{self.kibana_url}/api/alerting/rules/_find",
                    headers=headers,
                    params={"page": p, "per_page": 100},
                    timeout=60.0,
                ),
                label=f"list alert rules page {page}",
                attempts=2,
                base_delay=1.0,
            )
            if resp is None or resp.status_code >= 300:
                break
            body = resp.json()
            rules = body.get("data") or []
            if not rules:
                break
            for rule in rules:
                rid = str(rule.get("id") or "")
                if rule.get("enabled") is False:
                    continue
                # OOTB template ids are ``{package}-{slug}``.
                matched_pkg = next(
                    (
                        pkg
                        for pkg in sorted(active, key=len, reverse=True)
                        if rid == pkg or rid.startswith(f"{pkg}-")
                    ),
                    None,
                )
                if matched_pkg is not None:
                    continue
                # Only disable rules that look like integration OOTB templates.
                tags = set(rule.get("tags") or [])
                name = str(rule.get("name") or "")
                is_ootb = (
                    "otel" in rid
                    or any(t.startswith("fleet") or "integration" in t.lower() for t in tags)
                    or name.startswith("[") and " OTel]" in name
                )
                if not is_ootb:
                    continue
                dis = _retry_http(
                    lambda rule_id=rid: client.post(
                        f"{self.kibana_url}/api/alerting/rule/{rule_id}/_disable",
                        headers=headers,
                        timeout=30.0,
                    ),
                    label=f"disable orphaned alert {rid}",
                    attempts=2,
                    base_delay=1.0,
                )
                if dis is not None and dis.status_code < 300:
                    disabled += 1
                    logger.info("Disabled orphaned OOTB alert rule %s", rid)
            total = int(body.get("total") or 0)
            if page * 100 >= total:
                break
            page += 1
        return disabled

    def _create_ootb_package_templates(self, client: httpx.Client, notify: ProgressCallback):
        """Materialize each installed package's alert rule, SLO and ML templates.

        Alert rules come from ``alerting_rule_template``, SLOs from
        ``slo_template`` and anomaly detection jobs from ``ml_module`` assets.
        All three are cluster-shared (see ``deployer_package_templates``):
        packages the scenario does not install are switched off, not deleted.
        """
        step = self._step(StepIdx.OOTB_ALERTS)
        step.status = "running"
        step.items_done = 0
        step.detail = "Discovering integration package templates…"
        notify(self.progress)

        packages = self._packages_to_install()
        headers = _kibana_headers(self.api_key)

        # Collect asset ids per package first so items_total is accurate.
        alert_jobs: list[tuple[str, str, str]] = []  # (pkg, version, template_id)
        slo_jobs: list[tuple[str, str, str]] = []
        ml_jobs: list[tuple[str, str, str]] = []
        datasets: dict[str, list[str]] = {}
        for pkg in packages:
            version = self._package_version_for_templates(client, pkg, headers)
            if not version:
                logger.info("Skipping OOTB templates for %s (package not found)", pkg)
                continue
            for tid in self._list_package_alert_templates(client, pkg, version, headers):
                alert_jobs.append((pkg, version, tid))
            item = self._package_info(client, pkg, version, headers)
            datasets[pkg] = package_datasets(item)
            for tid in self._asset_ids(item, "slo_template"):
                slo_jobs.append((pkg, version, tid))
            for mid in self._asset_ids(item, "ml_module", installed_type="ml-module"):
                ml_jobs.append((pkg, version, mid))

        step.items_total = len(alert_jobs) + len(slo_jobs) + len(ml_jobs)
        notify(self.progress)

        def _tick() -> None:
            step.items_done += 1
            notify(self.progress)

        rules_ok = rules_failed = rules_wired = 0
        for pkg, version, tid in alert_jobs:
            template = self._get_alert_template(
                client, tid, headers, pkg=pkg, version=version
            )
            if template is not None:
                template.setdefault("id", tid)
            if template is not None and self._upsert_rule_from_template(client, template, headers):
                rules_ok += 1
                if hasattr(self, "_ootb_rule_actions") and self._ootb_rule_actions(template):
                    rules_wired += 1
            else:
                rules_failed += 1
            _tick()

        slos_ok = slos_failed = 0
        for pkg, version, tid in slo_jobs:
            try:
                template = self._get_package_asset(client, pkg, version, "slo_template", tid, headers)
                ok = template is not None and self._upsert_ootb_slo(client, pkg, tid, template, headers)
            except Exception as exc:  # one bad template must not abort the deploy
                logger.warning("OOTB SLO %s failed: %s", tid, exc)
                ok = False
            slos_ok += ok
            slos_failed += not ok
            _tick()

        ml_ok = ml_failed = 0
        for pkg, version, mid in ml_jobs:
            try:
                module = self._get_package_asset(client, pkg, version, "ml_module", mid, headers)
                running, failed = (
                    self._setup_ootb_ml_module(client, pkg, module, headers, datasets.get(pkg))
                    if module is not None else (0, 1)
                )
            except Exception as exc:  # one bad module must not abort the deploy
                logger.warning("OOTB ML module %s failed: %s", mid, exc)
                running, failed = 0, 1
            ml_ok += running
            ml_failed += failed
            _tick()

        orphaned = self._disable_orphaned_ootb_alerts(client, headers, packages)
        for switch_off in (
            lambda: self._disable_orphaned_ootb_slos(client, headers, packages),
            lambda: self._stop_orphaned_ootb_ml_jobs(client, packages),
        ):
            try:
                orphaned += switch_off()
            except Exception as exc:
                logger.warning("Switching off OOTB objects of inactive packages failed: %s", exc)

        failed = rules_failed + slos_failed + ml_failed
        if not (alert_jobs or slo_jobs or ml_jobs) and not orphaned:
            step.status = "skipped"
            step.detail = f"No package templates found for {len(packages)} package(s)"
            notify(self.progress)
            return
        step.detail = (
            f"{rules_ok} alert rules"
            + (f" ({rules_wired} start an investigation workflow)" if rules_wired else "")
            + f", {slos_ok} SLOs, {ml_ok} ML jobs"
            + (f"; {failed} failed" if failed else "")
            + (f"; switched off {orphaned} from inactive packages" if orphaned else "")
        )
        if rules_ok or slos_ok or ml_ok or orphaned:
            step.status = "ok"
        else:
            step.status = "failed"
        notify(self.progress)
