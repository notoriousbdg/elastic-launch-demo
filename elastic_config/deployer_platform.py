"""PlatformMixin — platform settings configuration methods."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path

import httpx

from elastic_config.deployer_base import (
    _kibana_headers, _es_headers, _retry_http, ProgressCallback, StepIdx,
)

logger = logging.getLogger("deployer-platform")

_SECURITY_DIR = Path(__file__).parent / "security"

# Kibana cluster-plan (user_settings_yaml) lines, split by stack generation so the
# 9.5 block can be deleted and the 9.6 block evolved independently.  The 9.5 line
# is indented because it lives inside the uiSettings.overrides block; it must
# follow _PLAN_LINES_COMMON, and the 9.6 keys (top-level) must come after it.
_PLAN_LINES_COMMON = [
    "uiSettings.overrides:",
    "  workflows:ui:enabled: true",
    "  aiAssistant:preferredChatExperience: agent",
    # readonly on 9.6 — must be set via uiSettings.overrides, not via API
    "  observability:streamsEnableQueryStreams: true",
]

# [9.5] remove when 9.5 support is dropped.  On 9.5 the feature_flags / coreApp /
# xpack.* keys below are unrecognised and would fail config validation after the
# plan write, so they are omitted.
_PLAN_LINES_95 = [
    "  observability:streamsEnableSignificantEvents: true",
]

# [9.6] update as 9.6 matures.  Nightshift gates (nightshift.enabled, kibana#292333;
# streams.significantEventsAvailable kept for builds before the PR) and
# coreApp.allowDynamicConfigOverrides so runtime PUT /internal/core/_settings is honoured.
_PLAN_LINES_96 = [
    "coreApp.allowDynamicConfigOverrides: true",
    "feature_flags.overrides:",
    "  nightshift.enabled: true",
    "  streams.significantEventsAvailable: true",
    # Nightshift investigations: report available:false until these are on.
    # Same values as the 9.6 cluster plan.
    "xpack.agenticInvestigations.enabled: true",
    "xpack.proposals.enabled: true",
    "xpack.sandbox.enabled: true",
    "xpack.sandbox.host: elastic-sandbox-api.elastic-observability-ocp.elastic.dev",
    "xpack.sandbox.port: 9090",
    "xpack.sandbox.api_key: unused",
]



class PlatformMixin:

    def _configure_platform_settings(
        self, client: httpx.Client, notify: ProgressCallback
    ):
        """Enable wired streams, significant events, agent builder, and AI docs."""
        step = self._step(StepIdx.PLATFORM_SETTINGS)
        step.status = "running"
        notify(self.progress)

        configured = []
        errors = []

        # 1. Enable wired streams (idempotent — safe to call if already enabled)
        try:
            resp = client.post(
                f"{self.kibana_url}/api/streams/_enable",
                headers=_kibana_headers(self.api_key),
                json={},
            )
            if resp.status_code < 300:
                configured.append("wired streams")
            else:
                errors.append(f"wired streams (HTTP {resp.status_code})")
        except Exception as exc:
            errors.append(f"wired streams ({exc})")

        # 2 & 3 & 5. Enable tech-preview UI settings.
        # On ECH with a Cloud API key, configure via the deployment plan's
        # user_settings_yaml.  Serverless projects share the .kb.*.elastic.cloud
        # URL pattern but use a different management API and do not expose
        # user_settings_yaml — fall through to the per-key endpoints below.
        _did_cloud_settings = False
        if self._is_elastic_cloud() and self.cloud_api_key:
            cloud_ok, cloud_err = self._configure_cloud_settings(client)
            configured.extend(cloud_ok)
            errors.extend(cloud_err)
            # Only block the fallback endpoints when the cloud path actually
            # succeeded.  If it failed (e.g. deployment not found), the
            # per-key endpoints below are still the best effort we can make.
            if cloud_ok:
                _did_cloud_settings = True
        elif self._is_elastic_cloud() and not self.cloud_api_key:
            logger.warning(
                "Elastic Cloud (ECH) detected but no Cloud API key provided. "
                "Tech-preview settings may not be configurable via deployment plan."
            )

        # 2. Enable the significant-events / query-streams gates.  They differ per
        # stack generation, so this dispatches to _enable_gates_{95,96,serverless}.
        # Skipped when the Cloud plan path already wrote the equivalent settings.
        if not _did_cloud_settings:
            self._enable_stack_gates(client, configured, errors)

        # 3. Enable agent builder as preferred chat experience
        # HTTP 400 means the setting is readonly (managed via uiSettings.overrides
        # in the cluster plan on Kibana 9.6+) — treat as already configured.
        if not _did_cloud_settings:
            try:
                resp = client.post(
                    f"{self.kibana_url}/internal/kibana/settings",
                    headers=_kibana_headers(self.api_key),
                    json={"changes": {"aiAssistant:preferredChatExperience": "agent"}},
                )
                if resp.status_code < 300:
                    configured.append("agent builder")
                elif resp.status_code == 400:
                    configured.append("agent builder (plan)")
                else:
                    errors.append(f"agent builder (HTTP {resp.status_code})")
            except Exception as exc:
                errors.append(f"agent builder ({exc})")

        # 4. Install Elastic product documentation (fire-and-forget — the server job is async)
        kibana_url = self.kibana_url
        api_key = self.api_key

        def _install_ai_docs():
            try:
                with httpx.Client(timeout=180.0, verify=True) as c:
                    c.post(
                        f"{kibana_url}/internal/product_doc_base/install",
                        headers=_kibana_headers(api_key),
                        json={"inferenceId": ".elser-2-elasticsearch", "resourceType": "product_doc"},
                    )
            except Exception:
                pass

        threading.Thread(target=_install_ai_docs, daemon=True).start()
        configured.append("AI docs")

        # 5. Enable workflows UI
        # HTTP 400 means the setting is readonly (managed via uiSettings.overrides
        # in the cluster plan on Kibana 9.6+) — treat as already configured.
        if not _did_cloud_settings:
            try:
                resp = client.post(
                    f"{self.kibana_url}/internal/kibana/settings",
                    headers=_kibana_headers(self.api_key),
                    json={"changes": {"workflows:ui:enabled": True}},
                )
                if resp.status_code < 300:
                    configured.append("workflows UI")
                elif resp.status_code == 400:
                    configured.append("workflows UI (plan)")
                else:
                    errors.append(f"workflows UI (HTTP {resp.status_code})")
            except Exception as exc:
                errors.append(f"workflows UI ({exc})")

        # 5b. [9.6] Configure Significant Events discovery to cover the full rule fleet
        # on every scheduled run (targetCoverageMinutes <= detectionIntervalMinutes).
        # The default round-robin only evaluates ~1/3 of rules per run, which means
        # a triggered rule might not be analysed until the 3rd cycle (~15 min).
        # With full coverage, discovery analyses all rules on every 5-minute run.
        # The endpoint does not exist on 9.5 (404), so skip it there.
        # Best-effort: 404 means the endpoint doesn't exist on this build (fine).
        if self._stack_generation() != "9.5":
            try:
                se_config_resp = client.put(
                    f"{self.kibana_url}/internal/streams/significant_events/config",
                    headers=_kibana_headers(self.api_key),
                    json={
                        "detectionIntervalMinutes": 5,
                        "targetCoverageMinutes": 5,
                    },
                )
                if se_config_resp.status_code < 300:
                    configured.append("SE discovery config")
                elif se_config_resp.status_code not in (404, 405):
                    logger.debug(
                        "SE discovery config skipped (HTTP %s)",
                        se_config_resp.status_code,
                    )
            except Exception as exc:
                logger.debug("SE discovery config attempt raised: %s", exc)

        # 6. Hide Kibana new-tab announcement popup (9.4+/9.5: global setting)
        try:
            resp = client.post(
                f"{self.kibana_url}/api/kibana/settings",
                headers=_kibana_headers(self.api_key),
                json={"changes": {"hideAnnouncements": True}},
            )
            if resp.status_code < 300:
                configured.append("hide announcements")
            else:
                resp2 = client.post(
                    f"{self.kibana_url}/internal/kibana/settings",
                    headers=_kibana_headers(self.api_key),
                    json={"changes": {"hideAnnouncements": True}},
                )
                if resp2.status_code < 300:
                    configured.append("hide announcements")
                else:
                    errors.append(
                        f"hide announcements (HTTP {resp.status_code}/{resp2.status_code})"
                    )
        except Exception as exc:
            errors.append(f"hide announcements ({exc})")

        # 7 & 8. Create viewer-custom role and guest user (only when KIBANA_RO_PASSWORD is set)
        ro_password = os.getenv("KIBANA_RO_PASSWORD", "").strip()
        if ro_password:
            try:
                role_body = json.loads(
                    (_SECURITY_DIR / "roles" / "viewer-custom.json").read_text()
                )
                role_body.pop("transient_metadata", None)
                resp = client.put(
                    f"{self.elastic_url}/_security/role/viewer-custom",
                    headers=_es_headers(self.api_key),
                    json=role_body,
                )
                if resp.status_code < 300:
                    configured.append("viewer-custom role")
                else:
                    errors.append(f"viewer-custom role (HTTP {resp.status_code})")
            except Exception as exc:
                errors.append(f"viewer-custom role ({exc})")

            try:
                user_body = json.loads(
                    (_SECURITY_DIR / "users" / "guest.json").read_text()
                )
                user_body["password"] = ro_password
                resp = client.put(
                    f"{self.elastic_url}/_security/user/guest",
                    headers=_es_headers(self.api_key),
                    json=user_body,
                )
                if resp.status_code < 300:
                    configured.append("guest user")
                else:
                    errors.append(f"guest user (HTTP {resp.status_code})")
            except Exception as exc:
                errors.append(f"guest user ({exc})")

        if configured:
            step.status = "ok"
            step.detail = f"Enabled: {', '.join(configured)}"
            if errors:
                step.detail += f"; failed: {', '.join(errors)}"
        else:
            step.status = "failed"
            step.detail = f"Failed: {', '.join(errors)}"

        notify(self.progress)

    # ── Significant-events gates, per stack generation ────────────────────
    #
    # Dispatcher + one method per generation (see _stack_generation in deployer.py).
    # Search for "[9.5]" to find what to delete when 9.5 support is dropped; the
    # "[9.6]" method is the one to keep updating as 9.6 matures.

    def _enable_stack_gates(
        self, client: httpx.Client, configured: list[str], errors: list[str]
    ) -> None:
        gen = self._stack_generation()
        if gen == "serverless":
            self._enable_gates_serverless(client, configured, errors)
        elif gen == "9.6":
            self._enable_gates_96(client, configured, errors)
        else:
            self._enable_gates_95(client, configured, errors)

    def _set_ui_setting(
        self, client: httpx.Client, key: str, label: str,
        configured: list[str], errors: list[str],
    ) -> None:
        """Set a boolean Advanced Setting to true via the public settings API.

        HTTP 400 means the setting is readonly (pinned by uiSettings.overrides in the
        cluster plan) — treated as already configured.
        """
        try:
            resp = client.post(
                f"{self.kibana_url}/api/kibana/settings",
                headers=_kibana_headers(self.api_key),
                json={"changes": {key: True}},
            )
            if resp.status_code < 300:
                configured.append(label)
            elif resp.status_code == 400:
                configured.append(f"{label} (plan)")
            else:
                errors.append(f"{label} (HTTP {resp.status_code})")
        except Exception as exc:
            errors.append(f"{label} ({exc})")

    # ── [9.5] remove when 9.5 support is dropped ──────────────────────────

    def _enable_gates_95(
        self, client: httpx.Client, configured: list[str], errors: list[str]
    ) -> None:
        """[9.5] Both gates are plain uiSettings and can be set at runtime."""
        self._set_ui_setting(
            client, "observability:streamsEnableSignificantEvents",
            "significant events", configured, errors,
        )
        self._set_ui_setting(
            client, "observability:streamsEnableQueryStreams",
            "query streams", configured, errors,
        )

    # ── [9.6] update as 9.6 matures ───────────────────────────────────────

    def _enable_gates_96(
        self, client: httpx.Client, configured: list[str], errors: list[str]
    ) -> None:
        """[9.6] Significant events are gated by feature flags, not a uiSetting.

        The uiSetting observability:streamsEnableSignificantEvents was removed in
        9.6 (PR #278392).  As of kibana#292333 the outer Nightshift gate is
        nightshift.enabled; streams.significantEventsAvailable is kept alongside it
        so the demo works on builds from before and after that PR.  Set at runtime
        via PUT /internal/core/_settings, which needs
        coreApp.allowDynamicConfigOverrides in kibana.yml (the Cloud plan injects it).

        streamsEnableQueryStreams is readonly on 9.6: it can only come from
        uiSettings.overrides in the Cloud plan (_PLAN_LINES_COMMON).
        """
        try:
            flag_resp = client.put(
                f"{self.kibana_url}/internal/core/_settings",
                headers=_kibana_headers(self.api_key),
                json={"feature_flags.overrides": {
                    "nightshift.enabled": True,
                    "streams.significantEventsAvailable": True,
                }},
            )
            if flag_resp.status_code < 300:
                logger.info("nightshift.enabled + streams.significantEventsAvailable feature flags set")
            elif flag_resp.status_code == 404:
                logger.debug(
                    "/internal/core/_settings not available on this build "
                    "(HTTP 404) — skipping feature flag override"
                )
            else:
                logger.warning(
                    "Feature flag override failed (HTTP %s): %s",
                    flag_resp.status_code, flag_resp.text[:200],
                )
        except Exception as exc:
            logger.warning("Feature flag override raised: %s", exc)

    # ── [serverless] ──────────────────────────────────────────────────────

    def _enable_gates_serverless(
        self, client: httpx.Client, configured: list[str], errors: list[str]
    ) -> None:
        """[serverless] The feature flags are managed by Elastic and
        /internal/core/_settings 404s, so never call it.  Try the uiSetting for
        projects that still expose it; a 400/403 means Elastic manages it.
        """
        soft_errors: list[str] = []
        self._set_ui_setting(
            client, "observability:streamsEnableQueryStreams",
            "query streams", configured, soft_errors,
        )
        if soft_errors:
            logger.info("serverless: %s not settable — managed by Elastic", ", ".join(soft_errors))
        self._configure_sandbox_secrets_serverless(client, configured, errors)
        if "Nightshift sandbox secrets" in configured:
            return  # builds that still have sandbox secrets need no connector
        try:
            # The investigation agent only accepts the connector once Nightshift is enabled.
            self._enable_nightshift_serverless(client, configured, errors)
            self._configure_sandbox_connector_serverless(client, configured, errors)
        except (httpx.HTTPError, ValueError) as exc:
            errors.append(f"nightshift sandbox connector ({exc.__class__.__name__})")

    # Nightshift's "Another Elastic deployment" credentials are sandbox secrets.  They
    # also exist on 9.6; this method can be reused there once it is tested on 9.6.
    _SANDBOX_SECRETS_PATH = "/internal/nightshift/sandbox_secrets"
    _SANDBOX_SECRETS_ATTEMPTS = 3

    def _configure_sandbox_secrets_serverless(
        self, client: httpx.Client, configured: list[str], errors: list[str]
    ) -> None:
        """[serverless] Give Nightshift's sandbox this project's own ES URL and API key.

        The same two entries the onboarding form's "Another Elastic deployment" button
        saves.  PUT replaces the whole list, so keys already stored are sent back
        without a value (kept); the version from GET guards against concurrent edits
        (409 -> re-read and retry).  Values are never logged.
        """
        ours = {
            "ELASTICSEARCH_URL": self.elastic_url,
            "ELASTICSEARCH_API_KEY": self.api_key,
        }
        url = f"{self.kibana_url}{self._SANDBOX_SECRETS_PATH}"
        headers = _kibana_headers(self.api_key)
        last = "no response"
        try:
            for _ in range(self._SANDBOX_SECRETS_ATTEMPTS):
                resp = client.get(url, headers=headers)
                if resp.status_code == 404:
                    logger.info("serverless: sandbox secrets route absent; using a sandbox connector")
                    return
                if resp.status_code >= 300:
                    last = f"HTTP {resp.status_code} reading secrets"
                    break
                current = resp.json()
                if not current.get("canEncrypt", True):
                    errors.append(
                        "nightshift sandbox secrets (need xpack.encryptedSavedObjects.encryptionKey)"
                    )
                    return
                entries = [{"key": k} for k in current.get("keys", []) if k not in ours]
                entries += [{"key": k, "value": v} for k, v in ours.items()]
                body: dict = {"entries": entries}
                if current.get("version") is not None:
                    body["version"] = current["version"]
                resp = client.put(url, headers=headers, json=body)
                if resp.status_code < 300:
                    # Names only: the values are the project's API key.
                    logger.info(
                        "serverless: Nightshift sandbox secrets saved (%s)", ", ".join(ours)
                    )
                    configured.append("Nightshift sandbox secrets")
                    return
                last = f"HTTP {resp.status_code}"
                if resp.status_code != 409:
                    logger.warning("Sandbox secrets save failed: %s", resp.text[:300])
                    break
        except (httpx.HTTPError, ValueError) as exc:
            last = exc.__class__.__name__
        errors.append(f"nightshift sandbox secrets ({last})")

    # "Enable Nightshift" in the UI: create (or reuse) the agent and automations
    # service accounts, bind them, then mark onboarding enabled.  The same calls
    # the button makes, so a fresh project needs no click before investigations
    # can run and before the investigation agent accepts a connector.
    _SERVICE_ACCOUNT_PATH = "/internal/security/service_account"
    _NIGHTSHIFT_SA_STATUS_PATH = "/internal/nightshift/investigations/service_account"
    _NIGHTSHIFT_SA_DESCRIPTIONS = {
        "agent": "Runs Nightshift investigations, their sandbox tools and learning hooks. Created by Nightshift.",
        "automations": "Runs Nightshift automations, which start investigations. Created by Nightshift.",
    }

    def _enable_nightshift_serverless(
        self, client: httpx.Client, configured: list[str], errors: list[str]
    ) -> None:
        """[serverless] Enable Nightshift the way its onboarding button does."""
        headers = _kibana_headers(self.api_key)
        status = client.get(f"{self.kibana_url}{self._NIGHTSHIFT_SA_STATUS_PATH}", headers=headers)
        if status.status_code == 404:
            return  # build without Nightshift service accounts
        if status.status_code < 300:
            st = status.json()
            if not st.get("serviceAccountsEnabled", True):
                return
            if st.get("installed") and st.get("enabled") and st.get("bound"):
                return  # already enabled

        accounts: list[dict] = []
        after = None
        while True:
            params = {"limit": 100, **({"after": after} if after else {})}
            resp = client.get(
                f"{self.kibana_url}{self._SERVICE_ACCOUNT_PATH}", headers=headers, params=params,
            )
            if resp.status_code >= 300:
                errors.append(f"enable nightshift (list service accounts HTTP {resp.status_code})")
                return
            page = resp.json()
            accounts += page.get("serviceAccounts", [])
            after = page.get("nextPage")
            if not after:
                break

        ids: dict[str, str] = {}
        for kind, description in self._NIGHTSHIFT_SA_DESCRIPTIONS.items():
            name, role = f"nightshift_{kind}", f"_nightshift_{kind}"
            match = next(
                (a for a in accounts
                 if a.get("name") == name and role in (a.get("roles") or [])
                 and a.get("enabled") and a.get("assumable")),
                None,
            )
            if match:
                ids[kind] = match["id"]
                continue
            resp = client.post(
                f"{self.kibana_url}{self._SERVICE_ACCOUNT_PATH}", headers=headers,
                json={"name": name, "description": description, "roles": [role]},
            )
            created = resp.json().get("id") if resp.status_code < 300 else None
            if not created and "UIAM" in resp.text:
                # Serverless service accounts live in UIAM and can only be created as a
                # signed-in user; an API key is refused.  The UI button does it once,
                # and the connector watcher finishes the setup after that click.
                logger.info("serverless: Nightshift needs the one-time 'Enable Elastic Nightshift' click")
                self._nightshift_enable_pending = True
                return
            if not created:
                errors.append(f"enable nightshift (create {name} HTTP {resp.status_code})")
                logger.warning("Creating %s failed: %s", name, resp.text[:300])
                return
            ids[kind] = created

        for path, body in (
            (f"{self._NIGHTSHIFT_SA_STATUS_PATH}/_enable", {"serviceAccountIds": ids}),
            ("/internal/nightshift/onboarding/enable", None),
        ):
            resp = _retry_http(
                lambda: client.post(f"{self.kibana_url}{path}", headers=headers, json=body),
                label=f"POST {path}",
            )
            if resp is None or resp.status_code >= 300:
                detail = f"HTTP {resp.status_code}" if resp is not None else "no response"
                errors.append(f"enable nightshift ({path.rsplit('/', 2)[-2]} {detail})")
                if resp is not None:
                    logger.warning("POST %s failed: %s", path, resp.text[:300])
                return
        logger.info("serverless: Nightshift enabled (service accounts bound)")
        configured.append("Nightshift enabled")

    # Newer builds drop sandbox secrets: Nightshift's sandbox queries data through
    # connectors whose auth type supports the sandbox (sandbox_connector_types).
    # Without one, investigations cannot run a single query.  One connector per
    # project, shared by every scenario, so teardown leaves it in place.
    _SANDBOX_CONNECTOR_ID = "launch-next-nightshift-es"
    _SANDBOX_CONNECTOR_NAME = "Nightshift sandbox: this project"
    _SANDBOX_CONNECTORS_PATH = "/internal/nightshift/sandbox_connectors"
    _SANDBOX_CONNECTOR_TYPES_PATH = "/internal/nightshift/sandbox_connector_types"
    _NIGHTSHIFT_AGENT_ID = "nightshift.investigation"
    _NIGHTSHIFT_AGENT_ATTEMPTS = 12
    _NIGHTSHIFT_AGENT_DELAY = 5.0

    def _configure_sandbox_connector_serverless(
        self, client: httpx.Client, configured: list[str], errors: list[str]
    ) -> None:
        """[serverless] Give Nightshift's sandbox an External Elasticsearch connector
        to this project, authenticated with the deploy API key.  Values are never logged.
        """
        headers = _kibana_headers(self.api_key)
        types = client.get(f"{self.kibana_url}{self._SANDBOX_CONNECTOR_TYPES_PATH}", headers=headers)
        if types.status_code == 404:
            return  # older build: sandbox secrets cover it
        if types.status_code < 300 and not any(
            t.get("id") == ".elasticsearch" for t in types.json().get("connector_types", [])
        ):
            errors.append("nightshift sandbox connector (.elasticsearch type not offered)")
            return

        connector_url = f"{self.kibana_url}/api/actions/connector/{self._SANDBOX_CONNECTOR_ID}"
        body = {
            "name": self._SANDBOX_CONNECTOR_NAME,
            "config": {
                "url": self.elastic_url,
                "authType": "api_key_header",
                "headerField": "Authorization",
            },
            "secrets": {
                "authType": "api_key_header",
                "Authorization": f"ApiKey {self.api_key}",
            },
        }
        existing = _retry_http(
            lambda: client.get(connector_url, headers=headers),
            label="get sandbox connector",
        )
        if existing is not None and existing.status_code < 300:
            resp = _retry_http(
                lambda: client.put(connector_url, headers=headers, json=body),
                label="update sandbox connector",
            )
        else:
            resp = _retry_http(
                lambda: client.post(
                    connector_url, headers=headers,
                    json={"connector_type_id": ".elasticsearch", **body},
                ),
                label="create sandbox connector",
            )
        if resp is None or resp.status_code >= 300:
            detail = f"HTTP {resp.status_code}" if resp is not None else "no response"
            if resp is not None:
                logger.warning("Sandbox connector save failed: %s", resp.text[:300])
            errors.append(f"nightshift sandbox connector ({detail})")
            return

        listed = client.get(f"{self.kibana_url}{self._SANDBOX_CONNECTORS_PATH}", headers=headers)
        ids = (
            {c.get("id") for c in listed.json().get("connectors", [])}
            if listed.status_code < 300 else set()
        )
        if self._SANDBOX_CONNECTOR_ID not in ids:
            errors.append("nightshift sandbox connector (saved but not offered to the sandbox)")
            return

        if getattr(self, "_nightshift_enable_pending", False):
            self._watch_nightshift_enable()
            configured.append("Nightshift sandbox connector (assigned once Nightshift is enabled)")
            return
        if self._assign_sandbox_connector(client, errors):
            logger.info("serverless: Nightshift sandbox connector ready")
            configured.append("Nightshift sandbox connector")

    def _assign_sandbox_connector(self, client: httpx.Client, errors: list[str]) -> bool:
        """Add the sandbox connector to the investigation agent.  Returns success."""
        headers = _kibana_headers(self.api_key)
        # Being offered is not enough: the sandbox only gets connectors assigned to the
        # investigation agent (Nightshift settings > connectors does the same update).
        agent_url = f"{self.kibana_url}/api/agent_builder/agents/{self._NIGHTSHIFT_AGENT_ID}"
        for _ in range(self._NIGHTSHIFT_AGENT_ATTEMPTS):
            agent = client.get(agent_url, headers=headers)
            if agent.status_code < 300:
                break
            time.sleep(self._NIGHTSHIFT_AGENT_DELAY)  # installed by the enable step above
        if agent.status_code >= 300:
            errors.append(f"nightshift sandbox connector (agent HTTP {agent.status_code})")
            return False
        assigned = agent.json().get("configuration", {}).get("connector_ids") or []
        if self._SANDBOX_CONNECTOR_ID not in assigned:
            resp = _retry_http(
                lambda: client.put(
                    agent_url, headers=headers,
                    json={"configuration": {"connector_ids": [*assigned, self._SANDBOX_CONNECTOR_ID]}},
                ),
                label="assign sandbox connector",
            )
            if resp is None or resp.status_code >= 300:
                detail = f"HTTP {resp.status_code}" if resp is not None else "no response"
                errors.append(f"nightshift sandbox connector (assign {detail})")
                return False
        return True

    _NIGHTSHIFT_WATCH_INTERVAL = 30.0
    _NIGHTSHIFT_WATCH_MAX = 12 * 3600.0

    def _nightshift_enabled(self, client: httpx.Client) -> bool:
        resp = client.get(
            f"{self.kibana_url}{self._NIGHTSHIFT_SA_STATUS_PATH}", headers=_kibana_headers(self.api_key),
        )
        st = resp.json() if resp.status_code < 300 else {}
        return bool(st.get("installed") and st.get("enabled") and st.get("bound"))

    def _watch_nightshift_enable(self) -> None:
        """Daemon thread: once someone clicks "Enable Elastic Nightshift", assign the
        sandbox connector.  Returns immediately; never raises."""
        def _worker():
            deadline = time.monotonic() + self._NIGHTSHIFT_WATCH_MAX
            with httpx.Client(timeout=60.0) as client:
                while time.monotonic() < deadline:
                    try:
                        if self._nightshift_enabled(client):
                            errors: list[str] = []
                            if self._assign_sandbox_connector(client, errors):
                                logger.info("Nightshift enabled: sandbox connector assigned")
                            else:
                                logger.warning("Nightshift enabled, connector assignment failed: %s", errors)
                            return
                    except (httpx.HTTPError, ValueError) as exc:
                        logger.debug("Nightshift enable watcher: %s", exc)
                    time.sleep(self._NIGHTSHIFT_WATCH_INTERVAL)
            logger.warning("Nightshift was not enabled within 12h; sandbox connector not assigned")

        threading.Thread(target=_worker, daemon=True, name="nightshift-enable-watch").start()
        logger.info("Nightshift enable watcher: waiting for the one-time enable click")

    def _configure_cloud_settings(self, client: httpx.Client) -> tuple[list[str], list[str]]:
        """Use Elastic Cloud management API to set Kibana user settings.

        Discovers the deployment matching self.kibana_url, patches the Kibana
        plan's user_settings_yaml with our uiSettings.overrides block, and waits
        for Kibana to come back. Idempotent — skips the update if the required
        lines are already present.

        Returns (configured, errors) lists for progress reporting.
        """
        configured: list[str] = []
        errors: list[str] = []
        cloud_base = "https://api.elastic-cloud.com/api/v1"
        cloud_headers = _es_headers(self.cloud_api_key)

        # 1. Find deployment by matching Kibana URL
        try:
            resp = client.get(
                f"{cloud_base}/deployments",
                headers=cloud_headers,
                params={"show_plan_defaults": "false", "show_metadata": "true"},
            )
            if resp.status_code != 200:
                errors.append(f"Cloud API list deployments (HTTP {resp.status_code})")
                return configured, errors

            deployments = resp.json().get("deployments", [])
            deployment_id = None
            kibana_ref_id = None

            kb_host = self.kibana_url.replace("https://", "").replace("http://", "").rstrip("/")
            for dep in deployments:
                for res in dep.get("resources", {}).get("kibana", []):
                    info = res.get("info", {})
                    service_url = info.get("metadata", {}).get("service_url", "")
                    if kb_host in service_url:
                        deployment_id = dep["id"]
                        kibana_ref_id = res.get("ref_id", "main-kibana")
                        break
                if deployment_id:
                    break

            if not deployment_id:
                errors.append("Could not find deployment matching Kibana URL in Cloud API")
                return configured, errors

            logger.info("Found Cloud deployment %s (kibana ref: %s)", deployment_id, kibana_ref_id)

        except Exception as exc:
            errors.append(f"Cloud API discovery ({exc})")
            return configured, errors

        # 2. Get current Kibana user settings
        try:
            resp = client.get(
                f"{cloud_base}/deployments/{deployment_id}",
                headers=cloud_headers,
                params={"show_plan_defaults": "false", "show_metadata": "false",
                        "show_settings": "true"},
            )
            if resp.status_code != 200:
                errors.append(f"Cloud API get deployment (HTTP {resp.status_code})")
                return configured, errors

            dep_data = resp.json()
            kibana_resources = dep_data.get("resources", {}).get("kibana", [])
            kibana_resource = None
            for kr in kibana_resources:
                if kr.get("ref_id") == kibana_ref_id:
                    kibana_resource = kr
                    break
            if not kibana_resource:
                kibana_resource = kibana_resources[0] if kibana_resources else None

            if not kibana_resource:
                errors.append("No Kibana resource found in deployment")
                return configured, errors

            current_plan = kibana_resource.get("info", {}).get("plan_info", {}).get("current", {}).get("plan", {})
            current_settings_yaml = current_plan.get("kibana", {}).get("user_settings_yaml", "")

        except Exception as exc:
            errors.append(f"Cloud API read settings ({exc})")
            return configured, errors

        # 3. Merge required settings into YAML string (idempotent, no PyYAML dependency).
        # Lines are assembled per stack generation from the _PLAN_LINES_* constants.
        required_lines = list(_PLAN_LINES_COMMON)
        if self._stack_generation() == "9.5":
            required_lines += _PLAN_LINES_95
        else:
            required_lines += _PLAN_LINES_96

        if all(line in current_settings_yaml for line in required_lines):
            configured.append("Cloud kibana.yml (already configured)")
            return configured, errors

        # Strip any existing managed blocks and re-add them cleanly.
        # Handles uiSettings.overrides, feature_flags.overrides, and the
        # top-level coreApp.allowDynamicConfigOverrides line.
        managed_prefixes = (
            "uiSettings.overrides:",
            "coreApp.allowDynamicConfigOverrides:",
            "feature_flags.overrides:",
            "xpack.agenticInvestigations.enabled:",
            "xpack.proposals.enabled:",
            "xpack.sandbox.enabled:",
            "xpack.sandbox.host:",
            "xpack.sandbox.port:",
            "xpack.sandbox.api_key:",
        )
        filtered_lines = []
        skip_block = False
        for line in current_settings_yaml.splitlines():
            if any(line.strip().startswith(p) for p in managed_prefixes):
                skip_block = True
                continue
            if skip_block:
                if line.startswith("  ") or line.strip() == "":
                    continue
                skip_block = False
            filtered_lines.append(line)

        new_settings_yaml = "\n".join(filtered_lines).strip()
        if new_settings_yaml:
            new_settings_yaml += "\n"
        new_settings_yaml += "\n".join(required_lines) + "\n"

        # 4. Update the deployment plan with new Kibana user settings
        try:
            update_payload = {
                "prune_orphans": False,
                "resources": {
                    "kibana": [{
                        "ref_id": kibana_ref_id,
                        "region": kibana_resource.get(
                            "region",
                            dep_data.get("resources", {}).get("kibana", [{}])[0].get("region", ""),
                        ),
                        "plan": {
                            "kibana": {
                                "user_settings_yaml": new_settings_yaml,
                            },
                            "cluster_topology": current_plan.get("cluster_topology", []),
                        },
                    }],
                },
            }

            resp = client.put(
                f"{cloud_base}/deployments/{deployment_id}",
                headers=cloud_headers,
                json=update_payload,
                timeout=30.0,
            )
            if resp.status_code not in (200, 201, 202):
                body = resp.text[:500]
                errors.append(f"Cloud API update deployment (HTTP {resp.status_code}: {body})")
                return configured, errors

            logger.info("Cloud deployment update accepted — waiting for Kibana restart")

        except Exception as exc:
            errors.append(f"Cloud API update ({exc})")
            return configured, errors

        # 5. Wait for Kibana to come back (up to 3 min)
        deadline = time.time() + 180
        while time.time() < deadline:
            time.sleep(10)
            try:
                resp = client.get(
                    f"{self.kibana_url}/api/status",
                    headers=_kibana_headers(self.api_key),
                    timeout=10.0,
                )
                if resp.status_code == 200:
                    logger.info("Kibana is back after Cloud settings update")
                    configured.append("Cloud kibana.yml (updated + restarted)")
                    return configured, errors
            except Exception:
                continue

        errors.append("Kibana did not come back within 3 minutes after Cloud settings update")
        return configured, errors
