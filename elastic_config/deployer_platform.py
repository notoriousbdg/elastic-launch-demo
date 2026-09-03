"""PlatformMixin — platform settings configuration methods."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path

import httpx

from elastic_config.deployer_base import _kibana_headers, _es_headers, ProgressCallback, StepIdx

logger = logging.getLogger("deployer-platform")

_SECURITY_DIR = Path(__file__).parent / "security"


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

        # 2. Enable significant events — two independent vectors, both best-effort.
        #
        # Vector A: legacy uiSetting (works on 9.5; newer builds ignore it in
        #   favour of the feature flag but it is safe to set both).
        # Vector B: runtime feature-flag override via /internal/core/_settings
        #   (required for 9.6+ where the feature gate moved to the flag).
        #   This endpoint 404s on serverless (where the flag is control-plane
        #   managed); treat 404 as "not available here" rather than an error.
        if not _did_cloud_settings:
            try:
                resp = client.post(
                    f"{self.kibana_url}/api/kibana/settings",
                    headers=_kibana_headers(self.api_key),
                    json={
                        "changes": {"observability:streamsEnableSignificantEvents": True}
                    },
                )
                if resp.status_code < 300:
                    configured.append("significant events")
                else:
                    # Fallback to internal API in case the public one isn't available
                    resp2 = client.post(
                        f"{self.kibana_url}/internal/kibana/settings",
                        headers=_kibana_headers(self.api_key),
                        json={
                            "changes": {
                                "observability:streamsEnableSignificantEvents": True
                            }
                        },
                    )
                    if resp2.status_code < 300:
                        configured.append("significant events")
                    else:
                        errors.append(
                            f"significant events (HTTP {resp.status_code}/{resp2.status_code})"
                        )
            except Exception as exc:
                errors.append(f"significant events ({exc})")

            # Vector B: runtime feature-flag override — 9.6+ without a Cloud key.
            # Skip on serverless (flag is control-plane managed; endpoint 404s).
            if not self._is_serverless():
                try:
                    flag_resp = client.put(
                        f"{self.kibana_url}/internal/core/_settings",
                        headers=_kibana_headers(self.api_key),
                        json={"feature_flags.overrides": {"streams.significantEventsAvailable": True}},
                    )
                    if flag_resp.status_code < 300:
                        logger.info("streams.significantEventsAvailable feature flag set")
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

        # 3. Enable agent builder as preferred chat experience
        if not _did_cloud_settings:
            try:
                resp = client.post(
                    f"{self.kibana_url}/internal/kibana/settings",
                    headers=_kibana_headers(self.api_key),
                    json={"changes": {"aiAssistant:preferredChatExperience": "agent"}},
                )
                if resp.status_code < 300:
                    configured.append("agent builder")
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
        if not _did_cloud_settings:
            try:
                resp = client.post(
                    f"{self.kibana_url}/internal/kibana/settings",
                    headers=_kibana_headers(self.api_key),
                    json={"changes": {"workflows:ui:enabled": True}},
                )
                if resp.status_code < 300:
                    configured.append("workflows UI")
                else:
                    errors.append(f"workflows UI (HTTP {resp.status_code})")
            except Exception as exc:
                errors.append(f"workflows UI ({exc})")

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
        cloud_headers = {
            "Authorization": f"ApiKey {self.cloud_api_key}",
            "Content-Type": "application/json",
        }

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
        # On Kibana >= 9.6 also inject feature_flags.overrides so the new gate
        # (streams.significantEventsAvailable) is satisfied alongside the legacy
        # uiSettings value.  On 9.5 the feature_flags key is unrecognised and
        # would fail Kibana config validation after the plan write — omit it.
        required_lines = [
            "uiSettings.overrides:",
            "  workflows:ui:enabled: true",
            "  observability:streamsEnableSignificantEvents: true",
            "  aiAssistant:preferredChatExperience: agent",
        ]
        if self._kibana_version_ge(9, 6):
            required_lines += [
                "feature_flags.overrides:",
                "  streams.significantEventsAvailable: true",
            ]

        if all(line in current_settings_yaml for line in required_lines):
            configured.append("Cloud kibana.yml (already configured)")
            return configured, errors

        # Strip any existing managed blocks and re-add them cleanly.
        # Handles both uiSettings.overrides and feature_flags.overrides sections.
        managed_prefixes = ("uiSettings.overrides:", "feature_flags.overrides:")
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
