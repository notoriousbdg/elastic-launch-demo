"""PlatformMixin — platform settings configuration methods."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path

import httpx

from elastic_config.deployer_base import _kibana_headers, _es_headers, ProgressCallback

logger = logging.getLogger("deployer-platform")

_SECURITY_DIR = Path(__file__).parent / "security"


class PlatformMixin:

    def _configure_platform_settings(
        self, client: httpx.Client, notify: ProgressCallback
    ):
        """Enable wired streams, significant events, agent builder, and AI docs."""
        step = self._step(4)
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
        # On Elastic Cloud with a Cloud API key, configure via the deployment
        # plan's user_settings_yaml (internal/api/kibana/settings is blocked on
        # Cloud for these uiSettings.overrides keys). Otherwise call the per-key
        # settings endpoints below.
        _did_cloud_settings = False
        if self._is_elastic_cloud() and self.cloud_api_key:
            cloud_ok, cloud_err = self._configure_cloud_settings(client)
            configured.extend(cloud_ok)
            errors.extend(cloud_err)
            _did_cloud_settings = True
        elif self._is_elastic_cloud() and not self.cloud_api_key:
            logger.warning(
                "Elastic Cloud detected but no Cloud API key provided. "
                "Tech-preview settings may not be configurable."
            )

        # 2. Enable significant events
        # Use the public /api/kibana/settings endpoint (same one the Advanced Settings UI
        # uses) rather than /internal/kibana/settings, which does not apply this setting.
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

        # 6. Hide Kibana new-tab announcement popup (9.4.0+ moved this to a global setting)
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

        # 3. Merge required settings into YAML string (idempotent, no PyYAML dependency)
        required_lines = [
            "uiSettings.overrides:",
            "  workflows:ui:enabled: true",
            "  observability:streamsEnableSignificantEvents: true",
            "  aiAssistant:preferredChatExperience: agent",
        ]

        if all(line in current_settings_yaml for line in required_lines):
            configured.append("Cloud kibana.yml (already configured)")
            return configured, errors

        # Strip any existing uiSettings.overrides block and re-add it cleanly
        filtered_lines = []
        skip_overrides = False
        for line in current_settings_yaml.splitlines():
            if line.strip().startswith("uiSettings.overrides:"):
                skip_overrides = True
                continue
            if skip_overrides:
                if line.startswith("  ") or line.strip() == "":
                    continue
                skip_overrides = False
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
