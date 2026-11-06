#!/bin/bash
set -euo pipefail

# Seed /root/.claude on first boot from the image snapshot (skills, etc.).
# Must happen before the settings/CLAUDE.md generation below so the directories exist.
# The marker file prevents re-seeding on subsequent starts (node failure restarts).
if [ ! -f /root/.claude/.seeded ]; then
  echo "First boot: seeding /root/.claude from image snapshot..."
  mkdir -p /root/.claude
  cp -rp /root-claude-seed/. /root/.claude/
  touch /root/.claude/.seeded
fi

# Seed the whole /app workspace on first boot.
# Because /app is mounted from a PVC it is empty on first pod start; the image
# snapshot at /app-seed is copied in exactly once. Subsequent boots (node failure
# restarts) find the marker and skip — preserving everything the user created.
if [ ! -f /app/.seeded ]; then
  echo "First boot: seeding /app from image snapshot..."
  cp -rp /app-seed/. /app/
  touch /app/.seeded
fi

node -e "
(async () => {
  const fs = require('fs');
  const e = process.env;

  const settings = {
    model: 'claude-sonnet-5[1m]',
    theme: 'dark',
    permissions: {
      defaultMode: 'plan',
      allow: [
        'Bash(*)',
        'mcp__elastic-agent-builder__*',
        'mcp__elastic-mcp-app-observability__*',
        'mcp__elastic-mcp-dashbuilder__*',
        'mcp__elastic-docs__*'
      ]
    },
    env: {
      IS_DEMO: '1',
      ANTHROPIC_BASE_URL: e.ANTHROPIC_BASE_URL || '',
      ANTHROPIC_AUTH_TOKEN: e.ANTHROPIC_AUTH_TOKEN || '',
      // LiteLLM -> Bedrock can't carry auto mode's server-side checks; use the local classifier
      // and skip the classifier billing notice.
      CLAUDE_CODE_AUTO_MODE_SERVER: '0'
    }
  };
  fs.writeFileSync('/root/.claude/settings.json', JSON.stringify(settings, null, 2));

  const state = {
    firstStartTime: new Date().toISOString(),
    opusProMigrationComplete: true,
    sonnet1m45MigrationComplete: true,
    migrationVersion: 12,
    projects: {
      '/app': {
        allowedTools: [
          'Bash(*)',
          'mcp__elastic-agent-builder__*',
          'mcp__elastic-mcp-app-observability__*',
          'mcp__elastic-mcp-dashbuilder__*',
          'mcp__elastic-docs__*'
        ],
        mcpContextUris: [],
        mcpServers: {
          'elastic-agent-builder': {
            type: 'stdio',
            command: 'npx',
            args: [
              'mcp-remote',
              (e.KIBANA_URL || '') + '/api/agent_builder/mcp',
              '--header',
              'Authorization:ApiKey ' + (e.ELASTICSEARCH_API_KEY || '')
            ],
            env: {
              NODE_OPTIONS: '--import /usr/lib/node_modules/@elastic/opentelemetry-node/import.mjs',
              OTEL_SERVICE_NAME: 'mcp-elastic-agent-builder',
              OTEL_EXPORTER_OTLP_PROTOCOL: 'http/protobuf',
              OTEL_EXPORTER_OTLP_ENDPOINT: e.INGEST_URL || '',
              OTEL_EXPORTER_OTLP_HEADERS: 'Authorization=ApiKey ' + (e.ELASTICSEARCH_API_KEY || ''),
              OTEL_RESOURCE_ATTRIBUTES: e.OTEL_RESOURCE_ATTRIBUTES || ''
            }
          },
          'elastic-mcp-app-observability': {
            type: 'stdio',
            command: 'npx',
            args: [
              '-y',
              'https://github.com/elastic/example-mcp-app-observability/releases/latest/download/example-mcp-app-observability.tgz',
              '--stdio'
            ],
            env: {
              ELASTICSEARCH_URL: e.ELASTICSEARCH_URL || '',
              ELASTICSEARCH_API_KEY: e.ELASTICSEARCH_API_KEY || '',
              KIBANA_URL: e.KIBANA_URL || '',
              KIBANA_API_KEY: e.ELASTICSEARCH_API_KEY || '',
              NODE_OPTIONS: '--import /usr/lib/node_modules/@elastic/opentelemetry-node/import.mjs',
              OTEL_SERVICE_NAME: 'mcp-elastic-mcp-app-observability',
              OTEL_EXPORTER_OTLP_PROTOCOL: 'http/protobuf',
              OTEL_EXPORTER_OTLP_ENDPOINT: e.INGEST_URL || '',
              OTEL_EXPORTER_OTLP_HEADERS: 'Authorization=ApiKey ' + (e.ELASTICSEARCH_API_KEY || ''),
              OTEL_RESOURCE_ATTRIBUTES: e.OTEL_RESOURCE_ATTRIBUTES || ''
            }
          },
          'elastic-mcp-dashbuilder': {
            type: 'stdio',
            command: 'node',
            args: [
              '/opt/example-mcp-dashbuilder/server/dist/index.js'
            ],
            env: {
              ES_NODE: e.ELASTICSEARCH_URL || '',
              ES_API_KEY: e.ELASTICSEARCH_API_KEY || '',
              KIBANA_URL: e.KIBANA_URL || '',
              NODE_OPTIONS: '--import /usr/lib/node_modules/@elastic/opentelemetry-node/import.mjs',
              OTEL_SERVICE_NAME: 'mcp-elastic-mcp-dashbuilder',
              OTEL_LOG_LEVEL: 'error',
              OTEL_EXPORTER_OTLP_PROTOCOL: 'http/protobuf',
              OTEL_EXPORTER_OTLP_ENDPOINT: e.INGEST_URL || '',
              OTEL_EXPORTER_OTLP_HEADERS: 'Authorization=ApiKey ' + (e.ELASTICSEARCH_API_KEY || ''),
              OTEL_RESOURCE_ATTRIBUTES: e.OTEL_RESOURCE_ATTRIBUTES || ''
            }
          },
          'elastic-docs': {
            type: 'http',
            url: 'https://www.elastic.co/docs/_mcp/'
          }
        },
        enabledMcpjsonServers: [],
        disabledMcpjsonServers: [],
        hasTrustDialogAccepted: true,
        projectOnboardingSeenCount: 0,
        hasClaudeMdExternalIncludesApproved: false,
        hasClaudeMdExternalIncludesWarningShown: false
      }
    }
  };
  fs.writeFileSync('/root/.claude.json', JSON.stringify(state, null, 2));
})().catch(err => { console.error(err); process.exit(1); });
"

mkdir -p /root/.local/share/code-server/User
cat > /root/.local/share/code-server/User/settings.json << 'EOF'
{
  "workbench.colorTheme": "Default Dark Modern",
  "workbench.startupEditor": "readme",
  "security.workspace.trust.enabled": false,
  "telemetry.telemetryLevel": "off",
  "extensions.autoCheckUpdates": false,
  "terminal.integrated.defaultProfile.linux": "bash",
  "claudeCode.disableLoginPrompt": true,
  "claudeCode.initialPermissionMode": "plan",
  "remote.autoForwardPorts": false,
  "remote.autoForwardPortsSource": "output",
  "chat.disableAIFeatures": true,
  "chat.commandCenter.enabled": false,
  "workbench.secondarySideBar.defaultVisibility": "hidden"
}
EOF

export KIBANA_API_KEY="${ELASTICSEARCH_API_KEY:-}"

# Quoted heredoc: the shell expands nothing, so the backticks and $VARS in the
# markdown below reach CLAUDE.md literally. (A double-quoted `node -e "..."` ran every
# unescaped backtick pair as a command substitution and emptied the $KIBANA_PROXY rules.)
node <<'JS'
(async () => {
  const fs = require('fs');
  const e = process.env;
  const md = [
    '# Elastic Stack — Connection Details',
    '',
    'The following environment variables are pre-set in every Bash tool call.',
    'Do **not** run env-discovery commands — just use them directly.',
    '',
    '| Variable | Value |',
    '|---|---|',
    '`KIBANA_URL` | ' + (e.KIBANA_URL || '(not set)') + ' |',
    '`KIBANA_PROXY` | ' + (e.KIBANA_PROXY || '(not set — fall back to KIBANA_URL)') + ' |',
    '`ELASTICSEARCH_URL` | ' + (e.ELASTICSEARCH_URL || '(not set)') + ' |',
    '`ELASTICSEARCH_API_KEY` | *(set — use directly in curl headers)* |',
    '`KIBANA_API_KEY` | same value as `ELASTICSEARCH_API_KEY` |',
    '`INGEST_URL` | ' + (e.INGEST_URL || '(not set)') + ' |',
    '',
    '## Kibana Links You Give the User',
    '',
    'Kibana sits behind an authenticated reverse proxy. The two hosts are not interchangeable:',
    '',
    '- **`$KIBANA_URL`** — the real Kibana endpoint. Use for **every** HTTP request, curl command,',
    '  and MCP call. **Never** show this URL to the user — it requires separate credentials.',
    '- **`$KIBANA_PROXY`** — the public, Okta-authenticated URL the user can actually open.',
    '  Use this for **every** Kibana link in your replies.',
    '',
    '**Rule:** any Kibana URL that appears in your reply to the user must start with `$KIBANA_PROXY`.',
    'Build links as `$KIBANA_PROXY` + the path — paths and query strings are identical on both',
    'hosts, only the origin differs. Examples:',
    '',
    '- Dashboard: `$KIBANA_PROXY/app/dashboards#/view/<id>`',
    '- Discover: `$KIBANA_PROXY/app/discover#/?_g=...`',
    '- Alerts: `$KIBANA_PROXY/app/observability/alerts`',
    '- APM service: `$KIBANA_PROXY/app/apm/services/<name>`',
    '',
    'This applies to URLs you did not build yourself too: if a tool result, MCP response, or API',
    'payload contains a `...kb....elastic-cloud.com` URL, swap its origin for `$KIBANA_PROXY`',
    'before including it in your reply. If `KIBANA_PROXY` is not set, fall back to `$KIBANA_URL`.',
    '',
    '## Project Skills',
    '',
    'Six project-level skills are available in `/app/.claude/skills/`:',
    '',
    '- **build-dashboard** — builds an ES|QL Kibana dashboard, validates every panel query,',
    '  and exports it immediately. Ask: *"Build a dashboard of log volume and error rate by service"*.',
    '- **add-scenario** — generates a new scenario under `/app/scenarios/<id>/` for a given customer vertical.',
    '  Ask: *"Add a healthcare scenario"* or *"Add a manufacturing scenario"*.',
    '- **add-fault-channel** — adds a new fault/channel type to an existing scenario.',
    '- **upgrade-scenario** — checks and upgrades scenario schema versions.',
    '  Ask: *"Is this scenario up to date?"* or *"Upgrade the retail scenario"*.',
    '- **add-telemetry-generator** — scaffolds a new generator under `/app/generators/<id>/`.',
    '- **upgrade-telemetry-generator** — validates and upgrades an existing generator.',
    '',
    'After adding a scenario, restart the app (or trigger the rediscover endpoint if available)',
    'so the new scenario appears in the Demo Management Console, then click Deploy.',
    '',
    '## Dashboards',
    '',
    'When asked to build a dashboard, use the dashbuilder MCP tools (`mcp__elastic-mcp-dashbuilder__*`).',
    'Validate every panel query with `run_esql` and require at least one non-null result row before',
    'creating the panel — drop any panel whose query fails after 3 repair attempts.',
    'Call `export_to_kibana` immediately once panels are built. **Do not ask whether to export.**',
    'The tool returns a `$KIBANA_URL`-based link — swap its origin for `$KIBANA_PROXY` before',
    'giving it to the user (per the Kibana Links rule above).',
    '',
    '## API Patterns',
    '',
    'These are internal API calls — always use `$KIBANA_URL` / `$ELASTICSEARCH_URL` here, never',
    'the proxy. The proxy is for user-facing links only (see section above).',
    '',
    '**Kibana REST API**:',
    '```bash',
    'curl -s \\',
    '  -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \\',
    '  -H "kbn-xsrf: true" \\',
    '  "$KIBANA_URL/api/status"',
    '```',
    '',
    '**Elasticsearch REST API**:',
    '```bash',
    'curl -s \\',
    '  -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \\',
    '  "$ELASTICSEARCH_URL/_cat/indices?v"',
    '```',
    '',
    '## Elastic CLI',
    '',
    'The [`elastic`](https://github.com/elastic/cli) CLI is installed and pre-configured',
    'against this demo\'s Elasticsearch and Kibana (context `demo` in `~/.elasticrc.yml`).',
    'No setup needed — try `elastic es info` or `elastic kb data-views list`.',
  ].join('\n');
  fs.writeFileSync('/app/CLAUDE.md', md + '\n');
})().catch(err => { console.error(err); process.exit(1); });
JS

# code-server SCM needs a git repo to mark edited files.
# On first boot /app was just seeded from the image snapshot, so initialise a
# git repo and commit the pristine workspace as the baseline. The .git directory
# lives on the PVC, so it persists across pod restarts — this block runs only once.
if [ ! -d /app/.git ]; then
  echo "Initializing /app as a git repo for code-server change tracking..."
  git config --global --add safe.directory /app
  git -C /app init -b main
  git -C /app config user.email "demo@localhost"
  git -C /app config user.name "Demo"
  git -C /app add -A
  git -C /app commit -m "Baseline" --quiet
fi

# Map staging env var names to what app/config.py reads.
# The staging environment uses ELASTICSEARCH_API_KEY / ELASTICSEARCH_URL / INGEST_URL;
# the Python app reads ELASTIC_API_KEY / ELASTIC_URL / OTLP_ENDPOINT.
export ELASTIC_API_KEY="${ELASTIC_API_KEY:-${ELASTICSEARCH_API_KEY:-}}"
export ELASTIC_URL="${ELASTIC_URL:-${ELASTICSEARCH_URL:-}}"
export OTLP_ENDPOINT="${OTLP_ENDPOINT:-${INGEST_URL:-}}"

# Start the app in the background; tini (PID 1) will reap it when code-server exits.
python -m uvicorn app.main:app --host 0.0.0.0 --port 8080 &

# `--auth none` is safe because the ingress already enforces Okta auth.
exec code-server \
  --auth none \
  --bind-addr 0.0.0.0:8443 \
  --disable-telemetry \
  --disable-update-check \
  /app
