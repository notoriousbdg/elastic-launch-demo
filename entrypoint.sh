#!/bin/bash
set -euo pipefail

# On first boot the /app PVC is empty — seed it from the image snapshot.
# On subsequent boots the PVC already has content (including any Claude Code edits).
if [ ! -f /app/app/main.py ]; then
  echo "First boot: seeding /app from image snapshot..."
  cp -rp /app-seed/. /app/
fi

mkdir -p /root/.claude /root/.claude/projects

node -e "
(async () => {
  const fs = require('fs');
  const e = process.env;

  const settings = {
    model: 'claude-sonnet',
    theme: 'dark',
    permissions: {
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
      CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS: '1'
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
              NODE_OPTIONS: '--import /usr/local/lib/node_modules/@elastic/opentelemetry-node/import.mjs',
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
              NODE_OPTIONS: '--import /usr/local/lib/node_modules/@elastic/opentelemetry-node/import.mjs',
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
              NODE_OPTIONS: '--import /usr/local/lib/node_modules/@elastic/opentelemetry-node/import.mjs',
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
  "remote.autoForwardPorts": false,
  "remote.autoForwardPortsSource": "output"
}
EOF

export KIBANA_API_KEY="${ELASTICSEARCH_API_KEY:-}"

node -e "
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
    '\`KIBANA_URL\` | ' + (e.KIBANA_URL || '(not set)') + ' |',
    '\`ELASTICSEARCH_URL\` | ' + (e.ELASTICSEARCH_URL || '(not set)') + ' |',
    '\`ELASTICSEARCH_API_KEY\` | *(set — use directly in curl headers)* |',
    '\`KIBANA_API_KEY\` | same value as \`ELASTICSEARCH_API_KEY\` |',
    '\`INGEST_URL\` | ' + (e.INGEST_URL || '(not set)') + ' |',
    '',
    '## Project Skills',
    '',
    'Two project-level skills are available in \`/app/.claude/skills/\`:',
    '',
    '- **add-scenario** — generates a new scenario under \`/app/scenarios/<id>/\` for a given customer vertical.',
    '  Ask: *\"Add a healthcare scenario\"* or *\"Add a manufacturing scenario\"*.',
    '- **add-fault-channel** — adds a new fault/channel type to an existing scenario.',
    '',
    'After adding a scenario, restart the app (or trigger the rediscover endpoint if available)',
    'so the new scenario appears in the Demo Management Console, then click Deploy.',
    '',
    '## API Patterns',
    '',
    '**Kibana REST API**:',
    '\`\`\`bash',
    'curl -s \\\\',
    '  -H \"Authorization: ApiKey \$ELASTICSEARCH_API_KEY\" \\\\',
    '  -H \"kbn-xsrf: true\" \\\\',
    '  \"\$KIBANA_URL/api/status\"',
    '\`\`\`',
    '',
    '**Elasticsearch REST API**:',
    '\`\`\`bash',
    'curl -s \\\\',
    '  -H \"Authorization: ApiKey \$ELASTICSEARCH_API_KEY\" \\\\',
    '  \"\$ELASTICSEARCH_URL/_cat/indices?v\"',
    '\`\`\`',
  ].join('\\n');
  fs.writeFileSync('/app/CLAUDE.md', md + '\\n');
})().catch(err => { console.error(err); process.exit(1); });
"

# Map staging env var names to what app/config.py reads.
# The staging environment uses ELASTICSEARCH_API_KEY / ELASTICSEARCH_URL / INGEST_URL;
# the Python app reads ELASTIC_API_KEY / ELASTIC_URL / OTLP_ENDPOINT.
export ELASTIC_API_KEY="${ELASTIC_API_KEY:-${ELASTICSEARCH_API_KEY:-}}"
export ELASTIC_URL="${ELASTIC_URL:-${ELASTICSEARCH_URL:-}}"
export OTLP_ENDPOINT="${OTLP_ENDPOINT:-${INGEST_URL:-}}"

# If credentials are available but no scenario was explicitly pinned, default to
# the space scenario so the app auto-deploys on first boot.
if [ -n "${ELASTIC_API_KEY}" ] && [ -n "${KIBANA_URL}" ] && [ -z "${ACTIVE_SCENARIO:-}" ]; then
  export ACTIVE_SCENARIO="space"
fi

# Start the app in the background; tini (PID 1) will reap it when code-server exits.
python -m uvicorn app.main:app --host 0.0.0.0 --port 8080 &

# `--auth none` is safe because the ingress already enforces Okta auth.
exec code-server \
  --auth none \
  --bind-addr 0.0.0.0:8443 \
  --disable-telemetry \
  --disable-update-check \
  /app
