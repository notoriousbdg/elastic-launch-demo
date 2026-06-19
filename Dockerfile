FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl tini git jq gnupg \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
       | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
    && chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
       > /etc/apt/sources.list.d/github-cli.list \
    && apt-get update && apt-get install -y --no-install-recommends gh \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://code-server.dev/install.sh | sh

RUN npm install -g @anthropic-ai/claude-code
RUN npm install -g mcp-remote
RUN npm install -g https://github.com/elastic/example-mcp-app-observability/releases/latest/download/example-mcp-app-observability.tgz
RUN npm install -g @elastic/opentelemetry-node

RUN code-server --install-extension anthropic.claude-code

# Pull elastic/agent-skills at build time; ADD busts the cache when main advances.
ADD https://api.github.com/repos/elastic/agent-skills/commits/main /tmp/agent-skills.commit
RUN git clone --depth 1 https://github.com/elastic/agent-skills.git /tmp/agent-skills \
    && mkdir -p /root/.claude/skills \
    && cp -r /tmp/agent-skills/skills/observability/. \
             /tmp/agent-skills/skills/kibana/. \
             /tmp/agent-skills/skills/elasticsearch/. \
             /tmp/agent-skills/skills/security/. \
             /root/.claude/skills/ \
    && rm -rf /tmp/agent-skills /tmp/agent-skills.commit

RUN set -e; \
    for skill_dir in /root/.claude/skills/*/; do \
      if [ -f "${skill_dir}package.json" ]; then \
        (cd "$skill_dir" && npm install --omit=dev --no-audit --no-fund); \
      fi; \
    done

ENV NODE_PATH=/usr/local/lib/node_modules

ADD https://api.github.com/repos/elastic/example-mcp-dashbuilder/commits/main /tmp/dashbuilder.commit
RUN git clone --depth 1 https://github.com/elastic/example-mcp-dashbuilder.git /opt/example-mcp-dashbuilder \
    && cd /opt/example-mcp-dashbuilder \
    && npm ci \
    && npm run build --workspace=shared \
    && npm run build --workspace=server \
    && npm prune --omit=dev \
    && rm /tmp/dashbuilder.commit

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# Move entrypoint outside /app so it survives the /app PVC mount on first boot.
# Also snapshot the full app tree to /app-seed for PVC initialization.
RUN cp /app/entrypoint.sh /entrypoint.sh \
    && chmod +x /entrypoint.sh \
    && cp -rp /app /app-seed

EXPOSE 8080 8443

ENTRYPOINT ["tini", "--", "/entrypoint.sh"]
