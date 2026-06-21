#!/usr/bin/env bash
set -euo pipefail

TF_DIR="${1:-infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"

PROJECT_DIR="/opt/bitcoin-realtime-forecasting-platform"
SERVICE_DIR="${PROJECT_DIR}/services/grafana"
DATA_DIR="${PROJECT_DIR}/data/grafana"
REMOTE_COMPOSE="${SERVICE_DIR}/docker-compose.yml"
LOCAL_DASHBOARD="grafana/dashboards/mvp-overview.json"
LOCAL_DATASOURCE="grafana/provisioning/datasources/clickhouse.yaml"
LOCAL_DASHBOARDS="grafana/provisioning/dashboards/dashboards.yaml"

GRAFANA_IMAGE="${GRAFANA_IMAGE:-grafana/grafana:12.4.1}"
GRAFANA_PLUGINS="${GRAFANA_PLUGINS:-grafana-clickhouse-datasource}"
GRAFANA_ADMIN_USER="${GRAFANA_ADMIN_USER:-admin}"

if [[ -z "${GRAFANA_ADMIN_PASSWORD:-}" ]]; then
  echo "GRAFANA_ADMIN_PASSWORD must be set before deploying Grafana" >&2
  exit 1
fi

for path in "${TF_DIR}" "${SSH_KEY}" "${LOCAL_DASHBOARD}" "${LOCAL_DATASOURCE}" "${LOCAL_DASHBOARDS}"; do
  if [[ ! -e "${path}" ]]; then
    echo "Required path not found: ${path}" >&2
    exit 1
  fi
done

for command_name in terraform jq ssh scp; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command not found: ${command_name}" >&2
    exit 1
  fi
done

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"
PRIVATE_IPS="$(terraform -chdir="${TF_DIR}" output -json node_private_ips)"

analytics_public="$(jq -er '."analytics-node"' <<<"${PUBLIC_IPS}")"
analytics_private="$(jq -er '."analytics-node"' <<<"${PRIVATE_IPS}")"

compose_file="$(mktemp)"
trap 'rm -f "${compose_file}"' EXIT

cat >"${compose_file}" <<EOF
services:
  grafana:
    image: ${GRAFANA_IMAGE}
    container_name: grafana
    hostname: grafana
    restart: unless-stopped
    network_mode: host
    environment:
      GF_SECURITY_ADMIN_USER: ${GRAFANA_ADMIN_USER}
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD}
      GF_SERVER_HTTP_PORT: "3000"
      GF_SERVER_ROOT_URL: http://${analytics_public}:3000
      GF_PLUGINS_PREINSTALL: ${GRAFANA_PLUGINS}
    volumes:
      - ${DATA_DIR}:/var/lib/grafana
      - ${SERVICE_DIR}/provisioning:/etc/grafana/provisioning:ro
      - ${SERVICE_DIR}/dashboards:/var/lib/grafana/dashboards:ro
EOF

echo "[grafana-deploy] Uploading provisioning and dashboard files"
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${analytics_public}" \
  "mkdir -p '${SERVICE_DIR}/provisioning/datasources' '${SERVICE_DIR}/provisioning/dashboards' '${SERVICE_DIR}/dashboards' '${DATA_DIR}' && chown -R 472:472 '${DATA_DIR}'"
scp -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${LOCAL_DASHBOARD}" "${SSH_USER}@${analytics_public}:${SERVICE_DIR}/dashboards/mvp-overview.json"
scp -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${LOCAL_DATASOURCE}" "${SSH_USER}@${analytics_public}:${SERVICE_DIR}/provisioning/datasources/clickhouse.yaml"
scp -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${LOCAL_DASHBOARDS}" "${SSH_USER}@${analytics_public}:${SERVICE_DIR}/provisioning/dashboards/dashboards.yaml"
scp -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${compose_file}" "${SSH_USER}@${analytics_public}:${REMOTE_COMPOSE}"

echo "[grafana-deploy] Starting Grafana on analytics node ${analytics_private}"
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${analytics_public}" \
  "cd '${SERVICE_DIR}' && docker compose down --remove-orphans || true && docker rm -f grafana 2>/dev/null || true && docker compose pull && docker compose up -d --remove-orphans"

echo "[grafana-deploy] Waiting for Grafana health endpoint"
for attempt in $(seq 1 36); do
  if ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    "${SSH_USER}@${analytics_public}" \
    "curl -fsS http://127.0.0.1:3000/api/health >/dev/null"; then
    echo "[grafana-deploy] Grafana is healthy"
    echo "[grafana-deploy] URL: http://${analytics_public}:3000"
    exit 0
  fi
  sleep 5
done

ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${analytics_public}" \
  "docker logs --tail=100 grafana 2>&1" >&2
echo "Grafana did not become healthy in time" >&2
exit 1
