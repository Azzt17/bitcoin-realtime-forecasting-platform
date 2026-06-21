#!/usr/bin/env bash
set -euo pipefail

TF_DIR="${1:-infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"
GRAFANA_ADMIN_USER="${GRAFANA_ADMIN_USER:-admin}"

if [[ -z "${GRAFANA_ADMIN_PASSWORD:-}" ]]; then
  echo "GRAFANA_ADMIN_PASSWORD must be set to verify Grafana" >&2
  exit 1
fi

for path in "${TF_DIR}" "${SSH_KEY}"; do
  if [[ ! -e "${path}" ]]; then
    echo "Required path not found: ${path}" >&2
    exit 1
  fi
done

for command_name in terraform jq ssh; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command not found: ${command_name}" >&2
    exit 1
  fi
done

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"
PRIVATE_IPS="$(terraform -chdir="${TF_DIR}" output -json node_private_ips)"

analytics_public="$(jq -er '."analytics-node"' <<<"${PUBLIC_IPS}")"
analytics_private="$(jq -er '."analytics-node"' <<<"${PRIVATE_IPS}")"

echo "===== Grafana container ====="
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${analytics_public}" \
  'docker inspect -f "{{.State.Running}}" grafana && docker ps --filter name=grafana --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"'

echo "===== Grafana health ====="
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${analytics_public}" \
  'curl -fsS http://127.0.0.1:3000/api/health'

echo "===== Grafana dashboard provisioning ====="
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${analytics_public}" \
  "curl -fsS -u '${GRAFANA_ADMIN_USER}:${GRAFANA_ADMIN_PASSWORD}' http://127.0.0.1:3000/api/dashboards/uid/btc-mvp-overview >/dev/null && echo dashboard-present"

echo "[grafana-verify] Grafana runtime verification completed"
