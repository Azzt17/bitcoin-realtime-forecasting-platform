#!/usr/bin/env bash
set -euo pipefail

TF_DIR="${1:-infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"

if [[ ! -d "${TF_DIR}" ]]; then
  echo "Terraform directory not found: ${TF_DIR}" >&2
  exit 1
fi

if [[ ! -f "${SSH_KEY}" ]]; then
  echo "SSH key not found: ${SSH_KEY}" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq command not found. Install jq first." >&2
  exit 1
fi

NODE_PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"

for node in $(echo "${NODE_PUBLIC_IPS}" | jq -r 'keys[]'); do
  ip="$(echo "${NODE_PUBLIC_IPS}" | jq -r --arg node "${node}" '.[$node]')"

  echo
  echo "===== ${node} (${ip}) ====="

  ssh \
    -i "${SSH_KEY}" \
    -o ConnectTimeout=10 \
    -o StrictHostKeyChecking=accept-new \
    "${SSH_USER}@${ip}" \
    'cat /etc/bitcoin-platform-node 2>/dev/null || true; echo; cat /opt/bitcoin-realtime-forecasting-platform/runtime-versions.txt; echo; docker ps --format "table {{.Names}}\t{{.Status}}"'
done
