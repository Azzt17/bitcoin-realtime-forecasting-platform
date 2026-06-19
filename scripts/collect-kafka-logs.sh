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

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"

for node in kafka-1 kafka-2 kafka-3; do
  public_ip="$(echo "${PUBLIC_IPS}" | jq -r --arg node "${node}" '.[$node]')"

  echo
  echo "===== ${node} (${public_ip}) ====="

  ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new "${SSH_USER}@${public_ip}" \
    'echo "[docker ps]"; docker ps -a --filter name=kafka --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"; echo; echo "[docker inspect state]"; docker inspect kafka --format "{{json .State}}" 2>/dev/null || true; echo; echo "[last logs]"; docker logs --tail=160 kafka 2>&1 || true'
done
