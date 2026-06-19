#!/usr/bin/env bash
set -euo pipefail

TF_DIR="${1:-infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"

if [[ ! -d "${TF_DIR}" ]]; then
  echo "Terraform directory not found: ${TF_DIR}" >&2
  echo "Usage: scripts/bootstrap-all-nodes.sh [infra/terraform]" >&2
  exit 1
fi

if [[ ! -f "${SSH_KEY}" ]]; then
  echo "SSH key not found: ${SSH_KEY}" >&2
  exit 1
fi

if ! command -v terraform >/dev/null 2>&1; then
  echo "terraform command not found" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq command not found. Install jq first." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE_SCRIPT="/tmp/bootstrap-node-runtime.sh"

NODE_PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"

echo "[bootstrap-all] Using Terraform dir: ${TF_DIR}"
echo "[bootstrap-all] Using SSH key: ${SSH_KEY}"

for node in $(echo "${NODE_PUBLIC_IPS}" | jq -r 'keys[]'); do
  ip="$(echo "${NODE_PUBLIC_IPS}" | jq -r --arg node "${node}" '.[$node]')"

  echo
  echo "[bootstrap-all] Bootstrapping ${node} (${ip})"

  scp \
    -i "${SSH_KEY}" \
    -o StrictHostKeyChecking=accept-new \
    "${SCRIPT_DIR}/bootstrap-node-runtime.sh" \
    "${SSH_USER}@${ip}:${REMOTE_SCRIPT}"

  ssh \
    -i "${SSH_KEY}" \
    -o StrictHostKeyChecking=accept-new \
    "${SSH_USER}@${ip}" \
    "chmod +x ${REMOTE_SCRIPT} && ${REMOTE_SCRIPT}"

  echo "[bootstrap-all] ${node} completed"
done

echo
echo "[bootstrap-all] All nodes bootstrapped successfully"
