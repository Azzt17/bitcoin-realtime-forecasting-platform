#!/usr/bin/env bash
set -euo pipefail

TF_DIR="${1:-infra/terraform}"
INGESTION_USER="${2:-ingestion}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"
IMPORT_BASE="/opt/bitcoin-realtime-forecasting-platform/data/imports"

if [[ ! -d "${TF_DIR}" ]]; then
  echo "Terraform directory not found: ${TF_DIR}" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq command not found. Install jq first." >&2
  exit 1
fi

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"
ANALYTICS_PUBLIC_IP="$(echo "${PUBLIC_IPS}" | jq -r '."analytics-node"')"

ssh \
  -i "${SSH_KEY}" \
  -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${ANALYTICS_PUBLIC_IP}" \
  "echo '[user]'; id ${INGESTION_USER}; echo; echo '[ssh dir]'; ls -ld /home/${INGESTION_USER}/.ssh; ls -l /home/${INGESTION_USER}/.ssh/authorized_keys; echo; echo '[imports]'; ls -lah ${IMPORT_BASE}; echo; echo '[sudo group check]'; id -nG ${INGESTION_USER}"
