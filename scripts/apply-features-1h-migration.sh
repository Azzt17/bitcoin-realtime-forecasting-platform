#!/usr/bin/env bash
set -euo pipefail

TF_DIR="${1:-infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"

for path in "${TF_DIR}" "${SSH_KEY}" "clickhouse/schema/features_1h_migration.sql"; do
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
analytics_public="$(jq -er '."analytics-node"' <<<"${PUBLIC_IPS}")"

remote_schema="/tmp/features_1h_migration.sql"

echo "[features-1h-migration] Applying live schema migration on analytics-node"
scp -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "clickhouse/schema/features_1h_migration.sql" \
  "${SSH_USER}@${analytics_public}:${remote_schema}"
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${analytics_public}" \
  "docker exec -i clickhouse clickhouse-client --multiquery < '${remote_schema}'"

echo "[features-1h-migration] Migration complete"
