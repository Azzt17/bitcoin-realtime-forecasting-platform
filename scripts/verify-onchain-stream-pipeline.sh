#!/usr/bin/env bash
set -euo pipefail

TF_DIR="${1:-infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"

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
spark_public="$(jq -er '."spark-master"' <<<"${PUBLIC_IPS}")"
analytics_public="$(jq -er '."analytics-node"' <<<"${PUBLIC_IPS}")"

echo "===== Streaming container ====="
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${spark_public}" \
  'test "$(docker inspect -f "{{.State.Running}}" spark-onchain-stream)" = true && docker ps --filter name=spark-onchain-stream --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"'

echo "===== ClickHouse row count ====="
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${analytics_public}" \
  'docker exec clickhouse clickhouse-client --query "SELECT count() AS rows, max(ingested_at) AS latest_ingested_at FROM btc.realtime_onchain_events FORMAT JSON"'
