#!/usr/bin/env bash
set -euo pipefail

TF_DIR="${1:-infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"

for path in "${TF_DIR}" "${SSH_KEY}" "scripts/feature_backfill_tools.py"; do
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
analytics_public="$(jq -er '."analytics-node"' <<<"${PUBLIC_IPS}")"

checkpoint_json="$(
  ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    "${SSH_USER}@${analytics_public}" \
    'docker exec clickhouse clickhouse-client --query "SELECT batch_id, feature_grain, batch_start, batch_end, warmup_start, target_table, source_hours, processed_hours, estimated_minutes, status, attempt_count, batch_owner, last_error, started_at, finished_at, updated_at FROM btc.feature_backfill_batches FINAL ORDER BY batch_start FORMAT JSON"'
)"

printf '%s\n' "${checkpoint_json}" | python3 scripts/feature_backfill_tools.py report
