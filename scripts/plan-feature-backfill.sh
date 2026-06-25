#!/usr/bin/env bash
set -euo pipefail

TF_DIR="${1:-infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"
WARMUP_HOURS="${WARMUP_HOURS:-96}"
THROUGHPUT_HOURS_PER_MINUTE="${THROUGHPUT_HOURS_PER_MINUTE:-1.0}"

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

monthly_counts_json="$(
  ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    "${SSH_USER}@${analytics_public}" \
    'docker exec clickhouse clickhouse-client --query "SELECT month_start, ohlcv_hours, block_hours, tx_hours FROM (
        SELECT month_start, maxIf(hours, source = '\''ohlcv'\'') AS ohlcv_hours, maxIf(hours, source = '\''blocks'\'') AS block_hours, maxIf(hours, source = '\''tx'\'') AS tx_hours
        FROM (
            SELECT '\''ohlcv'\'' AS source, toString(toStartOfMonth(feature_time)) AS month_start, count() AS hours FROM btc.view_ohlcv_1h GROUP BY month_start
            UNION ALL
            SELECT '\''blocks'\'' AS source, toString(toStartOfMonth(feature_time)) AS month_start, count() AS hours FROM btc.view_blocks_1h GROUP BY month_start
            UNION ALL
            SELECT '\''tx'\'' AS source, toString(toStartOfMonth(feature_time)) AS month_start, count() AS hours FROM btc.view_tx_1h GROUP BY month_start
        )
        GROUP BY month_start
    ) ORDER BY month_start FORMAT JSON"'
)"

start_time="$(printf '%s' "${monthly_counts_json}" | python3 -c 'import json, sys; from datetime import datetime; payload = json.load(sys.stdin); first = datetime.strptime(payload["data"][0]["month_start"], "%Y-%m-%d"); print(first.strftime("%F %T"))')"

end_time="$(printf '%s' "${monthly_counts_json}" | python3 -c 'import json, sys; from datetime import datetime; payload = json.load(sys.stdin); last = datetime.strptime(payload["data"][-1]["month_start"], "%Y-%m-%d"); year = last.year + (1 if last.month == 12 else 0); month = 1 if last.month == 12 else last.month + 1; print(last.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0).strftime("%F %T"))')"

printf '%s\n' "${monthly_counts_json}" | python3 scripts/feature_backfill_tools.py plan \
  --start-time "${start_time}" \
  --end-time "${end_time}" \
  --warmup-hours "${WARMUP_HOURS}" \
  --throughput-hours-per-minute "${THROUGHPUT_HOURS_PER_MINUTE}"
