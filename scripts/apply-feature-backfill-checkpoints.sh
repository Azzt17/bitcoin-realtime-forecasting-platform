#!/usr/bin/env bash
set -euo pipefail

TF_DIR="${1:-infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"
WARMUP_HOURS="${WARMUP_HOURS:-96}"
THROUGHPUT_HOURS_PER_MINUTE="${THROUGHPUT_HOURS_PER_MINUTE:-1.0}"

for path in "${TF_DIR}" "${SSH_KEY}" "clickhouse/schema/feature_backfill_batches.sql" "scripts/feature_backfill_tools.py"; do
  if [[ ! -e "${path}" ]]; then
    echo "Required path not found: ${path}" >&2
    exit 1
  fi
done

for command_name in terraform jq ssh scp python3; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command not found: ${command_name}" >&2
    exit 1
  fi
done

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"
analytics_public="$(jq -er '."analytics-node"' <<<"${PUBLIC_IPS}")"

remote_schema="/tmp/feature_backfill_batches.sql"

echo "[feature-backfill] Applying checkpoint schema on analytics-node"
scp -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "clickhouse/schema/feature_backfill_batches.sql" \
  "${SSH_USER}@${analytics_public}:${remote_schema}"
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${analytics_public}" \
  "docker exec -i clickhouse clickhouse-client --multiquery < '${remote_schema}'"
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${analytics_public}" \
  "docker exec clickhouse clickhouse-client --query 'TRUNCATE TABLE IF EXISTS btc.feature_backfill_batches'"

monthly_json="$(
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

echo "[feature-backfill] Seeding monthly checkpoints"
python3 - "${monthly_json}" "${WARMUP_HOURS}" "${THROUGHPUT_HOURS_PER_MINUTE}" <<'PY' \
  | ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
      "${SSH_USER}@${analytics_public}" \
      "docker exec -i clickhouse clickhouse-client --query \"INSERT INTO btc.feature_backfill_batches (batch_id, feature_grain, batch_start, batch_end, warmup_start, target_table, source_hours, processed_hours, estimated_minutes, status, attempt_count, batch_owner, last_error, started_at, finished_at) FORMAT TabSeparated\""
import json
import sys
from datetime import datetime, timedelta, timezone

monthly_payload = json.loads(sys.argv[1])
warmup_hours = int(sys.argv[2])
throughput = float(sys.argv[3])

def next_month(value: datetime) -> datetime:
    value = value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    year = value.year + (1 if value.month == 12 else 0)
    month = 1 if value.month == 12 else value.month + 1
    return value.replace(year=year, month=month)

for row in monthly_payload["data"]:
    month_start = datetime.fromisoformat(str(row["month_start"]).replace("Z", "+00:00"))
    ohlcv_hours = int(row.get("ohlcv_hours", 0) or 0)
    source_hours = ohlcv_hours
    batch_end = next_month(month_start)
    estimated_minutes = source_hours / throughput if throughput > 0 else 0.0
    values = [
        month_start.strftime("%Y-%m"),
        "1h",
        month_start.strftime("%F %T"),
        batch_end.strftime("%F %T"),
        (month_start - timedelta(hours=warmup_hours)).strftime("%F %T"),
        "btc.features_1h",
        str(source_hours),
        "0",
        f"{estimated_minutes:.6f}",
        "pending",
        "0",
        "",
        "",
        "\\N",
        "\\N",
    ]
    sys.stdout.write("\t".join(values) + "\n")
PY

echo "[feature-backfill] Checkpoint seed complete"
