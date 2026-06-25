#!/usr/bin/env bash
set -euo pipefail

TF_DIR="${1:-infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"
MAX_BATCHES="${MAX_BATCHES:-1}"
WARMUP_HOURS="${WARMUP_HOURS:-96}"
STALE_RUNNING_MINUTES="${STALE_RUNNING_MINUTES:-15}"
TARGET_TABLE="${TARGET_TABLE:-features_1h}"

for path in "${TF_DIR}" "${SSH_KEY}" "jobs/spark/build_features_1h.py" "scripts/report-feature-backfill-progress.sh" "scripts/apply-features-1h-migration.sh"; do
  if [[ ! -e "${path}" ]]; then
    echo "Required path not found: ${path}" >&2
    exit 1
  fi
done

for command_name in terraform jq ssh scp python3 date; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command not found: ${command_name}" >&2
    exit 1
  fi
done

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"
PRIVATE_IPS="$(terraform -chdir="${TF_DIR}" output -json node_private_ips)"

analytics_public="$(jq -er '."analytics-node"' <<<"${PUBLIC_IPS}")"
analytics_private="$(jq -er '."analytics-node"' <<<"${PRIVATE_IPS}")"
spark_public="$(jq -er '."spark-master"' <<<"${PUBLIC_IPS}")"
spark_private="$(jq -er '."spark-master"' <<<"${PRIVATE_IPS}")"

remote_job_dir="/opt/bitcoin-realtime-forecasting-platform/jobs/spark"
remote_job_path="${remote_job_dir}/build_features_1h.py"
remote_spark_job="/opt/spark/jobs/build_features_1h.py"

spark_jars="/tmp/.ivy2/jars/com.clickhouse_jdbc-v2-0.9.8.jar,/tmp/.ivy2/jars/com.clickhouse_client-v2-0.9.8.jar,/tmp/.ivy2/jars/com.clickhouse_clickhouse-client-0.9.8.jar,/tmp/.ivy2/jars/com.clickhouse_clickhouse-data-0.9.8.jar,/tmp/.ivy2/jars/com.clickhouse_clickhouse-http-client-0.9.8.jar,/tmp/.ivy2/jars/org.apache.httpcomponents.client5_httpclient5-5.4.4.jar,/tmp/.ivy2/jars/org.apache.httpcomponents.core5_httpcore5-5.3.4.jar,/tmp/.ivy2/jars/org.apache.httpcomponents.core5_httpcore5-h2-5.3.4.jar,/tmp/.ivy2/jars/com.google.guava_guava-33.4.6-jre.jar,/tmp/.ivy2/jars/com.google.guava_failureaccess-1.0.3.jar,/tmp/.ivy2/jars/com.google.guava_listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar,/tmp/.ivy2/jars/com.google.j2objc_j2objc-annotations-3.0.0.jar,/tmp/.ivy2/jars/com.google.errorprone_error_prone_annotations-2.36.0.jar"

run_clickhouse_query() {
  local host="$1"
  local query="$2"
  ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    "${SSH_USER}@${host}" \
    "docker exec clickhouse clickhouse-client --multiquery --query $(printf '%q' "${query}")"
}

run_clickhouse_json() {
  local host="$1"
  local query="$2"
  ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    "${SSH_USER}@${host}" \
    "docker exec clickhouse clickhouse-client --query $(printf '%q' "${query}")"
}

insert_checkpoint_row() {
  local batch_id="$1"
  local batch_start="$2"
  local batch_end="$3"
  local warmup_start="$4"
  local source_hours="$5"
  local processed_hours="$6"
  local estimated_minutes="$7"
  local status="$8"
  local attempt_count="$9"
  local batch_owner="${10}"
  local last_error="${11}"
  local started_at="${12}"
  local finished_at="${13}"

  {
    printf '%s\t' "${batch_id}"
    printf '1h\t'
    printf '%s\t' "${batch_start}"
    printf '%s\t' "${batch_end}"
    printf '%s\t' "${warmup_start}"
    printf 'btc.features_1h\t'
    printf '%s\t' "${source_hours}"
    printf '%s\t' "${processed_hours}"
    printf '%s\t' "${estimated_minutes}"
    printf '%s\t' "${status}"
    printf '%s\t' "${attempt_count}"
    printf '%s\t' "${batch_owner}"
    printf '%s\t' "${last_error}"
    if [[ -n "${started_at}" ]]; then
      printf '%s\t' "${started_at}"
    else
      printf '\\N\t'
    fi
    if [[ -n "${finished_at}" ]]; then
      printf '%s\n' "${finished_at}"
    else
      printf '\\N\n'
    fi
  } | ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    "${SSH_USER}@${analytics_public}" \
    "docker exec -i clickhouse clickhouse-client --query $(printf '%q' "INSERT INTO btc.feature_backfill_batches (batch_id, feature_grain, batch_start, batch_end, warmup_start, target_table, source_hours, processed_hours, estimated_minutes, status, attempt_count, batch_owner, last_error, started_at, finished_at) FORMAT TabSeparated")"
}

select_next_batch() {
  local checkpoint_json
  checkpoint_json="$(
    run_clickhouse_json "${analytics_public}" \
      'SELECT batch_id, batch_start, batch_end, warmup_start, source_hours, processed_hours, estimated_minutes, status, attempt_count, batch_owner, last_error, updated_at FROM btc.feature_backfill_batches FINAL ORDER BY batch_start FORMAT JSON'
  )"

  CHECKPOINT_JSON="${checkpoint_json}" python3 - "${STALE_RUNNING_MINUTES}" <<'PY'
import json
import os
import sys
from datetime import datetime, timedelta, timezone

stale_minutes = int(sys.argv[1])
payload = json.loads(os.environ["CHECKPOINT_JSON"])
rows = payload.get("data", [])
now = datetime.now(timezone.utc)

def parse_ts(raw: str) -> datetime:
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

for row in rows:
    status = str(row.get("status", ""))
    updated_at_raw = str(row.get("updated_at", ""))
    if status in {"pending", "failed"}:
        print("\t".join([
            str(row.get("batch_id", "")),
            str(row.get("batch_start", "")),
            str(row.get("batch_end", "")),
            str(row.get("warmup_start", "")),
            str(row.get("source_hours", "")),
            str(row.get("processed_hours", "")),
            str(row.get("estimated_minutes", "")),
            status,
            str(row.get("attempt_count", "")),
            str(row.get("batch_owner", "")),
            str(row.get("last_error", "")),
        ]))
        raise SystemExit(0)
    if status == "running":
        try:
            updated_at = parse_ts(updated_at_raw)
        except Exception:
            updated_at = now - timedelta(minutes=stale_minutes + 1)
        if now - updated_at >= timedelta(minutes=stale_minutes):
            print("\t".join([
                str(row.get("batch_id", "")),
                str(row.get("batch_start", "")),
                str(row.get("batch_end", "")),
                str(row.get("warmup_start", "")),
                str(row.get("source_hours", "")),
                str(row.get("processed_hours", "")),
                str(row.get("estimated_minutes", "")),
                "running",
                str(row.get("attempt_count", "")),
                str(row.get("batch_owner", "")),
                str(row.get("last_error", "")),
            ]))
            raise SystemExit(0)
        print(f"[feature-backfill] First unfinished batch {row.get('batch_id', '')} is still running; nothing to do.")
        raise SystemExit(2)

print("[feature-backfill] No pending or recoverable running batch found.")
raise SystemExit(1)
PY
}

sync_spark_job() {
  echo "[feature-backfill] Syncing Spark job to spark-master"
  ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    "${SSH_USER}@${spark_public}" \
    "mkdir -p '${remote_job_dir}'"
  scp -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    "jobs/spark/build_features_1h.py" \
    "${SSH_USER}@${spark_public}:${remote_job_path}"
}

run_spark_batch() {
  local batch_start="$1"
  local batch_end="$2"
  local remote_cmd
  remote_cmd=$(cat <<EOF
docker exec spark-master /opt/spark/bin/spark-submit \
  --master 'spark://${spark_private}:7077' \
  --deploy-mode client \
  --conf 'spark.driver.host=${spark_private}' \
  --conf 'spark.driver.bindAddress=0.0.0.0' \
  --conf 'spark.driver.userClassPathFirst=true' \
  --conf 'spark.executor.userClassPathFirst=true' \
  --conf 'spark.ui.enabled=false' \
  --jars ${spark_jars} \
  ${remote_spark_job} \
  --clickhouse-host '${analytics_private}' \
  --clickhouse-port 8123 \
  --clickhouse-database btc \
  --clickhouse-user default \
  --target-table '${TARGET_TABLE}' \
  --start-time '${batch_start}' \
  --end-time '${batch_end}' \
  --warmup-hours '${WARMUP_HOURS}'
EOF
)
  ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    "${SSH_USER}@${spark_public}" \
    "${remote_cmd}"
}

cleanup_batch_window() {
  local batch_start="$1"
  local batch_end="$2"
  run_clickhouse_query "${analytics_public}" \
    "SET mutations_sync = 1; ALTER TABLE btc.features_1h DELETE WHERE feature_time >= toDateTime64('${batch_start}', 3, 'UTC') AND feature_time < toDateTime64('${batch_end}', 3, 'UTC')"
}

verify_batch_rows() {
  local batch_start="$1"
  local batch_end="$2"
  run_clickhouse_json "${analytics_public}" \
    "SELECT count() AS row_count FROM btc.features_1h WHERE feature_time >= toDateTime64('${batch_start}', 3, 'UTC') AND feature_time < toDateTime64('${batch_end}', 3, 'UTC') FORMAT JSON"
}

mark_batch_state() {
  local batch_id="$1"
  local batch_start="$2"
  local batch_end="$3"
  local warmup_start="$4"
  local source_hours="$5"
  local processed_hours="$6"
  local estimated_minutes="$7"
  local status="$8"
  local attempt_count="$9"
  local batch_owner="${10}"
  local last_error="${11}"
  local started_at="${12}"
  local finished_at="${13}"
  insert_checkpoint_row \
    "${batch_id}" \
    "${batch_start}" \
    "${batch_end}" \
    "${warmup_start}" \
    "${source_hours}" \
    "${processed_hours}" \
    "${estimated_minutes}" \
    "${status}" \
    "${attempt_count}" \
    "${batch_owner}" \
    "${last_error}" \
    "${started_at}" \
    "${finished_at}"
}

echo "[feature-backfill] Initial checkpoint state"
bash scripts/report-feature-backfill-progress.sh

echo "[feature-backfill] Ensuring live 1h feature schema is migrated"
bash scripts/apply-features-1h-migration.sh "${TF_DIR}"

sync_spark_job

processed_batches=0
while (( processed_batches < MAX_BATCHES )); do
  set +e
  next_batch_tsv="$(select_next_batch)"
  select_exit_code=$?
  set -e
  if [[ ${select_exit_code} -eq 2 ]]; then
    exit 0
  fi
  if [[ ${select_exit_code} -ne 0 ]]; then
    exit 0
  fi

  IFS=$'\t' read -r batch_id batch_start batch_end warmup_start source_hours processed_hours estimated_minutes current_status attempt_count batch_owner last_error <<<"${next_batch_tsv}"
  attempt_count=$((attempt_count + 1))
  run_owner="$(hostname -s)"
  started_at="$(date -u +'%F %T')"

  echo "[feature-backfill] Starting batch ${batch_id} (${batch_start} -> ${batch_end})"
  mark_batch_state \
    "${batch_id}" \
    "${batch_start}" \
    "${batch_end}" \
    "${warmup_start}" \
    "${source_hours}" \
    "0" \
    "${estimated_minutes}" \
    "running" \
    "${attempt_count}" \
    "${run_owner}" \
    "" \
    "${started_at}" \
    ""

  echo "[feature-backfill] Cleaning destination rows for ${batch_id}"
  cleanup_batch_window "${batch_start}" "${batch_end}"

  echo "[feature-backfill] Running Spark batch ${batch_id}"
  if ! run_spark_batch "${batch_start}" "${batch_end}"; then
    failed_at="$(date -u +'%F %T')"
    mark_batch_state \
      "${batch_id}" \
      "${batch_start}" \
      "${batch_end}" \
      "${warmup_start}" \
      "${source_hours}" \
      "0" \
      "${estimated_minutes}" \
      "failed" \
      "${attempt_count}" \
      "${run_owner}" \
      "spark batch failed" \
      "${started_at}" \
      "${failed_at}"
    echo "[feature-backfill] Spark batch ${batch_id} failed"
    bash scripts/report-feature-backfill-progress.sh
    exit 1
  fi

  verify_json="$(verify_batch_rows "${batch_start}" "${batch_end}")"
  row_count="$(printf '%s' "${verify_json}" | python3 -c 'import json, sys; payload = json.load(sys.stdin); print(payload["data"][0]["row_count"])')"
  if [[ "${row_count}" != "${source_hours}" ]]; then
    failed_at="$(date -u +'%F %T')"
    mark_batch_state \
      "${batch_id}" \
      "${batch_start}" \
      "${batch_end}" \
      "${warmup_start}" \
      "${source_hours}" \
      "0" \
      "${estimated_minutes}" \
      "failed" \
      "${attempt_count}" \
      "${run_owner}" \
      "row count mismatch: expected ${source_hours}, got ${row_count}" \
      "${started_at}" \
      "${failed_at}"
    echo "[feature-backfill] Verification failed for ${batch_id}: expected ${source_hours}, got ${row_count}" >&2
    bash scripts/report-feature-backfill-progress.sh
    exit 1
  fi

  finished_at="$(date -u +'%F %T')"
  mark_batch_state \
    "${batch_id}" \
    "${batch_start}" \
    "${batch_end}" \
    "${warmup_start}" \
    "${source_hours}" \
    "${source_hours}" \
    "${estimated_minutes}" \
    "succeeded" \
    "${attempt_count}" \
    "${run_owner}" \
    "" \
    "${started_at}" \
    "${finished_at}"

  processed_batches=$((processed_batches + 1))
  echo "[feature-backfill] Completed batch ${batch_id} (${processed_batches}/${MAX_BATCHES})"
  bash scripts/report-feature-backfill-progress.sh
done
