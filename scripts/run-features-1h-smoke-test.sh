#!/usr/bin/env bash
set -euo pipefail

TF_DIR="${1:-infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"
SMOKE_DAYS="${SMOKE_DAYS:-30}"
WARMUP_HOURS="${WARMUP_HOURS:-96}"
SMOKE_TABLE="${SMOKE_TABLE:-features_1h_smoke}"

for path in "${TF_DIR}" "${SSH_KEY}" "jobs/spark/build_features_1h.py" "clickhouse/schema/features_1h_smoke.sql"; do
  if [[ ! -e "${path}" ]]; then
    echo "Required path not found: ${path}" >&2
    exit 1
  fi
done

for command_name in terraform jq ssh scp date; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command not found: ${command_name}" >&2
    exit 1
  fi
done

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"
PRIVATE_IPS="$(terraform -chdir="${TF_DIR}" output -json node_private_ips)"

spark_public="$(jq -er '."spark-master"' <<<"${PUBLIC_IPS}")"
spark_private="$(jq -er '."spark-master"' <<<"${PRIVATE_IPS}")"
analytics_public="$(jq -er '."analytics-node"' <<<"${PUBLIC_IPS}")"
analytics_private="$(jq -er '."analytics-node"' <<<"${PRIVATE_IPS}")"

remote_schema="/tmp/${SMOKE_TABLE}.sql"
remote_job_dir="/opt/bitcoin-realtime-forecasting-platform/jobs/spark"
remote_job_path="${remote_job_dir}/build_features_1h.py"

smoke_end="$(
  ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    "${SSH_USER}@${analytics_public}" \
    "docker exec clickhouse clickhouse-client --query \"SELECT formatDateTime(toStartOfHour(least((SELECT max(timestamp) FROM btc.raw_ohlcv), (SELECT max(time) FROM btc.raw_blocks), (SELECT max(tx_time) FROM btc.raw_transactions))) + INTERVAL 1 HOUR, '%F %T')\" | tr -d '\\r\\n'"
)"
smoke_start="$(
  python3 -c 'from datetime import datetime, timedelta, timezone; import sys; raw = datetime.fromisoformat(sys.argv[1].replace("Z", "+00:00")); end = raw if raw.tzinfo is not None else raw.replace(tzinfo=timezone.utc); end = end.astimezone(timezone.utc); start = end - timedelta(days=int(sys.argv[2])); print(start.strftime("%F %T"))' \
    "${smoke_end}" "${SMOKE_DAYS}"
)"

echo "[features-1h-smoke] Preparing smoke table ${SMOKE_TABLE}"
scp -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "clickhouse/schema/features_1h_smoke.sql" \
  "${SSH_USER}@${analytics_public}:${remote_schema}"
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${analytics_public}" \
  "docker exec -i clickhouse clickhouse-client --multiquery < '${remote_schema}'"
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${analytics_public}" \
  "docker exec clickhouse clickhouse-client --query \"TRUNCATE TABLE btc.${SMOKE_TABLE}\""

echo "[features-1h-smoke] Syncing job to Spark master"
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${spark_public}" \
  "mkdir -p '${remote_job_dir}'"
scp -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "jobs/spark/build_features_1h.py" \
  "${SSH_USER}@${spark_public}:${remote_job_path}"

echo "[features-1h-smoke] Running bounded Spark job for ${SMOKE_DAYS} days"
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${spark_public}" \
  "docker exec spark-master /opt/spark/bin/spark-submit \
    --master 'spark://${spark_private}:7077' \
    --deploy-mode client \
    --conf 'spark.driver.host=${spark_private}' \
    --conf 'spark.driver.bindAddress=0.0.0.0' \
    --conf 'spark.driver.userClassPathFirst=true' \
    --conf 'spark.executor.userClassPathFirst=true' \
    --conf 'spark.ui.enabled=false' \
    --jars /tmp/.ivy2/jars/com.clickhouse_jdbc-v2-0.9.8.jar,/tmp/.ivy2/jars/com.clickhouse_client-v2-0.9.8.jar,/tmp/.ivy2/jars/com.clickhouse_clickhouse-client-0.9.8.jar,/tmp/.ivy2/jars/com.clickhouse_clickhouse-data-0.9.8.jar,/tmp/.ivy2/jars/com.clickhouse_clickhouse-http-client-0.9.8.jar,/tmp/.ivy2/jars/org.apache.httpcomponents.client5_httpclient5-5.4.4.jar,/tmp/.ivy2/jars/org.apache.httpcomponents.core5_httpcore5-5.3.4.jar,/tmp/.ivy2/jars/org.apache.httpcomponents.core5_httpcore5-h2-5.3.4.jar,/tmp/.ivy2/jars/com.google.guava_guava-33.4.6-jre.jar,/tmp/.ivy2/jars/com.google.guava_failureaccess-1.0.3.jar,/tmp/.ivy2/jars/com.google.guava_listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar,/tmp/.ivy2/jars/com.google.j2objc_j2objc-annotations-3.0.0.jar,/tmp/.ivy2/jars/com.google.errorprone_error_prone_annotations-2.36.0.jar \
    /opt/spark/jobs/build_features_1h.py \
    --clickhouse-host '${analytics_private}' \
    --clickhouse-port 8123 \
    --clickhouse-database btc \
    --clickhouse-user default \
    --target-table '${SMOKE_TABLE}' \
    --start-time '${smoke_start}' \
    --end-time '${smoke_end}' \
    --warmup-hours '${WARMUP_HOURS}'"

echo "[features-1h-smoke] Smoke run complete"
