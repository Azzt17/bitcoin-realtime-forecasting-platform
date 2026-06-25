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

for command_name in terraform jq ssh scp; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command not found: ${command_name}" >&2
    exit 1
  fi
done

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"
PRIVATE_IPS="$(terraform -chdir="${TF_DIR}" output -json node_private_ips)"

master_public="$(jq -er '."spark-master"' <<<"${PUBLIC_IPS}")"
master_private="$(jq -er '."spark-master"' <<<"${PRIVATE_IPS}")"
analytics_private="$(jq -er '."analytics-node"' <<<"${PRIVATE_IPS}")"

remote_job_dir="/opt/bitcoin-realtime-forecasting-platform/jobs/spark"
remote_job_path="${remote_job_dir}/check_spark_clickhouse_read.py"
local_job_path="scripts/check-spark-clickhouse-read.py"

echo "[spark-clickhouse-read] Syncing checker to Spark master"
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${master_public}" \
  "mkdir -p '${remote_job_dir}'"

scp -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${local_job_path}" "${SSH_USER}@${master_public}:${remote_job_path}"

echo "[spark-clickhouse-read] Running Spark job"
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${master_public}" \
  "docker exec -e ANALYTICS_PRIVATE_IP='${analytics_private}' spark-master /opt/spark/bin/spark-submit \
    --master 'spark://${master_private}:7077' \
    --deploy-mode client \
    --conf 'spark.driver.host=${master_private}' \
    --conf 'spark.driver.bindAddress=0.0.0.0' \
    --conf 'spark.ui.enabled=false' \
    /opt/spark/jobs/check_spark_clickhouse_read.py"
