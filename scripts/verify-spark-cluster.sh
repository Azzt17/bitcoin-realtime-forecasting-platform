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

for command_name in terraform jq ssh; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command not found: ${command_name}" >&2
    exit 1
  fi
done

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"
PRIVATE_IPS="$(terraform -chdir="${TF_DIR}" output -json node_private_ips)"

master_public="$(jq -er '."spark-master"' <<<"${PUBLIC_IPS}")"
worker_public="$(jq -er '."spark-worker-1"' <<<"${PUBLIC_IPS}")"
master_private="$(jq -er '."spark-master"' <<<"${PRIVATE_IPS}")"

echo "===== Spark master container ====="
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${master_public}" \
  'test "$(docker inspect -f "{{.State.Running}}" spark-master)" = true && docker ps --filter name=spark-master --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"'

echo "===== Spark worker container ====="
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${worker_public}" \
  'test "$(docker inspect -f "{{.State.Running}}" spark-worker)" = true && docker ps --filter name=spark-worker --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"'

echo "===== Worker-to-master connectivity ====="
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${worker_public}" \
  "timeout 5 bash -c '</dev/tcp/${master_private}/7077' && echo '${master_private}:7077 reachable'"

echo "===== Master registration state ====="
master_state="$({
  ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    "${SSH_USER}@${master_public}" \
    "curl -fsS 'http://${master_private}:8080/json/'"
})"

alive_workers="$(jq '[.workers[]? | select(.state == "ALIVE")] | length' <<<"${master_state}")"
if [[ "${alive_workers}" -lt 1 ]]; then
  echo "No ALIVE Spark workers registered" >&2
  exit 1
fi
echo "ALIVE workers: ${alive_workers}"

echo "===== Distributed Spark Pi job ====="
spark_pi_output="$({
  ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    "${SSH_USER}@${master_public}" \
    "docker exec spark-master /opt/spark/bin/spark-submit \
      --master 'spark://${master_private}:7077' \
      --deploy-mode client \
      --conf 'spark.driver.host=${master_private}' \
      --conf 'spark.driver.bindAddress=0.0.0.0' \
      --conf 'spark.ui.enabled=false' \
      --class org.apache.spark.examples.SparkPi \
      /opt/spark/examples/jars/spark-examples_2.12-3.5.8.jar 10"
})"

if ! grep -F "Pi is roughly" <<<"${spark_pi_output}"; then
  echo "Spark Pi did not report a result" >&2
  exit 1
fi

echo "[spark-verify] Spark cluster verification completed"
