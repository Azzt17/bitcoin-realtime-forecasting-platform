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

for command_name in terraform jq ssh curl; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command not found: ${command_name}" >&2
    exit 1
  fi
done

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"
PRIVATE_IPS="$(terraform -chdir="${TF_DIR}" output -json node_private_ips)"

analytics_public="$(jq -er '."analytics-node"' <<<"${PUBLIC_IPS}")"
analytics_private="$(jq -er '."analytics-node"' <<<"${PRIVATE_IPS}")"
kafka1_public="$(jq -er '."kafka-1"' <<<"${PUBLIC_IPS}")"
kafka1_private="$(jq -er '."kafka-1"' <<<"${PRIVATE_IPS}")"
kafka2_private="$(jq -er '."kafka-2"' <<<"${PRIVATE_IPS}")"
kafka3_private="$(jq -er '."kafka-3"' <<<"${PRIVATE_IPS}")"
spark_public="$(jq -er '."spark-master"' <<<"${PUBLIC_IPS}")"
spark_private="$(jq -er '."spark-master"' <<<"${PRIVATE_IPS}")"

bootstrap="${kafka1_private}:9092,${kafka2_private}:9092,${kafka3_private}:9092"

echo "===== Kafka ====="
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${kafka1_public}" \
  "docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server '${bootstrap}' --list"

echo "===== Spark ====="
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${spark_public}" \
  "curl -fsS 'http://${spark_private}:8080/json/' >/dev/null && docker inspect -f '{{.State.Running}}' spark-master"

echo "===== ClickHouse ====="
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${analytics_public}" \
  "docker inspect -f '{{.State.Running}}' clickhouse && docker exec clickhouse clickhouse-client --query 'SELECT count() FROM btc.realtime_market_events FINAL'"

echo "===== Grafana ====="
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${analytics_public}" \
  "curl -fsS http://127.0.0.1:3000/api/health"

echo "[health-check] All infra checks passed"
