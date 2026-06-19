#!/usr/bin/env bash
set -euo pipefail

TF_DIR="${1:-infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"

if [[ ! -d "${TF_DIR}" ]]; then
  echo "Terraform directory not found: ${TF_DIR}" >&2
  exit 1
fi

if [[ ! -f "${SSH_KEY}" ]]; then
  echo "SSH key not found: ${SSH_KEY}" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq command not found. Install jq first." >&2
  exit 1
fi

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"
PRIVATE_IPS="$(terraform -chdir="${TF_DIR}" output -json node_private_ips)"

kafka1_public="$(echo "${PUBLIC_IPS}" | jq -r '."kafka-1"')"
kafka1_private="$(echo "${PRIVATE_IPS}" | jq -r '."kafka-1"')"
kafka2_private="$(echo "${PRIVATE_IPS}" | jq -r '."kafka-2"')"
kafka3_private="$(echo "${PRIVATE_IPS}" | jq -r '."kafka-3"')"

bootstrap="${kafka1_private}:9092,${kafka2_private}:9092,${kafka3_private}:9092"

create_topic() {
  local topic="$1"
  local partitions="$2"
  local replication="$3"

  echo "[kafka-topics] Creating ${topic}"

  ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new "${SSH_USER}@${kafka1_public}" \
    "docker exec kafka ${KAFKA_BIN}/kafka-topics.sh \
      --bootstrap-server ${bootstrap} \
      --create \
      --if-not-exists \
      --topic ${topic} \
      --partitions ${partitions} \
      --replication-factor ${replication}"
}

create_topic "btc.market.raw" "6" "3"
create_topic "btc.onchain.raw" "6" "3"
create_topic "btc.features.realtime" "6" "3"
create_topic "btc.predictions" "6" "3"
create_topic "btc.deadletter" "3" "3"
create_topic "btc.pipeline.metrics" "3" "3"

echo
echo "[kafka-topics] Topic list:"
ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new "${SSH_USER}@${kafka1_public}" \
  "docker exec kafka ${KAFKA_BIN}/kafka-topics.sh --bootstrap-server ${bootstrap} --list"

echo
echo "[kafka-topics] Topic descriptions:"
ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new "${SSH_USER}@${kafka1_public}" \
  "docker exec kafka ${KAFKA_BIN}/kafka-topics.sh --bootstrap-server ${bootstrap} --describe"
