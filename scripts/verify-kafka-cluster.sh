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

if ! command -v jq >/dev/null 2>&1; then
  echo "jq command not found. Install jq first." >&2
  exit 1
fi

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"
PRIVATE_IPS="$(terraform -chdir="${TF_DIR}" output -json node_private_ips)"

kafka1_public="$(echo "${PUBLIC_IPS}" | jq -r '."kafka-1"')"
kafka2_public="$(echo "${PUBLIC_IPS}" | jq -r '."kafka-2"')"
kafka3_public="$(echo "${PUBLIC_IPS}" | jq -r '."kafka-3"')"

kafka1_private="$(echo "${PRIVATE_IPS}" | jq -r '."kafka-1"')"
kafka2_private="$(echo "${PRIVATE_IPS}" | jq -r '."kafka-2"')"
kafka3_private="$(echo "${PRIVATE_IPS}" | jq -r '."kafka-3"')"

bootstrap="${kafka1_private}:9092,${kafka2_private}:9092,${kafka3_private}:9092"

verify_node() {
  local node_name="$1"
  local public_ip="$2"

  echo
  echo "===== ${node_name} ====="

  ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new "${SSH_USER}@${public_ip}" \
    "docker ps --filter name=kafka --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'; echo; docker logs --tail=30 kafka"
}

verify_node "kafka-1" "${kafka1_public}"
verify_node "kafka-2" "${kafka2_public}"
verify_node "kafka-3" "${kafka3_public}"

echo
echo "===== Kafka broker API versions ====="
ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new "${SSH_USER}@${kafka1_public}" \
  "docker exec kafka kafka-broker-api-versions.sh --bootstrap-server ${bootstrap} | head -n 30"

echo
echo "===== Kafka topics ====="
ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new "${SSH_USER}@${kafka1_public}" \
  "docker exec kafka kafka-topics.sh --bootstrap-server ${bootstrap} --list"

echo
echo "===== Produce/consume smoke test ====="
test_topic="btc.deadletter"
test_message="{\"test\":\"kafka-smoke\",\"created_at\":\"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\"}"

ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new "${SSH_USER}@${kafka1_public}" \
  "printf '%s\n' '${test_message}' | docker exec -i kafka kafka-console-producer.sh --bootstrap-server ${bootstrap} --topic ${test_topic}"

ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new "${SSH_USER}@${kafka1_public}" \
  "timeout 10 docker exec kafka kafka-console-consumer.sh --bootstrap-server ${bootstrap} --topic ${test_topic} --from-beginning --max-messages 1"

echo
echo "[kafka-verify] Kafka smoke test completed"
