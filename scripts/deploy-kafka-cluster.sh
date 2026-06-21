#!/usr/bin/env bash
set -euo pipefail

TF_DIR="${1:-infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"

PROJECT_DIR="/opt/bitcoin-realtime-forecasting-platform"
SERVICE_DIR="${PROJECT_DIR}/services/kafka"
KAFKA_DATA_BASE="${PROJECT_DIR}/data/kafka"
REMOTE_COMPOSE="${SERVICE_DIR}/docker-compose.yml"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.7.0}"
KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms512m -Xmx1g}"

if [[ ! -d "${TF_DIR}" ]]; then
  echo "Terraform directory not found: ${TF_DIR}" >&2
  echo "Usage: scripts/deploy-kafka-cluster.sh [infra/terraform]" >&2
  exit 1
fi

if [[ ! -f "${SSH_KEY}" ]]; then
  echo "SSH key not found: ${SSH_KEY}" >&2
  exit 1
fi

if ! command -v terraform >/dev/null 2>&1; then
  echo "terraform command not found" >&2
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

controller_quorum="1@${kafka1_private}:9093,2@${kafka2_private}:9093,3@${kafka3_private}:9093"

# Kafka KRaft cluster id must be identical across all brokers.
# Apache Kafka Docker image uses CLUSTER_ID during storage formatting.
cluster_id="5L6g3nShT-eMCtK--X86sw"

deploy_node() {
  local node_name="$1"
  local node_id="$2"
  local public_ip="$3"
  local private_ip="$4"
  local node_data_dir="${KAFKA_DATA_BASE}/${node_name}"

  echo
  echo "[kafka-deploy] Deploying ${node_name}"
  echo "[kafka-deploy] public=${public_ip} private=${private_ip} node_id=${node_id}"

  local tmp_compose
  tmp_compose="$(mktemp)"

  cat > "${tmp_compose}" <<EOF
services:
  kafka:
    image: ${KAFKA_IMAGE}
    container_name: kafka
    hostname: ${node_name}
    restart: unless-stopped
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      CLUSTER_ID: "${cluster_id}"
      KAFKA_NODE_ID: "${node_id}"
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "${controller_quorum}"
      KAFKA_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://${private_ip}:9092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "3"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "3"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "2"
      KAFKA_MIN_INSYNC_REPLICAS: "2"
      KAFKA_DEFAULT_REPLICATION_FACTOR: "3"
      KAFKA_NUM_PARTITIONS: "6"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0"
      KAFKA_LOG_DIRS: "/tmp/kraft-combined-logs"
      KAFKA_HEAP_OPTS: "${KAFKA_HEAP_OPTS}"
    volumes:
      - ${node_data_dir}:/tmp/kraft-combined-logs
    networks:
      - kafka_net

networks:
  kafka_net:
    driver: bridge
EOF

  ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new "${SSH_USER}@${public_ip}" \
    "mkdir -p ${SERVICE_DIR}"

  scp -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new "${tmp_compose}" \
    "${SSH_USER}@${public_ip}:${REMOTE_COMPOSE}"

  rm -f "${tmp_compose}"

  # First deployment environment has no production Kafka data yet.
  # Clean failed KRaft attempts and prepare a host bind mount writable by appuser UID 1000.
  ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new "${SSH_USER}@${public_ip}" \
    "cd ${SERVICE_DIR} && \
     docker compose down -v --remove-orphans || true && \
     docker rm -f kafka 2>/dev/null || true && \
     rm -rf ${node_data_dir} && \
     mkdir -p ${node_data_dir} && \
     chown -R 1000:1000 ${node_data_dir} && \
     chmod -R 775 ${node_data_dir} && \
     docker compose pull && \
     docker compose up -d"

  echo "[kafka-deploy] ${node_name} deployed"
}

deploy_node "kafka-1" "1" "${kafka1_public}" "${kafka1_private}"
deploy_node "kafka-2" "2" "${kafka2_public}" "${kafka2_private}"
deploy_node "kafka-3" "3" "${kafka3_public}" "${kafka3_private}"

echo
echo "[kafka-deploy] Waiting for Kafka containers to initialize..."
sleep 45

echo
echo "[kafka-deploy] Deployment complete"
echo "[kafka-deploy] Bootstrap servers:"
echo "${kafka1_private}:9092,${kafka2_private}:9092,${kafka3_private}:9092"
