#!/usr/bin/env bash
set -euo pipefail

TF_DIR="${1:-infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"
SPARK_IMAGE="${SPARK_IMAGE:-spark:3.5.8-scala2.12-java17-python3-r-ubuntu}"
KAFKA_CONNECTOR="${KAFKA_CONNECTOR:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8}"

PROJECT_DIR="/opt/bitcoin-realtime-forecasting-platform"
SERVICE_DIR="${PROJECT_DIR}/services/spark-onchain-stream"
JOBS_DIR="${PROJECT_DIR}/jobs/spark"
CHECKPOINT_DIR="${PROJECT_DIR}/data/spark-checkpoints/onchain-to-clickhouse"
CACHE_DIR="${PROJECT_DIR}/cache/spark"
REMOTE_COMPOSE="${SERVICE_DIR}/docker-compose.yml"
REMOTE_JOB="${JOBS_DIR}/onchain_to_clickhouse.py"
REMOTE_SCHEMA="/tmp/realtime_onchain_events.sql"

LOCAL_JOB="spark_streaming/onchain_to_clickhouse.py"
LOCAL_SCHEMA="clickhouse/schema/realtime_onchain_events.sql"

for path in "${TF_DIR}" "${LOCAL_JOB}" "${LOCAL_SCHEMA}" "${SSH_KEY}"; do
  if [[ ! -e "${path}" ]]; then
    echo "Required path not found: ${path}" >&2
    exit 1
  fi
done

for command_name in terraform jq ssh scp; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command not found: ${command_name}" >&2
    exit 1
  fi
done

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"
PRIVATE_IPS="$(terraform -chdir="${TF_DIR}" output -json node_private_ips)"

spark_public="$(jq -er '."spark-master"' <<<"${PUBLIC_IPS}")"
analytics_public="$(jq -er '."analytics-node"' <<<"${PUBLIC_IPS}")"
spark_private="$(jq -er '."spark-master"' <<<"${PRIVATE_IPS}")"
analytics_private="$(jq -er '."analytics-node"' <<<"${PRIVATE_IPS}")"
kafka_1_private="$(jq -er '."kafka-1"' <<<"${PRIVATE_IPS}")"
kafka_2_private="$(jq -er '."kafka-2"' <<<"${PRIVATE_IPS}")"
kafka_3_private="$(jq -er '."kafka-3"' <<<"${PRIVATE_IPS}")"

for ip in \
  "${spark_public}" "${analytics_public}" "${spark_private}" \
  "${analytics_private}" "${kafka_1_private}" "${kafka_2_private}" \
  "${kafka_3_private}"; do
  if [[ ! "${ip}" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]]; then
    echo "Terraform returned an invalid IPv4 address" >&2
    exit 1
  fi
done

kafka_bootstrap="${kafka_1_private}:9092,${kafka_2_private}:9092,${kafka_3_private}:9092"
compose_file="$(mktemp)"
trap 'rm -f "${compose_file}"' EXIT

cat >"${compose_file}" <<EOF
services:
  spark-onchain-stream:
    image: ${SPARK_IMAGE}
    container_name: spark-onchain-stream
    hostname: spark-onchain-stream
    user: "185:185"
    restart: unless-stopped
    network_mode: host
    extra_hosts:
      - spark-onchain-stream:${spark_private}
    environment:
      HOME: /opt/spark/cache
      PYTHONUNBUFFERED: "1"
    command:
      - /opt/spark/bin/spark-submit
      - --master
      - spark://${spark_private}:7077
      - --deploy-mode
      - client
      - --packages
      - ${KAFKA_CONNECTOR}
      - --conf
      - spark.jars.ivy=/opt/spark/cache/ivy
      - --conf
      - spark.driver.host=${spark_private}
      - --conf
      - spark.driver.bindAddress=0.0.0.0
      - --conf
      - spark.cores.max=2
      - --conf
      - spark.executor.cores=2
      - --conf
      - spark.executor.memory=1g
      - /opt/spark/jobs/onchain_to_clickhouse.py
      - --kafka-bootstrap
      - ${kafka_bootstrap}
      - --topic
      - btc.onchain.raw
      - --clickhouse-url
      - http://${analytics_private}:8123/
      - --checkpoint
      - /opt/spark/checkpoints/onchain-to-clickhouse
    volumes:
      - ${JOBS_DIR}:/opt/spark/jobs:ro
      - ${CHECKPOINT_DIR}:/opt/spark/checkpoints/onchain-to-clickhouse
      - ${CACHE_DIR}:/opt/spark/cache
EOF

echo "[onchain-stream-deploy] Applying on-chain realtime table schema"
scp -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${LOCAL_SCHEMA}" "${SSH_USER}@${analytics_public}:${REMOTE_SCHEMA}"
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${analytics_public}" \
  "docker exec -i clickhouse clickhouse-client --multiquery < '${REMOTE_SCHEMA}'"

echo "[onchain-stream-deploy] Uploading streaming job and service definition"
ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${spark_public}" \
  "mkdir -p '${SERVICE_DIR}' '${JOBS_DIR}' '${CHECKPOINT_DIR}' '${CACHE_DIR}' && chown -R 185:185 '${CHECKPOINT_DIR}' '${CACHE_DIR}'"
scp -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${LOCAL_JOB}" "${SSH_USER}@${spark_public}:${REMOTE_JOB}"
scp -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${compose_file}" "${SSH_USER}@${spark_public}:${REMOTE_COMPOSE}"

ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${spark_public}" \
  "cd '${SERVICE_DIR}' && docker compose pull && docker compose up -d --remove-orphans"

echo "[onchain-stream-deploy] Waiting for streaming query startup"
for attempt in $(seq 1 36); do
  state="$(
    ssh -i "${SSH_KEY}" -o BatchMode=yes "${SSH_USER}@${spark_public}" \
      "docker inspect -f '{{.State.Status}}' spark-onchain-stream 2>/dev/null || true"
  )"
  if [[ "${state}" == "exited" || "${state}" == "dead" ]]; then
    ssh -i "${SSH_KEY}" -o BatchMode=yes "${SSH_USER}@${spark_public}" \
      "docker logs --tail=100 spark-onchain-stream 2>&1" >&2
    exit 1
  fi
  if [[ "${state}" == "running" ]]; then
    echo "[onchain-stream-deploy] Streaming container is running"
    exit 0
  fi
  sleep 5
done

ssh -i "${SSH_KEY}" -o BatchMode=yes "${SSH_USER}@${spark_public}" \
  "docker logs --tail=100 spark-onchain-stream 2>&1" >&2
echo "Streaming query did not become ready in time" >&2
exit 1
