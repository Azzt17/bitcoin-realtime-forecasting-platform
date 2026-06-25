#!/usr/bin/env bash
set -euo pipefail

TF_DIR="${1:-infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"
SPARK_IMAGE="${SPARK_IMAGE:-spark:3.5.8-scala2.12-java17-python3-r-ubuntu}"
SPARK_WORKER_CORES="${SPARK_WORKER_CORES:-4}"
SPARK_WORKER_MEMORY="${SPARK_WORKER_MEMORY:-6g}"

PROJECT_DIR="/opt/bitcoin-realtime-forecasting-platform"
SERVICE_DIR="${PROJECT_DIR}/services/spark"
JOBS_DIR="${PROJECT_DIR}/jobs/spark"
REMOTE_COMPOSE="${SERVICE_DIR}/docker-compose.yml"

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

if [[ ! "${SPARK_IMAGE}" =~ ^[a-zA-Z0-9._/@:-]+$ ]]; then
  echo "SPARK_IMAGE contains unsupported characters" >&2
  exit 1
fi

if [[ ! "${SPARK_WORKER_CORES}" =~ ^[1-9][0-9]*$ ]]; then
  echo "SPARK_WORKER_CORES must be a positive integer" >&2
  exit 1
fi

if [[ ! "${SPARK_WORKER_MEMORY}" =~ ^[1-9][0-9]*[gGmM]$ ]]; then
  echo "SPARK_WORKER_MEMORY must use a value such as 6g or 2048m" >&2
  exit 1
fi

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"
PRIVATE_IPS="$(terraform -chdir="${TF_DIR}" output -json node_private_ips)"

master_public="$(jq -er '."spark-master"' <<<"${PUBLIC_IPS}")"
worker_public="$(jq -er '."spark-worker-1"' <<<"${PUBLIC_IPS}")"
master_private="$(jq -er '."spark-master"' <<<"${PRIVATE_IPS}")"
worker_private="$(jq -er '."spark-worker-1"' <<<"${PRIVATE_IPS}")"

for ip in "${master_public}" "${worker_public}" "${master_private}" "${worker_private}"; do
  if [[ ! "${ip}" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]]; then
    echo "Terraform returned an invalid IPv4 address" >&2
    exit 1
  fi
done

master_compose="$(mktemp)"
worker_compose="$(mktemp)"
trap 'rm -f "${master_compose}" "${worker_compose}"' EXIT

cat >"${master_compose}" <<EOF
services:
  spark-master:
    image: ${SPARK_IMAGE}
    container_name: spark-master
    hostname: spark-master
    restart: unless-stopped
    network_mode: host
    extra_hosts:
      - spark-master:${master_private}
    command:
      - /opt/spark/bin/spark-class
      - org.apache.spark.deploy.master.Master
      - --host
      - ${master_private}
      - --port
      - "7077"
      - --webui-port
      - "8080"
    volumes:
      - ${JOBS_DIR}:/opt/spark/jobs:ro
EOF

cat >"${worker_compose}" <<EOF
services:
  spark-worker:
    image: ${SPARK_IMAGE}
    container_name: spark-worker
    hostname: spark-worker-1
    restart: unless-stopped
    network_mode: host
    extra_hosts:
      - spark-master:${master_private}
      - spark-worker-1:${worker_private}
    command:
      - /opt/spark/bin/spark-class
      - org.apache.spark.deploy.worker.Worker
      - --host
      - ${worker_private}
      - --webui-port
      - "8081"
      - --cores
      - "${SPARK_WORKER_CORES}"
      - --memory
      - "${SPARK_WORKER_MEMORY}"
      - spark://${master_private}:7077
    volumes:
      - ${JOBS_DIR}:/opt/spark/jobs:ro
EOF

deploy_compose() {
  local node_name="$1"
  local public_ip="$2"
  local compose_file="$3"

  echo "[spark-deploy] Deploying ${node_name}"

  ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    "${SSH_USER}@${public_ip}" \
    "mkdir -p '${SERVICE_DIR}' '${JOBS_DIR}'"

  scp -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    "${compose_file}" "${SSH_USER}@${public_ip}:${REMOTE_COMPOSE}"

  ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    "${SSH_USER}@${public_ip}" \
    "cd '${SERVICE_DIR}' && docker compose pull && docker compose up -d --remove-orphans"
}

deploy_compose "spark-master" "${master_public}" "${master_compose}"
deploy_compose "spark-worker-1" "${worker_public}" "${worker_compose}"

echo "[spark-deploy] Waiting for worker registration"
sleep 15

echo "[spark-deploy] Spark master: spark://${master_private}:7077"
echo "[spark-deploy] Spark master UI (VPC only): http://${master_private}:8080"
