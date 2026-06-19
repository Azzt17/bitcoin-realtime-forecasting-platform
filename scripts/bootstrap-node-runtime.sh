#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "This script must run as root. Re-run with sudo or SSH as root." >&2
  exit 1
fi

echo "[bootstrap] Starting node runtime bootstrap on $(hostname)"

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y \
  ca-certificates \
  curl \
  gnupg \
  lsb-release \
  jq \
  htop \
  unzip \
  git \
  python3 \
  python3-pip \
  openjdk-17-jre-headless

install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc

UBUNTU_CODENAME="$(
  . /etc/os-release
  echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}"
)"

cat > /etc/apt/sources.list.d/docker.list <<EOF
deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${UBUNTU_CODENAME} stable
EOF

apt-get update
apt-get install -y \
  docker-ce \
  docker-ce-cli \
  containerd.io \
  docker-buildx-plugin \
  docker-compose-plugin

systemctl enable --now docker

mkdir -p /opt/bitcoin-realtime-forecasting-platform/{services,configs,data,logs,scripts}
chmod 755 /opt/bitcoin-realtime-forecasting-platform

cat > /opt/bitcoin-realtime-forecasting-platform/runtime-versions.txt <<EOF
BOOTSTRAPPED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
HOSTNAME=$(hostname)
$(cat /etc/bitcoin-platform-node 2>/dev/null || true)

JAVA_VERSION=$(java -version 2>&1 | head -n 1)
PYTHON_VERSION=$(python3 --version 2>&1)
DOCKER_VERSION=$(docker --version 2>&1)
DOCKER_COMPOSE_VERSION=$(docker compose version 2>&1)
EOF

echo "[bootstrap] Runtime versions:"
cat /opt/bitcoin-realtime-forecasting-platform/runtime-versions.txt

echo "[bootstrap] Docker test:"
docker run --rm hello-world >/tmp/docker-hello-world.log
tail -n 5 /tmp/docker-hello-world.log

echo "[bootstrap] Completed successfully on $(hostname)"
