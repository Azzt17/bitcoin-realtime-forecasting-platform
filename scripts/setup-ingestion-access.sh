#!/usr/bin/env bash
set -euo pipefail

PUBKEY_FILE="${1:-}"
TF_DIR="${2:-infra/terraform}"
INGESTION_USER="${3:-ingestion}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"

REMOTE_KEY_FILE="/tmp/${INGESTION_USER}_authorized_key.pub"
IMPORT_BASE="/opt/bitcoin-realtime-forecasting-platform/data/imports"

usage() {
  cat <<EOF
Usage:
  scripts/setup-ingestion-access.sh <public-key-file> [infra/terraform] [username]

Example:
  scripts/setup-ingestion-access.sh /tmp/ingestion_friend.pub infra/terraform ingestion

Environment overrides:
  SSH_KEY=/path/to/private/key
  SSH_USER=root
EOF
}

if [[ -z "${PUBKEY_FILE}" ]]; then
  usage >&2
  exit 1
fi

if [[ ! -f "${PUBKEY_FILE}" ]]; then
  echo "Public key file not found: ${PUBKEY_FILE}" >&2
  exit 1
fi

if [[ ! -d "${TF_DIR}" ]]; then
  echo "Terraform directory not found: ${TF_DIR}" >&2
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

PUBKEY_CONTENT="$(tr -d '\r' < "${PUBKEY_FILE}" | sed '/^[[:space:]]*$/d')"
PUBKEY_LINE_COUNT="$(printf '%s\n' "${PUBKEY_CONTENT}" | wc -l | tr -d ' ')"

if [[ "${PUBKEY_LINE_COUNT}" != "1" ]]; then
  echo "Public key file must contain exactly one non-empty public key line." >&2
  exit 1
fi

if ! printf '%s\n' "${PUBKEY_CONTENT}" | grep -Eq '^(ssh-ed25519|ssh-rsa|ecdsa-sha2-nistp256|ecdsa-sha2-nistp384|ecdsa-sha2-nistp521) '; then
  echo "Public key does not look like a valid OpenSSH public key." >&2
  echo "Expected prefix: ssh-ed25519, ssh-rsa, or ecdsa-sha2-*." >&2
  exit 1
fi

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"
ANALYTICS_PUBLIC_IP="$(echo "${PUBLIC_IPS}" | jq -r '."analytics-node"')"

if [[ -z "${ANALYTICS_PUBLIC_IP}" || "${ANALYTICS_PUBLIC_IP}" == "null" ]]; then
  echo "Could not read analytics-node public IP from Terraform output." >&2
  exit 1
fi

TMP_CLEAN_KEY="$(mktemp)"
printf '%s\n' "${PUBKEY_CONTENT}" > "${TMP_CLEAN_KEY}"

echo "[ingestion-access] Target analytics-node: ${ANALYTICS_PUBLIC_IP}"
echo "[ingestion-access] Target user: ${INGESTION_USER}"
echo "[ingestion-access] Public key fingerprint:"
ssh-keygen -lf "${TMP_CLEAN_KEY}" || true

scp \
  -i "${SSH_KEY}" \
  -o StrictHostKeyChecking=accept-new \
  "${TMP_CLEAN_KEY}" \
  "${SSH_USER}@${ANALYTICS_PUBLIC_IP}:${REMOTE_KEY_FILE}"

rm -f "${TMP_CLEAN_KEY}"

ssh \
  -i "${SSH_KEY}" \
  -o StrictHostKeyChecking=accept-new \
  "${SSH_USER}@${ANALYTICS_PUBLIC_IP}" \
  "INGESTION_USER='${INGESTION_USER}' REMOTE_KEY_FILE='${REMOTE_KEY_FILE}' IMPORT_BASE='${IMPORT_BASE}' bash -s" <<'REMOTE_SCRIPT'
set -euo pipefail

if [[ ! -f "${REMOTE_KEY_FILE}" ]]; then
  echo "Remote key file not found: ${REMOTE_KEY_FILE}" >&2
  exit 1
fi

if ! id "${INGESTION_USER}" >/dev/null 2>&1; then
  adduser --disabled-password --gecos "" "${INGESTION_USER}"
fi

install -d -m 700 -o "${INGESTION_USER}" -g "${INGESTION_USER}" "/home/${INGESTION_USER}/.ssh"
touch "/home/${INGESTION_USER}/.ssh/authorized_keys"
chown "${INGESTION_USER}:${INGESTION_USER}" "/home/${INGESTION_USER}/.ssh/authorized_keys"
chmod 600 "/home/${INGESTION_USER}/.ssh/authorized_keys"

KEY_CONTENT="$(cat "${REMOTE_KEY_FILE}")"

if ! grep -qxF "${KEY_CONTENT}" "/home/${INGESTION_USER}/.ssh/authorized_keys"; then
  printf '%s\n' "${KEY_CONTENT}" >> "/home/${INGESTION_USER}/.ssh/authorized_keys"
fi

rm -f "${REMOTE_KEY_FILE}"

mkdir -p \
  "${IMPORT_BASE}/incoming" \
  "${IMPORT_BASE}/processed" \
  "${IMPORT_BASE}/rejected" \
  "${IMPORT_BASE}/metadata"

chown -R "${INGESTION_USER}:${INGESTION_USER}" "${IMPORT_BASE}"
chmod 750 "${IMPORT_BASE}"
chmod 750 "${IMPORT_BASE}/incoming" "${IMPORT_BASE}/processed" "${IMPORT_BASE}/rejected" "${IMPORT_BASE}/metadata"

# Confirm user has no sudo group by default.
if id -nG "${INGESTION_USER}" | tr ' ' '\n' | grep -qx 'sudo'; then
  echo "WARNING: ${INGESTION_USER} is in sudo group. Remove manually if unintended." >&2
fi

echo "[remote] User:"
id "${INGESTION_USER}"

echo "[remote] Authorized keys:"
wc -l "/home/${INGESTION_USER}/.ssh/authorized_keys"

echo "[remote] Import directory:"
ls -ld "${IMPORT_BASE}" "${IMPORT_BASE}/incoming" "${IMPORT_BASE}/processed" "${IMPORT_BASE}/rejected" "${IMPORT_BASE}/metadata"
REMOTE_SCRIPT

echo
echo "[ingestion-access] Access setup completed."
echo
echo "Send this test command to the teammate:"
echo "ssh ${INGESTION_USER}@${ANALYTICS_PUBLIC_IP}"
echo
echo "Upload example:"
echo "scp file_historical.csv ${INGESTION_USER}@${ANALYTICS_PUBLIC_IP}:${IMPORT_BASE}/incoming/"
