#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TF_DIR="${1:-${REPO_ROOT}/infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-60}"
MAX_WAIT_MINUTES="${MAX_WAIT_MINUTES:-0}"

for path in "${TF_DIR}" "${SSH_KEY}" "${REPO_ROOT}/scripts/report-feature-backfill-progress.sh" "${REPO_ROOT}/scripts/run-modeling-1h.sh"; do
  if [[ ! -e "${path}" ]]; then
    echo "Required path not found: ${path}" >&2
    exit 1
  fi
done

for command_name in terraform jq ssh date; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command not found: ${command_name}" >&2
    exit 1
  fi
done

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"
analytics_public="$(jq -er '."analytics-node"' <<<"${PUBLIC_IPS}")"

query_status() {
  ssh -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    "${SSH_USER}@${analytics_public}" \
    "docker exec clickhouse clickhouse-client --query \"SELECT countIf(status = 'pending'), countIf(status = 'running'), countIf(status IN ('succeeded', 'complete')), count() FROM btc.feature_backfill_batches FINAL FORMAT TabSeparated\""
}

echo "[feature-watch] Watching feature backfill progress"
start_epoch="$(date +%s)"

while true; do
  bash "${REPO_ROOT}/scripts/report-feature-backfill-progress.sh" "${TF_DIR}" || true
  status_line="$(query_status)"
  IFS=$'\t' read -r pending running succeeded total <<<"${status_line}"

  pending="${pending:-0}"
  running="${running:-0}"
  succeeded="${succeeded:-0}"
  total="${total:-0}"

  echo "[feature-watch] pending=${pending} running=${running} succeeded=${succeeded} total=${total}"

  if [[ "${total}" -eq 0 ]]; then
    echo "[feature-watch] No checkpoint rows found yet; waiting for seeding"
  elif [[ "${pending}" -eq 0 && "${running}" -eq 0 ]]; then
    echo "[feature-watch] Feature backfill is complete; launching modeling pipeline"
    bash "${REPO_ROOT}/scripts/run-modeling-1h.sh" "${TF_DIR}"
    exit 0
  fi

  if [[ "${MAX_WAIT_MINUTES}" -gt 0 ]]; then
    now_epoch="$(date +%s)"
    elapsed_minutes="$(( (now_epoch - start_epoch) / 60 ))"
    if [[ "${elapsed_minutes}" -ge "${MAX_WAIT_MINUTES}" ]]; then
      echo "[feature-watch] Timed out after ${elapsed_minutes} minute(s)" >&2
      exit 1
    fi
  fi

  sleep "${POLL_INTERVAL_SECONDS}"
done
