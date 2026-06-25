#!/usr/bin/env bash
set -euo pipefail

API_URL="${BLOCKCHAIR_URL:-https://api.blockchair.com/bitcoin/blocks}"
API_KEY="${BLOCKCHAIR_API_KEY:-}"

tmp_headers="$(mktemp)"
trap 'rm -f "${tmp_headers}"' EXIT

curl_args=(
  --silent
  --show-error
  --location
  --max-time 30
  --dump-header "${tmp_headers}"
  "${API_URL}"
)

if [[ -n "${API_KEY}" ]]; then
  curl_args+=(--header "X-Auth-Token: ${API_KEY}")
fi

body="$(curl "${curl_args[@]}")"

status_line="$(sed -n '1p' "${tmp_headers}")"
status_code="$(awk '{print $2}' <<<"${status_line}")"

echo "status=${status_code}"
echo "--- headers ---"
sed -n '2,20p' "${tmp_headers}"
echo "--- body ---"
printf '%s\n' "${body}" | sed -n '1,40p'
