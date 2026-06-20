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

for command_name in terraform jq ssh; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command not found: ${command_name}" >&2
    exit 1
  fi
done

PUBLIC_IPS="$(terraform -chdir="${TF_DIR}" output -json node_public_ips)"
PRIVATE_IPS="$(terraform -chdir="${TF_DIR}" output -json node_private_ips)"

spark_public="$(jq -er '."spark-master"' <<<"${PUBLIC_IPS}")"
analytics_public="$(jq -er '."analytics-node"' <<<"${PUBLIC_IPS}")"
kafka_public="$(jq -er '."kafka-1"' <<<"${PUBLIC_IPS}")"
kafka_1_private="$(jq -er '."kafka-1"' <<<"${PRIVATE_IPS}")"
kafka_2_private="$(jq -er '."kafka-2"' <<<"${PRIVATE_IPS}")"
kafka_3_private="$(jq -er '."kafka-3"' <<<"${PRIVATE_IPS}")"
kafka_bootstrap="${kafka_1_private}:9092,${kafka_2_private}:9092,${kafka_3_private}:9092"

query_clickhouse() {
  local query="$1"
  ssh -i "${SSH_KEY}" -o BatchMode=yes "${SSH_USER}@${analytics_public}" \
    "docker exec clickhouse clickhouse-client --query $(printf '%q' "${query}")"
}

echo "===== Streaming container ====="
ssh -i "${SSH_KEY}" -o BatchMode=yes "${SSH_USER}@${spark_public}" \
  'test "$(docker inspect -f "{{.State.Running}}" spark-market-stream)" = true && docker ps --filter name=spark-market-stream --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"'

historical_before="$(
  query_clickhouse "SELECT name, total_rows FROM system.tables WHERE database = 'btc' AND name IN ('raw_ohlcv', 'raw_blocks', 'raw_transactions') ORDER BY name FORMAT TSV"
)"

smoke_id="$(date -u +%Y%m%dT%H%M%S)-$RANDOM"
source_name="e2e-smoke-${smoke_id}"
event_time="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
ingested_at="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"

payload="$(
  jq -cn \
    --arg event_time "${event_time}" \
    --arg ingested_at "${ingested_at}" \
    --arg source "${source_name}" \
    '{
      event_time: $event_time,
      ingested_at: $ingested_at,
      source: $source,
      asset: "BTC",
      interval: "1m",
      open: 62000.0,
      high: 62100.0,
      low: 61950.0,
      close: 62050.0,
      volume: 1.25
    }'
)"

echo "===== Producing smoke event ====="
printf '%s\n' "${payload}" | \
  ssh -i "${SSH_KEY}" -o BatchMode=yes "${SSH_USER}@${kafka_public}" \
    "docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server '${kafka_bootstrap}' --topic btc.market.raw"

echo "===== Waiting for ClickHouse row ====="
row_count=0
for _ in $(seq 1 30); do
  row_count="$(
    query_clickhouse "SELECT count() FROM btc.realtime_market_events FINAL WHERE source = '${source_name}'"
  )"
  if [[ "${row_count}" == "1" ]]; then
    break
  fi
  sleep 5
done

if [[ "${row_count}" != "1" ]]; then
  ssh -i "${SSH_KEY}" -o BatchMode=yes "${SSH_USER}@${spark_public}" \
    "docker logs --tail=100 spark-market-stream 2>&1" >&2
  echo "Smoke event was not persisted exactly once" >&2
  exit 1
fi

query_clickhouse "SELECT source, asset, close, kafka_topic, kafka_partition, kafka_offset FROM btc.realtime_market_events FINAL WHERE source = '${source_name}' FORMAT TSV"

historical_after="$(
  query_clickhouse "SELECT name, total_rows FROM system.tables WHERE database = 'btc' AND name IN ('raw_ohlcv', 'raw_blocks', 'raw_transactions') ORDER BY name FORMAT TSV"
)"

if [[ "${historical_before}" != "${historical_after}" ]]; then
  echo "Historical table metadata changed during verification" >&2
  exit 1
fi

echo "[market-stream-verify] End-to-end event persisted exactly once"
echo "[market-stream-verify] Historical table metadata unchanged"
