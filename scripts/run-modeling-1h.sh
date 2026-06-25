#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TF_DIR="${1:-${REPO_ROOT}/infra/terraform}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/bitcoin_realtime_platform}"
SSH_USER="${SSH_USER:-root}"

MODEL_NAME_BASELINE="${MODEL_NAME_BASELINE:-baseline_persistence}"
MODEL_VERSION_BASELINE="${MODEL_VERSION_BASELINE:-v1}"
MODEL_NAME_GBT="${MODEL_NAME_GBT:-spark_gbt}"
MODEL_VERSION_GBT="${MODEL_VERSION_GBT:-v1}"
MODEL_ARTIFACT_DIR="${MODEL_ARTIFACT_DIR:-/tmp/bitcoin-models}"

FEATURE_START="${FEATURE_START:-AUTO}"
FEATURE_END="${FEATURE_END:-AUTO}"
TRAIN_START="${TRAIN_START:-AUTO}"
TRAIN_END="${TRAIN_END:-AUTO}"
SCORE_START="${SCORE_START:-AUTO}"
SCORE_END="${SCORE_END:-AUTO}"
LABEL_HOURS="${LABEL_HOURS:-1}"
RUN_EVALUATION="${RUN_EVALUATION:-0}"
WAIT_FOR_EVALUATION="${WAIT_FOR_EVALUATION:-0}"

PROJECT_DIR="/opt/bitcoin-realtime-forecasting-platform"
REMOTE_SCHEMA_DIR="/tmp/modeling-schemas"
REMOTE_JOB_HOST_DIR="${PROJECT_DIR}/jobs/spark"
REMOTE_JOB_CONTAINER_DIR="/opt/spark/jobs"
REMOTE_FEATURE_JOB="${REMOTE_JOB_CONTAINER_DIR}/build_training_dataset_1h.py"
REMOTE_BASELINE_JOB="${REMOTE_JOB_CONTAINER_DIR}/train_baseline.py"
REMOTE_GBT_JOB="${REMOTE_JOB_CONTAINER_DIR}/train_gbt.py"
REMOTE_EVAL_JOB="${REMOTE_JOB_CONTAINER_DIR}/evaluate_predictions.py"
PREDICTIONS_STAGE_TABLE="predictions_staging"

spark_jars="/tmp/.ivy2/jars/com.clickhouse_jdbc-v2-0.9.8.jar,/tmp/.ivy2/jars/com.clickhouse_client-v2-0.9.8.jar,/tmp/.ivy2/jars/com.clickhouse_clickhouse-client-0.9.8.jar,/tmp/.ivy2/jars/com.clickhouse_clickhouse-data-0.9.8.jar,/tmp/.ivy2/jars/com.clickhouse_clickhouse-http-client-0.9.8.jar,/tmp/.ivy2/jars/org.apache.httpcomponents.client5_httpclient5-5.4.4.jar,/tmp/.ivy2/jars/org.apache.httpcomponents.core5_httpcore5-5.3.4.jar,/tmp/.ivy2/jars/org.apache.httpcomponents.core5_httpcore5-h2-5.3.4.jar,/tmp/.ivy2/jars/com.google.guava_guava-33.4.6-jre.jar,/tmp/.ivy2/jars/com.google.guava_failureaccess-1.0.3.jar,/tmp/.ivy2/jars/com.google.guava_listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar,/tmp/.ivy2/jars/com.google.j2objc_j2objc-annotations-3.0.0.jar,/tmp/.ivy2/jars/com.google.errorprone_error_prone_annotations-2.36.0.jar"
SSH_CMD=(ssh -F /dev/null -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new)
SCP_CMD=(scp -F /dev/null -i "${SSH_KEY}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new)

for path in \
  "${TF_DIR}" \
  "${SSH_KEY}" \
  "${REPO_ROOT}/clickhouse/schema/training_dataset_1h.sql" \
  "${REPO_ROOT}/clickhouse/schema/predictions.sql" \
  "${REPO_ROOT}/clickhouse/schema/prediction_errors.sql" \
  "${REPO_ROOT}/clickhouse/schema/model_metrics.sql" \
  "${REPO_ROOT}/clickhouse/schema/pipeline_metrics.sql" \
  "${REPO_ROOT}/spark_training/build_training_dataset_1h.py" \
  "${REPO_ROOT}/spark_training/train_baseline.py" \
  "${REPO_ROOT}/spark_training/train_gbt.py" \
  "${REPO_ROOT}/spark_evaluation/evaluate_predictions.py"; do
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

analytics_public="$(jq -er '."analytics-node"' <<<"${PUBLIC_IPS}")"
analytics_private="$(jq -er '."analytics-node"' <<<"${PRIVATE_IPS}")"
spark_public="$(jq -er '."spark-master"' <<<"${PUBLIC_IPS}")"
spark_private="$(jq -er '."spark-master"' <<<"${PRIVATE_IPS}")"
spark_worker_public="$(jq -er '."spark-worker-1"' <<<"${PUBLIC_IPS}")"
spark_worker_private="$(jq -er '."spark-worker-1"' <<<"${PRIVATE_IPS}")"

for ip in "${analytics_public}" "${analytics_private}" "${spark_public}" "${spark_private}"; do
  if [[ ! "${ip}" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]]; then
    echo "Terraform returned an invalid IPv4 address" >&2
    exit 1
  fi
done

for ip in "${spark_worker_public}" "${spark_worker_private}"; do
  if [[ ! "${ip}" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]]; then
    echo "Terraform returned an invalid IPv4 address" >&2
    exit 1
  fi
done

PIPELINE_RUN_ID="${PIPELINE_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
PIPELINE_COMPONENT="${PIPELINE_COMPONENT:-modeling_1h}"
PIPELINE_HEARTBEAT_SECONDS="${PIPELINE_HEARTBEAT_SECONDS:-60}"
SPARK_PYTHON_EXTRA_DIR="${SPARK_PYTHON_EXTRA_DIR:-/opt/spark/python-extra}"

emit_pipeline_metric() {
  local stage_name="$1"
  local event_name="$2"
  local metric_value="$3"
  local unit="${4:-state}"
  local metric_time payload tags_json
  metric_time="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  tags_json="{\"run_id\":\"${PIPELINE_RUN_ID}\",\"stage\":\"${stage_name}\",\"event\":\"${event_name}\"}"
  payload="$(
    jq -cn \
      --arg metric_time "${metric_time}" \
      --arg component "${PIPELINE_COMPONENT}" \
      --arg metric_name "${stage_name}_${event_name}" \
      --argjson metric_value "${metric_value}" \
      --arg unit "${unit}" \
      --arg tags "${tags_json}" \
      '{
        metric_time: $metric_time,
        component: $component,
        metric_name: $metric_name,
        metric_value: $metric_value,
        unit: $unit,
        tags: $tags
      }'
  )"

  if ! printf '%s\n' "${payload}" | "${SSH_CMD[@]}" \
    "${SSH_USER}@${analytics_public}" \
    "docker exec -i clickhouse clickhouse-client --query 'INSERT INTO btc.pipeline_metrics FORMAT JSONEachRow'"; then
    echo "[modeling-1h] Warning: failed to write pipeline metric ${stage_name}_${event_name}" >&2
  fi
}

ensure_python_package() {
  local node_public_ip="$1"
  local container_name="$2"
  local package_name="$3"
  local package_spec="${4:-${package_name}}"
  local docker_exec_root=(docker exec -u 0 -e PIP_DISABLE_PIP_VERSION_CHECK=1)

  echo "[modeling-1h] Installing ${package_spec} on ${container_name}"
  "${SSH_CMD[@]}" \
    "${SSH_USER}@${node_public_ip}" \
    "${docker_exec_root[*]} -e PYTHONPATH=${SPARK_PYTHON_EXTRA_DIR} ${container_name} sh -lc 'mkdir -p ${SPARK_PYTHON_EXTRA_DIR} && python3 -m pip install --no-cache-dir --target ${SPARK_PYTHON_EXTRA_DIR} ${package_spec}'"

  "${SSH_CMD[@]}" \
    "${SSH_USER}@${node_public_ip}" \
    "${docker_exec_root[*]} -e PYTHONPATH=${SPARK_PYTHON_EXTRA_DIR} ${container_name} python3 -c 'import ${package_name}; print(\"${package_name} ok\")'"
}

run_stage_with_heartbeat() {
  local stage_name="$1"
  shift

  echo "[modeling-1h] ${stage_name}"
  emit_pipeline_metric "${stage_name}" "started" 1 "state"

  local stage_start_epoch
  stage_start_epoch="$(date +%s)"

  "$@" &
  local stage_pid=$!

  (
    while kill -0 "${stage_pid}" 2>/dev/null; do
      local now_epoch elapsed_seconds
      now_epoch="$(date +%s)"
      elapsed_seconds="$((now_epoch - stage_start_epoch))"
      emit_pipeline_metric "${stage_name}" "elapsed_seconds" "${elapsed_seconds}" "seconds"
      sleep "${PIPELINE_HEARTBEAT_SECONDS}"
    done
  ) &
  local heartbeat_pid=$!

  wait "${stage_pid}"
  local stage_exit_code=$?

  kill "${heartbeat_pid}" >/dev/null 2>&1 || true
  wait "${heartbeat_pid}" >/dev/null 2>&1 || true

  if [[ "${stage_exit_code}" -eq 0 ]]; then
    emit_pipeline_metric "${stage_name}" "finished" 1 "state"
  else
    emit_pipeline_metric "${stage_name}" "failed" "${stage_exit_code}" "exit_code"
  fi

  return "${stage_exit_code}"
}

derive_modeling_windows() {
  local coverage_json
  coverage_json="$(
    "${SSH_CMD[@]}" \
      "${SSH_USER}@${analytics_public}" \
      'docker exec clickhouse clickhouse-client --query "SELECT min(feature_time) AS min_feature_time, max(feature_time) AS max_feature_time, count() AS row_count FROM btc.features_1h FORMAT JSON"'
  )"

  COVERAGE_JSON="${coverage_json}" python3 - <<'PY'
import json
import os
import sys
from datetime import datetime, timedelta, timezone

payload = json.loads(os.environ["COVERAGE_JSON"])
rows = payload.get("data", [])
if not rows:
    print("No rows found in btc.features_1h", file=sys.stderr)
    raise SystemExit(1)

row = rows[0]
min_raw = row.get("min_feature_time")
max_raw = row.get("max_feature_time")
row_count = int(row.get("row_count") or 0)
if not min_raw or not max_raw or row_count <= 0:
    print("btc.features_1h does not have enough coverage yet", file=sys.stderr)
    raise SystemExit(1)

def parse_ts(raw: str) -> datetime:
    parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

def floor_hour(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)

min_time = floor_hour(parse_ts(min_raw))
max_time = floor_hour(parse_ts(max_raw))
if max_time <= min_time:
    print("btc.features_1h coverage is too small for training", file=sys.stderr)
    raise SystemExit(1)

coverage = max_time - min_time
if coverage >= timedelta(days=730):
    score_start = max_time - timedelta(days=365)
else:
    score_start = min_time + timedelta(seconds=(coverage.total_seconds() * 0.8))

score_start = floor_hour(score_start)
train_start = min_time
train_end = score_start
score_end = max_time

if train_end <= train_start:
    train_end = min_time + timedelta(hours=24)
if score_start <= train_start:
    score_start = train_start + timedelta(hours=24)
if score_end <= score_start:
    score_end = score_start + timedelta(hours=24)

def emit(name: str, value: datetime) -> None:
    print(f"{name}={value.strftime('%Y-%m-%dT%H:%M:%SZ')}")

emit("FEATURE_START", train_start)
emit("FEATURE_END", score_end)
emit("TRAIN_START", train_start)
emit("TRAIN_END", train_end)
emit("SCORE_START", score_start)
emit("SCORE_END", score_end)
PY
}

if [[ "${FEATURE_START}" == "AUTO" || "${FEATURE_END}" == "AUTO" || "${TRAIN_START}" == "AUTO" || "${TRAIN_END}" == "AUTO" || "${SCORE_START}" == "AUTO" || "${SCORE_END}" == "AUTO" ]]; then
  echo "[modeling-1h] Deriving split windows from btc.features_1h coverage"
  if ! derived_windows="$(derive_modeling_windows)"; then
    echo "[modeling-1h] Failed to derive split windows from live feature coverage" >&2
    exit 1
  fi
  eval "${derived_windows}"
fi

echo "[modeling-1h] Feature window  : ${FEATURE_START} -> ${FEATURE_END}"
echo "[modeling-1h] Train window    : ${TRAIN_START} -> ${TRAIN_END}"
echo "[modeling-1h] Score window    : ${SCORE_START} -> ${SCORE_END}"

sync_modeling_jobs() {
  echo "[modeling-1h] Uploading Spark jobs to spark-master"
  "${SSH_CMD[@]}" \
    "${SSH_USER}@${spark_public}" \
    "mkdir -p '${REMOTE_JOB_HOST_DIR}'"
  "${SCP_CMD[@]}" \
    "${REPO_ROOT}/spark_training/build_training_dataset_1h.py" \
    "${SSH_USER}@${spark_public}:${REMOTE_JOB_HOST_DIR}/build_training_dataset_1h.py"
  "${SCP_CMD[@]}" \
    "${REPO_ROOT}/spark_training/train_baseline.py" \
    "${SSH_USER}@${spark_public}:${REMOTE_JOB_HOST_DIR}/train_baseline.py"
  "${SCP_CMD[@]}" \
    "${REPO_ROOT}/spark_training/train_gbt.py" \
    "${SSH_USER}@${spark_public}:${REMOTE_JOB_HOST_DIR}/train_gbt.py"
  "${SCP_CMD[@]}" \
    "${REPO_ROOT}/spark_evaluation/evaluate_predictions.py" \
    "${SSH_USER}@${spark_public}:${REMOTE_JOB_HOST_DIR}/evaluate_predictions.py"
}

apply_modeling_schemas() {
  local combined_schema
  combined_schema="$(mktemp)"
  trap 'rm -f "${combined_schema}"' RETURN
  cat \
    "${REPO_ROOT}/clickhouse/schema/training_dataset_1h.sql" \
    "${REPO_ROOT}/clickhouse/schema/predictions_staging.sql" \
    "${REPO_ROOT}/clickhouse/schema/predictions.sql" \
    "${REPO_ROOT}/clickhouse/schema/prediction_errors.sql" \
    "${REPO_ROOT}/clickhouse/schema/model_metrics.sql" \
    "${REPO_ROOT}/clickhouse/schema/pipeline_metrics.sql" \
    >"${combined_schema}"

  echo "[modeling-1h] Uploading schema bundle to analytics-node"
  "${SSH_CMD[@]}" \
    "${SSH_USER}@${analytics_public}" \
    "mkdir -p '${REMOTE_SCHEMA_DIR}'"
  "${SCP_CMD[@]}" \
    "${combined_schema}" "${SSH_USER}@${analytics_public}:${REMOTE_SCHEMA_DIR}/modeling_schemas.sql"

  echo "[modeling-1h] Applying ClickHouse schema bundle"
  "${SSH_CMD[@]}" \
    "${SSH_USER}@${analytics_public}" \
    "docker exec -i clickhouse clickhouse-client --multiquery < '${REMOTE_SCHEMA_DIR}/modeling_schemas.sql'"
}

run_spark_job() {
  local remote_job="$1"
  shift
  local remote_args=("$@")
  local remote_cmd=(
    docker exec -e PYTHONPATH="${SPARK_PYTHON_EXTRA_DIR}" spark-master /opt/spark/bin/spark-submit
    --master "spark://${spark_private}:7077"
    --deploy-mode client
    --conf "spark.driver.host=${spark_private}"
    --conf "spark.driver.bindAddress=0.0.0.0"
    --conf "spark.driverEnv.PYTHONPATH=${SPARK_PYTHON_EXTRA_DIR}"
    --conf "spark.executorEnv.PYTHONPATH=${SPARK_PYTHON_EXTRA_DIR}"
    --conf "spark.driver.userClassPathFirst=true"
    --conf "spark.executor.userClassPathFirst=true"
    --conf "spark.ui.enabled=false"
    --jars "${spark_jars}"
    "${remote_job}"
  )
  local remote_cmd_str=""
  local part
  for part in "${remote_cmd[@]}" "${remote_args[@]}"; do
    remote_cmd_str+=$(printf ' %q' "${part}")
  done
  "${SSH_CMD[@]}" \
    "${SSH_USER}@${spark_public}" \
    "${remote_cmd_str# }"
}

truncate_prediction_stage() {
  "${SSH_CMD[@]}" \
    "${SSH_USER}@${analytics_public}" \
    "docker exec clickhouse clickhouse-client --query \"TRUNCATE TABLE IF EXISTS btc.${PREDICTIONS_STAGE_TABLE}\""
}

promote_prediction_stage() {
  "${SSH_CMD[@]}" \
    "${SSH_USER}@${analytics_public}" \
    "docker exec clickhouse clickhouse-client --query \"INSERT INTO btc.predictions (prediction_time, target_time, asset, horizon, model_name, model_version, current_price, predicted_return, predicted_price, created_at) SELECT prediction_time, target_time, asset, horizon, model_name, model_version, current_price, predicted_return, predicted_price, created_at FROM btc.${PREDICTIONS_STAGE_TABLE}\""
}

echo "[modeling-1h] Preparing modeling runtime"
sync_modeling_jobs
apply_modeling_schemas

ensure_python_package "${spark_public}" "spark-master" "numpy" "${SPARK_NUMPY_PACKAGE:-numpy==1.26.4}"
ensure_python_package "${spark_worker_public}" "spark-worker" "numpy" "${SPARK_NUMPY_PACKAGE:-numpy==1.26.4}"

truncate_prediction_stage
run_stage_with_heartbeat "dataset_build" run_spark_job "${REMOTE_FEATURE_JOB}" \
  --clickhouse-host "${analytics_private}" \
  --clickhouse-port 8123 \
  --clickhouse-database btc \
  --clickhouse-user default \
  --target-table training_dataset_1h \
  --feature-table features_1h \
  --source-view view_ohlcv_1h \
  --start-time "${FEATURE_START}" \
  --end-time "${FEATURE_END}" \
  --label-hours "${LABEL_HOURS}"

truncate_prediction_stage
run_stage_with_heartbeat "baseline_train" run_spark_job "${REMOTE_BASELINE_JOB}" \
  --clickhouse-host "${analytics_private}" \
  --clickhouse-port 8123 \
  --clickhouse-database btc \
  --clickhouse-user default \
  --dataset-table training_dataset_1h \
  --prediction-table "${PREDICTIONS_STAGE_TABLE}" \
  --metrics-table model_metrics \
  --model-name "${MODEL_NAME_BASELINE}" \
  --model-version "${MODEL_VERSION_BASELINE}" \
  --horizon 1h \
  --predict-start "${SCORE_START}" \
  --predict-end "${SCORE_END}" \
  --artifact-dir "${MODEL_ARTIFACT_DIR}/baseline"

promote_prediction_stage

truncate_prediction_stage
run_stage_with_heartbeat "gbt_train" run_spark_job "${REMOTE_GBT_JOB}" \
  --clickhouse-host "${analytics_private}" \
  --clickhouse-port 8123 \
  --clickhouse-database btc \
  --clickhouse-user default \
  --dataset-table training_dataset_1h \
  --prediction-table "${PREDICTIONS_STAGE_TABLE}" \
  --metrics-table model_metrics \
  --model-name "${MODEL_NAME_GBT}" \
  --model-version "${MODEL_VERSION_GBT}" \
  --horizon 1h \
  --train-start "${TRAIN_START}" \
  --train-end "${TRAIN_END}" \
  --score-start "${SCORE_START}" \
  --score-end "${SCORE_END}" \
  --artifact-dir "${MODEL_ARTIFACT_DIR}/spark_gbt"

promote_prediction_stage

if [[ "${RUN_EVALUATION}" == "1" ]]; then
  if [[ "${WAIT_FOR_EVALUATION}" == "1" ]]; then
    echo "[modeling-1h] Waiting before delayed evaluation is unsupported in this wrapper"
  fi
  run_stage_with_heartbeat "evaluation" run_spark_job "${REMOTE_EVAL_JOB}" \
    --clickhouse-host "${analytics_private}" \
    --clickhouse-port 8123 \
    --clickhouse-database btc \
    --clickhouse-user default \
    --prediction-table predictions \
    --error-table prediction_errors \
    --metrics-table model_metrics \
    --model-name "${MODEL_NAME_GBT}" \
    --model-version "${MODEL_VERSION_GBT}" \
    --horizon 1h \
    --prediction-start "${SCORE_START}" \
    --prediction-end "${SCORE_END}"
else
  echo "[modeling-1h] Prediction evaluation deferred until target times are available"
fi

echo "[modeling-1h] Modeling run complete"
