# Service Contract — Bitcoin Real-Time Forecasting Platform

**Project:** Bitcoin Real-Time Forecasting Platform  
**Purpose:** Define clear integration contracts between Infrastructure, Data Ingestion, Modeling, and Dashboard responsibilities.  
**Status:** Initial team contract  
**Owner of this document:** Infrastructure team  

---

## 1. Scope

This document defines how each project component communicates with the others.

The project is divided into three main responsibilities:

```text
Infrastructure Team
    Provides Kafka, Spark, ClickHouse, Grafana, Streamlit, networking, deployment, and access.

Data Ingestion Team
    Prepares historical data and sends realtime/replay data into Kafka or ClickHouse.

Modeling Team
    Builds feature engineering, trains models, runs batch/streaming inference, and writes prediction outputs.

Dashboard / Reporting
    Reads from ClickHouse and visualizes data, predictions, errors, and pipeline health.
```

The goal is to avoid ambiguity between teams. Each team should know:

```text
where to send data
where to read data
what schema to follow
what tables/topics are available
what output is expected
```

---

## 2. High-Level Architecture

```text
Historical Data
OHLCV + Blocks + Transactions
        ↓
Batch ingestion / upload
        ↓
ClickHouse / Parquet storage
        ↓
Spark Batch Processing
        ↓
Feature Engineering
        ↓
Model Training
        ↓
Saved Model Artifact

Realtime / Replay Data
Live API / Replay Producer
        ↓
Kafka
        ↓
Spark Structured Streaming
        ↓
Realtime Feature Engineering
        ↓
Model Inference
        ↓
ClickHouse
        ↓
Grafana / Streamlit
```

---

## 3. Default Architectural Decision

The team agrees to use the following default data flow:

```text
Historical large data:
    Imported into ClickHouse or stored as Parquet first.

Realtime and replay data:
    Sent through Kafka.

Training:
    Spark batch jobs read from ClickHouse or Parquet.

Realtime inference:
    Spark Structured Streaming reads from Kafka, loads the trained model, and writes predictions to ClickHouse.

Dashboards:
    Grafana and Streamlit read from ClickHouse.
```

---

## 4. Team Responsibilities

## 4.1 Infrastructure Team

The infrastructure team is responsible for:

```text
Provisioning cloud infrastructure
Configuring private networking and firewall rules
Deploying Kafka
Deploying Spark
Deploying ClickHouse
Deploying Grafana
Deploying Streamlit
Preparing service credentials and access documentation
Creating Kafka topics
Creating ClickHouse databases and initial schemas
Providing deployment and teardown scripts
Documenting endpoint contracts
```

The infrastructure team is not responsible for:

```text
Cleaning historical datasets
Designing final ML features
Training final models
Optimizing model accuracy
Writing the final ingestion business logic
```

---

## 4.2 Data Ingestion Team

The ingestion team is responsible for:

```text
Preparing historical OHLCV data
Preparing historical block data
Preparing historical transaction data
Cleaning raw file formats before import
Uploading historical data to the agreed storage layer
Building batch ingestion scripts if needed
Building realtime/replay producers if needed
Following Kafka topic schema
Following ClickHouse schema
Handling malformed records before sending to production topics
```

The ingestion team must provide:

```text
dataset name
dataset source
date range
row count
file format
timestamp column
timezone assumption
field descriptions
known data quality issues
```

---

## 4.3 Modeling Team

The modeling team is responsible for:

```text
Spark batch feature engineering
Creating 1h / 4h / 24h feature datasets
Training baseline models
Training Spark ML models
Evaluating model performance
Saving model artifacts
Defining inference input schema
Defining prediction output schema
Writing batch inference jobs
Writing streaming inference jobs
Writing prediction results to ClickHouse
Writing prediction error evaluation logic
```

The modeling team must provide:

```text
model name
model version
training data range
target horizon
input feature schema
output prediction schema
model artifact path
dependency requirements
run command for training
run command for inference
evaluation metrics
```

---

## 5. Data Source Contract

## 5.1 Historical OHLCV Data

Expected fields:

```text
timestamp
open
high
low
close
volume
```

Recommended timestamp standard:

```text
UTC timestamp
```

Expected raw table:

```text
btc.raw_ohlcv
```

Recommended ClickHouse schema:

```sql
CREATE TABLE IF NOT EXISTS btc.raw_ohlcv
(
    timestamp DateTime64(3, 'UTC'),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
ORDER BY timestamp;
```

Data quality requirements:

```text
No duplicate timestamp
No null OHLC values
high >= low
high >= open
high >= close
low <= open
low <= close
volume >= 0
timestamp must be sorted or sortable
```

---

## 5.2 Historical Block Data

Expected fields may include:

```text
id
hash
time
transaction_count
input_count
output_count
input_total
input_total_usd
output_total
output_total_usd
fee_total
fee_total_usd
cdd_total
generation
generation_usd
reward
reward_usd
difficulty
size
weight
guessed_miner
```

Expected raw table:

```text
btc.raw_blocks
```

Recommended ClickHouse schema:

```sql
CREATE TABLE IF NOT EXISTS btc.raw_blocks
(
    id UInt64,
    hash String,
    time DateTime64(3, 'UTC'),
    median_time DateTime64(3, 'UTC'),
    size UInt64,
    stripped_size UInt64,
    weight UInt64,
    version UInt32,
    version_hex String,
    version_bits String,
    merkle_root String,
    nonce UInt32,
    bits String,
    difficulty Float64,
    chainwork String,
    coinbase_data_hex String,
    transaction_count UInt32,
    witness_count UInt32,
    input_count UInt32,
    output_count UInt32,
    input_total Float64,
    input_total_usd Float64,
    output_total Float64,
    output_total_usd Float64,
    fee_total Float64,
    fee_total_usd Float64,
    fee_per_kb Float64,
    fee_per_kb_usd Float64,
    fee_per_kwu Float64,
    fee_per_kwu_usd Float64,
    cdd_total Float64,
    generation Float64,
    generation_usd Float64,
    reward Float64,
    reward_usd Float64,
    guessed_miner String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
ORDER BY (time, id);
```

Data quality requirements:

```text
id should be unique
hash should be unique
time must be valid UTC
numeric values must be non-negative where applicable
difficulty must be positive
```

---

## 5.3 Historical Transaction Data

Expected fields may include:

```text
block_id
tx_hash
tx_time
size
weight
version
lock_time
is_coinbase
has_witness
input_count
output_count
input_total
input_total_usd
output_total
output_total_usd
fee
fee_usd
fee_per_kb
fee_per_kb_usd
fee_per_kwu
fee_per_kwu_usd
cdd_total
```

Expected raw table:

```text
btc.raw_transactions
```

Important rule:

```text
Raw transaction data can be very large. The model should not consume raw transaction rows directly.
Spark must aggregate transaction data into time-window features first.
```

Recommended aggregate tables:

```text
btc.features_1h
```

MVP rule:

```text
Raw transaction rows are never model inputs directly.
Spark must aggregate transaction data into the 1h feature table.
```

---

## 6. Kafka Topic Contract

Kafka is used for realtime ingestion and replay benchmark.

## 6.1 Required Topics

```text
btc.market.raw
btc.onchain.raw
btc.features.realtime
btc.predictions
btc.deadletter
```

Optional future topics:

```text
btc.ohlcv.replay
btc.blocks.replay
btc.transactions.replay
btc.model.metrics
btc.pipeline.metrics
```

---

## 6.2 Topic: btc.market.raw

Purpose:

```text
Live or replayed BTC market/OHLCV event stream.
```

Required message format:

```json
{
  "event_time": "2026-06-19T13:00:00Z",
  "ingested_at": "2026-06-19T13:00:02Z",
  "source": "binance_or_csv_replay",
  "asset": "BTC",
  "interval": "1m",
  "open": 62800.0,
  "high": 62920.0,
  "low": 62750.0,
  "close": 62885.0,
  "volume": 120.45
}
```

Required fields:

```text
event_time
ingested_at
source
asset
interval
open
high
low
close
volume
```

---

## 6.3 Topic: btc.onchain.raw

Purpose:

```text
Live Blockchair raw block/transaction envelope used by the MVP on-chain stream.
```

Required message format:

```json
{
  "event_type": "block",
  "ingested_at": 1718809202,
  "data": "{\"id\":1234567,\"time\":\"2026-06-19 13:00:00\", ... }"
}
```

Required fields:

```text
event_type
ingested_at
data
```

Recommended fields:

```text
data should be stringified JSON from Blockchair
event_type should be either block or transaction
ingested_at should be UNIX epoch seconds
```

---

## 6.4 Topic: btc.features.realtime

Purpose:

```text
Realtime features generated by Spark from market and on-chain events.
```

Example message:

```json
{
  "feature_time": "2026-06-19T13:00:00Z",
  "generated_at": "2026-06-19T13:00:10Z",
  "asset": "BTC",
  "horizon": "1h",
  "close": 62885.0,
  "return_1h": 0.0021,
  "volatility_6h": 0.019,
  "ma_7": 62410.5,
  "block_count": 6,
  "tx_count": 792811,
  "fee_total_sum": 43.18,
  "difficulty_avg": 8.21e13
}
```

---

## 6.5 Topic: btc.predictions

Purpose:

```text
Predictions generated by Spark streaming or batch inference.
```

Example message:

```json
{
  "prediction_time": "2026-06-19T13:00:00Z",
  "target_time": "2026-06-19T14:00:00Z",
  "asset": "BTC",
  "horizon": "1h",
  "model_name": "spark_gbt",
  "model_version": "v1.0.0",
  "current_price": 62885.0,
  "predicted_return": 0.003,
  "predicted_price": 63073.655,
  "created_at": "2026-06-19T13:00:15Z"
}
```

---

## 6.6 Topic: btc.deadletter

Purpose:

```text
Store malformed or failed events for debugging.
```

Example message:

```json
{
  "failed_at": "2026-06-19T13:00:00Z",
  "source_topic": "btc.market.raw",
  "error_type": "schema_validation_error",
  "error_message": "missing required field: close",
  "raw_payload": "{...}"
}
```

---

## 7. ClickHouse Database Contract

Database:

```text
btc
```

Core tables:

```text
btc.raw_ohlcv
btc.raw_blocks
btc.raw_transactions
btc.realtime_market_events
btc.features_1h
btc.training_dataset_1h
btc.predictions
btc.prediction_errors
btc.model_metrics
btc.pipeline_metrics
```

`btc.realtime_market_events` is the realtime sink for valid
`btc.market.raw` events. It is intentionally separate from historical tables
and preserves Kafka topic, partition, and offset for traceability and logical
deduplication.

---

## 7.1 Feature Tables

Feature tables are produced by Spark batch jobs.

Current live hourly inputs in ClickHouse:

```text
btc.view_ohlcv_1h
btc.view_blocks_1h
btc.view_tx_1h
```

Current live feature bootstrap table:

```text
btc.features_1h
```

Current live fields:

```text
feature_time
open
high
low
close
volume
return_1h
block_count
difficulty_avg
tx_count
fee_total_sum
fee_avg
created_at
```

Live state note:

```text
`btc.features_1h` exists but is currently empty in ClickHouse.
The table is the bootstrap sink for the MVP 1h pipeline.
```

Canonical MVP 1h schema:

These are lookback features inside one 1h table, not separate 4h/24h tables.

```text
feature_time
open
high
low
close
volume
return_1h
return_4h
return_24h
volatility_6h
volatility_24h
ma_7
ma_14
ma_30
volume_ma_24h
volume_change
high_low_spread
close_open_spread
rsi_14
macd
bollinger_upper
bollinger_lower
atr_14
hour_of_day
day_of_week
is_weekend
block_count
transaction_count_sum
fee_total_sum
fee_total_usd_sum
difficulty_avg
reward_sum
reward_usd_sum
size_avg
weight_avg
tx_count
fee_sum
fee_avg
fee_median
fee_per_kb_avg
input_total_sum
output_total_sum
output_total_usd_sum
tx_size_avg
tx_weight_avg
input_count_avg
output_count_avg
witness_ratio
large_tx_count
large_tx_value_sum
cdd_total_sum
fee_per_volume
tx_per_volume
cdd_per_price
fee_pressure_index
network_activity_change
created_at
```

Feature definitions:

```text
large_tx_count / large_tx_value_sum use a fixed threshold of output_total >= 1_000_000_000 satoshi (10 BTC)
witness_ratio is the share of tx rows where has_witness = 1
fee_pressure_index = fee_total_sum / block_count
network_activity_change = pct change in tx_count versus 24h prior
cdd_per_price = cdd_total_sum / close
fee_per_volume = fee_total_sum / volume
tx_per_volume = tx_count / volume
```

Important rule:

```text
Feature tables must not include future information in feature columns.
Only downstream training tables may add target columns such as target_return_1h.
```

---

## 7.2 Backfill Control and Resume Contract

Historical feature generation runs in monthly batches so a stopped job can be
resumed without losing completed work.

Checkpoint table:

```text
btc.feature_backfill_batches
```

Recommended row contract:

```text
batch_id
feature_grain
batch_start
batch_end
warmup_start
target_table
source_hours
processed_hours
estimated_minutes
status
attempt_count
batch_owner
last_error
started_at
finished_at
updated_at
```

Batch rules:

```text
Batch grain: 1 month
Warmup window: 96 hours
Batch window: [start, end) with end exclusive
Effective backfill window starts at the latest overlapping source month.
Status lifecycle: pending -> running -> succeeded/failed -> resumed if needed
Resume rule: succeeded batches are skipped on the next run
```

Operator workflow:

```text
1. Plan batches from the live hourly range.
2. Insert checkpoint rows for each month.
3. Run one batch at a time with `scripts/run-feature-backfill.sh`.
4. Re-run the reporter to inspect status and remaining batches.
5. Retry only failed or interrupted batches.
```

---

## 7.3 Training Dataset

Training data is derived from `btc.features_1h` by joining the future return
target after the feature timestamp.

Prepared job:

```text
spark_training/build_training_dataset_1h.py
```

Recommended table:

```text
btc.training_dataset_1h
```

Recommended columns:

```text
feature_time
<all btc.features_1h columns except created_at>
target_return_1h
created_at
```

Current contract note:

```text
The training dataset is generated from the feature table plus the next-hour
OHLCV close. The target is target_return_1h = (close[t+1] - close[t]) / close[t].
```

Recommended split rule:

```text
time-based split only
```

Example split:

```text
Train      : 2021–2024
Validation : 2025
Test       : 2026
```

---

## 7.4 Prediction Table

Table:

```text
btc.predictions
```

Current implementation note:

```text
The prediction rows are re-runnable and stored with a deterministic
prediction_id so repeated launches can replace the same logical window.
```

Recommended schema:

```sql
CREATE TABLE IF NOT EXISTS btc.predictions
(
    prediction_id UUID DEFAULT generateUUIDv4(),
    prediction_time DateTime64(3, 'UTC'),
    target_time DateTime64(3, 'UTC'),
    asset LowCardinality(String),
    horizon LowCardinality(String),
    model_name LowCardinality(String),
    model_version String,
    current_price Float64,
    predicted_return Float64,
    predicted_price Float64,
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (horizon, model_name, prediction_time, target_time, prediction_id);
```

---

## 7.3 Prediction Error Table

Table:

```text
btc.prediction_errors
```

Current implementation note:

```text
Error rows are written after the target time has arrived and use the same
prediction identity as the source row.
```

Recommended schema:

```sql
CREATE TABLE IF NOT EXISTS btc.prediction_errors
(
    prediction_id UUID,
    prediction_time DateTime64(3, 'UTC'),
    target_time DateTime64(3, 'UTC'),
    asset LowCardinality(String),
    horizon LowCardinality(String),
    model_name LowCardinality(String),
    model_version String,
    predicted_price Float64,
    actual_price Float64,
    absolute_error Float64,
    squared_error Float64,
    percentage_error Float64,
    predicted_direction Int8,
    actual_direction Int8,
    direction_correct UInt8,
    evaluated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(evaluated_at)
ORDER BY (horizon, model_name, target_time, prediction_time, prediction_id);
```

---

## 7.4 Model Metrics Table

Table:

```text
btc.model_metrics
```

Current implementation note:

```text
The MVP jobs write one summary row per model/version/horizon evaluation window.
```

Recommended fields:

```text
metric_time
model_name
model_version
horizon
mae
rmse
mape
directional_accuracy
sample_size
data_start
data_end
created_at
```

---

## 7.5 Pipeline Metrics Table

Table:

```text
btc.pipeline_metrics
```

Recommended fields:

```text
metric_time
component
metric_name
metric_value
unit
tags
created_at
```

Example metrics:

```text
kafka_input_events_per_sec
spark_processed_rows_per_sec
spark_batch_duration_ms
clickhouse_insert_latency_ms
prediction_latency_ms
modeling_1h_dataset_build_started
modeling_1h_gbt_train_elapsed_seconds
modeling_1h_gbt_train_finished
```

The 1h modeling wrapper writes stage-level rows into this table so Grafana can
show live progress while the dataset build, baseline training, and GBT training
are running.

---

## 8. Spark Contract

Spark is used for:

```text
batch feature engineering
training dataset construction
model training
batch inference
streaming inference
prediction error evaluation
```

Spark master endpoint for the cloud deployment:

```text
spark://<spark-master-private-ip>:7077
```

Resolve `<spark-master-private-ip>` from Terraform rather than hardcoding it:

```bash
terraform -chdir=infra/terraform output -json node_private_ips \
  | jq -r '."spark-master"'
```

The Spark RPC endpoint and web UI are private-VPC services. Do not add public
firewall access for ports `7077`, `8080`, or `8081`.

Expected job categories:

```text
jobs/spark/build_features_1h.py
spark_training/build_training_dataset_1h.py
spark_training/train_baseline.py
spark_training/train_gbt.py
spark_streaming/realtime_inference.py
spark_evaluation/evaluate_predictions.py
```

Operational launch scripts:

```text
scripts/watch-feature-backfill-and-run-modeling.sh
scripts/run-modeling-1h.sh
```

The live Spark master currently runs copied job files from:

```text
/opt/spark/jobs/
```

---

## 9. Model Artifact Contract

Model artifacts should be stored in:

```text
models/
```

Prepared jobs currently default to a local container path under
`/tmp/bitcoin-models/` until a persistent model volume is mounted.

Recommended structure:

```text
models/
├── baseline/
│   └── persistence_1h/
├── spark_gbt/
│   └── btc_return_1h_v1/
└── metadata/
    └── model_registry.json
```

Each model must have metadata:

```json
{
  "model_name": "spark_gbt",
  "model_version": "v1.0.0",
  "target": "target_return_1h",
  "horizon": "1h",
  "trained_at": "2026-06-20T00:00:00Z",
  "training_start": "2021-01-01T00:00:00Z",
  "training_end": "2025-12-31T23:00:00Z",
  "feature_table": "btc.features_1h",
  "feature_columns": [
    "close",
    "volume",
    "return_1h",
    "return_4h",
    "return_24h",
    "block_count",
    "tx_count",
    "fee_total_sum",
    "difficulty_avg",
    "hour_of_day",
    "day_of_week"
  ],
  "metrics": {
    "rmse_return": 0.0,
    "mae_return": 0.0,
    "directional_accuracy": 0.0
  }
}
```

---

## 10. Dashboard Contract

Dashboards read only from ClickHouse.

Grafana should focus on:

```text
BTC current price
predicted vs actual
prediction error
directional accuracy
Kafka/Spark/ClickHouse pipeline metrics
row count and ingestion rate
```

Streamlit should focus on:

```text
model explanation
feature exploration
horizon selection
prediction history
model comparison
data quality overview
```

Dashboards should not connect directly to Kafka.

---

## 11. Access Contract

Infrastructure team must provide:

```text
Kafka bootstrap servers
Spark master URL
ClickHouse host and port
Grafana URL
Streamlit URL
SSH access method
Database credentials
Topic names
Table names
```

Sensitive credentials must not be committed to Git.

Use:

```text
.env
.env.example
```

Commit only:

```text
.env.example
```

Never commit:

```text
.env
API keys
SSH private keys
Database passwords
Cloud provider tokens
```

---

## 12. Development Order

Recommended execution order:

```text
1. Infrastructure team deploys local/cloud baseline.
2. Infrastructure team creates Kafka topics and ClickHouse database.
3. Ingestion team imports historical OHLCV, blocks, and transaction aggregates.
4. Modeling team builds Spark feature tables.
5. Modeling team builds the 1h training dataset.
6. Modeling team trains the 1h baseline and Spark GBT model.
7. Modeling team writes batch predictions to ClickHouse.
8. Modeling team evaluates delayed prediction errors.
9. Infrastructure team exposes Grafana/Streamlit.
10. Ingestion team builds realtime/replay producer.
11. Modeling team builds Spark streaming inference.
12. Prediction results are written to ClickHouse.
13. Dashboard displays actual vs predicted and error.
```

Operational shortcut:

```text
When the 1h feature backfill is still running, use
scripts/watch-feature-backfill-and-run-modeling.sh to poll progress and
launch the modeling pipeline automatically once the checkpoint table reaches
the finished state.
```

Split policy:

```text
The modeling launcher derives train/score windows from btc.features_1h
coverage at runtime. It keeps the newest year as the default score window when
coverage is long enough, and falls back to an 80/20 time split when coverage
is shorter. The end boundary is exclusive, so the latest available hour can be
used safely without adding an artificial extra gap.
```

---

## 13. MVP Boundary

MVP must include:

```text
Kafka running
Spark running
ClickHouse running
Historical OHLCV imported
Historical block data imported
Transaction aggregate features generated
btc.features_1h table created
btc.training_dataset_1h table created
1h baseline model
1h Spark GBT model
btc.predictions table
btc.prediction_errors table
btc.model_metrics table
btc.pipeline_metrics table
basic Grafana or Streamlit dashboard
```

MVP does not require:

```text
4h and 24h feature tables
LSTM/GRU
XGBoost
full cloud hardening
automated retraining
perfect prediction accuracy
```

---

## 14. Open Questions

The team must still decide:

```text
Which live market price API will be used?
Will historical data be imported directly to ClickHouse or stored as Parquet first?
Will raw transaction data be stored fully or only as aggregates?
Which future horizons, if any, are added after the 1h MVP?
Will LSTM be included in final scope or optional appendix?
Will cloud deployment be required for final submission?
```

---

## 15. Final Agreement

The team agrees that:

```text
Historical data is used for training.
Kafka is used for realtime and replay data.
Spark is used for feature engineering, training, and inference.
ClickHouse is the shared storage and dashboard backend.
Dashboards read from ClickHouse.
Model output is predicted return, with predicted price derived from return.
Prediction error is evaluated after the target time arrives.
```
