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
    event_time DateTime64(3, 'UTC'),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
ORDER BY event_time;
```

Data quality requirements:

```text
No duplicate event_time
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
height
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
    height UInt64,
    hash String,
    event_time DateTime64(3, 'UTC'),
    transaction_count UInt32,
    input_count UInt32,
    output_count UInt32,
    input_total Float64,
    input_total_usd Float64,
    output_total Float64,
    output_total_usd Float64,
    fee_total Float64,
    fee_total_usd Float64,
    cdd_total Float64,
    generation Float64,
    generation_usd Float64,
    reward Float64,
    reward_usd Float64,
    difficulty Float64,
    size UInt64,
    weight UInt64,
    guessed_miner String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
ORDER BY (event_time, height);
```

Data quality requirements:

```text
height should be unique
hash should be unique
event_time must be valid UTC
numeric values must be non-negative where applicable
difficulty must be positive
```

---

## 5.3 Historical Transaction Data

Expected fields may include:

```text
block_id
hash
time
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
btc.tx_features_1h
btc.tx_features_4h
btc.tx_features_24h
```

Example aggregate fields:

```text
bucket_time
tx_count
fee_sum
fee_avg
fee_median
fee_per_kb_avg
input_total_sum
output_total_sum
input_total_usd_sum
output_total_usd_sum
tx_size_avg
tx_weight_avg
input_count_avg
output_count_avg
coinbase_tx_count
witness_tx_ratio
cdd_total_sum
large_tx_count
large_tx_value_usd_sum
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
Live Blockchair on-chain stats or replayed on-chain events.
```

Required message format:

```json
{
  "event_time": "2026-06-19T13:00:00Z",
  "ingested_at": "2026-06-19T13:00:02Z",
  "source": "blockchair",
  "asset": "BTC",
  "market_price_usd": 62885.0,
  "transactions_24h": 762752,
  "volume_24h_btc": 727991.26046252,
  "mempool_transactions": 114075,
  "mempool_size_mb": 94.979198,
  "mempool_tps": 3.3667,
  "avg_fee_usd_24h": 0.2696,
  "median_fee_usd_24h": 0.088,
  "suggested_fee_sat_per_byte": 2,
  "blocks_24h": 153,
  "difficulty": 124932866006548.2,
  "hashrate_24h_eh": 951.3875,
  "hodling_addresses": 59214523,
  "request_cost": 1
}
```

Required fields:

```text
event_time
ingested_at
source
asset
request_cost
```

Recommended fields:

```text
market_price_usd
transactions_24h
volume_24h_btc
mempool_transactions
mempool_size_mb
mempool_tps
avg_fee_usd_24h
median_fee_usd_24h
suggested_fee_sat_per_byte
blocks_24h
difficulty
hashrate_24h_eh
hodling_addresses
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
  "volatility_24h": 0.041,
  "ma_24h": 62100.5,
  "volume_ma_24h": 99.2,
  "transactions_24h": 762752,
  "mempool_transactions": 114075,
  "avg_fee_usd_24h": 0.2696
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
btc.features_1h
btc.features_4h
btc.features_24h
btc.predictions
btc.prediction_errors
btc.model_metrics
btc.pipeline_metrics
```

---

## 7.1 Feature Tables

Feature tables are produced by Spark batch jobs.

Example table:

```text
btc.features_1h
```

Expected fields:

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
volatility_24h
ma_24h
volume_ma_24h
block_count
block_tx_count_sum
block_fee_total_sum
block_fee_total_usd_sum
difficulty_avg
tx_count
tx_fee_sum
tx_fee_avg
tx_fee_median
tx_cdd_total_sum
target_return_1h
created_at
```

Important rule:

```text
Feature tables must not include future information in feature columns.
Only target columns may refer to future values.
```

---

## 7.2 Prediction Table

Table:

```text
btc.predictions
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
ENGINE = MergeTree()
ORDER BY (horizon, model_name, prediction_time);
```

---

## 7.3 Prediction Error Table

Table:

```text
btc.prediction_errors
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
ENGINE = MergeTree()
ORDER BY (horizon, model_name, target_time);
```

---

## 7.4 Model Metrics Table

Table:

```text
btc.model_metrics
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
```

---

## 8. Spark Contract

Spark is used for:

```text
batch feature engineering
model training
batch inference
streaming inference
prediction error evaluation
```

Spark master endpoint:

```text
spark://spark-master:7077
```

Expected job categories:

```text
spark_batch/build_features_1h.py
spark_batch/build_features_4h.py
spark_batch/build_features_24h.py
spark_training/train_baseline.py
spark_training/train_gbt.py
spark_streaming/realtime_inference.py
spark_evaluation/evaluate_predictions.py
```

---

## 9. Model Artifact Contract

Model artifacts should be stored in:

```text
models/
```

Recommended structure:

```text
models/
├── baseline/
│   └── persistence_1h/
├── spark_gbt/
│   ├── btc_return_1h_v1/
│   ├── btc_return_4h_v1/
│   └── btc_return_24h_v1/
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
    "volatility_24h",
    "ma_24h",
    "block_tx_count_sum",
    "tx_fee_avg"
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
5. Modeling team trains 1h baseline and Spark GBT model.
6. Infrastructure team exposes Grafana/Streamlit.
7. Ingestion team builds realtime/replay producer.
8. Modeling team builds Spark streaming inference.
9. Prediction results are written to ClickHouse.
10. Dashboard displays actual vs predicted and error.
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
1h baseline model
1h Spark GBT model
btc.predictions table
btc.prediction_errors table
basic Grafana or Streamlit dashboard
```

MVP does not require:

```text
all 3 horizons complete
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
What is the first official model horizon: 1h only or 1h + 4h?
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
