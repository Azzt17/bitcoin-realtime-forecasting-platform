CREATE TABLE IF NOT EXISTS btc.predictions_staging
(
    prediction_id String,
    prediction_time DateTime64(3, 'UTC'),
    target_time DateTime64(3, 'UTC'),
    asset LowCardinality(String),
    horizon LowCardinality(String),
    model_name LowCardinality(String),
    model_version String,
    current_price Float64,
    predicted_return Float64,
    predicted_price Float64,
    created_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (horizon, model_name, prediction_time, target_time, prediction_id);
