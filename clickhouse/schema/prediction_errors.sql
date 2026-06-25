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
