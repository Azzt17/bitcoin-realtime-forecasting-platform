CREATE TABLE IF NOT EXISTS btc.model_metrics
(
    metric_time DateTime64(3, 'UTC'),
    model_name LowCardinality(String),
    model_version String,
    horizon LowCardinality(String),
    mae Float64,
    rmse Float64,
    mape Float64,
    directional_accuracy Float64,
    sample_size UInt64,
    data_start DateTime64(3, 'UTC'),
    data_end DateTime64(3, 'UTC'),
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (horizon, model_name, metric_time);
