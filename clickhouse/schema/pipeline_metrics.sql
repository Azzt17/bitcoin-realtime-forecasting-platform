CREATE TABLE IF NOT EXISTS btc.pipeline_metrics
(
    metric_time DateTime64(3, 'UTC'),
    component LowCardinality(String),
    metric_name LowCardinality(String),
    metric_value Float64,
    unit LowCardinality(String),
    tags String,
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (component, metric_name, metric_time);
