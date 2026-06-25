CREATE TABLE IF NOT EXISTS btc.feature_backfill_batches
(
    batch_id String,
    feature_grain LowCardinality(String) DEFAULT '1h',
    batch_start DateTime64(3, 'UTC'),
    batch_end DateTime64(3, 'UTC'),
    warmup_start DateTime64(3, 'UTC'),
    target_table LowCardinality(String) DEFAULT 'btc.features_1h',
    source_hours UInt32 DEFAULT 0,
    processed_hours UInt32 DEFAULT 0,
    estimated_minutes Float64 DEFAULT 0,
    status LowCardinality(String) DEFAULT 'pending',
    attempt_count UInt16 DEFAULT 0,
    batch_owner LowCardinality(String) DEFAULT '',
    last_error String DEFAULT '',
    started_at Nullable(DateTime64(3, 'UTC')),
    finished_at Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (feature_grain, batch_start, batch_id);
