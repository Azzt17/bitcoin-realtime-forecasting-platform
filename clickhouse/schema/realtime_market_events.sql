CREATE TABLE IF NOT EXISTS btc.realtime_market_events
(
    event_time DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC'),
    processed_at DateTime64(3, 'UTC'),
    source LowCardinality(String),
    asset LowCardinality(String),
    `interval` LowCardinality(String),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    kafka_topic LowCardinality(String),
    kafka_partition UInt32,
    kafka_offset UInt64,
    kafka_timestamp DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (kafka_topic, kafka_partition, kafka_offset);
