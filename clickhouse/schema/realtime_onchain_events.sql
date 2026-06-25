CREATE TABLE IF NOT EXISTS btc.realtime_onchain_events
(
    event_time DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC'),
    event_type LowCardinality(String),
    id UInt64,
    hash String,
    
    -- Block fields (nullable for transactions)
    size Nullable(Float64),
    weight Nullable(Float64),
    transaction_count Nullable(UInt64),
    difficulty Nullable(Float64),
    reward Nullable(Float64),
    reward_usd Nullable(Float64),
    
    -- Transaction fields (nullable for blocks)
    fee Nullable(Float64),
    fee_usd Nullable(Float64),
    fee_per_kb Nullable(Float64),
    input_total Nullable(Float64),
    output_total Nullable(Float64),
    output_total_usd Nullable(Float64),
    input_count Nullable(UInt64),
    output_count Nullable(UInt64),
    has_witness Nullable(UInt8),
    cdd_total Nullable(Float64),

    kafka_topic LowCardinality(String),
    kafka_partition UInt32,
    kafka_offset UInt64,
    kafka_timestamp DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (kafka_topic, kafka_partition, kafka_offset);
