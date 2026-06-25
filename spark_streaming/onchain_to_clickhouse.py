#!/usr/bin/env python3

import argparse
import json
import time
import urllib.request
import urllib.parse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, from_json, lit, from_unixtime, when, coalesce
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType, TimestampType
)

# Schema for the JSON data published by stream_blockchair_to_kafka.py
ONCHAIN_SCHEMA = StructType([
    StructField("event_type", StringType(), False),
    StructField("ingested_at", LongType(), False),
    StructField("data", StringType(), False) # We will parse this nested JSON depending on event_type
])

BLOCK_SCHEMA = StructType([
    StructField("time", TimestampType(), True),
    StructField("id", LongType(), True),
    StructField("hash", StringType(), True),
    StructField("size", DoubleType(), True),
    StructField("weight", DoubleType(), True),
    StructField("transaction_count", LongType(), True),
    StructField("difficulty", DoubleType(), True),
    StructField("reward", DoubleType(), True),
    StructField("reward_usd", DoubleType(), True)
])

TX_SCHEMA = StructType([
    StructField("time", TimestampType(), True),
    StructField("id", LongType(), True),
    StructField("hash", StringType(), True),
    StructField("size", DoubleType(), True),
    StructField("weight", DoubleType(), True),
    StructField("fee", DoubleType(), True),
    StructField("fee_usd", DoubleType(), True),
    StructField("fee_per_kb", DoubleType(), True),
    StructField("input_total", DoubleType(), True),
    StructField("output_total", DoubleType(), True),
    StructField("output_total_usd", DoubleType(), True),
    StructField("input_count", LongType(), True),
    StructField("output_count", LongType(), True),
    StructField("has_witness", StringType(), True),
    StructField("cdd_total", DoubleType(), True)
])

INSERT_COLUMNS = (
    "event_time, ingested_at, event_type, id, hash, "
    "size, weight, transaction_count, difficulty, reward, reward_usd, "
    "fee, fee_usd, fee_per_kb, input_total, output_total, output_total_usd, "
    "input_count, output_count, has_witness, cdd_total, "
    "kafka_topic, kafka_partition, kafka_offset, kafka_timestamp"
)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-bootstrap", required=True)
    parser.add_argument("--topic", default="btc.onchain.raw")
    parser.add_argument("--clickhouse-url", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=500)
    return parser.parse_args()

def clickhouse_insert_url(base_url: str) -> str:
    query = f"INSERT INTO btc.realtime_onchain_events ({INSERT_COLUMNS}) FORMAT JSONEachRow"
    return f"{base_url.rstrip('/')}?{urllib.parse.urlencode({'query': query})}"


def bool_to_int(column):
    return when(column.cast("boolean") == True, 1).otherwise(0)

def write_partition(rows, insert_url: str, batch_size: int):
    batch = []
    for row in rows:
        batch.append(json.dumps(row.asDict(recursive=True), separators=(",", ":")))
        if len(batch) >= batch_size:
            post_rows(insert_url, ("\n".join(batch) + "\n").encode("utf-8"))
            batch.clear()
    if batch:
        post_rows(insert_url, ("\n".join(batch) + "\n").encode("utf-8"))

def post_rows(insert_url: str, payload: bytes):
    request = urllib.request.Request(insert_url, data=payload, headers={"Content-Type": "application/x-ndjson"}, method="POST")
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                if response.status == 200: return
        except Exception:
            if attempt == 3: raise
            time.sleep(attempt * 2)

def main():
    args = parse_args()
    spark = SparkSession.builder.appName("btc-onchain-to-clickhouse").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    insert_url = clickhouse_insert_url(args.clickhouse_url)

    kafka_stream = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load())

    parsed = kafka_stream.select(
        from_json(col("value").cast("string"), ONCHAIN_SCHEMA).alias("wrapper"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp")
    )

    events = parsed.select(
        col("wrapper.event_type").alias("event_type"),
        from_unixtime(col("wrapper.ingested_at")).cast("timestamp").alias("ingested_at"),
        col("wrapper.data").alias("raw_data"),
        col("kafka_topic"), col("kafka_partition"), col("kafka_offset"), col("kafka_timestamp")
    ).filter(col("raw_data").isNotNull())

    blocks = events.filter(col("event_type") == "block").withColumn("data", from_json("raw_data", BLOCK_SCHEMA))
    txs = events.filter(col("event_type") == "transaction").withColumn("data", from_json("raw_data", TX_SCHEMA))

    def format_ts(c): return date_format(c, "yyyy-MM-dd HH:mm:ss.SSS")

    def event_time_or_ingested(data_time_col, ingested_col):
        return coalesce(data_time_col.cast("timestamp"), ingested_col)

    blocks_formatted = blocks.select(
        format_ts(event_time_or_ingested(col("data.time"), col("ingested_at"))).alias("event_time"),
        format_ts(col("ingested_at")).alias("ingested_at"),
        col("event_type"), col("data.id"), col("data.hash"),
        col("data.size"), col("data.weight"), col("data.transaction_count"), col("data.difficulty"), col("data.reward"), col("data.reward_usd"),
        lit(None).cast("double").alias("fee"), lit(None).cast("double").alias("fee_usd"), lit(None).cast("double").alias("fee_per_kb"),
        lit(None).cast("double").alias("input_total"), lit(None).cast("double").alias("output_total"), lit(None).cast("double").alias("output_total_usd"),
        lit(None).cast("long").alias("input_count"), lit(None).cast("long").alias("output_count"), lit(None).cast("int").alias("has_witness"), lit(None).cast("double").alias("cdd_total"),
        col("kafka_topic"), col("kafka_partition"), col("kafka_offset"), format_ts(col("kafka_timestamp")).alias("kafka_timestamp")
    )

    txs_formatted = txs.select(
        format_ts(event_time_or_ingested(col("data.time"), col("ingested_at"))).alias("event_time"),
        format_ts(col("ingested_at")).alias("ingested_at"),
        col("event_type"), col("data.id"), col("data.hash"),
        col("data.size"), col("data.weight"), lit(None).cast("long").alias("transaction_count"), lit(None).cast("double").alias("difficulty"), lit(None).cast("double").alias("reward"), lit(None).cast("double").alias("reward_usd"),
        col("data.fee"), col("data.fee_usd"), col("data.fee_per_kb"),
        col("data.input_total"), col("data.output_total"), col("data.output_total_usd"),
        col("data.input_count"), col("data.output_count"),
        bool_to_int(col("data.has_witness")).alias("has_witness"),
        col("data.cdd_total"),
        col("kafka_topic"), col("kafka_partition"), col("kafka_offset"), format_ts(col("kafka_timestamp")).alias("kafka_timestamp")
    )

    unified = blocks_formatted.unionByName(txs_formatted)

    def write_batch(batch, batch_id):
        print(f"Processing onchain batch {batch_id}", flush=True)
        batch.foreachPartition(lambda rows: write_partition(rows, insert_url, args.batch_size))

    query = (unified.writeStream.foreachBatch(write_batch)
             .option("checkpointLocation", args.checkpoint)
             .trigger(processingTime=f"{args.trigger_seconds} seconds")
             .start())
    query.awaitTermination()

if __name__ == "__main__":
    main()
