#!/usr/bin/env python3

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    from_json,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


MARKET_SCHEMA = StructType(
    [
        StructField("event_time", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("source", StringType(), False),
        StructField("asset", StringType(), False),
        StructField("interval", StringType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", DoubleType(), False),
    ]
)

INSERT_COLUMNS = (
    "event_time, ingested_at, processed_at, source, asset, `interval`, "
    "open, high, low, close, volume, kafka_topic, kafka_partition, "
    "kafka_offset, kafka_timestamp"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream btc.market.raw events from Kafka to ClickHouse."
    )
    parser.add_argument("--kafka-bootstrap", required=True)
    parser.add_argument("--topic", default="btc.market.raw")
    parser.add_argument("--clickhouse-url", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--trigger-seconds", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=500)
    return parser.parse_args()


def clickhouse_insert_url(base_url: str) -> str:
    parsed = urllib.parse.urlparse(base_url)
    if parsed.scheme != "http" or not parsed.hostname:
        raise ValueError("ClickHouse URL must be an HTTP URL with a hostname")

    query = (
        f"INSERT INTO btc.realtime_market_events ({INSERT_COLUMNS}) "
        "FORMAT JSONEachRow"
    )
    return f"{base_url.rstrip('/')}?{urllib.parse.urlencode({'query': query})}"


def post_rows(insert_url: str, payload: bytes) -> None:
    request = urllib.request.Request(
        insert_url,
        data=payload,
        headers={"Content-Type": "application/x-ndjson"},
        method="POST",
    )

    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                if response.status != 200:
                    raise RuntimeError(
                        f"ClickHouse returned HTTP status {response.status}"
                    )
            return
        except (urllib.error.URLError, TimeoutError):
            if attempt == 3:
                raise
            time.sleep(attempt * 2)


def write_partition(rows: Iterable[Row], insert_url: str, batch_size: int) -> None:
    batch: list[str] = []

    for row in rows:
        batch.append(json.dumps(row.asDict(recursive=True), separators=(",", ":")))
        if len(batch) >= batch_size:
            post_rows(insert_url, ("\n".join(batch) + "\n").encode("utf-8"))
            batch.clear()

    if batch:
        post_rows(insert_url, ("\n".join(batch) + "\n").encode("utf-8"))


def build_events(spark: SparkSession, kafka_bootstrap: str, topic: str) -> DataFrame:
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = kafka.select(
        from_json(col("value").cast("string"), MARKET_SCHEMA).alias("event"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    )

    valid = parsed.filter(
        col("event").isNotNull()
        & col("event.event_time").isNotNull()
        & col("event.ingested_at").isNotNull()
        & col("event.source").isNotNull()
        & col("event.asset").isNotNull()
        & col("event.interval").isNotNull()
        & col("event.open").isNotNull()
        & col("event.high").isNotNull()
        & col("event.low").isNotNull()
        & col("event.close").isNotNull()
        & col("event.volume").isNotNull()
        & (col("event.high") >= col("event.low"))
        & (col("event.high") >= col("event.open"))
        & (col("event.high") >= col("event.close"))
        & (col("event.low") <= col("event.open"))
        & (col("event.low") <= col("event.close"))
        & (col("event.volume") >= 0)
    )

    timestamp_format = "yyyy-MM-dd HH:mm:ss.SSS"
    return valid.select(
        date_format(col("event.event_time"), timestamp_format).alias("event_time"),
        date_format(col("event.ingested_at"), timestamp_format).alias("ingested_at"),
        date_format(current_timestamp(), timestamp_format).alias("processed_at"),
        col("event.source").alias("source"),
        col("event.asset").alias("asset"),
        col("event.interval").alias("interval"),
        col("event.open").alias("open"),
        col("event.high").alias("high"),
        col("event.low").alias("low"),
        col("event.close").alias("close"),
        col("event.volume").alias("volume"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        date_format(col("kafka_timestamp"), timestamp_format).alias(
            "kafka_timestamp"
        ),
    )


def main() -> None:
    args = parse_args()
    if args.trigger_seconds < 1 or args.batch_size < 1:
        raise ValueError("Trigger seconds and batch size must be positive")

    insert_url = clickhouse_insert_url(args.clickhouse_url)
    spark = (
        SparkSession.builder.appName("btc-market-to-clickhouse")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    events = build_events(spark, args.kafka_bootstrap, args.topic)

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        print(f"Processing batch {batch_id}", flush=True)
        batch.foreachPartition(
            lambda rows: write_partition(rows, insert_url, args.batch_size)
        )

    query = (
        events.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )
    print(f"Streaming query started: {query.id}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
