#!/usr/bin/env python3
import os
import sys
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession


ANALYTICS_PRIVATE_IP = os.environ["ANALYTICS_PRIVATE_IP"]


def clickhouse_query(sql: str) -> str:
    url = (
        f"http://{ANALYTICS_PRIVATE_IP}:8123/?"
        + urllib.parse.urlencode({"query": sql})
    )
    with urllib.request.urlopen(url, timeout=15) as response:
        return response.read().decode("utf-8").strip()


def fetch_from_executor(_):
    raw_ohlcv = clickhouse_query("SELECT count() FROM btc.raw_ohlcv")
    raw_blocks = clickhouse_query("SELECT count() FROM btc.raw_blocks")
    raw_transactions = clickhouse_query("SELECT count() FROM btc.raw_transactions")
    yield "\t".join([raw_ohlcv, raw_blocks, raw_transactions])


def main() -> int:
    spark = (
        SparkSession.builder.appName("btc-spark-clickhouse-read-check")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    rows = spark.sparkContext.parallelize([1], 1).mapPartitions(fetch_from_executor).collect()
    if len(rows) != 1:
        raise RuntimeError(f"Expected one result row, got {len(rows)}")

    raw_ohlcv, raw_blocks, raw_transactions = rows[0].split("\t")
    print(f"raw_ohlcv={raw_ohlcv}")
    print(f"raw_blocks={raw_blocks}")
    print(f"raw_transactions={raw_transactions}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
