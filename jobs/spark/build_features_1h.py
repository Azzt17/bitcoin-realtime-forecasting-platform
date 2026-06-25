#!/usr/bin/env python3

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lag, when
from pyspark.sql.window import Window


SOURCE_VIEWS = {
    "ohlcv": "view_ohlcv_1h",
    "blocks": "view_blocks_1h",
    "transactions": "view_tx_1h",
}

TARGET_COLUMNS = [
    "feature_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "return_1h",
    "block_count",
    "difficulty_avg",
    "tx_count",
    "fee_total_sum",
    "fee_avg",
    "created_at",
]

NUMERIC_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "return_1h",
    "block_count",
    "difficulty_avg",
    "tx_count",
    "fee_total_sum",
    "fee_avg",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build btc.features_1h from ClickHouse hourly views."
    )
    parser.add_argument("--clickhouse-host", required=True)
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--clickhouse-database", default="btc")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="")
    parser.add_argument("--target-table", default="features_1h")
    return parser.parse_args()


def read_clickhouse_table(
    spark: SparkSession,
    jdbc_url: str,
    table_name: str,
    user: str,
    password: str,
):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", table_name)
        .option("user", user)
        .option("password", password)
        .load()
    )


def main() -> int:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("btc-build-features-1h")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    jdbc_url = (
        f"jdbc:clickhouse://{args.clickhouse_host}:{args.clickhouse_port}/"
        f"{args.clickhouse_database}"
    )

    print("Reading hourly OHLCV view...")
    df_ohlcv = read_clickhouse_table(
        spark,
        jdbc_url,
        SOURCE_VIEWS["ohlcv"],
        args.clickhouse_user,
        args.clickhouse_password,
    )

    print("Reading hourly block view...")
    df_blocks = read_clickhouse_table(
        spark,
        jdbc_url,
        SOURCE_VIEWS["blocks"],
        args.clickhouse_user,
        args.clickhouse_password,
    )

    print("Reading hourly transaction view...")
    df_tx = read_clickhouse_table(
        spark,
        jdbc_url,
        SOURCE_VIEWS["transactions"],
        args.clickhouse_user,
        args.clickhouse_password,
    )

    print("Joining hourly feature inputs...")
    df_features = (
        df_ohlcv.join(df_blocks, on="feature_time", how="left")
        .join(df_tx, on="feature_time", how="left")
        .orderBy("feature_time")
    )

    print("Computing return_1h...")
    window_spec = Window.orderBy("feature_time")
    df_features = df_features.withColumn("prev_close", lag("close", 1).over(window_spec))
    df_features = df_features.withColumn(
        "return_1h",
        when(col("prev_close").isNull() | (col("prev_close") == 0), 0.0).otherwise(
            (col("close") - col("prev_close")) / col("prev_close")
        ),
    ).drop("prev_close")

    df_final = (
        df_features.withColumn("created_at", current_timestamp())
        .select(*TARGET_COLUMNS)
        .fillna(0, subset=NUMERIC_COLUMNS)
    )

    print(f"Writing into btc.{args.target_table}...")
    (
        df_final.write.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", args.target_table)
        .option("user", args.clickhouse_user)
        .option("password", args.clickhouse_password)
        .mode("append")
        .save()
    )

    print("Feature batch complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
