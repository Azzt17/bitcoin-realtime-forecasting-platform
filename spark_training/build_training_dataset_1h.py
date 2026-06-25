#!/usr/bin/env python3

"""Build the MVP 1h training dataset from ClickHouse feature rows."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit, when

CLICKHOUSE_JDBC_DRIVER = "com.clickhouse.jdbc.Driver"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build btc.training_dataset_1h from btc.features_1h and future OHLCV closes."
    )
    parser.add_argument("--clickhouse-host", required=True)
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--clickhouse-database", default="btc")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="")
    parser.add_argument("--feature-table", default="features_1h")
    parser.add_argument("--source-view", default="view_ohlcv_1h")
    parser.add_argument("--target-table", default="training_dataset_1h")
    parser.add_argument("--start-time", default="")
    parser.add_argument("--end-time", default="")
    parser.add_argument("--label-hours", type=int, default=1)
    return parser.parse_args()


def parse_utc_timestamp(raw_value: str) -> datetime:
    parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def ch_datetime_literal(value: datetime) -> str:
    value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%d %H:%M:%S")


def build_time_filter(column_name: str, start_time: datetime | None, end_time: datetime | None) -> str:
    clauses = []
    if start_time is not None:
        clauses.append(
            f"{column_name} >= toDateTime64('{ch_datetime_literal(start_time)}', 3, 'UTC')"
        )
    if end_time is not None:
        clauses.append(
            f"{column_name} < toDateTime64('{ch_datetime_literal(end_time)}', 3, 'UTC')"
        )
    if not clauses:
        return ""
    return "WHERE " + " AND ".join(clauses)


def read_clickhouse_relation(
    spark: SparkSession,
    jdbc_url: str,
    relation_sql: str,
    user: str,
    password: str,
):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", CLICKHOUSE_JDBC_DRIVER)
        .option("dbtable", relation_sql.strip())
        .option("user", user)
        .option("password", password)
        .load()
    )


def main() -> int:
    args = parse_args()
    if args.label_hours < 1:
        raise ValueError("label-hours must be positive")

    start_time = parse_utc_timestamp(args.start_time) if args.start_time else None
    end_time = parse_utc_timestamp(args.end_time) if args.end_time else None
    feature_end_time = end_time
    future_start_time = start_time + timedelta(hours=args.label_hours) if start_time else None
    future_end_time = (
        end_time + timedelta(hours=args.label_hours) if end_time is not None else None
    )

    spark = (
        SparkSession.builder.appName("btc-build-training-dataset-1h")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    jdbc_url = (
        f"jdbc:clickhouse://{args.clickhouse_host}:{args.clickhouse_port}/"
        f"{args.clickhouse_database}"
    )

    feature_where = build_time_filter("feature_time", start_time, feature_end_time)
    future_where = build_time_filter("feature_time", future_start_time, future_end_time)

    print("Reading feature rows...")
    features = read_clickhouse_relation(
        spark,
        jdbc_url,
        f"(SELECT * FROM btc.{args.feature_table} {feature_where}) AS features_1h",
        args.clickhouse_user,
        args.clickhouse_password,
    )

    print("Reading future OHLCV labels...")
    future = read_clickhouse_relation(
        spark,
        jdbc_url,
        f"(SELECT feature_time, close FROM btc.{args.source_view} {future_where}) AS ohlcv_future",
        args.clickhouse_user,
        args.clickhouse_password,
    )

    feature_output_columns = [column for column in features.columns if column != "created_at"]
    features = features.withColumn(
        "target_time",
        expr(f"feature_time + INTERVAL {args.label_hours} HOUR"),
    )

    joined = (
        features.alias("f")
        .join(
            future.alias("future"),
            col("future.feature_time") == col("f.target_time"),
            "inner",
        )
        .select(
            *[col(f"f.{column}") for column in feature_output_columns],
            when(col("f.close") == 0, lit(0.0)).otherwise(
                (col("future.close") - col("f.close")) / col("f.close")
            ).alias("target_return_1h"),
            current_timestamp().alias("created_at"),
        )
    )

    print(f"Writing into btc.{args.target_table}...")
    (
        joined.write.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", CLICKHOUSE_JDBC_DRIVER)
        .option("dbtable", args.target_table)
        .option("user", args.clickhouse_user)
        .option("password", args.clickhouse_password)
        .mode("append")
        .save()
    )

    print("Training dataset batch complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
