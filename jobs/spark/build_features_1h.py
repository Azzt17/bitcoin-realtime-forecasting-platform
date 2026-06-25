#!/usr/bin/env python3

"""Build the canonical btc.features_1h table from live ClickHouse data."""

import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    col,
    collect_list,
    current_timestamp,
    dayofweek,
    expr,
    greatest,
    hour,
    lag,
    lit,
    stddev_samp,
    when,
)
from pyspark.sql.window import Window


WARMUP_HOURS = 96
CLICKHOUSE_JDBC_DRIVER = "com.clickhouse.jdbc.Driver"

TARGET_COLUMNS = [
    "feature_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "return_1h",
    "return_4h",
    "return_24h",
    "volatility_6h",
    "volatility_24h",
    "ma_7",
    "ma_14",
    "ma_30",
    "volume_ma_24h",
    "volume_change",
    "high_low_spread",
    "close_open_spread",
    "rsi_14",
    "macd",
    "bollinger_upper",
    "bollinger_lower",
    "atr_14",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "block_count",
    "transaction_count_sum",
    "fee_total_sum",
    "fee_total_usd_sum",
    "difficulty_avg",
    "reward_sum",
    "reward_usd_sum",
    "size_avg",
    "weight_avg",
    "tx_count",
    "fee_sum",
    "fee_avg",
    "fee_median",
    "fee_per_kb_avg",
    "input_total_sum",
    "output_total_sum",
    "output_total_usd_sum",
    "tx_size_avg",
    "tx_weight_avg",
    "input_count_avg",
    "output_count_avg",
    "witness_ratio",
    "large_tx_count",
    "large_tx_value_sum",
    "cdd_total_sum",
    "fee_per_volume",
    "tx_per_volume",
    "cdd_per_price",
    "fee_pressure_index",
    "network_activity_change",
    "created_at",
]

NUMERIC_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "return_1h",
    "return_4h",
    "return_24h",
    "volatility_6h",
    "volatility_24h",
    "ma_7",
    "ma_14",
    "ma_30",
    "volume_ma_24h",
    "volume_change",
    "high_low_spread",
    "close_open_spread",
    "rsi_14",
    "macd",
    "bollinger_upper",
    "bollinger_lower",
    "atr_14",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "block_count",
    "transaction_count_sum",
    "fee_total_sum",
    "fee_total_usd_sum",
    "difficulty_avg",
    "reward_sum",
    "reward_usd_sum",
    "size_avg",
    "weight_avg",
    "tx_count",
    "fee_sum",
    "fee_avg",
    "fee_median",
    "fee_per_kb_avg",
    "input_total_sum",
    "output_total_sum",
    "output_total_usd_sum",
    "tx_size_avg",
    "tx_weight_avg",
    "input_count_avg",
    "output_count_avg",
    "witness_ratio",
    "large_tx_count",
    "large_tx_value_sum",
    "cdd_total_sum",
    "fee_per_volume",
    "tx_per_volume",
    "cdd_per_price",
    "fee_pressure_index",
    "network_activity_change",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the canonical btc.features_1h feature table from ClickHouse."
    )
    parser.add_argument("--clickhouse-host", required=True)
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--clickhouse-database", default="btc")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="")
    parser.add_argument("--target-table", default="features_1h")
    parser.add_argument("--start-time", default="")
    parser.add_argument("--end-time", default="")
    parser.add_argument("--warmup-hours", type=int, default=WARMUP_HOURS)
    return parser.parse_args()


def read_clickhouse_relation(
    spark: SparkSession,
    jdbc_url: str,
    relation_sql: str,
    user: str,
    password: str,
) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", CLICKHOUSE_JDBC_DRIVER)
        .option("dbtable", relation_sql.strip())
        .option("user", user)
        .option("password", password)
        .load()
    )


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


def build_ohlcv_relation(start_time: datetime | None, end_time: datetime | None) -> str:
    where_clause = build_time_filter("feature_time", start_time, end_time)
    return f"(SELECT * FROM btc.view_ohlcv_1h {where_clause}) AS ohlcv_1h"


def build_blocks_relation(start_time: datetime | None, end_time: datetime | None) -> str:
    where_clause = build_time_filter("time", start_time, end_time)
    return f"""
    (
        SELECT
            toStartOfHour(time) AS feature_time,
            count() AS block_count,
            sum(transaction_count) AS transaction_count_sum,
            sum(fee_total) AS fee_total_sum,
            sum(fee_total_usd) AS fee_total_usd_sum,
            avg(difficulty) AS difficulty_avg,
            sum(reward) AS reward_sum,
            sum(reward_usd) AS reward_usd_sum,
            avg(size) AS size_avg,
            avg(weight) AS weight_avg
        FROM btc.raw_blocks
        {where_clause}
        GROUP BY feature_time
    ) AS blocks_1h
    """


def build_transactions_relation(start_time: datetime | None, end_time: datetime | None) -> str:
    where_clause = build_time_filter("tx_time", start_time, end_time)
    return f"""
    (
        SELECT
            toStartOfHour(tx_time) AS feature_time,
            count() AS tx_count,
            sum(fee) AS fee_sum,
            avg(fee) AS fee_avg,
            quantileTDigest(0.5)(fee) AS fee_median,
            avg(fee_per_kb) AS fee_per_kb_avg,
            sum(input_total) AS input_total_sum,
            sum(output_total) AS output_total_sum,
            sum(output_total_usd) AS output_total_usd_sum,
            avg(size) AS tx_size_avg,
            avg(weight) AS tx_weight_avg,
            avg(input_count) AS input_count_avg,
            avg(output_count) AS output_count_avg,
            avg(CAST(has_witness AS Float64)) AS witness_ratio,
            countIf(output_total >= 1000000000) AS large_tx_count,
            sumIf(output_total, output_total >= 1000000000) AS large_tx_value_sum,
            sum(cdd_total) AS cdd_total_sum
        FROM btc.raw_transactions
        {where_clause}
        GROUP BY feature_time
    ) AS tx_1h
    """


def pct_change(current_col: str, previous_col: str):
    previous = col(previous_col)
    return when(previous.isNull() | (previous == 0), lit(0.0)).otherwise(
        (col(current_col) - previous) / previous
    )


def add_ema(df: DataFrame, source_column: str, span: int, output_column: str) -> DataFrame:
    alpha = 2.0 / (span + 1.0)
    window = Window.orderBy("feature_time").rowsBetween(-(span - 1), 0)
    array_column = f"_{output_column}_window"
    df = df.withColumn(array_column, collect_list(col(source_column)).over(window))
    df = df.withColumn(
        output_column,
        expr(
            f"aggregate({array_column}, cast(null as double), "
            f"(acc, x) -> IF(acc IS NULL, x, x * {alpha} + acc * {1.0 - alpha}), "
            "acc -> acc)"
        ),
    )
    return df.drop(array_column)


def main() -> int:
    args = parse_args()

    start_time = parse_utc_timestamp(args.start_time) if args.start_time else None
    end_time = parse_utc_timestamp(args.end_time) if args.end_time else None
    read_start_time = (
        start_time - timedelta(hours=args.warmup_hours) if start_time is not None else None
    )

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
    df_ohlcv = read_clickhouse_relation(
        spark,
        jdbc_url,
        build_ohlcv_relation(read_start_time, end_time),
        args.clickhouse_user,
        args.clickhouse_password,
    )

    print("Reading hourly block aggregate...")
    df_blocks = read_clickhouse_relation(
        spark,
        jdbc_url,
        build_blocks_relation(read_start_time, end_time),
        args.clickhouse_user,
        args.clickhouse_password,
    )

    print("Reading hourly transaction aggregate...")
    df_tx = read_clickhouse_relation(
        spark,
        jdbc_url,
        build_transactions_relation(read_start_time, end_time),
        args.clickhouse_user,
        args.clickhouse_password,
    )

    print("Joining hourly feature inputs...")
    df_features = (
        df_ohlcv.join(df_blocks, on="feature_time", how="left")
        .join(df_tx, on="feature_time", how="left")
        .orderBy("feature_time")
    )

    df_features = df_features.fillna(
        0,
        subset=[
            "block_count",
            "transaction_count_sum",
            "fee_total_sum",
            "fee_total_usd_sum",
            "difficulty_avg",
            "reward_sum",
            "reward_usd_sum",
            "size_avg",
            "weight_avg",
            "tx_count",
            "fee_sum",
            "fee_avg",
            "fee_median",
            "fee_per_kb_avg",
            "input_total_sum",
            "output_total_sum",
            "output_total_usd_sum",
            "tx_size_avg",
            "tx_weight_avg",
            "input_count_avg",
            "output_count_avg",
            "witness_ratio",
            "large_tx_count",
            "large_tx_value_sum",
            "cdd_total_sum",
        ],
    )

    base_order = Window.orderBy("feature_time")
    rolling_6 = base_order.rowsBetween(-5, 0)
    rolling_7 = base_order.rowsBetween(-6, 0)
    rolling_14 = base_order.rowsBetween(-13, 0)
    rolling_20 = base_order.rowsBetween(-19, 0)
    rolling_24 = base_order.rowsBetween(-23, 0)
    rolling_30 = base_order.rowsBetween(-29, 0)

    df_features = df_features.withColumn("prev_close_1h", lag("close", 1).over(base_order))
    df_features = df_features.withColumn("prev_close_4h", lag("close", 4).over(base_order))
    df_features = df_features.withColumn("prev_close_24h", lag("close", 24).over(base_order))
    df_features = df_features.withColumn("prev_volume_1h", lag("volume", 1).over(base_order))
    df_features = df_features.withColumn("prev_tx_count_24h", lag("tx_count", 24).over(base_order))
    df_features = df_features.withColumn(
        "return_1h",
        pct_change("close", "prev_close_1h"),
    )
    df_features = df_features.withColumn(
        "return_4h",
        pct_change("close", "prev_close_4h"),
    )
    df_features = df_features.withColumn(
        "return_24h",
        pct_change("close", "prev_close_24h"),
    )
    df_features = df_features.withColumn(
        "volume_change",
        pct_change("volume", "prev_volume_1h"),
    )
    df_features = df_features.withColumn(
        "volatility_6h",
        stddev_samp("return_1h").over(rolling_6),
    )
    df_features = df_features.withColumn(
        "volatility_24h",
        stddev_samp("return_1h").over(rolling_24),
    )
    df_features = df_features.withColumn("ma_7", avg("close").over(rolling_7))
    df_features = df_features.withColumn("ma_14", avg("close").over(rolling_14))
    df_features = df_features.withColumn("ma_30", avg("close").over(rolling_30))
    df_features = df_features.withColumn("volume_ma_24h", avg("volume").over(rolling_24))
    df_features = df_features.withColumn("high_low_spread", col("high") - col("low"))
    df_features = df_features.withColumn("close_open_spread", col("close") - col("open"))

    price_change = col("close") - col("prev_close_1h")
    df_features = df_features.withColumn("price_change_1h", price_change)
    df_features = df_features.withColumn(
        "gain_1h",
        when(col("price_change_1h") > 0, col("price_change_1h")).otherwise(lit(0.0)),
    )
    df_features = df_features.withColumn(
        "loss_1h",
        when(col("price_change_1h") < 0, spark_abs(col("price_change_1h"))).otherwise(lit(0.0)),
    )
    df_features = df_features.withColumn("avg_gain_14", avg("gain_1h").over(rolling_14))
    df_features = df_features.withColumn("avg_loss_14", avg("loss_1h").over(rolling_14))
    df_features = df_features.withColumn(
        "rsi_14",
        when(
            col("avg_gain_14").isNull() & col("avg_loss_14").isNull(),
            lit(50.0),
        )
        .when((col("avg_gain_14") == 0) & (col("avg_loss_14") == 0), lit(50.0))
        .when(col("avg_loss_14") == 0, lit(100.0))
        .when(col("avg_gain_14") == 0, lit(0.0))
        .otherwise(100.0 - (100.0 / (1.0 + (col("avg_gain_14") / col("avg_loss_14"))))),
    )

    df_features = df_features.withColumn(
        "true_range",
        greatest(
            col("high") - col("low"),
            spark_abs(col("high") - col("prev_close_1h")),
            spark_abs(col("low") - col("prev_close_1h")),
        ),
    )
    df_features = df_features.withColumn("atr_14", avg("true_range").over(rolling_14))

    df_features = df_features.withColumn("bollinger_ma_20", avg("close").over(rolling_20))
    df_features = df_features.withColumn("bollinger_std_20", stddev_samp("close").over(rolling_20))
    df_features = df_features.withColumn(
        "bollinger_upper",
        col("bollinger_ma_20") + (lit(2.0) * col("bollinger_std_20")),
    )
    df_features = df_features.withColumn(
        "bollinger_lower",
        col("bollinger_ma_20") - (lit(2.0) * col("bollinger_std_20")),
    )

    df_features = add_ema(df_features, "close", 12, "ema_12")
    df_features = add_ema(df_features, "close", 26, "ema_26")
    df_features = df_features.withColumn("macd", col("ema_12") - col("ema_26"))

    df_features = df_features.withColumn("hour_of_day", hour(col("feature_time")))
    df_features = df_features.withColumn("day_of_week", dayofweek(col("feature_time")))
    df_features = df_features.withColumn(
        "is_weekend",
        when(col("day_of_week").isin(1, 7), lit(1)).otherwise(lit(0)),
    )

    df_features = df_features.withColumn(
        "fee_pressure_index",
        when(col("block_count") == 0, lit(0.0)).otherwise(col("fee_total_sum") / col("block_count")),
    )
    df_features = df_features.withColumn(
        "fee_per_volume",
        when(col("volume") == 0, lit(0.0)).otherwise(col("fee_total_sum") / col("volume")),
    )
    df_features = df_features.withColumn(
        "tx_per_volume",
        when(col("volume") == 0, lit(0.0)).otherwise(col("tx_count") / col("volume")),
    )
    df_features = df_features.withColumn(
        "cdd_per_price",
        when(col("close") == 0, lit(0.0)).otherwise(col("cdd_total_sum") / col("close")),
    )
    df_features = df_features.withColumn(
        "network_activity_change",
        when(
            col("prev_tx_count_24h").isNull() | (col("prev_tx_count_24h") == 0),
            lit(0.0),
        ).otherwise((col("tx_count") - col("prev_tx_count_24h")) / col("prev_tx_count_24h")),
    )

    df_features = df_features.withColumn("created_at", current_timestamp())

    if start_time is not None:
        df_features = df_features.where(col("feature_time") >= lit(start_time))
    if end_time is not None:
        df_features = df_features.where(col("feature_time") < lit(end_time))

    df_final = (
        df_features.select(*TARGET_COLUMNS)
        .fillna(0, subset=NUMERIC_COLUMNS)
    )

    print(f"Writing into btc.{args.target_table}...")
    (
        df_final.write.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", CLICKHOUSE_JDBC_DRIVER)
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
