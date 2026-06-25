#!/usr/bin/env python3

"""Evaluate persisted prediction rows against actual future prices."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when

CLICKHOUSE_JDBC_DRIVER = "com.clickhouse.jdbc.Driver"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate btc.predictions and write btc.prediction_errors."
    )
    parser.add_argument("--clickhouse-host", required=True)
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--clickhouse-database", default="btc")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="")
    parser.add_argument("--prediction-table", default="predictions")
    parser.add_argument("--error-table", default="prediction_errors")
    parser.add_argument("--metrics-table", default="model_metrics")
    parser.add_argument("--model-name", default="")
    parser.add_argument("--model-version", default="")
    parser.add_argument("--horizon", default="1h")
    parser.add_argument("--prediction-start", default="")
    parser.add_argument("--prediction-end", default="")
    return parser.parse_args()


def parse_utc_timestamp(raw_value: str) -> datetime:
    parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def ch_datetime_literal(value: datetime) -> str:
    value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%d %H:%M:%S")


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


def write_clickhouse(df, jdbc_url: str, table: str, user: str, password: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", CLICKHOUSE_JDBC_DRIVER)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .mode("append")
        .save()
    )


def main() -> int:
    args = parse_args()
    prediction_start = (
        parse_utc_timestamp(args.prediction_start) if args.prediction_start else None
    )
    prediction_end = parse_utc_timestamp(args.prediction_end) if args.prediction_end else None

    spark = (
        SparkSession.builder.appName("btc-evaluate-predictions-1h")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    jdbc_url = (
        f"jdbc:clickhouse://{args.clickhouse_host}:{args.clickhouse_port}/"
        f"{args.clickhouse_database}"
    )

    prediction_clauses = [f"horizon = '{args.horizon}'"]
    if args.model_name:
        prediction_clauses.append(f"model_name = '{args.model_name}'")
    if args.model_version:
        prediction_clauses.append(f"model_version = '{args.model_version}'")
    if prediction_start is not None:
        prediction_clauses.append(
            f"prediction_time >= toDateTime64('{ch_datetime_literal(prediction_start)}', 3, 'UTC')"
        )
    if prediction_end is not None:
        prediction_clauses.append(
            f"prediction_time < toDateTime64('{ch_datetime_literal(prediction_end)}', 3, 'UTC')"
        )
    prediction_where = "WHERE " + " AND ".join(prediction_clauses)

    predictions = read_clickhouse_relation(
        spark,
        jdbc_url,
        f"(SELECT * FROM btc.{args.prediction_table} {prediction_where}) AS predictions",
        args.clickhouse_user,
        args.clickhouse_password,
    )

    if predictions.rdd.isEmpty():
        print("No prediction rows found for the requested evaluation window.")
        return 0

    target_start = predictions.agg({"target_time": "min"}).first()[0]
    target_end = predictions.agg({"target_time": "max"}).first()[0]

    actuals = read_clickhouse_relation(
        spark,
        jdbc_url,
        f"""
        (
            SELECT feature_time, close
            FROM btc.view_ohlcv_1h
            WHERE feature_time >= toDateTime64('{ch_datetime_literal(target_start)}', 3, 'UTC')
              AND feature_time <= toDateTime64('{ch_datetime_literal(target_end)}', 3, 'UTC')
        ) AS ohlcv_future
        """,
        args.clickhouse_user,
        args.clickhouse_password,
    )

    joined = predictions.alias("p").join(
        actuals.alias("a"),
        col("p.target_time") == col("a.feature_time"),
        "inner",
    ).withColumn(
        "actual_price",
        col("a.close"),
    ).withColumn(
        "absolute_error",
        (col("p.predicted_price") - col("a.close")).abs(),
    ).withColumn(
        "squared_error",
        (col("p.predicted_price") - col("a.close")) * (col("p.predicted_price") - col("a.close")),
    ).withColumn(
        "percentage_error",
        when(col("a.close") == 0, lit(0.0)).otherwise(
            (col("p.predicted_price") - col("a.close")).abs() / col("a.close")
        ),
    ).withColumn(
        "predicted_direction",
        when(col("p.predicted_price") > col("p.current_price"), lit(1))
        .when(col("p.predicted_price") < col("p.current_price"), lit(-1))
        .otherwise(lit(0)),
    ).withColumn(
        "actual_direction",
        when(col("a.close") > col("p.current_price"), lit(1))
        .when(col("a.close") < col("p.current_price"), lit(-1))
        .otherwise(lit(0)),
    ).withColumn(
        "direction_correct",
        when(col("predicted_direction") == col("actual_direction"), lit(1)).otherwise(lit(0)),
    )

    errors = joined.select(
        col("p.prediction_id").alias("prediction_id"),
        col("p.prediction_time").alias("prediction_time"),
        col("p.target_time").alias("target_time"),
        col("p.asset").alias("asset"),
        col("p.horizon").alias("horizon"),
        col("p.model_name").alias("model_name"),
        col("p.model_version").alias("model_version"),
        col("p.predicted_price").alias("predicted_price"),
        col("actual_price"),
        col("absolute_error"),
        col("squared_error"),
        col("percentage_error"),
        col("predicted_direction"),
        col("actual_direction"),
        col("direction_correct"),
        current_timestamp().alias("evaluated_at"),
    )

    print(f"Writing evaluation rows into btc.{args.error_table}...")
    write_clickhouse(
        errors,
        jdbc_url,
        args.error_table,
        args.clickhouse_user,
        args.clickhouse_password,
    )

    mae = joined.selectExpr(
        "avg(abs(predicted_price - actual_price)) AS value"
    ).first()["value"] or 0.0
    rmse = joined.selectExpr(
        "sqrt(avg((predicted_price - actual_price) * (predicted_price - actual_price))) AS value"
    ).first()["value"] or 0.0
    mape = joined.selectExpr(
        "avg(abs(predicted_price - actual_price) / if(actual_price = 0, 1.0, abs(actual_price))) AS value"
    ).first()["value"] or 0.0
    directional_accuracy = joined.selectExpr(
        "avg(direction_correct) AS value"
    ).first()["value"] or 0.0
    sample_size = joined.count()
    data_start = (
        joined.select(col("p.prediction_time").alias("prediction_time"))
        .agg({"prediction_time": "min"})
        .first()[0]
    )
    data_end = (
        joined.select(col("p.prediction_time").alias("prediction_time"))
        .agg({"prediction_time": "max"})
        .first()[0]
    )
    first_meta = joined.select(
        col("p.model_name").alias("model_name"),
        col("p.model_version").alias("model_version"),
        col("p.horizon").alias("horizon"),
    ).first()

    metrics_df = spark.createDataFrame(
        [
            (
                datetime.now(timezone.utc),
                first_meta["model_name"],
                first_meta["model_version"],
                first_meta["horizon"],
                float(mae),
                float(rmse),
                float(mape),
                float(directional_accuracy),
                int(sample_size),
                data_start,
                data_end,
                datetime.now(timezone.utc),
            )
        ],
        [
            "metric_time",
            "model_name",
            "model_version",
            "horizon",
            "mae",
            "rmse",
            "mape",
            "directional_accuracy",
            "sample_size",
            "data_start",
            "data_end",
            "created_at",
        ],
    )

    print(f"Writing evaluation metrics into btc.{args.metrics_table}...")
    write_clickhouse(
        metrics_df,
        jdbc_url,
        args.metrics_table,
        args.clickhouse_user,
        args.clickhouse_password,
    )

    print("Prediction evaluation complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
