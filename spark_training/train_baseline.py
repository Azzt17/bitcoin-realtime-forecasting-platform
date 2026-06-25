#!/usr/bin/env python3

"""Train and score the MVP 1h persistence baseline."""

from __future__ import annotations

import argparse
import json
import pathlib
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid5

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    lit,
    udf,
    when,
)
from pyspark.sql.types import StringType

CLICKHOUSE_JDBC_DRIVER = "com.clickhouse.jdbc.Driver"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train the persistence baseline and write predictions/metrics."
    )
    parser.add_argument("--clickhouse-host", required=True)
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--clickhouse-database", default="btc")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="")
    parser.add_argument("--dataset-table", default="training_dataset_1h")
    parser.add_argument("--prediction-table", default="predictions")
    parser.add_argument("--metrics-table", default="model_metrics")
    parser.add_argument("--model-name", default="baseline_persistence")
    parser.add_argument("--model-version", default="v1")
    parser.add_argument("--horizon", default="1h")
    parser.add_argument("--predict-start", default="")
    parser.add_argument("--predict-end", default="")
    parser.add_argument("--artifact-dir", default="/tmp/bitcoin-models/baseline")
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


def build_prediction_id(model_name: str, model_version: str, horizon: str):
    def _builder(prediction_time, target_time) -> str:
        raw = (
            f"{model_name}:{model_version}:{horizon}:"
            f"{prediction_time.isoformat()}:{target_time.isoformat()}"
        )
        return str(uuid5(NAMESPACE_URL, raw))

    return udf(_builder, StringType())


def main() -> int:
    args = parse_args()
    predict_start = parse_utc_timestamp(args.predict_start) if args.predict_start else None
    predict_end = parse_utc_timestamp(args.predict_end) if args.predict_end else None

    spark = (
        SparkSession.builder.appName("btc-train-baseline-1h")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    jdbc_url = (
        f"jdbc:clickhouse://{args.clickhouse_host}:{args.clickhouse_port}/"
        f"{args.clickhouse_database}"
    )

    time_clauses = []
    if predict_start is not None:
        time_clauses.append(
            f"feature_time >= toDateTime64('{ch_datetime_literal(predict_start)}', 3, 'UTC')"
        )
    if predict_end is not None:
        time_clauses.append(
            f"feature_time < toDateTime64('{ch_datetime_literal(predict_end)}', 3, 'UTC')"
        )
    where_clause = "WHERE " + " AND ".join(time_clauses) if time_clauses else ""

    dataset = read_clickhouse_relation(
        spark,
        jdbc_url,
        f"(SELECT * FROM btc.{args.dataset_table} {where_clause}) AS training_dataset_1h",
        args.clickhouse_user,
        args.clickhouse_password,
    )

    if dataset.rdd.isEmpty():
        print("No rows found for the requested prediction window.")
        return 0

    prediction_id = build_prediction_id(args.model_name, args.model_version, args.horizon)
    scored = dataset.select(
        prediction_id(
            col("feature_time"), expr("feature_time + INTERVAL 1 HOUR")
        ).alias(
            "prediction_id"
        ),
        col("feature_time").alias("prediction_time"),
        (col("feature_time") + expr("INTERVAL 1 HOUR")).alias("target_time"),
        lit("BTC").alias("asset"),
        lit(args.horizon).alias("horizon"),
        lit(args.model_name).alias("model_name"),
        lit(args.model_version).alias("model_version"),
        col("close").alias("current_price"),
        lit(0.0).alias("predicted_return"),
        col("close").alias("predicted_price"),
        current_timestamp().alias("created_at"),
        col("target_return_1h"),
    ).withColumn(
        "actual_price",
        col("current_price") * (lit(1.0) + col("target_return_1h")),
    )

    print(f"Writing baseline predictions into btc.{args.prediction_table}...")
    write_clickhouse(
        scored.select(
            "prediction_id",
            "prediction_time",
            "target_time",
            "asset",
            "horizon",
            "model_name",
            "model_version",
            "current_price",
            "predicted_return",
            "predicted_price",
            "created_at",
        ),
        jdbc_url,
        args.prediction_table,
        args.clickhouse_user,
        args.clickhouse_password,
    )

    mae = scored.selectExpr(
        "avg(abs(predicted_price - actual_price)) AS value"
    ).first()["value"] or 0.0
    rmse = scored.selectExpr(
        "sqrt(avg((predicted_price - actual_price) * (predicted_price - actual_price))) AS value"
    ).first()["value"] or 0.0
    mape = scored.selectExpr(
        "avg(abs(predicted_price - actual_price) / if(actual_price = 0, 1.0, abs(actual_price))) AS value"
    ).first()["value"] or 0.0
    directional_accuracy = scored.selectExpr(
        """
        avg(
            if(
                (predicted_return > 0 AND target_return_1h > 0)
                OR (predicted_return < 0 AND target_return_1h < 0)
                OR (predicted_return = 0 AND target_return_1h = 0),
                1.0,
                0.0
            )
        ) AS value
        """
    ).first()["value"] or 0.0
    sample_size = scored.count()
    data_start = scored.agg({"prediction_time": "min"}).first()[0]
    data_end = scored.agg({"prediction_time": "max"}).first()[0]

    metrics_df = spark.createDataFrame(
        [
            (
                datetime.now(timezone.utc),
                args.model_name,
                args.model_version,
                args.horizon,
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

    print(f"Writing baseline metrics into btc.{args.metrics_table}...")
    write_clickhouse(
        metrics_df,
        jdbc_url,
        args.metrics_table,
        args.clickhouse_user,
        args.clickhouse_password,
    )

    artifact_dir = pathlib.Path(args.artifact_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / "baseline_persistence.json"
    artifact_path.write_text(
        json.dumps(
            {
                "model_name": args.model_name,
                "model_version": args.model_version,
                "horizon": args.horizon,
                "artifact_type": "persistence",
                "sample_size": int(sample_size),
                "trained_at": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print(f"Baseline artifact written to {artifact_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
