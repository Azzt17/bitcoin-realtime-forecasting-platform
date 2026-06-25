#!/usr/bin/env python3

"""Train and score the MVP 1h Spark GBT model."""

from __future__ import annotations

import argparse
import json
import pathlib
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid5

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit, udf, when
from pyspark.sql.types import StringType

CLICKHOUSE_JDBC_DRIVER = "com.clickhouse.jdbc.Driver"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train the GBT model and write predictions/metrics."
    )
    parser.add_argument("--clickhouse-host", required=True)
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--clickhouse-database", default="btc")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="")
    parser.add_argument("--dataset-table", default="training_dataset_1h")
    parser.add_argument("--prediction-table", default="predictions")
    parser.add_argument("--metrics-table", default="model_metrics")
    parser.add_argument("--model-name", default="spark_gbt")
    parser.add_argument("--model-version", default="v1")
    parser.add_argument("--horizon", default="1h")
    parser.add_argument("--train-start", required=True)
    parser.add_argument("--train-end", required=True)
    parser.add_argument("--score-start", required=True)
    parser.add_argument("--score-end", required=True)
    parser.add_argument("--artifact-dir", default="/tmp/bitcoin-models/spark_gbt")
    parser.add_argument("--max-iter", type=int, default=50)
    parser.add_argument("--max-depth", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
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
    train_start = parse_utc_timestamp(args.train_start)
    train_end = parse_utc_timestamp(args.train_end)
    score_start = parse_utc_timestamp(args.score_start)
    score_end = parse_utc_timestamp(args.score_end)

    spark = (
        SparkSession.builder.appName("btc-train-gbt-1h")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    jdbc_url = (
        f"jdbc:clickhouse://{args.clickhouse_host}:{args.clickhouse_port}/"
        f"{args.clickhouse_database}"
    )

    dataset = read_clickhouse_relation(
        spark,
        jdbc_url,
        f"""
        (
            SELECT *
            FROM btc.{args.dataset_table}
            WHERE feature_time >= toDateTime64('{ch_datetime_literal(train_start)}', 3, 'UTC')
              AND feature_time < toDateTime64('{ch_datetime_literal(score_end)}', 3, 'UTC')
        ) AS training_dataset_1h
        """,
        args.clickhouse_user,
        args.clickhouse_password,
    )

    if dataset.rdd.isEmpty():
        print("No rows found for the requested training window.")
        return 0

    feature_columns = [
        column
        for column in dataset.columns
        if column not in {"feature_time", "target_return_1h", "created_at"}
    ]

    prepared = dataset.fillna(0, subset=feature_columns + ["target_return_1h"])
    train_df = prepared.where(
        (col("feature_time") >= lit(train_start)) & (col("feature_time") < lit(train_end))
    )
    score_df = prepared.where(
        (col("feature_time") >= lit(score_start)) & (col("feature_time") < lit(score_end))
    )

    if train_df.rdd.isEmpty():
        print("Training window returned no rows.")
        return 1
    if score_df.rdd.isEmpty():
        print("Scoring window returned no rows.")
        return 1

    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features",
        handleInvalid="keep",
    )
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="target_return_1h",
        predictionCol="predicted_return",
        maxIter=args.max_iter,
        maxDepth=args.max_depth,
        seed=args.seed,
    )
    pipeline = Pipeline(stages=[assembler, gbt])

    print("Training GBT model...")
    model = pipeline.fit(train_df)

    artifact_dir = pathlib.Path(args.artifact_dir) / args.model_version
    artifact_dir.mkdir(parents=True, exist_ok=True)
    model.write().overwrite().save(str(artifact_dir / "pipeline_model"))
    (artifact_dir / "metadata.json").write_text(
        json.dumps(
            {
                "model_name": args.model_name,
                "model_version": args.model_version,
                "horizon": args.horizon,
                "feature_columns": feature_columns,
                "training_start": args.train_start,
                "training_end": args.train_end,
                "score_start": args.score_start,
                "score_end": args.score_end,
                "max_iter": args.max_iter,
                "max_depth": args.max_depth,
                "seed": args.seed,
                "trained_at": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    scored = model.transform(score_df)
    scored = scored.withColumn(
        "actual_price",
        col("close") * (lit(1.0) + col("target_return_1h")),
    ).withColumn(
        "predicted_price",
        col("close") * (lit(1.0) + col("predicted_return")),
    )
    prediction_id = build_prediction_id(args.model_name, args.model_version, args.horizon)
    predictions = scored.select(
        prediction_id(col("feature_time"), expr("feature_time + INTERVAL 1 HOUR")).alias(
            "prediction_id"
        ),
        col("feature_time").alias("prediction_time"),
        (col("feature_time") + expr("INTERVAL 1 HOUR")).alias("target_time"),
        lit("BTC").alias("asset"),
        lit(args.horizon).alias("horizon"),
        lit(args.model_name).alias("model_name"),
        lit(args.model_version).alias("model_version"),
        col("close").alias("current_price"),
        col("predicted_return"),
        col("predicted_price"),
        current_timestamp().alias("created_at"),
    )

    print(f"Writing GBT predictions into btc.{args.prediction_table}...")
    write_clickhouse(
        predictions,
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
    data_start = scored.agg({"feature_time": "min"}).first()[0]
    data_end = scored.agg({"feature_time": "max"}).first()[0]

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

    print(f"Writing GBT metrics into btc.{args.metrics_table}...")
    write_clickhouse(
        metrics_df,
        jdbc_url,
        args.metrics_table,
        args.clickhouse_user,
        args.clickhouse_password,
    )

    print(f"GBT artifact written to {artifact_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
