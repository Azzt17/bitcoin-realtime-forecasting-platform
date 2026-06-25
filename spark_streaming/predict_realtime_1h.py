#!/usr/bin/env python3

"""Real-time Model Inference for 1h Bitcoin Forecasting."""

import argparse
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, expr, lit, current_timestamp
from pyspark.sql.types import StringType
from uuid import NAMESPACE_URL, uuid5

CLICKHOUSE_JDBC_DRIVER = "com.clickhouse.jdbc.Driver"

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--clickhouse-host", required=True)
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--clickhouse-database", default="btc")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="")
    parser.add_argument("--feature-table", default="features_1h_realtime")
    parser.add_argument("--prediction-table", default="predictions")
    parser.add_argument("--model-path", default="/tmp/bitcoin-models/spark_gbt/v1/pipeline_model")
    parser.add_argument("--model-name", default="spark_gbt")
    parser.add_argument("--model-version", default="v1")
    parser.add_argument("--horizon", default="1h")
    parser.add_argument("--poll-interval", type=int, default=60)
    return parser.parse_args()

def build_prediction_id(model_name: str, model_version: str, horizon: str):
    from pyspark.sql.functions import udf
    def _builder(prediction_time, target_time) -> str:
        raw = f"{model_name}:{model_version}:{horizon}:{prediction_time.isoformat()}:{target_time.isoformat()}"
        return str(uuid5(NAMESPACE_URL, raw))
    return udf(_builder, StringType())

def main():
    args = parse_args()
    spark = SparkSession.builder.appName("btc-realtime-inference-1h").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    jdbc_url = f"jdbc:clickhouse://{args.clickhouse_host}:{args.clickhouse_port}/{args.clickhouse_database}"

    print(f"Loading trained model from {args.model_path}...")
    try:
        model = PipelineModel.load(args.model_path)
    except Exception as e:
        print(f"Model not found or failed to load. Ensure training is finished. Error: {e}")
        return 1

    print("Model loaded. Starting real-time inference loop...")
    prediction_id_udf = build_prediction_id(args.model_name, args.model_version, args.horizon)
    last_processed_time = None

    while True:
        try:
            # Query the latest feature row from ClickHouse
            query = f"(SELECT * FROM {args.clickhouse_database}.{args.feature_table} ORDER BY feature_time DESC LIMIT 1) AS latest_feature"
            
            latest_df = spark.read.format("jdbc") \
                .option("url", jdbc_url) \
                .option("driver", CLICKHOUSE_JDBC_DRIVER) \
                .option("dbtable", query) \
                .option("user", args.clickhouse_user) \
                .option("password", args.clickhouse_password) \
                .load()

            if not latest_df.rdd.isEmpty():
                current_feature_time = latest_df.select("feature_time").first()[0]
                
                if last_processed_time != current_feature_time:
                    print(f"[{datetime.utcnow()}] New feature detected for {current_feature_time}. Running inference...")
                    
                    # Ensure nulls are handled identically to training
                    feature_columns = [c for c in latest_df.columns if c not in {"feature_time", "created_at"}]
                    prepared_df = latest_df.fillna(0, subset=feature_columns)
                    
                    # Generate Prediction
                    scored = model.transform(prepared_df)
                    
                    predictions = scored.select(
                        prediction_id_udf(col("feature_time"), expr("feature_time + INTERVAL 1 HOUR")).alias("prediction_id"),
                        col("feature_time").alias("prediction_time"),
                        (col("feature_time") + expr("INTERVAL 1 HOUR")).alias("target_time"),
                        lit("BTC").alias("asset"),
                        lit(args.horizon).alias("horizon"),
                        lit(args.model_name).alias("model_name"),
                        lit(args.model_version).alias("model_version"),
                        col("close").alias("current_price"),
                        col("predicted_return"),
                        (col("close") * (lit(1.0) + col("predicted_return"))).alias("predicted_price"),
                        current_timestamp().alias("created_at")
                    )
                    
                    # Write to ClickHouse
                    predictions.write.format("jdbc") \
                        .option("url", jdbc_url) \
                        .option("driver", CLICKHOUSE_JDBC_DRIVER) \
                        .option("dbtable", args.prediction_table) \
                        .option("user", args.clickhouse_user) \
                        .option("password", args.clickhouse_password) \
                        .mode("append") \
                        .save()
                    
                    print(f"[{datetime.utcnow()}] Prediction saved successfully.")
                    last_processed_time = current_feature_time
                else:
                    print(f"[{datetime.utcnow()}] No new features. Waiting...")
            else:
                print(f"[{datetime.utcnow()}] No data in {args.feature_table} yet.")
                
        except Exception as e:
            print(f"Error during inference: {e}")

        time.sleep(args.poll_interval)

if __name__ == "__main__":
    main()
