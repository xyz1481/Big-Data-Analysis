"""
=============================================================================
EXPERIMENT 7: Real-Time Streaming Data Pipeline - Spark Structured Streaming
Project: Comparative Study: Solar vs Wind Output
Input: Kafka topic populated by exp7_kafka_producer.py
Output: Console sink with 5-minute sliding window aggregations
=============================================================================

The streaming consumer reads JSON messages from Kafka, parses the renewable
energy event schema, and computes a sliding window aggregation based on the
real-time event timestamp.
=============================================================================
"""

import argparse
from datetime import datetime, timezone
import os
import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    max as spark_max,
    round as spark_round,
    sum as spark_sum,
    to_timestamp,
    window,
)
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

DEFAULT_TOPIC = "renewable-energy-stream"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
BASE_DIR = Path(__file__).resolve().parent
EXP5_DIR = BASE_DIR / "output" / "exp5_results"


def banner(title):
    print("\n" + "=" * 78)
    print(f" >>> {title}")
    print("=" * 78)


def parse_args():
    parser = argparse.ArgumentParser(description="Spark Structured Streaming consumer for renewable-energy Kafka topic")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--packages", default=DEFAULT_PACKAGE, help="Maven package coordinate for Spark Kafka connector")
    parser.add_argument("--checkpoint-dir", default=os.path.join("output", "exp7_checkpoint"))
    parser.add_argument("--output-dir", default=os.path.join("output", "exp7_live_metrics"))
    parser.add_argument("--window-minutes", type=int, default=5)
    parser.add_argument("--slide-minutes", type=int, default=1)
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--demo", action="store_true", help="Generate local live-metrics files from Experiment 5 outputs instead of Kafka")
    return parser.parse_args()


def generate_demo_live_metrics(output_dir):
    comparison_path = EXP5_DIR / "source_comparison.parquet"
    hourly_path = EXP5_DIR / "hourly_generation_patterns.parquet"

    if not comparison_path.exists() or not hourly_path.exists():
        raise FileNotFoundError(
            "Experiment 5 outputs not found. Run exp5_spark_sql.py before demo mode."
        )

    comparison_df = pd.read_parquet(comparison_path)
    hourly_df = pd.read_parquet(hourly_path)

    hourly_df = hourly_df.sort_values(["hour_of_day", "source"]).reset_index(drop=True)
    hourly_df["window_start"] = pd.Timestamp.utcnow().floor("min") - pd.to_timedelta(
        (hourly_df["hour_of_day"].rank(method="dense") - 1).astype(int), unit="m"
    )
    hourly_df["window_end"] = hourly_df["window_start"] + pd.to_timedelta(5, unit="m")
    hourly_df["total_power_output_mw"] = hourly_df["avg_power_output_mw"] * hourly_df["record_count"].astype(float)
    hourly_df["peak_power_output_mw"] = hourly_df["avg_power_output_mw"]
    hourly_df["captured_at_utc"] = pd.Timestamp.utcnow().isoformat()
    hourly_df["batch_id"] = 0

    live_history = hourly_df[
        [
            "captured_at_utc",
            "batch_id",
            "window_start",
            "window_end",
            "source",
            "avg_power_output_mw",
            "total_power_output_mw",
            "avg_capacity_factor_pct",
            "peak_power_output_mw",
            "record_count",
        ]
    ].copy()

    latest_snapshot = (
        comparison_df.rename(
            columns={
                "peak_output_mw": "peak_power_output_mw",
            }
        )
        .assign(
            captured_at_utc=pd.Timestamp.utcnow().isoformat(),
            batch_id=1,
            window_start=pd.Timestamp.utcnow().floor("min"),
            window_end=pd.Timestamp.utcnow().ceil("min"),
            total_power_output_mw=lambda frame: frame["avg_power_output_mw"],
            record_count=0,
        )
    )[
        [
            "captured_at_utc",
            "batch_id",
            "window_start",
            "window_end",
            "source",
            "avg_power_output_mw",
            "total_power_output_mw",
            "avg_capacity_factor_pct",
            "peak_power_output_mw",
            "record_count",
        ]
    ]

    live_dir = Path(output_dir)
    live_dir.mkdir(parents=True, exist_ok=True)
    snapshot_file = live_dir / "latest_snapshot.csv"
    history_file = live_dir / "history.csv"

    latest_snapshot.to_csv(snapshot_file, index=False)
    live_history.to_csv(history_file, index=False)

    print("[DEMO] Generated live metrics from Experiment 5 outputs.")
    print(f"[DEMO] Snapshot: {snapshot_file}")
    print(f"[DEMO] History: {history_file}")
    print(latest_snapshot.to_string(index=False))


def build_spark_session(packages):
    builder = (
        SparkSession.builder.appName("Exp7_SparkStreaming_Solar_vs_Wind")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if packages:
        builder = builder.config("spark.jars.packages", packages)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    args = parse_args()
    os.makedirs(args.checkpoint_dir, exist_ok=True)
    os.makedirs(args.output_dir, exist_ok=True)

    if args.demo:
        banner("STEP 1: Demo Mode Live-Metrics Generation")
        generate_demo_live_metrics(args.output_dir)
        print("[RESULT] Experiment 7 demo mode completed successfully.")
        return

    banner("STEP 1: Initializing Spark Structured Streaming")
    spark = build_spark_session(args.packages)
    print(f"[+] Kafka topic: {args.topic}")
    print(f"[+] Bootstrap servers: {args.bootstrap_servers}")
    print(f"[+] Checkpoint directory: {args.checkpoint_dir}")
    print(f"[+] Window: {args.window_minutes} minutes sliding every {args.slide_minutes} minute(s)")

    event_schema = StructType(
        [
            StructField("sequence_number", LongType(), True),
            StructField("event_time", StringType(), True),
            StructField("measurement_timestamp", StringType(), True),
            StructField("country", StringType(), True),
            StructField("source", StringType(), True),
            StructField("power_output_mw", DoubleType(), True),
            StructField("capacity_mw", DoubleType(), True),
            StructField("capacity_factor_pct", DoubleType(), True),
        ]
    )

    banner("STEP 2: Reading Kafka Stream")
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_df.selectExpr("CAST(key AS STRING) AS kafka_key", "CAST(value AS STRING) AS kafka_value")
        .select(from_json(col("kafka_value"), event_schema).alias("data"), col("kafka_key"))
        .select("data.*", "kafka_key")
        .withColumn("event_ts", to_timestamp(col("event_time")))
        .withColumn("measurement_ts", to_timestamp(col("measurement_timestamp")))
    )

    banner("STEP 3: Sliding-Window Aggregation")
    aggregated_df = (
        parsed_df.withWatermark("event_ts", "10 minutes")
        .groupBy(
            window(
                col("event_ts"),
                f"{args.window_minutes} minutes",
                f"{args.slide_minutes} minute",
            ),
            col("source"),
        )
        .agg(
            spark_round(avg("power_output_mw"), 2).alias("avg_power_output_mw"),
            spark_round(spark_sum("power_output_mw"), 2).alias("total_power_output_mw"),
            spark_round(avg("capacity_factor_pct"), 2).alias("avg_capacity_factor_pct"),
            spark_max("power_output_mw").alias("peak_power_output_mw"),
            count("*").alias("record_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "source",
            "avg_power_output_mw",
            "total_power_output_mw",
            "avg_capacity_factor_pct",
            "peak_power_output_mw",
            "record_count",
        )
    )

    banner("STEP 4: Console Sink")
    print("[+] Streaming output will appear below. Stop the job with Ctrl+C after capturing a screenshot.")

    snapshot_file = os.path.join(args.output_dir, "latest_snapshot.csv")
    history_file = os.path.join(args.output_dir, "history.csv")

    def write_batch(batch_df, batch_id):
        batch_df = batch_df.orderBy("window_start", "source")
        pdf = batch_df.toPandas()
        if pdf.empty:
            print(f"[BATCH {batch_id}] No rows available yet.")
            return

        pdf.insert(0, "batch_id", int(batch_id))
        pdf.insert(0, "captured_at_utc", datetime.now(timezone.utc).isoformat())

        pdf.to_csv(snapshot_file, index=False)

        if os.path.exists(history_file):
            pdf.to_csv(history_file, index=False, mode="a", header=False)
        else:
            pdf.to_csv(history_file, index=False)

        print(f"[BATCH {batch_id}] Updated snapshot: {snapshot_file}")
        print(pdf.to_string(index=False))

    query = (
        aggregated_df.writeStream.outputMode("complete")
        .foreachBatch(write_batch)
        .option("checkpointLocation", args.checkpoint_dir)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    print("[+] Streaming query started successfully.")
    print("[+] Kafka -> Spark Structured Streaming -> Sliding Window -> Local snapshot + console")

    query.awaitTermination()


if __name__ == "__main__":
    main()