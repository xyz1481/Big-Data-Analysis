"""
=============================================================================
EXPERIMENT 5: Batch Analytics with Spark SQL
Project: Comparative Study: Solar vs Wind Output
Dataset: Cleaned Solar/Wind hourly data from Experiment 4
=============================================================================

Aim:
  To execute structured Spark SQL queries on the cleaned Solar and Wind dataset
  in order to analyze historical generation trends, hourly patterns, daily
  aggregation, peak periods, and source-wise comparisons.

Objectives:
  1. Read the cleaned Solar/Wind dataset into a PySpark DataFrame.
  2. Create a temporary Spark SQL view for structured querying.
  3. Run Spark SQL queries for totals, averages, minimums, maximums, and
     capacity factor analysis.
  4. Analyze hourly, daily, and monthly generation patterns.
  5. Demonstrate a window function and show Spark execution plans.
  6. Save the important aggregated outputs in Parquet format for Experiment 6.
=============================================================================
"""

import os
import sys
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    date_format,
    hour,
    month,
    row_number,
    to_date,
    to_timestamp,
)
from pyspark.sql.window import Window


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CLEANED_FILE = os.path.join(
    BASE_DIR, "hdfs_simulation", "renewable_energy", "processed", "cleaned_energy_data.csv"
)
OUTPUT_ROOT = os.path.join(BASE_DIR, "output", "exp5_results")


def banner(title):
    print("\n" + "=" * 78)
    print(f" >>> {title}")
    print("=" * 78)


def ensure_output_dir():
    os.makedirs(OUTPUT_ROOT, exist_ok=True)


def print_table(title, pdf):
    print(f"\n--- {title} ---")
    if pdf.empty:
        print("(no rows)")
    else:
        print(pdf.to_string(index=False))


def save_parquet(df, folder_name):
    output_path = os.path.join(OUTPUT_ROOT, folder_name)
    if os.path.isdir(output_path):
        shutil.rmtree(output_path)
    elif os.path.isfile(output_path):
        os.remove(output_path)
    pdf = df.toPandas()
    pdf.to_parquet(output_path, index=False)
    print(f"[+] Saved Parquet: {output_path}")


def build_spark_session():
    spark = (
        SparkSession.builder.appName("Exp5_SparkSQL_Solar_vs_Wind")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_cleaned_dataset(spark):
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(CLEANED_FILE)
    )

    df = (
        df.withColumn("utc_timestamp", to_timestamp(col("utc_timestamp")))
        .withColumn("date_only", to_date(col("utc_timestamp")))
        .withColumn("hour_of_day", hour(col("utc_timestamp")))
        .withColumn("month_number", month(col("utc_timestamp")))
        .withColumn("month_label", date_format(col("utc_timestamp"), "MMMM yyyy"))
    )
    return df


def main():
    print("#" * 78)
    print("  EXPERIMENT 5: BATCH ANALYTICS WITH SPARK SQL")
    print("#" * 78)

    if not os.path.exists(CLEANED_FILE):
        raise FileNotFoundError(
            f"Cleaned dataset not found at: {CLEANED_FILE}\n"
            "Run Experiment 4 first to create cleaned_energy_data.csv."
        )

    ensure_output_dir()
    spark = build_spark_session()

    banner("STEP 1: Loading Cleaned Dataset into PySpark DataFrame")
    df = load_cleaned_dataset(spark)
    total_rows = df.count()
    print(f"[+] Loaded cleaned dataset from: {CLEANED_FILE}")
    print(f"[+] Total Rows Read: {total_rows}")
    print("[+] PySpark Schema:")
    df.printSchema()
    print("\n[+] First 5 Rows:")
    df.select(
        "utc_timestamp",
        "country",
        "source",
        "power_output_mw",
        "capacity_mw",
        "capacity_factor_pct",
    ).show(5, truncate=False)

    banner("STEP 2: Creating Temporary Spark SQL View")
    df.createOrReplaceTempView("energy_data")
    print("[+] Temporary view 'energy_data' created successfully.")

    banner("STEP 3: Total Records by Source")
    total_records_df = spark.sql(
        """
        SELECT source, COUNT(*) AS total_records
        FROM energy_data
        GROUP BY source
        ORDER BY source
        """
    )
    total_records_df.explain(True)
    total_records_pdf = total_records_df.toPandas()
    print_table("Total Records by Source", total_records_pdf)
    save_parquet(total_records_df, "total_records_by_source.parquet")

    banner("STEP 4: Average Power Output, Minimum, Maximum and Capacity Factor")
    avg_output_df = spark.sql(
        """
        SELECT
            source,
            ROUND(AVG(power_output_mw), 2) AS avg_power_output_mw,
            ROUND(MIN(power_output_mw), 2) AS min_power_output_mw,
            ROUND(MAX(power_output_mw), 2) AS max_power_output_mw,
            ROUND(AVG(capacity_factor_pct), 2) AS avg_capacity_factor_pct
        FROM energy_data
        GROUP BY source
        ORDER BY source
        """
    )
    avg_output_pdf = avg_output_df.toPandas()
    print_table("Source-wise Power Output Summary", avg_output_pdf)
    save_parquet(avg_output_df, "average_power_output_by_source.parquet")

    min_max_df = spark.sql(
        """
        SELECT
            source,
            MIN(power_output_mw) AS minimum_output_mw,
            MAX(power_output_mw) AS maximum_output_mw
        FROM energy_data
        GROUP BY source
        ORDER BY source
        """
    )
    min_max_pdf = min_max_df.toPandas()
    print_table("Minimum and Maximum Output by Source", min_max_pdf)
    save_parquet(min_max_df, "min_max_output_by_source.parquet")

    capacity_factor_df = spark.sql(
        """
        SELECT
            source,
            ROUND(AVG(capacity_factor_pct), 2) AS avg_capacity_factor_pct
        FROM energy_data
        GROUP BY source
        ORDER BY source
        """
    )
    capacity_factor_pdf = capacity_factor_df.toPandas()
    print_table("Average Capacity Factor by Source", capacity_factor_pdf)
    save_parquet(capacity_factor_df, "average_capacity_factor_by_source.parquet")

    banner("STEP 5: Hourly Generation Pattern Analysis")
    hourly_df = spark.sql(
        """
        SELECT
            hour_of_day,
            source,
            ROUND(AVG(power_output_mw), 2) AS avg_power_output_mw,
            ROUND(AVG(capacity_factor_pct), 2) AS avg_capacity_factor_pct,
            COUNT(*) AS record_count
        FROM energy_data
        GROUP BY hour_of_day, source
        ORDER BY hour_of_day, source
        """
    )
    hourly_pdf = hourly_df.toPandas()
    print_table("Hourly Generation Pattern (All Hours)", hourly_pdf)
    save_parquet(hourly_df, "hourly_generation_patterns.parquet")

    banner("STEP 6: Daily and Monthly Aggregation")
    daily_df = spark.sql(
        """
        SELECT
            date_only,
            source,
            ROUND(SUM(power_output_mw), 2) AS total_daily_output_mw,
            ROUND(AVG(power_output_mw), 2) AS avg_hourly_output_mw,
            ROUND(AVG(capacity_factor_pct), 2) AS avg_capacity_factor_pct,
            COUNT(*) AS hourly_records
        FROM energy_data
        GROUP BY date_only, source
        ORDER BY date_only, source
        """
    )
    daily_pdf = daily_df.toPandas()
    print_table("Daily Generation Summary (First 12 Rows)", daily_pdf.head(12))
    save_parquet(daily_df, "daily_generation_summary.parquet")

    monthly_df = spark.sql(
        """
        SELECT
            month_label,
            source,
            ROUND(SUM(power_output_mw), 2) AS total_monthly_output_mw,
            ROUND(AVG(power_output_mw), 2) AS avg_hourly_output_mw,
            ROUND(AVG(capacity_factor_pct), 2) AS avg_capacity_factor_pct
        FROM energy_data
        GROUP BY month_label, source
        ORDER BY source
        """
    )
    monthly_pdf = monthly_df.toPandas()
    print_table("Monthly Generation Summary", monthly_pdf)
    save_parquet(monthly_df, "monthly_generation_summary.parquet")

    banner("STEP 7: Peak Generation Periods Using Window Function")
    peak_window = Window.partitionBy("source").orderBy(col("power_output_mw").desc(), col("utc_timestamp").asc())
    peak_df = (
        df.select(
            "utc_timestamp",
            "date_only",
            "hour_of_day",
            "source",
            "power_output_mw",
            "capacity_factor_pct",
        )
        .withColumn("generation_rank", row_number().over(peak_window))
        .filter(col("generation_rank") <= 5)
        .orderBy("source", "generation_rank")
    )
    peak_df.explain(True)
    peak_pdf = peak_df.toPandas()
    print_table("Top 5 Peak Generation Hours by Source", peak_pdf)
    save_parquet(peak_df, "peak_generation_periods.parquet")

    banner("STEP 8: Rolling 24-Hour Window Average")
    rolling_window = Window.partitionBy("source").orderBy(col("utc_timestamp")).rowsBetween(-23, 0)
    rolling_df = (
        df.select(
            "utc_timestamp",
            "date_only",
            "hour_of_day",
            "source",
            "power_output_mw",
            "capacity_factor_pct",
        )
        .withColumn("rolling_24h_avg_output_mw", avg(col("power_output_mw")).over(rolling_window))
        .orderBy("source", "utc_timestamp")
    )
    rolling_pdf = rolling_df.limit(12).toPandas()
    print_table("Rolling 24-Hour Average Output (Sample Rows)", rolling_pdf)
    save_parquet(rolling_df, "rolling_24h_window.parquet")

    banner("STEP 9: Solar vs Wind Source Comparison")
    comparison_df = spark.sql(
        """
        SELECT
            source,
            ROUND(AVG(power_output_mw), 2) AS avg_power_output_mw,
            ROUND(AVG(capacity_factor_pct), 2) AS avg_capacity_factor_pct,
            ROUND(MAX(power_output_mw), 2) AS peak_output_mw,
            SUM(CASE WHEN power_output_mw = 0 THEN 1 ELSE 0 END) AS zero_output_hours
        FROM energy_data
        GROUP BY source
        ORDER BY source
        """
    )
    comparison_pdf = comparison_df.toPandas()
    print_table("Solar vs Wind Comparison", comparison_pdf)
    save_parquet(comparison_df, "source_comparison.parquet")

    banner("STEP 10: Spark SQL Execution Plan Notes")
    print("[+] The execution plans above show how Spark parses, optimizes, and executes the SQL queries.")
    print("[+] Important aggregated outputs were saved to Parquet in the output/exp5_results directory.")

    banner("STEP 11: Experiment 5 Conclusion Summary")
    print(
        """
Experiment 5 produced structured Spark SQL summaries for Solar and Wind.
The dataset was queried by source, hour, date, and month, with both normal
aggregations and a window-function-based ranking and rolling calculation.
The resulting Parquet outputs are ready to be reused in Experiment 6 for
visual reporting and analytical charts.
        """.strip()
    )

    spark.stop()
    print("\n" + "=" * 78)
    print(" [RESULT] Experiment 5 executed successfully.")
    print("=" * 78)


if __name__ == "__main__":
    main()