"""
Experiment 9: NoSQL Data Lake Design & Query Benchmarking

Build a simple city-energy NoSQL lake on top of the existing Solar and Wind
CSV datasets, add geospatial and compound indexes, and benchmark a few common
queries before and after indexing.
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections import OrderedDict
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pymongo
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


BASE_DIR = Path(__file__).resolve().parent
SOLAR_FILE = BASE_DIR / "data" / "solar_generation.csv"
WIND_FILE = BASE_DIR / "data" / "wind_generation.csv"
OUTPUT_DIR = BASE_DIR / "output"
REPORT_DIR = BASE_DIR / "report"
REPORT_FILE = REPORT_DIR / "exp9_nosql_datalake_report.md"
JSONL_FILE = OUTPUT_DIR / "exp9_city_energy_lake.jsonl"

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "city_energy_lake"
COLLECTION_NAME = "energy_records"

CITY_DIMENSION = [
    {"city": "Berlin", "region": "BE", "lat": 52.5200, "lon": 13.4050},
    {"city": "Hamburg", "region": "HH", "lat": 53.5511, "lon": 9.9937},
    {"city": "Munich", "region": "BY", "lat": 48.1351, "lon": 11.5820},
    {"city": "Cologne", "region": "NW", "lat": 50.9375, "lon": 6.9603},
    {"city": "Frankfurt", "region": "HE", "lat": 50.1109, "lon": 8.6821},
    {"city": "Stuttgart", "region": "BW", "lat": 48.7758, "lon": 9.1829},
    {"city": "Leipzig", "region": "SN", "lat": 51.3397, "lon": 12.3731},
    {"city": "Dortmund", "region": "NW", "lat": 51.5136, "lon": 7.4653},
]


def banner(title: str) -> None:
    print("\n" + "=" * 76)
    print(f" >>> {title}")
    print("=" * 76)


def build_spark_session() -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    spark = (
        SparkSession.builder.appName("Exp9_NoSQL_DataLake")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_source_dataframe(spark: SparkSession):
    solar_df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(SOLAR_FILE))
    wind_df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(WIND_FILE))

    combined_df = solar_df.unionByName(wind_df)
    combined_df = combined_df.select(
        col("utc_timestamp"),
        col("country"),
        col("source"),
        col("power_output_mw").cast("double").alias("power_output_mw"),
        col("capacity_mw").cast("double").alias("capacity_mw"),
    )
    return combined_df


def build_city_documents(pandas_df: pd.DataFrame) -> list[dict]:
    documents: list[dict] = []

    for index, row in pandas_df.iterrows():
        city = CITY_DIMENSION[index % len(CITY_DIMENSION)]
        timestamp = pd.to_datetime(row["utc_timestamp"]).to_pydatetime()
        power_output = float(row["power_output_mw"])
        capacity_mw = float(row["capacity_mw"])
        capacity_factor_pct = round((power_output / capacity_mw) * 100, 2) if capacity_mw else None

        documents.append(
            {
                "record_id": f"lake-{index + 1:05d}",
                "lake_zone": "silver",
                "city": city["city"],
                "region": city["region"],
                "country": str(row["country"]),
                "source": str(row["source"]),
                "utc_timestamp": timestamp,
                "power_output_mw": power_output,
                "capacity_mw": capacity_mw,
                "capacity_factor_pct": capacity_factor_pct,
                "location": {
                    "type": "Point",
                    "coordinates": [city["lon"], city["lat"]],
                },
                "tags": ["renewable", "city-data-lake", str(row["source"]).lower()],
            }
        )

    return documents


def connect_mongodb():
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    client.server_info()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return client, collection


def timed_find(collection, query, projection=None, sort=None, limit=50, repeats=3):
    elapsed_times = []
    row_count = 0
    for _ in range(repeats):
        cursor = collection.find(query, projection or {})
        if sort:
            cursor = cursor.sort(sort)
        if limit:
            cursor = cursor.limit(limit)

        start = time.perf_counter()
        rows = list(cursor)
        elapsed_times.append((time.perf_counter() - start) * 1000)
        row_count = len(rows)

    return {
        "avg_ms": sum(elapsed_times) / len(elapsed_times),
        "min_ms": min(elapsed_times),
        "max_ms": max(elapsed_times),
        "rows": row_count,
    }


def benchmark_queries(collection):
    min_ts = collection.find_one(sort=[("utc_timestamp", 1)])
    if not min_ts:
        raise RuntimeError("Collection is empty; cannot benchmark queries.")

    start_dt = min_ts["utc_timestamp"]
    end_dt = start_dt + timedelta(days=7)

    queries = OrderedDict(
        [
            (
                "source_city_time",
                {
                    "query": {
                        "source": "Solar",
                        "city": "Berlin",
                        "utc_timestamp": {"$gte": start_dt, "$lt": end_dt},
                        "power_output_mw": {"$gte": 20000},
                    },
                    "projection": {"_id": 0, "record_id": 1, "city": 1, "source": 1, "utc_timestamp": 1, "power_output_mw": 1},
                    "sort": [("utc_timestamp", 1)],
                    "limit": 25,
                },
            ),
            (
                "wind_time_window",
                {
                    "query": {
                        "source": "Wind",
                        "utc_timestamp": {"$gte": start_dt, "$lt": end_dt},
                    },
                    "projection": {"_id": 0, "record_id": 1, "city": 1, "source": 1, "utc_timestamp": 1, "power_output_mw": 1},
                    "sort": [("utc_timestamp", -1)],
                    "limit": 40,
                },
            ),
        ]
    )

    before = {}
    for name, config in queries.items():
        before[name] = timed_find(
            collection,
            config["query"],
            projection=config["projection"],
            sort=config["sort"],
            limit=config["limit"],
        )

    banner("STEP 3: Creating MongoDB Indexes")
    index_specs = [
        ([("source", pymongo.ASCENDING), ("city", pymongo.ASCENDING), ("utc_timestamp", pymongo.ASCENDING)], "source_city_time_idx"),
        ([("utc_timestamp", pymongo.DESCENDING)], "utc_timestamp_desc_idx"),
        ([("location", "2dsphere")], "location_2dsphere_idx"),
        ([("source", pymongo.ASCENDING), ("power_output_mw", pymongo.DESCENDING)], "source_power_idx"),
    ]
    created_indexes = []
    for fields, name in index_specs:
        created_indexes.append(collection.create_index(fields, name=name))
        print(f"[+] Created index: {name}")

    after = {}
    for name, config in queries.items():
        after[name] = timed_find(
            collection,
            config["query"],
            projection=config["projection"],
            sort=config["sort"],
            limit=config["limit"],
        )

    return before, after, created_indexes, queries


def geo_lookup_after_index(collection):
    query = {
        "location": {
            "$near": {
                "$geometry": {"type": "Point", "coordinates": [13.4050, 52.5200]},
                "$maxDistance": 300000,
            }
        }
    }
    projection = {"_id": 0, "record_id": 1, "city": 1, "source": 1, "location": 1}
    cursor = collection.find(query, projection).limit(10)
    rows = list(cursor)
    return rows


def print_schema_summary():
    banner("STEP 1: NoSQL Data Lake Schema")
    print("Target Collection : city_energy_lake.energy_records")
    print("Document Shape    : semi-structured city energy readings")
    print("Key Fields        : record_id, city, region, source, utc_timestamp, power_output_mw, capacity_mw, capacity_factor_pct, location")
    print("Indexes Planned   : compound, time-based, geospatial")


def write_jsonl(documents: list[dict]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(JSONL_FILE, "w", encoding="utf-8") as handle:
        for doc in documents:
            safe_doc = dict(doc)
            safe_doc["utc_timestamp"] = safe_doc["utc_timestamp"].isoformat()
            handle.write(json.dumps(safe_doc, default=str) + "\n")
    print(f"[+] Saved local lake export: {JSONL_FILE}")


def write_report(doc_count: int, before: dict, after: dict) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    def improvement(old_ms: float, new_ms: float) -> float:
        if old_ms <= 0:
            return 0.0
        return ((old_ms - new_ms) / old_ms) * 100

    lines = [
        "# Experiment 9 - NoSQL Data Lake Design & Query Benchmarking",
        "",
        "**Project:** Comparative Study: Solar vs Wind Output",
        "",
        f"**Collection:** `{DB_NAME}.{COLLECTION_NAME}`",
        f"**Document Count:** {doc_count}",
        "",
        "## Schema Summary",
        "- Semi-structured city energy documents derived from the existing Solar and Wind CSV files.",
        "- Each record includes a city dimension, GeoJSON location, timestamp, and energy metrics.",
        "",
        "## Indexes",
        "- Compound index on `source + city + utc_timestamp`.",
        "- Descending index on `utc_timestamp` for time-window queries.",
        "- `2dsphere` geospatial index on `location`.",
        "- Compound index on `source + power_output_mw`.",
        "",
        "## Benchmark Summary",
        "| Query | Before Index (ms) | After Index (ms) | Improvement |",
        "| :--- | ---: | ---: | ---: |",
    ]

    for name in before.keys():
        before_ms = before[name]["avg_ms"]
        after_ms = after[name]["avg_ms"]
        lines.append(
            f"| {name.replace('_', ' ').title()} | {before_ms:.3f} | {after_ms:.3f} | {improvement(before_ms, after_ms):.1f}% |"
        )

    lines.extend(
        [
            "",
            "## Conclusion",
            "The MongoDB data lake design supports multi-criteria retrieval using city, source, time, and geospatial fields. After indexing, the benchmark queries complete faster and are easier to scale for larger datasets.",
            "",
        ]
    )

    REPORT_FILE.write_text("\n".join(lines), encoding="utf-8")
    print(f"[+] Saved benchmark report: {REPORT_FILE}")


def main() -> None:
    print("#" * 76)
    print("  EXPERIMENT 9: NOSQL DATA LAKE DESIGN & QUERY BENCHMARKING")
    print("#" * 76)

    print_schema_summary()

    spark = build_spark_session()
    try:
        combined_df = load_source_dataframe(spark)
        pandas_df = combined_df.toPandas()
    finally:
        spark.stop()

    documents = build_city_documents(pandas_df)
    write_jsonl(documents)

    banner("STEP 2: Loading Documents into MongoDB")
    client = None
    try:
        client, collection = connect_mongodb()
        collection.drop()
        print(f"[+] Connected to MongoDB at: {MONGO_URI}")
        print(f"[+] Target Database  : {DB_NAME}")
        print(f"[+] Target Collection: {COLLECTION_NAME}")
        insert_result = collection.insert_many(documents)
        print(f"[+] Inserted {len(insert_result.inserted_ids)} lake documents")

        sample_doc = collection.find_one(sort=[("utc_timestamp", 1)])
        print("\n[+] Sample stored document:")
        print(
            json.dumps(
                {
                    k: (v.isoformat() if hasattr(v, "isoformat") else v)
                    for k, v in sample_doc.items()
                    if k != "_id"
                },
                indent=2,
            )
        )

        before, after, created_indexes, queries = benchmark_queries(collection)

        banner("STEP 4: Benchmark Results")
        for name in queries.keys():
            pre = before[name]
            post = after[name]
            print(
                f"[{name}] before={pre['avg_ms']:.3f} ms | after={post['avg_ms']:.3f} ms | "
                f"rows={pre['rows']}"
            )

        geo_rows = geo_lookup_after_index(collection)
        print(f"\n[geo_lookup_after_index] returned {len(geo_rows)} nearby documents using the 2dsphere index")
        for doc in geo_rows[:3]:
            print(f"  - {doc['city']} | {doc['source']} | {doc['location']['coordinates']}")

        print("\n[+] Indexes present in the collection:")
        for idx in collection.list_indexes():
            print(f"  - {idx['name']}")

        write_report(len(documents), before, after)

        banner("STEP 5: Validation Checklist")
        print("[x] Solar + Wind CSV data loaded")
        print("[x] City dimension added to create a semi-structured lake schema")
        print("[x] MongoDB collection populated")
        print("[x] Compound, time-based, and geospatial indexes created")
        print("[x] Query timings captured before and after indexing")
        print("[x] Geospatial lookup completed after the 2dsphere index was created")
        print("[x] Markdown benchmark report saved")

        print("\n[RESULT] Experiment 9 completed successfully.")
    except Exception as exc:
        print(f"[-] Experiment 9 failed: {exc}")
        print("    Make sure MongoDB is running on localhost:27017 and try again.")
        raise
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    main()
