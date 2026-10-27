"""
=============================================================================
EXPERIMENT 7: Real-Time Streaming Data Pipeline - Kafka Producer
Project: Comparative Study: Solar vs Wind Output
Input: Cleaned Solar/Wind dataset from Experiment 4
Output: Kafka topic messages containing renewable-energy sensor events
=============================================================================

This script reads the cleaned Solar/Wind hourly data and publishes it as JSON
messages to a Kafka topic. The message payload includes the original measured
timestamp plus a real-time event timestamp so the Spark streaming consumer can
demonstrate a meaningful 5-minute sliding window on local demo time.
=============================================================================
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CLEANED_FILE = os.path.join(
    BASE_DIR, "hdfs_simulation", "renewable_energy", "processed", "cleaned_energy_data.csv"
)
DEFAULT_TOPIC = "renewable-energy-stream"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"


def banner(title):
    print("\n" + "=" * 78)
    print(f" >>> {title}")
    print("=" * 78)


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka producer for renewable-energy events")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--limit", type=int, default=60, help="Number of records to send")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay in seconds between records")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print messages instead of sending them to Kafka",
    )
    return parser.parse_args()


def load_records():
    if not os.path.exists(CLEANED_FILE):
        raise FileNotFoundError(
            f"Cleaned dataset not found at: {CLEANED_FILE}\n"
            "Run Experiment 4 first to create cleaned_energy_data.csv."
        )

    df = pd.read_csv(CLEANED_FILE)
    df["utc_timestamp"] = pd.to_datetime(df["utc_timestamp"])
    df = df.sort_values(["utc_timestamp", "source"]).reset_index(drop=True)
    return df


def build_payload(row, sequence_number):
    event_time = datetime.now(timezone.utc).isoformat()
    measurement_time = row["utc_timestamp"].to_pydatetime().replace(tzinfo=timezone.utc).isoformat()
    return {
        "sequence_number": sequence_number,
        "event_time": event_time,
        "measurement_timestamp": measurement_time,
        "country": str(row["country"]),
        "source": str(row["source"]),
        "power_output_mw": float(row["power_output_mw"]),
        "capacity_mw": float(row["capacity_mw"]),
        "capacity_factor_pct": float(row["capacity_factor_pct"]),
    }


def create_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=50,
    )


def main():
    args = parse_args()
    records_df = load_records()

    banner("STEP 1: Loading Cleaned Solar/Wind Data")
    print(f"[+] Source file: {CLEANED_FILE}")
    print(f"[+] Records available: {len(records_df)}")
    print(f"[+] Kafka topic: {args.topic}")
    print(f"[+] Bootstrap servers: {args.bootstrap_servers}")

    banner("STEP 2: Preparing Kafka Producer")
    producer = None
    if not args.dry_run:
        producer = create_producer(args.bootstrap_servers)
        print("[+] Kafka producer initialized successfully.")
    else:
        print("[+] Dry-run mode enabled. Messages will be printed only.")

    banner("STEP 3: Streaming Sensor Events")
    limit = min(args.limit, len(records_df))
    for index in range(limit):
        row = records_df.iloc[index]
        payload = build_payload(row, index + 1)
        message_key = payload["source"].lower()

        if args.dry_run:
            print(
                f"[DRY-RUN] #{payload['sequence_number']:03d} | key={message_key} | "
                f"event_time={payload['event_time']} | measurement={payload['measurement_timestamp']} | "
                f"power={payload['power_output_mw']:.2f} MW"
            )
        else:
            producer.send(args.topic, key=message_key, value=payload)
            print(
                f"[SENT] #{payload['sequence_number']:03d} | key={message_key} | "
                f"event_time={payload['event_time']} | measurement={payload['measurement_timestamp']} | "
                f"power={payload['power_output_mw']:.2f} MW"
            )

        time.sleep(args.delay)

    if producer is not None:
        producer.flush()
        producer.close()
        print("[+] Kafka producer flushed and closed successfully.")

    banner("STEP 4: Producer Summary")
    print(f"[+] Total messages processed: {limit}")
    print("[+] Message flow: cleaned CSV -> Python producer -> Kafka topic")
    print("[RESULT] Experiment 7 Kafka producer completed successfully.")


if __name__ == "__main__":
    main()