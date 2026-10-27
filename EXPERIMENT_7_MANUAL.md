# EXPERIMENT 7 - Real-Time Streaming Data Pipeline

**Title:** Comparative Study: Solar vs Wind Output  
**Pipeline Flow:** CSV -> Kafka Producer -> Kafka Topic -> Spark Structured Streaming -> Sliding Window Aggregation -> Console Sink

---

## 1. Experiment Title

Real-Time Streaming Data Pipeline for Solar and Wind sensor events.

---

## 2. Aim

To build a simulated real-time streaming pipeline using Apache Kafka and PySpark Structured Streaming for renewable-energy events.

---

## 3. Objectives

1. Create a Kafka producer from the existing Solar/Wind dataset.
2. Publish records continuously to a Kafka topic.
3. Consume the messages with Spark Structured Streaming.
4. Parse the incoming JSON payload.
5. Apply a sliding-window aggregation.
6. Display the output in the console.

---

## 4. Dataset Description

- Source: `hdfs_simulation/renewable_energy/processed/cleaned_energy_data.csv`
- Origin: OPSD renewable-energy data for Germany (`DE`)
- Fields used in streaming:
  - `utc_timestamp` / `measurement_timestamp`
  - `country`
  - `source`
  - `power_output_mw`
  - `capacity_mw`
  - `capacity_factor_pct`

---

## 5. Tools & Technologies

| Tool                              | Purpose                                  |
| :-------------------------------- | :--------------------------------------- |
| Apache Kafka                      | Message broker for streaming events      |
| kafka-python                      | Kafka producer client in Python          |
| Apache Spark Structured Streaming | Real-time stream consumer and aggregator |
| PySpark                           | Stream parsing and window computations   |

---

## 6. Architecture / Workflow

```
Cleaned Solar/Wind CSV
        │
        ▼
Kafka Producer (Python)
        │
        ▼
Kafka Topic: renewable-energy-stream
        │
        ▼
Spark Structured Streaming Consumer
        │
        ▼
5-minute Sliding Window Aggregation
        │
        ▼
Console Sink
```

---

## 7. Step-by-Step Execution

1. Start Kafka and create the topic.
2. Run the producer script to publish Solar/Wind messages.
3. Run the Spark Structured Streaming consumer.
4. Observe the rolling aggregates in the console.
5. Capture screenshots of the producer and consumer outputs.

---

## 8. Complete Source Code

The complete scripts are available in:

- [exp7_kafka_producer.py](exp7_kafka_producer.py)
- [exp7_streaming.py](exp7_streaming.py)

---

## 9. Explanation of Important Code

- The producer reads the cleaned dataset and sends each row as a JSON payload.
- A real-time `event_time` is added so the stream can be windowed in a local demo.
- The consumer uses `from_json()` to parse Kafka messages into structured columns.
- A 5-minute sliding window is used to calculate average output and capacity factor.

---

## 10. Commands to Run

### Start Kafka

```bat
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
bin\windows\kafka-server-start.bat config\server.properties
```

### Create Topic

```bat
bin\windows\kafka-topics.bat --create --topic renewable-energy-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Start Producer

```bash
python exp7_kafka_producer.py --topic renewable-energy-stream --limit 120 --delay 0.5
```

### Start Spark Streaming Consumer

```bash
python exp7_streaming.py --topic renewable-energy-stream --trigger-seconds 10
```

---

## 11. Actual / Expected Output

Expected console output includes:

- Producer messages showing each record being sent.
- Spark streaming console tables showing windowed Solar/Wind aggregates.
- A clear message flow from Kafka to Spark to the console.

---

## 12. Output Tables

The Spark console sink prints rows such as:

- `window_start`
- `window_end`
- `source`
- `avg_power_output_mw`
- `total_power_output_mw`
- `avg_capacity_factor_pct`
- `peak_power_output_mw`
- `record_count`

---

## 13. Screenshots to Add

1. Kafka topic creation terminal.
2. Producer terminal showing sent Solar/Wind events.
3. Spark Structured Streaming console output.
4. Window aggregation table.

---

## 14. Result

The pipeline streams renewable-energy events from the cleaned dataset through Kafka and aggregates them in Spark using a sliding event-time window.

---

## 15. Conclusion

Experiment 7 demonstrates a simple local streaming architecture suitable for a college practical and viva.

---

## 16. Viva Questions & Answers

1. **Why use Kafka here?**
   Kafka acts as a buffer between the producer and Spark consumer.

2. **Why add a real-time event timestamp?**
   It allows a meaningful 5-minute sliding window in a local demo.

3. **Why use Structured Streaming?**
   It gives Spark a simple and readable streaming API.

4. **Why parse JSON messages?**
   JSON is easy to serialize and inspect in a lab environment.

5. **What does the window aggregation show?**
   It shows short-time rolling Solar/Wind trends and record counts.
