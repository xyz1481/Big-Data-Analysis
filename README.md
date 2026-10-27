# Comparative Study: Solar vs Wind Output

### Big Data Laboratory Portfolio (Experiments 1 to 9)

A structured academic Big Data engineering project investigating time-series power generation patterns from the European electrical power grid using **Apache Spark (PySpark)**, **Hadoop Distributed File System (HDFS)**, and **MongoDB NoSQL**.

---

## System Architecture

```
[ Stage 1: Solar & Wind CSV Datasets (Open Power System Data - 60min Hourly) ]
                                    │
                                    ▼
[ Stage 2: Hadoop HDFS Storage (/renewable_energy/raw/solar/ & /wind/) ]
                                    │
                                    ▼
[ Stage 3: PySpark Processing Engine (Data cleaning, schema validation, capacity factor) ]
                                    │
                                    ▼
[ Stage 4: Processed Data Staging (/renewable_energy/processed/) ]
                                    │
                                    ▼
[ Stage 5: MongoDB Document Store (Database: renewable_db | Collection: energy_output) ]
                                    │
                                    ▼
[ Stage 6: Comparative Analytics & Reporting (Peak output, diurnal cycles, capacity utilization) ]
```

---

## Master Deliverables Matrix

| Experiment # | Experiment Title                                | Script                                          | Datasets / Artifacts                                                           | Lab Manual Documentation                         | Screenshots                                                                                                                                        |
| :----------: | :---------------------------------------------- | :---------------------------------------------- | :----------------------------------------------------------------------------- | :----------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------- |
|  **Exp 1**   | **Domain Selection, Setup & EDA**               | `verify_environment.py`<br>`exp1_eda.py`        | `data/solar_generation.csv`<br>`data/wind_generation.csv`                      | [EXPERIMENT_1_MANUAL.md](EXPERIMENT_1_MANUAL.md) | `screenshots/exp1_01_environment_check.png`<br>`screenshots/exp1_02_eda_comparison.png`                                                            |
|  **Exp 2**   | **Project Proposal & Architecture**             | `exp2_generate_architecture.py`                 | `architecture_diagram.png`<br>`architecture.drawio`                            | [EXPERIMENT_2_MANUAL.md](EXPERIMENT_2_MANUAL.md) | `screenshots/exp2_01_architecture_diagram.png`<br>`screenshots/exp2_02_generator_terminal.png`                                                     |
|  **Exp 3**   | **MapReduce on Unstructured Text**              | `exp3_mapreduce.py`                             | `data/renewable_energy_text.txt`                                               | [EXPERIMENT_3_MANUAL.md](EXPERIMENT_3_MANUAL.md) | `screenshots/exp3_01_map_phase.png`<br>`screenshots/exp3_02_reduce_and_filter.png`                                                                 |
|  **Exp 4**   | **Ingestion Pipeline & MongoDB Staging**        | `exp4_pipeline.py`                              | `cleaned_energy_data.csv`<br>MongoDB: `renewable_db`                           | [EXPERIMENT_4_MANUAL.md](EXPERIMENT_4_MANUAL.md) | `screenshots/exp4_01_pyspark_ingestion.png`<br>`screenshots/exp4_02_mongodb_staging_queries.png`<br>`screenshots/exp4_03_validation_checklist.png` |
|  **Exp 5**   | **Batch Analytics with Spark SQL**              | `exp5_spark_sql.py`                             | `output/exp5_results/`<br>Parquet aggregates                                   | [EXPERIMENT_5_MANUAL.md](EXPERIMENT_5_MANUAL.md) | Terminal screenshots to capture from `exp5_spark_sql.py`                                                                                           |
|  **Exp 6**   | **Batch Analytics Visualization & Reporting**   | `exp6_visualization.py`                         | `output/exp6_charts/`<br>`report/exp6_batch_analytics_report.md`               | `report/exp6_batch_analytics_report.md`          | Terminal output and generated chart images                                                                                                         |
|  **Exp 7**   | **Real-Time Streaming Data Pipeline**           | `exp7_kafka_producer.py`<br>`exp7_streaming.py` | Kafka topic `renewable-energy-stream`<br>`output/exp7_checkpoint/`             | [EXPERIMENT_7_MANUAL.md](EXPERIMENT_7_MANUAL.md) | Kafka producer console<br>Spark streaming console                                                                                                  |
|  **Exp 8**   | **Real-Time Operational Dashboard**             | `exp8_dashboard.py`                             | `output/exp7_live_metrics/`<br>`output/exp5_results/`                          | [EXPERIMENT_8_MANUAL.md](EXPERIMENT_8_MANUAL.md) | Streamlit dashboard screenshots                                                                                                                    |
|  **Exp 9**   | **NoSQL Data Lake Design & Query Benchmarking** | `exp9_nosql_datalake.py`                        | `output/exp9_city_energy_lake.jsonl`<br>`report/exp9_nosql_datalake_report.md` | [EXPERIMENT_9_MANUAL.md](EXPERIMENT_9_MANUAL.md) | Terminal output and benchmark report                                                                                                               |

---

## Interactive Web Portfolio

An interactive HTML laboratory portfolio is deployed live and included locally:

- **Live Deployment:** [https://big-data-analysis.vercel.app](https://big-data-analysis.vercel.app)
- **Local File:** Open [`index.html`](index.html) in any web browser to view the complete interactive dashboard with embedded terminal figures, statistical comparison tables, and full-screen image zoom.

---

## Dataset Information

- **Primary Source:** [Open Power System Data (OPSD) Time Series](https://data.open-power-system-data.org/time_series/)
- **Geographic Coverage:** German Transmission Grid Zone (`DE`).
- **Resolution:** 60-minute (hourly) intervals across 720 observations (June 2019).
- **Core Attributes:**
  - `utc_timestamp`: Standardized ISO-8601 UTC observation timestamp.
  - `country`: Regional grid code (`DE`).
  - `source`: Energy source category (`Solar` or `Wind`).
  - `power_output_mw`: Actual electrical power generated and supplied to the grid (in Megawatts).
  - `capacity_mw`: Nameplate installed generator capacity (in Megawatts).

---

## Quickstart & Execution Guide

### 1. Prerequisites

- **Python:** 3.8+ (Tested on Python 3.12)
- **Java:** Java SE 8, 11, 17, or 22 (Required for Apache Spark JVM engine)
- **MongoDB:** Community Server (Running on `localhost:27017`)

### 2. Install Required Python Libraries

```bash
pip install pyspark pandas pymongo matplotlib Pillow requests kafka-python pyarrow streamlit streamlit-autorefresh
```

### 3. Verify Environment

```bash
python verify_environment.py
```

### 4. Execute Individual Experiments

```bash
# Experiment 1: Exploratory Data Analysis
python exp1_eda.py

# Experiment 2: Generate Architecture Diagram
python exp2_generate_architecture.py

# Experiment 3: PySpark RDD MapReduce Processing
python exp3_mapreduce.py

# Experiment 4: End-to-End Data Ingestion Pipeline to MongoDB
python exp4_pipeline.py

# Experiment 5: Batch Analytics with Spark SQL
python exp5_spark_sql.py

# Experiment 6: Batch Analytics Visualization & Reporting
python exp6_visualization.py

# Experiment 7: Real-Time Streaming Data Pipeline
python exp7_kafka_producer.py --topic renewable-energy-stream --limit 120 --delay 0.5
python exp7_streaming.py --topic renewable-energy-stream --trigger-seconds 10

# Experiment 8: Real-Time Operational Dashboard
streamlit run exp8_dashboard.py

# Experiment 9: NoSQL Data Lake Design & Query Benchmarking
python exp9_nosql_datalake.py
```

### 5. Automated One-Click Compilation

To run all experiments sequentially and automatically generate terminal verification screenshots into the `screenshots/` directory:

```bash
python compile_all_experiments_and_screenshots.py
```

---

## Comparative Statistical Findings

| Metric Attribute          | Solar Power Generation | Wind Power Generation | Engineering Significance                                   |
| :------------------------ | :--------------------- | :-------------------- | :--------------------------------------------------------- |
| **Installed Capacity**    | 49,497.0 MW            | 49,370.0 MW           | Baseline parity allows direct comparison                   |
| **Minimum Output**        | **0.0 MW**             | **1,196.0 MW**        | Solar drops to zero at night; wind operates 24/7           |
| **Peak Output (Max)**     | 29,967.0 MW            | 33,025.0 MW           | Solar peaks sharply at midday; wind surges in storm fronts |
| **Mean Generation**       | 8,964.28 MW            | 9,144.17 MW           | Average yield within 2% variance over the month            |
| **Zero-Generation Hours** | **179 hrs (24.86%)**   | **0 hrs (0.00%)**     | Empirical demonstration of solar diurnal cycle             |
| **Capacity Factor**       | 18.11%                 | 18.52%                | Comparable monthly asset utilization                       |

---

## Repository Structure

```text
├── data/
│   ├── solar_generation.csv         # 720 real hourly solar records from OPSD
│   ├── wind_generation.csv          # 720 real hourly wind records from OPSD
│   └── renewable_energy_text.txt    # Authentic scientific corpus for MapReduce
├── screenshots/
│   ├── exp1_01_environment_check.png
│   ├── exp1_02_eda_comparison.png
│   ├── exp2_01_architecture_diagram.png
│   ├── exp2_02_generator_terminal.png
│   ├── exp3_01_map_phase.png
│   ├── exp3_02_reduce_and_filter.png
│   ├── exp4_01_pyspark_ingestion.png
│   ├── exp4_02_mongodb_staging_queries.png
│   └── exp4_03_validation_checklist.png
├── EXPERIMENT_1_MANUAL.md           # Exp 1 complete write-up & screenshot guide
├── EXPERIMENT_2_MANUAL.md           # Exp 2 proposal, architecture & Draw.io guide
├── EXPERIMENT_3_MANUAL.md           # Exp 3 PySpark RDD MapReduce write-up
├── EXPERIMENT_4_MANUAL.md           # Exp 4 pipeline & MongoDB NoSQL write-up
├── DELIVERABLES_GUIDE.md            # Comprehensive deliverables breakdown
├── VIVA_CHEAT_SHEET.md              # 13 core viva questions and answers
├── architecture_diagram.png         # High-resolution 300 DPI architecture diagram
├── architecture.drawio              # Editable XML Draw.io architecture file
├── compile_all_experiments_and_screenshots.py # Master automation compiler
├── verify_environment.py            # Environment validation script
├── exp1_eda.py                      # Exp 1 EDA script
├── exp2_generate_architecture.py    # Exp 2 diagram generator
├── exp3_mapreduce.py                # Exp 3 PySpark RDD MapReduce script
├── exp4_pipeline.py                 # Exp 4 pipeline & MongoDB loader
├── exp5_spark_sql.py                # Exp 5 Spark SQL batch analytics
├── exp6_visualization.py            # Exp 6 visualization & reporting
├── exp7_kafka_producer.py           # Exp 7 Kafka producer
├── exp7_streaming.py                # Exp 7 Spark streaming consumer
├── exp8_dashboard.py                # Exp 8 Streamlit operational dashboard
├── exp9_nosql_datalake.py           # Exp 9 NoSQL data lake benchmarking
├── download_real_datasets.py        # OPSD automated data fetcher
├── index.html                       # Web-based laboratory portfolio
└── README.md                        # Main project documentation
```

---

## License & Attribution

- Data: Open Power System Data (OPSD) &mdash; Creative Commons Attribution 4.0 International (CC BY 4.0).
- Syllabus: Academic Big Data Laboratory Project.
