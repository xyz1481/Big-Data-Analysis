# EXPERIMENT 8 - Real-Time Operational Dashboard

**Title:** Comparative Study: Solar vs Wind Output  
**Pipeline Flow:** Kafka -> Spark Streaming -> Processed Output Files -> Streamlit Dashboard -> Live Metrics + Charts + Alerts

---

## 1. Experiment Title

Real-Time Operational Dashboard for Solar and Wind generation.

---

## 2. Aim

To build a simple interactive dashboard that displays live Solar and Wind operational metrics from the streaming pipeline.

---

## 3. Objectives

1. Connect the dashboard to the streaming output files from Experiment 7.
2. Display live Solar power, Wind power, total generation, rolling average, and latest timestamp.
3. Show time-series charts and Solar vs Wind comparison charts.
4. Implement threshold-based alerts.
5. Keep the dashboard simple enough for a college viva/demo.

---

## 4. Dataset Description

- Live streaming output from Experiment 7:
  - `output/exp7_live_metrics/latest_snapshot.csv`
  - `output/exp7_live_metrics/history.csv`
- Fallback baseline data from Experiment 5:
  - `output/exp5_results/source_comparison.parquet`
  - `output/exp5_results/hourly_generation_patterns.parquet`

---

## 5. Tools & Technologies

| Tool                   | Purpose                           |
| :--------------------- | :-------------------------------- |
| Streamlit              | Interactive dashboard UI          |
| Pandas                 | File loading and reshaping        |
| Streamlit Auto Refresh | Periodic refresh of the dashboard |

---

## 6. Architecture / Workflow

```
Kafka
   │
   ▼
Spark Structured Streaming
   │
   ▼
Local Snapshot/History Files
   │
   ▼
Streamlit Dashboard
   │
   ▼
Live Metrics + Charts + Alerts
```

---

## 7. Step-by-Step Execution

1. Run Experiment 7 streaming so the snapshot files are created.
2. Start the Streamlit dashboard.
3. Watch the dashboard update every few seconds.
4. Capture screenshots of the metrics, charts, and alerts.

---

## 8. Complete Source Code

The full implementation is available in [exp8_dashboard.py](exp8_dashboard.py).

---

## 9. Explanation of Important Code

- The dashboard reads the latest snapshot and history CSV files.
- If streaming files are not available, it falls back to Experiment 5 aggregates.
- `st_autorefresh()` keeps the dashboard updating periodically.
- Threshold logic is used to flag high or low generation levels.

---

## 10. Commands to Run

```bash
streamlit run exp8_dashboard.py
```

---

## 11. Actual / Expected Output

Expected dashboard output includes:

- Live Solar and Wind metrics
- Total generation and rolling average
- Status indicators and alerts
- Time-series chart
- Solar vs Wind comparison chart

---

## 12. Output Tables

The dashboard displays the latest snapshot table with columns such as:

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

1. Dashboard metrics cards.
2. Live trend chart.
3. Alerts panel.
4. Solar vs Wind comparison chart.

---

## 14. Result

The dashboard provides a simple live operational view of the renewable-energy stream and can fall back to batch analytics if the stream has not yet been started.

---

## 15. Conclusion

Experiment 8 demonstrates how streamed renewable-energy data can be turned into a compact operational dashboard for monitoring and viva presentation.

---

## 16. Viva Questions & Answers

1. **Why use Streamlit?**
   It is quick to build and easy to demonstrate in a college practical.

2. **Why keep auto-refresh enabled?**
   It helps the dashboard update as new stream snapshots arrive.

3. **Why show a fallback baseline?**
   It keeps the dashboard usable even before streaming data is generated.

4. **What does the rolling average mean?**
   It smooths short-term fluctuations in generation.

5. **Why include alerts?**
   Alerts make the dashboard more operational and easier to interpret.
