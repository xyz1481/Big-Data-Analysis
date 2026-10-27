"""
=============================================================================
EXPERIMENT 6: Batch Analytics Visualization & Reporting
Project: Comparative Study: Solar vs Wind Output
Input: Aggregated Parquet outputs from Experiment 5
=============================================================================

Objective:
  Transform the Spark SQL outputs from Experiment 5 into clear visual insights
  using matplotlib charts and generate a concise analytical report.
=============================================================================
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, "output", "exp5_results")
CHART_DIR = os.path.join(BASE_DIR, "output", "exp6_charts")
REPORT_DIR = os.path.join(BASE_DIR, "report")
REPORT_FILE = os.path.join(REPORT_DIR, "exp6_batch_analytics_report.md")


def banner(title):
    print("\n" + "=" * 78)
    print(f" >>> {title}")
    print("=" * 78)


def ensure_dirs():
    os.makedirs(CHART_DIR, exist_ok=True)
    os.makedirs(REPORT_DIR, exist_ok=True)


def load_parquet(name):
    path = os.path.join(INPUT_DIR, name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing Experiment 5 output: {path}")
    return pd.read_parquet(path)


def save_figure(fig, filename):
    path = os.path.join(CHART_DIR, filename)
    fig.savefig(path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    print(f"[+] Saved chart: {path}")


def build_daily_time_series(daily_df):
    pivot = daily_df.pivot(index="date_only", columns="source", values="total_daily_output_mw").sort_index()
    pivot.index = pd.to_datetime(pivot.index)

    fig, ax = plt.subplots(figsize=(12, 5), dpi=150)
    pivot.plot(ax=ax, linewidth=2.5, marker="o")
    ax.set_title("Solar vs Wind Daily Generation Trend", fontsize=14, fontweight="bold")
    ax.set_xlabel("Date")
    ax.set_ylabel("Total Daily Output (MW)")
    ax.grid(True, alpha=0.25)
    ax.legend(title="Source")
    fig.autofmt_xdate()
    save_figure(fig, "exp6_01_daily_time_series.png")


def build_average_output_bar(comparison_df):
    fig, ax = plt.subplots(figsize=(8, 5), dpi=150)
    bars = ax.bar(comparison_df["source"], comparison_df["avg_power_output_mw"], color=["#4C78A8", "#F58518"])
    ax.set_title("Average Power Output Comparison", fontsize=14, fontweight="bold")
    ax.set_xlabel("Source")
    ax.set_ylabel("Average Output (MW)")
    ax.bar_label(bars, fmt="%.2f", padding=3)
    ax.grid(axis="y", alpha=0.25)
    save_figure(fig, "exp6_02_average_output_bar.png")


def build_capacity_factor_bar(comparison_df):
    fig, ax = plt.subplots(figsize=(8, 5), dpi=150)
    bars = ax.bar(comparison_df["source"], comparison_df["avg_capacity_factor_pct"], color=["#54A24B", "#E45756"])
    ax.set_title("Average Capacity Factor Comparison", fontsize=14, fontweight="bold")
    ax.set_xlabel("Source")
    ax.set_ylabel("Average Capacity Factor (%)")
    ax.bar_label(bars, fmt="%.2f", padding=3)
    ax.grid(axis="y", alpha=0.25)
    save_figure(fig, "exp6_03_capacity_factor_bar.png")


def build_hourly_pattern(hourly_df):
    pivot = hourly_df.pivot(index="hour_of_day", columns="source", values="avg_power_output_mw").sort_index()

    fig, ax = plt.subplots(figsize=(12, 5), dpi=150)
    pivot.plot(ax=ax, linewidth=2.5, marker="o")
    ax.set_title("Average Hourly Generation Pattern", fontsize=14, fontweight="bold")
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Average Output (MW)")
    ax.set_xticks(range(0, 24))
    ax.grid(True, alpha=0.25)
    ax.legend(title="Source")
    save_figure(fig, "exp6_04_hourly_generation_pattern.png")


def build_heatmap(hourly_df):
    pivot = hourly_df.pivot(index="source", columns="hour_of_day", values="avg_power_output_mw").fillna(0)

    fig, ax = plt.subplots(figsize=(13, 3.5), dpi=150)
    image = ax.imshow(pivot.values, aspect="auto", cmap="YlOrRd")
    ax.set_title("Average Power Output Heatmap", fontsize=14, fontweight="bold")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns)
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Source")
    fig.colorbar(image, ax=ax, label="Average Output (MW)")
    save_figure(fig, "exp6_05_hourly_heatmap.png")


def compute_report(daily_df, hourly_df, comparison_df):
    solar = comparison_df[comparison_df["source"] == "Solar"].iloc[0]
    wind = comparison_df[comparison_df["source"] == "Wind"].iloc[0]

    solar_daily = daily_df[daily_df["source"] == "Solar"].sort_values("total_daily_output_mw", ascending=False).iloc[0]
    wind_daily = daily_df[daily_df["source"] == "Wind"].sort_values("total_daily_output_mw", ascending=False).iloc[0]

    solar_hourly = hourly_df[hourly_df["source"] == "Solar"].sort_values("avg_power_output_mw", ascending=False).iloc[0]
    wind_hourly = hourly_df[hourly_df["source"] == "Wind"].sort_values("avg_power_output_mw", ascending=False).iloc[0]

    report = f"""# EXPERIMENT 6 - Batch Analytics Visualization & Reporting

**Input Source:** Experiment 5 Parquet outputs  
**Chart Folder:** `output/exp6_charts/`

---

## 1. Experiment Title
Batch Analytics Visualization & Reporting for Solar vs Wind generation.

## 2. Aim
To convert the Spark SQL aggregates from Experiment 5 into clear charts and a short analytical report.

## 3. Objectives
1. Read the aggregated Parquet outputs from Experiment 5.
2. Generate visual comparisons for Solar and Wind.
3. Save charts as image files for the lab record.
4. Summarize the actual observations from the data.

## 4. Dataset Description
- Source: Experiment 5 aggregated Parquet files.
- Underlying data: OPSD Solar and Wind hourly generation records for Germany.

## 5. Tools & Technologies
- Python
- Pandas
- Matplotlib
- Parquet

## 6. Architecture / Workflow

```
Experiment 5 Parquet Outputs
        │
        ▼
Pandas DataFrames
        │
        ▼
Charts + Analytical Report
```

## 7. Step-by-Step Execution
1. Load the Parquet outputs from Experiment 5.
2. Build the daily time-series chart.
3. Plot the average output comparison bar chart.
4. Plot the capacity factor comparison chart.
5. Plot the hourly generation pattern line chart.
6. Plot the heatmap for hour-wise intensity.
7. Write the analysis report using computed values.

## 8. Complete Source Code
The full script is available in [exp6_visualization.py](exp6_visualization.py).

## 9. Explanation of Important Code
- `pivot()` is used to reshape the aggregated data for line charts and heatmaps.
- The report values are computed directly from the aggregated data.
- All charts are saved as PNG files in `output/exp6_charts/`.

## 10. Commands to Run
```bash
python exp6_visualization.py
```

## 11. Actual / Expected Output
Expected output includes:
- Five saved chart images
- A written markdown report
- A terminal confirmation for each saved file

## 12. Output Tables
The report includes the actual top-day and top-hour observations derived from the data.

## 13. Screenshots to Add
1. The terminal showing chart save confirmations.
2. The generated Solar vs Wind comparison charts.
3. The rendered markdown report summary.

## 14. Result
The charts clearly show Solar's daytime shape, Wind's more continuous output, and the differences in average output and capacity factor.

## 15. Conclusion
Experiment 6 converts the Spark SQL batch analytics into visual reporting that is easy to present in a lab record or viva.

## 16. Viva Questions & Answers
1. **Why use a heatmap here?**
   It shows the hour-wise intensity pattern for Solar and Wind in one view.

2. **Why is the daily line chart useful?**
   It highlights how total generation changes day by day.

3. **Why save charts as PNG?**
   PNG is easy to paste into a report or screenshot sheet.

4. **What is the main Solar pattern?**
   Solar rises during daytime, peaks around midday/afternoon, and drops at night.

5. **What is the main Wind pattern?**
   Wind is more continuous and less tied to daytime hours.

## Analytical Observations
- Solar average output is {solar['avg_power_output_mw']:.2f} MW, while Wind average output is {wind['avg_power_output_mw']:.2f} MW.
- Solar average capacity factor is {solar['avg_capacity_factor_pct']:.2f}%, while Wind average capacity factor is {wind['avg_capacity_factor_pct']:.2f}%.
- Solar peak output in the aggregated results is {solar['peak_output_mw']:.1f} MW, while Wind peak output is {wind['peak_output_mw']:.1f} MW.
- Solar has {int(solar['zero_output_hours'])} zero-output hours, while Wind has {int(wind['zero_output_hours'])} zero-output hours.
- The highest daily Solar total is {solar_daily['total_daily_output_mw']:.1f} MW on {pd.to_datetime(solar_daily['date_only']).date()}.
- The highest daily Wind total is {wind_daily['total_daily_output_mw']:.1f} MW on {pd.to_datetime(wind_daily['date_only']).date()}.
- The highest average hourly Solar output occurs at hour {int(solar_hourly['hour_of_day'])}, with {solar_hourly['avg_power_output_mw']:.2f} MW.
- The highest average hourly Wind output occurs at hour {int(wind_hourly['hour_of_day'])}, with {wind_hourly['avg_power_output_mw']:.2f} MW.
"""

    with open(REPORT_FILE, "w", encoding="utf-8") as file_handle:
        file_handle.write(report)

    print(f"[+] Saved report: {REPORT_FILE}")


def main():
    print("#" * 78)
    print("  EXPERIMENT 6: BATCH ANALYTICS VISUALIZATION & REPORTING")
    print("#" * 78)

    ensure_dirs()

    daily_df = load_parquet("daily_generation_summary.parquet")
    hourly_df = load_parquet("hourly_generation_patterns.parquet")
    comparison_df = load_parquet("source_comparison.parquet")

    daily_df["date_only"] = pd.to_datetime(daily_df["date_only"])
    hourly_df["hour_of_day"] = hourly_df["hour_of_day"].astype(int)

    banner("STEP 1: Creating Solar vs Wind Daily Time-Series Chart")
    build_daily_time_series(daily_df)

    banner("STEP 2: Creating Average Output Comparison Chart")
    build_average_output_bar(comparison_df)

    banner("STEP 3: Creating Capacity Factor Comparison Chart")
    build_capacity_factor_bar(comparison_df)

    banner("STEP 4: Creating Hourly Generation Pattern Chart")
    build_hourly_pattern(hourly_df)

    banner("STEP 5: Creating Heatmap")
    build_heatmap(hourly_df)

    banner("STEP 6: Writing Analytical Report")
    compute_report(daily_df, hourly_df, comparison_df)

    print("\n" + "=" * 78)
    print(" [RESULT] Experiment 6 executed successfully.")
    print("=" * 78)


if __name__ == "__main__":
    main()