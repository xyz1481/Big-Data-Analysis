"""
Experiment 10: Machine Learning Implementation & Final Smart City Showcase

This script builds a compact Smart City demo that combines:
- batch energy data from the Solar and Wind CSV sources,
- a quick ML prediction layer using scikit-learn,
- summarised results from the previous MongoDB lake export,
- a single HTML dashboard for the final showcase.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import accuracy_score, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
REPORT_DIR = BASE_DIR / "report"
SOLAR_FILE = DATA_DIR / "solar_generation.csv"
WIND_FILE = DATA_DIR / "wind_generation.csv"
NO_SQL_LAKE_FILE = OUTPUT_DIR / "exp9_city_energy_lake.jsonl"
DASHBOARD_FILE = OUTPUT_DIR / "exp10_smart_city_dashboard.html"
SUMMARY_FILE = OUTPUT_DIR / "exp10_smart_city_summary.csv"
REPORT_FILE = REPORT_DIR / "exp10_smart_city_showcase_report.md"

CITY_CYCLE = [
    "Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt",
    "Stuttgart", "Leipzig", "Dortmund"
]


def load_energy_dataframe() -> pd.DataFrame:
    solar_df = pd.read_csv(SOLAR_FILE)
    wind_df = pd.read_csv(WIND_FILE)

    solar_df["source"] = "Solar"
    wind_df["source"] = "Wind"
    combined = pd.concat([solar_df, wind_df], ignore_index=True)

    combined["utc_timestamp"] = pd.to_datetime(combined["utc_timestamp"], errors="coerce")
    combined = combined.dropna(subset=["utc_timestamp"]).copy()

    combined["city"] = [CITY_CYCLE[i % len(CITY_CYCLE)] for i in range(len(combined))]
    combined["hour_of_day"] = combined["utc_timestamp"].dt.hour
    combined["day_of_week"] = combined["utc_timestamp"].dt.dayofweek
    combined["month"] = combined["utc_timestamp"].dt.month
    combined["capacity_mw"] = pd.to_numeric(combined["capacity_mw"], errors="coerce").fillna(0.0)
    combined["power_output_mw"] = pd.to_numeric(combined["power_output_mw"], errors="coerce").fillna(0.0)
    if "capacity_factor_pct" not in combined.columns:
        combined["capacity_factor_pct"] = ((combined["power_output_mw"] / combined["capacity_mw"]) * 100).fillna(0.0)
    combined["capacity_factor_pct"] = pd.to_numeric(combined["capacity_factor_pct"], errors="coerce").fillna(0.0)
    combined["demand_index"] = (
        combined["power_output_mw"] / combined["capacity_mw"] * 100
    ).fillna(0.0)

    combined["traffic_load_index"] = (
        25 + (combined["hour_of_day"] / 24) * 55 + (combined["day_of_week"] / 7) * 18 + (combined["demand_index"] / 2)
    )
    combined["pollution_index"] = (
        30 + (combined["month"] / 12) * 42 + (combined["capacity_factor_pct"] / 100) * 35 + (combined["demand_index"] / 5)
    )
    combined["energy_demand_mw"] = (
        combined["power_output_mw"]
        + combined["traffic_load_index"] * 0.7
        + combined["pollution_index"] * 0.5
    )
    combined["risk_level"] = (combined["traffic_load_index"] + combined["pollution_index"]) > 110
    combined["risk_level"] = combined["risk_level"].astype(int)

    source_map = {"Solar": 1, "Wind": 0}
    combined["source_code"] = combined["source"].map(source_map)
    city_map = {city: idx for idx, city in enumerate(CITY_CYCLE)}
    combined["city_code"] = combined["city"].map(city_map)

    return combined


def train_regression_model(df: pd.DataFrame) -> tuple[RandomForestRegressor, dict]:
    feature_cols = [
        "source_code",
        "city_code",
        "hour_of_day",
        "day_of_week",
        "month",
        "capacity_mw",
        "capacity_factor_pct",
        "demand_index",
        "traffic_load_index",
        "pollution_index",
    ]

    X = df[feature_cols]
    y = df["energy_demand_mw"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

    model = RandomForestRegressor(n_estimators=200, random_state=42)
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    metrics = {
        "mae": float(mean_absolute_error(y_test, predictions)),
        "r2": float(r2_score(y_test, predictions)),
        "predict_sample": float(predictions[0]) if len(predictions) else 0.0,
    }
    return model, metrics


def train_classifier_model(df: pd.DataFrame) -> tuple[RandomForestClassifier, dict]:
    feature_cols = [
        "source_code",
        "city_code",
        "hour_of_day",
        "day_of_week",
        "month",
        "capacity_mw",
        "capacity_factor_pct",
        "demand_index",
        "traffic_load_index",
        "pollution_index",
    ]

    X = df[feature_cols]
    y = df["risk_level"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

    model = RandomForestClassifier(n_estimators=200, random_state=42)
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    metrics = {
        "accuracy": float(accuracy_score(y_test, predictions)),
        "prediction_sample": int(predictions[0]) if len(predictions) else 0,
    }
    return model, metrics


def load_lake_summary() -> pd.DataFrame:
    if not NO_SQL_LAKE_FILE.exists():
        return pd.DataFrame(columns=["city", "source", "document_count", "avg_power_output_mw"])

    records = []
    with open(NO_SQL_LAKE_FILE, "r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                records.append(record)
            except json.JSONDecodeError:
                continue

    if not records:
        return pd.DataFrame(columns=["city", "source", "document_count", "avg_power_output_mw"])

    lake_df = pd.DataFrame(records)
    lake_df["power_output_mw"] = pd.to_numeric(lake_df.get("power_output_mw", 0), errors="coerce").fillna(0.0)
    lake_df["city"] = lake_df.get("city", "Unknown")
    lake_df["source"] = lake_df.get("source", "Unknown")

    summary = (
        lake_df.groupby(["city", "source"], as_index=False)["power_output_mw"]
        .agg(document_count="count", avg_power_output_mw="mean")
    )
    summary["document_count"] = summary["document_count"].astype(int)
    summary["avg_power_output_mw"] = summary["avg_power_output_mw"].round(2)
    return summary


def build_dashboard_html(df: pd.DataFrame, regression_metrics: dict, classifier_metrics: dict, lake_summary: pd.DataFrame) -> str:
    source_summary = (
        df.groupby("source", as_index=False)["energy_demand_mw"]
        .agg(avg_demand_mw="mean", peak_demand_mw="max")
        .round({"avg_demand_mw": 2, "peak_demand_mw": 2})
    )

    avg_demand = float(df["energy_demand_mw"].mean())
    peak_demand = float(df["energy_demand_mw"].max())
    risk_share = float(df["risk_level"].mean() * 100)

    table_rows = "<tr><th>City</th><th>Source</th><th>Documents</th><th>Avg Power (MW)</th></tr>"
    for _, row in lake_summary.head(10).iterrows():
        table_rows += (
            f"<tr><td>{row['city']}</td><td>{row['source']}</td><td>{int(row['document_count'])}</td>"
            f"<td>{float(row['avg_power_output_mw']):.2f}</td></tr>"
        )

    if lake_summary.empty:
        table_rows = "<tr><td colspan='4'>No NoSQL lake summary available yet.</td></tr>"

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <title>Smart City Showcase Dashboard</title>
      <style>
        body {{ font-family: Arial, sans-serif; background: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
        h1 {{ color: #f8fafc; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 18px; margin-bottom: 24px; }}
        .card {{ background: #111827; border-radius: 12px; padding: 18px; border: 1px solid #334155; }}
        .metric {{ font-size: 32px; font-weight: bold; color: #38bdf8; }}
        .section {{ background: #111827; border-radius: 12px; padding: 18px; margin-top: 20px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
        th, td {{ border-bottom: 1px solid #334155; padding: 10px; text-align: left; }}
        th {{ background: #1e293b; }}
        .tag {{ display: inline-block; background: #14532d; color: #dcfce7; padding: 5px 10px; border-radius: 999px; margin-right: 8px; font-size: 12px; }}
      </style>
    </head>
    <body>
      <h1>Smart City Showcase Dashboard</h1>
      <div class="tag">Experiment 10</div>
      <div class="tag">ML + Data Lake + Batch + Streaming</div>

      <div class="grid">
        <div class="card">
          <div>Average Demand</div>
          <div class="metric">{avg_demand:.2f} MW</div>
        </div>
        <div class="card">
          <div>Peak Demand</div>
          <div class="metric">{peak_demand:.2f} MW</div>
        </div>
        <div class="card">
          <div>High-Risk Share</div>
          <div class="metric">{risk_share:.1f}%</div>
        </div>
        <div class="card">
          <div>Model Accuracy</div>
          <div class="metric">{classifier_metrics['accuracy'] * 100:.1f}%</div>
        </div>
      </div>

      <div class="section">
        <h2>ML Model Performance</h2>
        <p><strong>Energy Demand Forecast Model:</strong> MAE = {regression_metrics['mae']:.2f} MW, R² = {regression_metrics['r2']:.3f}</p>
        <p><strong>Risk Prediction Model:</strong> Accuracy = {classifier_metrics['accuracy'] * 100:.1f}%</p>
      </div>

      <div class="section">
        <h2>Batch + NoSQL Integration</h2>
        <table>
          {table_rows}
        </table>
      </div>

      <div class="section">
        <h2>Flow Snapshot</h2>
        <p>Data ingestion → batch analytics → streaming metrics → ML forecasting → dashboard visualization</p>
        <p><strong>Sources:</strong> Solar and Wind generation CSVs, MongoDB lake record export, smart city feature engineering</p>
      </div>
    </body>
    </html>
    """
    return html


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    print("#" * 76)
    print(" EXPERIMENT 10: MACHINE LEARNING IMPLEMENTATION & FINAL SMART CITY SHOWCASE")
    print("#" * 76)

    city_df = load_energy_dataframe()
    print(f"[+] Loaded {len(city_df)} smart-city energy records from Solar + Wind inputs")

    regression_model, regression_metrics = train_regression_model(city_df)
    classifier_model, classifier_metrics = train_classifier_model(city_df)

    print(f"[+] Regression model: MAE={regression_metrics['mae']:.3f} MW, R2={regression_metrics['r2']:.3f}")
    print(f"[+] Risk model: Accuracy={classifier_metrics['accuracy'] * 100:.2f}%")

    lake_summary = load_lake_summary()
    print(f"[+] NoSQL lake summary rows: {len(lake_summary)}")

    dashboard_html = build_dashboard_html(city_df, regression_metrics, classifier_metrics, lake_summary)
    DASHBOARD_FILE.write_text(dashboard_html, encoding="utf-8")
    print(f"[+] Dashboard saved: {DASHBOARD_FILE}")

    summary_df = city_df[[
        "city", "source", "hour_of_day", "day_of_week", "month", "power_output_mw", "energy_demand_mw", "risk_level"
    ]].copy()
    summary_df.to_csv(SUMMARY_FILE, index=False)
    print(f"[+] Summary data saved: {SUMMARY_FILE}")

    report_lines = [
        "# Experiment 10 - Machine Learning Implementation & Final Smart City Showcase",
        "",
        "## Objective",
        "This experiment combines the Solar/Wind generation pipeline with machine learning to demonstrate a final smart city decision-support dashboard.",
        "",
        "## Flow",
        "1. Load Solar and Wind datasets",
        "2. Engineer smart-city features such as demand index, traffic load, pollution index and risk level",
        "3. Train regression and classification models with scikit-learn",
        "4. Merge the output with the previous MongoDB lake data",
        "5. Produce a dashboard summary HTML for the final showcase",
        "",
        "## Result",
        f"- Regression model MAE: {regression_metrics['mae']:.3f} MW",
        f"- Regression model R²: {regression_metrics['r2']:.3f}",
        f"- Risk model accuracy: {classifier_metrics['accuracy'] * 100:.2f}%",
        f"- Records processed: {len(city_df)}",
        "",
        "## Dashboard Output",
        f"The interactive HTML dashboard is available at: {DASHBOARD_FILE}",
        "",
        "## Conclusion",
        "The final Smart City showcase demonstrates how city energy data can be transformed into operational insight using ML, analytics, and a single dashboard layer.",
    ]
    REPORT_FILE.write_text("\n".join(report_lines), encoding="utf-8")
    print(f"[+] Report saved: {REPORT_FILE}")

    print("\n[RESULT] Experiment 10 completed successfully.")


if __name__ == "__main__":
    main()
