"""
=============================================================================
EXPERIMENT 8: Real-Time Operational Dashboard
Project: Comparative Study: Solar vs Wind Output
Input: Live snapshot/history files written by Experiment 7
Fallback: Aggregated batch analytics from Experiment 5
=============================================================================

The dashboard reads the latest streaming metrics produced by the Structured
Streaming pipeline and displays live operational indicators, charts, and
threshold-based alerts. When live metrics are not yet available, the dashboard
falls back to the batch analytics outputs so the page still opens cleanly in a
college demo environment.
=============================================================================
"""

import os
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh


BASE_DIR = Path(__file__).resolve().parent
LIVE_DIR = BASE_DIR / "output" / "exp7_live_metrics"
EXP5_DIR = BASE_DIR / "output" / "exp5_results"


st.set_page_config(
    page_title="Solar vs Wind Live Dashboard",
    layout="wide",
    page_icon="⚡",
)


def ensure_timestamp_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in ["window_start", "window_end", "captured_at_utc"]:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


@st.cache_data(ttl=5)
def load_live_metrics() -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    snapshot_path = LIVE_DIR / "latest_snapshot.csv"
    history_path = LIVE_DIR / "history.csv"

    snapshot_df = None
    history_df = None

    if snapshot_path.exists():
        snapshot_df = ensure_timestamp_columns(pd.read_csv(snapshot_path))
    if history_path.exists():
        history_df = ensure_timestamp_columns(pd.read_csv(history_path))

    return snapshot_df, history_df


@st.cache_data(ttl=10)
def load_baseline_metrics() -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    comparison_path = EXP5_DIR / "source_comparison.parquet"
    hourly_path = EXP5_DIR / "hourly_generation_patterns.parquet"

    comparison_df = None
    hourly_df = None

    if comparison_path.exists():
        comparison_df = pd.read_parquet(comparison_path)
    if hourly_path.exists():
        hourly_df = pd.read_parquet(hourly_path)

    return comparison_df, hourly_df


def latest_rows(snapshot_df: pd.DataFrame | None) -> pd.DataFrame:
    if snapshot_df is None or snapshot_df.empty:
        return pd.DataFrame()

    time_column = "window_end" if "window_end" in snapshot_df.columns else "captured_at_utc"
    return (
        snapshot_df.sort_values(time_column)
        .groupby("source", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )


def build_time_series(history_df: pd.DataFrame | None) -> pd.DataFrame:
    if history_df is None or history_df.empty:
        return pd.DataFrame()

    working = history_df.copy()
    working["window_end"] = pd.to_datetime(working["window_end"], errors="coerce")
    working = working.dropna(subset=["window_end"])
    if working.empty:
        return pd.DataFrame()

    series = (
        working.sort_values("window_end")
        .pivot_table(index="window_end", columns="source", values="total_power_output_mw", aggfunc="last")
        .fillna(0)
        .sort_index()
    )
    return series


def status_label(value: float, high_threshold: float, low_threshold: float) -> tuple[str, str]:
    if value >= high_threshold:
        return "High", "normal"
    if value <= low_threshold:
        return "Low", "inverse"
    return "Normal", "off"


st_autorefresh(interval=5000, key="exp8_dashboard_refresh")

st.title("Real-Time Operational Dashboard")
st.caption("Experiment 8: Solar vs Wind live monitoring from Experiment 7 streaming output")

live_snapshot_df, live_history_df = load_live_metrics()
baseline_comparison_df, baseline_hourly_df = load_baseline_metrics()

using_live_data = live_snapshot_df is not None and not live_snapshot_df.empty

if using_live_data:
    data_source = "Experiment 7 live snapshot/history"
    latest_df = latest_rows(live_snapshot_df)
    history_plot_df = build_time_series(live_history_df)
else:
    data_source = "Experiment 5 baseline aggregates"
    latest_df = pd.DataFrame()
    history_plot_df = pd.DataFrame()

if latest_df.empty and baseline_comparison_df is not None:
    latest_df = baseline_comparison_df.copy()
    latest_df["window_end"] = pd.Timestamp.utcnow()
    latest_df["avg_power_output_mw"] = latest_df["avg_power_output_mw"].astype(float)
    latest_df["total_power_output_mw"] = latest_df["avg_power_output_mw"]
    latest_df["avg_capacity_factor_pct"] = latest_df["avg_capacity_factor_pct"].astype(float)
    latest_df["peak_power_output_mw"] = latest_df["peak_output_mw"].astype(float)
    latest_df["record_count"] = 0

if history_plot_df.empty and baseline_hourly_df is not None:
    history_plot_df = (
        baseline_hourly_df.pivot_table(index="hour_of_day", columns="source", values="avg_power_output_mw", aggfunc="mean")
        .fillna(0)
        .sort_index()
    )

st.sidebar.header("Operational Status")
st.sidebar.write(f"Data source: {data_source}")
st.sidebar.write("Auto-refresh: every 5 seconds")

if latest_df.empty:
    st.warning("No live snapshot found yet. Start Experiment 7 streaming to populate output/exp7_live_metrics/.")
else:
    latest_df = latest_df.sort_values("source")
    solar_row = latest_df[latest_df["source"] == "Solar"].iloc[0] if (latest_df["source"] == "Solar").any() else None
    wind_row = latest_df[latest_df["source"] == "Wind"].iloc[0] if (latest_df["source"] == "Wind").any() else None

    solar_value = float(solar_row["avg_power_output_mw"] if solar_row is not None and "avg_power_output_mw" in solar_row else solar_row["total_power_output_mw"]) if solar_row is not None else 0.0
    wind_value = float(wind_row["avg_power_output_mw"] if wind_row is not None and "avg_power_output_mw" in wind_row else wind_row["total_power_output_mw"]) if wind_row is not None else 0.0
    total_value = solar_value + wind_value
    rolling_average = float(latest_df["avg_power_output_mw"].mean()) if "avg_power_output_mw" in latest_df.columns else total_value / max(len(latest_df), 1)
    latest_timestamp = latest_df["window_end"].max() if "window_end" in latest_df.columns else pd.Timestamp.utcnow()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Solar Power Output", f"{solar_value:,.2f} MW")
    col2.metric("Wind Power Output", f"{wind_value:,.2f} MW")
    col3.metric("Total Generation", f"{total_value:,.2f} MW")
    col4.metric("Rolling Average", f"{rolling_average:,.2f} MW")

    st.subheader("Latest Snapshot")
    st.dataframe(latest_df, use_container_width=True, hide_index=True)

    st.subheader("Operational Alerts")
    if baseline_comparison_df is not None and not baseline_comparison_df.empty:
        baseline_lookup = baseline_comparison_df.set_index("source")
        solar_avg = float(baseline_lookup.loc["Solar", "avg_power_output_mw"]) if "Solar" in baseline_lookup.index else solar_value
        wind_avg = float(baseline_lookup.loc["Wind", "avg_power_output_mw"]) if "Wind" in baseline_lookup.index else wind_value
    else:
        solar_avg = solar_value
        wind_avg = wind_value

    solar_status, solar_mode = status_label(solar_value, solar_avg * 1.2, solar_avg * 0.25)
    wind_status, wind_mode = status_label(wind_value, wind_avg * 1.2, wind_avg * 0.25)
    total_status = "Low" if total_value < (solar_avg + wind_avg) * 0.5 else "Normal"

    alert_col1, alert_col2, alert_col3 = st.columns(3)
    alert_col1.info(f"Solar status: {solar_status}")
    alert_col2.info(f"Wind status: {wind_status}")
    alert_col3.info(f"System status: {total_status}")

    if solar_status == "High":
        st.success("High Solar generation detected.")
    elif solar_status == "Low":
        st.warning("Solar generation is unusually low.")

    if wind_status == "High":
        st.success("High Wind generation detected.")
    elif wind_status == "Low":
        st.warning("Wind generation is unusually low.")

    if total_status == "Low":
        st.error("Total generation is below the normal operating range.")

    st.subheader("Live Trend")
    if history_plot_df.empty:
        st.info("No historical live trend is available yet. Start Experiment 7 streaming to populate the trend chart.")
    else:
        if isinstance(history_plot_df.index, pd.Index) and history_plot_df.index.name == "window_end":
            chart_df = history_plot_df.copy()
            chart_df.index.name = None
            st.line_chart(chart_df)
        else:
            st.line_chart(history_plot_df)

    st.subheader("Solar vs Wind Comparison")
    comparison_view = latest_df[[c for c in latest_df.columns if c in ["source", "avg_power_output_mw", "total_power_output_mw", "avg_capacity_factor_pct", "peak_power_output_mw"]]].copy()
    if not comparison_view.empty:
        chart_column = "avg_power_output_mw" if "avg_power_output_mw" in comparison_view.columns else "total_power_output_mw"
        st.bar_chart(comparison_view.set_index("source")[[chart_column]])

    st.caption(f"Latest update: {latest_timestamp}")

st.markdown(
    """
**Dashboard Flow**  
Kafka -> Spark Streaming -> Local Snapshot/History Files -> Streamlit Dashboard -> Live Metrics + Charts + Alerts
"""
)