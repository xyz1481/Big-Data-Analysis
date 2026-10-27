# EXPERIMENT 10 - Machine Learning Implementation & Final Smart City Showcase

**Title:** Smart City Demo: Prediction + Dashboard Integration  
**Focus:** ML-based demand and risk modeling integrated with the analytics pipeline and final showcase dashboard

---

## 1. Aim

To combine the energy analytics, streaming outputs, and MongoDB data lake into one smart city showcase that uses machine learning to forecast city demand and detect risk conditions.

---

## 2. Objective

1. Use the available Solar and Wind generation datasets to create a city-level smart energy dataset.
2. Train machine learning models for demand forecasting and risk prediction.
3. Combine the prediction outputs with batch analytics and the previous MongoDB data lake summary.
4. Generate a single dashboard that presents the final system demonstration.
5. Save the results as a lab report and dashboard output.

---

## 3. Methodology

The script creates a compact smart-city dataset by:

- loading the Solar and Wind CSV files,
- assigning city labels in a cyclic pattern,
- deriving features like hour, weekday, month, demand index, traffic load, and pollution index,
- creating a target `energy_demand_mw` for regression,
- creating a binary `risk_level` label for classification.

Two ML models are trained using scikit-learn:

- `RandomForestRegressor` for energy demand prediction
- `RandomForestClassifier` for traffic/pollution risk prediction

The final dashboard output is saved as a static HTML file under the `output` folder and includes a summary of ML metrics and data lake records.

---

## 4. Tools Used

| Tool | Purpose |
| :--- | :--- |
| Python | Core implementation |
| Pandas | Data preparation and aggregation |
| scikit-learn | Machine learning model training |
| MongoDB export | NoSQL data lake summary integration |
| HTML dashboard | Final live-style showcase view |

---

## 5. How to Run

```bash
python exp10_smart_city_showcase.py
```

This will generate:

- `output/exp10_smart_city_dashboard.html`
- `output/exp10_smart_city_summary.csv`
- `report/exp10_smart_city_showcase_report.md`

---

## 6. Expected Output

The terminal should display:

- the number of records processed,
- the ML model results,
- the saved dashboard path,
- the saved report path,
- final completion message.

---

## 7. Viva Explanation

### What is the Smart City showcase?
It is a final demonstration that combines data ingestion, analysis, prediction, and visualization in one operational flow.

### Why use machine learning here?
The model helps predict future demand and identify high-risk operating periods so that the city system can react proactively.

### Why is the dashboard important?
It provides a single user-facing layer that shows operational state, forecast quality, and city-level summary from the same data pipeline.

---

## 8. Short Conclusion

Experiment 10 demonstrates a complete smart-city analytics flow by combining solar and wind energy data, machine learning models, and a final dashboard presentation. It integrates the learning from the previous experiments and shows how predictive analysis can support final lab and viva-level project storytelling.
