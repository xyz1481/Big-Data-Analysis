# EXPERIMENT 6 - Batch Analytics Visualization & Reporting

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

- Solar average output is 8964.28 MW, while Wind average output is 9144.17 MW.
- Solar average capacity factor is 18.09%, while Wind average capacity factor is 18.51%.
- Solar peak output in the aggregated results is 29967.0 MW, while Wind peak output is 33025.0 MW.
- Solar has 179 zero-output hours, while Wind has 0 zero-output hours.
- The highest daily Solar total is 264546.0 MW on 2019-06-29.
- The highest daily Wind total is 626233.0 MW on 2019-06-08.
- The highest average hourly Solar output occurs at hour 16, with 24625.40 MW.
- The highest average hourly Wind output occurs at hour 3, with 11266.10 MW.
