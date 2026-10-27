# EXPERIMENT 5 - Batch Analytics with Spark SQL

**Title:** Comparative Study: Solar vs Wind Output  
**Pipeline Flow:** Cleaned CSV -> PySpark -> Spark SQL -> Aggregated Parquet Outputs  
**Dataset:** `hdfs_simulation/renewable_energy/processed/cleaned_energy_data.csv`  
**Output Folder:** `output/exp5_results/`

---

## 1. Experiment Title

Batch Analytics with Spark SQL on Solar and Wind hourly generation data.

---

## 2. Aim

To execute structured Spark SQL queries on the cleaned Solar and Wind dataset to analyze historical generation trends, hourly behavior, daily/monthly patterns, peak output periods, and source-wise comparisons.

---

## 3. Objectives

1. Read the cleaned Solar/Wind dataset into a PySpark DataFrame.
2. Create a temporary Spark SQL view.
3. Run Spark SQL queries for total records, averages, min/max values, and capacity factor.
4. Analyze hourly, daily, and monthly generation patterns.
5. Demonstrate a window function.
6. Show Spark execution plans using `explain()`.
7. Save important aggregate results as Parquet.

---

## 4. Dataset Description

- **Source file:** `hdfs_simulation/renewable_energy/processed/cleaned_energy_data.csv`
- **Original data:** Open Power System Data (OPSD), Germany (`DE`)
- **Rows:** 1,440 combined hourly records
- **Columns:**
  - `utc_timestamp`
  - `country`
  - `source`
  - `power_output_mw`
  - `capacity_mw`
  - `capacity_factor_pct`

---

## 5. Tools & Technologies

| Tool                   | Purpose                                      |
| :--------------------- | :------------------------------------------- |
| Apache Spark / PySpark | Batch SQL analytics and aggregations         |
| Spark SQL              | Structured queries on cleaned energy data    |
| Pandas                 | Pretty terminal tables for screenshot output |
| Parquet                | Storage format for aggregated outputs        |

---

## 6. Architecture / Workflow

```
cleaned_energy_data.csv
        │
        ▼
PySpark DataFrame
        │
        ▼
Temporary Spark SQL View (energy_data)
        │
        ├── Totals / Averages / Min-Max Queries
        ├── Hourly / Daily / Monthly Aggregations
        ├── Window Function Ranking and Rolling Average
        ▼
Parquet Outputs in output/exp5_results/
```

---

## 7. Step-by-Step Execution

1. Ensure Experiment 4 has already created `cleaned_energy_data.csv`.
2. Run the Spark SQL script.
3. Observe the terminal output for query results and execution plans.
4. Check the `output/exp5_results/` folder for Parquet output.

---

## 8. Complete Source Code

The complete implementation is available in [exp5_spark_sql.py](exp5_spark_sql.py).

---

## 9. Explanation of Important Code

- The script reads the cleaned dataset from the Experiment 4 staging folder.
- `createOrReplaceTempView("energy_data")` makes the DataFrame queryable with Spark SQL.
- `row_number()` is used to identify the peak generation hours per source.
- A rolling 24-hour average is computed with a Spark window specification.
- Aggregated outputs are written to Parquet for reuse in Experiment 6.

---

## 10. Commands to Run

```bash
python exp5_spark_sql.py
```

---

## 11. Actual / Expected Output

Expected terminal output includes:

- Total records by source
- Average, minimum, maximum output
- Average capacity factor
- Hourly generation pattern table
- Daily and monthly summaries
- Peak generation periods
- Rolling 24-hour window sample rows
- Source comparison table
- Spark execution plans

---

## 12. Output Tables

The script prints these tables in the terminal:

- Total Records by Source
- Source-wise Power Output Summary
- Minimum and Maximum Output by Source
- Average Capacity Factor by Source
- Hourly Generation Pattern
- Daily Generation Summary
- Monthly Generation Summary
- Top 5 Peak Generation Hours by Source
- Rolling 24-Hour Average Output sample
- Solar vs Wind Comparison

---

## 13. Screenshots to Add

Capture screenshots for:

1. Dataset loading and schema output.
2. Spark SQL query results.
3. Window function output.
4. Execution plan output.
5. Parquet save confirmation.

---

## 14. Result

Spark SQL successfully analyzed the cleaned Solar and Wind records and produced source-wise, hourly, daily, monthly, and window-based summaries.

---

## 15. Conclusion

Experiment 5 demonstrates batch analytics using Spark SQL on the renewable-energy dataset. The outputs provide a clean foundation for visualization and reporting in Experiment 6.

---

## 16. Viva Questions & Answers

1. **Why use Spark SQL instead of only DataFrame operations?**
   Spark SQL makes structured analysis easier to read and closer to relational query style.

2. **What is the purpose of a temporary view?**
   It allows a DataFrame to be queried like a SQL table.

3. **Why are window functions useful?**
   They calculate ranking or rolling metrics without collapsing the rows.

4. **Why save results as Parquet?**
   Parquet is efficient, columnar, and easy to reuse in later experiments.

5. **What is the advantage of daily and monthly grouping?**
   It reveals time-based trends in generation behavior.
