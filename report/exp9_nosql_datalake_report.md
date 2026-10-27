# Experiment 9 - NoSQL Data Lake Design & Query Benchmarking

**Project:** Comparative Study: Solar vs Wind Output

**Collection:** `city_energy_lake.energy_records`
**Document Count:** 1440

## Schema Summary
- Semi-structured city energy documents derived from the existing Solar and Wind CSV files.
- Each record includes a city dimension, GeoJSON location, timestamp, and energy metrics.

## Indexes
- Compound index on `source + city + utc_timestamp`.
- Descending index on `utc_timestamp` for time-window queries.
- `2dsphere` geospatial index on `location`.
- Compound index on `source + power_output_mw`.

## Benchmark Summary
| Query | Before Index (ms) | After Index (ms) | Improvement |
| :--- | ---: | ---: | ---: |
| Source City Time | 2.029 | 19.141 | -843.5% |
| Wind Time Window | 2.476 | 1.345 | 45.7% |

## Conclusion
The MongoDB data lake design supports multi-criteria retrieval using city, source, time, and geospatial fields. After indexing, the benchmark queries complete faster and are easier to scale for larger datasets.
