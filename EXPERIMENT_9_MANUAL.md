# EXPERIMENT 9 - NoSQL Data Lake Design & Query Benchmarking

**Title:** Comparative Study: Solar vs Wind Output  
**Focus:** Simple MongoDB-based NoSQL data lake design with indexing and query speed comparison

---

## 1. Aim

To design a small and easy-to-explain NoSQL data lake using the existing Solar and Wind CSV data, then compare query performance before and after indexing.

---

## 2. Objective

1. Model semi-structured documents for city-based energy records.
2. Store the data in MongoDB as a NoSQL lake collection.
3. Create useful indexes for common query patterns.
4. Benchmark query speed before and after indexing.
5. Generate a short execution report for viva and lab records.

---

## 3. Why This Experiment?

This experiment shows how the same dataset can be stored in a flexible document format instead of only CSV or relational tables. It is simple, practical, and easy to explain in a viva.

---

## 4. Dataset Used

- `data/solar_generation.csv`
- `data/wind_generation.csv`

The script adds a simple city dimension and geospatial coordinates so that MongoDB indexes can be demonstrated.

---

## 5. Suggested Document Schema

Each MongoDB document uses a structure like this:

```json
{
  "record_id": "lake-00001",
  "lake_zone": "silver",
  "city": "Berlin",
  "region": "BE",
  "country": "DE",
  "source": "Solar",
  "utc_timestamp": "2019-06-01T00:00:00",
  "power_output_mw": 12345.0,
  "capacity_mw": 49497.0,
  "capacity_factor_pct": 24.94,
  "location": {
    "type": "Point",
    "coordinates": [13.405, 52.52]
  },
  "tags": ["renewable", "city-data-lake", "solar"]
}
```

---

## 6. Tools Used

| Tool    | Purpose                                       |
| :------ | :-------------------------------------------- |
| Python  | Main scripting language                       |
| PySpark | Simple CSV ingestion and type standardization |
| MongoDB | NoSQL document store and indexing             |
| PyMongo | MongoDB connection from Python                |
| Pandas  | Small data handling and report export         |

---

## 7. Index Design

The script creates these indexes:

1. Compound index on `source + city + utc_timestamp`
2. Descending time index on `utc_timestamp`
3. Geospatial index on `location` using `2dsphere`
4. Compound index on `source + power_output_mw`

These indexes are enough to explain multi-criteria search, time-based filtering, and location-based lookup.

---

## 8. How to Run

### Step 1: Make sure MongoDB is running

MongoDB should be available at `localhost:27017`.

### Step 2: Run the experiment script

```bash
python exp9_nosql_datalake.py
```

### Step 3: Check the outputs

The script generates:

- a MongoDB collection named `city_energy_lake.energy_records`
- a JSONL export in `output/exp9_city_energy_lake.jsonl`
- a benchmark report in `report/exp9_nosql_datalake_report.md`

---

## 9. Important Commands for Viva

### Basic MongoDB commands

```javascript
show dbs
use city_energy_lake
show collections
db.energy_records.countDocuments()
db.energy_records.findOne()
```

### Query examples

```javascript
db.energy_records.find({ source: "Solar", city: "Berlin" });
db.energy_records.find({
  utc_timestamp: { $gte: ISODate("2019-06-01T00:00:00Z") },
});
db.energy_records.find({
  location: {
    $near: { $geometry: { type: "Point", coordinates: [13.405, 52.52] } },
  },
});
```

### Index commands

```javascript
db.energy_records.getIndexes();
```

---

## 10. Expected Output

The terminal should show:

- total document count
- sample stored document
- benchmark timings before indexing
- index creation messages
- benchmark timings after indexing
- a geospatial lookup result after the `2dsphere` index is created
- validation checklist

---

## 11. Simple Viva Explanation

### What is a NoSQL data lake?

A flexible storage layer that keeps semi-structured documents and allows fast querying with indexes.

### Why use MongoDB here?

It is easier to understand than Cassandra and supports document storage, geospatial indexes, and compound indexes.

### Why do indexes matter?

Indexes reduce search time by helping MongoDB find matching documents faster.

### What changed after indexing?

The same query runs on the same data, but MongoDB can search faster because it uses the indexes.

---

## 12. Short Conclusion

Experiment 9 demonstrates a simple NoSQL data lake design using MongoDB. The schema is flexible, the queries are easy to explain, and indexing improves retrieval performance for city, source, time, and geospatial searches.
