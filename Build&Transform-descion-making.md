# Task 3: Design Decisions & Assumptions

## Overview
This document outlines the engineering choices made for the manufacturing data pipeline (Task 3). Each decision is based on production best practices, data characteristics discovered during profiling, and typical warehouse-scale requirements.

---

## Design Decisions Table

| **Component** | **Options Considered** | **Final Choice** | **Rationale** |
|---------------|----------------------|------------------|---------------|
| **Fact Table Grain** | A. Daily (date, factory, line, shift)<br>B. Hourly (timestamp, factory, line)<br>C. Event-level (every production record) | **A. Daily Aggregation** | Standard for BI reporting and shift performance analysis. Reduces data volume while preserving critical business context. Aligns with typical query patterns ("How did Line-A perform yesterday?"). |
| **Performance > 1.0 Handling** | A. Cap at 1.0<br>B. Keep as-is<br>C. Flag as anomaly | **B. Keep as-is** | In manufacturing, performance >1.0 indicates overclocking or conservative baseline cycle times. Capping loses valuable operational insight. Real-world machines do exceed theoretical capacity. |
| **OEE Formula Mismatch** | A. Recalculate and replace<br>B. Trust original column<br>C. Add both (oee_original, oee_calculated) | **C. Add Both Columns** | Defensive engineering practice. Provides transparency for data auditors. Allows detection of sensor drift or calculation lag in source systems. Let analysts decide which to trust. |
| **Missing parts_used** | A. Leave as NULL<br>B. Fill with "No Parts"<br>C. Exclude from cost calculations | **A. Leave as NULL** | For inspections/calibrations, NULL is factually correct. Forcing "No Parts" string complicates downstream numeric analysis. NULL properly represents "not applicable" state. |
| **Partitioning Strategy** | A. Partition by date<br>B. Partition by date + factory_id<br>C. Partition by factory_id + line_id | **A. Partition by Date** | Most efficient for incremental loads. Analysts typically query by time range ("last week"). Date partitioning enables Spark to skip 99% of data. Industry standard for time-series data. |
| **Deduplication Strategy** | A. Drop all duplicates<br>B. Keep first occurrence<br>C. Keep latest by timestamp | **C. Keep Latest** | Most recent data is most accurate. Uses window function (row_number) partitioned by primary key, ordered by timestamp DESC. Production-safe approach. |
| **Outlier Handling** | A. Remove outliers<br>B. Cap outliers<br>C. Flag for investigation | **C. Flag for Investigation** | Outliers represent real operational issues (breakdowns, quality problems). Removing loses critical failure signals. Add `is_outlier` flag column instead. |
| **Data Cleaning Functions** | Must implement minimum 2 | **1. remove_duplicates()**<br>**2. standardize_oee_metrics()** | Remove duplicates ensures data quality. OEE standardization adds calculated column and flags high-performance records (>1.2). Both production-critical. |

---

## Fact Table Schema

**Grain:** One row per **date × factory × line × shift**

**Dimensions (Keys):**
- date (DateType)
- factory_id (String)
- line_id (String)
- shift (String)

**Production Metrics:**
- total_produced (Integer)
- total_scrap (Integer)
- total_defects (Integer)

**OEE Metrics:**
- avg_oee (Double)
- oee_calculated (Double) - Added for transparency
- min_oee (Double)
- max_oee (Double)

**Machine Health:**
- avg_temperature_c (Double)
- avg_vibration_mm_s (Double)
- avg_pressure_bar (Double)

**Downtime & Maintenance:**
- total_downtime_min (Integer)
- maintenance_events (Integer)
- total_maintenance_cost_eur (Double)

**Calculated KPIs:**
- scrap_rate (Double) = total_scrap / total_produced
- efficiency_score (Double) = composite metric

---

## Pipeline Architecture

Following **Medallion Architecture** (Bronze → Silver → Gold):

```
Bronze Layer (Raw)
├── Load CSV files with explicit schemas
├── Validate primary keys (not null)
└── Preserve immutable source data

Silver Layer (Clean)
├── remove_duplicates() - Keep latest by timestamp
├── standardize_oee_metrics() - Add calculated OEE + flags
└── Validate business rules (timestamps, quantities)

Gold Layer (Aggregated)
├── Add derived columns (date, day_of_week)
├── Join: production + maintenance + operators
├── Aggregate to fact table grain
└── Calculate KPIs (scrap_rate, efficiency_score)

Export
├── Parquet: partitioned by date
└── CSV: single file for BI tools
```

---

## Data Quality Rules

| **Rule** | **Action** |
|----------|-----------|
| Duplicate records | Keep latest by timestamp |
| NULL in primary keys | Drop row (invalid) |
| performance > 1.2 | Flag as "High Speed" (keep data) |
| OEE mismatch > 0.01 | Add flag column (keep both values) |
| Negative quantities | Flag as invalid (investigate) |
| end_time < start_time | Flag as invalid (data error) |
| Outliers (z-score > 3) | Flag for investigation (don't remove) |

---

## Key Assumptions

1. **Data Volume:** Current dataset is small (2,016 rows), but pipeline is designed for billions of rows
2. **Timestamps:** All timestamps are in UTC
3. **Primary Keys:** 
   - Production: (timestamp, factory_id, line_id)
   - Maintenance: event_id
   - Operators: operator_id
4. **Update Strategy:** Full refresh (not incremental) for this assessment
5. **Performance Target:** Pipeline completes in <5 minutes for current dataset
6. **Scalability Target:** Linear scaling to 1TB+ datasets

---

## Technology Choices

| **Component** | **Technology** | **Why** |
|---------------|----------------|---------|
| Processing Engine | PySpark 3.5+ | Distributed processing, scales to petabytes |
| Schema Management | Explicit StructType | Type safety, no inference overhead |
| Storage Format | Parquet (primary) | Columnar, compressed, partition-aware |
| Partitioning | By date | Aligns with query patterns (time-based) |
| Deduplication | Window functions | Single-pass, distributed, memory-efficient |
| Aggregations | SQL window functions | Readable, optimized by Catalyst |

---

## Production Considerations

1. **Idempotency:** Pipeline can be re-run safely (overwrites output)
2. **Error Handling:** Invalid records logged but don't crash pipeline
3. **Monitoring:** Row counts logged at each stage
4. **Audit Trail:** Source file and timestamp tracked
5. **Data Lineage:** Each transformation documented in code

---

## References

- Databricks Data Engineering Best Practices - Ebook
- Manufacturing Analytics Standards (ISA-95) 

---

**Document Version:** 1.0  
**Status:** Approved for Implementation