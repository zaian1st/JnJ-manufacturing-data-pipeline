# Manufacturing Data Pipeline

## Overview

This repository contains a modular PySpark End-to-End ETL pipeline designed for manufacturing analytics. The project transforms raw factory telemetry, operator rosters, and maintenance logs into an integrated, analytics-ready fact table using a Medallion architecture approach.

## Project Structure

```text
manufacturing-data-pipeline/
├── .github/
│   └── workflows/          # GitHub Actions CI/CD automation
├── data/
│   ├── raw/                # Source CSV files (Immutable)
│   └── processed/          # Pipeline outputs (Parquet/CSV)
├── sql/
│   ├── queries/            # Raw SQL script files
│   └── run_queries.py      # SQL-based metric exploration engine
├── src/
│   ├── data_loader.py      # Logic for schema-enforced reading
│   ├── data_cleaner.py     # Deduplication and data quality logic
│   ├── data_transformer.py # Business logic and Fact Table creation
│   └── pipeline.py         # Main orchestration script
├── tests/
│   ├── conftest.py         # Pytest fixtures and Spark configuration
│   ├── test_cleaner.py     # Unit tests for cleaning functions
│   └── test_transformer.py # Unit tests for transformation logic
├── .gitignore              # Excludes local environments and logs
├── requirements.txt        # Python dependency list
└── README.md               # Project documentation

```

## Setup and Execution

### Prerequisites

* Python 3.11+
* Java 17 (Required for PySpark)

### Installation

```powershell
# Clone repository
git clone https://github.com/zaian1st/JnJ-manufacturing-data-pipeline.git
cd manufacturing-data-pipeline

# Create virtual environment
python -m venv test1_env
test1_env\Scripts\activate

# Install dependencies
pip install -r requirements.txt

```

## Usage

### Run ETL Pipeline

Executes full pipeline: **Load → Clean → Transform → Export**

```powershell
python src/pipeline.py

```

**Outputs:**

* `data/processed/parquet/` - High-performance Parquet storage.
* `data/processed/csv/fact_table.csv` - Business-ready CSV export.

### Run SQL Analysis

Executes SQL-based exploration to answer key maintenance and downtime questions:

```powershell
python sql/run_queries.py

```

### Run Unit Tests & Coverage

```powershell
# Set environment for Windows Spark
$env:PYSPARK_PYTHON = "python"
$env:PYSPARK_DRIVER_PYTHON = "python"

# Run tests
pytest tests/ -v --cov=src

```

## Architecture

1. **Bronze Layer**: Raw CSVs loaded with strict `StructType` schemas to ensure data type safety.
2. **Silver Layer**: Deduplicated data using Window functions and standardized OEE calculations.
3. **Gold Layer**: Fact table aggregated at the grain of **Date × Factory × Line × Shift**.

## Design Decisions & Engineering Rationale

### Explicit Schemas

* **Performance**: Eliminates schema inference, which requires a full scan of the dataset ( overhead). By providing the `StructType` metadata upfront, Spark initializes execution plans instantly.
* **Data Integrity**: Prevents "type-drift" where numeric sensor data (e.g., Vibration) might be misclassified as strings due to dirty input files, ensuring downstream mathematical operations remain safe.

### Silver Layer: Data Cleaning Strategy

The cleaning logic in `data_cleaner.py` is designed to transform raw telemetry into a trusted source of truth:

* **Window-Based Deduplication**:
Instead of a standard `dropDuplicates()`, the pipeline utilizes `Window.partitionBy("timestamp", "factory_id", "line_id")` ordered by timestamp descending. This ensures the pipeline is **Idempotent**—if a file is reprocessed, the logic always preserves the most recent sensor state.
* **OEE Synchronization Audit**:
The creation of `oee_calculated` serves as a data quality check. By multiplying , we can programmatically identify "Sensor Synchronization Errors" where the reported OEE disagrees with its constituent parts.
* **Anomaly Flagging (Is_High_Performance)**:
Setting a performance threshold of  proactively flags **Baseline Drift** or **Sensor Double Counting**. This allows the business to filter out hardware-level logic errors before they skew financial reports.
* **Null Defense**:
The use of `F.coalesce(col, F.lit(0.0))` prevents a single failed sensor from "poisoning" a record. Without this, a single missing telemetry value would cause the entire OEE calculation to result in `null`.

### Gold Layer: Fact Table Transformation

* **Aggregation Grain**:
Data is rolled up to the **Date × Factory × Line × Shift** level. This specific grain allows management to compare performance across global facilities while maintaining enough granularity to identify underperforming shifts.
* **Maintenance Attribution**:
Downtime is joined at the Line level. Since maintenance logs often lack shift-specific timestamps, the pipeline attributes maintenance costs to all active shifts on that line for that day, providing a comprehensive view of operational impact.

### Storage Strategy

* **Parquet Format**: Optimized for analytical queries using columnar storage.
* **Single-File CSV Export**: For accessibility in downstream BI tools (Excel/PowerBI) that do not support partitioned Parquet directories.

## Engineering Assumptions

* Raw data is immutable; all modifications happen in memory and are written to `processed/`.
* Maintenance logs are attributed to the Line level and shared across all active shifts for that specific date.
* `event_id` and `operator_id` are unique identifiers for join and deduplication logic.

---

**Author:** Zaian

**Date:** 18 January 2026

