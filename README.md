# Manufacturing Data Pipeline

## Overview

This repository contains a modular PySpark End-to-End ETL pipeline designed for manufacturing analytics. The project transforms raw factory telemetry, operator rosters, and maintenance logs into an integrated, analytics-ready fact table.

## Project Structure

```text
manufacturing-data-pipeline/
├── data/
│   ├── schemas.py          # Strict StructType definitions for ingestion
│   ├── raw/                # Source CSV files (Immutable)
│   └── processed/          # Pipeline outputs (Parquet/CSV)
├── sql/
│   └── run_queries.py      # SQL-based metric exploration
├── src/
│   ├── data_loader.py      # Logic for schema-enforced reading
│   ├── data_cleaner.py     # Deduplication and data quality logic
│   ├── data_transformer.py # Business logic and Fact Table creation
│   └── pipeline.py         # Main orchestration script
├── tests/
│   ├── test_cleaner.py     # Unit tests for cleaning functions
│   └── test_transformer.py # Unit tests for transformation logic
├── .gitignore              # Excludes local environments and large data
├── requirements.txt        # Python dependency list
└── README.md               # Project documentation
```

## Setup and Execution

### Prerequisites

* Python 3.8+

### Installation
```bash
# Clone repository
git clone https://github.com/zaian1st/JnJ-manufacturing-data-pipeline.git
cd manufacturing-data-pipeline

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```


## Usage

### Run SQL Analysis 
Executes 5 SQL queries to answer business questions:
1. Total maintenance cost
2. Total downtime minutes
3. Maintenance events count
4. Unplanned breakdowns count
5. Average downtime per event
```bash
python sql/run_queries.py
```

### Run ETL Pipeline 
Executes full pipeline: Load → Clean → Transform → Export
```bash
python src/pipeline.py
```

**Outputs:**
- `data/processed/parquet/` - Partitioned Parquet files
- `data/processed/csv/fact_table.csv` - Single CSV file

### Run Tests 
```bash
pytest tests/ -v
```

## Architecture
```
CSV Files (Raw) 
    ↓
Bronze Layer (Loaded with explicit schemas)
    ↓
Silver Layer (Cleaned)
    ↓
Silver Layer (Transformed)
    ↓
Gold Layer (Fact table: aggregated by date/factory/line/shift)
    ↓
Output (Parquet + CSV)
```

## Design Decisions


### Explicit Schemas
- Performance: No schema inference overhead
- Type safety: Early error detection
- Documentation: Schema as code

### Data Cleaning Strategy
** to be filled ** 

### Partitioning Strategy
Parquet files partitioned by `date` and `factory_id`:
Enables partition pruning and parallel processing for query optimization

### Medallion Architecture
Bronze (raw) → Silver (clean/transformed) → Gold (aggregated):
Separates concerns, enables audit trail and replayability 

## Engineering Assumptions

* Raw data is immutable and stored in `data/raw/`.
* Timestamps are provided in UTC.
* `event_id` and `workorder_id` are treated as unique primary keys for deduplication logic.


**Author:** Zaian  
**Date:** 17 January 2026

---