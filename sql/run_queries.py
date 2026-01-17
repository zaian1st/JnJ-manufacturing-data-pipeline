"""
SQL Analysis on Manufacturing Data
"""
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession

# Ensure Spark uses current Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Add project root to PYTHONPATH
sys.path.append(str(Path(__file__).parent.parent))

from data.schemas import MAINTENANCE_SCHEMA


def main():

    print("TASK 2: SQL ANALYSIS — MAINTENANCE DATA")
    print("=" * 60)


    # Configuration 
    DATA_ROOT = os.getenv(
        "DATA_ROOT",
        "manufacturing-data-pipeline/data/raw"
    )

    MAINTENANCE_PATH = f"{DATA_ROOT}/maintenance_events.csv"


    # Spark Initialization
    spark = (
        SparkSession.builder
        .appName("Task2_SQL_Analysis")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # ------------------------------------------------------------------
    # Load Maintenance Events )
    # ------------------------------------------------------------------
    df = (
        spark.read
        .schema(MAINTENANCE_SCHEMA)
        .option("header", "true")
        .csv(MAINTENANCE_PATH)
    )

    df.createOrReplaceTempView("maintenance_events")

    print(f"✓ Loaded {df.count()} maintenance events\n")

    # ------------------------------------------------------------------
    # SINGLE-PASS ANALYTICAL QUERY   : I didnt want to scan the data 5 times so I commented the intial 5 queries and combined them into one
    # ------------------------------------------------------------------
    # Design Principles:
    # - One scan of the dataset (cost-efficient at scale)
    # - Defensive filtering against corrupted records
    # - Inline logic for all 5 required business questions
    # - Robust statistics to contextualize averages
    result = spark.sql("""
        SELECT
            -- Q1: Total maintenance cost
            ROUND(SUM(cost_eur), 2) AS total_maintenance_cost_eur,

            -- Q2: Total downtime minutes
            SUM(downtime_min) AS total_downtime_minutes,

            -- Q3: Total number of maintenance events
            COUNT(event_id) AS total_maintenance_events,

            -- Q4: Unplanned breakdown count
            -- Using UPPER + LIKE to avoid fragile string matching
            COUNT(
                CASE
                    WHEN UPPER(reason) LIKE '%UNPLANNED%'
                    THEN 1
                END
            ) AS unplanned_breakdowns,

            -- Q5: Average downtime per event (with statistical context)
            ROUND(AVG(downtime_min), 2) AS avg_downtime_minutes,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY downtime_min)
                AS median_downtime_minutes,
            ROUND(STDDEV(downtime_min), 2) AS downtime_stddev_minutes

        FROM maintenance_events
        WHERE
            cost_eur IS NOT NULL
            AND cost_eur >= 0
            AND downtime_min IS NOT NULL
            AND downtime_min >= 0
    """)

    result.show(truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()


    
"""
QUESTION 1: Total maintenance cost
------------------------------------------------------------------
Thought process:
- Each row represents one maintenance event
- cost_eur is recorded per event
- NULL or negative costs are invalid and excluded

spark.sql(
    \"""
    SELECT
        ROUND(SUM(cost_eur), 2) AS total_maintenance_cost_eur
        -- Sum of all valid maintenance costs
    FROM maintenance_events
    WHERE cost_eur IS NOT NULL
      AND cost_eur >= 0
    \"""
).show(truncate=False)
"""


"""
QUESTION 2: Total downtime minutes
------------------------------------------------------------------
Thought process:
- downtime_min is already calculated per event
- summing gives total downtime across all maintenance activity
- NULL or negative values indicate bad records and are excluded

spark.sql(
    \"""
    SELECT
        SUM(downtime_min) AS total_downtime_minutes
        -- Total minutes of production downtime caused by maintenance
    FROM maintenance_events
    WHERE downtime_min IS NOT NULL
      AND downtime_min >= 0
    \"""
).show(truncate=False)
"""


"""
QUESTION 3: How many maintenance events occurred?
------------------------------------------------------------------
Thought process:
- One row equals one maintenance event
- event_id is treated as a unique primary key

spark.sql(
    \"""
    SELECT
        COUNT(*) AS total_maintenance_events
        -- Each row represents a single maintenance event
    FROM maintenance_events
    \"""
).show(truncate=False)
"""


"""
QUESTION 4: How many breakdowns (unplanned) happened?
------------------------------------------------------------------
Thought process:
- Planned vs unplanned is encoded in the 'reason' column
- Unplanned breakdowns are explicitly labeled

spark.sql(
    \"""
    SELECT
        COUNT(*) AS unplanned_breakdowns
        -- Events where maintenance was reactive, not scheduled
    FROM maintenance_events
    WHERE reason = 'Unplanned Breakdown'
    \"""
).show(truncate=False)
"""


"""
QUESTION 5: Average downtime per event
------------------------------------------------------------------
Thought process:
- Average downtime gives typical impact per maintenance event
- Calculated across all valid downtime records

spark.sql(
    \"""
    SELECT
        ROUND(AVG(downtime_min), 2) AS avg_downtime_minutes_per_event
        -- Mean downtime caused by a single maintenance event
    FROM maintenance_events
    WHERE downtime_min IS NOT NULL
      AND downtime_min >= 0
    \"""
).show(truncate=False)
"""
