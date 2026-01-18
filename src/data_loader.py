"""
TASK 3: Data Loader - Bronze Layer

Loads CSV files with explicit schemas
Validates primary keys
Optimizations: FAILFAST mode, single count(), centralized paths
"""

import os
import sys
from pathlib import Path

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).parent.parent))
from data.schemas import PRODUCTION_SCHEMA, MAINTENANCE_SCHEMA, OPERATORS_SCHEMA


PROJECT_ROOT = Path(__file__).parent.parent
DATA_PATHS = {
    "production": str(PROJECT_ROOT / "data" / "raw" / "manufacturing_factory_dataset.csv"),
    "maintenance": str(PROJECT_ROOT / "data" / "raw" / "maintenance_events.csv"),
    "operators": str(PROJECT_ROOT / "data" / "raw" / "operators_roster.csv")
}


def load_production_data(spark: SparkSession) -> DataFrame:
    df = spark.read \
        .schema(PRODUCTION_SCHEMA) \
        .option("header", "true") \
        .option("mode", "FAILFAST") \
        .csv(DATA_PATHS["production"])
    
    df_valid = df.filter(
        F.col("timestamp").isNotNull() &
        F.col("factory_id").isNotNull() &
        F.col("line_id").isNotNull()
    )
    
    print("Production: loaded")
    return df_valid


def load_maintenance_data(spark: SparkSession) -> DataFrame:
    df = spark.read \
        .schema(MAINTENANCE_SCHEMA) \
        .option("header", "true") \
        .option("mode", "FAILFAST") \
        .csv(DATA_PATHS["maintenance"])
    
    df_valid = df.filter(F.col("event_id").isNotNull())
    print("Maintenance: loaded")
    return df_valid


def load_operators_data(spark: SparkSession) -> DataFrame:
    df = spark.read \
        .schema(OPERATORS_SCHEMA) \
        .option("header", "true") \
        .option("mode", "FAILFAST") \
        .csv(DATA_PATHS["operators"])
    
    df_valid = df.filter(F.col("operator_id").isNotNull())
    print("Operators: loaded")
    return df_valid