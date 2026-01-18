"""
Pytest Fixtures for Manufacturing Pipeline Tests
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
from datetime import datetime, date


@pytest.fixture(scope="session")
def spark():
    """SparkSession for testing"""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("Manufacturing-Tests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()


@pytest.fixture
def sample_production_df(spark):
    """Sample production data with duplicates"""
    schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("factory_id", StringType(), False),
        StructField("line_id", StringType(), False),
        StructField("shift", StringType(), True),
        StructField("produced_qty", IntegerType(), True),
        StructField("scrap_qty", IntegerType(), True),
        StructField("defects_count", IntegerType(), True),
        StructField("oee", DoubleType(), True),
        StructField("availability", DoubleType(), True),
        StructField("performance", DoubleType(), True),
        StructField("quality", DoubleType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("vibration_mm_s", DoubleType(), True),
        StructField("pressure_bar", DoubleType(), True),
        StructField("operator_id", StringType(), True),
    ])
    
    data = [
        (datetime(2025, 11, 3, 8, 0), "F1", "L1", "S1", 100, 5, 2, 0.85, 0.9, 1.0, 0.95, 35.0, 1.5, 6.0, "OP1"),
        (datetime(2025, 11, 3, 8, 0), "F1", "L1", "S1", 100, 5, 2, 0.85, 0.9, 1.0, 0.95, 35.0, 1.5, 6.0, "OP1"),  # Duplicate
        (datetime(2025, 11, 3, 9, 0), "F1", "L1", "S1", 110, 3, 1, 0.90, 0.95, 1.0, 0.95, 36.0, 1.6, 6.1, "OP1"),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_production_with_nulls(spark):
    """Production data with NULL OEE components"""
    schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("factory_id", StringType(), False),
        StructField("line_id", StringType(), False),
        StructField("availability", DoubleType(), True),
        StructField("performance", DoubleType(), True),
        StructField("quality", DoubleType(), True),
    ])
    
    data = [
        (datetime(2025, 11, 3, 8, 0), "F1", "L1", None, 1.0, 0.95),  # NULL availability
        (datetime(2025, 11, 3, 9, 0), "F1", "L1", 0.9, None, 0.95),  # NULL performance
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_aggregated_production(spark):
    """Pre-aggregated production data for transformer tests"""
    schema = StructType([
        StructField("date", DateType(), False),
        StructField("factory_id", StringType(), False),
        StructField("line_id", StringType(), False),
        StructField("shift", StringType(), False),
        StructField("produced_qty", IntegerType(), True),
        StructField("scrap_qty", IntegerType(), True),
        StructField("defects_count", IntegerType(), True),
        StructField("oee", DoubleType(), True),
        StructField("oee_calculated", DoubleType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("vibration_mm_s", DoubleType(), True),
        StructField("pressure_bar", DoubleType(), True),
        StructField("operator_id", StringType(), True),
    ])
    
    data = [
        (date(2025, 11, 3), "FRA-PLANT-01", "Line-A", "Shift-1", 200, 10, 5, 0.85, 0.855, 35.0, 1.5, 6.0, "OP1"),
        (date(2025, 11, 3), "FRA-PLANT-01", "Line-B", "Shift-1", 150, 15, 8, 0.75, 0.76, 36.0, 1.6, 6.1, "OP2"),
        (date(2025, 11, 3), "FRA-PLANT-01", "Line-A", "Shift-2", 180, 0, 0, 0.95, 0.95, 34.0, 1.4, 5.9, "OP3"),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_maintenance_df(spark):
    """Sample maintenance events"""
    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("factory_id", StringType(), False),
        StructField("line_id", StringType(), False),
        StructField("start_time", TimestampType(), False),
        StructField("downtime_min", IntegerType(), True),
        StructField("cost_eur", DoubleType(), True),
    ])
    
    data = [
        ("M1", "FRA-PLANT-01", "Line-A", datetime(2025, 11, 3, 10, 0), 60, 500.0),
        ("M2", "FRA-PLANT-01", "Line-B", datetime(2025, 11, 3, 14, 0), 90, 750.0),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_operators_df(spark):
    """Sample operators roster"""
    schema = StructType([
        StructField("operator_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("factory_id", StringType(), True),
        StructField("primary_line", StringType(), True),
        StructField("primary_shift", StringType(), True),
    ])
    
    data = [
        ("OP1", "John Doe", "F1", "L1", "S1"),
        ("OP2", "Jane Smith", "F1", "L2", "S1"),
        ("OP3", "Bob Wilson", "F1", "L1", "S2"),
    ]
    
    return spark.createDataFrame(data, schema)