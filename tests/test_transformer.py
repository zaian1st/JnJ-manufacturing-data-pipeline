"""
Unit Tests for data_transformer.py
Tests: aggregate_to_fact_table() grain and calculations
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from src.data_transformer import aggregate_to_fact_table
from pyspark.sql import functions as F
from datetime import date


def test_aggregate_fact_table_grain(spark, sample_aggregated_production, sample_maintenance_df, sample_operators_df):
    """Test that fact table has correct grain: date × factory × line × shift"""
    
    df_prod = sample_aggregated_production.withColumn("timestamp", F.current_timestamp())
    
    result = aggregate_to_fact_table(df_prod, sample_maintenance_df, sample_operators_df)
    
    assert result.count() == 3, "Should have 3 rows (3 unique date/factory/line/shift combinations)"
    
    grain_columns = ["date", "factory_id", "line_id", "shift"]
    for col in grain_columns:
        assert col in result.columns, f"Missing grain column: {col}"


def test_aggregate_fact_table_columns(spark, sample_aggregated_production, sample_maintenance_df, sample_operators_df):
    """Test that fact table has all required columns"""
    
    df_prod = sample_aggregated_production.withColumn("timestamp", F.current_timestamp())
    
    result = aggregate_to_fact_table(df_prod, sample_maintenance_df, sample_operators_df)
    
    required_columns = [
        "date", "factory_id", "line_id", "shift",
        "total_produced", "total_scrap", "total_defects",
        "avg_oee", "avg_oee_calculated", "min_oee", "max_oee",
        "avg_temperature_c", "avg_vibration_mm_s", "avg_pressure_bar",
        "total_downtime_min", "maintenance_events", "total_maintenance_cost_eur",
        "scrap_rate", "efficiency_score"
    ]
    
    for col in required_columns:
        assert col in result.columns, f"Missing required column: {col}"


def test_aggregate_scrap_rate_calculation(spark, sample_aggregated_production, sample_maintenance_df, sample_operators_df):
    """Test that scrap_rate = total_scrap / total_produced"""
    
    df_prod = sample_aggregated_production.withColumn("timestamp", F.current_timestamp())
    
    result = aggregate_to_fact_table(df_prod, sample_maintenance_df, sample_operators_df)
    
    rows = result.collect()
    
    for row in rows:
        if row['total_produced'] > 0:
            expected_scrap_rate = row['total_scrap'] / row['total_produced']
            assert abs(row['scrap_rate'] - expected_scrap_rate) < 0.001


def test_aggregate_efficiency_score_calculation(spark, sample_aggregated_production, sample_maintenance_df, sample_operators_df):
    """Test that efficiency_score = avg_oee × (1 - scrap_rate)"""
    
    df_prod = sample_aggregated_production.withColumn("timestamp", F.current_timestamp())
    
    result = aggregate_to_fact_table(df_prod, sample_maintenance_df, sample_operators_df)
    
    rows = result.collect()
    
    for row in rows:
        expected_efficiency = row['avg_oee'] * (1 - row['scrap_rate'])
        assert abs(row['efficiency_score'] - expected_efficiency) < 0.001


def test_aggregate_division_by_zero(spark, sample_maintenance_df, sample_operators_df):
    """Edge case: total_produced = 0 should result in scrap_rate = 0"""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
    
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
        (date(2025, 11, 3), "F1", "L1", "S1", 0, 0, 0, 0.0, 0.0, 35.0, 1.5, 6.0, "OP1"),
    ]
    
    df_prod = spark.createDataFrame(data, schema).withColumn("timestamp", F.current_timestamp())
    
    result = aggregate_to_fact_table(df_prod, sample_maintenance_df, sample_operators_df)
    
    row = result.collect()[0]
    assert row['scrap_rate'] == 0.0, "Division by zero should result in scrap_rate = 0.0"


def test_aggregate_no_maintenance(spark, sample_aggregated_production, sample_operators_df):
    """Edge case: No maintenance events should result in 0 values"""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
    
    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("factory_id", StringType(), False),
        StructField("line_id", StringType(), False),
        StructField("start_time", TimestampType(), False),
        StructField("downtime_min", IntegerType(), True),
        StructField("cost_eur", DoubleType(), True),
    ])
    
    empty_maintenance = spark.createDataFrame([], schema)
    
    df_prod = sample_aggregated_production.withColumn("timestamp", F.current_timestamp())
    
    result = aggregate_to_fact_table(df_prod, empty_maintenance, sample_operators_df)
    
    rows = result.collect()
    
    for row in rows:
        assert row['total_downtime_min'] == 0
        assert row['maintenance_events'] == 0
        assert row['total_maintenance_cost_eur'] == 0.0


def test_aggregate_maintenance_join(spark, sample_aggregated_production, sample_maintenance_df, sample_operators_df):
    """Test that maintenance data is correctly joined at line level"""
    
    test_date_str = "2026-01-18"
    df_prod = sample_aggregated_production.withColumn("timestamp", F.to_timestamp(F.lit(f"{test_date_str} 08:00:00")))

    # FIX: Only update date and factory, but leave line_id as it was in the fixture
    # This prevents M1 and M2 from merging into the same line
    maint_fixed = sample_maintenance_df.withColumn("start_time", F.to_timestamp(F.lit(f"{test_date_str} 10:00:00"))) \
                                       .withColumn("factory_id", F.lit("FRA-PLANT-01"))

    # Act
    result = aggregate_to_fact_table(df_prod, maint_fixed, sample_operators_df)

    # Assert
    # Now L1 (which is Line-A in your fixture) should have exactly 60
    l1_s1_row = result.filter((F.col("line_id") == "Line-A") & (F.col("shift") == "Shift-1")).collect()[0]
    assert l1_s1_row['total_downtime_min'] == 60
    
    # And L2 (Line-B) should have exactly 90
    l2_row = result.filter((F.col("line_id") == "Line-B")).collect()[0]
    assert l2_row['total_downtime_min'] == 90

def test_aggregate_null_values_handled(spark, sample_maintenance_df, sample_operators_df):
    """Edge case: NULL values in numeric fields should be handled gracefully"""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
    
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
        (date(2025, 11, 3), "F1", "L1", "S1", 100, 5, None, None, None, None, None, None, "OP1"),
    ]
    
    df_prod = spark.createDataFrame(data, schema).withColumn("timestamp", F.current_timestamp())
    
    result = aggregate_to_fact_table(df_prod, sample_maintenance_df, sample_operators_df)
    
    assert result.count() == 1
    row = result.collect()[0]
    
    assert row['total_produced'] == 100
    assert row['avg_oee'] == 0.0
    assert row['avg_temperature_c'] == 0.0