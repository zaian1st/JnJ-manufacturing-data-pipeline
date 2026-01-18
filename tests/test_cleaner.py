"""
Unit Tests for data_cleaner.py
Tests: remove_duplicates() and standardize_oee_metrics()
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from src.data_cleaner import remove_duplicates, standardize_oee_metrics


def test_remove_duplicates_removes_duplicates(sample_production_df):
    """Test that duplicates are removed"""
    result = remove_duplicates(sample_production_df)
    
    assert result.count() == 2, "Should have 2 unique rows (1 duplicate removed)"
    
    rows = result.collect()
    assert len(rows) == 2


def test_remove_duplicates_keeps_latest(sample_production_df):
    """Test that the latest record is kept when duplicates exist"""
    result = remove_duplicates(sample_production_df)
    
    rows = [row.asDict() for row in result.collect()]
    
    assert len(rows) == 2
    assert any(row['produced_qty'] == 100 for row in rows)
    assert any(row['produced_qty'] == 110 for row in rows)


def test_remove_duplicates_no_duplicates(spark):
    """Edge case: DataFrame with no duplicates"""
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    from datetime import datetime
    
    schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("factory_id", StringType(), False),
        StructField("line_id", StringType(), False),
    ])
    
    data = [
        (datetime(2025, 11, 3, 8, 0), "F1", "L1"),
        (datetime(2025, 11, 3, 9, 0), "F1", "L1"),
        (datetime(2025, 11, 3, 10, 0), "F1", "L1"),
    ]
    
    df = spark.createDataFrame(data, schema)
    result = remove_duplicates(df)
    
    assert result.count() == 3, "All rows should remain (no duplicates)"


def test_standardize_oee_adds_calculated_column(sample_production_df):
    """Test that oee_calculated column is added"""
    result = standardize_oee_metrics(sample_production_df)
    
    assert "oee_calculated" in result.columns
    assert "is_high_performance" in result.columns


def test_standardize_oee_calculation_correct(spark):
    """Test that oee_calculated = availability × performance × quality"""
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
    from datetime import datetime
    
    schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("factory_id", StringType(), False),
        StructField("line_id", StringType(), False),
        StructField("availability", DoubleType(), True),
        StructField("performance", DoubleType(), True),
        StructField("quality", DoubleType(), True),
    ])
    
    data = [
        (datetime(2025, 11, 3, 8, 0), "F1", "L1", 0.9, 1.0, 0.95),  # Expected: 0.855
        (datetime(2025, 11, 3, 9, 0), "F1", "L1", 0.8, 0.9, 1.0),   # Expected: 0.72
    ]
    
    df = spark.createDataFrame(data, schema)
    result = standardize_oee_metrics(df)
    
    rows = result.collect()
    
    assert abs(rows[0]['oee_calculated'] - 0.855) < 0.001
    assert abs(rows[1]['oee_calculated'] - 0.72) < 0.001


def test_standardize_oee_null_handling(sample_production_with_nulls):
    """Edge case: NULL values in OEE components should use 0.0"""
    result = standardize_oee_metrics(sample_production_with_nulls)
    
    rows = result.collect()
    
    for row in rows:
        assert row['oee_calculated'] == 0.0, "NULL components should result in 0.0"


def test_standardize_oee_high_performance_flag(spark):
    """Test that is_high_performance flags performance > 1.2"""
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
    from datetime import datetime
    
    schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("factory_id", StringType(), False),
        StructField("line_id", StringType(), False),
        StructField("availability", DoubleType(), True),
        StructField("performance", DoubleType(), True),
        StructField("quality", DoubleType(), True),
    ])
    
    data = [
        (datetime(2025, 11, 3, 8, 0), "F1", "L1", 0.9, 1.0, 0.95),   # Normal
        (datetime(2025, 11, 3, 9, 0), "F1", "L1", 0.9, 1.25, 0.95),  # High performance
        (datetime(2025, 11, 3, 10, 0), "F1", "L1", 0.9, 1.1, 0.95),  # Normal
    ]
    
    df = spark.createDataFrame(data, schema)
    result = standardize_oee_metrics(df)
    
    rows = result.collect()
    
    assert rows[0]['is_high_performance'] == False
    assert rows[1]['is_high_performance'] == True
    assert rows[2]['is_high_performance'] == False


def test_standardize_oee_empty_dataframe(spark):
    """Edge case: Empty DataFrame"""
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
    
    schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("factory_id", StringType(), False),
        StructField("line_id", StringType(), False),
        StructField("availability", DoubleType(), True),
        StructField("performance", DoubleType(), True),
        StructField("quality", DoubleType(), True),
    ])
    
    df = spark.createDataFrame([], schema)
    result = standardize_oee_metrics(df)
    
    assert result.count() == 0
    assert "oee_calculated" in result.columns
    assert "is_high_performance" in result.columns