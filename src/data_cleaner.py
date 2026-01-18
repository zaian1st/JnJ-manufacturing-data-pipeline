"""
TASK 3: Data Cleaner - Silver Layer
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def remove_duplicates(df: DataFrame) -> DataFrame:
    """Keep latest by timestamp"""
    window_spec = Window.partitionBy("timestamp", "factory_id", "line_id") \
                        .orderBy(F.col("timestamp").desc())
    
    df_dedup = df.withColumn("row_num", F.row_number().over(window_spec)) \
                 .filter(F.col("row_num") == 1) \
                 .drop("row_num")
    
    print(f"Deduplication: {df.count() - df_dedup.count()} duplicates removed")
    return df_dedup


def standardize_oee_metrics(df: DataFrame) -> DataFrame:
    """
    Existing: oee, availability, performance, quality
    New: oee_calculated, is_high_performance
    why ? If A * P * Q != OEE, then it is a sensor synchronization error
    For high performance, flag Overclocking , Baseline Drift or sensor double counting
    """
    df_clean = df.withColumn(
        "oee_calculated",
        F.coalesce(F.col("availability"), F.lit(0.0)) * 
        F.coalesce(F.col("performance"), F.lit(0.0)) * 
        F.coalesce(F.col("quality"), F.lit(0.0))
    ).withColumn(
        "is_high_performance",
        F.when(F.col("performance") > 1.2, True).otherwise(False)
    )
    
    print(f"OEE Standardization: {df_clean.filter(F.col('is_high_performance')).count()} high-performance flagged")
    return df_clean