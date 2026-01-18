"""
TASK 3: Data Transformer - Gold Layer
One row per: date × factory × line × shift
aggregating the data, not keeping every raw record
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Add date, hour, day_of_week"""
    df_enriched = df.withColumn("date", F.to_date(F.col("timestamp"))) \
                    .withColumn("hour", F.hour(F.col("timestamp"))) \
                    .withColumn("day_of_week", F.dayofweek(F.col("timestamp")))
    
    print("Derived columns added")
    return df_enriched


def aggregate_to_fact_table(
    df_production: DataFrame,
    df_maintenance: DataFrame,
    df_operators: DataFrame
) -> DataFrame:
    """Aggregate to fact table grain"""
    
    df_prod_with_date = df_production.withColumn("date", F.to_date(F.col("timestamp")))
    
    df_enriched = df_prod_with_date.join(
        F.broadcast(df_operators),
        on="operator_id",
        how="left"
    )
    
    df_enriched = df_enriched.drop(df_operators["factory_id"]) \
                             .drop(df_operators["primary_line"]) \
                             .drop(df_operators["primary_shift"])
    
    print("Broadcast join: Production + Operators")
    
    fact_table = df_enriched.groupBy("date", "factory_id", "line_id", "shift").agg(
        F.sum("produced_qty").alias("total_produced"),
        F.sum("scrap_qty").alias("total_scrap"),
        F.sum("defects_count").alias("total_defects"),
        F.avg(F.coalesce(F.col("oee"), F.lit(0.0))).alias("avg_oee"),
        F.avg(F.coalesce(F.col("oee_calculated"), F.lit(0.0))).alias("avg_oee_calculated"),
        F.min("oee").alias("min_oee"),
        F.max("oee").alias("max_oee"),
        F.avg(F.coalesce(F.col("temperature_c"), F.lit(0.0))).alias("avg_temperature_c"),
        F.avg(F.coalesce(F.col("vibration_mm_s"), F.lit(0.0))).alias("avg_vibration_mm_s"),
        F.avg(F.coalesce(F.col("pressure_bar"), F.lit(0.0))).alias("avg_pressure_bar"),
    )
    
    df_maint_with_date = df_maintenance.withColumn("date", F.to_date(F.col("start_time")))
    
    maint_agg = df_maint_with_date.groupBy("date", "factory_id", "line_id").agg(
        F.sum(F.coalesce(F.col("downtime_min"), F.lit(0))).alias("total_downtime_min"),
        F.count("event_id").alias("maintenance_events"),
        F.sum(F.coalesce(F.col("cost_eur"), F.lit(0.0))).alias("total_maintenance_cost_eur")
    )
    
    fact_table = fact_table.join(
        maint_agg,
        on=["date", "factory_id", "line_id"],
        how="left"
    )
    
    fact_table = fact_table.fillna(0, subset=["total_downtime_min", "maintenance_events", "total_maintenance_cost_eur"])
    
    fact_table = fact_table.withColumn(
        "scrap_rate",
        F.when(F.col("total_produced") > 0, 
               F.col("total_scrap") / F.col("total_produced")).otherwise(0.0)
    ).withColumn(
        "efficiency_score",
        F.col("avg_oee") * (1 - F.col("scrap_rate"))
    )
    
    fact_table = fact_table.select(
        "date",
        "factory_id",
        "line_id",
        "shift",
        "total_produced",
        "total_scrap",
        "total_defects",
        "avg_oee",
        "avg_oee_calculated",
        "min_oee",
        "max_oee",
        "avg_temperature_c",
        "avg_vibration_mm_s",
        "avg_pressure_bar",
        "total_downtime_min",
        "maintenance_events",
        "total_maintenance_cost_eur",
        "scrap_rate",
        "efficiency_score"
    )
    
    print(f"Fact table created: {fact_table.count():,} rows")
    return fact_table