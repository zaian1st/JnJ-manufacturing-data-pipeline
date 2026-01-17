# Explicit Schema Definitions

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, DoubleType, TimestampType, DateType, BooleanType
)


PRODUCTION_SCHEMA = StructType([
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("factory_id", StringType(), nullable=False),
    StructField("line_id", StringType(), nullable=False),
    StructField("shift", StringType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("order_id", StringType(), nullable=True),
    StructField("planned_qty", IntegerType(), nullable=True),
    StructField("produced_qty", IntegerType(), nullable=True),
    StructField("scrap_qty", IntegerType(), nullable=True),
    StructField("defects_count", IntegerType(), nullable=True),
    StructField("defect_type", StringType(), nullable=True),
    StructField("cycle_time_s", DoubleType(), nullable=True),
    StructField("oee", DoubleType(), nullable=True),
    StructField("availability", DoubleType(), nullable=True),
    StructField("performance", DoubleType(), nullable=True),
    StructField("quality", DoubleType(), nullable=True),
    StructField("machine_state", StringType(), nullable=True),
    StructField("downtime_reason", StringType(), nullable=True),
    StructField("maintenance_type", StringType(), nullable=True),
    StructField("maintenance_due_date", DateType(), nullable=True),
    StructField("vibration_mm_s", DoubleType(), nullable=True),
    StructField("temperature_c", DoubleType(), nullable=True),
    StructField("pressure_bar", DoubleType(), nullable=True),
    StructField("energy_kwh", DoubleType(), nullable=True),
    StructField("operator_id", StringType(), nullable=True),
    StructField("workorder_status", StringType(), nullable=True),
])


MAINTENANCE_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("factory_id", StringType(), nullable=False),
    StructField("line_id", StringType(), nullable=False),
    StructField("maintenance_type", StringType(), nullable=True),
    StructField("reason", StringType(), nullable=True),
    StructField("start_time", TimestampType(), nullable=False),
    StructField("end_time", TimestampType(), nullable=True),
    StructField("downtime_min", IntegerType(), nullable=True),
    StructField("technician_id", StringType(), nullable=True),
    StructField("parts_used", StringType(), nullable=True),
    StructField("cost_eur", DoubleType(), nullable=True),
    StructField("outcome", StringType(), nullable=True),
    StructField("next_due_date", DateType(), nullable=True),
])


OPERATORS_SCHEMA = StructType([
    StructField("operator_id", StringType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("factory_id", StringType(), nullable=True),
    StructField("primary_line", StringType(), nullable=True),
    StructField("primary_shift", StringType(), nullable=True),
    StructField("skill_level", StringType(), nullable=True),
    StructField("certifications", StringType(), nullable=True),
    StructField("team", StringType(), nullable=True),
    StructField("hire_date", DateType(), nullable=True),
    StructField("overtime_eligible", BooleanType(), nullable=True),
    StructField("hourly_rate_eur", DoubleType(), nullable=True),
    StructField("reliability_score", DoubleType(), nullable=True),
])


FACT_TABLE_SCHEMA = StructType([
    StructField("date", DateType(), nullable=False),
    StructField("factory_id", StringType(), nullable=False),
    StructField("line_id", StringType(), nullable=False),
    StructField("shift", StringType(), nullable=False),
    StructField("total_produced", IntegerType(), nullable=True),
    StructField("total_scrap", IntegerType(), nullable=True),
    StructField("total_defects", IntegerType(), nullable=True),
    StructField("avg_oee", DoubleType(), nullable=True),
    StructField("min_oee", DoubleType(), nullable=True),
    StructField("max_oee", DoubleType(), nullable=True),
    StructField("avg_temperature_c", DoubleType(), nullable=True),
    StructField("avg_vibration_mm_s", DoubleType(), nullable=True),
    StructField("avg_pressure_bar", DoubleType(), nullable=True),
    StructField("total_downtime_min", IntegerType(), nullable=True),
    StructField("maintenance_events", IntegerType(), nullable=True),
    StructField("total_maintenance_cost_eur", DoubleType(), nullable=True),
    StructField("scrap_rate", DoubleType(), nullable=True),
    StructField("efficiency_score", DoubleType(), nullable=True),
])