"""
DATA PROFILING SCRIPT - Run Outside Project
============================================

Purpose: Discover ALL data quality issues before building the pipeline

Run this script to understand:
- Schema and data types
- Missing values (NULLs)
- Duplicates
- Outliers
- Invalid values
- Data distributions
- Relationships between tables

Usage:
    python data_profiling.py
"""


import sys
import os
import sys

# FIX: Tell Spark to use the current Python executable (Windows fix)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Now import PySpark
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Initialize Spark
spark = SparkSession.builder \
    .appName("DataProfiling") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# ✅ ADD THIS LINE - Suppress verbose logs
spark.sparkContext.setLogLevel("ERROR")  # Only show errors, not warnings/info


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def profile_dataframe(df, name):
    """Comprehensive profiling of a DataFrame"""
    
    print("\n" + "=" * 80)
    print(f"PROFILING: {name}")
    print("=" * 80)
    
    # 1. BASIC INFO
    print("\n--- BASIC INFORMATION ---")
    row_count = df.count()
    col_count = len(df.columns)
    print(f"Rows: {row_count:,}")
    print(f"Columns: {col_count}")
    print(f"Size estimate: {row_count * col_count:,} cells")
    
    # 2. SCHEMA
    print("\n--- SCHEMA ---")
    df.printSchema()
    
    # 3. SAMPLE DATA
    print("\n--- SAMPLE DATA (First 5 Rows) ---")
    df.show(5, truncate=False)
    
    # 4. DATA TYPES CHECK
    print("\n--- DATA TYPES ---")
    for field in df.schema.fields:
        print(f"{field.name:30s} | {str(field.dataType):20s} | Nullable: {field.nullable}")
    
    # 5. MISSING VALUES (NULLs)
    print("\n--- MISSING VALUES (NULLs) ---")
    null_counts = []
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = (null_count / row_count * 100) if row_count > 0 else 0
        null_counts.append({
            'column': col,
            'null_count': null_count,
            'null_percentage': null_pct
        })
    
    null_df = spark.createDataFrame(null_counts)
    null_df.orderBy(F.desc('null_count')).show(100, truncate=False)
    
    # 6. DUPLICATES
    print("\n--- DUPLICATE ROWS ---")
    duplicate_count = df.count() - df.dropDuplicates().count()
    print(f"Total duplicate rows: {duplicate_count}")
    if duplicate_count > 0:
        print("⚠ WARNING: Duplicates found!")
    
    # 7. UNIQUE VALUES (for categorical columns)
    print("\n--- UNIQUE VALUE COUNTS (Categorical Columns) ---")
    for col in df.columns:
        dtype = dict(df.dtypes)[col]
        if dtype == 'string':
            unique_count = df.select(col).distinct().count()
            print(f"{col:30s} | Unique values: {unique_count}")
            if unique_count <= 20:  # Show values if small cardinality
                print(f"  Values: {[row[col] for row in df.select(col).distinct().collect()]}")
    
    # 8. NUMERIC STATISTICS
    print("\n--- NUMERIC STATISTICS ---")
    numeric_cols = [field.name for field in df.schema.fields 
                   if isinstance(field.dataType, (IntegerType, FloatType, DoubleType, LongType))]
    
    if numeric_cols:
        df.select(numeric_cols).describe().show()
        
        # Check for outliers (values beyond mean ± 3*std)
        print("\n--- OUTLIER DETECTION (Z-score > 3) ---")
        for col in numeric_cols:
            stats = df.select(
                F.mean(col).alias('mean'),
                F.stddev(col).alias('stddev'),
                F.min(col).alias('min'),
                F.max(col).alias('max')
            ).collect()[0]
            
            if stats['stddev'] and stats['stddev'] > 0:
                outlier_count = df.filter(
                    (F.col(col) < stats['mean'] - 3 * stats['stddev']) |
                    (F.col(col) > stats['mean'] + 3 * stats['stddev'])
                ).count()
                
                if outlier_count > 0:
                    print(f"{col:30s} | Outliers: {outlier_count} "
                          f"(Range: [{stats['min']:.2f}, {stats['max']:.2f}])")
    
    # 9. EMPTY STRINGS (for string columns)
    print("\n--- EMPTY STRINGS ---")
    for col in df.columns:
        dtype = dict(df.dtypes)[col]
        if dtype == 'string':
            empty_count = df.filter((F.col(col) == "") | (F.col(col) == " ")).count()
            if empty_count > 0:
                print(f"{col:30s} | Empty strings: {empty_count}")
    
    # 10. VALUE DISTRIBUTIONS (Top values)
    print("\n--- VALUE DISTRIBUTIONS (Top 10 Values) ---")
    for col in df.columns:
        dtype = dict(df.dtypes)[col]
        if dtype == 'string':
            print(f"\n{col}:")
            df.groupBy(col).count().orderBy(F.desc('count')).show(10, truncate=False)
    
    # 11. DATE/TIME CHECKS (if timestamp columns exist)
    timestamp_cols = [field.name for field in df.schema.fields 
                     if isinstance(field.dataType, (TimestampType, DateType))]
    
    if timestamp_cols:
        print("\n--- DATE/TIME RANGE ---")
        for col in timestamp_cols:
            df.select(
                F.min(col).alias(f'min_{col}'),
                F.max(col).alias(f'max_{col}')
            ).show(truncate=False)
    
    # 12. SPECIFIC DATA QUALITY CHECKS
    print("\n--- SPECIFIC DATA QUALITY CHECKS ---")
    
    # Check for negative values in quantity/cost columns
    negative_cols = [c for c in numeric_cols if any(x in c.lower() 
                     for x in ['qty', 'quantity', 'cost', 'price', 'amount'])]
    if negative_cols:
        print("\nNegative values in quantity/cost columns:")
        for col in negative_cols:
            neg_count = df.filter(F.col(col) < 0).count()
            if neg_count > 0:
                print(f"  ⚠ {col:30s} | Negative values: {neg_count}")
    
    # Check for values > 1 in percentage/ratio columns
    ratio_cols = [c for c in numeric_cols if any(x in c.lower() 
                  for x in ['oee', 'availability', 'performance', 'quality', 'rate', 'score'])]
    if ratio_cols:
        print("\nValues outside [0, 1] in ratio columns:")
        for col in ratio_cols:
            invalid_count = df.filter((F.col(col) < 0) | (F.col(col) > 1)).count()
            if invalid_count > 0:
                print(f"  ⚠ {col:30s} | Invalid values: {invalid_count}")
    
    print("\n" + "-" * 80)
    return df


def check_relationships(df1, df2, key_col, name1, name2):
    """Check referential integrity between two DataFrames"""
    
    print("\n" + "=" * 80)
    print(f"RELATIONSHIP CHECK: {name1} <-> {name2} (Key: {key_col})")
    print("=" * 80)
    
    # Keys in df1 but not in df2
    orphaned = df1.join(df2, df1[key_col] == df2[key_col], 'left_anti').count()
    print(f"Records in {name1} without matching {name2}: {orphaned}")
    
    if orphaned > 0:
        print(f"  ⚠ WARNING: Orphaned records found!")
        print(f"  Sample orphaned keys:")
        df1.join(df2, df1[key_col] == df2[key_col], 'left_anti') \
           .select(key_col).distinct().show(10, truncate=False)
    
    return orphaned


# =============================================================================
# LOAD DATA
# =============================================================================

print("\n" + "=" * 80)
print("LOADING CSV FILES")
print("=" * 80)

# Update these paths to your actual CSV locations
try:
    df_production = spark.read.csv(
        "manufacturing-data-pipeline/data/raw/manufacturing_factory_dataset.csv",
        header=True,
        inferSchema=True
    )
    print("✓ Loaded: manufacturing_factory_dataset.csv")
except Exception as e:
    print(f"✗ Error loading manufacturing_factory_dataset.csv: {e}")
    df_production = None

try:
    df_maintenance = spark.read.csv(
        "manufacturing-data-pipeline/data/raw/maintenance_events.csv",
        header=True,
        inferSchema=True
    )
    print("✓ Loaded: maintenance_events.csv")
except Exception as e:
    print(f"✗ Error loading maintenance_events.csv: {e}")
    df_maintenance = None

try:
    df_operators = spark.read.csv(
        "manufacturing-data-pipeline/data/raw/operators_roster.csv",
        header=True,
        inferSchema=True
    )
    print("✓ Loaded: operators_roster.csv")
except Exception as e:
    print(f"✗ Error loading operators_roster.csv: {e}")
    df_operators = None


# =============================================================================
# PROFILE EACH DATASET
# =============================================================================

if df_production:
    profile_dataframe(df_production, "PRODUCTION DATA")

if df_maintenance:
    profile_dataframe(df_maintenance, "MAINTENANCE EVENTS")

if df_operators:
    profile_dataframe(df_operators, "OPERATORS ROSTER")


# =============================================================================
# CHECK RELATIONSHIPS (Referential Integrity)
# =============================================================================

print("\n\n" + "=" * 80)
print("REFERENTIAL INTEGRITY CHECKS")
print("=" * 80)

if df_production and df_operators:
    check_relationships(
        df_production, df_operators, 
        'operator_id', 
        'Production', 'Operators'
    )

if df_production and df_maintenance:
    # Check if factory_id + line_id exist in both
    print("\n--- Factory/Line Consistency ---")
    prod_lines = df_production.select('factory_id', 'line_id').distinct()
    maint_lines = df_maintenance.select('factory_id', 'line_id').distinct()
    
    print(f"Unique factory/line combinations in Production: {prod_lines.count()}")
    print(f"Unique factory/line combinations in Maintenance: {maint_lines.count()}")


# =============================================================================
# BUSINESS LOGIC VALIDATION
# =============================================================================

if df_production:
    print("\n\n" + "=" * 80)
    print("BUSINESS LOGIC VALIDATION")
    print("=" * 80)
    
    # Check OEE formula: oee = availability × performance × quality
    print("\n--- OEE Formula Validation ---")
    df_oee_check = df_production.withColumn(
        'oee_calculated',
        F.col('availability') * F.col('performance') * F.col('quality')
    ).withColumn(
        'oee_diff',
        F.abs(F.col('oee') - F.col('oee_calculated'))
    )
    
    invalid_oee = df_oee_check.filter(F.col('oee_diff') > 0.01).count()
    print(f"Records where OEE formula doesn't match (tolerance 0.01): {invalid_oee}")
    
    if invalid_oee > 0:
        print("  ⚠ WARNING: OEE calculation inconsistencies found!")
        df_oee_check.filter(F.col('oee_diff') > 0.01) \
                    .select('oee', 'oee_calculated', 'oee_diff', 
                           'availability', 'performance', 'quality') \
                    .show(10)
    
    # Check: produced_qty + scrap_qty <= planned_qty (with tolerance)
    print("\n--- Production Quantity Validation ---")
    df_qty_check = df_production.withColumn(
        'total_qty',
        F.col('produced_qty') + F.col('scrap_qty')
    ).withColumn(
        'exceeds_planned',
        F.col('total_qty') > F.col('planned_qty') * 1.1  # 10% tolerance
    )
    
    exceeds_planned = df_qty_check.filter(F.col('exceeds_planned')).count()
    print(f"Records where (produced + scrap) > planned * 1.1: {exceeds_planned}")
    
    if exceeds_planned > 0:
        print("  ⚠ WARNING: Production exceeds planned quantity!")
        df_qty_check.filter(F.col('exceeds_planned')) \
                    .select('planned_qty', 'produced_qty', 'scrap_qty', 'total_qty') \
                    .show(10)


if df_maintenance:
    print("\n--- Maintenance Time Logic Validation ---")
    
    # Check: end_time > start_time
    df_time_check = df_maintenance.filter(
        (F.col('end_time').isNotNull()) & 
        (F.col('start_time').isNotNull())
    ).withColumn(
        'time_valid',
        F.col('end_time') > F.col('start_time')
    )
    
    invalid_times = df_time_check.filter(~F.col('time_valid')).count()
    print(f"Records where end_time <= start_time: {invalid_times}")
    
    if invalid_times > 0:
        print("  ⚠ WARNING: Invalid time ranges found!")
        df_time_check.filter(~F.col('time_valid')) \
                     .select('event_id', 'start_time', 'end_time') \
                     .show(10)


# =============================================================================
# SUMMARY REPORT
# =============================================================================

print("\n\n" + "=" * 80)
print("DATA QUALITY SUMMARY REPORT")
print("=" * 80)

print("""
Key Findings to Address in Pipeline:
1. Check NULL counts above - which columns need imputation?
2. Check duplicate counts - need deduplication logic?
3. Check outliers - need outlier detection/removal?
4. Check OEE formula - need validation/recalculation?
5. Check time ranges - need data filtering?
6. Check referential integrity - need handling of orphaned records?

Next Steps:
1. Review all ⚠ WARNING messages above
2. Decide cleaning strategy for each issue
3. Implement in data_cleaner.py
4. Add validation in data_transformer.py
""")

# Stop Spark
spark.stop()
print("\n✓ Profiling complete. Spark session stopped.\n")