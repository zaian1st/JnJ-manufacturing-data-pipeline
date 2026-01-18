"""
TASK 3: Main Pipeline Orchestrator
Bronze (Load) → Silver (Clean) → Gold (Transform) → Export
"""

import os
import sys
from pathlib import Path
import platform

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

sys.path.append(str(Path(__file__).parent.parent))

from src.data_loader import load_production_data, load_maintenance_data, load_operators_data
from src.data_cleaner import remove_duplicates, standardize_oee_metrics
from src.data_transformer import add_derived_columns, aggregate_to_fact_table


def setup_windows_hadoop():
    """Setup Hadoop for Windows to enable Parquet writes"""
    if platform.system() != 'Windows':
        return
    
    import urllib.request
    import zipfile
    import io
    
    hadoop_home = Path.cwd() / "hadoop"
    bin_dir = hadoop_home / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    
    winutils_path = bin_dir / "winutils.exe"
    hadoop_dll_path = bin_dir / "hadoop.dll"
    
    if winutils_path.exists() and hadoop_dll_path.exists():
        os.environ['HADOOP_HOME'] = str(hadoop_home)
        return
    
    print("Downloading Hadoop binaries for Windows...")
    
    files_to_download = {
        "winutils.exe": "https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/winutils.exe",
        "hadoop.dll": "https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/hadoop.dll"
    }
    
    try:
        for filename, url in files_to_download.items():
            file_path = bin_dir / filename
            if not file_path.exists():
                print(f"Downloading {filename}...")
                urllib.request.urlretrieve(url, file_path)
        
        print("Hadoop binaries downloaded successfully")
        os.environ['HADOOP_HOME'] = str(hadoop_home)
        
    except Exception as e:
        print(f"Warning: Could not download Hadoop binaries: {e}")
        print("Manual setup: https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin")



def main():
    print("\n" + "=" * 80)
    print("TASK 3: MANUFACTURING DATA PIPELINE")
    print("=" * 80)
    
    setup_windows_hadoop()
    
    spark = SparkSession.builder \
        .appName("Task3_ETL_Pipeline") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    print("Spark initialized\n")
    
    PROJECT_ROOT = Path(__file__).parent.parent
    OUTPUT_DIR = str(PROJECT_ROOT / "data" / "processed")
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("BRONZE LAYER: Loading Data")
    print("=" * 80)
    
    df_production = load_production_data(spark)
    df_maintenance = load_maintenance_data(spark)
    df_operators = load_operators_data(spark)
    
    print("\n" + "=" * 80)
    print("SILVER LAYER: Cleaning Data")
    print("=" * 80)
    
    df_production = remove_duplicates(df_production)
    df_production = standardize_oee_metrics(df_production)
    
    print("\n" + "=" * 80)
    print("GOLD LAYER: Transform & Aggregate")
    print("=" * 80)
    
    df_production = add_derived_columns(df_production)
    fact_table = aggregate_to_fact_table(df_production, df_maintenance, df_operators)
    
    print("\n" + "=" * 80)
    print("EXPORT: Saving Results")
    print("=" * 80)
    
    # Convert to Pandas for Windows-compatible file writing
    fact_table_pd = fact_table.toPandas()
    
    # Export to Parquet
    parquet_path = f"{OUTPUT_DIR}/fact_table.parquet"
    
    # Cleanup existing outputs to prevent Folder-vs-File conflicts
    if os.path.exists(parquet_path):
        import shutil
        if os.path.isdir(parquet_path): shutil.rmtree(parquet_path)
        else: os.remove(parquet_path)
    
    fact_table_pd.to_parquet(parquet_path, engine='pyarrow', compression='snappy', index=False)
    print(f"Parquet saved: {parquet_path}")
    
    # Export to CSV
    csv_path = f"{OUTPUT_DIR}/fact_table.csv"
    fact_table_pd.to_csv(csv_path, index=False)
    print(f"CSV saved: {csv_path}")
    
    print("\n" + "=" * 80)
    print("PIPELINE SUMMARY")
    print("=" * 80)
    
    final_count = fact_table.count()
    
    print(f"""
Fact Table:
  Rows:                   {final_count:,}
  Grain:                  date × factory × line × shift
  Partitioning:           By date
  
Output Formats:
  Parquet:                {parquet_path}
  CSV:                    {csv_path}
    """)
    
    print("Sample Fact Table (First 5 rows):")
    fact_table.show(5, truncate=False)
    
    spark.stop()
    print("\nPipeline Complete\n")


if __name__ == "__main__":
    main()