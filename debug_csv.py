"""
Debug script to check CSV column names and data
"""

from pyspark.sql import SparkSession

def debug_csv_files():
    """Debug CSV files to see actual column names."""
    
    # Create Spark session
    spark = SparkSession.builder.appName("DebugCSV").getOrCreate()
    
    # Test coverage allocation file
    print("=== Coverage Allocation File ===")
    coverage_file = "scratch/gold/host_coverage_pattern_allocation.csv"
    coverage_df = spark.read.csv(coverage_file, header=True, inferSchema=True)
    
    print(f"Schema:")
    coverage_df.printSchema()
    
    print(f"Columns: {coverage_df.columns}")
    
    print("First few rows:")
    coverage_df.show(5)
    
    # Test pattern factors file
    print("\n=== Pattern Factors File ===")
    factors_file = "scratch/gold/host_pattern_factor.csv"
    factors_df = spark.read.csv(factors_file, header=True, inferSchema=True)
    
    print(f"Schema:")
    factors_df.printSchema()
    
    print(f"Columns: {factors_df.columns}")
    
    print("First few rows:")
    factors_df.show(5)
    
    # Test pattern file
    print("\n=== Pattern File ===")
    pattern_file = "scratch/gold/host_pattern.csv"
    pattern_df = spark.read.csv(pattern_file, header=True, inferSchema=True)
    
    print(f"Schema:")
    pattern_df.printSchema()
    
    print(f"Columns: {pattern_df.columns}")
    
    print("First few rows:")
    pattern_df.show(5)
    
    spark.stop()

if __name__ == "__main__":
    debug_csv_files()
